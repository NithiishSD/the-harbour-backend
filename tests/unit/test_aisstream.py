import asyncio
import json

import pytest
from websockets.asyncio.server import serve

from common import topics
from common.schema import normalize
from ingest.aisstream import MESSAGE_TYPES, FatalStreamError, backoff, run, subscription
from tests.samples import aisstream_position, aisstream_static

BBOX = ((1.0, 103.5, 1.5, 104.3),)


class FakeSender:
    def __init__(self):
        self.sent = []

    def send(self, topic, key, value):
        self.sent.append((topic, key, value))

    def flush(self, timeout=30):
        return 0


def test_subscription_shape():
    sub = subscription("k", (*BBOX, (33.4, -118.6, 33.9, -117.9)))
    assert sub == {
        "APIKey": "k",
        "BoundingBoxes": [[[1.0, 103.5], [1.5, 104.3]], [[33.4, -118.6], [33.9, -117.9]]],
        "FilterMessageTypes": MESSAGE_TYPES,
    }


def test_backoff_is_capped_and_jittered():
    assert all(0.5 <= backoff(0) <= 1 for _ in range(50))
    assert all(backoff(20, cap=60) <= 60 for _ in range(50))


async def _fake_server(handler):
    server = await serve(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    return server, f"ws://127.0.0.1:{port}"


def test_streams_messages_and_reconnects_after_drop(caplog):
    connections = []

    async def handler(ws):
        sub = json.loads(await ws.recv())
        connections.append(sub)
        if len(connections) == 1:
            await ws.send(json.dumps(aisstream_position(mmsi=419000001)))
            await ws.send("not json")
            await ws.send(json.dumps(aisstream_static(mmsi=419000002)).encode())  # binary frame
            return  # server drops the connection
        await ws.send(json.dumps(aisstream_position(mmsi=419000003)))
        await asyncio.sleep(1)

    async def scenario():
        server, url = await _fake_server(handler)
        sender = FakeSender()
        stats = await run(url, "secret-key", BBOX, sender, stop_after=3, max_backoff=0.05)
        server.close()
        return sender, stats

    sender, stats = asyncio.run(scenario())
    assert len(connections) == 2
    assert all(c["APIKey"] == "secret-key" for c in connections)
    assert stats.received == 3 and stats.unparseable == 1
    assert [key for _, key, _ in sender.sent] == ["419000001", "419000002", "419000003"]
    assert all(topic == topics.RAW for topic, _, _ in sender.sent)
    assert all(normalize(env).reject is None for _, _, env in sender.sent)
    assert "secret-key" not in caplog.text


def test_server_error_is_fatal():
    async def handler(ws):
        await ws.recv()
        await ws.send(json.dumps({"error": "Api Key Is Not Valid"}))
        await asyncio.sleep(1)

    async def scenario():
        server, url = await _fake_server(handler)
        try:
            await run(url, "bad", BBOX, FakeSender(), stop_after=1, max_backoff=0.05)
        finally:
            server.close()

    with pytest.raises(FatalStreamError, match="Api Key"):
        asyncio.run(scenario())


def test_the_connection_starts_again_with_the_new_boxes_when_the_choice_changes(monkeypatch):
    import ingest.aisstream as ais

    monkeypatch.setattr(ais, "CHECK_EVERY_S", 0)  # ask on every message
    seen = []
    first, second = ((1.0, 103.5, 1.5, 104.3),), ((1.0, 103.5, 1.5, 104.3), (22.0, 113.5, 22.6, 114.5))
    wanted = [first]

    async def handler(ws):
        seen.append(json.loads(await ws.recv())["BoundingBoxes"])
        for mmsi in (419000001, 419000002, 419000003):
            await ws.send(json.dumps(aisstream_position(mmsi=mmsi)))
            wanted[0] = second  # the viewer chose another country while it was connected
            await asyncio.sleep(0.05)

    async def scenario():
        server, url = await _fake_server(handler)
        stats = await run(url, "k", lambda: wanted[0], FakeSender(), stop_after=4, max_backoff=0.05)
        server.close()
        return stats

    asyncio.run(scenario())
    assert seen[0] == [[[1.0, 103.5], [1.5, 104.3]]]
    assert seen[1] == [[[1.0, 103.5], [1.5, 104.3]], [[22.0, 113.5], [22.6, 114.5]]]  # then the wider ones
