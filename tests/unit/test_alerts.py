import asyncio
import json

from fastapi.testclient import TestClient

from common.geo import load_ports
from dashboard.alerts import AlertFeed, alert_events, parse_alert
from dashboard.app import create_app
from tests.fakes import FakeHBase


def alert(n: int, kind: str = "dark_ship") -> dict:
    return {
        "alert_id": f"a{n}",
        "type": kind,
        "port_id": "USLAX",
        "event_time": f"2023-01-02T0{n}:00:00.000Z",
    }


def test_feed_is_newest_first_deduped_and_bounded():
    feed = AlertFeed(size=3)
    assert [feed.add(alert(n)) for n in (1, 2, 2, 3, 4)] == [True, True, False, True, True]
    assert [a["alert_id"] for a in feed.latest(10)] == ["a4", "a3", "a2"]  # a1 fell out of the window
    assert [a["alert_id"] for a in feed.latest(2)] == ["a4", "a3"]
    assert feed.add(alert(1))  # evicted ids can come back
    assert feed.total == 5


def test_since_returns_only_what_arrived_after():
    feed = AlertFeed(size=3)
    for n in (1, 2):
        feed.add(alert(n))
    assert feed.since(2) == []
    assert [a["alert_id"] for a in feed.since(1)] == ["a2"]
    for n in (3, 4, 5):
        feed.add(alert(n))
    assert [a["alert_id"] for a in feed.since(0)] == ["a3", "a4", "a5"]  # older ones are gone, not an error


def test_parse_alert_rejects_junk():
    good = json.dumps(alert(1)).encode()
    assert parse_alert(good) == alert(1)
    for bad in (None, b"", b"not json", b"[1]", b'{"type":"dark_ship"}', b'{"alert_id":5,"type":"x"}'):
        assert parse_alert(bad) is None


def test_stream_sends_new_alerts_then_keepalive():
    feed = AlertFeed()
    feed.add(alert(1))  # already in the feed: the stream starts after it

    async def run() -> list[str]:
        events = alert_events(feed, poll=0.01, keepalive=0.05)
        out = [await anext(events)]  # nothing new yet -> keepalive
        feed.add(alert(2, "queue_growth"))
        out.append(await anext(events))
        await events.aclose()
        return out

    keepalive, data = asyncio.run(run())
    assert keepalive == ": keepalive\n\n"
    assert data.startswith("data: ") and data.endswith("\n\n")
    assert json.loads(data[6:]) == alert(2, "queue_growth")


def test_alerts_endpoint():
    feed = AlertFeed()
    for n in (1, 2, 3):
        feed.add(alert(n))
    api = TestClient(create_app(FakeHBase(), load_ports(), {}, feed))  # type: ignore[arg-type]
    r = api.get("/api/alerts")
    assert [a["alert_id"] for a in r.json()] == ["a3", "a2", "a1"]
    assert [a["alert_id"] for a in api.get("/api/alerts?limit=1").json()] == ["a3"]
    for bad in ("0", "501", "x"):
        r = api.get(f"/api/alerts?limit={bad}")
        assert r.status_code == 422 and r.json()["error"]["code"] == "INVALID_PARAM"
