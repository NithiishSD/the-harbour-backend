import asyncio
import contextlib
import json
import time

from websockets.asyncio.server import serve

from common.follow import arrived, voyage_mmsis
from common.geo import load_world_ports
from common.monitor import Monitor
from ingest.aisstream import MAX_FOLLOWED, WORLD, run, subscription
from tests.samples import aisstream_position


class FakeSender:
    def __init__(self):
        self.sent = []

    def send(self, topic, key, value):
        self.sent.append((topic, key, value))

    def flush(self, timeout=30):
        return 0


WORLD_PORTS = load_world_ports()
SG = Monitor.build(WORLD_PORTS, "SG", partners=("CN", "JP"))
NOW = 1_790_000_000_000
H = 3_600_000
SINGAPORE = (1.264, 103.84)
SHANGHAI = (31.23, 121.47)


def ship(mmsi, lat, lon, src=None, dest=None, age_h=1.0, state="underway", sog=12.0, first_h=100.0):
    from dashboard.live import iso

    return {
        "mmsi": mmsi, "lat": lat, "lon": lon, "destination": dest, "state": state, "sog": sog,
        "src_lat": src[0] if src else None, "src_lon": src[1] if src else None,
        "last_seen": iso(int(NOW - age_h * H)), "first_seen": iso(int(NOW - first_h * H)),
    }  # fmt: skip


def test_the_list_holds_ships_on_a_voyage_to_or_from_a_monitored_port_silent_ones_first():
    ships = [
        ship("100000001", 5.0, 108.0, src=SINGAPORE, dest="JP YOK", age_h=30),  # left Singapore, silent 30 h
        ship("100000002", 20.0, 119.0, src=SHANGHAI, dest="SGSIN", age_h=2),  # bound for Singapore from China
        ship("100000003", 1.28, 103.95),  # only in the waters: not a voyage
        ship("100000004", 40.0, 150.0),  # unrelated
        ship("100000005", 5.0, 108.0, src=SINGAPORE, dest="JP YOK", age_h=10),  # left, silent 10 h
    ]
    got = voyage_mmsis(ships, SG, WORLD_PORTS, NOW)
    # the longest silent first; the ship only in the waters and the unrelated one are not followed
    assert got == ["100000001", "100000005", "100000002"]


def test_a_ship_that_has_arrived_or_whose_voyage_is_too_old_is_not_followed():
    berthed = ship("100000001", 1.26, 103.84, src=SHANGHAI, dest="SGSIN", state="berthed", sog=0.0)
    at_dest = ship("100000002", 35.45, 139.65, src=SINGAPORE, dest="JP YOK", sog=0.1)  # standing at Yokohama
    old = ship("100000003", 5.0, 108.0, src=SINGAPORE, dest="JP YOK", first_h=24 * 40)  # 40 days ago
    fine = ship("100000004", 5.0, 108.0, src=SINGAPORE, dest="JP YOK", first_h=24 * 5)
    assert arrived(berthed, WORLD_PORTS) and arrived(at_dest, WORLD_PORTS) and not arrived(fine, WORLD_PORTS)
    assert voyage_mmsis([berthed, at_dest, old, fine], SG, WORLD_PORTS, NOW) == ["100000004"]


def test_the_list_is_capped_at_what_one_subscription_may_name():
    left = {"src": SINGAPORE, "dest": "JP YOK"}
    many = [ship(f"2{i:08d}", 5.0, 108.0, age_h=1 + i / 100, **left) for i in range(250)]
    assert len(voyage_mmsis(many, SG, WORLD_PORTS, NOW)) == MAX_FOLLOWED
    assert len(voyage_mmsis(many, SG, WORLD_PORTS, NOW, limit=10)) == 10


def test_the_subscription_names_the_ships_and_the_whole_world():
    sub = subscription("k", WORLD, ["111111111", "222222222"])
    assert sub["BoundingBoxes"] == [[[-90.0, -180.0], [90.0, 180.0]]]
    assert sub["FiltersShipMMSI"] == ["111111111", "222222222"]
    assert "FiltersShipMMSI" not in subscription("k", WORLD)  # the regional feed names no ships
    assert len(subscription("k", WORLD, [f"{i:09d}" for i in range(500)])["FiltersShipMMSI"]) == MAX_FOLLOWED


def test_a_changed_list_is_sent_on_the_open_connection(monkeypatch):
    import ingest.aisstream as ais

    monkeypatch.setattr(ais, "CHECK_EVERY_S", 0)
    received: list[dict] = []
    wanted = [["111111111"]]

    async def handler(ws):
        received.append(json.loads(await ws.recv()))
        for mmsi in (419000001, 419000002, 419000003):
            await ws.send(json.dumps(aisstream_position(mmsi=mmsi)))
            wanted[0] = ["111111111", "222222222"]  # the list grew while connected
            await asyncio.sleep(0.05)
        with contextlib.suppress(TimeoutError):
            received.append(json.loads(await asyncio.wait_for(ws.recv(), 1)))

    async def scenario():
        server = await serve(handler, "127.0.0.1", 0)
        url = f"ws://127.0.0.1:{server.sockets[0].getsockname()[1]}"
        await run(url, "k", WORLD, FakeSender(), stop_after=3, follow=lambda: wanted[0])
        await asyncio.sleep(0.2)
        server.close()

    asyncio.run(scenario())
    assert received[0]["FiltersShipMMSI"] == ["111111111"]
    assert any(m.get("FiltersShipMMSI") == ["111111111", "222222222"] for m in received[1:])


# ---- what a server pays: the shared ship list, compression, settings from the environment ----------------


def _locked_app(tmp_path, monkeypatch, cache_s):
    from fastapi.testclient import TestClient

    from common.config import settings
    from common.geo import load_ports
    from dashboard.alerts import AlertFeed
    from dashboard.app import create_app
    from tests.fakes import FakeHBase

    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("MONITOR_COUNTRY", "SG")
    monkeypatch.setenv("MONITOR_PORTS", "")
    monkeypatch.setenv("SHIPS_CACHE_S", str(cache_s))
    monkeypatch.setattr("common.monitor.coverage_cells", lambda: None)
    settings.cache_clear()
    h = FakeHBase()
    checks = {"kafka": lambda: "ok", "hbase": lambda: "ok", "warehouse": lambda: "ok"}
    app = create_app(h, load_ports(), checks, AlertFeed(), world=WORLD_PORTS, scoped=True)  # type: ignore[arg-type]
    return h, TestClient(app)


def _put_ship(h, mmsi, lat=1.28, lon=103.95):
    from storage.hbase.tables import VESSEL_STATE

    now = str(int(time.time() * 1000))
    cells = {"last:ts": now, "last:lat": str(lat), "last:lon": str(lon), "last:state": "underway",
             "last:sog": "5", "info:name": f"SHIP {mmsi}"}  # fmt: skip
    h.put(VESSEL_STATE, mmsi, cells, 1)


def test_one_scan_is_shared_between_requests_for_SHIPS_CACHE_S(tmp_path, monkeypatch):
    from common.config import settings

    try:
        h, client = _locked_app(tmp_path, monkeypatch, cache_s=30)
        _put_ship(h, "100000001")
        assert {s["mmsi"] for s in client.get("/api/ships").json()} == {"100000001"}
        _put_ship(h, "100000002")  # arrives after the scan: the next 30 s of requests share the old answer
        assert {s["mmsi"] for s in client.get("/api/ships").json()} == {"100000001"}
    finally:
        settings.cache_clear()


def test_a_pinned_ship_stays_on_the_map_outside_the_monitoring(tmp_path, monkeypatch):
    from common.config import settings

    try:
        h, client = _locked_app(tmp_path, monkeypatch, cache_s=0)
        _put_ship(h, "100000001", lat=40.0, lon=-30.0)  # mid-Atlantic: no role for a Singapore monitoring
        assert client.get("/api/ships").json() == []
        assert [s["map"] for s in client.get("/api/ships?pinned=100000001").json()] == ["pinned"]
    finally:
        settings.cache_clear()


def test_the_cache_can_be_switched_off_and_a_response_is_compressed(tmp_path, monkeypatch):
    from common.config import settings

    try:
        h, client = _locked_app(tmp_path, monkeypatch, cache_s=0)
        for i in range(40):
            _put_ship(h, f"2000000{i:02d}")
        first = client.get("/api/ships", headers={"Accept-Encoding": "gzip"})
        assert first.headers.get("content-encoding") == "gzip" and len(first.json()) == 40
        _put_ship(h, "300000001")
        assert len(client.get("/api/ships").json()) == 41  # no cache: the new ship is there at once
    finally:
        settings.cache_clear()


def test_what_the_job_costs_comes_from_the_environment(monkeypatch):
    from common.config import settings

    monkeypatch.setenv("SPARK_MASTER", "local[2]")
    monkeypatch.setenv("SPARK_DRIVER_MEMORY", "1g")
    monkeypatch.setenv("SPARK_SHUFFLE_PARTITIONS", "4")
    monkeypatch.setenv("SPARK_UI", "0")
    monkeypatch.setenv("STREAM_TRIGGER_S", "20")
    monkeypatch.setenv("HISTORY_EVERY_S", "1800")
    monkeypatch.setenv("FOLLOW_MAX", "50")
    settings.cache_clear()
    try:
        s = settings()
        assert (s.spark_master, s.spark_driver_memory, s.spark_shuffle_partitions) == ("local[2]", "1g", 4)
        assert (s.spark_ui, s.stream_trigger_s, s.history_every_s, s.follow_max) == (False, 20, 1800.0, 50)
    finally:
        settings.cache_clear()


def test_the_alert_stream_sends_its_headers_at_once_and_is_not_compressed(tmp_path, monkeypatch):
    """Behind the gzip middleware the headers of the idle stream came only with the first keepalive."""
    import threading

    import uvicorn

    from common.config import settings

    try:
        _, client = _locked_app(tmp_path, monkeypatch, cache_s=0)
        import socket

        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()
        server = uvicorn.Server(uvicorn.Config(client.app, host="127.0.0.1", port=port, log_level="error"))
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        for _ in range(50):
            if server.started:
                break
            time.sleep(0.1)
        conn = socket.create_connection(("127.0.0.1", port), timeout=3)
        conn.sendall(b"GET /api/alerts/stream HTTP/1.1\r\nHost: x\r\nAccept-Encoding: gzip\r\n\r\n")
        head = conn.recv(400).decode("latin-1").lower()  # within 3 s: the idle stream must already answer
        conn.close()
        server.should_exit = True
        assert "200 ok" in head and "text/event-stream" in head and "content-encoding" not in head
    finally:
        settings.cache_clear()
