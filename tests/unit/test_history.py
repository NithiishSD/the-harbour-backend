import json
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from batch.archive_history import run as archive
from common import scopes
from common.config import settings
from common.geo import ZoneIndex, load_ports, load_zones
from dashboard.alerts import AlertFeed
from dashboard.app import create_app
from dashboard.history import HistoryStore
from tests.fakes import FakeHBase
from tests.samples import aisstream_position, aisstream_static, envelope
from tests.scenarios import LB_ANCHORAGE, LB_BERTH

DAY = "2023-01-01"
H = 3_600_000
MIN = 60_000
T0 = int(datetime(2023, 1, 1, tzinfo=UTC).timestamp() * 1000)
SEA = (33.30, -118.30)  # open water, outside every port area


def at(hours: float) -> int:
    return T0 + int(hours * H)


def pos(mmsi: str, hours: float, spot: tuple[float, float], sog: float, nav: int | None = None) -> dict:
    return {
        "mmsi": mmsi, "ts": at(hours), "lat": spot[0], "lon": spot[1], "sog": sog, "cog": 90.0,
        "heading": 90, "nav_status": nav, "source": "noaa",
    }  # fmt: skip


def write(root: Path, name: str, rows: list[dict], day: str = DAY) -> None:
    folder = root / name / f"date={day}"
    folder.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.register("rows", pd.DataFrame(rows))
    con.execute(f"COPY (SELECT * FROM rows) TO '{folder / 'part-0.parquet'}' (FORMAT parquet)")


@pytest.fixture
def root(tmp_path: Path) -> Path:
    a, b, c, d = "367000001", "367000002", "367000003", "367000004"
    rows = [
        # A: sea, anchors at Long Beach for 2 h, berths, leaves
        pos(a, 6, SEA, 12), pos(a, 7, (33.60, -118.20), 10),
        pos(a, 8, LB_ANCHORAGE, 0.1, 1), pos(a, 9, LB_ANCHORAGE, 0.1, 1), pos(a, 10, LB_ANCHORAGE, 0.1, 1),
        pos(a, 10.5, LB_BERTH, 0.0, 5), pos(a, 11, LB_BERTH, 0.0, 5),
        pos(a, 13, (33.74, -118.19), 8), pos(a, 14, SEA, 14),
        # B: starts the day at the berth: it has not arrived
        pos(b, 0, LB_BERTH, 0.0, 5), pos(b, 1, LB_BERTH, 0.0, 5), pos(b, 2, LB_BERTH, 0.0, 5),
        # C: silent for 3 hours
        pos(c, 0, SEA, 10), pos(c, 0.2, SEA, 10), pos(c, 3.2, SEA, 10),
        # D: stopped at sea for 40 minutes
        pos(d, 1, SEA, 9), pos(d, 1.1, SEA, 0.1), pos(d, 1.5, SEA, 0.1), pos(d, 1.8, SEA, 9),
    ]  # fmt: skip
    write(tmp_path, "positions", rows)
    write(tmp_path, "vessels", [
        {"mmsi": a, "ts": at(6), "name": "ALPHA", "ship_type": "cargo", "length_m": 200.0, "draught_m": None,
         "imo": None, "callsign": None},
        {"mmsi": b, "ts": at(0), "name": "BRAVO", "ship_type": "tug", "length_m": 30.0, "draught_m": None,
         "imo": None, "callsign": None},
    ])  # fmt: skip
    write(tmp_path, "destinations", [{"mmsi": a, "ts": at(6), "destination": "LONG BEACH", "eta": None}])
    return tmp_path


@pytest.fixture
def store(root: Path) -> HistoryStore:
    return HistoryStore(root, ZoneIndex(load_zones()), load_ports())


def test_nothing_in_the_history_folder(tmp_path: Path):
    empty = HistoryStore(tmp_path, ZoneIndex(load_zones()), load_ports())
    assert empty.dates() == [] and empty.snapshot(T0) == [] and empty.journey("367000001") is None


def test_dates_list_the_days_with_data(store: HistoryStore):
    (day,) = store.dates()
    assert day["date"] == DAY and day["ships"] == 4 and day["sources"] == ["noaa"]
    assert day["first_ts"] == at(0) and day["last_ts"] == at(14)


def test_snapshot_is_the_newest_report_of_each_ship_with_its_state(store: HistoryStore):
    ships = {s["mmsi"]: s for s in store.snapshot(at(9.5))}
    assert set(ships) == {"367000001"}  # the others were last heard more than an hour before
    a = ships["367000001"]
    assert (a["name"], a["ship_type"], a["state"], a["port"]) == ("ALPHA", "cargo", "anchored", "USLGB")
    assert a["last_seen"] == "2023-01-01T09:00:00.000Z" and a["destination"] == "LONG BEACH"
    early = {s["mmsi"]: s for s in store.snapshot(at(1.5))}
    assert early["367000002"]["state"] == "berthed" and early["367000004"]["state"] == "underway"
    assert [s["mmsi"] for s in store.snapshot(at(1.5), limit=1)] == ["367000004"]  # newest report first


def test_snapshot_window_decides_who_is_still_on_the_map(store: HistoryStore):
    assert "367000003" in {s["mmsi"] for s in store.snapshot(at(0.5))}
    assert "367000003" not in {s["mmsi"] for s in store.snapshot(at(2.5))}  # silent since 00:12
    assert "367000003" in {s["mmsi"] for s in store.snapshot(at(2.5), window_ms=3 * H)}


def test_journey_runs_the_live_state_machine_and_stops_at_arrival(store: HistoryStore):
    j = store.journey("367000001")
    assert j["name"] == "ALPHA" and j["declared"] == "LONG BEACH"
    assert j["reached"] and j["arrived_at"] == at(10.5) and j["end_ts"] == at(10.5)
    assert j["origin"]["port_id"] == "USLAX" and j["origin"]["basis"] == "near port"  # 48 km off Los Angeles
    assert j["destination"]["port_id"] == "USLGB" and j["destination"]["basis"] == "declared"
    kinds = [e["type"] for e in j["events"]]
    assert kinds == ["first_seen", "anchored", "berthed", "waited", "arrived"]
    waited = next(e for e in j["events"] if e["type"] == "waited")
    assert "Waited 2.5 h" in waited["text"]
    assert j["track"][0][0] == at(6) and j["track"][-1][0] == at(10.5)


def test_journey_can_continue_past_the_arrival_and_stop_at_a_time(store: HistoryStore):
    full = store.journey("367000001", stop_at_arrival=False)
    assert full["reached"] and full["arrived_at"] == at(10.5) and full["end_ts"] == at(14)  # data goes on
    assert "arrived" in [e["type"] for e in full["events"]]
    assert "departed" in [e["type"] for e in full["events"]]
    early = store.journey("367000001", until_ms=at(9))
    assert early["end_ts"] == at(9) and not early["reached"]
    assert early["events"][-1]["type"] == "anchored"


def test_a_ship_that_starts_at_a_berth_has_not_arrived(store: HistoryStore):
    j = store.journey("367000002")
    assert not j["reached"] and j["end_ts"] == at(2)
    assert j["origin"]["port_id"] == "USLGB"


def test_gaps_and_stops_are_events(store: HistoryStore):
    lost = store.journey("367000003")["events"]
    assert [e["text"] for e in lost if e["type"] == "signal_lost"] == ["Signal lost for 3.0 h"]
    stopped = store.journey("367000004")["events"]
    assert [e["type"] for e in stopped if e["type"] == "stopped"] == ["stopped"]
    assert "0.7 h" in next(e for e in stopped if e["type"] == "stopped")["text"]


def test_an_unknown_ship_has_no_journey(store: HistoryStore):
    assert store.journey("999999999") is None


def test_track_is_thinned_but_keeps_both_ends(tmp_path: Path):
    rows = [pos("367000009", i / 60, SEA, 0.1) for i in range(0, 600)]  # one report a minute, not moving
    write(tmp_path, "positions", rows)
    j = HistoryStore(tmp_path, ZoneIndex(load_zones()), load_ports()).journey("367000009")
    assert len(j["track"]) < 70 and j["track"][0][0] == at(0) and j["track"][-1][0] == at(599 / 60)


@pytest.fixture
def api(store: HistoryStore) -> TestClient:
    checks = {"kafka": lambda: "ok", "hbase": lambda: "ok", "warehouse": lambda: "ok"}
    return TestClient(create_app(FakeHBase(), load_ports(), checks, AlertFeed(), history=store))  # type: ignore[arg-type]


def test_api_offline_views(api: TestClient):
    assert api.get("/api/history/dates").json()[0]["date"] == DAY
    ships = api.get("/api/history/ships", params={"at": "2023-01-01T09:30:00Z"}).json()
    assert {s["mmsi"] for s in ships} == {"367000001"}
    ports = {p["port_id"]: p for p in api.get("/api/ports", params={"at": "2023-01-01T09:30:00Z"}).json()}
    assert ports["USLGB"]["waiting_now"] == 1 and ports["SGSIN"]["waiting_now"] == 0
    j = api.get("/api/history/ships/367000001/journey", params={"until": "2023-01-01T09:00:00Z"}).json()
    assert j["end_ts"] == at(9)
    full = api.get("/api/history/ships/367000001/journey", params={"full": "true"}).json()
    assert full["end_ts"] == at(14)


def test_api_offline_errors(api: TestClient):
    assert api.get("/api/history/ships").status_code == 422
    bad = api.get("/api/history/ships", params={"at": "yesterday"})
    assert bad.status_code == 422 and bad.json()["error"]["code"] == "INVALID_PARAM"
    assert api.get("/api/history/ships/999999999/journey").json()["error"]["code"] == "UNKNOWN_SHIP"
    assert api.get("/api/history/ships/abc/journey").status_code == 422


def _live(payload: dict, name: str = "LIVE TANKER") -> dict:
    """A message of a real-looking ship: the sample builders name every ship TEST TANKER."""
    payload["MetaData"]["ShipName"] = name
    for body in payload["Message"].values():
        if "Name" in body:
            body["Name"] = name
    return payload


def _cold(tmp_path: Path, values: list[object], extra: list[bytes] | None = None) -> None:
    blobs = [json.dumps(v).encode() for v in values] + (extra or [])
    folder = tmp_path / "cold" / "topic=ais.raw" / "date=2026-10-10"
    folder.mkdir(parents=True)
    con = duckdb.connect()
    con.register("rows", pd.DataFrame({"key": "k", "value": blobs, "kafka_ts": datetime(2026, 10, 10, 6, 1)}))
    con.execute(f"COPY (SELECT key, value, kafka_ts FROM rows) TO '{folder / 'p.parquet'}' (FORMAT parquet)")


def test_archive_history_keeps_aisstream_only_and_is_repeatable(tmp_path: Path):
    ts = int(datetime(2026, 10, 10, 6, 0, tzinfo=UTC).timestamp() * 1000)
    ship = 244850097
    position = _live(aisstream_position(mmsi=ship, ts_ms=ts, lat=20.0, lon=130.0, sog=14.0))
    _cold(
        tmp_path,
        [
            envelope(position),
            envelope(position),  # duplicate
            envelope(_live(aisstream_static(mmsi=ship, ts_ms=ts))),
            {"source": "noaa", "payload": {"MMSI": "367000001", "BaseDateTime": "2023-01-01T00:00:00"}},
        ],
        [b"\xff not json"],
    )
    counts = archive(tmp_path)
    assert (counts["positions"], counts["vessels"], counts["destinations"]) == (1, 1, 1)
    assert counts["history_days_removed"] == [] and counts["cold_days_removed"] == []
    assert archive(tmp_path) == counts  # same result when run again
    store = HistoryStore(tmp_path / "history", ZoneIndex(load_zones()), load_ports())
    (found,) = store.snapshot(ts + MIN)
    assert (found["mmsi"], found["name"], found["destination"], found["sog"]) == (
        str(ship), "LIVE TANKER", "SINGAPORE", 14.0,
    )  # fmt: skip
    assert store.dates()[0]["sources"] == ["aisstream"]


def test_archive_history_leaves_out_demo_ships_probes_and_test_fixtures(tmp_path: Path):
    ts = int(datetime(2026, 10, 10, 6, 0, tzinfo=UTC).timestamp() * 1000)
    old = int(datetime(2025, 1, 6, 6, 0, tzinfo=UTC).timestamp() * 1000)
    _cold(
        tmp_path,
        [
            envelope(_live(aisstream_position(mmsi=244850097, ts_ms=ts))),  # the one real ship
            envelope(aisstream_position(mmsi=244850098, ts_ms=ts)),  # named TEST TANKER by the fixture
            envelope(_live(aisstream_position(mmsi=538070003, ts_ms=ts), "PACIFIC ARROW 1")),  # demo fleet
            envelope(_live(aisstream_position(mmsi=340654001, ts_ms=ts, lat=20.0, lon=-150.0), "MEASURE")),
            envelope(_live(aisstream_position(mmsi=244850099, ts_ms=old))),  # a day of the test suites
        ],
    )
    counts = archive(tmp_path)
    assert counts["positions"] == 1 and counts["test_data_skipped"] == 4
    store = HistoryStore(tmp_path / "history", ZoneIndex(load_zones()), load_ports())
    assert [s["mmsi"] for s in store.snapshot(ts + MIN)] == ["244850097"]


def test_archive_history_merges_so_a_recent_run_keeps_older_messages(tmp_path: Path):
    ts = int(datetime(2026, 10, 10, 6, 0, tzinfo=UTC).timestamp() * 1000)
    first = _live(aisstream_position(mmsi=244850097, ts_ms=ts))
    _cold(tmp_path, [envelope(first)])
    assert archive(tmp_path)["positions"] == 1
    # a later message of the same event day arrives in a new Kafka day; only that day is read again
    later = _live(aisstream_position(mmsi=244850097, ts_ms=ts + 5 * MIN))
    folder = tmp_path / "cold" / "topic=ais.raw" / "date=2026-10-11"
    folder.mkdir(parents=True)
    con = duckdb.connect()
    con.register(
        "rows",
        pd.DataFrame(
            {
                "key": ["k"],
                "value": [json.dumps(envelope(later)).encode()],
                "kafka_ts": [datetime(2026, 10, 11, 0, 1)],
            }
        ),
    )
    con.execute(f"COPY (SELECT key, value, kafka_ts FROM rows) TO '{folder / 'p.parquet'}' (FORMAT parquet)")
    archive(tmp_path, since=date(2026, 10, 11))
    store = HistoryStore(tmp_path / "history", ZoneIndex(load_zones()), load_ports())
    assert store.journey("244850097")["track"][0][0] == ts  # the older report is still there
    assert len(store.journey("244850097")["track"]) == 2


def test_one_ship_is_found_at_any_time_whatever_the_age_of_its_last_report(store: HistoryStore):
    (c,) = store.snapshot(at(8), mmsi="367000003")  # last heard at 03:12, five hours before
    assert c["mmsi"] == "367000003" and c["last_seen"] == "2023-01-01T03:12:00.000Z"
    assert store.snapshot(at(1), mmsi="367000001") == []  # not heard of yet
    assert [s["mmsi"] for s in store.snapshot(at(9.5), mmsi="367000001")] == ["367000001"]


def test_api_route_plan_and_one_ship_snapshot(api: TestClient):
    plan = api.get("/api/route", params={"lat": 23.5, "lon": 126.2, "to": "USLAX"}).json()
    assert plan["path"][0] == [23.5, 126.2] and plan["path"][-1] == [33.729, -118.262]
    assert [28.0, 134.0] in plan["path"] and 5600 < plan["distance_nm"] < 6000  # along the sea route
    assert api.get("/api/route", params={"lat": 1, "lon": 2, "to": "NOWHERE"}).status_code == 404
    assert api.get("/api/route", params={"lat": 99, "lon": 2, "to": "USLAX"}).status_code == 422
    one = api.get("/api/history/ships", params={"at": "2023-01-01T08:00:00Z", "mmsi": "367000003"}).json()
    assert [s["mmsi"] for s in one] == ["367000003"]


def _scoped_api(store: HistoryStore, root: Path, tmp_path: Path, mp: pytest.MonkeyPatch, country: str):
    """The dashboard held to `country`, with the test's day (NOAA, US waters) as its history."""
    from common.geo import load_world_ports

    data = tmp_path / "d"
    data.mkdir(exist_ok=True)
    if not (data / "history").exists():
        (data / "history").symlink_to(root)
    mp.setenv("DATA_DIR", str(data))
    mp.setenv("MONITOR_COUNTRY", country)
    mp.setenv("MONITOR_PORTS", "")
    settings.cache_clear()
    checks = {"kafka": lambda: "ok", "hbase": lambda: "ok", "warehouse": lambda: "ok"}
    app = create_app(FakeHBase(), load_ports(), checks, AlertFeed(), world=load_world_ports(), scoped=True)  # type: ignore[arg-type]
    return data, TestClient(app)


def test_replay_holds_only_the_monitored_countrys_data(
    store: HistoryStore, root: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    when = "2023-01-01T09:30:00Z"
    try:
        # the US: NOAA's day is its own, with or without a ledger
        data, us = _scoped_api(store, root, tmp_path, monkeypatch, "US")
        assert [d["date"] for d in us.get("/api/history/dates").json()] == [DAY]
        assert us.get("/api/history/ships", params={"at": when}).json()  # ships of the day, tracked by role
        # Singapore, redeployed over the same stores: the US day is another country's, nothing of it shows
        sg_box = ((1.0, 103.0, 2.0, 105.0),)
        scopes.record(data, "SG", ("SGSIN",), sg_box, first_data_ms=at(-1), now_ms=at(-1))
        _, sg = _scoped_api(store, root, tmp_path, monkeypatch, "SG")
        assert sg.get("/api/history/dates").json() == []
        refused = sg.get("/api/history/ships", params={"at": "2022-12-31T00:00:00Z"})
        assert refused.status_code == 409 and refused.json()["error"]["code"] == "NOT_MONITORED"
        quiet = sg.get("/api/history/ships", params={"at": when})
        assert quiet.status_code == 200 and quiet.json() == []  # in Singapore's span, but nothing of it kept
        # and the country cannot be changed from the browser
        assert sg.get("/api/monitor", params={"country": "US"}).status_code == 422
        assert sg.get("/api/monitor").json()["country"] == "SG"
    finally:
        settings.cache_clear()
