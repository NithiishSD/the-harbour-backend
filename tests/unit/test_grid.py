import json

import pytest
from fastapi.testclient import TestClient

from common.geo import load_ports, load_world_ports
from dashboard.alerts import AlertFeed
from dashboard.app import create_app
from dashboard.weather import GridFeed
from storage.hbase.tables import VESSEL_STATE
from tests.fakes import FakeHBase

T0 = 1_672_617_600_000  # 2023-01-02 00:00 UTC
HOUR = 3_600_000


def raw(point_id="SEA-00", lat=20.0, lon=130.0, hour="2023-01-02T00:00:00Z", wind=20.0, wave=2.0):
    rec = {
        "point_id": point_id,
        "lat": lat,
        "lon": lon,
        "hour": hour,
        "wind_speed_kn": wind,
        "wave_height_m": wave,
    }
    return json.dumps(rec).encode()


def test_bad_records_are_refused():
    feed = GridFeed()
    bad = [
        None,
        b"not json",
        b"[]",
        raw(point_id=None),
        raw(lat=91),
        raw(lon=181),
        raw(lat="x"),
        raw(hour="soon"),
        raw(wind=None, wave=None),
    ]
    assert not any(feed.add_raw(b) for b in bad)
    assert feed.at(T0) == []


def test_conditions_and_the_hour_lookup():
    feed = GridFeed()
    for point, wind in (("A", 5.0), ("B", 25.0), ("C", 40.0)):
        assert feed.add_raw(raw(point, wind=wind, wave=None))
    assert feed.add_raw(raw("A", hour="2023-01-02T01:00:00Z", wind=5.0))
    assert {p["point_id"]: p["condition"] for p in feed.at(T0 + 600_000)} == {
        "A": "calm",
        "B": "rough",
        "C": "storm",
    }
    assert [p["point_id"] for p in feed.at(T0 + HOUR + 1)] == ["A"]  # the next hour holds only A
    assert [p["hour_ms"] for p in feed.at(T0 - 1)] == [T0] * 3  # the hour after, while it is the newest
    assert feed.at(T0 + 5 * HOUR) == []


def test_newest_record_wins_and_only_the_strongest_neighbour_is_a_cyclone():
    feed = GridFeed()
    feed.add_raw(raw("A", wind=10.0))
    feed.add_raw(raw("A", wind=60.0))
    feed.add_raw(raw("B", lat=22.0, lon=132.0, wind=70.0))  # stronger and within 10 degrees of A
    feed.add_raw(raw("C", lat=45.0, lon=-178.0, wind=50.0))  # far away
    feed.add_raw(raw("D", lat=45.0, lon=175.0, wind=49.0))  # 7 degrees from C across the antimeridian
    by_id = {p["point_id"]: p for p in feed.at(T0)}
    assert by_id["A"]["wind_speed_kn"] == 60.0
    assert {k: v["cyclone"] for k, v in by_id.items()} == {"A": False, "B": True, "C": True, "D": False}


@pytest.fixture
def api() -> TestClient:
    h = FakeHBase()
    h.put(
        VESSEL_STATE,
        "367000001",
        {"last:ts": str(T0), "last:lat": "33.7", "last:lon": "-118.2", "last:state": "underway"},
        T0,
    )
    grid = GridFeed()
    grid.add_raw(raw("SEA-07", wind=60.0))
    checks = {"kafka": lambda: "ok", "hbase": lambda: "ok", "warehouse": lambda: "ok"}
    return TestClient(create_app(h, load_ports(), checks, AlertFeed(), grid=grid))  # type: ignore[arg-type]


def test_api_serves_the_grid_for_the_feed_hour(api):
    points = api.get("/api/weather/grid").json()
    assert [(p["point_id"], p["condition"], p["cyclone"]) for p in points] == [("SEA-07", "storm", True)]


def test_api_grid_is_empty_without_ships():
    checks = {"kafka": lambda: "ok", "hbase": lambda: "ok", "warehouse": lambda: "ok"}
    client = TestClient(create_app(FakeHBase(), load_ports(), checks, AlertFeed(), grid=GridFeed()))  # type: ignore[arg-type]
    assert client.get("/api/weather/grid").json() == []


def test_api_serves_the_sea_lanes_and_the_ports_they_join():
    checks = {"kafka": lambda: "ok", "hbase": lambda: "ok", "warehouse": lambda: "ok"}
    app = create_app(FakeHBase(), load_ports(), checks, AlertFeed(), world=load_world_ports())  # type: ignore[arg-type]
    body = TestClient(app).get("/api/sea-lanes", params={"country": "SG", "partners": "JP,US"}).json()
    assert 15 < len(body["lanes"]) < len(__import__("common.lanes", fromlist=["EDGES"]).EDGES) / 2
    assert all(len(lane) == 2 and -180 <= lon <= 180 for lane in body["lanes"] for _, lon in lane)
    ports = {p["port_id"]: p for p in body["ports"]}
    assert ports["SGSIN"]["watched"] and not ports["JPYOK"]["watched"] and "USSEA" in ports
    assert "NLRTM" not in ports and "ZADUR" not in ports  # nobody on the list goes there


def test_open_ocean_route_follows_the_sea_route_then_the_lanes():
    store = FakeHBase()
    cells = {
        "last:ts": str(T0),
        "last:lat": "23.5",
        "last:lon": "126.2",
        "last:state": "underway",
        "last:sog": "16",
    }
    store.put(VESSEL_STATE, "538070001", cells | {"info:dest": "LOS ANGELES"}, T0)
    checks = {"kafka": lambda: "ok", "hbase": lambda: "ok", "warehouse": lambda: "ok"}
    client = TestClient(create_app(store, load_ports(), checks, AlertFeed(), grid=GridFeed()))  # type: ignore[arg-type]
    route = client.get("/api/ships/538070001/route").json()
    assert route["destination"]["port_id"] == "USLAX" and route["destination"]["basis"] == "declared"
    path = route["path"]
    assert path[0] == [23.5, 126.2] and [28.0, 134.0] in path and [45.0, -165.0] in path
    assert path[-1] == [33.729, -118.262]  # ends at the port
    assert 5600 < route["distance_nm"] < 6000


def test_route_to_a_port_we_do_not_watch_runs_all_the_way_and_names_the_source():
    from common.geo import ZoneIndex, load_zones
    from dashboard import live
    from storage.hbase.tables import VESSEL_TRACK, track_key

    store = FakeHBase()
    cells = {
        "last:ts": str(T0),
        "last:lat": "23.5",
        "last:lon": "126.2",
        "last:state": "underway",
        "last:sog": "16",
    }
    store.put(VESSEL_STATE, "538070001", cells | {"info:dest": "SEATTLE"}, T0)
    for hours, (lat, lon) in enumerate(
        [(1.264, 103.84), (12.0, 113.5), (23.5, 126.2)]
    ):  # Singapore, then at sea
        store.put(
            VESSEL_TRACK,
            track_key("538070001", T0 - (2 - hours) * 86_400_000),
            {"pos:lat": str(lat), "pos:lon": str(lon)},
            1,
        )
    world, watched = load_world_ports(), load_ports()
    found = live.route(store, "538070001", world, ZoneIndex(load_zones()), watched)  # type: ignore[arg-type]
    assert found["destination"] == {
        "port_id": "USSEA", "name": "Seattle", "lat": 47.6, "lon": -122.34, "basis": "declared",
    }  # fmt: skip
    assert found["path"][0] == [23.5, 126.2] and found["path"][-1] == [
        47.6,
        -122.34,
    ]  # all the way, over the lanes
    assert [45.0, -165.0] in found["path"] and 5000 < found["distance_nm"] < 6000
    assert found["origin"]["port_id"] == "SGSIN" and found["origin"]["basis"] == "port area"
    assert found["origin"]["at"] == "2022-12-31T00:00:00.000Z"  # its oldest stored report
