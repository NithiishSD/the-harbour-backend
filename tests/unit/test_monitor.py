import pytest
from fastapi.testclient import TestClient

from common.geo import load_ports, load_world_ports
from common.monitor import Monitor
from dashboard.alerts import AlertFeed
from dashboard.app import create_app
from storage.hbase.tables import VESSEL_STATE
from tests.fakes import FakeHBase

WORLD = load_world_ports()
PARTNERS = ("CN", "HK", "TW", "KR", "JP", "MY", "ID", "TH", "VN", "US")  # the tests' own list
SG = Monitor.build(WORLD, "SG", partners=PARTNERS)
SINGAPORE = (1.26, 103.84)
SHANGHAI = (31.23, 121.47)
ROTTERDAM = (51.95, 4.1)


@pytest.fixture(autouse=True)
def own_data_dir(tmp_path, monkeypatch):
    """The tests do not read the developer's saved choice (data/monitor_selection.json)."""
    from common.config import settings

    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    settings.cache_clear()
    yield
    settings.cache_clear()


def ship(lat, lon, dest=None, src=None, state="underway"):
    return {
        "lat": lat, "lon": lon, "destination": dest, "state": state,
        "src_lat": src[0] if src else None, "src_lon": src[1] if src else None,
    }  # fmt: skip


def test_build_picks_the_countrys_ports_and_ignores_the_rest():
    assert SG.country == "SG" and SG.ports == ("SGSIN",) and SG.partners == set(PARTNERS)
    nl = Monitor.build(
        WORLD, "NL", ("NLRTM", "USLAX", "NOPE")
    )  # a port of another country and an unknown one
    assert nl.ports == ("NLRTM",)
    assert set(Monitor.build(WORLD, "US").ports) >= {"USLAX", "USLGB", "USSEA", "USNYC"}  # none chosen: all
    assert Monitor.build(WORLD, "SG", partners=("SG", "CN")).partners == {
        "CN"
    }  # the country is not its own partner


@pytest.mark.parametrize(
    ("where", "expected"),
    [
        (ship(*SINGAPORE, src=SINGAPORE, dest="JP YOK", state="berthed"), ["in"]),  # at the quay, leaving
        (ship(5.0, 108.0, src=SINGAPORE, dest="JP YOK"), ["from"]),  # left Singapore for Yokohama
        (ship(35.0, 140.0, src=SINGAPORE, dest="US LAX"), ["from"]),  # and is now off Japan
        (ship(5.0, 108.0, src=SINGAPORE, dest="NL RTM"), ["from"]),  # from Singapore, to anywhere
        (ship(5.0, 108.0, src=SINGAPORE), ["from"]),  # from Singapore, destination not typed
        (ship(1.4, 104.0, dest="USLAX"), ["in"]),  # passing through Singapore's waters
        (ship(1.28, 103.95, src=SINGAPORE, dest="SGSIN"), ["in"]),  # local traffic: no partner country
        (ship(20.0, 119.0, src=SHANGHAI, dest="SGSIN"), ["to"]),  # from a chosen country, to Singapore
        (ship(20.0, 119.0, dest="SINGAPORE"), []),  # bound for Singapore, source not known
        (ship(40.0, 0.0, src=ROTTERDAM, dest="SGSIN"), []),  # to Singapore, from a country not chosen
        (ship(20.0, 119.0, src=SHANGHAI, dest="USLAX"), []),  # from a chosen country, to somewhere else
        (ship(35.0, 140.0, dest="USLAX"), []),  # nothing to do with Singapore
        (ship(1.26, 103.84, src=SHANGHAI, dest="SGSIN", state="berthed"), ["in"]),  # arrived: in the waters
    ],
)
def test_roles(where, expected):
    assert SG.roles(where, WORLD) == expected


def test_a_ship_can_be_from_and_in_a_country_with_several_ports():
    us = Monitor.build(WORLD, "US", ("USLAX", "USSEA"), PARTNERS)
    assert us.roles(ship(33.7, -118.2, dest="JPYOK"), WORLD) == ["in"]  # Long Beach waters, not a chosen port
    assert us.roles(ship(40.0, 170.0, src=(47.6, -122.34), dest="JPYOK"), WORLD) == ["from"]  # left Seattle
    assert us.roles(ship(40.0, 170.0, src=(40.7, -74.0), dest="USSEA"), WORLD) == []  # New York: domestic
    assert us.roles(ship(30.0, 140.0, src=(35.45, 139.65), dest="USLAX"), WORLD) == ["to"]  # from Yokohama
    assert us.roles(ship(30.0, 140.0, src=(35.45, 139.65), dest="USSEA"), WORLD) == ["to"]  # to a chosen port


def test_ais_boxes_cover_the_country_and_the_lanes_to_the_partners_only():
    def covered(boxes, lat, lon):
        return any(s <= lat <= n and w <= lon <= e for s, w, n, e in boxes)

    boxes = SG.ais_boxes(WORLD)
    assert 5 < len(boxes) < 80
    assert covered(boxes, *SINGAPORE) and covered(boxes, 33.7, -118.3)  # Singapore and Los Angeles
    assert covered(boxes, 22.3, 114.2) and covered(boxes, 35.1, 129.0)  # Hong Kong and Busan
    assert not covered(boxes, 51.9, 4.1) and not covered(boxes, -34.0, 18.4)  # Rotterdam, Cape Town
    nl = Monitor.build(WORLD, "NL", partners=("GB", "DE")).ais_boxes(WORLD)
    assert covered(nl, 51.95, 4.1) and covered(nl, 53.5, 10.0) and not covered(nl, *SINGAPORE)


def api() -> TestClient:
    h = FakeHBase()
    base = {"last:ts": "1672617600000", "last:state": "underway", "last:sog": "12"}
    h.put(
        VESSEL_STATE, "367000001", base | {"last:lat": "1.30", "last:lon": "103.95", "info:dest": "USLAX"}, 1
    )
    left_singapore = {"last:lat": "5.0", "last:lon": "108.0", "src:lat": "1.26", "src:lon": "103.84"}
    h.put(VESSEL_STATE, "367000002", base | left_singapore | {"info:dest": "JP YOK"}, 1)
    h.put(
        VESSEL_STATE, "367000003", base | {"last:lat": "35.0", "last:lon": "140.0", "info:dest": "USLAX"}, 1
    )
    checks = {"kafka": lambda: "ok", "hbase": lambda: "ok", "warehouse": lambda: "ok"}
    return TestClient(create_app(h, load_ports(), checks, AlertFeed(), world=WORLD))  # type: ignore[arg-type]


def test_api_tracked_ships_and_roles():
    client = api()
    everyone = client.get("/api/ships").json()
    assert {s["mmsi"] for s in everyone} == {
        "367000001",
        "367000002",
        "367000003",
    }  # nothing hidden by default
    mine = client.get("/api/ships", params={"tracked": "true", "country": "SG", "partners": "JP"}).json()
    assert {s["mmsi"]: s["roles"] for s in mine} == {"367000001": ["in"], "367000002": ["from"]}
    japan = client.get("/api/ships", params={"tracked": "true", "country": "JP", "ports": "JPYOK"}).json()
    assert {s["mmsi"]: s["roles"] for s in japan} == {"367000003": ["in"]}  # Singapore is not a chosen source
    chosen = {"tracked": "true", "country": "JP", "ports": "JPYOK", "partners": "SG"}
    japan = client.get("/api/ships", params=chosen).json()
    assert {s["mmsi"]: s["roles"] for s in japan} == {"367000002": ["to"], "367000003": ["in"]}
    nobody = {"tracked": "true", "country": "JP", "ports": "JPYOK", "partners": ""}
    assert {s["mmsi"] for s in client.get("/api/ships", params=nobody).json()} == {"367000003"}


def test_api_refuses_an_unknown_country_or_ports_of_another_one():
    client = api()
    bad = ({"country": "ZZ"}, {"country": "JP", "ports": "SGSIN"}, {"country": "SG", "ports": "NOPE"})
    for params in bad:
        for path in ("/api/monitor", "/api/ships", "/api/sea-lanes"):
            r = client.get(path, params=params | ({"tracked": "true"} if path == "/api/ships" else {}))
            assert r.status_code == 422 and r.json()["error"]["code"] == "INVALID_PARAM"


def test_api_monitor_options_and_chosen_ports(monkeypatch):
    monkeypatch.setattr("common.monitor.coverage_cells", lambda: None)  # the estimate, not a survey on disk
    monkeypatch.setattr("dashboard.app.coverage_cells", lambda: None)
    client = api()
    cfg = client.get("/api/monitor").json()
    assert cfg["country"] == "SG" and cfg["ports"] == ["SGSIN"] and "CN" in cfg["partners"]
    by_country = {c["code"]: c["ports"] for c in cfg["countries"]}
    assert {"port_id": "NLRTM", "name": "Rotterdam", "watched": False} in by_country["NL"]
    assert len(by_country) > 20
    ports = client.get("/api/ports", params={"ports": "NLRTM,JPYOK"}).json()
    assert [p["port_id"] for p in ports] == ["NLRTM", "JPYOK"] and ports[0]["waiting_now"] == 0


def test_flag_does_not_decide_both_ends_of_the_voyage_do():
    # a Singapore-flagged ship far away and going elsewhere is nothing to do with this monitoring
    far = {"mmsi": "563012345", "lat": 35.0, "lon": 140.0, "destination": "USLAX", "state": "underway"}
    assert SG.roles(far, WORLD) == []
    inbound = {"lat": 20.0, "lon": 119.0, "destination": "SGSIN", "state": "underway"}
    from_shanghai = {"src_lat": SHANGHAI[0], "src_lon": SHANGHAI[1]}
    for mmsi in ("412000001", "636000001", "999000001"):  # tracked whatever flag it flies
        assert SG.roles({"mmsi": mmsi, **inbound, **from_shanghai}, WORLD) == ["to"]
    assert SG.roles({"mmsi": "412000001", **inbound}, WORLD) == []  # a Chinese flag is not a source
    only_japan = Monitor.build(WORLD, "SG", partners=("JP",))
    assert only_japan.roles({"mmsi": "412000001", **inbound, **from_shanghai}, WORLD) == []  # not chosen


def test_validate_refuses_what_build_would_quietly_change():
    Monitor.validate(WORLD, "SG", ("SGSIN",))
    Monitor.validate(WORLD, "US")
    with pytest.raises(ValueError, match="unknown country"):
        Monitor.validate(WORLD, "ZZ")
    with pytest.raises(ValueError, match="not of US"):
        Monitor.validate(WORLD, "US", ("USLAX", "SGSIN"))


def test_what_is_monitored_comes_from_the_environment_not_the_code(monkeypatch):
    from common import monitor
    from common.config import settings

    monkeypatch.setenv("MONITOR_COUNTRY", "NL")
    monkeypatch.setenv("MONITOR_PORTS", "NLRTM")
    monkeypatch.setenv("MONITOR_PARTNERS", "GB,DE")
    monkeypatch.setenv("MONITOR_MAX_PARTNERS", "3")
    monkeypatch.setenv("MONITOR_SOURCE_PORT_KM", "5")
    monkeypatch.setenv("MONITOR_WATERS", "51.0,3.0,52.0,4.0; 53.0,4.0,54.0,5.0")
    settings.cache_clear()
    try:
        nl = Monitor.from_settings(WORLD)
        assert (nl.country, nl.ports, nl.partners) == ("NL", ("NLRTM",), {"GB", "DE"})
        assert settings().monitor_max_partners == 3
        # the waters are the two boxes of the environment, not the csv and not circles round the ports
        assert nl.in_waters(51.5, 3.5, WORLD) and nl.in_waters(53.5, 4.5, WORLD)
        assert not nl.in_waters(51.95, 4.1, WORLD)  # Rotterdam is outside both boxes: they are the list
        assert monitor.waters_of("NL") and monitor.waters_of("SG") is not None  # csv for the other countries
        # a first report 20 km from a port is "at" it only when the environment allows that far
        assert nl.source_port(51.95 + 0.18, 4.1, WORLD) is None
        monkeypatch.setenv("MONITOR_SOURCE_PORT_KM", "60")
        settings.cache_clear()
        assert nl.source_port(51.95 + 0.18, 4.1, WORLD) is not None
    finally:
        settings.cache_clear()


def test_the_listening_area_is_widened_by_the_environment(monkeypatch):
    from common.config import settings

    def covered(boxes, lat, lon):
        return any(s <= lat <= n and w <= lon <= e for s, w, n, e in boxes)

    monkeypatch.setenv("MONITOR_AIS_PAD", "0")
    monkeypatch.setenv("MONITOR_AIS_REGION_KM", "100")
    monkeypatch.setenv("MONITOR_AIS_SOURCE_KM", "50")
    settings.cache_clear()
    try:
        narrow = Monitor.build(WORLD, "SG", partners=("CN", "JP")).ais_boxes(WORLD)
        monkeypatch.setenv("MONITOR_AIS_PAD", "3")
        monkeypatch.setenv("MONITOR_AIS_REGION_KM", "1200")
        monkeypatch.setenv("MONITOR_AIS_SOURCE_KM", "800")
        settings.cache_clear()
        wide = Monitor.build(WORLD, "SG", partners=("CN", "JP")).ais_boxes(WORLD)
    finally:
        settings.cache_clear()
    # Bangkok is off the Singapore-China lane and 1,400 km from Singapore: only the wide area hears it
    assert not covered(narrow, 13.75, 100.5) and covered(wide, 13.75, 100.5)
    assert covered(wide, *SINGAPORE) and sum(1 for _ in wide) >= sum(1 for _ in narrow)


def test_the_waters_are_the_territorial_sea_and_eez_not_the_port_box():
    assert SG.in_waters(1.28, 103.95, WORLD)  # off the east coast of Singapore
    assert not SG.in_waters(1.3, 104.3, WORLD)  # Pedra Branca side: outside the delimited zone
    assert not SG.in_waters(1.6, 103.8, WORLD)  # Johor Strait mouth, Malaysian water
    drawn = SG.waters(WORLD)
    assert {z["kind"] for z in drawn} == {"territorial", "eez"} and all(len(z["ring"]) > 100 for z in drawn)
    # a country with no zone file entry falls back to the boxes or circles, as before
    nl = Monitor.build(WORLD, "NL", ("NLRTM",))
    assert nl.waters(WORLD) and "ring" not in nl.waters(WORLD)[0]


def test_a_ship_that_started_at_a_monitored_port_is_tracked_whatever_it_says_it_is_bound_for():
    out = ship(10.0, 110.0, src=SINGAPORE)  # no destination typed, far out at sea
    assert SG.roles(out, WORLD) == ["from"]
    back = ship(10.0, 110.0, src=SINGAPORE, dest="SGSIN")  # started here, coming back: not a voyage from here
    assert SG.roles(back, WORLD) == ["from"]  # at sea, outside the waters: it is on its way
    inside = ship(1.28, 103.95, src=SINGAPORE, dest="SGSIN")
    assert SG.roles(inside, WORLD) == ["in"]


def test_kind_tells_a_voyage_of_ours_from_another_countrys_crossing_the_waters():
    inside = (1.28, 103.95)
    from_a_listed_country = ship(*inside, src=SHANGHAI, dest="AU SYD")  # CN is one of the countries we follow
    assert SG.kind(from_a_listed_country, WORLD) == "other"  # not a stranger: its voyage touches our list
    stranger = ship(*inside, src=ROTTERDAM, dest="AU SYD")  # Rotterdam to Sydney through our water
    assert SG.kind(stranger, WORLD) == "crossing"
    unknown_source = ship(*inside, dest="AU SYD")  # bound for Sydney, first heard in our water
    assert SG.kind(unknown_source, WORLD) == "crossing"
    no_destination = ship(*inside)  # nothing said: not clearly anyone's voyage
    assert SG.kind(no_destination, WORLD) == "other"
    assert SG.kind(ship(*inside, dest="SGSIN"), WORLD) == "monitored"  # bound for our port
    assert SG.kind(ship(10.0, 110.0, src=SINGAPORE), WORLD) == "monitored"  # left our port
    assert SG.kind(ship(40.0, 0.0), WORLD) is None  # not tracked at all


def test_a_ship_first_heard_passing_through_the_strait_did_not_start_at_the_port():
    # at a berth: a start; in the Strait outside every berth and anchorage: only passing
    assert SG.roles(ship(5.0, 108.0, src=(1.265, 103.84)), WORLD) == ["from"]  # first heard at Tanjong Pagar
    assert SG.roles(ship(5.0, 108.0, src=(1.1, 103.6)), WORLD) == []  # in the approach, not at a berth


def test_times_are_shown_in_the_monitored_countrys_zone(monkeypatch):
    from common.config import settings
    from common.monitor import timezone_of

    assert timezone_of("SG") == "Asia/Singapore" and timezone_of("JP") == "Asia/Tokyo"
    assert timezone_of("ZZ") == "UTC"  # no row: UTC
    monkeypatch.setenv("MONITOR_TIMEZONE", "America/Los_Angeles")
    monkeypatch.setenv("MONITOR_COUNTRY", "US")
    settings.cache_clear()
    try:
        assert timezone_of("US") == "America/Los_Angeles"
        monkeypatch.setenv("MONITOR_TIMEZONE", "Mars/Olympus")
        settings.cache_clear()
        assert timezone_of("US") in ("America/Los_Angeles", "UTC")  # an unknown name is not used
    finally:
        settings.cache_clear()


def test_a_ship_first_heard_in_our_own_waters_has_no_source_of_ours():
    off_the_berths = (1.28, 104.0)  # in the approach: no berth or anchorage, nearest port is Singapore
    assert SG.source_port(*off_the_berths, WORLD).port_id == "SGSIN"
    passing = ship(1.28, 103.95, src=off_the_berths, dest="AU SYD")  # bound for Sydney
    assert SG.kind(passing, WORLD) == "crossing"
    started = ship(1.28, 103.95, src=(1.265, 103.84), dest="AU SYD")  # first heard at a berth: left from here
    assert SG.kind(started, WORLD) == "monitored"
