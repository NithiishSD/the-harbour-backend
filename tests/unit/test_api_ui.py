import pytest
from fastapi.testclient import TestClient

from common.geo import load_ports
from dashboard.alerts import AlertFeed
from dashboard.app import create_app
from storage.hbase.tables import VESSEL_STATE
from tests.fakes import FakeHBase

T0 = 1_672_617_600_000


def ship(
    h: FakeHBase,
    mmsi: str,
    lat: float,
    lon: float,
    state: str,
    area: str,
    sog: float | None,
    course: bool = False,
) -> None:
    cells = {
        "last:ts": str(T0),
        "last:lat": str(lat),
        "last:lon": str(lon),
        "last:area": area,
        "last:state": state,
        "info:name": "SAMPLE",
    }
    if sog is not None:
        cells["last:sog"] = str(sog)
    if course:
        cells |= {"last:cog": "91.5", "last:heading": "90.0"}
    h.put(VESSEL_STATE, mmsi, cells, T0)


@pytest.fixture
def api() -> TestClient:
    h = FakeHBase()
    ship(h, "367000001", 33.70, -118.20, "underway", "USLGB", 10.0, course=True)  # Long Beach area, moving
    ship(h, "367000002", 33.75, -118.26, "berthed", "USLAX", 0.0)
    ship(h, "367000003", 33.60, -118.10, "underway", "", 12.0)  # open water, near LA/LB
    ship(h, "367000004", 33.60, -118.10, "departed", "", 12.0)
    ship(h, "367000005", 0.0, 0.0, "underway", "", 9.0)  # nowhere near a configured port
    ship(h, "367000006", 33.70, -118.20, "anchored", "USLGB", None)
    checks = {"kafka": lambda: "ok", "hbase": lambda: "ok", "warehouse": lambda: "ok"}
    return TestClient(create_app(h, load_ports(), checks, AlertFeed()))  # type: ignore[arg-type]


def test_ships_include_speed_course_and_heading(api):
    by_mmsi = {s["mmsi"]: s for s in api.get("/api/ships").json()}
    moving, quiet = by_mmsi["367000001"], by_mmsi["367000006"]
    assert (moving["sog"], moving["cog"], moving["heading"]) == (10.0, 91.5, 90)
    assert (quiet["sog"], quiet["cog"], quiet["heading"]) == (None, None, None)


def test_route_to_port_area_has_distance_and_eta(api):
    r = api.get("/api/ships/367000001/route").json()
    assert r["destination"]["port_id"] == "USLGB" and r["destination"]["basis"] == "port area"
    assert r["path"][0] == [33.70, -118.20] and r["path"][-1] == [33.754, -118.216]
    assert len(r["path"]) > 2  # along the lanes, not straight over the breakwater
    assert 3.4 < r["distance_nm"] < 10  # longer than the 3.4 nm straight line
    assert r["eta_hours"] == pytest.approx(r["distance_nm"] / 10.0, abs=0.01)


def test_route_open_water_uses_nearest_port(api):
    r = api.get("/api/ships/367000003/route").json()
    assert r["destination"]["basis"] == "nearest port" and r["destination"]["port_id"] in {"USLAX", "USLGB"}


def test_route_without_eta_or_destination(api):
    berthed = api.get("/api/ships/367000002/route").json()
    assert berthed["destination"]["port_id"] == "USLAX" and berthed["eta_hours"] is None
    assert api.get("/api/ships/367000006/route").json()["eta_hours"] is None  # no speed known
    for mmsi in ("367000004", "367000005"):  # departed / too far from every port
        r = api.get(f"/api/ships/{mmsi}/route").json()
        assert r["destination"] is None and r["path"] == [] and r["eta_hours"] is None


def test_route_errors(api):
    assert api.get("/api/ships/367999999/route").json()["error"]["code"] == "NOT_FOUND"
    assert api.get("/api/ships/12/route").json()["error"]["code"] == "INVALID_MMSI"


def test_zones_are_geojson(api):
    geo = api.get("/api/zones").json()
    assert geo["type"] == "FeatureCollection" and len(geo["features"]) >= 10
    f = geo["features"][0]
    assert f["geometry"]["type"] == "Polygon" and {"port_id", "kind", "name"} <= set(f["properties"])


def test_page_and_assets_are_served(api):
    page = api.get("/")
    assert (
        page.status_code == 200
        and "text/html" in page.headers["content-type"]
        and "HarbourWatch" in page.text
    )
    assert "/static/app.js" in page.text
    for asset in ("app.js", "app.css"):
        assert api.get(f"/static/{asset}").status_code == 200
    assert api.get("/static/nope.js").json()["error"]["code"] == "NOT_FOUND"


def test_a_source_that_is_no_port_is_named_after_the_nearest_port_or_left_to_its_coordinates():
    from dashboard.live import source_of

    ports = load_ports()
    near = source_of(33.0, -118.2, T0, None, ports)  # beyond the "at the port" radius
    assert near["port_id"] is None and near["place"].startswith("near ") and near["lat"] == 33.0
    far = source_of(0.0, -140.0, T0, None, ports)  # open Pacific
    assert far["port_id"] is None and far["place"] is None and (far["lat"], far["lon"]) == (0.0, -140.0)
