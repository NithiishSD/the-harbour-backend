import json

from common import geo
from common.geo import ZoneIndex, circle_ring, haversine_km, load_ports, load_zones, point_in_ring


def square(lon0, lat0, lon1, lat1):
    return ((lon0, lat0), (lon1, lat0), (lon1, lat1), (lon0, lat1), (lon0, lat0))


def zone(kind, ring, port="USLAX", name="z"):
    xs, ys = [p[0] for p in ring], [p[1] for p in ring]
    return geo.Zone(port, kind, name, ring, (min(xs), min(ys), max(xs), max(ys)))


def test_point_in_square_and_outside():
    ring = square(0, 0, 10, 10)
    assert point_in_ring(5, 5, ring)
    assert not point_in_ring(15, 5, ring)
    assert not point_in_ring(5, -1, ring)


def test_concave_polygon():
    # U shape: the notch (5, 8) is outside even though it is inside the bounding box.
    ring = ((0, 0), (10, 0), (10, 10), (7, 10), (7, 3), (3, 3), (3, 10), (0, 10), (0, 0))
    assert point_in_ring(1, 8, ring)
    assert not point_in_ring(5, 8, ring)


def test_lookup_prefers_berth_over_anchorage_over_area():
    idx = ZoneIndex(
        [
            zone("area", square(-119, 33, -118, 34)),
            zone("anchorage", square(-118.6, 33.2, -118.4, 33.4), name="anch"),
            zone("berth", square(-118.55, 33.25, -118.5, 33.3), name="berth"),
        ]
    )
    assert idx.lookup(33.27, -118.52).kind == "berth"  # lat, lon order
    assert idx.lookup(33.35, -118.45).kind == "anchorage"
    assert idx.lookup(33.8, -118.9).kind == "area"
    assert idx.lookup(10, 10) is None
    assert idx.area_of(33.27, -118.52) == "USLAX"


def test_repo_geofences_are_valid_and_cover_ports():
    ports = load_ports()
    zones = load_zones()
    assert {"SGSIN", "USLAX", "USLGB"} <= set(ports)
    for port_id in ports:
        kinds = {z.kind for z in zones if z.port_id == port_id}
        assert kinds == {"area", "anchorage", "berth"}, port_id
    idx = ZoneIndex(zones)
    for z in zones:
        assert z.ring[0] == z.ring[-1], f"{z.name} ring not closed"
        if z.kind != "area":
            # every anchorage/berth must sit inside its own port's area (checked just inside each
            # corner: corners on a shared port boundary are ambiguous by definition)
            cx = sum(p[0] for p in z.ring[:-1]) / (len(z.ring) - 1)
            cy = sum(p[1] for p in z.ring[:-1]) / (len(z.ring) - 1)
            for lon, lat in z.ring[:-1]:
                inner_lon, inner_lat = cx + 0.95 * (lon - cx), cy + 0.95 * (lat - cy)
                assert idx.area_of(inner_lat, inner_lon) == z.port_id, z.name


def test_learned_anchorages_are_loaded(tmp_path, monkeypatch):
    learned = tmp_path / "geo" / "learned_anchorages.geojson"
    learned.parent.mkdir()
    learned.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"port_id": "USLAX", "kind": "anchorage", "name": "learned-0"},
                        "geometry": {"type": "Polygon", "coordinates": [circle_ring(33.0, -118.0, 1.0)]},
                    }
                ],
            }
        )
    )
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    geo.settings.cache_clear()
    try:
        names = {z.name for z in load_zones()}
    finally:
        geo.settings.cache_clear()
    assert "learned-0" in names


def test_circle_ring_radius():
    ring = circle_ring(33.7, -118.2, 2.0)
    assert ring[0] == ring[-1]
    for lon, lat in ring:
        assert abs(haversine_km(33.7, -118.2, lat, lon) - 2.0) < 0.05
