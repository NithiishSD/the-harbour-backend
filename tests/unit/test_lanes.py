import math

import pytest

from common import lanes
from common.geo import haversine_km, load_ports, load_world_ports

PORTS = load_world_ports()


def test_every_port_joins_the_graph_and_every_node_is_reachable():
    assert {p.node for p in PORTS.values()} <= set(lanes.NODES)
    assert len(PORTS) > 45 and set(load_ports()) <= set(PORTS)
    dist, _ = lanes._from_goal("SIN_W")
    assert set(dist) == set(lanes.NODES)  # one connected network
    assert all(a in lanes.NODES and b in lanes.NODES for a, b in lanes.EDGES)


@pytest.mark.parametrize(
    ("start", "end"),
    [("SGSIN", "USLAX"), ("SGSIN", "USSEA"), ("JPYOK", "USLAX"), ("CNSHA", "USOAK"), ("HKHKG", "PABLB"),
     ("KRPUS", "CAVAN"), ("IDJKT", "JPYOK"), ("USHNL", "CNSHA")],
)  # fmt: skip
def test_routes_between_ports_stay_near_the_great_circle(start, end):
    a, b = PORTS[start], PORTS[end]
    path = lanes.route(a.lat, a.lon, b)
    assert path[0] == [a.lat, a.lon] and path[-1] == [b.lat, b.lon]
    ratio = lanes.length_km(path) / haversine_km(a.lat, a.lon, b.lat, b.lon)
    assert 1.0 <= ratio <= 1.25, ratio  # a lane network, not a detour
    assert all(-180 <= lon <= 180 for _, lon in path)


def test_a_ship_mid_ocean_goes_on_along_the_lanes_without_turning_back():
    path = lanes.route(40.5, 170.0, PORTS["USSEA"])  # south of the Aleutians, heading for Seattle
    assert path[1][1] < -100 or path[1][1] > 170  # the first lane node is ahead (east), not Japan
    assert path[-2] == [48.25, -123.2]  # the strait into Puget Sound
    assert lanes.route(40.5, 170.0, PORTS["JPYOK"])[1][0] < 42  # and the other way it goes back to Japan


def test_a_ship_far_from_every_lane_or_a_port_off_the_lanes_has_no_route():
    assert lanes.route(85.0, 100.0, PORTS["USLAX"]) is None  # the Arctic: no lane within 3,000 km
    off = PORTS["USLAX"].__class__("XXXXX", "Nowhere", "XX", 0.0, 0.0, "")
    assert lanes.route(33.0, 142.0, off) is None


def test_corridor_boxes_cover_every_lane_and_port_and_are_valid_for_aisstream():
    boxes = lanes.corridor_boxes()
    assert 10 < len(boxes) < 150
    assert all(-90 <= s < n <= 90 and -180 <= w < e <= 180 for s, w, n, e in boxes)

    def covered(lat: float, lon: float) -> bool:
        return any(s <= lat <= n and w <= lon <= e for s, w, n, e in boxes)

    assert all(covered(lat, lanes.norm_lon(lon)) for lat, lon in lanes.NODES.values())
    assert all(covered(p.lat, p.lon) for p in PORTS.values())
    assert covered(44.0, 180.0) and covered(44.0, -179.9)  # both sides of the date line


def test_lanes_for_the_map_and_weather_points():
    drawn = lanes.lanes()
    assert len(drawn) == len(lanes.EDGES) and math.isclose(drawn[0][0][0], lanes.NODES["SIN_W"][0])
    ids = [i for i, _, _ in lanes.weather_points()]
    assert len(set(ids)) == len(ids) == len(lanes.NODES)
