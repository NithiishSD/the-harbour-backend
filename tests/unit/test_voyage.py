from common.geo import load_world_ports
from dashboard.live import voyage_origin
from dashboard.voyage import departure

WORLD = load_world_ports()
H = 3_600_000
SINGAPORE = (1.264, 103.84)
SHANGHAI = (31.23, 121.47)
AT_SEA = (20.0, 119.0)


def points(*steps):
    """(hours, (lat, lon)) -> (ms, lat, lon)"""
    return [(int(h * H), *spot) for h, spot in steps]


def test_a_ship_that_stood_at_a_port_and_left_began_its_voyage_there():
    track = points((0, SINGAPORE), (1, SINGAPORE), (6, (3.0, 105.0)), (30, AT_SEA))
    port, i = departure(track, WORLD.values())  # type: ignore[misc]
    assert (port.port_id, i) == ("SGSIN", 1)
    origin = voyage_origin(track, None, WORLD)
    assert origin and origin["port_id"] == "SGSIN" and origin["known"] and origin["voyage_from"] == H


def test_the_last_port_it_left_is_the_source_not_the_first():
    track = points(
        (0, SHANGHAI), (1, SHANGHAI), (40, AT_SEA), (70, SINGAPORE), (71, SINGAPORE), (90, (3.0, 105.0))
    )
    port, _ = departure(track, WORLD.values())  # type: ignore[misc]
    assert port.port_id == "SGSIN"


def test_a_ship_still_at_the_port_has_not_left_it():
    track = points((0, AT_SEA), (30, SINGAPORE), (31, SINGAPORE))
    assert departure(track, WORLD.values()) is None


def test_a_ship_first_heard_at_sea_has_no_known_source():
    track = points((0, AT_SEA), (5, (21.0, 121.0)), (10, (22.5, 124.0)))
    assert departure(track, WORLD.values()) is None
    origin = voyage_origin(track, None, WORLD)
    assert origin and origin["port_id"] is None and origin["known"] is False  # only where it was first heard


def test_sailing_past_a_port_is_not_a_stop():
    track = points((0, (1.0, 102.0)), (1, (1.26, 103.84)), (2, (1.0, 106.0)))  # 20 kn through the strait
    assert departure(track, WORLD.values()) is None
