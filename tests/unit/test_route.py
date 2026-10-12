import pytest

from common.route import LENGTH_NM, WAYPOINTS, along, lattice, norm_lon


def test_route_runs_from_singapore_to_los_angeles():
    assert pytest.approx(7672, abs=60) == LENGTH_NM
    assert along(0)[:2] == (1.26, 103.84)
    lat, lon, _ = along(LENGTH_NM)
    assert (lat, lon) == (33.55, -118.45)
    assert along(LENGTH_NM * 2) == along(LENGTH_NM)  # clamped to the route


def test_ships_cross_the_antimeridian_with_longitudes_in_range():
    lons = [along(nm)[1] for nm in range(0, int(LENGTH_NM), 50)]
    assert all(-180 <= lon <= 180 for lon in lons)
    assert any(lon > 170 for lon in lons) and any(lon < -170 for lon in lons)
    assert norm_lon(195) == -165 and norm_lon(104) == 104


def test_heading_points_along_the_leg():
    assert 30 < along(500)[2] < 60  # north-east out of the South China Sea
    assert 80 < along(LENGTH_NM * 0.75)[2] < 140  # east/south-east on the last stretch


def test_lattice_has_unique_ids_and_covers_the_route():
    points = lattice()
    assert len({p[0] for p in points}) == len(points) == 3 * (len(WAYPOINTS) - 2)
    assert all(-90 <= lat <= 90 and -180 <= lon <= 180 for _, lat, lon in points)
