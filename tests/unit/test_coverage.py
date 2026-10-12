import json
from pathlib import Path

import pytest

from common import coverage, lanes
from common.geo import load_world_ports
from common.monitor import recommend_partners
from ingest.coverage_survey import cell_key, tally, write

WORLD = load_world_ports()


def cells_along(nodes: list[str], size: float = 2.0, skip: range | None = None) -> coverage.Cells:
    """A survey that heard a ship in every square the lane route crosses (except the squares listed)."""
    pts = [lanes.NODES[n] for n in nodes]
    counts: dict[tuple[int, int], int] = {}
    for i, (lat, lon) in enumerate(coverage._densify(pts)):
        if skip and i in skip:
            continue
        counts[coverage.Cells(size, {}).of(lat, lon)] = 3
    return coverage.Cells(size, counts)


def test_a_route_heard_all_the_way_is_fully_covered():
    nodes = lanes.chain("SIN_W", "HK")
    c = coverage.route_coverage(nodes, cells_along(nodes))
    assert c.covered == 1.0 and c.longest_gap_km == 0 and c.basis == "measured"


def test_a_stretch_nobody_was_heard_in_is_the_gap():
    nodes = lanes.chain("SIN_E", "SHA")
    full = cells_along(nodes)
    hole = range(40, 110)  # about 1,750 km without a report
    # take the squares of the hole out of the survey, and keep a ship in the ones the route enters afterwards
    cut = cells_along(nodes, skip=hole)
    assert len(cut.counts) < len(full.counts)
    c = coverage.route_coverage(nodes, cut)
    assert 0.4 < c.covered < 0.9 and 500 < c.longest_gap_km < 1750
    assert c.gap_hours == pytest.approx(c.longest_gap_km / 1.852 / 14, rel=0.01)


def test_the_date_line_does_not_make_a_route_longer():
    nodes = lanes.chain("NP2", "NP3")  # 44 N 180 E to 45 N 165 W
    c = coverage.route_coverage(nodes, None)
    assert c.length_km < 1500  # not the 19,000 km round the world the other way


def test_without_a_survey_the_lane_graph_says_which_voyages_hug_coasts():
    rows = {r.country: r for r in coverage.country_coverage(WORLD, "SG", None)}
    assert rows["MY"].covered == 1.0 and rows["CN"].covered == 1.0 and rows["TH"].gap_hours == 0
    assert rows["US"].covered < 0.5 and rows["US"].gap_hours > 100  # the North Pacific is blind
    assert rows["US"].basis == "estimated"
    assert list(rows)[0] in {"CN", "HK", "ID", "KR", "MY", "PH", "TH", "TW", "VN"}  # best first


def test_partners_are_picked_by_coverage_and_never_more_than_ten(monkeypatch):
    monkeypatch.setattr("common.monitor.coverage_cells", lambda: None)  # not the survey of the dev stack
    picked = recommend_partners(WORLD, "SG")
    assert 5 <= len(picked) <= 10 and "SG" not in picked
    assert {"MY", "ID", "TH", "VN", "CN", "HK"} <= set(picked) and "US" not in picked and "PA" not in picked
    assert (
        coverage.country_coverage(WORLD, "SG", None)[0].covered == 1.0
    )  # the estimate: Asian neighbours first
    assert "US" in recommend_partners(WORLD, "CA")  # next door: the coast is followed the whole way
    assert coverage.rank_home_countries(WORLD, None)[0].mean_covered > 0.8


def test_survey_counts_distinct_ships_per_square_and_round_trips(tmp_path: Path):
    seen: dict[str, set[str]] = {}
    msg = lambda lat, lon, mmsi: json.dumps({"MetaData": {"latitude": lat, "longitude": lon, "MMSI": mmsi}})  # noqa: E731
    assert tally(msg(1.26, 103.84, 1), seen, 2.0) and tally(msg(1.3, 103.9, 2), seen, 2.0)
    assert tally(msg(1.27, 103.85, 1), seen, 2.0)  # the same ship again
    assert (
        not tally("not json", seen, 2.0)
        and not tally(msg(0, 0, 3), seen, 2.0)
        and not tally(msg(95, 0, 4), seen, 2.0)
    )
    assert {k: len(v) for k, v in seen.items()} == {cell_key(1.26, 103.84, 2.0): 2}
    doc = write(tmp_path / "cells.json", seen, 2.0, 1.0)
    assert doc["ships"] == 2
    cells = coverage.load_cells(tmp_path / "cells.json")
    assert cells and cells.heard(1.26, 103.84, pad=0) and not cells.heard(40.0, -30.0)
    assert coverage.load_cells(tmp_path / "missing.json") is None
    # a second survey adds to the first
    again: dict[str, set[str]] = {}
    assert tally(msg(40.0, -30.0, 9), again, 2.0)
    merged = write(tmp_path / "cells.json", again, 2.0, 2.0)
    assert merged["minutes"] == 3.0 and set(merged["cells"]) == {
        cell_key(1.26, 103.84, 2.0),
        cell_key(40.0, -30.0, 2.0),
    }


def test_the_reach_outline_is_the_edges_of_heard_squares_that_nothing_heard_borders():
    from common.coverage import Cells, reach_outline

    cells = Cells(2.0, {(0, 0): 5, (0, 1): 3, (3, 3): 1})  # a pair of neighbours and a lone square
    edges = reach_outline(cells)
    assert len(edges) == 6 + 4  # the pair: 6 outer edges (the shared one is inside); the lone square: 4
    shared = [[0.0, -178.0], [2.0, -178.0]]  # the edge the two heard squares share
    assert shared not in edges and [list(reversed(p)) for p in shared] not in edges
    assert reach_outline(Cells(2.0, {})) == []
