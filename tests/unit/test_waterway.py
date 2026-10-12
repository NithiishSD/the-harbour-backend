from dashboard.waterway import Lanes, build, default, length_km

CELL = 0.01
# A U-shaped channel round a block of land: water in rows 0 and 4 (columns 0..9), joined by column 9.
WATER = {(i, j) for i in (0, 4) for j in range(10)} | {(i, 9) for i in range(5)}


def lanes(ships: int = 5) -> Lanes:
    return Lanes(dict.fromkeys(WATER, ships), CELL)


def at(i: float, j: float) -> tuple[float, float]:
    return (i + 0.5) * CELL, (j + 0.5) * CELL


def test_route_goes_round_the_land_and_never_over_it():
    L = lanes()
    path = L.route(*at(0, 0), *at(4, 0))
    assert path[0] == [round(x, 5) for x in at(0, 0)] and path[-1] == [round(x, 5) for x in at(4, 0)]
    assert len(path) > 2  # not the straight line over rows 1..3
    for a, b in zip(path, path[1:], strict=False):
        assert L.clear(tuple(a), tuple(b)), (a, b)
    assert any(abs(lon - at(0, 9)[1]) < CELL for _, lon in path)  # it turned at the far end of the U


def test_straight_line_when_the_water_is_open():
    path = lanes().route(*at(0, 1), *at(0, 8))
    assert len(path) == 2
    assert length_km(path) > 0


def test_straight_line_when_the_destination_is_off_the_lanes():
    far = (50.0, 50.0)
    assert lanes().route(*at(0, 0), *far) == [list(at(0, 0)), list(far)]
    assert Lanes({}).route(1.0, 2.0, 3.0, 4.0) == [[1.0, 2.0], [3.0, 4.0]]


def test_a_ship_off_the_lanes_joins_the_nearest_one():
    L = lanes()
    path = L.route(-0.02, 0.005, *at(4, 0))  # two cells south of the channel
    assert path[0] == [-0.02, 0.005] and len(path) > 2


def test_busier_cells_are_preferred():
    # Two ways from A to B, same length: the busy one wins.
    quiet = {(0, j): 1 for j in range(5)} | {(1, 0): 1, (1, 4): 1}
    busy = {(2, j): 50 for j in range(5)} | {(1, 0): 1, (1, 4): 1}
    L = Lanes(quiet | busy, CELL)
    cells = L.search((1, 0), (1, 4))
    assert cells is not None and all(i != 0 for i, _ in cells)


def test_build_joins_reports_and_drops_noise(tmp_path):
    rows = ["MMSI,BaseDateTime,LAT,LON,Length"]
    for mmsi in ("1", "2"):  # two ships along the same line, reports 3 cells apart a minute apart
        rows += [f"{mmsi},2023-01-01T00:0{k}:00,0.005,{0.005 + 0.03 * k},200" for k in range(4)]
    rows.append("3,2023-01-01T00:00:00,0.5,0.5,200")  # one ship alone, far away: not a lane
    rows.append("4,2023-01-01T00:00:00,0.005,0.005,10")  # a small boat: ignored
    csv = tmp_path / "ais.csv"
    csv.write_text("\n".join(rows) + "\n")
    doc = build([csv], CELL)
    cells = {(i, j): n for i, j, n in doc["cells"]}  # type: ignore[attr-defined]
    assert set(cells) == {(0, j) for j in range(10)}  # the gaps between reports are filled in
    assert set(cells.values()) == {2}


def test_the_shipped_lane_map_routes_into_both_harbours():
    L = default()
    assert len(L.ships) > 1000
    for port in ((33.729, -118.262), (33.754, -118.216)):  # Los Angeles, Long Beach
        path = L.route(33.62, -118.20, *port)
        assert len(path) > 2
        assert all(L.clear(tuple(a), tuple(b)) for a, b in zip(path[1:-2], path[2:-1], strict=False))
