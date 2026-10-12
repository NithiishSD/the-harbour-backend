import csv
from collections import defaultdict
from pathlib import Path

from common.geo import Zone, ZoneIndex, load_zones
from common.schema import normalize
from streaming.state import (
    ANCHORED,
    BERTHED,
    DEPARTED,
    HOUR_MS,
    UNDERWAY,
    ShipState,
    apply_positions,
    monitored,
    next_timeout,
    on_timeout,
)

MMSI = "419000123"
H = HOUR_MS


def box(port, kind, lat0, lat1, lon0, lon1):
    ring = ((lon0, lat0), (lon1, lat0), (lon1, lat1), (lon0, lat1), (lon0, lat0))
    return Zone(port, kind, f"{port}-{kind}", ring, (lon0, lat0, lon1, lat1))


# Two neighbouring ports side by side, like Los Angeles and Long Beach.
ZONES = ZoneIndex(
    [
        box("PORTA", "area", 0.0, 1.0, 0.0, 1.0),
        box("PORTA", "anchorage", 0.1, 0.3, 0.1, 0.3),
        box("PORTA", "berth", 0.7, 0.9, 0.7, 0.9),
        box("PORTB", "area", 0.0, 1.0, 1.0, 2.0),
        box("PORTB", "anchorage", 0.1, 0.3, 1.1, 1.3),
    ]
)
OUTSIDE = (5.0, 5.0)
IN_AREA = (0.5, 0.5)
ANCHORAGE_A = (0.2, 0.2)
ANCHORAGE_B = (0.2, 1.2)
BERTH_A = (0.8, 0.8)


def pos(ts, where, sog=0.0, nav=None):
    return {"ts": ts, "lat": where[0], "lon": where[1], "sog": sog, "nav_status": nav}


def run(positions, state=None):
    return apply_positions(state, MMSI, positions, ZONES)


def of(events, kind):
    return [e for e in events if e["type"] == kind]


def test_full_port_call():
    s, events = run(
        [
            pos(0, OUTSIDE, sog=12),
            pos(1 * H, IN_AREA, sog=10),
            pos(2 * H, ANCHORAGE_A, sog=0.1, nav=1),
            pos(20 * H, ANCHORAGE_A, sog=0.2, nav=1),
            pos(21 * H, IN_AREA, sog=6, nav=0),  # heading to the berth: call continues
            pos(22 * H, BERTH_A, sog=0.0, nav=5),
            pos(40 * H, BERTH_A, sog=4.0, nav=0),  # leaves the berth
            pos(41 * H, OUTSIDE, sog=12, nav=0),
        ]
    )
    states = [e["data"]["state"] for e in of(events, "state")]
    assert states == [UNDERWAY, ANCHORED, UNDERWAY, BERTHED, DEPARTED, UNDERWAY]
    calls = of(events, "port_call")
    assert len(calls) == 2  # upsert on berthing, final row on departure
    assert calls[0]["data"]["wait_hours"] == 20.0 and calls[0]["data"]["departed_at"] is None
    final = calls[1]["data"]
    assert calls[1]["port_id"] == "PORTA"
    assert (final["anchored_at"], final["berthed_at"], final["departed_at"]) == (2 * H, 22 * H, 40 * H)
    assert final["wait_hours"] == 20.0 and final["turnaround_hours"] == 38.0
    assert s.state == UNDERWAY and s.call_port is None
    assert of(events, "last")[0]["ts"] == 41 * H


def test_state_survives_across_batches_via_json():
    s, _ = run([pos(0, ANCHORAGE_A, sog=0.1, nav=1)])
    restored = ShipState.from_json(s.to_json())
    _, events = run([pos(5 * H, BERTH_A, nav=5)], restored)
    assert of(events, "port_call")[0]["data"]["wait_hours"] == 5.0


def test_late_and_duplicate_positions_are_ignored():
    s, _ = run([pos(10 * H, ANCHORAGE_A, sog=0.1, nav=1)])
    _, events = run([pos(10 * H, BERTH_A, nav=5), pos(9 * H, BERTH_A, nav=5)], s)
    assert events == []
    assert s.state == ANCHORED


def test_anchored_ship_swinging_outside_polygon_stays_anchored():
    s, events = run([pos(0, ANCHORAGE_A, sog=0.1, nav=1), pos(H, IN_AREA, sog=1.3, nav=0)])
    assert s.state == ANCHORED
    assert len(of(events, "state")) == 1
    run([pos(2 * H, IN_AREA, sog=6.0, nav=0)], s)
    assert s.state == UNDERWAY


def test_tug_work_at_berth_does_not_end_berthed_state():
    s, _ = run([pos(0, BERTH_A, nav=5), pos(H, BERTH_A, sog=1.4, nav=0), pos(2 * H, BERTH_A, sog=0.2, nav=0)])
    assert s.state == BERTHED and s.since == 0
    run([pos(3 * H, BERTH_A, sog=4.0, nav=0)], s)
    assert s.state == DEPARTED


def test_nav_status_at_anchor_wins_anywhere_in_port():
    s, _ = run([pos(0, BERTH_A, sog=0.1, nav=1)])
    assert s.state == ANCHORED


def test_left_without_berthing_closes_call():
    _, events = run(
        [pos(0, ANCHORAGE_A, sog=0.1, nav=1), pos(3 * H, IN_AREA, sog=8), pos(4 * H, OUTSIDE, sog=12)]
    )
    call = of(events, "port_call")[-1]["data"]
    assert call["berthed_at"] is None and call["departed_at"] == 4 * H and call["wait_hours"] is None


def test_wait_follows_ship_to_neighbour_port_berth():
    zones = ZoneIndex([*ZONES.zones, box("PORTB", "berth", 0.7, 0.9, 1.7, 1.9)])
    voyage = [
        pos(0, ANCHORAGE_A, sog=0.1, nav=1),
        pos(6 * H, ANCHORAGE_B, sog=0.1, nav=1),
        pos(9 * H, (0.8, 1.8), nav=5),
    ]
    _, events = apply_positions(None, MMSI, voyage, zones)
    call = of(events, "port_call")[0]
    assert call["port_id"] == "PORTB" and call["data"]["wait_hours"] == 9.0


def test_dark_ship_candidate_and_reappearance():
    timeout = 6 * H
    s, _ = run([pos(0, ANCHORAGE_A, sog=0.1, nav=1)])
    assert monitored(s) and next_timeout(s, timeout) == 6 * H
    events, keep = on_timeout(s, MMSI, timeout)
    assert keep and s.dark
    assert events[0]["type"] == "dark_candidate" and events[0]["port_id"] == "PORTA"
    assert events[0]["data"]["state"] == ANCHORED
    assert on_timeout(s, MMSI, timeout) == ([], False)  # one candidate per silence
    _, events = run([pos(9 * H, ANCHORAGE_A, sog=0.1, nav=1)], s)
    assert of(events, "reappeared")[0]["data"]["silent_hours"] == 9.0
    assert not s.dark


def test_ships_passing_through_are_not_monitored():
    s, _ = run([pos(0, IN_AREA, sog=12)])
    assert not monitored(s)
    assert on_timeout(s, MMSI, 6 * H) == ([], False)
    s, _ = run([pos(0, OUTSIDE, sog=0)])
    assert not monitored(s)


def test_real_noaa_port_calls():
    """The three ships that anchor and then berth on 2023-01-01 at LA/Long Beach give calls with a wait."""
    fixture = Path(__file__).parent.parent / "fixtures" / "noaa_la_sample.csv"
    rows = defaultdict(list)
    with fixture.open() as f:
        for r in csv.DictReader(f):
            n = normalize({"source": "noaa", "payload": r})  # same path as the pipeline
            if n.position:
                rows[r["MMSI"]].append(n.position)
    zones = ZoneIndex(load_zones())
    waits = {}
    for mmsi in ("368126190", "205763000", "369567000"):
        _, events = apply_positions(None, mmsi, rows[mmsi], zones)
        berthed = [e for e in events if e["type"] == "port_call" and e["data"]["berthed_at"]]
        assert berthed, mmsi
        waits[mmsi] = berthed[0]["data"]["wait_hours"]
    assert all(w is not None and w > 0 for w in waits.values()), waits
    # Swinging at anchor and tug work at the berth must not make ships flap between states.
    for mmsi, positions in rows.items():
        _, events = apply_positions(None, mmsi, positions, zones)
        assert len([e for e in events if e["type"] == "state"]) <= 12, mmsi


def test_last_event_carries_speed_course_and_heading_of_the_newest_position():
    older = {**pos(0, OUTSIDE, sog=9), "cog": 80.0, "heading": 79}
    newest = {**pos(60_000, OUTSIDE, sog=11), "cog": 91.5, "heading": 90}
    _, events = run([newest, older])  # arrival order does not matter
    data = of(events, "last")[0]["data"]
    assert data == {
        "sog": 11.0, "cog": 91.5, "heading": 90.0, "state": "underway", "first_seen": 0,
        "src_lat": OUTSIDE[0], "src_lon": OUTSIDE[1],  # the first position heard: the source
    }  # fmt: skip


def test_first_seen_is_kept_across_batches_and_reset_once_the_state_is_forgotten():
    s, _ = run([pos(0, OUTSIDE, sog=9)])
    s, events = apply_positions(s, "367000001", [pos(60_000, OUTSIDE, sog=9)], ZONES)
    assert of(events, "last")[0]["data"]["first_seen"] == 0
    _, events = apply_positions(None, "367000001", [pos(10 * 24 * 3_600_000, OUTSIDE, sog=9)], ZONES)
    assert of(events, "last")[0]["data"]["first_seen"] == 10 * 24 * 3_600_000  # back after being forgotten


def test_last_event_course_is_none_when_missing():
    nan = float("nan")
    for missing in ({}, {"cog": nan, "heading": nan}, {"cog": None, "heading": None}):
        _, events = run([{**pos(0, OUTSIDE, sog=5), **missing}])
        assert (
            of(events, "last")[0]["data"]["cog"] is None and of(events, "last")[0]["data"]["heading"] is None
        )
