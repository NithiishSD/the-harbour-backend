import time

from batch.prune_scope import prune_hbase, shells, stale_ships
from common.geo import load_world_ports
from common.monitor import Monitor
from storage.hbase.tables import PORT_CALLS, PORT_QUEUE, VESSEL_STATE, VESSEL_TRACK, track_key
from tests.fakes import FakeHBase

WORLD = load_world_ports()
SG = Monitor.build(WORLD, "SG", partners=("CN", "JP"))
NOW = 1_790_000_000_000


def ship(h: FakeHBase, mmsi: str, lat: float, lon: float, age_h: float = 0, **extra: str) -> None:
    seen = NOW - int(age_h * 3_600_000)
    cells = {"last:ts": str(seen), "last:lat": str(lat), "last:lon": str(lon), "last:state": "underway"}
    h.put(VESSEL_STATE, mmsi, cells | extra, NOW)
    h.put(VESSEL_TRACK, track_key(mmsi, NOW), {"pos:lat": str(lat), "pos:lon": str(lon)}, NOW)


def store() -> FakeHBase:
    h = FakeHBase()
    ship(h, "563000001", 1.30, 103.9)  # in Singapore's waters: tracked
    ship(h, "412000002", 3.0, 104.5)  # off Malaysia's coast, in the feed's area: kept though not tracked
    ship(h, "367000003", 33.7, -118.2, age_h=48)  # Los Angeles: nothing to do with Singapore, silent, outside
    ship(h, "232000004", 51.9, 4.1, age_h=72)  # Rotterdam: the same
    ship(h, "232000005", 51.9, 4.1, age_h=2)  # outside too, but heard two hours ago: kept for now
    h.put(PORT_QUEUE, "USLGB" + "9" * 19, {"q:ships": "5"}, NOW)
    h.put(PORT_QUEUE, "SGSIN" + "9" * 19, {"q:ships": "2"}, NOW)
    h.put(PORT_CALLS, "USLAX" + "9" * 19 + "367000003", {"call:wait_hours": "3"}, NOW)
    return h


def test_only_untracked_ships_outside_the_feeds_area_go():
    assert stale_ships(store(), SG, WORLD) == {"367000003", "232000004"}


def test_dry_run_counts_and_apply_deletes_only_those():
    h = store()
    counted = prune_hbase(h, SG, WORLD, apply=False)
    assert counted == {"ships": 2, "leftovers": 0, "track_rows": 2, "queue_rows": 1, "call_rows": 1}
    assert len(h.scan_all(VESSEL_STATE)) == 5  # nothing happened
    prune_hbase(h, SG, WORLD, apply=True)
    kept = {"563000001", "412000002", "232000005"}
    assert {k for k, _ in h.scan_all(VESSEL_STATE)} == kept
    assert {k[:9] for k, _ in h.scan_all(VESSEL_TRACK)} == kept
    assert [k[:5] for k, _ in h.scan_all(PORT_QUEUE)] == ["SGSIN"]  # the country's own port stays
    assert h.scan_all(PORT_CALLS) == []
    assert prune_hbase(h, SG, WORLD, apply=False)["ships"] == 0  # again: nothing left to do


def test_a_row_of_only_a_source_is_a_leftover_once_it_is_old():
    far = 9_999_999_999_999  # a source cell is stamped in the far future
    old = {"src:lat": "1.0", "src:lon": "2.0", "src:ts": "1000"}
    h = FakeHBase()
    h.put(VESSEL_STATE, "100000001", old, far)
    h.put(VESSEL_STATE, "100000002", old | {"last:lat": "1"}, 1)
    h.put(VESSEL_STATE, "100000003", old | {"src:ts": str(int(time.time() * 1000))}, far)
    # only the old, source-only row: a ship being added (fresh) or one with a position stays
    assert shells(h, int(time.time() * 1000)) == {"100000001"}
    assert prune_hbase(h, SG, WORLD, apply=True)["leftovers"] == 1
    assert {k for k, _ in h.scan_all(VESSEL_STATE)} == {"100000002", "100000003"}
