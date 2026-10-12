from storage.hbase.tables import track_key
from streaming.sinks import SRC_TS_BASE, chunks, info_put, state_puts, track_put


def test_track_put_uses_event_time_for_key_and_cell_timestamp():
    p = dict(mmsi="419000123", ts=1000, lat=1.25, lon=103.9, sog=None, cog=10.0, heading=None, nav_status=1)
    key, cells, ts = track_put(p)
    assert key == track_key("419000123", 1000) and ts == 1000
    assert cells == {"pos:lat": "1.25", "pos:lon": "103.9", "pos:cog": "10.0", "pos:nav": "1"}


def test_info_put_skips_unknown_fields():
    key, cells, ts = info_put(
        {"mmsi": "419000123", "ts": 5, "name": "X", "ship_type": "tanker", "length_m": None}
    )
    assert (key, ts) == ("419000123", 5)
    assert cells == {"info:name": "X", "info:type": "tanker"}


def test_chunks():
    assert [len(c) for c in chunks(range(1201), 500)] == [500, 500, 201]
    assert list(chunks([], 500)) == []


def test_the_source_of_a_ship_is_written_once_with_a_timestamp_the_earliest_report_wins():
    def last(first_seen: int, lat: float) -> dict:
        data = {"sog": 9.0, "cog": 90.0, "heading": 90, "state": "underway", "first_seen": first_seen,
                "src_lat": lat, "src_lon": 103.8}  # fmt: skip
        return {
            "type": "last",
            "mmsi": "367000001",
            "ts": first_seen + 5,
            "port_id": None,
            "lat": 5.0,
            "lon": 5.0,
            "data": data,
        }

    rows, _ = state_puts([last(1_000, 1.26)])
    (src,) = [r for r in rows if "src:lat" in r[1]]
    assert src[1] == {"src:lat": "1.26", "src:lon": "103.8", "src:ts": "1000"}
    assert src[2] == SRC_TS_BASE - 1_000
    later = [r for r in state_puts([last(9_000, 40.0)])[0] if "src:lat" in r[1]][0]
    assert (
        later[2] < src[2]
    )  # a later first report has a lower timestamp: HBase keeps showing the earlier one


def test_backfill_gives_ships_without_a_source_their_oldest_position():
    from batch.backfill_sources import run
    from storage.hbase.tables import VESSEL_STATE, VESSEL_TRACK
    from tests.fakes import FakeHBase

    h = FakeHBase()
    h.put(VESSEL_STATE, "367000001", {"last:lat": "33.0", "last:lon": "-118.0"}, 5_000)
    h.put(
        VESSEL_STATE, "367000002", {"last:lat": "1.0", "last:lon": "103.0", "src:lat": "9.0"}, 5_000
    )  # has one
    h.put(VESSEL_STATE, "367000003", {"last:lat": "5.0", "last:lon": "5.0"}, 5_000)  # and no track at all
    for ts, lat in ((9_000, 33.0), (2_000, 1.26), (4_000, 5.0)):  # newest first in the store
        h.put(VESSEL_TRACK, track_key("367000001", ts), {"pos:lat": str(lat), "pos:lon": "103.84"}, ts)
    assert run(h, apply=False) == {"ships": 3, "without_source": 2, "with_track": 1, "written": 0}
    assert run(h, apply=True)["written"] == 1
    row = h.get(VESSEL_STATE, "367000001")
    assert (row["src:lat"], row["src:lon"], row["src:ts"]) == ("1.26", "103.84", "2000")  # the oldest report
    assert run(h, apply=False)["without_source"] == 1  # only the shipless one is left


def test_backfill_ignores_ids_that_are_not_nine_digits_and_rows_from_the_future():
    from batch.backfill_sources import run
    from storage.hbase.tables import VESSEL_STATE, VESSEL_TRACK
    from tests.fakes import FakeHBase

    h = FakeHBase()
    h.put(VESSEL_STATE, "36700000", {"last:lat": "1.0", "last:lon": "1.0"}, 5_000)  # eight digits
    h.put(VESSEL_STATE, "367000001", {"last:lat": "1.0", "last:lon": "1.0"}, 5_000)
    far_future = 5_292_612_000_000  # a ship with a clock set to 2137
    h.put(VESSEL_TRACK, track_key("367000001", far_future), {"pos:lat": "9.0", "pos:lon": "9.0"}, 1)
    h.put(VESSEL_TRACK, track_key("367000001", 4_000), {"pos:lat": "1.26", "pos:lon": "103.84"}, 1)
    out = run(h, apply=True)
    assert out["written"] == 1 and out["without_source"] == 1
    assert h.get(VESSEL_STATE, "367000001")["src:lat"] == "1.26"  # never the skewed row


def test_every_cell_the_state_sink_writes_has_its_column_family():
    from storage.hbase.tables import TABLES, VESSEL_STATE

    data = {"sog": 9.0, "cog": 90.0, "heading": 90, "state": "underway", "first_seen": 1_000,
            "src_lat": 1.26, "src_lon": 103.8, "since": 1_000}  # fmt: skip
    events = [
        {"type": "last", "mmsi": "367000001", "ts": 1_005, "port_id": None, "lat": 5.0, "lon": 5.0,
         "data": data},
        {"type": "state", "mmsi": "367000001", "ts": 1_005, "port_id": "SGSIN", "data": data},
    ]  # fmt: skip
    rows, _ = state_puts(events)
    written = {column.split(":")[0] for _, cells, _ in rows for column in cells}
    # HBase drops a cell whose family is not in the table: the sources went missing that way once
    assert {"src", "last", "cur"} <= written and written <= set(TABLES[VESSEL_STATE])
