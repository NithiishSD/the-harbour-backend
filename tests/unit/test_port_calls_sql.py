from datetime import date

from pyspark.sql import functions as F

from batch.port_calls_sql import port_calls
from tests.history import write_history
from tests.scenarios import HOUR, LB_ANCHORAGE, LB_BERTH, MIN, every, position

DAY = date(2023, 1, 2)
D0 = 1_672_617_600_000  # 2023-01-02T00:00Z
OFFSHORE = (33.50, -118.00)  # inside the NOAA area, outside every port area


def approach(mmsi, ts):
    """Seen at sea before the call, so the arrival itself is observed."""
    return [position(mmsi, ts - 30 * MIN, OFFSHORE, 12.0, 0)]


def voyages():
    a, b, c, tug, early = "367000001", "367000002", "367000003", "367000004", "367000005"
    out = []
    for mmsi, first in [(a, D0 + 2 * HOUR), (b, D0 + HOUR), (c, D0 - 4 * HOUR), (tug, D0 + 8 * HOUR)]:
        out += approach(mmsi, first)
    # A: anchors 02:00, berths 06:00 (waits 4 h), leaves the berth at 14:00 (turnaround 12 h).
    out += every(a, D0 + 2 * HOUR, D0 + 6 * HOUR, LB_ANCHORAGE, 0.1, 1)
    out += every(a, D0 + 6 * HOUR, D0 + 14 * HOUR, LB_BERTH, 0.0, 5)
    out += every(a, D0 + 14 * HOUR, D0 + 14 * HOUR + 30 * MIN, LB_BERTH, 8.0, 0)
    # B: anchors 01:00 and is still waiting at the end of the day (open call).
    out += every(b, D0 + 1 * HOUR, D0 + 23 * HOUR, LB_ANCHORAGE, 0.1, 1)
    # C: anchored since 20:00 the day before, berths 04:00 (waits 8 h), leaves 10:00 (turnaround 14 h).
    out += every(c, D0 - 4 * HOUR, D0 + 4 * HOUR, LB_ANCHORAGE, 0.1, 1)
    out += every(c, D0 + 4 * HOUR, D0 + 10 * HOUR, LB_BERTH, 0.0, 5)
    out += every(c, D0 + 10 * HOUR, D0 + 10 * HOUR + 30 * MIN, LB_BERTH, 8.0, 0)
    # A tug berths for an hour: a call in port_calls, but not part of the cargo/tanker KPIs.
    out += every(tug, D0 + 8 * HOUR, D0 + 9 * HOUR, LB_BERTH, 0.0, 5)
    out += every(tug, D0 + 9 * HOUR, D0 + 9 * HOUR + 30 * MIN, LB_BERTH, 8.0, 0)
    # Already berthed when the data starts: arrival unknown, so it stays out of the KPIs.
    out += every(early, D0, D0 + 5 * HOUR, LB_BERTH, 0.0, 5)
    out += every(early, D0 + 5 * HOUR, D0 + 5 * HOUR + 30 * MIN, LB_BERTH, 8.0, 0)
    return a, b, c, tug, early, out


def test_port_calls_and_daily_kpis(spark, tmp_path):
    a, b, c, tug, early, positions = voyages()
    types = {a: "cargo", b: "tanker", c: "cargo", tug: "tug", early: "cargo"}
    write_history(spark, tmp_path, positions, types, D0)

    summary = port_calls(spark, DAY, tmp_path)
    assert summary["ports"] == [
        {
            "port_id": "USLGB",
            "calls": 2,  # A and B arrived on the day; C arrived the day before
            "median_wait_hours": 6.0,  # A 4 h and C 8 h berthed on the day
            "avg_turnaround_hours": 13.0,  # A 12 h and C 14 h departed on the day
            "max_hourly_queue": 3,  # 02:00-04:00: A, B and C at anchor
        }
    ]
    out = spark.read.parquet(str(tmp_path / "batch" / "port_calls"))
    calls = {r.mmsi: r for r in out.where(F.col("date") == DAY).collect()}
    assert set(calls) == {a, b, c, tug, early}
    assert not calls[early].arrival_seen and calls[a].arrival_seen and calls[c].arrival_seen
    assert calls[tug].ship_type == "tug" and calls[tug].turnaround_hours == 1.0
    assert calls[a].wait_hours == 4.0 and calls[a].departed_at == D0 + 14 * HOUR
    assert calls[b].berthed_at is None and calls[b].anchored_at == D0 + HOUR
    assert calls[c].anchored_at == D0 - 4 * HOUR and calls[c].turnaround_hours == 14.0

    # Re-running the date replaces its output instead of adding to it.
    port_calls(spark, DAY, tmp_path)
    assert spark.read.parquet(str(tmp_path / "batch" / "port_calls")).count() == 5
    assert spark.read.parquet(str(tmp_path / "batch" / "port_daily")).count() == 1
