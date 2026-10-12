"""Spark SQL: port calls and daily KPIs per port for one date, from the clean position history.

    python -m batch.port_calls_sql --date 2023-01-01

1. Ship states: each ship's positions (the date plus LOOKBACK_DAYS before it, so calls that began earlier are
   complete) run through streaming.state, the rules the live job uses, so nightly and live calls agree.
2. Spark SQL window functions:
   - LAG marks where a ship's state or port changes; a running SUM numbers the segments between changes.
   - ROW_NUMBER keeps the newest version of each call (reported at berthing, again at departure).
3. KPIs per port for the date, cargo ships and tankers only (tugs, ferries and pleasure craft berth many
   times a day and would drown the berth-congestion signal): calls (arrivals that day), median wait (calls
   berthed that day), average turnaround (calls departed that day), max hourly queue (most ships anchored
   in any hour of the day). port_calls keeps every call, with its ship type.
   Only calls whose arrival was seen count: a ship already anchored or berthed at its first position in the
   loaded data arrived earlier, so its arrival, wait and turnaround are unknown (arrival_seen = false).

Output (re-running a date replaces that date)
  data/batch/port_calls/date=D/   calls with an arrival, berthing or departure on D
  data/batch/port_daily/date=D/
"""

import argparse
import shutil
from collections.abc import Callable
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from pyspark import Broadcast
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.config import settings
from common.geo import ZoneIndex, load_zones
from common.log import get_logger
from common.spark import session
from streaming.state import Event, ShipState, call_event, step

log = get_logger("batch.port_calls_sql")

# ponytail: a wait that started more than a week before the date is cut at the start of the lookback.
LOOKBACK_DAYS = 7
CALL_FIELDS = ["arrival_at", "anchored_at", "berthed_at", "departed_at", "wait_hours", "turnaround_hours"]
ROW_SCHEMA = (
    "kind string, mmsi string, ts long, state string, port_id string, arrival_at long, anchored_at long, "
    "berthed_at long, departed_at long, wait_hours double, turnaround_hours double, arrival_seen boolean"
)
ROW_COLUMNS = [c.split()[0] for c in ROW_SCHEMA.split(", ")]
KPI_TYPES = "'cargo', 'tanker'"

SHIP_TYPES = "SELECT mmsi, max_by(ship_type, ts) AS ship_type FROM vessels GROUP BY mmsi"

SEGMENTS = """
WITH marked AS (
  SELECT mmsi, ts, state, port_id,
         CASE WHEN LAG(state) OVER w = state AND LAG(port_id) OVER w <=> port_id THEN 0 ELSE 1 END AS changed
  FROM ship_positions
  WINDOW w AS (PARTITION BY mmsi ORDER BY ts)
),
numbered AS (
  SELECT *, SUM(changed) OVER (PARTITION BY mmsi ORDER BY ts ROWS UNBOUNDED PRECEDING) AS segment
  FROM marked
)
SELECT mmsi, segment, FIRST(state) AS state, FIRST(port_id) AS port_id, MIN(ts) AS start_ts, MAX(ts) AS end_ts
FROM numbered
GROUP BY mmsi, segment
"""

HOURLY_QUEUE = """
SELECT port_id, hour, COUNT(DISTINCT mmsi) AS ships
FROM (
  SELECT port_id, mmsi, explode(sequence(div(start_ts, 3600000), div(end_ts, 3600000))) AS hour
  FROM segments JOIN ship_types USING (mmsi)
  WHERE state = 'anchored' AND ship_type IN ({types})
)
WHERE to_date(timestamp_millis(hour * 3600000)) = :day
GROUP BY port_id, hour
"""

DAY_CALLS = """
SELECT port_id, mmsi, coalesce(ship_type, 'unknown') AS ship_type, arrival_at, anchored_at, berthed_at,
       departed_at, wait_hours, turnaround_hours, arrival_seen
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY mmsi, arrival_at ORDER BY ts DESC) AS version
  FROM ship_calls
) LEFT JOIN ship_types USING (mmsi)
WHERE version = 1
  AND :day IN (to_date(timestamp_millis(arrival_at)), to_date(timestamp_millis(berthed_at)),
               to_date(timestamp_millis(departed_at)))
"""

PORT_DAILY = """
WITH per_port AS (
  SELECT port_id,
         count_if(to_date(timestamp_millis(arrival_at)) = :day) AS calls,
         median(wait_hours) FILTER (WHERE to_date(timestamp_millis(berthed_at)) = :day) AS median_wait_hours,
         avg(turnaround_hours) FILTER (WHERE to_date(timestamp_millis(departed_at)) = :day)
           AS avg_turnaround_hours
  FROM day_calls
  WHERE ship_type IN ({types}) AND arrival_seen
  GROUP BY port_id
),
queue AS (SELECT port_id, max(ships) AS max_hourly_queue FROM hourly_queue GROUP BY port_id)
SELECT coalesce(p.port_id, q.port_id) AS port_id,
       coalesce(p.calls, 0) AS calls,
       p.median_wait_hours,
       p.avg_turnaround_hours,
       coalesce(q.max_hourly_queue, 0) AS max_hourly_queue
FROM per_port p FULL OUTER JOIN queue q ON p.port_id = q.port_id
"""


def make_ship_func(zones: Broadcast[ZoneIndex]) -> Callable[[pd.DataFrame], pd.DataFrame]:
    """applyInPandas body: one ship's positions -> its state at every position, plus its port calls."""

    def func(pdf: pd.DataFrame) -> pd.DataFrame:
        mmsi = str(pdf["mmsi"].iloc[0])
        s = ShipState()
        events: list[Event] = []
        rows: list[dict[str, Any]] = []
        first_ts = int(pdf["ts"].min())
        for pos in pdf.sort_values("ts").to_dict("records"):  # history holds one row per (MMSI, time)
            step(s, mmsi, pos, zones.value, events)
            rows.append(
                {"kind": "position", "mmsi": mmsi, "ts": pos["ts"], "state": s.state, "port_id": s.port}
            )
        if s.call_port is not None:
            events.append(call_event(s, mmsi, s.last_seen))  # still open when the data ends
        for e in events:
            if e["type"] == "port_call":
                data = e["data"]
                rows.append(
                    {"kind": "call", "mmsi": mmsi, "ts": e["ts"], "port_id": e["port_id"]}
                    | {k: data[k] for k in CALL_FIELDS}
                    | {"arrival_seen": data["arrival_at"] != first_ts}
                )
        out = pd.DataFrame(rows, columns=ROW_COLUMNS)
        # Missing values become NaN; Arrow needs None in text/bool columns and nullable ints for longs.
        text = ["state", "port_id", "arrival_seen"]
        out[text] = out[text].astype(object).where(out[text].notna(), None)
        return out.astype({c: "Int64" for c in ["ts", *CALL_FIELDS[:4]]})

    return func


def _write(df: DataFrame, root: Path, day: date) -> None:
    shutil.rmtree(root / f"date={day.isoformat()}", ignore_errors=True)
    df.withColumn("date", F.lit(day)).write.mode("append").partitionBy("date").parquet(str(root))


def port_calls(spark: SparkSession, day: date, data_dir: Path) -> dict[str, Any]:
    def history(name: str) -> DataFrame:
        return (
            spark.read.parquet(str(data_dir / "history" / name))
            .where(F.col("date").between(day - timedelta(days=LOOKBACK_DAYS), day))
            .drop("date")
        )

    history("vessels").createOrReplaceTempView("vessels")
    spark.sql(SHIP_TYPES).createOrReplaceTempView("ship_types")
    zones = spark.sparkContext.broadcast(ZoneIndex(load_zones()))
    rows = history("positions").groupBy("mmsi").applyInPandas(make_ship_func(zones), ROW_SCHEMA).cache()
    rows.where("kind = 'position'").createOrReplaceTempView("ship_positions")
    rows.where("kind = 'call'").createOrReplaceTempView("ship_calls")
    spark.sql(SEGMENTS).createOrReplaceTempView("segments")
    args = {"day": day}
    spark.sql(HOURLY_QUEUE.format(types=KPI_TYPES), args).createOrReplaceTempView("hourly_queue")
    calls = spark.sql(DAY_CALLS, args).cache()
    calls.createOrReplaceTempView("day_calls")
    daily = spark.sql(PORT_DAILY.format(types=KPI_TYPES), args)

    _write(calls, data_dir / "batch" / "port_calls", day)
    _write(daily, data_dir / "batch" / "port_daily", day)
    summary = {
        "date": day.isoformat(),
        "calls": calls.count(),
        "ports": [r.asDict() for r in daily.orderBy("port_id").collect()],
    }
    rows.unpersist()
    calls.unpersist()
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    args = parser.parse_args()
    spark = session("harbourwatch-port-calls", driver_memory="2g")
    summary = port_calls(spark, args.date, settings().data_dir)
    log.info("port calls finished", extra={"data": summary})


if __name__ == "__main__":
    main()
