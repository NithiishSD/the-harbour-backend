"""RDD backfill: NOAA CSV history -> clean, de-duplicated positions and ship details in Parquet.

    python -m batch.backfill_rdd --input data/noaa/csv/AIS_2023_01_01_bbox.csv

Steps (RDD API on purpose; the Spark SQL jobs come next)
  textFile -> drop header -> parse CSV -> normalise + validate -> keep positions (filter)
  -> ((MMSI, event time), position) -> reduceByKey: one row per message, duplicates dropped
  -> (MMSI, 1) -> reduceByKey: messages per ship -> sortBy: busiest ships first
  ship details: ((MMSI, date), details) -> reduceByKey: newest details per ship per day
Actions: count, countByValue (reject reasons), take (busiest ships), saveAsTextFile (messages per ship).

Output
  data/history/positions/date=YYYY-MM-DD/   Parquet; re-running replaces only the dates in the input
  data/history/vessels/date=YYYY-MM-DD/     Parquet, one row per ship per day
  data/batch/ship_counts/<input name>/      text, "mmsi<TAB>messages"
"""

import argparse
import csv
import json
import shutil
from datetime import UTC, datetime
from operator import add
from pathlib import Path
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.config import settings
from common.log import get_logger
from common.schema import Normalized, normalize
from common.spark import session
from streaming.job import POSITION_FIELDS, POSITION_SCHEMA, STATIC_FIELDS, STATIC_SCHEMA

log = get_logger("batch.backfill_rdd")


def parse_row(columns: list[str], values: list[str]) -> Normalized:
    if len(values) != len(columns):
        return Normalized(reject="wrong number of fields")
    return normalize({"source": "noaa", "payload": dict(zip(columns, values, strict=True))})


def day_of(ts_ms: Any) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, UTC).date().isoformat()


def write_by_date(df: DataFrame, path: Path) -> None:
    """Parquet by event date; only the dates present in df are replaced."""
    (
        df.withColumn("date", F.to_date(F.timestamp_millis("ts")))
        .write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("date")
        .parquet(str(path))
    )


def backfill(spark: SparkSession, input_path: str, data_dir: Path, top: int = 10) -> dict[str, Any]:
    sc = spark.sparkContext
    lines = sc.textFile(input_path)
    header = lines.first()
    columns = next(csv.reader([header]))
    parsed = (
        lines.filter(lambda line: line != header)  # every file of a glob starts with the header
        .mapPartitions(csv.reader)
        .map(lambda values: parse_row(columns, values))
        .persist(StorageLevel.MEMORY_AND_DISK)  # three NOAA days do not fit a 2 GB heap
    )
    rejects = parsed.filter(lambda n: n.reject is not None).map(lambda n: n.reject)
    positions = parsed.flatMap(lambda n: [dict(n.position)] if n.position else [])
    unique = (
        positions.map(lambda p: ((p["mmsi"], p["ts"]), p))
        # Same ship + same event time = the same message. min() keeps the choice the same on every run.
        .reduceByKey(lambda a, b: min(a, b, key=str))
        .values()
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    per_ship = unique.map(lambda p: (p["mmsi"], 1)).reduceByKey(add).sortBy(lambda kv: (-kv[1], kv[0]))

    counts_dir = data_dir / "batch" / "ship_counts" / Path(input_path).stem.replace("*", "_")
    shutil.rmtree(counts_dir, ignore_errors=True)  # saveAsTextFile refuses an existing directory
    per_ship.map(lambda kv: f"{kv[0]}\t{kv[1]}").saveAsTextFile(str(counts_dir))

    vessels = (
        parsed.flatMap(lambda n: [dict(n.static)] if n.static else [])
        .map(lambda s: ((s["mmsi"], day_of(s["ts"])), s))
        .reduceByKey(lambda a, b: max(a, b, key=lambda s: (s["ts"], str(s))))
        .values()
    )

    history = data_dir / "history"
    write_by_date(
        spark.createDataFrame(unique.map(lambda p: tuple(p[f] for f in POSITION_FIELDS)), POSITION_SCHEMA),
        history / "positions",
    )
    vessel_rows = vessels.map(lambda s: tuple(s[f] for f in STATIC_FIELDS))
    write_by_date(spark.createDataFrame(vessel_rows, STATIC_SCHEMA), history / "vessels")

    total = parsed.count()
    position_count = positions.count()
    unique_count = unique.count()
    summary = {
        "rows": total,
        "rejects": dict(sorted(rejects.countByValue().items())),
        "positions": position_count,
        "duplicates": position_count - unique_count,
        "unique_positions": unique_count,
        "ships": per_ship.count(),
        "busiest": per_ship.take(top),
        "vessel_days": vessel_rows.count(),
        "history": str(history),
        "ship_counts": str(counts_dir),
    }
    parsed.unpersist()
    unique.unpersist()
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--input", required=True, help="NOAA CSV path or glob")
    parser.add_argument("--top", type=int, default=10, help="busiest ships to list")
    args = parser.parse_args()
    spark = session("harbourwatch-backfill", driver_memory="2g")
    summary = backfill(spark, args.input, settings().data_dir, args.top)
    log.info("backfill finished", extra={"data": {k: v for k, v in summary.items() if k != "busiest"}})
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
