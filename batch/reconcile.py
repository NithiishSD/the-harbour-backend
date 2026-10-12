"""Verification: do Kafka, cold storage, rejects and HBase agree on how many messages there are?

    python -m batch.reconcile

Checks
  archive    every raw message still in Kafka is also in cold storage
  rejects    every invalid raw message is in data/rejects
  track      every distinct (MMSI, event time) position is a row in HBase vessel_track

Exit code 1 when any check fails. Counts are exact only when the stack is fed by this pipeline alone
(tests that write straight to ais.positions or HBase add rows the raw topic does not know about).
"""

import json
import sys
from pathlib import Path
from typing import Any

import duckdb
from confluent_kafka import Consumer, TopicPartition
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import topics
from common.config import settings
from common.log import get_logger
from common.spark import session
from storage.hbase.client import HBaseClient
from storage.hbase.tables import VESSEL_TRACK
from streaming.job import NORMALIZED_SCHEMA, normalize_batches

log = get_logger("batch.reconcile")


def kafka_available(topic: str) -> int:
    """Messages currently held by Kafka for a topic (end offset minus start offset, all partitions)."""
    consumer = Consumer({"bootstrap.servers": settings().kafka_bootstrap, "group.id": "reconcile"})
    total = 0
    for p in range(topics.PARTITIONS[topic]):
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        total += high - low
    consumer.close()
    return total


def parquet_count(path: Path, where: str = "true") -> int:
    if not any(path.rglob("*.parquet")):
        return 0
    query = f"SELECT count(*) FROM read_parquet(?, hive_partitioning = true) WHERE {where}"  # noqa: S608 - fixed text
    row = duckdb.connect().execute(query, [str(path / "**" / "*.parquet")]).fetchone()
    return int(row[0]) if row else 0


def raw_breakdown(spark: SparkSession) -> dict[str, int]:
    """Re-run normalisation over everything in ais.raw (batch read) and count each outcome."""
    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", settings().kafka_bootstrap)
        .option("subscribe", topics.RAW)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .select("value", F.col("timestamp").alias("kafka_ts"))
    )
    norm = raw.mapInPandas(normalize_batches, NORMALIZED_SCHEMA).cache()
    kinds = {r["kind"]: int(r["count"]) for r in norm.groupBy("kind").count().collect()}
    distinct_positions = norm.filter("kind = 'position'").select("mmsi", "ts").distinct().count()
    norm.unpersist()
    return {
        "raw": raw.count(),
        "position_records": kinds.get("position", 0),
        "static_records": kinds.get("static", 0),
        "rejects": kinds.get("reject", 0),
        "distinct_positions": distinct_positions,
    }


def reconcile(spark: SparkSession, data_dir: Path, client: HBaseClient) -> dict[str, Any]:
    raw_in_kafka = kafka_available(topics.RAW)
    breakdown = raw_breakdown(spark)
    cold_raw = parquet_count(data_dir / "cold", f"topic = '{topics.RAW}'")
    rejects_stored = parquet_count(data_dir / "rejects")
    track_rows = client.count_rows(VESSEL_TRACK, "pos:lat")
    checks = {
        # Cold storage keeps messages after Kafka retention has dropped them, so it may hold more.
        "archive": cold_raw >= raw_in_kafka,
        "rejects": rejects_stored >= breakdown["rejects"],
        "track": track_rows == breakdown["distinct_positions"],
    }
    return {
        "counts": {
            "kafka_raw": raw_in_kafka,
            "cold_raw": cold_raw,
            "rejects_stored": rejects_stored,
            "hbase_vessel_track": track_rows,
            "kafka_positions": kafka_available(topics.POSITIONS),
            **breakdown,
        },
        "checks": checks,
        "ok": all(checks.values()),
    }


def main() -> None:
    spark = session("harbourwatch-reconcile", kafka=True)
    report = reconcile(spark, settings().data_dir, HBaseClient())
    print(json.dumps(report, indent=2))
    sys.exit(0 if report["ok"] else 1)


if __name__ == "__main__":
    main()
