"""Write crafted positions and ship types as the clean history the batch jobs read."""

from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from streaming.job import POSITION_FIELDS, POSITION_SCHEMA, STATIC_SCHEMA


def write_history(
    spark: SparkSession, data_dir: Path, positions: list[dict[str, Any]], types: dict[str, str], ts: int
) -> None:
    rows = [tuple(p[f] for f in POSITION_FIELDS) for p in positions]
    vessels = [
        (mmsi, ts, None, ship_type, None, None, None, None, None, None) for mmsi, ship_type in types.items()
    ]
    for name, df in [
        ("positions", spark.createDataFrame(rows, POSITION_SCHEMA)),
        ("vessels", spark.createDataFrame(vessels, STATIC_SCHEMA)),
    ]:
        (
            df.withColumn("date", F.to_date(F.timestamp_millis("ts")))
            .write.partitionBy("date")
            .parquet(str(data_dir / "history" / name))
        )
