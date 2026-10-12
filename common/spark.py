"""SparkSession factory shared by the streaming job and the batch jobs."""

import os
import sys

from pyspark.sql import SparkSession

from common.config import settings

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"


def session(
    app: str, master: str | None = None, kafka: bool = False, driver_memory: str = "1g"
) -> SparkSession:
    # Workers must run the same interpreter as the driver (pandas UDFs pickle functions between them).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.appName(app)
        .master(master or settings().spark_master)
        .config("spark.driver.memory", driver_memory)
        # 200 shuffle partitions is the cluster default; on one machine that is pure overhead.
        .config("spark.sql.shuffle.partitions", str(settings().spark_shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
        .config("spark.ui.showConsoleProgress", "false")
        # the driver keeps the UI's history of jobs and queries in memory: a short one is enough
        .config("spark.ui.retainedJobs", "50")
        .config("spark.ui.retainedStages", "50")
        .config("spark.sql.ui.retainedExecutions", "20")
        .config("spark.sql.streaming.ui.retainedProgressUpdates", "20")
        .config("spark.sql.streaming.minBatchesToRetain", "20")
    )
    if not settings().spark_ui:
        builder = builder.config("spark.ui.enabled", "false")
    if kafka:
        builder = builder.config("spark.jars.packages", KAFKA_PACKAGE)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
