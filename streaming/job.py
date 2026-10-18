"""The streaming job: one Spark application running several checkpointed queries.

    python -m streaming.job                          # all queries, run forever
    python -m streaming.job --queries normalize,archive --available-now

Queries
  normalize  ais.raw -> parse, validate, de-duplicate -> ais.positions / ais.static
  rejects    ais.raw -> invalid messages with their reason -> data/rejects (Parquet)
  dlq        ais.raw -> invalid messages -> ais.dlq (Kafka Dead-Letter Queue)
  archive    ais.raw + weather.marine -> data/cold (Parquet by topic and date)
  track      ais.positions -> HBase vessel_track
  static     ais.static -> HBase vessel_state info family (latest details per ship)
  state      ais.positions -> per-ship state machine -> vessel_state, port_calls, port.events, alerts
  queue      ais.positions -> ships at anchor per port, 30 min window every 5 min -> port_queue, alerts
"""

import argparse
import json
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
from pyspark import Broadcast
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

from common import topics
from common.config import settings
from common.geo import ZoneIndex, load_zones
from common.kafka import JsonProducer
from common.log import get_logger
from common.schema import Normalized, normalize
from common.spark import session
from storage.hbase.client import HBaseClient
from streaming import sinks
from streaming.state import ANCHORED, UNDERWAY, ShipState, apply_positions, classify, next_timeout, on_timeout

log = get_logger("streaming.job")

WATERMARK = "10 minutes"
MAX_OFFSETS_PER_TRIGGER = 50_000

NORMALIZED_SCHEMA = (
    "kind string, mmsi string, ts long, lat double, lon double, sog double, cog double, heading int, "
    "nav_status int, source string, name string, ship_type string, length_m double, draught_m double, "
    "imo string, callsign string, destination string, eta long, reason string, raw string, kafka_ts timestamp"
)
NORMALIZED_COLUMNS = [c.split()[0] for c in NORMALIZED_SCHEMA.split(", ")]
STRING_COLUMNS = [c.split()[0] for c in NORMALIZED_SCHEMA.split(", ") if c.split()[1] == "string"]
POSITION_SCHEMA = (
    "mmsi string, ts long, lat double, lon double, sog double, cog double, "
    "heading int, nav_status int, source string"
)
STATIC_SCHEMA = (
    "mmsi string, ts long, name string, ship_type string, length_m double, draught_m double, "
    "imo string, callsign string, destination string, eta long"
)
POSITION_FIELDS = ["mmsi", "ts", "lat", "lon", "sog", "cog", "heading", "nav_status", "source"]
STATIC_FIELDS = [
    "mmsi", "ts", "name", "ship_type", "length_m", "draught_m", "imo", "callsign", "destination", "eta"
]  # fmt: skip
RAW_KEEP_CHARS = 4000


@dataclass
class JobConfig:
    data_dir: Path = field(default_factory=lambda: settings().data_dir)
    checkpoint_dir: Path | None = None
    available_now: bool = False
    # Per-topic Spark startingOffsets ("earliest", "latest" or a JSON offsets map).
    # Only used when a query starts with an empty checkpoint.
    starting_offsets: dict[str, str] = field(default_factory=dict)
    trigger_seconds: int = field(default_factory=lambda: settings().stream_trigger_s)

    @property
    def checkpoints(self) -> Path:
        return self.checkpoint_dir or self.data_dir / "checkpoints"


# ---- shared helpers ----------------------------------------------------------------------------


def starting_offsets(cfg: JobConfig, subscribed: str) -> str:
    """startingOffsets for a query; a JSON offsets map is cut down to the topics this query reads."""
    names = subscribed.split(",")
    value = next(
        (cfg.starting_offsets[n] for n in [subscribed, *names] if n in cfg.starting_offsets), "earliest"
    )
    if not value.lstrip().startswith("{"):
        return value
    offsets = json.loads(value)
    return json.dumps({n: offsets[n] for n in names if n in offsets})


def read_topic(spark: SparkSession, cfg: JobConfig, topic: str) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings().kafka_bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets(cfg, topic))
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        # Retention may delete data an old checkpoint still points at; carry on instead of dying.
        .option("failOnDataLoss", "false")
        .load()
    )


def _start(
    writer: DataStreamWriter, cfg: JobConfig, name: str, trigger_seconds: int | None = None
) -> StreamingQuery:
    writer = writer.queryName(name).option("checkpointLocation", str(cfg.checkpoints / name))
    if cfg.available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=f"{trigger_seconds or cfg.trigger_seconds} seconds")
    query: StreamingQuery = writer.start()
    log.info("query started", extra={"data": {"query": name}})
    return query


def normalize_batches(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    """mapInPandas body: raw Kafka values -> one row per position / static / reject."""
    for pdf in batches:
        rows: list[dict[str, object]] = []
        for value, kafka_ts in zip(pdf["value"], pdf["kafka_ts"], strict=True):
            # Values arrive as bytes; a sender may put anything there, including invalid UTF-8.
            text = (
                value.decode("utf-8", errors="replace")
                if isinstance(value, bytes | bytearray)
                else str(value)
            )
            try:
                n = normalize(json.loads(text))
            except ValueError:
                n = Normalized(reject="not json")
            if n.reject:
                rows.append(
                    {"kind": "reject", "reason": n.reject, "raw": text[:RAW_KEEP_CHARS], "kafka_ts": kafka_ts}
                )
            if n.position:
                rows.append({"kind": "position", **n.position, "kafka_ts": kafka_ts})
            if n.static:
                rows.append({"kind": "static", **n.static, "kafka_ts": kafka_ts})
        out = pd.DataFrame(rows, columns=NORMALIZED_COLUMNS)
        # Missing keys become float NaN; Arrow cannot turn an all-NaN column into strings, so use None.
        out[STRING_COLUMNS] = out[STRING_COLUMNS].astype(object).where(out[STRING_COLUMNS].notna(), None)
        yield out.astype({"ts": "Int64", "eta": "Int64", "heading": "Int32", "nav_status": "Int32"})


def normalized(spark: SparkSession, cfg: JobConfig) -> DataFrame:
    raw = read_topic(spark, cfg, topics.RAW).select("value", F.col("timestamp").alias("kafka_ts"))
    return raw.mapInPandas(normalize_batches, NORMALIZED_SCHEMA)


def parsed(spark: SparkSession, cfg: JobConfig, topic: str, schema: str) -> DataFrame:
    """A normalised topic (JSON values) as typed columns plus an event_time timestamp.

    Anything else written to the topic (no MMSI or time after parsing, or a time more than a day ahead) is
    dropped, so one stray message cannot stop every query that reads it."""
    return (
        read_topic(spark, cfg, topic)
        .select(F.from_json(F.col("value").cast("string"), schema).alias("r"))
        .where("r.mmsi IS NOT NULL AND r.ts IS NOT NULL")
        .select("r.*")
        .withColumn("event_time", F.timestamp_millis("ts"))
        # A time more than a day ahead would pull every event-time watermark there, and each later message
        # would be "late" for ever. The ingest already refuses such times; this keeps old topic data out too.
        .where(F.col("event_time") <= F.current_timestamp() + F.expr("INTERVAL 1 DAY"))
    )


def _json_of(fields: list[str]) -> Column:
    return F.to_json(F.struct(*[F.col(f) for f in fields]))


# ---- queries -----------------------------------------------------------------------------------


def q_normalize(spark: SparkSession, cfg: JobConfig) -> StreamingQuery:
    clean = (
        normalized(spark, cfg)
        .filter(F.col("kind").isin("position", "static"))
        .withColumn("event_time", F.timestamp_millis("ts"))
        .withWatermark("event_time", WATERMARK)
        # Same ship + same event time = the same message received twice.
        .dropDuplicatesWithinWatermark(["kind", "mmsi", "ts"])
    )
    out = clean.select(
        F.col("mmsi").alias("key"),
        F.when(F.col("kind") == "position", _json_of(POSITION_FIELDS))
        .otherwise(_json_of(STATIC_FIELDS))
        .alias("value"),
        F.when(F.col("kind") == "position", F.lit(topics.POSITIONS))
        .otherwise(F.lit(topics.STATIC))
        .alias("topic"),
    )
    writer = out.writeStream.format("kafka").option("kafka.bootstrap.servers", settings().kafka_bootstrap)
    return _start(writer, cfg, "normalize")


def q_rejects(spark: SparkSession, cfg: JobConfig) -> StreamingQuery:
    rejects = (
        normalized(spark, cfg)
        .filter(F.col("kind") == "reject")
        .select("reason", "raw", "kafka_ts", F.to_date("kafka_ts").alias("date"))
    )
    writer = (
        rejects.writeStream.format("parquet")
        .partitionBy("date")
        .option("path", str(cfg.data_dir / "rejects"))
    )
    return _start(writer, cfg, "rejects", trigger_seconds=30)


def q_dlq(spark: SparkSession, cfg: JobConfig) -> StreamingQuery:
    """Stream malformed/rejected messages to the Kafka Dead-Letter Queue (ais.dlq)."""
    rejects = (
        normalized(spark, cfg)
        .filter(F.col("kind") == "reject")
        .select(
            F.coalesce(F.col("reason"), F.lit("unknown")).alias("key"),
            F.to_json(
                F.struct(
                    F.col("reason"),
                    F.col("raw"),
                    F.col("kafka_ts"),
                )
            ).alias("value"),
        )
    )
    writer = (
        rejects.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", settings().kafka_bootstrap)
        .option("topic", topics.DLQ)
    )
    return _start(writer, cfg, "dlq", trigger_seconds=30)


def q_archive(spark: SparkSession, cfg: JobConfig) -> StreamingQuery:
    both = topics.RAW + "," + topics.WEATHER
    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings().kafka_bootstrap)
        .option("subscribe", both)
        .option("startingOffsets", starting_offsets(cfg, both))
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .option("failOnDataLoss", "false")
        .load()
        .select(
            "topic",
            F.col("key").cast("string").alias("key"),
            "value",  # raw bytes, exactly as received
            F.col("timestamp").alias("kafka_ts"),
            "partition",
            "offset",
            F.to_date("timestamp").alias("date"),
        )
    )
    writer = (
        stream.writeStream.format("parquet")
        .partitionBy("topic", "date")
        .option("path", str(cfg.data_dir / "cold"))
    )
    return _start(writer, cfg, "archive", trigger_seconds=30)


def q_track(spark: SparkSession, cfg: JobConfig) -> StreamingQuery:
    positions = parsed(spark, cfg, topics.POSITIONS, POSITION_SCHEMA)

    def write(batch: DataFrame, batch_id: int) -> None:
        batch.foreachPartition(sinks.write_track_partition)

    return _start(positions.writeStream.foreachBatch(write), cfg, "track")


def q_static(spark: SparkSession, cfg: JobConfig) -> StreamingQuery:
    statics = parsed(spark, cfg, topics.STATIC, STATIC_SCHEMA)

    def write(batch: DataFrame, batch_id: int) -> None:
        # Ship details repeat in every NOAA row; keep only the newest per ship in this batch.
        latest = batch.groupBy("mmsi").agg(F.max_by(F.struct(*STATIC_FIELDS), "ts").alias("s")).select("s.*")
        sinks.write_info(r.asDict() for r in latest.toLocalIterator())

    return _start(statics.writeStream.foreachBatch(write), cfg, "static")


EVENT_SCHEMA = "type string, mmsi string, ts long, port_id string, lat double, lon double, data string"
EVENT_COLUMNS = [c.split()[0] for c in EVENT_SCHEMA.split(", ")]


StateFunc = Callable[[tuple[str], Iterator[pd.DataFrame], GroupState], Iterator[pd.DataFrame]]


def make_state_func(zones: Broadcast[ZoneIndex], dark_timeout_ms: int) -> StateFunc:
    """applyInPandasWithState body. The zone index is a broadcast: shipped once per executor, not per task."""

    def func(key: tuple[str], pdfs: Iterator[pd.DataFrame], state: GroupState) -> Iterator[pd.DataFrame]:
        mmsi = key[0]
        ship = ShipState.from_json(state.get[0]) if state.exists else None
        if state.hasTimedOut:
            assert ship is not None
            events, keep = on_timeout(ship, mmsi, dark_timeout_ms)
            if not keep:
                state.remove()
        else:
            positions = [p for pdf in pdfs for p in pdf.to_dict("records")]
            ship, events = apply_positions(ship, mmsi, positions, zones.value)
            keep = True
        if keep:
            assert ship is not None
            state.update((ship.to_json(),))
            # The timeout must lie beyond the current watermark, or Spark rejects it.
            state.setTimeoutTimestamp(
                max(next_timeout(ship, dark_timeout_ms), state.getCurrentWatermarkMs() + 1)
            )
        rows = [{**e, "data": json.dumps(e["data"])} for e in events]
        yield pd.DataFrame(rows, columns=EVENT_COLUMNS)

    return func


def q_state(spark: SparkSession, cfg: JobConfig) -> StreamingQuery:
    s = settings()
    dark_timeout_ms = int(s.dark_timeout_hours * 3_600_000)
    zones = spark.sparkContext.broadcast(ZoneIndex(load_zones()))
    events = (
        parsed(spark, cfg, topics.POSITIONS, POSITION_SCHEMA)
        .withWatermark("event_time", WATERMARK)
        .groupBy("mmsi")
        .applyInPandasWithState(
            make_state_func(zones, dark_timeout_ms),
            outputStructType=EVENT_SCHEMA,
            stateStructType="s string",
            outputMode="append",
            timeoutConf=GroupStateTimeout.EventTimeTimeout,
        )
    )
    producer: list[JsonProducer] = []

    def write(batch: DataFrame, batch_id: int) -> None:
        rows = [{**r.asDict(), "data": json.loads(r.data)} for r in batch.collect()]
        if not rows:
            return
        if not producer:
            producer.append(JsonProducer())
        counts = sinks.handle_events(rows, producer[0], HBaseClient(), dark_timeout_ms, s.min_active_ships)
        log.info("state batch", extra={"data": {"batch": batch_id, "events": len(rows), **counts}})

    return _start(events.writeStream.foreachBatch(write), cfg, "state")


ANCHORED_SCHEMA = "port_id string, mmsi string, event_time timestamp"


def make_anchored_filter(
    zones: Broadcast[ZoneIndex],
) -> Callable[[Iterator[pd.DataFrame]], Iterator[pd.DataFrame]]:
    """mapInPandas body: keep positions that on their own say "at anchor in port X"."""

    def func(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        for pdf in batches:
            keep = []
            for p in pdf.to_dict("records"):
                zone = zones.value.lookup(p["lat"], p["lon"])
                sog = None if p["sog"] != p["sog"] else p["sog"]
                nav = None if p["nav_status"] != p["nav_status"] else p["nav_status"]
                state, port = classify(zone, nav, sog, UNDERWAY, None)
                if state == ANCHORED:
                    keep.append({"port_id": port, "mmsi": p["mmsi"], "event_time": p["event_time"]})
            yield pd.DataFrame(keep, columns=["port_id", "mmsi", "event_time"])

    return func


def q_queue(spark: SparkSession, cfg: JobConfig) -> StreamingQuery:
    s = settings()
    zones = spark.sparkContext.broadcast(ZoneIndex(load_zones()))
    windows = (
        parsed(spark, cfg, topics.POSITIONS, POSITION_SCHEMA)
        .mapInPandas(make_anchored_filter(zones), ANCHORED_SCHEMA)
        .withWatermark("event_time", WATERMARK)
        .groupBy(F.window("event_time", "30 minutes", "5 minutes"), "port_id")
        # count(DISTINCT) is not allowed on streams. A set of MMSIs per window is exact and small (a port
        # has at most a few thousand ships); approx_count_distinct miscounted 12 ships as 11 in testing.
        .agg(F.size(F.collect_set("mmsi")).alias("ships"))
        .select(
            "port_id",
            F.unix_millis(F.col("window.start")).alias("window_start"),
            F.unix_millis(F.col("window.end")).alias("window_end"),
            "ships",
        )
    )
    producer: list[JsonProducer] = []

    def write(batch: DataFrame, batch_id: int) -> None:
        rows = [r.asDict() for r in batch.collect()]
        if not rows:
            return
        if not producer:
            producer.append(JsonProducer())
        alerts = sinks.handle_queue(rows, producer[0], HBaseClient(), s.queue_growth_pct, s.queue_growth_min)
        log.info("queue batch", extra={"data": {"batch": batch_id, "windows": len(rows), "alerts": alerts}})

    # update mode: a window is re-emitted whenever its count changes, so the dashboard sees it fill up.
    return _start(windows.writeStream.outputMode("update").foreachBatch(write), cfg, "queue")


QUERIES = {
    "normalize": q_normalize,
    "rejects": q_rejects,
    "dlq": q_dlq,
    "archive": q_archive,
    "track": q_track,
    "static": q_static,
    "state": q_state,
    "queue": q_queue,
}


def start(spark: SparkSession, names: list[str], cfg: JobConfig) -> list[StreamingQuery]:
    unknown = set(names) - set(QUERIES)
    if unknown:
        raise ValueError(f"unknown queries: {sorted(unknown)}")
    return [QUERIES[name](spark, cfg) for name in names]


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--queries", default=",".join(QUERIES))
    parser.add_argument("--available-now", action="store_true", help="process what is in Kafka, then stop")
    parser.add_argument("--starting-offsets", choices=["earliest", "latest"], default="earliest")
    args = parser.parse_args()
    topics.ensure_topics()
    cfg = JobConfig(available_now=args.available_now)
    names = [n.strip() for n in args.queries.split(",") if n.strip()]
    cfg.starting_offsets = {t: args.starting_offsets for t in (topics.RAW, topics.POSITIONS, topics.STATIC)}
    spark = session("harbourwatch-streaming", kafka=True, driver_memory=settings().spark_driver_memory)
    queries = start(spark, names, cfg)
    if cfg.available_now:
        for q in queries:
            q.awaitTermination()
    else:
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
