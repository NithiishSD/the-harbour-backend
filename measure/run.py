"""Measurements against the dev stack (Kafka + HBase from docker compose). Each command prints JSON.

    python -m measure.run throughput --noaa data/noaa/csv/AIS_2023_01_01_bbox.csv
    python -m measure.run latency --samples 20
    python -m measure.run alerts --noaa 'data/noaa/csv/AIS_2023_01_0[123]_bbox.csv'

throughput  replay a NOAA file into ais.raw as fast as possible (ingest rate), then drain it with the
            streaming job: normalize (raw -> positions) and track (positions -> HBase vessel_track),
            each as its own run, in a throw-away data directory so nothing else is touched.
alerts      replay NOAA days, run normalize, track and state over them, count alerts per type and day.
latency     run the streaming job continuously and time (a) one AIS report from Kafka until it is in
            HBase, (b) a crafted queue jump until the queue_growth alert is on the alerts topic.
"""

import argparse
import json
import os
import random
import statistics
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, TopicPartition

from common import topics
from common.config import ROOT, settings
from common.kafka import JsonProducer
from ingest.replay import noaa_records, replay
from storage.hbase.client import HBaseClient
from storage.hbase.tables import VESSEL_TRACK, create_tables
from streaming.sinks import iso

HOUR = 3_600_000
MIN = 60_000


def end_offset(name: str) -> int:
    consumer = Consumer({"bootstrap.servers": settings().kafka_bootstrap, "group.id": "measure-probe"})
    try:
        total = 0
        for p in range(topics.PARTITIONS[name]):
            _, high = consumer.get_watermark_offsets(TopicPartition(name, p), timeout=10)
            total += high
        return total
    finally:
        consumer.close()


def offsets_json(*names: str) -> dict[str, str]:
    consumer = Consumer({"bootstrap.servers": settings().kafka_bootstrap, "group.id": "measure-probe"})
    try:
        out = {}
        for name in names:
            high = {
                str(p): consumer.get_watermark_offsets(TopicPartition(name, p), timeout=10)[1]
                for p in range(topics.PARTITIONS[name])
            }
            out[name] = json.dumps({name: high})
        return out
    finally:
        consumer.close()


JOB = """
import json, sys
from pathlib import Path
from common.spark import session
from streaming.job import JobConfig, start

names, offsets, mode, data = sys.argv[1].split(","), json.loads(sys.argv[2]), sys.argv[3], Path(sys.argv[4])
cfg = JobConfig(data_dir=data, starting_offsets=offsets, available_now=mode == "once",
                trigger_seconds=int(sys.argv[5]))
spark = session("measure", master="local[4]", kafka=True, driver_memory="2g")
queries = start(spark, names, cfg)
for q in queries:
    q.awaitTermination()
"""


def job(
    queries: str, offsets: dict[str, str], mode: str, data: Path, trigger: int = 2
) -> subprocess.Popen[bytes]:
    return subprocess.Popen(  # noqa: S603 - fixed script, our own arguments
        [sys.executable, "-c", JOB, queries, json.dumps(offsets), mode, str(data), str(trigger)],
        cwd=ROOT,
        env=os.environ | {"PYTHONPATH": str(ROOT)},
        start_new_session=True,
    )


def throughput(noaa: str) -> dict[str, Any]:
    topics.ensure_topics()
    create_tables()
    raw_before = end_offset(topics.RAW)
    pos_before = end_offset(topics.POSITIONS)
    start_offsets = offsets_json(topics.RAW, topics.POSITIONS)
    producer = JsonProducer()
    t = time.monotonic()
    sent = replay(noaa_records(noaa), producer, speed=0)
    ingest_s = time.monotonic() - t
    result: dict[str, Any] = {
        "file": noaa,
        "messages": sent,
        "ingest_seconds": round(ingest_s, 2),
        "ingest_msgs_per_s": round(sent / ingest_s),
    }
    with tempfile.TemporaryDirectory(prefix="hw-measure-") as tmp:
        t = time.monotonic()
        assert job("normalize", start_offsets, "once", Path(tmp)).wait(1800) == 0
        normalize_s = time.monotonic() - t
        positions = end_offset(topics.POSITIONS) - pos_before
        result |= {
            "raw_in_topic": end_offset(topics.RAW) - raw_before,
            "normalize_seconds": round(normalize_s, 1),
            "normalize_msgs_per_s": round(sent / normalize_s),
            "positions": positions,
        }
        t = time.monotonic()
        assert (
            job("track", {topics.POSITIONS: start_offsets[topics.POSITIONS]}, "once", Path(tmp)).wait(1800)
            == 0
        )
        track_s = time.monotonic() - t
        result |= {
            "track_seconds": round(track_s, 1),
            "hbase_rows_per_s": round(positions / track_s),
        }
    return result


def read_since(name: str, start: dict[str, str]) -> list[dict[str, Any]]:
    """Every message on `name` after the offsets in start[name] (a Spark startingOffsets JSON map)."""
    begin = json.loads(start[name])[name]
    consumer = Consumer({"bootstrap.servers": settings().kafka_bootstrap, "group.id": "measure-read"})
    out = []
    try:
        for p, offset in begin.items():
            tp = TopicPartition(name, int(p))
            _, high = consumer.get_watermark_offsets(tp, timeout=10)
            consumer.assign([TopicPartition(name, int(p), offset)])
            position = offset
            while position < high:
                msg = consumer.poll(5)
                if msg is None:
                    break
                position = (msg.offset() or position) + 1
                value = msg.value()
                if not msg.error() and value is not None:
                    out.append(json.loads(value))
    finally:
        consumer.close()
    return out


def alerts(noaa: str) -> dict[str, Any]:
    topics.ensure_topics()
    create_tables()
    start = offsets_json(topics.RAW, topics.POSITIONS, topics.ALERTS)
    sent = replay(noaa_records(noaa), JsonProducer(), speed=0)
    per_day: dict[str, dict[str, int]] = {}
    with tempfile.TemporaryDirectory(prefix="hw-measure-") as tmp:
        positions = {topics.POSITIONS: start[topics.POSITIONS]}
        assert job("normalize", start, "once", Path(tmp)).wait(1800) == 0
        assert job("track", positions, "once", Path(tmp)).wait(1800) == 0  # state asks HBase who is active
        assert job("state", positions, "once", Path(tmp)).wait(1800) == 0
    for a in read_since(topics.ALERTS, start):
        day = per_day.setdefault(str(a.get("event_time", ""))[:10], {})
        day[a["type"]] = day.get(a["type"], 0) + 1
    return {"file": noaa, "messages": sent, "alerts_per_day": dict(sorted(per_day.items()))}


def summarise(values: list[float]) -> dict[str, float]:
    ordered = sorted(values)
    return {
        "n": len(values),
        "min_s": round(ordered[0], 2),
        "median_s": round(statistics.median(ordered), 2),
        "p95_s": round(ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))], 2),
        "max_s": round(ordered[-1], 2),
    }


def raw_report(mmsi: int, now_ms: int) -> dict[str, Any]:
    """An aisstream PositionReport at sea (no zone), the shape the live producer delivers."""
    time_utc = time.strftime("%Y-%m-%d %H:%M:%S.000000000 +0000 UTC", time.gmtime(now_ms / 1000))
    body = {"Cog": 90, "Latitude": 20.0, "Longitude": -150.0, "Sog": 12.0, "TrueHeading": 90, "UserID": mmsi}
    msg = {
        "Message": {"PositionReport": body | {"NavigationalStatus": 0}},
        "MessageType": "PositionReport",
        "MetaData": {"MMSI": mmsi, "ShipName": "MEASURE", "time_utc": time_utc},
    }
    return {"source": "aisstream", "received_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"), "payload": msg}


def visible(client: HBaseClient, mmsi: int, deadline: float) -> float | None:
    while time.monotonic() < deadline:
        if client.scan_prefix(VESSEL_TRACK, str(mmsi), limit=1):
            return time.monotonic()
        time.sleep(0.05)
    return None


def alert_seen(consumer: Consumer, port: str, event_ms: int, deadline: float) -> float | None:
    while time.monotonic() < deadline:
        msg = consumer.poll(0.1)
        value = None if msg is None or msg.error() else msg.value()
        if value is None:
            continue
        alert = json.loads(value)
        same = alert.get("type") == "queue_growth" and alert.get("port_id") == port
        if same and alert.get("event_time") == iso(event_ms):
            return time.monotonic()
    return None


def latency(samples: int, trigger: int) -> dict[str, Any]:
    topics.ensure_topics()
    create_tables()
    client = HBaseClient()
    producer = JsonProducer()
    base = random.randint(300_000, 399_000) * 1000
    with tempfile.TemporaryDirectory(prefix="hw-measure-") as tmp:
        proc = job(
            "normalize,track,queue",
            {t: "latest" for t in (topics.RAW, topics.POSITIONS)},
            "stream",
            Path(tmp),
            trigger,
        )
        try:
            warm = 0
            while True:  # first report only counts once the job is up
                mmsi = base + warm
                producer.send(topics.RAW, str(mmsi), raw_report(mmsi, int(time.time() * 1000)))
                producer.flush()
                if visible(client, mmsi, time.monotonic() + 15):
                    break
                warm += 1
                assert warm < 20, "streaming job did not come up"
            times = []
            for i in range(samples):
                time.sleep(random.uniform(1, 4))  # land at random points of the trigger cycle
                mmsi = base + 100 + i
                sent = time.monotonic()
                producer.send(topics.RAW, str(mmsi), raw_report(mmsi, int(time.time() * 1000)))
                producer.flush()
                seen = visible(client, mmsi, sent + 60)
                assert seen is not None, f"{mmsi} never reached HBase"
                times.append(seen - sent)
            alert = alert_latency(producer)
        finally:
            os.killpg(proc.pid, 9)
    return {"report_to_hbase": summarise(times), "queue_alert": alert, "trigger_seconds": trigger}


def alert_latency(producer: JsonProducer) -> dict[str, Any]:
    """Two ships at anchor off Long Beach, then ten more half an hour later: window count 2 -> 12."""
    t0 = 1_893_456_000_000 + random.randint(0, 2_000_000) * HOUR
    first = random.randint(300_000, 399_000) * 1000
    consumer = Consumer(
        {
            "bootstrap.servers": settings().kafka_bootstrap,
            "group.id": f"measure-{uuid.uuid4()}",
            "auto.offset.reset": "latest",
        }
    )
    consumer.subscribe([topics.ALERTS])
    consumer.poll(2)  # join the group before the alert can be produced

    def send(ships: range, start: int) -> None:
        for i in ships:
            for ts in range(start, start + 30 * MIN, 3 * MIN):
                p = dict(
                    mmsi=str(first + i),
                    ts=ts,
                    lat=33.740,
                    lon=-118.165,
                    sog=0.1,
                    cog=None,
                    heading=None,
                    nav_status=1,
                    source="measure",
                )
                producer.send(topics.POSITIONS, str(p["mmsi"]), p)
        producer.flush()

    send(range(2), t0)
    time.sleep(10)
    sent = time.monotonic()
    send(range(2, 12), t0 + 30 * MIN)
    seen = alert_seen(consumer, "USLGB", t0 + HOUR, sent + 90)
    consumer.close()
    return {"seconds": None if seen is None else round(seen - sent, 2)}


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("throughput").add_argument("--noaa", required=True)
    sub.add_parser("alerts").add_argument("--noaa", required=True)
    lat = sub.add_parser("latency")
    lat.add_argument("--samples", type=int, default=20)
    lat.add_argument("--trigger", type=int, default=10, help="streaming trigger seconds (job default 10)")
    args = parser.parse_args()
    if args.command == "latency":
        out, name = latency(args.samples, args.trigger), f"latency_{args.trigger}"
    else:
        out, name = (throughput if args.command == "throughput" else alerts)(args.noaa), args.command
    path = Path(settings().data_dir) / "measure" / f"{name}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, indent=2))
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
