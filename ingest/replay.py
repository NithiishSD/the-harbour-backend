"""Replay recorded AIS into ais.raw at 1x-60x (or as fast as possible), in event-time order.

    python -m ingest.replay --noaa data/noaa/csv/AIS_2023_01_01_bbox.csv --speed 60
    python -m ingest.replay --cold data/cold/ais.raw --from 2026-10-01 --to 2026-10-02 --speed 1

Spark cannot tell a replay from the live feed: same topic, same envelope format, original event times.
DuckDB does the sorting, so files larger than memory are streamed rather than loaded.
"""

import argparse
import json
import time
from collections.abc import Callable, Iterator
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Protocol

import duckdb

from common import topics
from common.kafka import JsonProducer
from common.log import get_logger
from common.schema import key_of, parse_time

log = get_logger("ingest.replay")
Record = tuple[int, dict[str, Any]]  # (event time ms, raw envelope)


class Sender(Protocol):
    def send(self, topic: str, key: str | None, value: dict[str, Any]) -> None: ...
    def flush(self, timeout: float = 30) -> int: ...


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")


def noaa_records(path: str) -> Iterator[Record]:
    con = duckdb.connect()
    try:
        cur = con.execute(
            "SELECT * FROM read_csv(?, all_varchar = true, header = true) ORDER BY base_date_time", [path]
        )
    except duckdb.BinderException:
        cur = con.execute(
            "SELECT * FROM read_csv(?, all_varchar = true, header = true) ORDER BY BaseDateTime", [path]
        )
    cols = [d[0] for d in cur.description]
    while batch := cur.fetchmany(10_000):
        for values in batch:
            row = dict(zip(cols, values, strict=True))
            try:
                raw_time = row.get("base_date_time") or row.get("BaseDateTime")
                ts = parse_time(raw_time)
            except ValueError:
                ts = 0  # still sent: the streaming job rejects and counts it
            yield ts, {"source": "noaa", "received_at": _now_iso(), "payload": row}


def cold_records(root: Path, start: date, end: date) -> Iterator[Record]:
    """Raw envelopes archived by the streaming job, between start and end inclusive."""
    con = duckdb.connect()
    cur = con.execute(
        "SELECT value, epoch_ms(kafka_ts) FROM read_parquet(?, hive_partitioning = true) "
        "WHERE CAST(date AS DATE) BETWEEN ? AND ? ORDER BY kafka_ts",
        [str(root / "**" / "*.parquet"), start, end],
    )
    skipped = 0
    while batch := cur.fetchmany(10_000):
        for value, kafka_ms in batch:
            try:
                envelope = json.loads(value)
            except ValueError:
                skipped += 1  # rejected the first time too; nothing worth replaying
                continue
            yield int(kafka_ms), envelope
    if skipped:
        log.info("skipped unparseable archived messages", extra={"data": {"count": skipped}})


class Pacer:
    """Sleeps so that event-time gaps are reproduced at `speed` times real time (speed <= 0: no sleep)."""

    def __init__(
        self,
        speed: float,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.speed, self.clock, self.sleep = speed, clock, sleep
        self._first_event: int | None = None
        self._start = 0.0

    def wait(self, event_ms: int) -> None:
        if self.speed <= 0:
            return
        if self._first_event is None:
            self._first_event, self._start = event_ms, self.clock()
            return
        target = self._start + (event_ms - self._first_event) / 1000 / self.speed
        delay = target - self.clock()
        if delay > 0:
            self.sleep(delay)


def replay(records: Iterator[Record], sender: Sender, speed: float, limit: int | None = None) -> int:
    pacer = Pacer(speed)
    sent = 0
    started = last_report = time.monotonic()
    for ts, envelope in records:
        if limit is not None and sent >= limit:
            break
        pacer.wait(ts)
        sender.send(topics.RAW, key_of(envelope), envelope)
        sent += 1
        now = time.monotonic()
        if now - last_report >= 10:
            log.info(
                "progress", extra={"data": {"sent": sent, "rate_per_s": round(sent / (now - started), 1)}}
            )
            last_report = now
    pending = sender.flush()
    elapsed = max(time.monotonic() - started, 1e-9)
    log.info(
        "replay finished",
        extra={
            "data": {
                "sent": sent,
                "pending": pending,
                "seconds": round(elapsed, 2),
                "rate_per_s": round(sent / elapsed, 1),
            }
        },
    )
    return sent


def main() -> None:
    import glob

    from common.config import settings

    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    source = parser.add_mutually_exclusive_group(required=False)
    source.add_argument("--noaa", help="NOAA CSV path or glob")
    source.add_argument("--cold", type=Path, help="cold storage root for ais.raw")
    parser.add_argument("--from", dest="start", type=date.fromisoformat)
    parser.add_argument("--to", dest="end", type=date.fromisoformat)
    parser.add_argument("--speed", type=float, default=60, help="1-60 for paced replay, 0 for max speed")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--loop", action="store_true", help="repeat replay in a loop")
    args = parser.parse_args()
    if args.speed < 0 or args.speed > 60:
        parser.error("--speed must be between 0 (max) and 60")

    def _get_records() -> Iterator[Record]:
        if args.cold:
            if not (args.start and args.end):
                parser.error("--cold needs --from and --to")
            return cold_records(args.cold, args.start, args.end)
        if args.noaa:
            return noaa_records(args.noaa)
        data_dir = settings().data_dir
        noaa_files = sorted(glob.glob(str(Path(data_dir) / "noaa" / "csv" / "*.csv")))
        if not noaa_files:
            parser.error("No NOAA csv files found in data/noaa/csv; provide --noaa or --cold")
        log.info("auto-selected noaa file for replay", extra={"data": {"file": noaa_files[0]}})
        return noaa_records(noaa_files[0])

    topics.ensure_topics()
    producer = JsonProducer()
    while True:
        replay(_get_records(), producer, args.speed, args.limit)
        if not args.loop:
            break
        log.info("replay iteration finished, looping in 5 seconds...")
        time.sleep(5)


if __name__ == "__main__":
    main()
