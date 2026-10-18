"""Automated AIS data ingestion worker.

Selects between live AIS streaming and demo replay automatically:
- If INGEST_MODE is 'replay' or 'demo', runs continuous NOAA replay.
- If AISSTREAM_API_KEY is present (and INGEST_MODE is 'auto' or 'live'), runs live AIS stream + follower.
- If AISSTREAM_API_KEY is missing, falls back to continuous NOAA replay with looping.
"""

import asyncio
import glob
import json
import os
import time
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from common import scopes, topics
from common.config import settings
from common.geo import load_world_ports
from common.kafka import JsonProducer
from common.log import get_logger
from common.monitor import Monitor
from ingest.aisstream import (
    URL,
    WORLD,
    FatalStreamError,
)
from ingest.aisstream import (
    run as run_aisstream,
)
from ingest.follow import STATUS, ships_to_follow
from ingest.replay import noaa_records, replay
from storage.hbase.client import HBaseClient

log = get_logger("ingest.auto")


async def run_live(s: Any) -> None:
    world = load_world_ports()
    environment = Monitor.from_settings(world)
    producer = JsonProducer()

    def _live_since_ms() -> int:
        d = s.live_since
        return int(datetime(d.year, d.month, d.day, tzinfo=UTC).timestamp() * 1000)

    def boxes() -> tuple[tuple[float, float, float, float], ...]:
        monitor = Monitor.selected(world, environment)
        found = tuple(s.live_bboxes) or monitor.ais_boxes(world)
        scopes.record(s.data_dir, monitor.country, monitor.ports, found, first_data_ms=_live_since_ms())
        return found

    client = HBaseClient(timeout=30)
    last: list[tuple[float, list[str]]] = []

    def follow_listing() -> Sequence[str]:
        if last and time.monotonic() - last[0][0] < s.follow_refresh_s:
            return last[0][1]
        try:
            monitor = Monitor.selected(world, environment)
            ships = ships_to_follow(client, monitor, world)
            last[:] = [(time.monotonic(), ships)]
            status_path = s.data_dir / STATUS
            status = {"ships": len(ships), "max": s.follow_max, "at": int(time.time() * 1000), "mmsis": ships}
            status_path.write_text(json.dumps(status))
            return ships
        except Exception:
            return []

    log.info("starting live regional stream and voyage follower")
    regional_task = asyncio.create_task(run_aisstream(URL, s.aisstream_api_key, boxes, producer))
    follow_task = asyncio.create_task(
        run_aisstream(URL, s.aisstream_api_key, WORLD, producer, follow=follow_listing)
    )
    await asyncio.gather(regional_task, follow_task)


def run_replay(s: Any, speed: float = 60.0) -> None:
    topics.ensure_topics()
    data_dir = Path(s.data_dir)
    noaa_files = sorted(glob.glob(str(data_dir / "noaa" / "csv" / "*.csv")))
    if not noaa_files:
        log.error("no NOAA CSV files found for replay in data/noaa/csv")
        return
    file_path = noaa_files[0]
    log.info("starting automated replay", extra={"data": {"file": file_path, "speed": speed}})
    producer = JsonProducer()
    while True:
        try:
            replay(noaa_records(file_path), producer, speed=speed)
            log.info("replay iteration completed; looping in 5 seconds...")
            time.sleep(5)
        except Exception as err:
            log.error("replay iteration failed, retrying in 10s", extra={"data": {"error": str(err)}})
            time.sleep(10)


def main() -> None:
    s = settings()
    topics.ensure_topics()
    mode = os.environ.get("INGEST_MODE", "auto").lower()

    use_live = (mode == "live") or (mode == "auto" and bool(s.aisstream_api_key))

    if use_live:
        log.info("ingest worker: live mode (AISSTREAM_API_KEY detected)")
        try:
            asyncio.run(run_live(s))
        except FatalStreamError as err:
            log.error(
                "aisstream authentication error, falling back to replay", extra={"data": {"error": str(err)}}
            )
            run_replay(s)
        except Exception as err:
            log.error("live stream error, falling back to replay", extra={"data": {"error": str(err)}})
            run_replay(s)
    else:
        log.info("ingest worker: automated replay mode (no API key or demo mode requested)")
        run_replay(s)


if __name__ == "__main__":
    main()
