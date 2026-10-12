"""Take the test and demo data out of the dev stack: only NOAA and the live feed (and its history) stay.

    python -m batch.purge_test_data                         # dry run: what would go
    python -m batch.purge_test_data --apply                 # do it
    python -m batch.purge_test_data --apply --weather-grid  # and recreate the weather.grid topic

What counts as test data is decided by common.testdata: the demo fleet of ingest.seed, the measurement
probes, the fixtures of the test suites and anything stamped outside NOAA's days and the live feed's life.

  HBase     vessel_state and vessel_track rows of those ships, port_queue and port_calls rows of such times
  history   every aisstream file is rebuilt from the raw archive without them (batch.archive_history)
  Kafka     with --weather-grid, the weather.grid topic is recreated (it held the demo storms; the weather
            producer refills it within the hour)

The raw archive (data/cold) stays as it is: it is the audit log, and archive_history leaves test data out.
Alerts are filtered when the dashboard reads them. Run the dashboard again afterwards (it reads weather.grid
and alerts from the start).
"""

import argparse
import shutil
import time
from typing import Any

from confluent_kafka.admin import AdminClient

from batch import archive_history
from common import topics
from common.config import settings
from common.log import get_logger
from common.testdata import RealData
from storage.hbase.client import HBaseClient
from storage.hbase.tables import PORT_CALLS, PORT_QUEUE, VESSEL_STATE, VESSEL_TRACK

log = get_logger("batch.purge_test_data")
MAX_TRACK_ROWS = 1_000_000


def _ms(row: dict[str, str], *columns: str) -> int | None:
    for column in columns:
        if row.get(column):
            try:
                return int(float(row[column]))
            except ValueError:
                return None
    return None


def _float(value: str | None) -> float | None:
    try:
        return None if value is None else float(value)
    except ValueError:
        return None


def purge_hbase(client: HBaseClient, real: RealData, apply: bool) -> dict[str, int]:
    out = {"ships": 0, "track_rows": 0, "queue_rows": 0, "call_rows": 0}
    for mmsi, row in client.scan_all(VESSEL_STATE, 1_000_000):
        if not real.is_test_ship(
            mmsi,
            row.get("info:name"),
            _float(row.get("last:lat")),
            _float(row.get("last:lon")),
            _ms(row, "last:ts"),
        ):
            continue
        out["ships"] += 1
        track = client.scan_prefix(VESSEL_TRACK, mmsi, MAX_TRACK_ROWS)
        out["track_rows"] += len(track)
        if apply:
            for key, _ in track:
                client.delete_row(VESSEL_TRACK, key)
            client.delete_row(VESSEL_STATE, mmsi)
    for key, row in client.scan_all(PORT_QUEUE, 1_000_000):
        if not real.real_time(_ms(row, "q:window_end")):
            out["queue_rows"] += 1
            if apply:
                client.delete_row(PORT_QUEUE, key)
    for key, row in client.scan_all(PORT_CALLS, 1_000_000):
        if not real.real_time(_ms(row, "call:anchored_at", "call:berthed_at", "call:departed_at")):
            out["call_rows"] += 1
            if apply:
                client.delete_row(PORT_CALLS, key)
    return out


def recreate_topic(name: str) -> None:
    admin = AdminClient({"bootstrap.servers": settings().kafka_bootstrap})
    if name in admin.list_topics(timeout=10).topics:
        for future in admin.delete_topics([name]).values():
            future.result()
        for _ in range(30):  # deletion is asynchronous
            if name not in admin.list_topics(timeout=10).topics:
                break
            time.sleep(1)
    topics.ensure_topics()


def run(apply: bool, weather_grid: bool = False) -> dict[str, Any]:
    s = settings()
    real = RealData.load(s.data_dir, s.live_since)
    report: dict[str, Any] = {"applied": apply, "hbase": purge_hbase(HBaseClient(timeout=60), real, apply)}
    history = s.data_dir / "history"
    files = [
        f
        for n in ("positions", "vessels", "destinations")
        for f in (history / n).glob("date=*/aisstream.parquet")
    ]
    report["history_files"] = len(files)
    if apply:
        for f in files:
            f.unlink()
            if not any(f.parent.iterdir()):
                shutil.rmtree(f.parent)
        report["history"] = archive_history.run(s.data_dir)
        if weather_grid:
            recreate_topic(topics.WEATHER_GRID)
    log.info("purge", extra={"data": report})
    return report


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--apply", action="store_true", help="delete (without it only counts)")
    parser.add_argument("--weather-grid", action="store_true", help="also recreate the weather.grid topic")
    args = parser.parse_args()
    run(args.apply, args.weather_grid)


if __name__ == "__main__":
    main()
