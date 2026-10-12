"""One nightly command: NOAA day -> clean history -> port calls and KPIs -> learned anchorages -> DuckDB.

    python -m batch.nightly --date 2023-01-01
    python -m batch.nightly --date 2026-10-10 --live    # a day of the live feed: steps 1, 2 and 4 are skipped

Steps (one Spark session)
  1. data/noaa/csv/AIS_YYYY_MM_DD_bbox.csv is used when present, else the day is downloaded, clipped to --bbox
  2. batch.backfill_rdd   history/positions + history/vessels
  3. batch.port_calls_sql batch/port_calls + batch/port_daily
  4. batch.anchorage_kmeans  geo/learned_anchorages.geojson (history up to the date)
  5. storage.warehouse.load  the date into data/warehouse.duckdb
Every step replaces only its own output for the date, so a failed night is repaired by running it again.
"""

import argparse
from datetime import date
from pathlib import Path
from typing import Any

from batch.anchorage_kmeans import learn
from batch.backfill_rdd import backfill
from batch.port_calls_sql import port_calls
from common.config import settings
from common.geo import load_ports
from common.log import get_logger
from common.spark import session
from ingest import noaa
from storage.warehouse.load import connect, load_date

log = get_logger("batch.nightly")

# Los Angeles and Long Beach: the only ports in geo/ports.csv that NOAA (US waters) covers.
DEFAULT_BBOX: noaa.BBox = (33.4, -118.6, 33.9, -117.9)


def source_csv(day: date, data_dir: Path, bbox: noaa.BBox) -> Path:
    csv = data_dir / "noaa" / "csv" / f"{noaa.file_name(day)}_bbox.csv"
    if not csv.is_file():
        zip_path = noaa.download(day, data_dir / "noaa" / "zip")
        noaa.extract(zip_path, csv, bbox)
        zip_path.unlink()
    return csv


def nightly(
    day: date, data_dir: Path, warehouse: Path, bbox: noaa.BBox = DEFAULT_BBOX, live: bool = False
) -> dict[str, Any]:
    """`live`: the day is one of the live feed's (batch.archive_history wrote its history): no NOAA file to
    fetch or clean, the port calls are worked out from what the feed archived."""
    spark = session("harbourwatch-nightly", driver_memory="2g")
    summary: dict[str, Any] = {"date": day.isoformat()}
    if not live:
        csv = source_csv(day, data_dir, bbox)
        summary["backfill"] = backfill(spark, str(csv), data_dir)
        log.info("backfill done", extra={"data": {"rows": summary["backfill"]["rows"]}})
    summary["port_calls"] = port_calls(spark, day, data_dir)
    log.info("port calls done", extra={"data": {"calls": summary["port_calls"]["calls"]}})
    if not live:
        summary["anchorages"] = learn(spark, data_dir, until=day)
    con = connect(warehouse)
    try:
        summary["warehouse"] = load_date(con, day, data_dir, load_ports())
    finally:
        con.close()
    log.info("nightly finished", extra={"data": {"date": summary["date"], "warehouse": summary["warehouse"]}})
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument(
        "--bbox", default=",".join(map(str, DEFAULT_BBOX)), help="min_lat,min_lon,max_lat,max_lon"
    )
    parser.add_argument(
        "--live", action="store_true", help="a day of the live feed (its archived history), not a NOAA day"
    )
    args = parser.parse_args()
    p = [float(x) for x in args.bbox.split(",")]
    cfg = settings()
    nightly(args.date, cfg.data_dir, cfg.warehouse_path, (p[0], p[1], p[2], p[3]), args.live)


if __name__ == "__main__":
    main()
