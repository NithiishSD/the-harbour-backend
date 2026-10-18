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
import contextlib
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


def available_dates(data_dir: Path) -> list[tuple[date, bool]]:
    """Returns list of (day, is_live) available in data_dir."""
    found: list[tuple[date, bool]] = []
    # 1. NOAA CSVs (live=False)
    noaa_dir = data_dir / "noaa" / "csv"
    if noaa_dir.is_dir():
        for p in sorted(noaa_dir.glob("AIS_*_bbox.csv")):
            stem = p.name.replace("AIS_", "").replace("_bbox.csv", "")
            with contextlib.suppress(ValueError):
                found.append((date.fromisoformat(stem.replace("_", "-")), False))
    # 2. Archived live history (live=True)
    hist_dir = data_dir / "history" / "positions"
    if hist_dir.is_dir():
        for p in sorted(hist_dir.glob("date=*")):
            val = p.name.split("=")[-1]
            with contextlib.suppress(ValueError):
                found.append((date.fromisoformat(val), True))
    return sorted(set(found), key=lambda x: (x[0], x[1]))


def loaded_dates(warehouse: Path) -> set[date]:
    if not warehouse.is_file():
        return set()
    import duckdb

    con = duckdb.connect(str(warehouse), read_only=True)
    query = (
        "SELECT DISTINCT date FROM dim_date WHERE date_key IN (SELECT DISTINCT date_key FROM fact_port_daily)"
    )
    try:
        rows = con.execute(query).fetchall()
        return {r[0] for r in rows if r[0] is not None}
    except Exception:
        return set()
    finally:
        con.close()


def run_auto(data_dir: Path, warehouse: Path, bbox: noaa.BBox = DEFAULT_BBOX) -> list[dict[str, Any]]:
    from datetime import UTC, datetime, timedelta

    avail = available_dates(data_dir)
    loaded = loaded_dates(warehouse)
    results = []
    for day, is_live in avail:
        if day not in loaded:
            log.info(
                "auto nightly: processing day",
                extra={"data": {"date": day.isoformat(), "live": is_live}},
            )
            try:
                res = nightly(day, data_dir, warehouse, bbox=bbox, live=is_live)
                results.append(res)
            except Exception as err:
                log.error(
                    "auto nightly: failed for day",
                    extra={"data": {"date": day.isoformat(), "error": str(err)}},
                )

    # Check yesterday if live feed was archived
    yesterday = datetime.now(UTC).date() - timedelta(days=1)
    yesterday_dir = data_dir / "history" / "positions" / f"date={yesterday.isoformat()}"
    if yesterday_dir.is_dir() and yesterday not in loaded:
        try:
            res = nightly(yesterday, data_dir, warehouse, bbox=bbox, live=True)
            results.append(res)
        except Exception as err:
            log.error(
                "auto nightly: failed for yesterday",
                extra={"data": {"date": yesterday.isoformat(), "error": str(err)}},
            )
    return results


def main() -> None:
    import time

    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--date", type=date.fromisoformat, help="specific date to process YYYY-MM-DD")
    parser.add_argument(
        "--auto", action="store_true", help="automatically detect and load un-processed dates"
    )
    parser.add_argument(
        "--every",
        type=float,
        nargs="?",
        const=86400,
        help="repeat every this many seconds (default 86400 / 24h)",
    )
    parser.add_argument(
        "--bbox", default=",".join(map(str, DEFAULT_BBOX)), help="min_lat,min_lon,max_lat,max_lon"
    )
    parser.add_argument(
        "--live", action="store_true", help="a day of the live feed (its archived history), not a NOAA day"
    )
    args = parser.parse_args()
    p = [float(x) for x in args.bbox.split(",")]
    cfg = settings()
    bbox: noaa.BBox = (p[0], p[1], p[2], p[3])

    if args.auto or not args.date:
        run_auto(cfg.data_dir, cfg.warehouse_path, bbox)
        if args.every:
            while True:
                time.sleep(args.every)
                run_auto(cfg.data_dir, cfg.warehouse_path, bbox)
        return

    nightly(args.date, cfg.data_dir, cfg.warehouse_path, bbox, args.live)


if __name__ == "__main__":
    main()
