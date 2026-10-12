"""Past aisstream data -> the offline history: raw messages archived by the streaming job, cleaned and capped.

    python -m batch.archive_history                 # every archived day, once
    python -m batch.archive_history --every 600     # keep doing it every 10 minutes (the compose service)
    python -m batch.archive_history --date 2026-10-10

Reads data/cold/topic=ais.raw (the Kafka date of the message) and keeps the aisstream envelopes; NOAA rows are
left to the backfill (batch.backfill_rdd), which writes the same layout:

  data/history/positions/date=YYYY-MM-DD/aisstream.parquet     one row per (MMSI, report time), event date
  data/history/vessels/date=YYYY-MM-DD/aisstream.parquet       newest ship details per ship and day
  data/history/destinations/date=YYYY-MM-DD/aisstream.parquet  destination and ETA the crew typed

The dashboard's offline mode reads these folders. A file is merged with what it already held, so running
again (or only for the recent days) gives the same result. Demo ships, test fixtures and the measurement
probes are left out (common.testdata).

Size caps (HISTORY_MAX_MB for the three folders above, COLD_MAX_MB for the raw archive): after every run
the oldest days are removed until each is under its cap. The newest day always stays; NOAA files are never
touched.
"""

import argparse
import json
import shutil
import time
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from common.config import settings
from common.log import get_logger
from common.schema import normalize
from common.testdata import RealData

log = get_logger("batch.archive_history")
OUT_NAME = "aisstream.parquet"
POSITION_COLUMNS = ["mmsi", "ts", "lat", "lon", "sog", "cog", "heading", "nav_status", "source"]
VESSEL_COLUMNS = ["mmsi", "ts", "name", "ship_type", "length_m", "draught_m", "imo", "callsign"]
DESTINATION_COLUMNS = ["mmsi", "ts", "destination", "eta"]
# Column types, so every day's file reads the same whatever its values were.
TYPES = {
    "ts": "BIGINT", "eta": "BIGINT", "heading": "INTEGER", "nav_status": "INTEGER",
    "lat": "DOUBLE", "lon": "DOUBLE", "sog": "DOUBLE", "cog": "DOUBLE",
    "length_m": "DOUBLE", "draught_m": "DOUBLE",
}  # fmt: skip


def _day(ts_ms: int) -> date:
    return datetime.fromtimestamp(ts_ms / 1000, UTC).date()


def envelopes(cold: Path, since: date | None, only: date | None) -> Any:
    """Archived aisstream envelopes, oldest first. Rows that are not JSON (the rejects) are skipped by SQL."""
    where = "WHERE TRY(json_extract_string(decode(value, 'replace'), '$.source')) = 'aisstream'"
    if only:
        where += f" AND date = DATE '{only.isoformat()}'"
    elif since:
        where += f" AND date >= DATE '{since.isoformat()}'"
    if not any((cold / "topic=ais.raw").glob("date=*/*.parquet")):
        return
    con = duckdb.connect()
    cur = con.execute(
        f"SELECT decode(value, 'replace') FROM read_parquet(?, hive_partitioning = true) {where} "  # noqa: S608
        "ORDER BY kafka_ts",
        [str(cold / "topic=ais.raw" / "date=*" / "*.parquet")],
    )
    while batch := cur.fetchmany(10_000):
        for (text,) in batch:
            yield json.loads(text)


def _ship_name(envelope: dict[str, Any]) -> str | None:
    meta = (envelope.get("payload") or {}).get("MetaData") or {}
    name = meta.get("ShipName")
    return name if isinstance(name, str) else None


def collect(
    cold: Path, real: RealData, since: date | None = None, only: date | None = None
) -> tuple[dict[date, dict[tuple[str, int], dict[str, Any]]], dict[date, dict[str, dict[str, Any]]], int]:
    """Positions per event date (deduplicated by MMSI and time), the newest static per ship and date, and how
    many messages were left out as test data."""
    positions: dict[date, dict[tuple[str, int], dict[str, Any]]] = defaultdict(dict)
    statics: dict[date, dict[str, dict[str, Any]]] = defaultdict(dict)
    skipped = 0
    for envelope in envelopes(cold, since, only):
        result = normalize(envelope)
        item = result.position or result.static
        if item is None:
            continue
        p = result.position
        if real.is_test_ship(
            item["mmsi"], _ship_name(envelope), p["lat"] if p else None, p["lon"] if p else None, item["ts"]
        ):
            skipped += 1
            continue
        if p:
            positions[_day(p["ts"])][(p["mmsi"], p["ts"])] = dict(p)
        elif result.static:
            s = result.static
            day = statics[_day(s["ts"])]
            if s["mmsi"] not in day or day[s["mmsi"]]["ts"] <= s["ts"]:
                day[s["mmsi"]] = dict(s)
    return positions, statics, skipped


def _existing(target: Path, columns: list[str]) -> list[dict[str, Any]]:
    if not target.is_file():
        return []
    frame = duckdb.connect().execute("SELECT * FROM read_parquet(?)", [str(target)]).df()
    frame = frame.astype(object).where(frame.notna(), None)
    return [{c: r.get(c) for c in columns} for r in frame.to_dict("records")]


def write(
    root: Path, name: str, day: date, rows: list[dict[str, Any]], columns: list[str], key: tuple[str, ...]
) -> Path:
    """Write the day's file: what it held plus `rows`, one row per key (the newest wins)."""
    folder = root / name / f"date={day.isoformat()}"
    folder.mkdir(parents=True, exist_ok=True)
    target = folder / OUT_NAME
    merged: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in [*_existing(target, columns), *rows]:
        merged[tuple(row[k] for k in key)] = row
    frame = pd.DataFrame(list(merged.values()), columns=columns)
    con = duckdb.connect()
    con.register("rows", frame)
    select = ", ".join(f"TRY_CAST({c} AS {TYPES.get(c, 'VARCHAR')}) AS {c}" for c in columns)
    tmp = target.with_suffix(".tmp")
    con.execute(f"COPY (SELECT {select} FROM rows ORDER BY mmsi, ts) TO '{tmp}' (FORMAT parquet)")  # noqa: S608
    tmp.replace(target)  # readers see the old file or the new one, never half of one
    return target


def _size_mb(paths: list[Path]) -> float:
    return sum(p.stat().st_size for p in paths if p.is_file()) / 1_048_576


def prune_history(history: Path, max_mb: float) -> list[str]:
    """Remove the oldest days of live-feed history until it is under max_mb. Returns the days removed."""
    days: dict[str, list[Path]] = defaultdict(list)
    for name in ("positions", "vessels", "destinations"):
        for f in (history / name).glob(f"date=*/{OUT_NAME}"):
            days[f.parent.name.removeprefix("date=")].append(f)
    removed = []
    total = _size_mb([f for files in days.values() for f in files])
    for day in sorted(days)[:-1]:  # the newest day always stays
        if total <= max_mb:
            break
        total -= _size_mb(days[day])
        for f in days[day]:
            f.unlink()
            if not any(f.parent.iterdir()):
                f.parent.rmdir()
        removed.append(day)
    return removed


def prune_cold(cold: Path, max_mb: float) -> list[str]:
    """Remove the oldest Kafka days of the raw archive until it is under max_mb. Returns the days removed.

    Nothing reads the archive through Spark's sink log (DuckDB does), so a removed day is simply gone."""
    folders = sorted((cold / "topic=ais.raw").glob("date=*"))
    sizes = {f: _size_mb(list(f.glob("*"))) for f in folders}
    total, removed = sum(sizes.values()), []
    for folder in folders[:-1]:
        if total <= max_mb:
            break
        total -= sizes[folder]
        shutil.rmtree(folder)
        removed.append(folder.name.removeprefix("date="))
    return removed


def run(
    data_dir: Path,
    only: date | None = None,
    since: date | None = None,
    history_max_mb: float | None = None,
    cold_max_mb: float | None = None,
) -> dict[str, Any]:
    s = settings()
    real = RealData.load(data_dir, s.live_since)
    positions, statics, skipped = collect(data_dir / "cold", real, since, only)
    history = data_dir / "history"
    counts: dict[str, Any] = {"positions": 0, "vessels": 0, "destinations": 0, "test_data_skipped": skipped}
    for day, by_key in positions.items():
        rows = list(by_key.values())
        write(history, "positions", day, rows, POSITION_COLUMNS, ("mmsi", "ts"))
        counts["positions"] += len(rows)
    for day, by_ship in statics.items():
        rows = list(by_ship.values())
        write(history, "vessels", day, rows, VESSEL_COLUMNS, ("mmsi",))
        dest = [r for r in rows if r.get("destination")]
        if dest:
            write(history, "destinations", day, dest, DESTINATION_COLUMNS, ("mmsi",))
        counts["vessels"] += len(rows)
        counts["destinations"] += len(dest)
    counts["history_days_removed"] = prune_history(
        history, history_max_mb if history_max_mb is not None else s.history_max_mb
    )
    counts["cold_days_removed"] = prune_cold(
        data_dir / "cold", cold_max_mb if cold_max_mb is not None else s.cold_max_mb
    )
    log.info("history written", extra={"data": {**counts, "days": len(positions)}})
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--date", type=date.fromisoformat, help="only messages archived on this Kafka date")
    parser.add_argument(
        "--every",
        type=float,
        nargs="?",
        const=-1,
        help="repeat every this many seconds (the two latest days); no number: HISTORY_EVERY_S",
    )
    args = parser.parse_args()
    data_dir = settings().data_dir
    if not args.every:
        run(data_dir, args.date)
        return
    every = settings().history_every_s if args.every < 0 else args.every
    run(data_dir)  # everything once, then only the recent days: the merge keeps what is already there
    while True:
        time.sleep(every)
        run(data_dir, since=datetime.now(UTC).date() - timedelta(days=1))


if __name__ == "__main__":
    main()
