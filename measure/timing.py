"""How often do ships really report, and how long are they silent? Numbers to set the monitor's timings by.

    python -m measure.timing                    # every day in data/history
    python -m measure.timing --source noaa      # or aisstream

For each source (NOAA's files, the live feed's archive) and for ships standing still (SOG < 0.5 kn) and moving
(SOG >= 3 kn): the report interval of one ship (median, 90th and 99th percentile, seconds), and how many
standing-still ships went quiet for longer than each of 1, 3, 6, 12 and 24 hours between two reports. The
second table is what a dark-ship timeout trades: a short one flags many ships that only left coverage.
"""

import argparse
from typing import Any

import duckdb

from common.config import settings

GAP_HOURS = (1, 3, 6, 12, 24)


def intervals(con: duckdb.DuckDBPyConnection, glob: str, source: str | None) -> list[tuple[Any, ...]]:
    where = "WHERE source = ?" if source else ""
    sql = f"""
    WITH p AS (
      SELECT mmsi, ts, sog, source,
             ts - lag(ts) OVER (PARTITION BY mmsi, source ORDER BY ts) AS dt_ms,
             lag(sog) OVER (PARTITION BY mmsi, source ORDER BY ts) AS prev_sog
      FROM read_parquet(?, hive_partitioning = true, union_by_name = true) {where}
    )
    SELECT source,
           CASE WHEN sog < 0.5 AND prev_sog < 0.5 THEN 'standing still'
                WHEN sog >= 3 AND prev_sog >= 3 THEN 'moving' END AS motion,
           count(*) AS n,
           round(quantile_cont(dt_ms / 1000.0, 0.5), 1) AS median_s,
           round(quantile_cont(dt_ms / 1000.0, 0.9), 1) AS p90_s,
           round(quantile_cont(dt_ms / 1000.0, 0.99), 1) AS p99_s
    FROM p
    WHERE dt_ms > 0 AND sog IS NOT NULL AND prev_sog IS NOT NULL
    GROUP BY source, motion
    HAVING motion IS NOT NULL
    ORDER BY source, motion
    """  # noqa: S608 - the only formatted part is a constant
    return con.execute(sql, [glob, *([source] if source else [])]).fetchall()


def gaps(con: duckdb.DuckDBPyConnection, glob: str, source: str | None) -> list[tuple[Any, ...]]:
    where = "WHERE source = ?" if source else ""
    counts = ", ".join(
        f"count(DISTINCT mmsi) FILTER (WHERE dt_ms > {h * 3_600_000}) AS ships_over_{h}h" for h in GAP_HOURS
    )
    sql = f"""
    WITH p AS (
      SELECT mmsi, source, sog,
             ts - lag(ts) OVER (PARTITION BY mmsi, source ORDER BY ts) AS dt_ms,
             lag(sog) OVER (PARTITION BY mmsi, source ORDER BY ts) AS prev_sog
      FROM read_parquet(?, hive_partitioning = true, union_by_name = true) {where}
    )
    SELECT source, count(DISTINCT mmsi) AS ships, {counts}
    FROM p WHERE sog < 0.5 AND prev_sog < 0.5 AND dt_ms > 0
    GROUP BY source ORDER BY source
    """  # noqa: S608 - constants only
    return con.execute(sql, [glob, *([source] if source else [])]).fetchall()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--source", choices=["noaa", "aisstream"])
    args = parser.parse_args()
    glob = str(settings().data_dir / "history" / "positions" / "date=*" / "*.parquet")
    con = duckdb.connect()
    print("report interval of one ship (seconds)")
    print(f"{'source':10} {'motion':15} {'reports':>10} {'median':>8} {'p90':>8} {'p99':>8}")
    for source, motion, n, med, p90, p99 in intervals(con, glob, args.source):
        print(f"{source:10} {motion:15} {n:>10} {med:>8} {p90:>8} {p99:>8}")
    print("\nstanding-still ships that were quiet for longer than ... between two reports")
    heads = "".join(f"{f'>{h} h':>9}" for h in GAP_HOURS)
    print(f"{'source':10} {'ships':>8}{heads}")
    for source, ships, *over in gaps(con, glob, args.source):
        print(f"{source:10} {ships:>8}" + "".join(f"{v:>9}" for v in over))


if __name__ == "__main__":
    main()
