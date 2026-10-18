"""DuckDB star schema and the idempotent load of one date from the batch Parquet output.

    from storage.warehouse.load import connect, load_date

Sources (all under the data directory)
  batch/port_calls/date=D, batch/port_daily/date=D   written by batch.port_calls_sql
  history/vessels/date=D                             written by batch.backfill_rdd
  cold/topic=weather.marine/date=*                   raw Open-Meteo records archived by the streaming job

Re-loading a date gives the same tables: the date's daily rows, weather hours and calls that arrived on it are
deleted and inserted again in one transaction; ships, ports and dates are upserted.
"""

import glob
from datetime import date
from pathlib import Path

import duckdb

from common.geo import Port

SCHEMA = """
CREATE TABLE IF NOT EXISTS dim_port (
  port_id VARCHAR PRIMARY KEY, name VARCHAR, country VARCHAR, lat DOUBLE, lon DOUBLE);
CREATE TABLE IF NOT EXISTS dim_vessel (
  mmsi VARCHAR PRIMARY KEY, name VARCHAR, ship_type VARCHAR, length_m DOUBLE);
CREATE TABLE IF NOT EXISTS dim_date (
  date_key INTEGER PRIMARY KEY, date DATE, year INTEGER, month INTEGER, day INTEGER, weekday INTEGER);
CREATE TABLE IF NOT EXISTS fact_port_call (
  port_id VARCHAR, mmsi VARCHAR, date_key INTEGER, arrival_at TIMESTAMP, anchored_at TIMESTAMP,
  berthed_at TIMESTAMP, departed_at TIMESTAMP, wait_hours DOUBLE, turnaround_hours DOUBLE,
  ship_type VARCHAR, arrival_seen BOOLEAN, PRIMARY KEY (port_id, mmsi, arrival_at));
CREATE TABLE IF NOT EXISTS fact_port_daily (
  port_id VARCHAR, date_key INTEGER, calls INTEGER, median_wait_hours DOUBLE,
  avg_turnaround_hours DOUBLE, max_hourly_queue INTEGER, PRIMARY KEY (port_id, date_key));
CREATE TABLE IF NOT EXISTS fact_weather_hourly (
  port_id VARCHAR, hour TIMESTAMP, wind_speed_kn DOUBLE, wave_height_m DOUBLE, PRIMARY KEY (port_id, hour));
"""

TS = "make_timestamp({col} * 1000)"  # epoch milliseconds -> TIMESTAMP (UTC)
KEY = "CAST(strftime(make_timestamp({col} * 1000), '%Y%m%d') AS INTEGER)"


def connect(path: Path) -> duckdb.DuckDBPyConnection:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    con.execute(SCHEMA)
    return con


def date_key(day: date) -> int:
    return day.year * 10000 + day.month * 100 + day.day


def _files(pattern: Path) -> list[str]:
    return sorted(glob.glob(str(pattern)))


def load_date(
    con: duckdb.DuckDBPyConnection, day: date, data_dir: Path, ports: dict[str, Port]
) -> dict[str, int]:
    """Load one date; returns the number of rows now held for it per table."""
    key = date_key(day)
    d = day.isoformat()
    calls = _files(data_dir / "batch" / "port_calls" / f"date={d}" / "*.parquet")
    daily = _files(data_dir / "batch" / "port_daily" / f"date={d}" / "*.parquet")
    vessels = _files(data_dir / "history" / "vessels" / f"date={d}" / "*.parquet")
    weather = _files(data_dir / "cold" / "topic=weather.marine" / "date=*" / "*.parquet")

    con.begin()
    try:
        con.executemany(
            "INSERT OR REPLACE INTO dim_port VALUES (?, ?, ?, ?, ?)",
            [(p.port_id, p.name, p.country, p.lat, p.lon) for p in ports.values()],
        )
        con.execute(
            "INSERT OR REPLACE INTO dim_date VALUES (?, ?, year(?::DATE), month(?::DATE), day(?::DATE), "
            "isodow(?::DATE))",
            [key, d, d, d, d, d],
        )
        if vessels:
            con.execute(
                "INSERT OR REPLACE INTO dim_vessel SELECT mmsi, name, ship_type, length_m "
                "FROM read_parquet(?)",
                [vessels],
            )
        con.execute("DELETE FROM fact_port_call WHERE date_key = ?", [key])
        if calls:
            con.execute(
                f"""INSERT OR REPLACE INTO fact_port_call
                SELECT port_id, mmsi, {KEY.format(col="arrival_at")}, {TS.format(col="arrival_at")},
                       {TS.format(col="anchored_at")}, {TS.format(col="berthed_at")},
                       {TS.format(col="departed_at")}, wait_hours, turnaround_hours, ship_type, arrival_seen
                FROM read_parquet(?)""",  # noqa: S608 - only the fixed constants above are formatted in
                [calls],
            )
        con.execute("DELETE FROM fact_port_daily WHERE date_key = ?", [key])
        if daily:
            con.execute(
                "INSERT INTO fact_port_daily SELECT port_id, ?, calls, median_wait_hours, "
                "avg_turnaround_hours, max_hourly_queue FROM read_parquet(?)",
                [key, daily],
            )
        con.execute("DELETE FROM fact_weather_hourly WHERE CAST(hour AS DATE) = ?::DATE", [d])
        if weather:
            # Forecasts are re-published every hour: keep the newest record of each port and hour.
            con.execute(
                """INSERT INTO fact_weather_hourly
                SELECT port_id, hour, wind_speed_kn, wave_height_m FROM (
                  SELECT json_extract_string(rec, '$.port_id') AS port_id,
                         CAST(replace(json_extract_string(rec, '$.hour'), 'Z', '') AS TIMESTAMP) AS hour,
                         TRY_CAST(json_extract_string(rec, '$.wind_speed_kn') AS DOUBLE) AS wind_speed_kn,
                         TRY_CAST(json_extract_string(rec, '$.wave_height_m') AS DOUBLE) AS wave_height_m,
                         row_number() OVER (
                           PARTITION BY json_extract_string(rec, '$.port_id'),
                                        json_extract_string(rec, '$.hour')
                           ORDER BY kafka_ts DESC) AS version
                  FROM (SELECT decode(value) AS rec, kafka_ts FROM read_parquet(?))
                ) WHERE version = 1 AND CAST(hour AS DATE) = ?::DATE""",
                [weather, d],
            )
        con.commit()
    except Exception:
        con.rollback()
        raise
    return {
        "fact_port_call": _count(con, "fact_port_call", "date_key = ?", key),
        "fact_port_daily": _count(con, "fact_port_daily", "date_key = ?", key),
        "fact_weather_hourly": _count(con, "fact_weather_hourly", "CAST(hour AS DATE) = ?::DATE", d),
    }


def _count(con: duckdb.DuckDBPyConnection, table: str, where: str, arg: object) -> int:
    row = con.execute(f"SELECT count(*) FROM {table} WHERE {where}", [arg]).fetchone()  # noqa: S608 - fixed names
    return int(row[0]) if row else 0


def main() -> None:
    import argparse

    from common.config import settings

    parser = argparse.ArgumentParser(description="DuckDB Warehouse operations")
    parser.add_argument("--init", action="store_true", help="Initialize warehouse tables")
    args = parser.parse_args()
    if args.init or len(sys.argv) == 1:
        con = connect(settings().warehouse_path)
        con.close()


if __name__ == "__main__":
    import sys

    main()
