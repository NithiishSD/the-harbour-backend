"""Report queries over the warehouse. "Now" is the newest loaded date, so a replayed 2023 day has a report."""

import time
from pathlib import Path
from typing import Any

import duckdb

# {scope} is a condition on the port and the day (see _scope); "now" is the newest day inside it.
DAILY = """
WITH f AS (
  SELECT f.port_id, strftime(d.date, '%Y-%m-%d') AS day, d.date, f.calls, f.median_wait_hours,
         f.avg_turnaround_hours, f.max_hourly_queue
  FROM fact_port_daily f JOIN dim_date d USING (date_key)
  WHERE {scope}
)
SELECT port_id, day AS date, calls, median_wait_hours, avg_turnaround_hours, max_hourly_queue
FROM f
WHERE date > (SELECT max(date) FROM f) - ?::INTEGER
ORDER BY date, port_id
"""

# Cargo ships and tankers with an observed arrival, by month of berthing: the population of the daily KPIs.
COMPARE = """
WITH latest AS (
  SELECT date_trunc('month', max(date)) AS month
  FROM fact_port_daily f JOIN dim_date d USING (date_key) WHERE {scope}
),
waits AS (
  SELECT port_id,
         median(wait_hours) FILTER (WHERE date_trunc('month', berthed_at) = (SELECT month FROM latest))
           AS this_month_median,
         median(wait_hours) FILTER (
           WHERE date_trunc('month', berthed_at) = (SELECT month FROM latest) - INTERVAL 1 MONTH
         ) AS last_month_median
  FROM fact_port_call f JOIN dim_date d USING (date_key)
  WHERE arrival_seen AND ship_type IN ('cargo', 'tanker') AND {scope}
  GROUP BY port_id
)
SELECT port_id, this_month_median, last_month_median,
       CASE WHEN last_month_median > 0 AND this_month_median IS NOT NULL
            THEN round((this_month_median - last_month_median) / last_month_median * 100, 1) END AS change_pct
FROM waits
WHERE this_month_median IS NOT NULL OR last_month_median IS NOT NULL
ORDER BY port_id
"""


def _connect_read_only(
    path: Path, max_retries: int = 5, base_delay: float = 0.05
) -> duckdb.DuckDBPyConnection:
    """Connect with retry backoff to handle transient lock contention from concurrent batch writes."""
    for attempt in range(max_retries):
        try:
            return duckdb.connect(str(path), read_only=True)
        except duckdb.Error as err:
            if "lock" in str(err).lower() and attempt < max_retries - 1:
                time.sleep(base_delay * (2**attempt))
                continue
            raise
    return duckdb.connect(str(path), read_only=True)


def _rows(path: Path, sql: str, *args: Any) -> list[dict[str, Any]]:
    """Run a read-only query; a warehouse the nightly job has not created yet has no rows."""
    if not path.is_file():
        return []
    con = _connect_read_only(path)
    try:
        cur = con.execute(sql, list(args))
        names = [c[0] for c in cur.description]
        return [dict(zip(names, row, strict=True)) for row in cur.fetchall()]
    finally:
        con.close()


def _scope(ports: list[str] | None, days: list[str] | None) -> tuple[str, list[Any]]:
    """The condition and its parameters that keep a report to the monitored country: its ports, and only the
    days whose data was received while the feed was set to it. None: not restricted."""
    parts, params = ["TRUE"], []
    if ports is not None:
        parts.append(f"f.port_id IN ({', '.join('?' * len(ports)) or 'NULL'})")
        params += ports
    if days is not None:
        parts.append(f"strftime(d.date, '%Y-%m-%d') IN ({', '.join('?' * len(days)) or 'NULL'})")
        params += days
    return " AND ".join(parts), params


def ports_daily(
    path: Path, days: int, ports: list[str] | None = None, dates: list[str] | None = None
) -> list[dict[str, Any]]:
    scope, params = _scope(ports, dates)
    return _rows(path, DAILY.format(scope=scope), *params, days)


def compare(
    path: Path, ports: list[str] | None = None, dates: list[str] | None = None
) -> list[dict[str, Any]]:
    scope, params = _scope(ports, dates)
    sql = COMPARE.format(scope=scope)
    return _rows(path, sql, *params, *params)  # the latest-month CTE and the waits CTE each take the scope
