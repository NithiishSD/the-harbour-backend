"""Remove from the live store what the monitored country has no use for.

    python -m batch.prune_scope             # dry run: what would go
    python -m batch.prune_scope --apply     # do it

The stores hold whatever was ever received, under whichever settings were in force. For the monitored country
(MONITOR_COUNTRY, with the ports and source countries chosen) this deletes

  HBase vessel_state and vessel_track   ships that are not tracked, were last heard outside the area the
                                        live feed listens to now and have been silent for MAP_SILENT_HOURS
                                        (if one returns it starts as a new ship), and the rows a delete
                                        leaves of a ship (only its source, stamped in the far future)
  HBase port_queue and port_calls       rows of ports of other countries
  warehouse (DuckDB)                    port calls, daily figures and weather rows of other countries' ports

and keeps every ship that is tracked or is inside the live feed's area (it may become relevant), every row of
the country's ports, and the raw archive (data/cold), the replay history and the NOAA files: those are the
source of truth and are cut at read time (common.scopes) instead. Everything deleted here can be rebuilt from
them (replay of the archive, `batch.nightly`). Stop nothing: the deletes are single-row and idempotent.
"""

import argparse
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import duckdb

from common.config import settings
from common.geo import Port, load_world_ports
from common.log import get_logger
from common.monitor import Monitor
from dashboard import live, reasons
from storage.hbase.client import HBaseClient
from storage.hbase.tables import PORT_CALLS, PORT_QUEUE, VESSEL_STATE, VESSEL_TRACK
from streaming.sinks import SRC_TS_BASE

log = get_logger("batch.prune_scope")
THREADS = 16
MAX_ROWS = 10_000_000
MMSI_LEN = 9
SHELL_AGE_MS = 3_600_000  # a row of only a source is a leftover once it is this old (not a ship being added)


def _inside(boxes: tuple[tuple[float, float, float, float], ...], lat: float, lon: float) -> bool:
    return any(a <= lat <= c and b <= lon <= d for a, b, c, d in boxes)


def stale_ships(client: Any, monitor: Monitor, world: dict[str, Port]) -> set[str]:
    """Ships not tracked, last heard outside the live feed's area and silent for longer than the map keeps a
    silent ship (MAP_SILENT_HOURS of feed time): history only. Ships with no position are left."""
    boxes = monitor.ais_boxes(world)
    ships = live.ships(client)
    now = reasons.feed_time(ships)
    silent_ms = settings().map_silent_hours * reasons.HOUR_MS
    gone: set[str] = set()
    for s in ships:
        heard = reasons.ms(s["last_seen"]) or 0
        if now is not None and now - heard <= silent_ms:
            continue
        if monitor.roles(s, world) or _inside(boxes, s["lat"], s["lon"]):
            continue
        gone.add(s["mmsi"])
    return gone


def shells(client: Any, now_ms: int) -> set[str]:
    """Rows that hold nothing but a source: what a row delete leaves of a ship, because the source cell is
    stamped in the far future (write-once, streaming.sinks.SRC_TS_BASE) and a delete only reaches the past."""
    out: set[str] = set()
    for mmsi, row in client.scan_all(VESSEL_STATE, MAX_ROWS):
        if row and all(c.startswith("src:") for c in row):
            try:
                if now_ms - int(float(row.get("src:ts", "0"))) > SHELL_AGE_MS:
                    out.add(mmsi)
            except ValueError:
                out.add(mmsi)
    return out


def _delete_all(client: Any, table: str, keys: list[str]) -> None:
    with ThreadPoolExecutor(THREADS) as pool:
        list(pool.map(lambda k: client.delete_row(table, k), keys))


def _delete_sources(client: Any, mmsis: list[str]) -> None:
    with ThreadPoolExecutor(THREADS) as pool:
        list(pool.map(lambda m: client.delete_family(VESSEL_STATE, m, "src", SRC_TS_BASE + 1), mmsis))


def prune_hbase(client: Any, monitor: Monitor, world: dict[str, Port], apply: bool) -> dict[str, int]:
    out = {"ships": 0, "leftovers": 0, "track_rows": 0, "queue_rows": 0, "call_rows": 0}
    gone = stale_ships(client, monitor, world)
    left = shells(client, int(time.time() * 1000))
    out["ships"], out["leftovers"] = len(gone), len(left - gone)
    tracks = [k for k, _ in client.scan_all(VESSEL_TRACK, MAX_ROWS) if k[:MMSI_LEN] in gone]
    out["track_rows"] = len(tracks)
    own = {p.port_id for p in world.values() if p.country == monitor.country}
    queue = [k for k, _ in client.scan_all(PORT_QUEUE, MAX_ROWS) if k[:5] not in own]
    calls = [k for k, _ in client.scan_all(PORT_CALLS, MAX_ROWS) if k[:5] not in own]
    out["queue_rows"], out["call_rows"] = len(queue), len(calls)
    if apply:
        _delete_all(client, VESSEL_TRACK, tracks)
        _delete_all(client, VESSEL_STATE, sorted(gone))
        _delete_sources(client, sorted(gone | left))
        _delete_all(client, PORT_QUEUE, queue)
        _delete_all(client, PORT_CALLS, calls)
    return out


def prune_warehouse(monitor: Monitor, world: dict[str, Port], apply: bool) -> dict[str, int]:
    path = settings().warehouse_path
    if not path.is_file():
        return {}
    own = [p.port_id for p in world.values() if p.country == monitor.country]
    marks = ", ".join("?" * len(own)) or "NULL"
    out: dict[str, int] = {}
    con = duckdb.connect(str(path), read_only=not apply)
    try:
        for table in ("fact_port_call", "fact_port_daily", "fact_weather_hourly"):
            sql = f"SELECT count(*) FROM {table} WHERE port_id NOT IN ({marks})"  # noqa: S608 - constants
            row = con.execute(sql, own).fetchone()
            out[table] = int(row[0]) if row else 0
            if apply and out[table]:
                con.execute(f"DELETE FROM {table} WHERE port_id NOT IN ({marks})", own)  # noqa: S608
    finally:
        con.close()
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--apply", action="store_true", help="delete (without it only counts)")
    args = parser.parse_args()
    world = load_world_ports()
    monitor = Monitor.selected(world)
    started = time.monotonic()
    hbase = prune_hbase(HBaseClient(timeout=120), monitor, world, args.apply)
    warehouse = prune_warehouse(monitor, world, args.apply)
    log.info(
        "prune",
        extra={
            "data": {
                "country": monitor.country,
                "applied": args.apply,
                "hbase": hbase,
                "warehouse": warehouse,
                "seconds": round(time.monotonic() - started, 1),
            }
        },
    )


if __name__ == "__main__":
    main()
