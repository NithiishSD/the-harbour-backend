"""HBase writers used by the streaming job's foreachBatch sinks.

Row keys and cell timestamps come from the message itself (MMSI + event time), so re-processing a
micro-batch after a crash overwrites the same cells instead of adding duplicates.
"""

import time
import uuid
from collections.abc import Callable, Iterable, Iterator
from datetime import UTC, datetime
from itertools import islice
from typing import Any

from common import topics
from common.log import get_logger
from ingest.replay import Sender
from storage.hbase.client import HBaseClient, HBaseError, Row
from storage.hbase.tables import (
    AREA_ACTIVITY,
    PORT_CALLS,
    PORT_QUEUE,
    VESSEL_STATE,
    VESSEL_TRACK,
    _check_port,
    area_activity_key,
    port_call_key,
    port_queue_key,
    track_key,
)

log = get_logger("streaming.sinks")

CHUNK = 500
Put = tuple[str, Row, int | None]


def _fmt(value: Any) -> str | None:
    return None if value is None else str(value)


def track_put(p: dict[str, Any]) -> Put:
    cells = {
        "pos:lat": _fmt(p["lat"]),
        "pos:lon": _fmt(p["lon"]),
        "pos:sog": _fmt(p.get("sog")),
        "pos:cog": _fmt(p.get("cog")),
        "pos:heading": _fmt(p.get("heading")),
        "pos:nav": _fmt(p.get("nav_status")),
    }
    return track_key(p["mmsi"], p["ts"]), {k: v for k, v in cells.items() if v is not None}, p["ts"]


def info_put(s: dict[str, Any]) -> Put:
    cells = {
        "info:name": _fmt(s.get("name")),
        "info:type": _fmt(s.get("ship_type")),
        "info:length": _fmt(s.get("length_m")),
        "info:draught": _fmt(s.get("draught_m")),
        "info:imo": _fmt(s.get("imo")),
        # Only aisstream sends these; a NOAA row (None) leaves a stored live destination alone.
        "info:dest": _fmt(s.get("destination")),
        "info:eta": _fmt(s.get("eta")),
    }
    return s["mmsi"], {k: v for k, v in cells.items() if v is not None}, s["ts"]


def chunks(items: Iterable[Put], size: int = CHUNK) -> Iterator[list[Put]]:
    it = iter(items)
    while batch := list(islice(it, size)):
        yield batch


def write_puts(table: str, puts: Iterable[Put], client: HBaseClient | None = None) -> int:
    client = client or HBaseClient()
    return sum(client.put_rows(table, batch) for batch in chunks(puts))


def write_track_partition(rows: Iterable[Any]) -> None:
    """foreachPartition body: each Spark partition writes its own rows (writes scale with partitions)."""
    write_puts(VESSEL_TRACK, (track_put(r.asDict()) for r in rows))


def write_info(rows: Iterable[dict[str, Any]], client: HBaseClient | None = None) -> int:
    return write_puts(VESSEL_STATE, (info_put(r) for r in rows), client)


# ---- state-machine events ----------------------------------------------------------------------

PORT_EVENT_TYPES = {"anchored", "berthed", "departed"}
CALL_FIELDS = ("anchored_at", "berthed_at", "departed_at", "wait_hours", "turnaround_hours")
# A dark-ship candidate counts as real only if other ships in its area reported during the silence.
# The upper bound keeps ships from unrelated time ranges (old replays) out of the count.
ACTIVE_SLACK_MS = 24 * 3_600_000


def event_id(*parts: Any) -> str:
    """Deterministic id: re-processing a micro-batch yields the same id, so consumers can de-duplicate."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, "/".join(str(p) for p in parts)))


def iso(ms: int | None) -> str | None:
    return None if ms is None else datetime.fromtimestamp(ms / 1000, UTC).isoformat(timespec="seconds")


SRC_TS_BASE = 4_102_444_800_000  # 2100-01-01: a cell timestamp the real ones never reach


def state_puts(events: list[dict[str, Any]]) -> tuple[list[Put], list[Put]]:
    """vessel_state and port_calls rows for one batch of state-machine events."""
    state_rows: list[Put] = []
    call_rows: list[Put] = []
    for e in events:
        d, ts = e["data"], e["ts"]
        if e["type"] == "state":
            cells = {"cur:state": d["state"], "cur:port": e["port_id"], "cur:since": str(d["since"])}
            state_rows.append((e["mmsi"], {k: v for k, v in cells.items() if v is not None}, ts))
        elif e["type"] == "last":
            cells = {
                "last:ts": str(ts),
                "last:lat": _fmt(e["lat"]),
                "last:lon": _fmt(e["lon"]),
                "last:sog": _fmt(d.get("sog")),
                "last:cog": _fmt(d.get("cog")),
                "last:heading": _fmt(d.get("heading")),
                "last:area": e["port_id"] or "",
                "last:state": d.get("state"),
                "last:first": _fmt(d.get("first_seen")),
            }
            state_rows.append((e["mmsi"], {k: v for k, v in cells.items() if v is not None}, ts))
            if d.get("src_lat") is not None and d.get("first_seen", -1) >= 0:
                # The source is kept once: HBase shows the cell with the highest timestamp, so the earliest
                # first report gets the highest one and a ship that returns after its state was forgotten
                # keeps it.
                src = {
                    "src:lat": _fmt(d["src_lat"]),
                    "src:lon": _fmt(d["src_lon"]),
                    "src:ts": str(d["first_seen"]),
                }
                state_rows.append(
                    (
                        e["mmsi"],
                        {k: v for k, v in src.items() if v is not None},
                        SRC_TS_BASE - d["first_seen"],
                    )
                )
        elif e["type"] == "port_call" and e["port_id"] and d.get("arrival_at") is not None:
            cells = {f"call:{k}": _fmt(d.get(k)) for k in CALL_FIELDS}
            key = port_call_key(e["port_id"], d["arrival_at"], e["mmsi"])
            call_rows.append((key, {k: v for k, v in cells.items() if v is not None}, ts))
    return state_rows, call_rows


def activity_puts(events: list[dict[str, Any]]) -> list[Put]:
    """area_activity secondary index rows for vessels reporting in monitored port areas."""
    rows: list[Put] = []
    for e in events:
        if e["type"] == "last" and e.get("port_id") and e.get("mmsi") and e.get("ts") is not None:
            try:
                key = area_activity_key(e["port_id"], e["ts"], e["mmsi"])
                rows.append((key, {"act:ts": str(e["ts"])}, e["ts"]))
            except (ValueError, TypeError):
                continue
    return rows


def active_ships(client: HBaseClient, area: str, mmsi: str, since_ms: int, until_ms: int) -> int:
    """Count active vessels in the given port area during (since_ms, until_ms].

    Uses the AREA_ACTIVITY secondary index table (prefix scan by area) when available,
    falling back to full VESSEL_STATE table scan for backward compatibility with simple test stubs.
    """
    if not area:
        return 0
    if hasattr(client, "scan_prefix"):
        try:
            prefix = _check_port(area)
            rows = client.scan_prefix(AREA_ACTIVITY, prefix, limit=10_000)
            if rows:
                seen_ships: set[str] = set()
                for key, cells in rows:
                    if len(key) >= 33:
                        ship_mmsi = key[-9:]
                        ts = int(cells.get("act:ts", "0"))
                        if ship_mmsi != mmsi and since_ms < ts <= until_ms:
                            seen_ships.add(ship_mmsi)
                return len(seen_ships)
        except (HBaseError, KeyError, ValueError, AttributeError) as err:
            log.debug("area_activity index scan fallback: %s", err)

    count = 0
    for key, row in client.scan_all(VESSEL_STATE, limit=100_000):
        if key == mmsi or row.get("last:area") != area:
            continue
        if since_ms < int(row.get("last:ts", "0")) <= until_ms:
            count += 1
    return count


def handle_events(
    events: list[dict[str, Any]],
    sender: Sender,
    client: HBaseClient,
    dark_timeout_ms: int,
    min_active: int,
    now_ms: int | None = None,
) -> dict[str, int]:
    """Write one batch of state-machine events everywhere they belong. Returns counts per outcome."""
    now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    state_rows, call_rows = state_puts(events)
    act_rows = activity_puts(events)
    write_puts(VESSEL_STATE, state_rows, client)  # before the dark check, which reads last:ts
    write_puts(PORT_CALLS, call_rows, client)
    if act_rows:
        write_puts(AREA_ACTIVITY, act_rows, client)
    counts = {"state_rows": len(state_rows), "port_calls": len(call_rows), "alerts": 0, "port_events": 0}
    for e in events:
        kind, d = e["type"], e["data"]
        base = {
            "mmsi": e["mmsi"],
            "port_id": e["port_id"],
            "event_time": iso(e["ts"]),
            "lat": e["lat"],
            "lon": e["lon"],
        }
        if kind == "state" and d["state"] in PORT_EVENT_TYPES:
            port_event = {"event_id": event_id(e["mmsi"], d["state"], e["ts"]), "type": d["state"], **base}
            sender.send(topics.PORT_EVENTS, e["mmsi"], port_event)
            counts["port_events"] += 1
        elif kind == "reappeared":
            back = {"event_id": event_id(e["mmsi"], kind, e["ts"]), "type": kind, **base, "details": d}
            sender.send(topics.PORT_EVENTS, e["mmsi"], back)
            counts["port_events"] += 1
        elif kind == "dark_candidate":
            area = e["port_id"]
            active = active_ships(
                client, area, e["mmsi"], e["ts"], e["ts"] + dark_timeout_ms + ACTIVE_SLACK_MS
            )
            details = {**d, "active_ships": active}
            if area and active >= min_active:
                alert = {
                    "alert_id": event_id("dark_ship", e["mmsi"], e["ts"]),
                    "type": "dark_ship",
                    **base,
                    "emitted_at": iso(now_ms),
                    "details": details,
                }
                sender.send(topics.ALERTS, "dark_ship", alert)
                counts["alerts"] += 1
            else:
                gap_id = event_id("coverage_gap", e["mmsi"], e["ts"])
                gap = {"event_id": gap_id, "type": "coverage_gap", **base, "details": details}
                sender.send(topics.PORT_EVENTS, e["mmsi"], gap)
                counts["port_events"] += 1
    sender.flush()
    return counts


# ---- port queue windows ------------------------------------------------------------------------

WINDOW_MS = 30 * 60_000


def queue_puts(rows: list[dict[str, Any]]) -> list[Put]:
    return [
        (
            port_queue_key(r["port_id"], r["window_end"]),
            {
                "q:window_start": str(r["window_start"]),
                "q:window_end": str(r["window_end"]),
                "q:ships": str(r["ships"]),
            },
            r["window_end"],
        )
        for r in rows
    ]


def queue_alerts(
    rows: list[dict[str, Any]],
    previous: Callable[[str, int], int | None],
    pct: float,
    min_ships: int,
    now_ms: int,
) -> list[dict[str, Any]]:
    """queue_growth alerts: a window's count against the window that ended 30 minutes earlier."""
    alerts = []
    for r in rows:
        before = previous(r["port_id"], r["window_end"] - WINDOW_MS)
        if not before:
            continue  # no earlier window to compare with
        growth = r["ships"] - before
        if growth >= min_ships and growth * 100 >= pct * before:
            alerts.append(
                {
                    "alert_id": event_id("queue_growth", r["port_id"], r["window_end"]),
                    "type": "queue_growth",
                    "mmsi": None,
                    "port_id": r["port_id"],
                    "event_time": iso(r["window_end"]),
                    "emitted_at": iso(now_ms),
                    "lat": None,
                    "lon": None,
                    "details": {
                        "ships": r["ships"],
                        "previous": before,
                        "growth_pct": round(growth * 100 / before, 1),
                    },
                }
            )
    return alerts


def handle_queue(
    rows: list[dict[str, Any]],
    sender: Sender,
    client: HBaseClient,
    pct: float,
    min_ships: int,
    now_ms: int | None = None,
) -> int:
    write_puts(PORT_QUEUE, queue_puts(rows), client)
    in_batch = {(r["port_id"], r["window_end"]): r["ships"] for r in rows}

    def previous(port_id: str, window_end: int) -> int | None:
        if (port_id, window_end) in in_batch:
            return int(in_batch[(port_id, window_end)])
        row = client.get(PORT_QUEUE, port_queue_key(port_id, window_end))
        return int(row["q:ships"]) if row else None

    alerts = queue_alerts(
        rows, previous, pct, min_ships, now_ms if now_ms is not None else int(time.time() * 1000)
    )
    for alert in alerts:
        sender.send(topics.ALERTS, "queue_growth", alert)
    sender.flush()
    return len(alerts)
