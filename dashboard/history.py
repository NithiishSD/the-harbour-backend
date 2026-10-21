"""Offline history for the dashboard: where the ships were at a past time, and one ship's whole journey.

Reads the clean history Parquet (data/history): NOAA from the backfill (batch.backfill_rdd) and the past
aisstream data from batch.archive_history. Nothing here touches Kafka or HBase, so the offline mode
works with the live stack switched off.

- snapshot: the newest report of each ship in the hour before the time asked for, in the shape of /api/ships.
  Its state comes from the position alone (zones, speed, nav status), without the hysteresis the streaming job
  has: a snapshot has no memory of the minutes before.
- journey: every report of one ship, run through the same state machine as the live job (streaming.state), so
  the anchored / berthed / departed events and the waits are the live rules. Gaps in the reports and stops at
  sea are added as events.
"""

# ruff: noqa: S608 - the only text formatted into SQL is a Parquet glob built from our own data folder

import threading
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from common.geo import Port, ZoneIndex, haversine_km
from dashboard import crossing, destination, voyage
from dashboard.live import NEAREST_PORT_KM, iso
from streaming.state import ANCHORED, BERTHED, UNDERWAY, apply_positions, classify

HOUR_MS = 3_600_000
SNAPSHOT_WINDOW_MS = HOUR_MS  # a ship silent for longer is not on the map at that time
GAP_MS = HOUR_MS  # reports further apart than this are a signal gap
STOP_MS = 30 * 60_000  # slower than STOP_SOG for this long, outside a berth or anchorage: a stop at sea
STOP_SOG = 0.5
THIN_S, THIN_M = 600, 50  # keep a point every 10 minutes or 50 m, so a ship at a quay is a few points
MAX_TRACK_POINTS = 3000
POSITION_COLUMNS = "mmsi, ts, lat, lon, sog, cog, heading, nav_status, source"


def event(
    ts: int, kind: str, lat: float, lon: float, text: str, port_id: str | None = None
) -> dict[str, Any]:
    return {"ts": ts, "type": kind, "lat": lat, "lon": lon, "port_id": port_id, "text": text}


def day_of(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).date().isoformat()


class HistoryStore:
    def __init__(
        self,
        root: Path,
        zones: ZoneIndex,
        ports: dict[str, Port],
        scope: Callable[[], str | None] | None = None,
    ) -> None:
        """`scope` returns, per query, an SQL condition on `ts` and `source` that the positions must meet (the
        monitored country's data: common.scopes), or None for no restriction."""
        self.scope = scope
        self.root = root
        self.zones = zones
        self.ports = ports
        temp_dir = Path("/tmp/duckdb")  # noqa: S108
        temp_dir.mkdir(parents=True, exist_ok=True)
        self._con = duckdb.connect(config={"temp_directory": str(temp_dir)})
        self._lock = threading.Lock()  # one query at a time on the shared connection

    # ---- helpers --------------------------------------------------------------------------------

    def _glob(self, name: str) -> str | None:
        """Parquet glob of a history folder, or None while it holds no file."""
        folder = self.root / name
        return str(folder / "date=*" / "*.parquet") if any(folder.glob("date=*/*.parquet")) else None

    def _query(self, sql: str, params: list[Any]) -> list[tuple[Any, ...]]:
        with self._lock:
            return self._con.execute(sql, params).fetchall()

    def _read(self, name: str) -> str | None:
        glob = self._glob(name)
        if not glob:
            return None
        source = f"read_parquet('{glob}', hive_partitioning = true, union_by_name = true)"
        keep = self.scope() if self.scope and name == "positions" else None
        return f"(SELECT * FROM {source} WHERE {keep})" if keep else source

    def _port_name(self, port_id: str | None) -> str:
        return self.ports[port_id].name if port_id in self.ports else (port_id or "open water")

    def _nearest(self, lat: float, lon: float) -> Port | None:
        port = min(self.ports.values(), key=lambda p: haversine_km(lat, lon, p.lat, p.lon), default=None)
        if port and haversine_km(lat, lon, port.lat, port.lon) <= NEAREST_PORT_KM:
            return port
        return None

    # ---- what is there --------------------------------------------------------------------------

    def dates(self) -> list[dict[str, Any]]:
        """One row per day with data: ships, positions, first and last report (epoch ms) and sources."""
        positions = self._read("positions")
        if positions is None:
            return []
        rows = self._query(
            "SELECT CAST(date AS VARCHAR), count(*), count(DISTINCT mmsi), min(ts), max(ts), "
            f"list(DISTINCT source ORDER BY source) FROM {positions} GROUP BY date ORDER BY date",
            [],
        )
        return [
            {"date": d, "positions": n, "ships": ships, "first_ts": first, "last_ts": last, "sources": src}
            for d, n, ships, first, last, src in rows
        ]

    # ---- snapshot -------------------------------------------------------------------------------

    def snapshot(
        self, at_ms: int, window_ms: int = SNAPSHOT_WINDOW_MS, limit: int = 5000, mmsi: str | None = None
    ) -> list[dict[str, Any]]:
        """With `mmsi`: only that ship, its newest report at or before the time whatever its age (a journey
        replay follows one ship through silences)."""
        positions = self._read("positions")
        if positions is None:
            return []
        # Ships last heard before the window are read too: one on a long voyage out of AIS range gets an
        # estimated position (dashboard.crossing); the rest are not on the map.
        start = 0 if mmsi else at_ms - max(window_ms, crossing.MAX_AGE_MS)
        window_start = at_ms - window_ms
        days = sorted({day_of(max(start, 0)), day_of(at_ms)})
        only = "AND mmsi = ?" if mmsi else ""
        day_filter = "" if mmsi else "CAST(date AS VARCHAR) BETWEEN ? AND ? AND"
        params: list[Any] = [start, at_ms, mmsi, limit] if mmsi else [days[0], days[-1], start, at_ms, limit]
        vessels, dests = self._read("vessels"), self._read("destinations")
        info = (
            "LEFT JOIN (SELECT mmsi, arg_max(name, ts) AS name, arg_max(ship_type, ts) AS ship_type, "
            f"arg_max(length_m, ts) AS length_m FROM {vessels} GROUP BY mmsi) v USING (mmsi)"
            if vessels
            else ""
        )
        declared = (
            f"LEFT JOIN (SELECT mmsi, arg_max(destination, ts) AS dest FROM {dests} "
            "GROUP BY mmsi) d USING (mmsi)"
            if dests
            else ""
        )
        cols = "v.name, v.ship_type, v.length_m" if vessels else "NULL, NULL, NULL"
        cols += ", d.dest" if dests else ", NULL"
        rows = self._query(
            f"""SELECT p.mmsi, p.ts, p.lat, p.lon, p.sog, p.cog, p.heading, p.nav_status, p.source, {cols}
            FROM (
              SELECT {POSITION_COLUMNS} FROM {positions}
              WHERE {day_filter} ts BETWEEN ? AND ? {only}
              QUALIFY row_number() OVER (PARTITION BY mmsi ORDER BY ts DESC) = 1
            ) p {info} {declared}
            ORDER BY p.ts DESC LIMIT ?""",
            params,
        )
        one_ship = mmsi is not None  # the loop below reuses the name for each row's ship
        ships = []
        for mmsi, ts, lat, lon, sog, cog, heading, nav, _source, name, kind, length, dest in rows:
            zone = self.zones.lookup(lat, lon)
            state, _ = classify(zone, nav, sog, UNDERWAY, None)
            ship = {
                "mmsi": mmsi,
                "name": name,
                "ship_type": kind,
                "length_m": length,
                "state": state,
                "port": zone.port_id if zone else None,
                "since": None,
                "last_seen": iso(ts),
                "first_seen": iso(ts),
                "lat": lat,
                "lon": lon,
                "sog": sog,
                "cog": cog,
                "heading": heading,
                "destination": dest,
                "eta": None,
                "reasons": [],
                "map": "live",
            }
            if ts < window_start and not one_ship:  # silent for longer than the window
                found = crossing.estimate(ship, at_ms, self.ports)
                if found is None:
                    continue
                ship.update(found)
            ships.append(ship)
        return ships[:limit]

    # ---- journey --------------------------------------------------------------------------------

    def journey(
        self, mmsi: str, until_ms: int | None = None, stop_at_arrival: bool = True
    ) -> dict[str, Any] | None:
        """The ship's reports up to until_ms (or arrival at a berth), with the events along the way."""
        positions = self._read("positions")
        if positions is None:
            return None
        where = "mmsi = ?"
        params: list[Any] = [mmsi]
        if until_ms is not None:
            where += " AND ts <= ?"
            params.append(until_ms)
        rows = self._query(
            f"SELECT ts, lat, lon, sog, cog, heading, nav_status FROM {positions} WHERE {where} ORDER BY ts",
            params,
        )
        if not rows:
            return None
        by_time = {  # one report per time
            ts: {"ts": ts, "lat": lat, "lon": lon, "sog": sog, "cog": cog, "heading": hd, "nav_status": nav}
            for ts, lat, lon, sog, cog, hd, nav in rows
        }
        pts = list(by_time.values())
        name, kind, length = self._details(mmsi)
        declared = self._declared(mmsi, pts[-1]["ts"])

        _, raw = apply_positions(None, mmsi, pts, self.zones)
        events = self._events(pts, raw)
        arrived = self._arrival(pts, raw)
        end_ts = arrived["ts"] if arrived and stop_at_arrival else pts[-1]["ts"]
        pts = [p for p in pts if p["ts"] <= end_ts]
        events = [e for e in events if e["ts"] <= end_ts]
        if arrived:
            where_to = self._port_name(arrived["port_id"])
            events.append(
                event(
                    arrived["ts"],
                    "arrived",
                    arrived["lat"],
                    arrived["lon"],
                    f"Arrived: berthed at {where_to}",
                    arrived["port_id"],
                )
            )
        events.sort(key=lambda e: e["ts"])

        first, last = pts[0], pts[-1]
        left = voyage.departure([(p["ts"], p["lat"], p["lon"]) for p in pts], self.ports.values())
        origin_zone = self.zones.lookup(first["lat"], first["lon"])
        origin_port = self.ports.get(origin_zone.port_id) if origin_zone else None
        origin_basis = "port area"
        if origin_port is None:  # a first report within 60 km of a port is "at" it
            near = min(
                self.ports.values(), key=lambda p: haversine_km(first["lat"], first["lon"], p.lat, p.lon)
            )
            if haversine_km(first["lat"], first["lon"], near.lat, near.lon) <= 60:
                origin_port, origin_basis = near, "near port"
        if left is not None:  # the voyage began where the ship last stood at a port and left it
            origin_port, i = left
            origin = {
                **self._place(origin_port, pts[i], "left this port"),
                "at": iso(pts[i]["ts"]),
                "known": True,
                "voyage_from": pts[i]["ts"],
            }
        else:
            origin = {
                **self._place(origin_port, first, origin_basis if origin_port else "first heard here"),
                "at": iso(first["ts"]),
                "known": origin_port is not None,
                "voyage_from": first["ts"],
            }
        dest_port, basis = None, None
        named = destination.match(declared, self.ports)
        if named:
            dest_port, basis = self.ports[named], "declared"
        elif arrived and arrived["port_id"] in self.ports:
            dest_port, basis = self.ports[arrived["port_id"]], "arrival"
        else:
            dest_port, basis = self._nearest(last["lat"], last["lon"]), "nearest port"
        return {
            "mmsi": mmsi,
            "name": name,
            "ship_type": kind,
            "length_m": length,
            "declared": declared,
            "origin": origin,
            "destination": self._place(dest_port, last, basis) if dest_port else None,
            "start_ts": first["ts"],
            "end_ts": last["ts"],
            "reached": arrived is not None,
            "arrived_at": arrived["ts"] if arrived else None,
            "track": self._thin([(p["ts"], p["lat"], p["lon"], p["sog"], p["cog"]) for p in pts]),
            "events": events,
        }

    def _details(self, mmsi: str) -> tuple[str | None, str | None, float | None]:
        vessels = self._read("vessels")
        if vessels is None:
            return None, None, None
        rows = self._query(
            "SELECT arg_max(name, ts) FILTER (WHERE name IS NOT NULL), arg_max(ship_type, ts), "
            f"arg_max(length_m, ts) FROM {vessels} WHERE mmsi = ?",
            [mmsi],
        )
        return rows[0] if rows and rows[0][1] is not None else (None, None, None)

    def _declared(self, mmsi: str, until_ms: int) -> str | None:
        dests = self._read("destinations")
        if dests is None:
            return None
        rows = self._query(
            f"SELECT arg_max(destination, ts) FROM {dests} WHERE mmsi = ? AND ts <= ?",
            [mmsi, until_ms],
        )
        return rows[0][0] if rows else None

    def _place(self, port: Port | None, point: dict[str, Any], basis: str | None) -> dict[str, Any]:
        if port is None:
            return {"port_id": None, "name": None, "lat": point["lat"], "lon": point["lon"], "basis": basis}
        return {"port_id": port.port_id, "name": port.name, "lat": port.lat, "lon": port.lon, "basis": basis}

    @staticmethod
    def _thin(points: list[tuple[Any, ...]]) -> list[list[Any]]:
        keep = [points[0]]
        for p in points[1:-1]:
            far = haversine_km(keep[-1][1], keep[-1][2], p[1], p[2]) * 1000 >= THIN_M
            if p[0] - keep[-1][0] >= THIN_S * 1000 or far:
                keep.append(p)
        if len(points) > 1:
            keep.append(points[-1])
        if len(keep) > MAX_TRACK_POINTS:
            stride = -(-len(keep) // MAX_TRACK_POINTS)
            keep = keep[::stride] + [keep[-1]]
        return [list(p) for p in keep]

    def _events(self, pts: list[dict[str, Any]], raw: list[dict[str, Any]]) -> list[dict[str, Any]]:
        first = pts[0]
        out = [event(first["ts"], "first_seen", first["lat"], first["lon"], "First report")]
        text = {
            ANCHORED: "Anchored at {port}: waiting",
            BERTHED: "Berthed at {port}",
            "departed": "Left the berth at {port}",
            UNDERWAY: "Under way",
        }
        waits = []  # after the state events: the berthing comes first, then how long it waited for it
        for e in raw:
            port = self._port_name(e["port_id"])
            if e["type"] == "state":
                state = e["data"]["state"]
                out.append(
                    event(
                        e["ts"],
                        state,
                        e["lat"],
                        e["lon"],
                        text.get(state, state).format(port=port),
                        e["port_id"],
                    )
                )
            elif e["type"] == "port_call" and e["data"].get("wait_hours") is not None:
                said = f"Waited {e['data']['wait_hours']:.1f} h at anchor before berthing at {port}"
                waits.append(event(e["ts"], "waited", e["lat"], e["lon"], said, e["port_id"]))
        out += waits
        # Silence between two reports, and slow stretches outside any berth or anchorage.
        for a, b in zip(pts, pts[1:], strict=False):
            if b["ts"] - a["ts"] > GAP_MS:
                said = f"Signal lost for {(b['ts'] - a['ts']) / HOUR_MS:.1f} h"
                out.append(event(a["ts"], "signal_lost", a["lat"], a["lon"], said))
        stop_from: dict[str, Any] | None = None
        for p in pts:
            zone = self.zones.lookup(p["lat"], p["lon"])
            slow = p["sog"] is not None and p["sog"] < STOP_SOG and (zone is None or zone.kind == "area")
            if slow and stop_from is None:
                stop_from = p
            elif not slow and stop_from is not None:
                if p["ts"] - stop_from["ts"] >= STOP_MS:
                    out.append(self._stop(stop_from, p["ts"]))
                stop_from = None
        if stop_from is not None and pts[-1]["ts"] - stop_from["ts"] >= STOP_MS:
            out.append(self._stop(stop_from, pts[-1]["ts"]))
        return out

    @staticmethod
    def _stop(start: dict[str, Any], end_ts: int) -> dict[str, Any]:
        hours = (end_ts - start["ts"]) / HOUR_MS
        said = f"Stopped for {hours:.1f} h (under {STOP_SOG} kn, outside a berth or anchorage)"
        return event(start["ts"], "stopped", start["lat"], start["lon"], said)

    @staticmethod
    def _arrival(pts: list[dict[str, Any]], raw: list[dict[str, Any]]) -> dict[str, Any] | None:
        """The first berthing after the ship has been seen away from a berth. A ship that starts the data
        already berthed has not arrived yet: its journey begins when it leaves."""
        states = [e for e in raw if e["type"] == "state"]
        away = not (states and states[0]["data"]["state"] == BERTHED and states[0]["ts"] == pts[0]["ts"])
        for e in states:
            if e["data"]["state"] == BERTHED and away:
                return {"ts": e["ts"], "lat": e["lat"], "lon": e["lon"], "port_id": e["port_id"]}
            if e["data"]["state"] != BERTHED:
                away = True
        return None
