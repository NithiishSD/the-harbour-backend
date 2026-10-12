"""Live data for the API: reads HBase rows and shapes them into the JSON the dashboard uses.

Times leave here as ISO-8601 UTC strings with milliseconds. Cells missing in HBase become null.

vessel_state layout (see streaming/sinks.py): `last:*` is rewritten with every batch (latest position and
current state), `cur:*` only when the state changes (history), `info:*` holds name and type.
"""

from datetime import UTC, datetime
from typing import Any

from common import lanes
from common.config import settings
from common.geo import Port, ZoneIndex, haversine_km
from common.schema import MAX_AHEAD
from dashboard import destination, voyage, waterway
from storage.hbase.client import HBaseClient, Row
from storage.hbase.tables import PORT_CALLS, PORT_QUEUE, VESSEL_STATE, VESSEL_TRACK, unrev

MAX_SHIPS = 100_000  # rows read per vessel_state scan
PLACE_KM = 200  # a place with no port of its own is named after a port at most this far away


def iso(ms: int | None) -> str | None:
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _num(row: Row, column: str) -> float | None:
    try:
        return float(row[column])
    except (KeyError, ValueError):
        return None


def _int(row: Row, column: str) -> int | None:
    value = _num(row, column)
    return None if value is None else int(value)


def _time(row: Row, column: str) -> str | None:
    return iso(_int(row, column))


def ships(client: HBaseClient) -> list[dict[str, Any]]:
    """Every ship with a known position, most recently seen first.

    Rows stamped in the future (written before the ingest check existed) are left out: they would set the
    map's clock and send every real ship to history.
    """
    out = []
    latest = (datetime.now(UTC) + MAX_AHEAD).timestamp() * 1000
    for mmsi, row in client.scan_all(VESSEL_STATE, MAX_SHIPS):
        lat, lon = _num(row, "last:lat"), _num(row, "last:lon")
        if lat is None or lon is None:
            continue  # only static details so far
        if (_num(row, "last:ts") or 0) > latest:
            continue
        state = row.get("last:state") or "underway"
        out.append(
            {
                "mmsi": mmsi,
                "name": row.get("info:name"),
                "ship_type": row.get("info:type"),
                "length_m": _num(row, "info:length"),
                "state": state,
                "port": row.get("last:area") or None,
                # cur:* holds the last state *change*; it only describes the current state when they match.
                "since": _time(row, "cur:since") if row.get("cur:state") == state else None,
                "last_seen": _time(row, "last:ts"),
                # first report since the stream (re)started tracking it; older rows lack it
                "first_seen": _time(row, "last:first"),
                "src_lat": _num(row, "src:lat"),  # where the ship was first heard (its source)
                "src_lon": _num(row, "src:lon"),
                "lat": lat,
                "lon": lon,
                "sog": _num(row, "last:sog"),
                "cog": _num(row, "last:cog"),
                "heading": _int(row, "last:heading"),
                "destination": row.get("info:dest"),  # as the crew typed it (live feed only)
                "eta": _time(row, "info:eta"),  # declared arrival time
            }
        )
    return sorted(out, key=lambda s: s["last_seen"] or "", reverse=True)


def ports(client: HBaseClient, known: dict[str, Port], waiting: dict[str, int]) -> list[dict[str, Any]]:
    """waiting: ships at anchor now per port (see dashboard.reasons.waiting_counts)."""
    out = []
    for port in known.values():
        latest = port_queue(client, port.port_id, 1)
        out.append(
            {
                "port_id": port.port_id,
                "name": port.name,
                "lat": port.lat,
                "lon": port.lon,
                "waiting_now": waiting.get(port.port_id, 0),
                "window_count": latest[0]["ships"] if latest else None,
                "window_end": latest[0]["window_end"] if latest else None,
            }
        )
    return out


def port_calls(client: HBaseClient, port_id: str, limit: int) -> list[dict[str, Any]]:
    return [
        {
            "mmsi": key[-9:],
            "anchored_at": _time(row, "call:anchored_at"),
            "berthed_at": _time(row, "call:berthed_at"),
            "departed_at": _time(row, "call:departed_at"),
            "wait_hours": _num(row, "call:wait_hours"),
        }
        for key, row in client.scan_prefix(PORT_CALLS, port_id, limit)
    ]


def port_queue(client: HBaseClient, port_id: str, limit: int) -> list[dict[str, Any]]:
    return [
        {
            "window_start": _time(row, "q:window_start"),
            "window_end": _time(row, "q:window_end"),
            "ships": _int(row, "q:ships"),
        }
        for _, row in client.scan_prefix(PORT_QUEUE, port_id, limit)
    ]


def track(client: HBaseClient, mmsi: str, limit: int) -> list[dict[str, Any]]:
    return [
        {
            "event_time": iso(unrev(key[len(mmsi) :])),
            "lat": _num(row, "pos:lat"),
            "lon": _num(row, "pos:lon"),
            "sog": _num(row, "pos:sog"),
            "cog": _num(row, "pos:cog"),
            "heading": _int(row, "pos:heading"),
            "nav": _int(row, "pos:nav"),
        }
        for key, row in client.scan_prefix(VESSEL_TRACK, mmsi, limit)
    ]


def known_ship(client: HBaseClient, mmsi: str) -> bool:
    return client.get(VESSEL_STATE, mmsi) is not None


def states(client: HBaseClient, mmsi: str) -> list[dict[str, Any]]:
    """The last 10 state changes, newest first (HBase keeps 10 versions of the cur: cells)."""
    by_time: dict[int, dict[str, str]] = {}
    for column in ("state", "port", "since"):
        for ts, value in client.get_versions(VESSEL_STATE, mmsi, f"cur:{column}", 10):
            by_time.setdefault(ts, {})[column] = value
    return [
        {
            "state": cells.get("state"),
            "port": cells.get("port"),
            "since": iso(int(cells["since"])) if "since" in cells else None,
            "ts": iso(ts),
        }
        for ts, cells in sorted(by_time.items(), reverse=True)
        if "state" in cells
    ]


KM_PER_NM = 1.852
NEAREST_PORT_KM = 200  # a ship further from every port than this has no inferred destination
MOVING_KNOTS = 0.5


def path_to(lat: float, lon: float, port: Port) -> list[list[float]]:
    """From a position to a port: along the sea lanes (common.lanes) to the last lane node, then the shipping
    lanes of the approach (dashboard.waterway). Near the port, or off the lanes, only the approach."""
    approach = waterway.default()
    if haversine_km(lat, lon, port.lat, port.lon) <= waterway.START_KM:
        return approach.route(lat, lon, port.lat, port.lon)
    sea = lanes.route(lat, lon, port)
    if not sea:
        return approach.route(lat, lon, port.lat, port.lon)
    *head, last_node, _ = sea
    return [*head, *approach.route(last_node[0], last_node[1], port.lat, port.lon)]


def voyage_origin(
    points: list[tuple[int, float, float]], zones: ZoneIndex | None, ports: dict[str, Port]
) -> dict[str, Any] | None:
    """The source of the ship's current voyage from its reports (time, lat, lon), oldest first: the port it
    last stood at and left, else the port of its first report, else only the place it was first heard
    (`known` False: the source is not in the data; dashboard.voyage). `voyage_from` is when it left."""
    if not points:
        return None
    left = voyage.departure(points, ports.values())
    if left is not None:
        port, i = left
        place = {"port_id": port.port_id, "name": port.name, "lat": port.lat, "lon": port.lon}
        left_at = points[i][0]
        return {**place, "basis": "left this port", "at": iso(left_at), "known": True, "voyage_from": left_at}
    ts, lat, lon = points[0]
    first = source_of(lat, lon, ts, zones, ports)
    return {**first, "known": first["port_id"] is not None, "voyage_from": ts}


def source_of(
    lat: float, lon: float, ts: int | None, zones: ZoneIndex | None, ports: dict[str, Port]
) -> dict[str, Any]:
    """Where a ship's story starts: the port area of its first report, or the port within
    MONITOR_SOURCE_PORT_KM, else just the place it was first heard (the feed is coastal: often the edge of
    coverage), named by the nearest port within PLACE_KM when there is one."""
    zone = zones.lookup(lat, lon) if zones else None
    port = ports.get(zone.port_id) if zone else None
    basis = "port area"
    if port is None:
        near = min(ports.values(), key=lambda p: haversine_km(lat, lon, p.lat, p.lon), default=None)
        if near and haversine_km(lat, lon, near.lat, near.lon) <= settings().monitor_source_port_km:
            port, basis = near, "near port"
    if port is None:
        km = haversine_km(lat, lon, near.lat, near.lon) if near else None
        return {
            "port_id": None,
            "name": None,
            "place": f"near {near.name}, {km:.0f} km" if near and km is not None and km <= PLACE_KM else None,
            "lat": lat,
            "lon": lon,
            "basis": "first heard here",
            "at": iso(ts),
        }
    place = {"port_id": port.port_id, "name": port.name, "lat": port.lat, "lon": port.lon}
    return {**place, "basis": basis, "at": iso(ts)}


def route(
    client: HBaseClient,
    mmsi: str,
    known: dict[str, Port],
    zones: ZoneIndex | None = None,
    monitored: dict[str, Port] | None = None,
) -> dict[str, Any] | None:
    """Where the ship came from and is heading, as far as the data says. None if the ship is unknown.

    `known` is every port a ship may name (geo/ports.csv and geo/world_ports.csv), `monitored` the ports this
    dashboard watches. The live feed's static message carries the destination the crew typed; when it names a
    known port it is used (basis "declared") and the path runs there along the sea lanes, however far.
    Otherwise (NOAA replay, unknown text, 'FOR ORDERS') it is inferred: the port area the ship is in, else the
    nearest monitored port within NEAREST_PORT_KM. Departed ships have none. `eta_hours` is travel time at the
    current speed; `declared` keeps the crew's text and ETA either way. `origin` is the place of the oldest
    report stored for the ship (its source).
    """
    watched = monitored if monitored is not None else known
    row = client.get(VESSEL_STATE, mmsi)
    lat, lon = (_num(row, "last:lat"), _num(row, "last:lon")) if row else (None, None)
    if row is None or lat is None or lon is None:
        return None
    state = row.get("last:state") or "underway"
    sog = _num(row, "last:sog")
    typed = row.get("info:dest")
    named = destination.match(typed, known)
    declared = {
        "text": typed,
        "eta": _time(row, "info:eta"),
        "port_id": named,
        "orders": destination.is_orders(typed),
    }
    stored = client.scan_prefix(VESSEL_TRACK, mmsi, 5000)  # newest first
    points = [  # oldest first
        (unrev(key[len(mmsi) :]), lat, lon)
        for key, cells in reversed(stored)
        if (lat := _num(cells, "pos:lat")) is not None and (lon := _num(cells, "pos:lon")) is not None
    ]
    origin = voyage_origin(points, zones, known)
    port, basis = known.get(named or ""), "declared"
    if port is None:
        port, basis = watched.get(row.get("last:area") or ""), "port area"
    if port is None and state != "departed":
        nearest = min(watched.values(), key=lambda p: haversine_km(lat, lon, p.lat, p.lon), default=None)
        if nearest and haversine_km(lat, lon, nearest.lat, nearest.lon) <= NEAREST_PORT_KM:
            port, basis = nearest, "nearest port"
    if port is None or state == "departed":
        return {
            "mmsi": mmsi,
            "state": state,
            "origin": origin,
            "destination": None,
            "distance_nm": None,
            "eta_hours": None,
            "path": [],
            "declared": declared,
        }
    path = path_to(lat, lon, port)
    nm = waterway.length_km(path) / KM_PER_NM
    moving = state == "underway" and sog is not None and sog >= MOVING_KNOTS
    return {
        "mmsi": mmsi,
        "state": state,
        "origin": origin,
        "destination": {
            "port_id": port.port_id,
            "name": port.name,
            "lat": port.lat,
            "lon": port.lon,
            "basis": basis,
        },
        "distance_nm": round(nm, 1),
        "eta_hours": round(nm / sog, 2) if moving and sog else None,
        "path": path,
        "declared": declared,
    }
