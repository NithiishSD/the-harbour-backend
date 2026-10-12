"""Per-ship state machine: underway -> anchored -> berthed -> departed, port calls and dark-ship timeouts.

Pure Python with no Spark import, so the streaming job and the tests run exactly the same rules.

Rules (zones come from common.geo; a ship's own nav status wins when it says "at anchor"):
- anchored: inside any zone of a port with nav status 1 (at anchor) and not moving, or inside an
  anchorage polygon with SOG < 0.5 kn.
- berthed: inside a berth polygon, nav status not 1, and moored (nav 5) or SOG < 1 kn.
- departed: was berthed and is now moving or out of the berth; stays departed until it leaves the port area.
- anchored and berthed only end on a clear move (>= 3 kn) or on leaving the zone (hysteresis).
- underway: anything else.

A port call opens when a ship anchors (or berths without anchoring first) and closes when it departs from
the berth, or when it is seen outside the port's area without ever berthing.
"""

import json
from dataclasses import asdict, dataclass
from typing import Any

from common.geo import Zone, ZoneIndex

UNDERWAY, ANCHORED, BERTHED, DEPARTED = "underway", "anchored", "berthed", "departed"
NAV_AT_ANCHOR, NAV_MOORED = 1, 5
ANCHOR_SOG, BERTH_SOG, RELEASE_SOG, LOITER_SOG = 0.5, 1.0, 3.0, 3.0
HOUR_MS = 3_600_000
FORGET_AFTER_MS = 7 * 24 * HOUR_MS  # drop state for ships not seen for a week


@dataclass
class ShipState:
    state: str = UNDERWAY
    port: str | None = None  # port the current state belongs to
    since: int = 0
    last_seen: int = -1  # nothing seen yet
    lat: float = 0.0
    lon: float = 0.0
    sog: float | None = None
    area: str | None = None  # port area of the last position (for "other ships in the same area")
    call_port: str | None = None
    anchored_at: int | None = None
    berthed_at: int | None = None
    dark: bool = False
    first_seen: int = -1  # first report since the state was created: when the ship "arrived on our map"
    src_lat: float | None = None  # where that first report was: the ship's source (kept once in HBase)
    src_lon: float | None = None

    def to_json(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"))

    @classmethod
    def from_json(cls, text: str | None) -> "ShipState | None":
        return cls(**json.loads(text)) if text else None


Event = dict[str, Any]


def _event(kind: str, mmsi: str, ts: int, port: str | None, lat: float, lon: float, **data: Any) -> Event:
    return {"type": kind, "mmsi": mmsi, "ts": ts, "port_id": port, "lat": lat, "lon": lon, "data": data}


def classify(
    zone: Zone | None, nav: int | None, sog: float | None, prev: str, prev_port: str | None
) -> tuple[str, str | None]:
    """New (state, port) for one position, given the previous state."""
    port = zone.port_id if zone else None
    if zone and nav == NAV_AT_ANCHOR and (sog is None or sog < ANCHOR_SOG):
        return ANCHORED, port
    moored = nav == NAV_MOORED or (sog is not None and sog < BERTH_SOG)
    if zone and zone.kind == "berth" and nav != NAV_AT_ANCHOR and moored:
        return BERTHED, port
    if zone and zone.kind == "anchorage" and sog is not None and sog < ANCHOR_SOG:
        return ANCHORED, port
    same_port = zone is not None and zone.port_id == prev_port
    # Hysteresis: anchored ships swing on the chain and berthed ships are pushed by tugs at 1-1.5 kn.
    # Only a clear move (>= 3 kn) or leaving the zone ends the state; otherwise the state flaps.
    settled = sog is None or sog < RELEASE_SOG
    if prev == ANCHORED and same_port and settled:
        return ANCHORED, prev_port
    if prev == BERTHED and same_port and zone is not None and zone.kind == "berth" and settled:
        return BERTHED, prev_port
    if prev in (BERTHED, DEPARTED):
        # Leaving the berth completes the call; the ship counts as departed until it leaves the port area.
        return (DEPARTED, prev_port) if same_port or prev == BERTHED else (UNDERWAY, port)
    return UNDERWAY, port


def monitored(s: ShipState) -> bool:
    """Ships whose silence is suspicious: stationary in port, or loitering inside a port area.

    A ship that has departed, or is moving through, will leave our coverage on its own; that is not dark.
    """
    if s.state in (ANCHORED, BERTHED):
        return True
    return s.state == UNDERWAY and s.area is not None and (s.sog or 0) < LOITER_SOG


def _close_call(s: ShipState, mmsi: str, ts: int, out: list[Event]) -> None:
    arrival = s.anchored_at if s.anchored_at is not None else s.berthed_at
    if s.call_port is None or arrival is None:
        return
    out.append(call_event(s, mmsi, ts, departed_at=ts))
    s.call_port, s.anchored_at, s.berthed_at = None, None, None


def call_event(s: ShipState, mmsi: str, ts: int, departed_at: int | None = None) -> Event:
    arrival = s.anchored_at if s.anchored_at is not None else s.berthed_at
    wait = (
        (s.berthed_at - s.anchored_at) / HOUR_MS
        if s.anchored_at is not None and s.berthed_at is not None
        else None
    )
    turnaround = (
        (departed_at - arrival) / HOUR_MS if departed_at is not None and arrival is not None else None
    )
    return _event(
        "port_call",
        mmsi,
        ts,
        s.call_port,
        s.lat,
        s.lon,
        arrival_at=arrival,
        anchored_at=s.anchored_at,
        berthed_at=s.berthed_at,
        departed_at=departed_at,
        wait_hours=None if wait is None else round(wait, 3),
        turnaround_hours=None if turnaround is None else round(turnaround, 3),
    )


def _opt(value: Any) -> float | None:
    """A number, or None for missing values (None, NaN, or pandas NA)."""
    try:
        return None if value is None or value != value else float(value)
    except TypeError:  # pandas NA refuses to be compared
        return None


def step(s: ShipState, mmsi: str, pos: dict[str, Any], zones: ZoneIndex, out: list[Event]) -> None:
    ts, lat, lon = int(pos["ts"]), float(pos["lat"]), float(pos["lon"])
    sog = pos.get("sog")
    sog = None if sog is None or sog != sog else float(sog)  # pandas gives NaN for missing
    nav = pos.get("nav_status")
    nav = None if nav is None or nav != nav else int(nav)
    if s.first_seen < 0:
        s.first_seen = ts
    if s.src_lat is None:
        s.src_lat, s.src_lon = lat, lon
    if s.dark:
        out.append(
            _event(
                "reappeared", mmsi, ts, s.area, lat, lon, silent_hours=round((ts - s.last_seen) / HOUR_MS, 2)
            )
        )
        s.dark = False
    zone = zones.lookup(lat, lon)
    new_state, new_port = classify(zone, nav, sog, s.state, s.port)

    # A ship with an open call seen outside every port area has left without berthing.
    area = zone.port_id if zone else None
    if s.call_port is not None and area is None:
        _close_call(s, mmsi, ts, out)

    if new_state != s.state or new_port != s.port:
        waiting = s.call_port is not None and s.berthed_at is None
        if new_state == ANCHORED:
            if waiting:
                s.call_port = new_port  # moved to another anchorage: the wait continues
            else:
                _close_call(s, mmsi, ts, out)
                s.call_port, s.anchored_at, s.berthed_at = new_port, ts, None
        elif new_state == BERTHED:
            if not waiting:
                _close_call(s, mmsi, ts, out)
                s.anchored_at = None
            # The call belongs to the port of the berth, even if the ship anchored off a neighbour port.
            s.call_port, s.berthed_at = new_port, ts
            s.lat, s.lon = lat, lon
            out.append(call_event(s, mmsi, ts))
        elif new_state == DEPARTED and s.state == BERTHED:
            s.lat, s.lon = lat, lon
            _close_call(s, mmsi, ts, out)
        s.state, s.port, s.since = new_state, new_port, ts
        out.append(_event("state", mmsi, ts, new_port, lat, lon, state=new_state, since=ts))
    s.last_seen, s.lat, s.lon, s.sog, s.area = ts, lat, lon, sog, area


def apply_positions(
    s: ShipState | None, mmsi: str, positions: list[dict[str, Any]], zones: ZoneIndex
) -> tuple[ShipState, list[Event]]:
    """Feed positions (any order) through the state machine. Old or duplicate positions are skipped."""
    s = s or ShipState()
    out: list[Event] = []
    newest: dict[str, Any] | None = None
    for pos in sorted(positions, key=lambda p: p["ts"]):
        if int(pos["ts"]) <= s.last_seen:
            continue  # late or duplicate: the track still stores it, the state machine must not go backwards
        step(s, mmsi, pos, zones, out)
        newest = pos
    if newest is not None:
        # speed, course and heading let the dashboard keep the ship moving between position reports
        last = {
            "sog": s.sog,
            "cog": _opt(newest.get("cog")),
            "heading": _opt(newest.get("heading")),
            "first_seen": s.first_seen,
            "src_lat": s.src_lat,
            "src_lon": s.src_lon,
        }
        out.append(_event("last", mmsi, s.last_seen, s.area, s.lat, s.lon, state=s.state, **last))
    return s, out


def next_timeout(s: ShipState, dark_timeout_ms: int) -> int:
    """Event time at which this ship's state times out."""
    if monitored(s) and not s.dark:
        return s.last_seen + dark_timeout_ms
    return s.last_seen + FORGET_AFTER_MS


def on_timeout(s: ShipState, mmsi: str, dark_timeout_ms: int) -> tuple[list[Event], bool]:
    """Returns (events, keep_state). A monitored ship becomes a dark-ship candidate; others are forgotten."""
    if monitored(s) and not s.dark:
        s.dark = True
        silent = round(dark_timeout_ms / HOUR_MS, 2)
        event = _event(
            "dark_candidate", mmsi, s.last_seen, s.area, s.lat, s.lon, state=s.state, silent_hours=silent
        )
        return [event], True
    return [], False
