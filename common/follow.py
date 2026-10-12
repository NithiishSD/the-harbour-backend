"""Which ships to follow wherever in the world they are heard: those on a voyage to or from a monitored port.

The regional feed hears a ship while it is near the monitored country or the source countries. A ship that
left (or is coming from afar) is out of that area for days, and comes back into some station's range
anywhere: aisstream can be told which ships to send from the whole world (a list of at most 200). This
picks the list: ships with the role `from` or `to` (common.monitor), until they arrive, silent ones first.
"""

from datetime import UTC, datetime
from typing import Any

from common.config import settings
from common.geo import Port, haversine_km
from common.monitor import Monitor, _bound_for

HOUR_MS = 3_600_000


def _ms(text: str | None) -> int | None:
    try:
        return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp() * 1000) if text else None
    except ValueError:
        return None


def arrived(ship: dict[str, Any], world: dict[str, Port]) -> bool:
    """Berthed, or standing at the port it said it was bound for: the voyage is over."""
    if ship.get("state") == "berthed":
        return True
    port = world.get(_bound_for(ship.get("destination"), world) or "")
    if port is None:
        return False
    s = settings()
    slow = (ship.get("sog") or 0.0) < s.monitor_slow_kn
    return slow and haversine_km(ship["lat"], ship["lon"], port.lat, port.lon) <= s.monitor_stop_km


def voyage_mmsis(
    ships: list[dict[str, Any]],
    monitor: Monitor,
    world: dict[str, Port],
    now_ms: int,
    limit: int | None = None,
) -> list[str]:
    """MMSIs to follow: `from`/`to` ships that have not arrived and began their voyage within FOLLOW_DAYS,
    the ones not heard for longest first (they are the ones out of the regional feed), at most `limit`."""
    s = settings()
    limit = s.follow_max if limit is None else limit
    horizon = now_ms - int(s.follow_days * 24 * HOUR_MS)
    picked: list[tuple[int, str]] = []
    for ship in ships:
        if not ({"from", "to"} & set(monitor.roles(ship, world))) or arrived(ship, world):
            continue
        started = _ms(ship.get("first_seen")) or 0
        if started and started < horizon:
            continue
        picked.append((_ms(ship.get("last_seen")) or 0, ship["mmsi"]))
    picked.sort()  # oldest report first
    return [mmsi for _, mmsi in picked[:limit]]


def now_ms() -> int:
    return int(datetime.now(UTC).timestamp() * 1000)
