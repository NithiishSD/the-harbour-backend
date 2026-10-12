"""Ships that have sailed out of AIS range: an estimated position along the sea route.

The live feed is coastal: a few tens of miles off the land a ship stops reporting, and reports again when it
comes in range of the far coast, one to three weeks later. The ship is still there. For a ship last heard
under way at sea speed with a declared destination on the sea lanes, the position is estimated: its last
report moved along the lanes to that port at its last speed for the time since. The ship is marked
`estimated`, keeps its real `last_seen` and is drawn differently; its next real report replaces the estimate.
"""

import math
from typing import Any

from common import lanes
from common.geo import Port, haversine_km
from common.route import KM_PER_NM, norm_lon
from dashboard import destination
from dashboard.reasons import ms

MIN_SOG_KN = 6.0  # slower than this is not a crossing
MIN_AGE_MS = 30 * 60_000  # a newer report is the position
MAX_AGE_MS = 25 * 24 * 3_600_000  # longer than any crossing of the route


def along_path(path: list[list[float]], km: float) -> tuple[float, float, float, bool]:
    """(lat, lon, course, ran off the end) after km along the polyline `path`."""
    for a, b in zip(path, path[1:], strict=False):
        leg = haversine_km(a[0], a[1], b[0], b[1])
        if km <= leg or b is path[-1]:
            f = 1.0 if leg == 0 else min(1.0, km / leg)
            lo_b = b[1] if abs(b[1] - a[1]) <= 180 else b[1] + (360 if b[1] < a[1] else -360)
            lat, lon = a[0] + (b[0] - a[0]) * f, a[1] + (lo_b - a[1]) * f
            course = math.degrees(math.atan2((lo_b - a[1]) * math.cos(math.radians(lat)), b[0] - a[0])) % 360
            return round(lat, 5), round(norm_lon(lon), 5), round(course, 1), km > leg and b is path[-1]
        km -= leg
    return path[-1][0], path[-1][1], 0.0, True


def estimate(ship: dict[str, Any], now_ms: int, ports: dict[str, Port]) -> dict[str, Any] | None:
    """The estimated position fields of a ship, or None when it is not a ship out of range on a crossing."""
    sog = ship.get("sog") or 0.0
    if ship.get("state") != "underway" or ship.get("port") or sog < MIN_SOG_KN:
        return None
    seen = ms(ship.get("last_seen"))
    if seen is None or not MIN_AGE_MS <= now_ms - seen <= MAX_AGE_MS:
        return None
    code = destination.match(ship.get("destination"), ports)
    if code is None:
        return None
    port = ports[code]
    path = lanes.route(ship["lat"], ship["lon"], port)  # along the sea lanes to wherever it declared
    if not path:
        return None
    km = sog * KM_PER_NM * (now_ms - seen) / 3_600_000
    lat, lon, course, finished = along_path(path, km)
    if finished:  # it should be in range of the far coast by now: wait for its report
        return None
    return {
        "lat": lat,
        "lon": lon,
        "cog": course,
        "heading": None,
        "estimated": True,
        "estimated_from": {"lat": ship["lat"], "lon": ship["lon"], "last_seen": ship["last_seen"]},
    }


def apply(ships: list[dict[str, Any]], now_ms: int | None, ports: dict[str, Port]) -> int:
    """Replace the position of every ship that is out of range with its estimate. Returns how many."""
    if now_ms is None:
        return 0
    count = 0
    for s in ships:
        found = estimate(s, now_ms, ports)
        if found:
            s.update(found)
            count += 1
    return count
