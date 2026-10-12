"""Where a ship's current voyage began: the last port it stopped at and then left.

AIS carries no origin, only the destination the crew typed. What the data does hold is where a ship was
standing still: a ship that was stopped at a port and is now away started its voyage there. A ship first heard
at sea (it was already under way when the feed began) has no such stop: its source is not known, and the place
it was first heard is only the edge of what the feed covers.
"""

from collections.abc import Iterable, Sequence

from common.config import settings
from common.geo import Port, haversine_km

KM_PER_NM = 1.852  # the distances and the speed are the environment's: MONITOR_STOP_KM, MONITOR_LEFT_KM,
# MONITOR_SLOW_KN (a stop this close to a port, left once a later report is this far away, "standing still")


def _slow(points: Sequence[tuple[int, float, float]], i: int) -> bool:
    ts, lat, lon = points[i]
    for j in (i - 1, i + 1):
        if 0 <= j < len(points):
            t2, la2, lo2 = points[j]
            hours = abs(ts - t2) / 3_600_000
            knots = haversine_km(lat, lon, la2, lo2) / hours / KM_PER_NM if hours > 0 else None
            if knots is not None and knots < settings().monitor_slow_kn:
                return True
    return False


def departure(points: Sequence[tuple[int, float, float]], ports: Iterable[Port]) -> tuple[Port, int] | None:
    """(port, index of the last report there) for the latest stop at a port that the ship has since left;
    `points` are (time, lat, lon), oldest first. None when the ship was never seen standing at a port."""
    known = list(ports)
    stop_km, left_km = settings().monitor_stop_km, settings().monitor_left_km
    stops: list[tuple[int, Port]] = []
    for i, (_, lat, lon) in enumerate(points):
        if not _slow(points, i):
            continue
        near = min(known, key=lambda p: haversine_km(lat, lon, p.lat, p.lon), default=None)
        if near and haversine_km(lat, lon, near.lat, near.lon) <= stop_km:
            stops.append((i, near))
    last_away: dict[str, int] = {}  # per port: the newest report that is away from it
    for _, port in stops:
        if port.port_id not in last_away:
            last_away[port.port_id] = next(
                (
                    j
                    for j in range(len(points) - 1, -1, -1)
                    if haversine_km(points[j][1], points[j][2], port.lat, port.lon) > left_km
                ),
                -1,
            )
    left = [(i, p) for i, p in stops if i < last_away[p.port_id]]
    if not left:
        return None
    i, port = max(left, key=lambda s: s[0])
    return port, i
