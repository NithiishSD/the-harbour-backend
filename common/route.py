"""The Singapore -> Los Angeles sea route as one line: positions along it (the demo seed) and its lattice.

The lanes ships are routed over, the live AIS boxes and the weather points of the dashboard come from the lane
graph in common.lanes; this line is the same Pacific spine, kept for ingest.seed.

Longitudes run on past 180 (195 is 165 W) so the Pacific is one piece; `norm_lon` turns them back into the
-180..180 that AIS and the API use.
"""

import math

from common.geo import haversine_km

# ponytail: hand-placed waypoints off the great-circle track, not a router; a real lane graph if ships must
# be routed round land or ice.
WAYPOINTS: tuple[tuple[float, float], ...] = (
    (1.26, 103.84),  # Singapore
    (1.30, 104.50),  # Singapore Strait, east exit
    (6.0, 108.5),
    (12.0, 113.5),
    (19.0, 119.0),
    (20.8, 121.2),  # Luzon Strait
    (23.0, 126.0),
    (28.0, 134.0),
    (33.0, 142.0),  # south-east of Japan
    (40.0, 160.0),
    (44.0, 180.0),
    (45.0, 195.0),
    (42.0, 215.0),
    (37.0, 232.0),
    (33.55, 241.55),  # off Los Angeles
)
LATTICE_OFFSET_DEG = 3.5  # weather points sit on the route and this far north and south of it
KM_PER_NM = 1.852


def norm_lon(lon: float) -> float:
    return lon - 360 if lon > 180 else lon


def _leg_nm(a: tuple[float, float], b: tuple[float, float]) -> float:
    return haversine_km(a[0], a[1], b[0], b[1]) / KM_PER_NM


LEGS = [_leg_nm(a, b) for a, b in zip(WAYPOINTS, WAYPOINTS[1:], strict=False)]
LENGTH_NM = sum(LEGS)


def along(nm: float) -> tuple[float, float, int]:
    """(lat, lon, heading in degrees) after nm nautical miles from Singapore, clamped to the route."""
    nm = min(max(nm, 0.0), LENGTH_NM)
    for i, leg in enumerate(LEGS):
        if nm <= leg or i == len(LEGS) - 1:
            (la, lo), (lb, lp) = WAYPOINTS[i], WAYPOINTS[i + 1]
            f = nm / leg
            lat = la + (lb - la) * f
            heading = math.degrees(math.atan2((lp - lo) * math.cos(math.radians(lat)), lb - la))
            return round(lat, 5), round(norm_lon(lo + (lp - lo) * f), 5), round(heading) % 360
        nm -= leg
    raise AssertionError("unreachable: the last leg always matches")


def lattice() -> list[tuple[str, float, float]]:
    """(point id, lat, lon) of the weather points: the route's waypoints between the ports, each with a
    point north and south of it."""
    points: list[tuple[str, float, float]] = []
    for lat, lon in WAYPOINTS[1:-1]:
        for d in (-LATTICE_OFFSET_DEG, 0.0, LATTICE_OFFSET_DEG):
            points.append((f"SEA-{len(points):02d}", round(lat + d, 2), round(norm_lon(lon), 2)))
    return points
