"""Crafted voyages around Los Angeles / Long Beach for streaming tests (positions in ais.positions format).

Coordinates sit inside the repo geofences: LB inner anchorage east, LB terminals, LA outer anchorage.
"""

import random
from typing import Any

MIN = 60_000
HOUR = 60 * MIN
LB_ANCHORAGE = (33.740, -118.165)
LB_BERTH = (33.760, -118.210)
LA_ANCHORAGE = (33.700, -118.215)


def position(mmsi: str, ts: int, where: tuple[float, float], sog: float, nav: int) -> dict[str, Any]:
    lat, lon = where
    return dict(
        mmsi=mmsi, ts=ts, lat=lat, lon=lon, sog=sog, cog=None, heading=None, nav_status=nav, source="noaa"
    )


def every(
    mmsi: str, start: int, end: int, where: tuple[float, float], sog: float, nav: int, step: int = 3 * MIN
):
    return [position(mmsi, t, where, sog, nav) for t in range(start, end, step)]


class Scenario:
    """Unique MMSIs and a unique time range per run, so repeated runs never see each other's data."""

    def __init__(self) -> None:
        first = random.randint(200_000, 789_000) * 1000
        self.dark = str(first + 1)
        self.gap = str(first + 2)
        self.call = str(first + 3)
        self.neighbours = [str(first + 10 + i) for i in range(4)]
        # Somewhere between 2030 and 2300: far from real data and from other runs.
        self.t0 = 1_893_456_000_000 + random.randint(0, 2_000_000) * HOUR

    def positions(self) -> list[dict[str, Any]]:
        t0 = self.t0
        out = []
        out += every(self.dark, t0, t0 + HOUR, LB_ANCHORAGE, 0.1, 1)  # anchored, then silent
        out += every(self.gap, t0, t0 + HOUR, LA_ANCHORAGE, 0.1, 1)  # silent in an area nobody else reports
        out += every(self.call, t0, t0 + 2 * HOUR, LB_ANCHORAGE, 0.1, 1)  # waits 2 h ...
        out += every(self.call, t0 + 2 * HOUR, t0 + 9 * HOUR, LB_BERTH, 0.0, 5)  # ... then berths
        for n in self.neighbours:
            out += every(n, t0, t0 + 9 * HOUR, LB_ANCHORAGE, 0.1, 1)  # keep reporting through the silence
        return sorted(out, key=lambda p: p["ts"])


def queue_growth(t0: int, first_mmsi: int) -> list[dict[str, Any]]:
    """2 ships at anchor off Long Beach, then 10 more arrive half an hour later."""
    out = []
    for i in range(2):
        out += every(str(first_mmsi + i), t0, t0 + HOUR, LB_ANCHORAGE, 0.1, 1)
    for i in range(2, 12):
        out += every(str(first_mmsi + i), t0 + 30 * MIN, t0 + HOUR, LB_ANCHORAGE, 0.1, 1)
    return sorted(out, key=lambda p: p["ts"])
