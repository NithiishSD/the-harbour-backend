"""Sea routes along the lanes ships really use, instead of a straight line over land.

The lane map is a grid of cells that real AIS positions fall in, built from NOAA days with
`python -m dashboard.waterway data/noaa/csv/*.csv` into geo/lanes.json. A route is the cheapest path through
those cells (A*, busier cells cost less, so it keeps to the channels), then pulled straight wherever a
straight line stays on lane cells. Outside the lane map the route is a straight line, as before.
"""

import csv
import heapq
import json
import math
import sys
from collections import defaultdict
from datetime import datetime
from functools import cache
from pathlib import Path

from common.config import settings
from common.geo import haversine_km

CELL_DEG = 0.0025  # about 280 m north-south
MIN_SHIPS = 2  # a cell is water once this many different ships reported in it: one bad GPS fix is not a lane
MIN_LENGTH_M = 30  # lanes from ships this long or longer (or of unknown length): no marina shortcuts
SNAP_KM = 5.0  # a destination further than this from every lane cell gets a straight route
START_KM = 200.0  # as far as routes go (dashboard.live.NEAREST_PORT_KM)
MAX_GAP_S = 600  # two reports of a ship further apart than this are not joined into a track line
MAX_GAP_KM = 5.0
BUSY_BONUS = 3.0  # step cost is distance * (1 + BUSY_BONUS / ships in the cell)
STEPS = [(-1, -1), (-1, 0), (-1, 1), (0, -1), (0, 1), (1, -1), (1, 0), (1, 1)]

Cell = tuple[int, int]


class Lanes:
    def __init__(self, ships: dict[Cell, int], cell: float = CELL_DEG) -> None:
        self.ships = ships
        self.cell = cell

    def of(self, lat: float, lon: float) -> Cell:
        return math.floor(lat / self.cell), math.floor(lon / self.cell)

    def centre(self, c: Cell) -> tuple[float, float]:
        return (c[0] + 0.5) * self.cell, (c[1] + 0.5) * self.cell

    def nearest(self, lat: float, lon: float, within_km: float) -> Cell | None:
        """The lane cell nearest the point, if one is within `within_km`."""
        here = self.of(lat, lon)
        if here in self.ships:
            return here
        best = min(self.ships, key=lambda c: haversine_km(lat, lon, *self.centre(c)), default=None)
        return best if best and haversine_km(lat, lon, *self.centre(best)) <= within_km else None

    def cells_on(self, a: tuple[float, float], b: tuple[float, float]) -> set[Cell]:
        """The cells the straight line a -> b passes (sampled every half cell)."""
        n = max(1, math.ceil(max(abs(b[0] - a[0]), abs(b[1] - a[1])) / (self.cell / 2)))
        return {self.of(a[0] + (b[0] - a[0]) * k / n, a[1] + (b[1] - a[1]) * k / n) for k in range(n + 1)}

    def clear(self, a: tuple[float, float], b: tuple[float, float]) -> bool:
        """True when the straight line a -> b stays on lane cells."""
        return self.cells_on(a, b) <= self.ships.keys()

    def search(self, start: Cell, goal: Cell) -> list[Cell] | None:
        """A* over lane cells; the heuristic is straight distance, never more than the real cost."""

        def km(a: Cell, b: Cell) -> float:
            return haversine_km(*self.centre(a), *self.centre(b))

        best = {start: 0.0}
        came: dict[Cell, Cell] = {}
        todo = [(km(start, goal), start)]
        while todo:
            _, c = heapq.heappop(todo)
            if c == goal:
                path = [c]
                while c in came:
                    c = came[c]
                    path.append(c)
                return path[::-1]
            for di, dj in STEPS:
                n = (c[0] + di, c[1] + dj)
                if n not in self.ships:
                    continue
                cost = best[c] + km(c, n) * (1 + BUSY_BONUS / self.ships[n])
                if cost < best.get(n, math.inf):
                    best[n], came[n] = cost, c
                    heapq.heappush(todo, (cost + km(n, goal), n))
        return None

    def route(self, lat: float, lon: float, to_lat: float, to_lon: float) -> list[list[float]]:
        """[[lat, lon], ...] from the point to the destination; a straight line when no lane route exists."""
        straight = [[lat, lon], [to_lat, to_lon]]
        # A ship out at sea sails straight to the nearest lane; a port must lie on the lane map.
        a, b = self.nearest(lat, lon, START_KM), self.nearest(to_lat, to_lon, SNAP_KM)
        cells = self.search(a, b) if a and b else None
        if not cells:
            return straight
        pts = [(lat, lon), *(self.centre(c) for c in cells), (to_lat, to_lon)]
        # String pulling: from each kept point jump to the furthest point still in straight sight.
        out, i = [pts[0]], 0
        while i < len(pts) - 1:
            j = i + 1
            while j + 1 < len(pts) and self.clear(pts[i], pts[j + 1]):
                j += 1
            out.append(pts[j])
            i = j
        return [[round(p[0], 5), round(p[1], 5)] for p in out]


def length_km(path: list[list[float]]) -> float:
    return sum(haversine_km(*a, *b) for a, b in zip(path, path[1:], strict=False))


@cache
def default() -> Lanes:
    """geo/lanes.json; an empty lane map (straight routes) when it has not been built."""
    path = settings().geo_dir / "lanes.json"
    if not path.exists():
        return Lanes({})
    doc = json.loads(path.read_text())
    return Lanes({(i, j): n for i, j, n in doc["cells"]}, doc["cell"])


def largest_network(cells: set[Cell]) -> set[Cell]:
    """The biggest group of touching cells: small separate patches would strand a route."""
    best: set[Cell] = set()
    left = set(cells)
    while left:
        group, todo = set(), [left.pop()]
        while todo:
            c = todo.pop()
            group.add(c)
            for di, dj in STEPS:
                if (n := (c[0] + di, c[1] + dj)) in left:
                    left.remove(n)
                    todo.append(n)
        best = max(best, group, key=len)
    return best


def build(csv_paths: list[Path], cell: float = CELL_DEG) -> dict[str, object]:
    """Distinct ships per cell along their tracks in NOAA CSVs, keeping cells with at least MIN_SHIPS.

    Ships report every few minutes, so the cells between two consecutive reports of a ship are filled in
    (when the reports are close in time and space): the lanes are the tracks, not separate dots.
    """
    tracks: dict[str, list[tuple[str, float, float]]] = defaultdict(list)
    for path in csv_paths:
        with path.open(newline="") as f:
            for r in csv.DictReader(f):
                if r.get("Length") and float(r["Length"]) < MIN_LENGTH_M:
                    continue
                tracks[r["MMSI"]].append((r["BaseDateTime"], float(r["LAT"]), float(r["LON"])))
    lanes = Lanes({}, cell)
    seen: dict[Cell, set[str]] = defaultdict(set)
    for mmsi, track in tracks.items():
        track.sort()
        for (t1, lat1, lon1), (t2, lat2, lon2) in zip(track, [*track[1:], track[-1]], strict=True):
            gap_s = (datetime.fromisoformat(t2) - datetime.fromisoformat(t1)).total_seconds()
            joined = gap_s <= MAX_GAP_S and haversine_km(lat1, lon1, lat2, lon2) <= MAX_GAP_KM
            for c in lanes.cells_on((lat1, lon1), (lat2, lon2) if joined else (lat1, lon1)):
                seen[c].add(mmsi)
    keep = {c: len(s) for c, s in seen.items() if len(s) >= MIN_SHIPS}
    cells = sorted([i, j, keep[i, j]] for i, j in largest_network(set(keep)))
    return {"cell": cell, "min_ships": MIN_SHIPS, "min_length_m": MIN_LENGTH_M, "cells": cells}


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("usage: python -m dashboard.waterway NOAA.csv [...]")
    doc = build([Path(p) for p in sys.argv[1:]])
    out = settings().geo_dir / "lanes.json"
    out.write_text(json.dumps(doc, separators=(",", ":")))
    print(f"{len(doc['cells'])} lane cells -> {out}")  # type: ignore[arg-type]
