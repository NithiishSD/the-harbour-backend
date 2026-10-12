"""How much of a voyage can be followed by coastal AIS: choosing the country to monitor and its partners.

Terrestrial AIS reaches a few tens of miles from a coast. A voyage between two countries is followed the
whole way where it hugs coasts, and is blind where it crosses open ocean. This module measures that for the
routes of the lane graph (common.lanes):

- measured: a survey of where ships were really heard (`ingest.coverage_survey` writes
  data/coverage/cells.json, ship counts per 2-degree cell); a point of a route is covered when its cell, or a
  neighbour, was heard
- estimated, when there is no survey: a leg between two open-ocean lane nodes is blind, any other is covered

For every route the answer is the covered share of its length and the longest blind stretch (km and hours at
cruising speed). A country whose partner routes are covered in the high nineties is one a ship can be followed
from the first port to the last; where a blind stretch remains, the dashboard estimates the position along the
lanes (dashboard.crossing).
"""

import json
import math
from dataclasses import dataclass
from pathlib import Path

from common import lanes
from common.geo import Port, haversine_km

CRUISE_KN = 14.0
KM_PER_NM = 1.852
STEP_KM = 25.0
# Lane nodes in open water: the legs between two of them are out of range of any coast station.
OPEN_NODES = frozenset({
    "NP1", "NP2", "NP3", "NP4", "NP5", "HI1", "HI2", "HNL", "PHS", "JPS", "JPE", "IO1", "IOA", "IOC", "IOD",
    "IOB", "ARBS", "ARBW", "ATL1", "ATL2", "AZO", "ATS0", "ATS1", "ATS2", "ATS3", "AFW1", "AFW3", "IOE",
    "AUS0", "AUS2", "AUS4", "BAJ", "SLS", "GAD",
})  # fmt: skip


@dataclass(frozen=True)
class Cells:
    """Ship counts per square of `size` degrees, from a survey of the live feed."""

    size: float
    counts: dict[tuple[int, int], int]
    minutes: float = 0.0

    def of(self, lat: float, lon: float) -> tuple[int, int]:
        lon = (lon + 180) % 360 - 180  # the lanes' longitudes run past 180
        return math.floor(lat / self.size), math.floor((lon + 180) / self.size)

    def heard(self, lat: float, lon: float, pad: int = 1, min_ships: int = 1) -> bool:
        r, c = self.of(lat, lon)
        return any(
            self.counts.get((r + dr, c + dc), 0) >= min_ships
            for dr in range(-pad, pad + 1)
            for dc in range(-pad, pad + 1)
        )


def load_cells(path: Path) -> Cells | None:
    if not path.is_file():
        return None
    doc = json.loads(path.read_text())
    counts = {(int(k.split(":")[0]), int(k.split(":")[1])): int(v) for k, v in doc["cells"].items()}
    return Cells(float(doc["cell"]), counts, float(doc.get("minutes", 0)))


@dataclass(frozen=True)
class Coverage:
    covered: float  # share of the route length that coastal AIS sees, 0..1
    longest_gap_km: float  # the longest stretch it does not see
    length_km: float
    basis: str  # "measured" or "estimated"

    @property
    def gap_hours(self) -> float:
        return self.longest_gap_km / KM_PER_NM / CRUISE_KN


def _densify(points: list[tuple[float, float]]) -> list[tuple[float, float]]:
    out: list[tuple[float, float]] = []
    for a, b in zip(points, points[1:], strict=False):
        km = haversine_km(a[0], a[1], b[0], b[1])
        n = max(1, math.ceil(km / STEP_KM))
        out += [(a[0] + (b[0] - a[0]) * i / n, a[1] + (b[1] - a[1]) * i / n) for i in range(n)]
    return out + [points[-1]]


def _walk(nodes: list[str], cells: Cells | None) -> tuple[list[tuple[float, float]], list[bool], str]:
    """The route through `nodes` as dense points, whether each is heard, and the basis."""
    pts = [lanes.NODES[n] for n in nodes]  # continuous longitudes: a leg over the date line is short
    if cells is not None:
        dense = _densify(pts)
        seen = [cells.heard(lat, lon) for lat, lon in dense]
        basis = "measured"
    else:  # leg by leg from the lane graph: sample each leg and call it blind when both ends are open water
        dense, seen = [], []
        for (a, b), (pa, pb) in zip(
            zip(nodes, nodes[1:], strict=False), zip(pts, pts[1:], strict=False), strict=False
        ):
            leg = _densify([pa, pb])[:-1]
            blind = a in OPEN_NODES and b in OPEN_NODES
            dense += leg
            seen += [not blind] * len(leg)
        dense.append(pts[-1])
        seen.append(nodes[-1] not in OPEN_NODES)
        basis = "estimated"
    return dense, seen, basis


def route_coverage(nodes: list[str], cells: Cells | None) -> Coverage:
    """Coverage of the lane route through `nodes` (ids of lanes.NODES, in order)."""
    dense, seen, basis = _walk(nodes, cells)
    length = longest = run = 0.0
    covered = 0.0
    for i in range(len(dense) - 1):
        d = haversine_km(dense[i][0], dense[i][1], dense[i + 1][0], dense[i + 1][1])
        length += d
        if seen[i]:
            covered += d
            run = 0.0
        else:
            run += d
            longest = max(longest, run)
    return Coverage(covered / length if length else 1.0, longest, length, basis)


def principal_port(world: dict[str, Port], country: str) -> Port | None:
    """The country's first port in the list (the monitored ones come first), joined to the lanes."""
    return next((p for p in world.values() if p.country == country and p.node in lanes.NODES), None)


@dataclass(frozen=True)
class PartnerRow:
    country: str
    port: str  # the port the route was measured to
    covered: float
    gap_km: int
    gap_hours: float
    length_km: int
    basis: str


@dataclass(frozen=True)
class HomeRow:
    country: str
    partners: int
    well_covered: int  # partners whose route is covered for at least 90 % of its length
    mean_covered: float
    worst_gap_hours: float


def country_coverage(
    world: dict[str, Port], home: str, cells: Cells | None, ports: tuple[str, ...] = ()
) -> list[PartnerRow]:
    """Coverage of the routes from the home country's ports to every other country, shortest blind first.

    Each row names a partner country, a port of it, the covered share and the longest blind stretch. With
    several home ports the route taken is the best covered of them."""
    mine = [world[p] for p in ports if p in world] or [p for p in world.values() if p.country == home]
    mine = [p for p in mine if p.node in lanes.NODES]
    rows: list[PartnerRow] = []
    for country in sorted({p.country for p in world.values()} - {home}):
        there = principal_port(world, country)
        if there is None or not mine:
            continue
        best = min(
            (route_coverage(lanes.chain(p.node, there.node), cells) for p in mine),
            key=lambda c: (-c.covered, c.longest_gap_km),
        )
        rows.append(
            PartnerRow(
                country,
                there.name,
                round(best.covered, 3),
                round(best.longest_gap_km),
                round(best.gap_hours, 1),
                round(best.length_km),
                best.basis,
            )  # fmt: skip
        )
    # the shortest blind stretch first: a ship out of sight for a day is still followed (estimated along the
    # lanes), one out of sight for two weeks is not
    return sorted(rows, key=lambda r: (r.gap_km, -r.covered))


def rank_home_countries(world: dict[str, Port], cells: Cells | None) -> list[HomeRow]:
    """Every country as a place to monitor from, best first: the mean covered share of its routes to all the
    others, so a hub whose voyages hug coasts ranks above one an ocean away from everyone."""
    out: list[HomeRow] = []
    for home in sorted({p.country for p in world.values()}):
        rows = country_coverage(world, home, cells)
        if rows:
            good = sum(1 for r in rows if r.covered >= 0.9)
            mean = round(sum(r.covered for r in rows) / len(rows), 3)
            out.append(HomeRow(home, len(rows), good, mean, max(r.gap_hours for r in rows)))
    return sorted(out, key=lambda r: (-r.mean_covered, r.worst_gap_hours))


def blind_stretches(
    world: dict[str, Port], home: str, cells: Cells | None, ports: tuple[str, ...], countries: set[str]
) -> list[dict[str, object]]:
    """The stretches of the routes from the home ports to each country that no coastal station hears, as
    polylines [[lat, lon], ...] (longitudes -180..180) to draw: where a ship has to be estimated."""
    mine = [world[p] for p in ports if p in world] or [p for p in world.values() if p.country == home]
    mine = [p for p in mine if p.node in lanes.NODES]
    out: list[dict[str, object]] = []
    for country in sorted(countries - {home}):
        there = principal_port(world, country)
        if there is None or not mine:
            continue
        best = min(
            (route_coverage(lanes.chain(p.node, there.node), cells) for p in mine),
            key=lambda c: (-c.covered, c.longest_gap_km),
        )
        origin = next(p for p in mine if route_coverage(lanes.chain(p.node, there.node), cells) == best)
        dense, seen, _ = _walk(lanes.chain(origin.node, there.node), cells)
        run: list[tuple[float, float]] = []
        for point, heard in [*zip(dense, seen, strict=True), ((0.0, 0.0), True)]:
            if not heard:
                run.append(point)
                continue
            if len(run) >= 2:
                km = sum(haversine_km(a[0], a[1], b[0], b[1]) for a, b in zip(run, run[1:], strict=False))
                out.append(
                    {
                        "country": country,
                        "to": there.name,
                        "km": round(km),
                        "path": [[lat, round(lanes.norm_lon(lon), 4)] for lat, lon in run],
                    }
                )
            run = []
    return out


def reach_outline(cells: Cells) -> list[list[list[float]]]:
    """The boundary of the area where ships were heard: the edges of heard squares that have no heard
    neighbour, as [[lat, lon], [lat, lon]] pairs. The resolution is the survey's square (2 degrees)."""
    heard = {k for k, n in cells.counts.items() if n > 0}
    size, edges = cells.size, []
    for r, c in heard:
        south, west = r * size, c * size - 180
        north, east = south + size, west + size
        if (r - 1, c) not in heard:
            edges.append([[south, west], [south, east]])
        if (r + 1, c) not in heard:
            edges.append([[north, west], [north, east]])
        if (r, c - 1) not in heard:
            edges.append([[south, west], [north, west]])
        if (r, c + 1) not in heard:
            edges.append([[south, east], [north, east]])
    return edges
