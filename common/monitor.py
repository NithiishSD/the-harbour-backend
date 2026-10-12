"""What this dashboard watches: one country and a selection of its ports.

    MONITOR_COUNTRY=SG          ISO country code; every port of it in geo/ is a candidate
    MONITOR_PORTS=              UN/LOCODEs of the ports to monitor; empty means all the country's ports
    MONITOR_PARTNERS=auto       the 5-10 source countries: `auto` picks those whose voyages coastal AIS
                                follows end to end (common.coverage); the dashboard can narrow the choice
    MONITOR_WATERS=             the country's sea as boxes `min_lat,min_lon,max_lat,max_lon;...` (else the
                                geo/national_waters.csv, else circles of MONITOR_REGION_KM round its ports)
    MONITOR_REGION_KM=150       how far a port's waters reach when there is no box for it
    MONITOR_MAX_PARTNERS / MONITOR_MIN_PARTNERS / MONITOR_FOLLOWABLE_HOURS   how many source countries and
                                what counts as a followed voyage
    MONITOR_SOURCE_PORT_KM / MONITOR_STOP_KM / MONITOR_LEFT_KM / MONITOR_SLOW_KN   where a voyage began
    (the map's keep times, MAP_TRANSIT_HOURS, MAP_ARRIVED_HOURS, MAP_SILENT_HOURS, are in dashboard.mapview)

A ship is tracked for exactly three reasons, wherever in the world it is, and any ship of any flag:
  to       it started in one of the partner countries AND its crew typed a monitored port as the destination
  from     it started at a monitored port AND its destination is a port of a partner country
  in       it is inside the country's waters, for as long as it is there
Both ends of a voyage are checked, so a ship from a partner country bound anywhere else (or from anywhere
else to a monitored port) is not tracked, and neither is one whose source is not known (it was already under
way when the feed first heard it). The partner countries are the source countries chosen in the dashboard
(default MONITOR_PARTNERS, 5-10 whose coasts the feed follows end to end, common.coverage). A ship's role is
worked out from what it reported: the first position seen (its source), the destination typed and where it
is now.
"""

import csv
from collections import Counter
from dataclasses import dataclass
from functools import cache
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from common import coverage, lanes, maritime, selection
from common.config import settings
from common.geo import Port, ZoneIndex, haversine_km, load_zones
from dashboard import destination

Box = tuple[float, float, float, float]  # min_lat, min_lon, max_lat, max_lon


@cache
def _zone_index() -> ZoneIndex:
    return ZoneIndex(load_zones())


def timezone_of(country: str) -> str:
    """The IANA time zone the dashboard shows times in: MONITOR_TIMEZONE for the monitored country, else the
    country's row of geo/timezones.csv, else UTC. A name the system does not know is UTC too."""
    s = settings()
    name = s.monitor_timezone if country == s.monitor_country and s.monitor_timezone else ""
    path = s.geo_dir / "timezones.csv"
    if not name and path.is_file():
        with path.open(newline="") as f:
            name = next((r["timezone"] for r in csv.DictReader(f) if r["country"] == country), "")
    try:
        ZoneInfo(name or "UTC")
    except ZoneInfoNotFoundError:
        return "UTC"
    return name or "UTC"


_BOUND: dict[tuple[int, str], str | None] = {}


def _bound_for(text: str | None, world: dict[str, Port]) -> str | None:
    """The port a destination text names (dashboard.destination), remembered: the same few hundred texts come
    back on every poll of every ship."""
    if not text:
        return None
    key = (id(world), text)
    if key not in _BOUND:
        if len(_BOUND) > 50_000:
            _BOUND.clear()
        _BOUND[key] = destination.match(text, world)
    return _BOUND[key]


@cache
def _csv_waters() -> dict[str, tuple[Box, ...]]:
    path = settings().geo_dir / "national_waters.csv"
    out: dict[str, list[Box]] = {}
    if path.is_file():
        with path.open(newline="") as f:
            for r in csv.DictReader(f):
                box = (float(r["min_lat"]), float(r["min_lon"]), float(r["max_lat"]), float(r["max_lon"]))
                out.setdefault(r["country"], []).append(box)
    return {k: tuple(v) for k, v in out.items()}


def waters_of(country: str) -> tuple[Box, ...] | None:
    """The boxes that stand for a country's own sea: MONITOR_WATERS for the monitored country when it is set,
    else geo/national_waters.csv; None means the circles of MONITOR_REGION_KM round the country's ports."""
    s = settings()
    if s.monitor_waters and country == s.monitor_country:
        return s.monitor_waters
    return _csv_waters().get(country)


@dataclass(frozen=True)
class Monitor:
    country: str
    ports: tuple[str, ...]  # UN/LOCODEs, all of `country`
    partners: frozenset[str]
    region_km: float

    @classmethod
    def build(
        cls,
        world: dict[str, Port],
        country: str,
        ports: tuple[str, ...] = (),
        partners: frozenset[str] | tuple[str, ...] = (),
        region_km: float = 150.0,
    ) -> "Monitor":
        """Ports not of the country, or unknown, are ignored; none left means all the country has."""
        of_country = [p.port_id for p in world.values() if p.country == country]
        chosen = tuple(p for p in ports if p in of_country) or tuple(of_country)
        return cls(country, chosen, frozenset(partners) - {country}, region_km)

    def waters(self, world: dict[str, Port]) -> list[dict[str, Any]]:
        """The country's sea as the map draws it: the boxes of MONITOR_WATERS, else its territorial sea and
        EEZ (polygons), else the boxes of geo/national_waters.csv, else a circle round each of its ports."""
        explicit = settings().monitor_waters and self.country == settings().monitor_country
        zones = () if explicit else maritime.zones_of(self.country)
        if zones:
            return maritime.polygons(zones)
        boxes = waters_of(self.country)
        if boxes:
            return [{"min_lat": b[0], "min_lon": b[1], "max_lat": b[2], "max_lon": b[3]} for b in boxes]
        return [
            {"lat": p.lat, "lon": p.lon, "radius_km": self.region_km}
            for p in world.values()
            if p.country == self.country
        ]

    def in_waters(self, lat: float, lon: float, world: dict[str, Port]) -> bool:
        """Inside the country's sea: the territorial sea or EEZ polygons, else one of its boxes, else within
        the region of any of its ports."""
        explicit = settings().monitor_waters and self.country == settings().monitor_country
        zones = () if explicit else maritime.zones_of(self.country)
        if zones:
            return maritime.contains(zones, lat, lon)
        boxes = waters_of(self.country)
        if boxes:
            return any(b[0] <= lat <= b[2] and b[1] <= lon <= b[3] for b in boxes)
        return any(
            haversine_km(lat, lon, p.lat, p.lon) <= self.region_km
            for p in world.values()
            if p.country == self.country
        )

    def source_port(self, lat: float | None, lon: float | None, world: dict[str, Port]) -> Port | None:
        """The port the first report was in or next to."""
        if lat is None or lon is None:
            return None
        near = min(world.values(), key=lambda p: haversine_km(lat, lon, p.lat, p.lon), default=None)
        if near and haversine_km(lat, lon, near.lat, near.lon) <= settings().monitor_source_port_km:
            return near
        return None

    def roles(self, ship: dict[str, Any], world: dict[str, Port]) -> list[str]:
        """['to', 'from', 'in'] (why the ship is tracked; none: it is not) for a ship dict (lat, lon,
        destination, src_*, state):

        to    its destination is a monitored port and it started in a chosen source country
        from  it started at a monitored port, wherever it goes (or has not said)
        in    it is inside the country's territorial sea or EEZ
        """
        out: list[str] = []
        inside = self.in_waters(ship["lat"], ship["lon"], world)
        bound = _bound_for(ship.get("destination"), world)
        if ship.get("state") != "berthed":
            if bound in self.ports:  # the source is only looked up for a ship bound for us
                origin = self.source_port(ship.get("src_lat"), ship.get("src_lon"), world)
                if origin and origin.country in self.partners:
                    out.append("to")
            # it has left: outside the waters, or inside with a destination that is not ours
            left = not inside or (bound is not None and bound not in self.ports)
            if left and self._started_here(ship, world):
                out.append("from")
        if inside and not out:
            out.append("in")
        return out

    def _started_here(self, ship: dict[str, Any], world: dict[str, Port]) -> bool:
        """Was the ship first heard at one of the monitored ports: in a berth or an anchorage of it (a ship
        first heard in the port's wide approach area is passing, not starting), or, for a port with none
        drawn, within MONITOR_STOP_KM of it?"""
        lat, lon = ship.get("src_lat"), ship.get("src_lon")
        if lat is None or lon is None:
            return False
        zone = _zone_index().lookup(lat, lon)
        if zone is not None and zone.kind != "area":
            return zone.port_id in self.ports
        limit = settings().monitor_stop_km
        ports = [world[p] for p in self.ports if p in world]
        return any(haversine_km(lat, lon, p.lat, p.lon) <= limit for p in ports)

    def kind(
        self, ship: dict[str, Any], world: dict[str, Port], roles: list[str] | None = None
    ) -> str | None:
        """Why the ship is of interest, for the one drop-down:

        monitored  a voyage of ours: it started at a monitored port or is bound for one (`from`, `to`, or a
                   destination that is a monitored port with the source not known)
        crossing   in the country's waters on a voyage of its own: its destination is known and outside the
                   countries we list (the monitored one and the chosen sources), and so is its source if
                   known; it does not come to our port, it uses our water
        other      in the waters, voyage not known or not clearly one of the two
        None       not tracked
        The ship's flag plays no part."""
        roles = self.roles(ship, world) if roles is None else roles
        bound = _bound_for(ship.get("destination"), world)
        if "from" in roles or "to" in roles or (roles and bound in self.ports):
            return "monitored"  # a ship bound for our port is ours even when its source is not known
        if "in" not in roles:
            return None
        listed = self.partners | {self.country}
        if bound is None or world[bound].country in listed:
            return "other"
        origin = self.source_port(ship.get("src_lat"), ship.get("src_lon"), world)
        if origin is not None and origin.country == self.country and not self._started_here(ship, world):
            origin = None  # first heard in our own waters, not at a berth: it did not start here
        return "other" if origin is not None and origin.country in listed else "crossing"

    @classmethod
    def validate(cls, world: dict[str, Port], country: str, ports: tuple[str, ...] = ()) -> None:
        """Raise ValueError for a country with no port in geo/, or ports that are not of the country: a
        monitoring is never silently turned into another one."""
        of_country = {p.port_id for p in world.values() if p.country == country}
        if not of_country:
            raise ValueError(f"unknown country {country!r}: no port of it in geo/")
        stray = [p for p in ports if p not in of_country]
        if stray:
            raise ValueError(f"ports not of {country}: {', '.join(stray)}")

    @classmethod
    def from_settings(cls, world: dict[str, Port]) -> "Monitor":
        """The environment's monitoring: MONITOR_COUNTRY, MONITOR_PORTS, MONITOR_PARTNERS."""
        s = settings()
        partners: tuple[str, ...] = s.monitor_partners
        if partners == ("AUTO",):
            partners = recommend_partners(world, s.monitor_country, s.monitor_ports)
        return cls.build(world, s.monitor_country, s.monitor_ports, partners, s.monitor_region_km)

    @classmethod
    def selected(cls, world: dict[str, Port], base: "Monitor | None" = None) -> "Monitor":
        """The environment's monitoring with the ports and source countries the viewer chose
        (common.selection) in place of its own, when there is a choice for this country."""
        base = cls.from_settings(world)
        chosen = selection.load(settings().data_dir, base.country)
        if chosen is None:
            return base
        known = {p.country for p in world.values()}
        valid = [c for c in chosen["partners"] if c in known and c != base.country]
        partners = tuple(valid[: settings().monitor_max_partners])
        return cls.build(world, base.country, tuple(chosen["ports"]), partners, base.region_km)

    def voyages(self, ships: list[dict[str, Any]], world: dict[str, Port]) -> "Counter[str]":
        """Ships seen on voyages between the monitored ports and each other country (both ends known): the
        country's count is what makes it worth following."""
        found: Counter[str] = Counter()
        for s in ships:
            bound = _bound_for(s.get("destination"), world)
            if bound is None:
                continue
            origin = self.source_port(s.get("src_lat"), s.get("src_lon"), world)
            if origin is None:
                continue
            if bound in self.ports and origin.country != self.country:
                found[origin.country] += 1
            elif origin.port_id in self.ports and world[bound].country != self.country:
                found[world[bound].country] += 1
        return found

    def far_ports(self, world: dict[str, Port], extra_countries: tuple[str, ...] = ()) -> list[Port]:
        """The ports at the other end of this monitoring's voyages: those of the partner countries (only the
        chosen source ports when some are set) and of `extra_countries`, the countries the country's own ships
        were seen to sail to."""
        countries = self.partners | set(extra_countries)
        out = [p for p in world.values() if p.country in countries and p.node in lanes.NODES]
        return out

    def edges(self, world: dict[str, Port], extra_countries: tuple[str, ...] = ()) -> list[tuple[str, str]]:
        """The lanes (pairs of lane node ids) of the voyages between the monitored ports and the far ports:
        the only part of the lane graph this monitoring has any use for."""
        found: set[tuple[str, str]] = set()
        for a in (world[p] for p in self.ports):
            if a.node not in lanes.NODES:
                continue
            for b in self.far_ports(world, extra_countries):
                nodes = lanes.chain(a.node, b.node)
                found |= {tuple(sorted(pair)) for pair in zip(nodes, nodes[1:], strict=False)}  # type: ignore[misc]
        return sorted(found)

    def nodes(self, world: dict[str, Port], extra_countries: tuple[str, ...] = ()) -> list[str]:
        """The lane nodes of those lanes, plus the nodes the country's own ports join at."""
        ids = {n for e in self.edges(world, extra_countries) for n in e}
        ids |= {world[p].node for p in self.ports if world[p].node in lanes.NODES}
        return sorted(ids)

    def ais_boxes(self, world: dict[str, Port], extra_countries: tuple[str, ...] = ()) -> tuple[Box, ...]:
        """Where the live AIS subscription looks: round the monitored ports (MONITOR_AIS_REGION_KM), round
        every port of the chosen source countries (MONITOR_AIS_SOURCE_KM) and along the lanes between them
        (MONITOR_AIS_PAD squares either side). Nowhere else."""
        s = settings()
        around = [
            (p.lat, p.lon, max(self.region_km, s.monitor_ais_region_km))
            for p in world.values()
            if p.country == self.country
        ]
        theirs = self.partners | set(extra_countries)
        around += [(p.lat, p.lon, s.monitor_ais_source_km) for p in world.values() if p.country in theirs]
        return lanes.corridor_boxes(
            pad=s.monitor_ais_pad, edges=self.edges(world, extra_countries), around=around
        )

    def weather_points(
        self, world: dict[str, Port], extra_countries: tuple[str, ...] = ()
    ) -> list[tuple[str, float, float]]:
        """(point id, lat, lon) of the weather points: the lane nodes of this monitoring's voyages only."""
        return [
            (f"LANE-{n}", lanes.NODES[n][0], round(lanes.norm_lon(lanes.NODES[n][1]), 2))
            for n in self.nodes(world, extra_countries)
        ]


COVERAGE_FILE = "coverage/cells.json"


def coverage_cells() -> "coverage.Cells | None":
    """The survey of where ships are heard (python -m ingest.coverage_survey), when there is one."""
    return coverage.load_cells(settings().data_dir / COVERAGE_FILE)


def recommend_partners(
    world: dict[str, Port], country: str, ports: tuple[str, ...] = (), limit: int | None = None
) -> tuple[str, ...]:
    """The partner countries whose voyages to this country can be followed from end to end, best first: the
    routes blind for no longer than MONITOR_FOLLOWABLE_HOURS (the position is estimated across the gap),
    topped up to MONITOR_MIN_PARTNERS with the next shortest, at most `limit` (MONITOR_MAX_PARTNERS)."""
    s = settings()
    rows = coverage.country_coverage(world, country, coverage_cells(), ports)
    followable = [r for r in rows if r.gap_hours <= s.monitor_followable_hours]
    chosen = followable if len(followable) >= s.monitor_min_partners else rows[: s.monitor_min_partners]
    return tuple(r.country for r in chosen[: limit or s.monitor_max_partners])


def suggest_partners(
    world: dict[str, Port], monitor: Monitor, ships: list[dict[str, Any]], limit: int | None = None
) -> tuple[str, ...]:
    """The source countries worth following: first those the ships actually seen are sailing to and from the
    monitored ports (most ships first), then, up to MONITOR_MIN_PARTNERS, the best followed by coastal AIS."""
    s = settings()
    top = limit or s.monitor_max_partners
    ranked = [c for c, _ in monitor.voyages(ships, world).most_common()][:top]
    if len(ranked) < s.monitor_min_partners:
        for c in recommend_partners(world, monitor.country, monitor.ports):
            if c not in ranked and len(ranked) < s.monitor_min_partners:
                ranked.append(c)
    return tuple(ranked[:top])
