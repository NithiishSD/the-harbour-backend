"""Ports, geofence zones and point-in-polygon lookup.

GeoJSON stores coordinates as [lon, lat]. Every public function here takes (lat, lon).
Zone lookup priority: berth, then anchorage, then the port's wider area.
"""

import csv
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from common.config import settings

KIND_PRIORITY = {"berth": 0, "anchorage": 1, "area": 2}


@dataclass(frozen=True)
class Port:
    port_id: str
    name: str
    country: str
    lat: float
    lon: float
    node: str = ""  # where the port joins the sea-lane graph (common.lanes)


@dataclass(frozen=True)
class Zone:
    port_id: str
    kind: str  # area | anchorage | berth
    name: str
    ring: tuple[tuple[float, float], ...]  # (lon, lat) vertices, outer ring only
    bbox: tuple[float, float, float, float]  # min_lon, min_lat, max_lon, max_lat


def point_in_ring(lon: float, lat: float, ring: tuple[tuple[float, float], ...]) -> bool:
    """Ray casting: count edge crossings of a ray going east from the point."""
    inside = False
    j = len(ring) - 1
    for i in range(len(ring)):
        xi, yi = ring[i]
        xj, yj = ring[j]
        if (yi > lat) != (yj > lat) and lon < (xj - xi) * (lat - yi) / (yj - yi) + xi:
            inside = not inside
        j = i
    return inside


def load_ports(path: Path | None = None) -> dict[str, Port]:
    path = path or settings().geo_dir / "ports.csv"
    with path.open(newline="") as f:
        return {
            r["port_id"]: Port(
                r["port_id"], r["name"], r["country"], float(r["lat"]), float(r["lon"]), r.get("node") or ""
            )
            for r in csv.DictReader(f)
        }


def load_world_ports() -> dict[str, Port]:
    """The monitored ports (geo/ports.csv) first, then the other ports ships sail to (geo/world_ports.csv).

    Monitored ports come first so a name that both lists know is the monitored one."""
    ports = load_ports()
    extra = settings().geo_dir / "world_ports.csv"
    return (
        {**ports, **{k: v for k, v in load_ports(extra).items() if k not in ports}}
        if extra.is_file()
        else ports
    )


def _zones_from_geojson(doc: dict[str, Any]) -> list[Zone]:
    zones = []
    for feature in doc.get("features", []):
        props = feature.get("properties") or {}
        geom = feature.get("geometry") or {}
        if geom.get("type") != "Polygon" or props.get("kind") not in KIND_PRIORITY:
            continue
        ring = tuple((float(x), float(y)) for x, y in geom["coordinates"][0])
        xs, ys = [p[0] for p in ring], [p[1] for p in ring]
        zones.append(
            Zone(
                props["port_id"],
                props["kind"],
                props.get("name", ""),
                ring,
                (min(xs), min(ys), max(xs), max(ys)),
            )
        )
    return zones


def load_zones(extra: list[Path] | None = None, learned: bool = True) -> list[Zone]:
    """Hand-drawn zones from geo/, plus learned anchorages when the batch job has produced them."""
    s = settings()
    paths = [
        s.geo_dir / "geofences.geojson",
        *([s.data_dir / "geo" / "learned_anchorages.geojson"] if learned else []),
        *(extra or []),
    ]
    zones: list[Zone] = []
    for path in paths:
        if path.is_file():
            zones.extend(_zones_from_geojson(json.loads(path.read_text())))
    return sorted(zones, key=lambda z: KIND_PRIORITY[z.kind])


GRID_SIZE_DEG = 0.5  # ~55km spatial bin for O(1) containment pre-filtering


class ZoneIndex:
    def __init__(self, zones: list[Zone]) -> None:
        self.zones = sorted(zones, key=lambda z: KIND_PRIORITY[z.kind])
        self._learned_mtime: float | None = None
        self._build_grid()

    def _build_grid(self) -> None:
        self._grid: dict[tuple[int, int], list[Zone]] = {}
        for z in self.zones:
            min_lon, min_lat, max_lon, max_lat = z.bbox
            lat_start = int(math.floor(min_lat / GRID_SIZE_DEG))
            lat_end = int(math.floor(max_lat / GRID_SIZE_DEG))
            lon_start = int(math.floor(min_lon / GRID_SIZE_DEG))
            lon_end = int(math.floor(max_lon / GRID_SIZE_DEG))
            for b_lat in range(lat_start, lat_end + 1):
                for b_lon in range(lon_start, lon_end + 1):
                    self._grid.setdefault((b_lat, b_lon), []).append(z)

    def lookup(self, lat: float, lon: float) -> Zone | None:
        """Most specific zone containing the point, or None."""
        bin_key = (int(math.floor(lat / GRID_SIZE_DEG)), int(math.floor(lon / GRID_SIZE_DEG)))
        candidates = self._grid.get(bin_key)
        if not candidates:
            return None
        for z in candidates:
            min_lon, min_lat, max_lon, max_lat = z.bbox
            if min_lon <= lon <= max_lon and min_lat <= lat <= max_lat and point_in_ring(lon, lat, z.ring):
                return z
        return None

    def area_of(self, lat: float, lon: float) -> str | None:
        """Port whose wider area contains the point (used for 'other ships in the same area')."""
        bin_key = (int(math.floor(lat / GRID_SIZE_DEG)), int(math.floor(lon / GRID_SIZE_DEG)))
        candidates = self._grid.get(bin_key)
        if not candidates:
            return None
        for z in candidates:
            if z.kind == "area" and point_in_ring(lon, lat, z.ring):
                return z.port_id
        return None

    def refresh_learned(self, data_dir: Path | None = None) -> bool:
        """Dynamically reload learned anchorages if updated by the batch KMeans job."""
        s = settings()
        target = (data_dir or s.data_dir) / "geo" / "learned_anchorages.geojson"
        if not target.is_file():
            return False
        current_mtime = target.stat().st_mtime
        if self._learned_mtime is not None and current_mtime <= self._learned_mtime:
            return False
        self._learned_mtime = current_mtime
        self.zones = load_zones()
        self._build_grid()
        return True


def circle_ring(lat: float, lon: float, radius_km: float, points: int = 16) -> list[list[float]]:
    """Closed polygon approximating a circle, as GeoJSON [lon, lat] pairs."""
    dlat = radius_km / 111.32
    dlon = radius_km / (111.32 * max(math.cos(math.radians(lat)), 1e-6))
    ring = [
        [
            round(lon + dlon * math.cos(2 * math.pi * k / points), 6),
            round(lat + dlat * math.sin(2 * math.pi * k / points), 6),
        ]
        for k in range(points)
    ]
    return [*ring, ring[0]]


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * 6371.0 * math.asin(math.sqrt(a))
