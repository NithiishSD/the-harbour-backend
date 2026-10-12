"""A country's territorial sea and exclusive economic zone as polygons (geo/maritime_zones.geojson).

Written by `python -m ingest.maritime_zones` from Marine Regions. A ship inside either zone is "in the
country's waters". Rings are (lon, lat) as in GeoJSON; every public function takes (lat, lon). A polygon's
holes are the islands of the country: they are left out of the drawing, not of the test.
"""

import json
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Any

from common.config import settings
from common.geo import point_in_ring

Ring = tuple[tuple[float, float], ...]
Box = tuple[float, float, float, float]  # min_lon, min_lat, max_lon, max_lat


@dataclass(frozen=True)
class Polygon:
    box: Box
    outer: Ring
    holes: tuple[Ring, ...]


@dataclass(frozen=True)
class Area:
    kind: str  # territorial | eez
    name: str
    polygons: tuple[Polygon, ...]


def _ring(points: Any) -> Ring:
    return tuple((float(x), float(y)) for x, y in points)


def _area(feature: dict[str, Any]) -> Area:
    geometry = feature["geometry"]
    shapes = geometry["coordinates"] if geometry["type"] == "MultiPolygon" else [geometry["coordinates"]]
    polygons = []
    for shape in shapes:
        outer = _ring(shape[0])
        xs, ys = [p[0] for p in outer], [p[1] for p in outer]
        box = (min(xs), min(ys), max(xs), max(ys))
        polygons.append(Polygon(box, outer, tuple(_ring(h) for h in shape[1:])))
    props = feature.get("properties") or {}
    return Area(props.get("kind", "territorial"), props.get("name") or "", tuple(polygons))


@cache
def _load(path: Path) -> dict[str, tuple[Area, ...]]:
    if not path.is_file():
        return {}
    out: dict[str, list[Area]] = {}
    for f in json.loads(path.read_text()).get("features", []):
        out.setdefault(f["properties"]["country"], []).append(_area(f))
    return {k: tuple(v) for k, v in out.items()}


def zones_of(country: str) -> tuple[Area, ...]:
    """The country's zones, empty when geo/maritime_zones.geojson has none for it."""
    return _load(settings().geo_dir / "maritime_zones.geojson").get(country, ())


def contains(areas: tuple[Area, ...], lat: float, lon: float) -> bool:
    """Inside the outer ring of any zone: the bounding box first, the ring only past it. The islands are not
    cut out: a berth is on the shoreline, which the coarse island outline may cover, and a ship at a quay is
    in the country's waters."""
    for area in areas:
        for poly in area.polygons:
            min_lon, min_lat, max_lon, max_lat = poly.box
            if not (min_lon <= lon <= max_lon and min_lat <= lat <= max_lat):
                continue
            if point_in_ring(lon, lat, poly.outer):
                return True
    return False


def polygons(areas: tuple[Area, ...]) -> list[dict[str, Any]]:
    """To draw: [{kind, name, ring: [[lat, lon], ...], holes: [[[lat, lon], ...], ...]}]."""

    def swap(ring: Ring) -> list[list[float]]:
        return [[lat, lon] for lon, lat in ring]

    return [
        {"kind": a.kind, "name": a.name, "ring": swap(p.outer), "holes": [swap(h) for h in p.holes]}
        for a in areas
        for p in a.polygons
    ]
