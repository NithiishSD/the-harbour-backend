"""A country's territorial sea and EEZ, from Marine Regions, into geo/maritime_zones.geojson.

    python -m ingest.maritime_zones SG SGP           # country code, then the alpha-3 code Marine Regions uses
    python -m ingest.maritime_zones NL NLD US USA    # several countries in one go

The dashboard shows every ship inside these zones (common.maritime, Monitor.roles `in`). Source: Marine
Regions, Flanders Marine Institute (VLIZ), Maritime Boundaries, 12 nautical mile zone and EEZ layers
(https://www.marineregions.org, CC BY 4.0). They are the delimited zones: where a neighbour's claim overlaps,
the part is not the country's, so Singapore's 12 NM sea and its EEZ come out as one small area.
Running it again replaces that country's zones and keeps the others.
"""

import json
import sys
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from common.config import settings

WFS = "https://geo.vliz.be/geoserver/MarineRegions/ows"
LAYERS = {"territorial": "MarineRegions:eez_12nm", "eez": "MarineRegions:eez"}
DECIMALS = 5  # about a metre


def fetch(layer: str, iso3: str) -> list[dict[str, Any]]:
    query = urllib.parse.urlencode(
        {
            "service": "WFS",
            "version": "1.0.0",
            "request": "GetFeature",
            "typeName": layer,
            "outputFormat": "application/json",
            "cql_filter": f"iso_ter1='{iso3}'",
        }
    )
    with urllib.request.urlopen(f"{WFS}?{query}", timeout=90) as resp:  # noqa: S310 - fixed https URL
        features: list[dict[str, Any]] = json.load(resp)["features"]
    return features


def _round(rings: Any) -> Any:
    if isinstance(rings[0], int | float):
        return [round(rings[0], DECIMALS), round(rings[1], DECIMALS)]
    return [_round(r) for r in rings]


def zones_for(country: str, iso3: str) -> list[dict[str, Any]]:
    out = []
    for kind, layer in LAYERS.items():
        for f in fetch(layer, iso3):
            props = f.get("properties") or {}
            out.append(
                {
                    "type": "Feature",
                    "properties": {
                        "country": country,
                        "iso3": iso3,
                        "kind": kind,
                        "name": props.get("geoname"),
                        "area_km2": props.get("area_km2"),
                        "source": "Marine Regions (VLIZ), CC BY 4.0",
                    },
                    "geometry": {
                        "type": f["geometry"]["type"],
                        "coordinates": _round(f["geometry"]["coordinates"]),
                    },
                }
            )
    return out


def write(path: Path, additions: list[dict[str, Any]], countries: set[str]) -> int:
    kept: list[dict[str, Any]] = []
    if path.is_file():
        kept = [
            f
            for f in json.loads(path.read_text()).get("features", [])
            if f["properties"].get("country") not in countries
        ]
    doc = {"type": "FeatureCollection", "features": kept + additions}
    path.write_text(json.dumps(doc, separators=(",", ":")))
    return len(additions)


def main() -> None:
    args = sys.argv[1:]
    if not args or len(args) % 2:
        raise SystemExit(__doc__)
    pairs = list(zip(args[::2], args[1::2], strict=True))
    found: list[dict[str, Any]] = []
    for country, iso3 in pairs:
        zones = zones_for(country.upper(), iso3.upper())
        if not zones:
            print(f"{country}: Marine Regions has no zone for {iso3}")
        found += zones
    n = write(settings().geo_dir / "maritime_zones.geojson", found, {c.upper() for c, _ in pairs})
    print(f"wrote {n} zones")


if __name__ == "__main__":
    main()
