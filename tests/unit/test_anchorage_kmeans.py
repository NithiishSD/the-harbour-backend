import json
import random

from batch.anchorage_kmeans import learn
from common import geo
from common.geo import haversine_km, load_zones
from tests.history import write_history
from tests.scenarios import LB_ANCHORAGE, LB_BERTH, MIN, position

T0 = 1_672_617_600_000
LAX_STOP = (33.66, -118.28)  # port areas, outside every berth and anchorage polygon
LGB_STOP = (33.75, -118.12)
LAX_FEW = (33.77, -118.30)


def stops(first: int, ships: int, where: tuple[float, float], ship_type: str, types: dict[str, str]):
    """`ships` ships, 6 stopped positions each, scattered within ~200 m (every position is its own cell)."""
    rng = random.Random(first)
    out = []
    for s in range(ships):
        mmsi = str(first + s)
        types[mmsi] = ship_type
        for i in range(6):
            spot = (where[0] + rng.uniform(-0.002, 0.002), where[1] + rng.uniform(-0.002, 0.002))
            out.append(position(mmsi, T0 + (s * 6 + i) * 10 * MIN, spot, 0.1, 1))
    return out


def test_kmeans_learns_anchorages_from_stopped_cargo_ships(spark, tmp_path, monkeypatch):
    types: dict[str, str] = {}
    positions = (
        stops(367100000, 5, LAX_STOP, "cargo", types)
        + stops(367200000, 5, LGB_STOP, "tanker", types)
        + stops(367300000, 2, LAX_FEW, "cargo", types)  # only 2 ships: not a cluster
        + stops(367400000, 5, LGB_STOP, "tug", types)  # tugs are not anchoring
        + stops(367500000, 5, LB_BERTH, "cargo", types)  # inside a berth polygon
        + stops(367600000, 5, LB_ANCHORAGE, "cargo", types)  # already a hand-drawn anchorage
    )
    write_history(spark, tmp_path, positions, types, T0)

    summary = learn(spark, tmp_path)
    # Tugs, berth stops and the known anchorage never reach the clustering.
    assert set(summary["points_per_port"]) == {"USLAX", "USLGB"}
    assert [(z["name"], z["ships"]) for z in summary["zones"]] == [
        ("learned USLAX 1", 5),
        ("learned USLGB 1", 5),
    ]
    assert all(
        20 <= z["points"] <= 30 for z in summary["zones"]
    )  # 6 positions per ship, minus repeats per cell
    doc = json.loads((tmp_path / "geo" / "learned_anchorages.geojson").read_text())
    for feature, where in zip(doc["features"], [LAX_STOP, LGB_STOP], strict=True):
        ring = feature["geometry"]["coordinates"][0][:-1]
        lat = sum(p[1] for p in ring) / len(ring)
        lon = sum(p[0] for p in ring) / len(ring)
        assert haversine_km(lat, lon, *where) < 0.1
        assert (
            feature["properties"]["kind"] == "anchorage" and 0.25 <= feature["properties"]["radius_km"] <= 2
        )

    # The streaming job's zone loader picks the file up; the zone then sits where the ships stopped.
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    geo.settings.cache_clear()
    try:
        index = geo.ZoneIndex(load_zones())
        assert (
            (zone := index.lookup(*LAX_STOP)) and zone.kind == "anchorage" and zone.name == "learned USLAX 1"
        )
        assert (zone := geo.ZoneIndex(load_zones(learned=False)).lookup(*LAX_STOP)) and zone.kind == "area"
    finally:
        geo.settings.cache_clear()

    # A rerun replaces the file; --include-known also learns the hand-drawn anchorage again.
    again = learn(spark, tmp_path, include_known=True)
    assert [z["name"] for z in again["zones"]] == ["learned USLAX 1", "learned USLGB 1", "learned USLGB 2"]
