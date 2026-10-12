"""MLlib KMeans: where do cargo ships and tankers actually sit still? -> learned anchorage zones.

    python -m batch.anchorage_kmeans [--date 2023-01-01]

1. Stopped positions (SOG < 0.5 kn) of cargo ships and tankers from the clean history (up to --date).
   Only points inside a port area but outside every hand-drawn berth and anchorage count: the job finds
   anchorages the geofences miss. Each ship counts once per ~100 m cell, so one ship that sits for a week
   does not outweigh ten that stop for an hour.
2. Per port: positions in kilometres around the port, KMeans for k = 2..MAX_K, the k with the best
   silhouette score wins.
3. Clusters whose centres are closer than MERGE_KM are one anchorage and are merged (silhouette needs
   k >= 2, so a port with a single anchorage would otherwise be cut in pieces).
4. A cluster becomes a zone (circle around its centre, radius = 95th percentile distance, 0.25-2 km) when at
   least MIN_SHIPS different ships stopped in it.

--include-known also clusters the points inside hand-drawn anchorages (to check that KMeans finds them again).

Output: data/geo/learned_anchorages.geojson, replaced on every run (a failed run leaves the old file).
The streaming job reads it through common.geo.load_zones when its queries start.
"""

import argparse
import json
import math
import os
from collections.abc import Callable, Iterator
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from pyspark import Broadcast
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from batch.port_calls_sql import KPI_TYPES
from common.config import settings
from common.geo import Port, ZoneIndex, circle_ring, load_ports, load_zones
from common.log import get_logger
from common.spark import session

log = get_logger("batch.anchorage_kmeans")

STOPPED_KN = 0.5
MAX_K = 6
MIN_SHIPS = 3
MERGE_KM = 1.0
MIN_POINTS = 20  # fewer deduplicated points than this in a port: not worth clustering
RADIUS_KM = (0.25, 2.0)
KM_PER_DEG = 111.32
SEED = 42
POINT_SCHEMA = "mmsi string, lat double, lon double, port_id string"


def make_tagger(
    zones: Broadcast[ZoneIndex], kinds: frozenset[str]
) -> Callable[[Iterator[pd.DataFrame]], Iterator[pd.DataFrame]]:
    """mapInPandas body: keep points whose most specific zone is one of `kinds`; tag them with the port."""

    def func(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        for pdf in batches:
            ports = [
                z.port_id if (z := zones.value.lookup(lat, lon)) and z.kind in kinds else None
                for lat, lon in zip(pdf["lat"], pdf["lon"], strict=True)
            ]
            out = pdf.assign(port_id=ports)
            yield out[out["port_id"].notna()][["mmsi", "lat", "lon", "port_id"]]

    return func


def to_km(lat: Any, lon: Any, port: Port) -> tuple[Any, Any]:
    return (
        (lon - port.lon) * KM_PER_DEG * math.cos(math.radians(port.lat)),
        (lat - port.lat) * KM_PER_DEG,
    )


def from_km(x: float, y: float, port: Port) -> tuple[float, float]:
    return port.lat + y / KM_PER_DEG, port.lon + x / (KM_PER_DEG * math.cos(math.radians(port.lat)))


def merge_close(centers: list[Any]) -> dict[int, int]:
    """Cluster id -> group id; clusters with centres closer than MERGE_KM end up in the same group."""
    group = list(range(len(centers)))
    for i in range(len(centers)):
        for j in range(i):
            if np.hypot(*(np.asarray(centers[i]) - np.asarray(centers[j]))) < MERGE_KM:
                old = group[i]
                group = [group[j] if g == old else g for g in group]
    return dict(enumerate(group))


def cluster_port(spark: SparkSession, points: pd.DataFrame, port: Port) -> list[dict[str, Any]]:
    """KMeans over one port's points; returns zone features for the clusters that qualify."""
    x, y = to_km(points["lat"].to_numpy(), points["lon"].to_numpy(), port)
    frame = points.assign(x=x, y=y)[["mmsi", "x", "y"]]
    df = VectorAssembler(inputCols=["x", "y"], outputCol="features").transform(spark.createDataFrame(frame))
    evaluator = ClusteringEvaluator()  # silhouette, squared euclidean
    best = None
    for k in range(2, min(MAX_K, len(frame) // 2) + 1):
        model = KMeans(k=k, seed=SEED, maxIter=50).fit(df)
        score = evaluator.evaluate(model.transform(df))
        if best is None or score > best[0]:
            best = (score, model)
    if best is None:
        return []
    score, model = best
    labelled: pd.DataFrame = (
        model.transform(df).select("mmsi", "x", "y", F.col("prediction").alias("cluster")).toPandas()
    )
    labelled["cluster"] = labelled["cluster"].map(merge_close(model.clusterCenters()))
    features: list[dict[str, Any]] = []
    for _, members in labelled.groupby("cluster"):
        ships = members["mmsi"].nunique()
        if ships < MIN_SHIPS:
            continue
        cx, cy = members["x"].mean(), members["y"].mean()
        dist = np.hypot(members["x"] - cx, members["y"] - cy)
        radius = float(min(max(np.percentile(dist, 95), RADIUS_KM[0]), RADIUS_KM[1]))
        lat, lon = from_km(float(cx), float(cy), port)
        features.append(
            {
                "type": "Feature",
                "properties": {
                    "port_id": port.port_id,
                    "kind": "anchorage",
                    "name": "",
                    "ships": int(ships),
                    "points": len(members),
                    "radius_km": round(radius, 2),
                    "silhouette": round(float(score), 3),
                },
                "geometry": {"type": "Polygon", "coordinates": [circle_ring(lat, lon, radius)]},
            }
        )
    features.sort(key=lambda f: f["geometry"]["coordinates"][0][0])
    for i, f in enumerate(features):
        f["properties"]["name"] = f"learned {f['properties']['port_id']} {i + 1}"
    return features


def learn(
    spark: SparkSession, data_dir: Path, until: date | None = None, include_known: bool = False
) -> dict[str, Any]:
    history = data_dir / "history"
    positions = spark.read.parquet(str(history / "positions"))
    if until:
        positions = positions.where(F.col("date") <= F.lit(until))
    types = (
        spark.read.parquet(str(history / "vessels"))
        .groupBy("mmsi")
        .agg(F.max_by("ship_type", "ts").alias("ship_type"))
    )
    zones = spark.sparkContext.broadcast(ZoneIndex(load_zones(learned=False)))
    points = (
        positions.where(F.col("sog") < STOPPED_KN)
        .join(types.where(f"ship_type IN ({KPI_TYPES})"), "mmsi")
        .select("mmsi", "lat", "lon")
        .mapInPandas(
            make_tagger(zones, frozenset({"area", "anchorage"} if include_known else {"area"})), POINT_SCHEMA
        )
        .withColumn("cell", F.concat_ws(",", F.round("lat", 3), F.round("lon", 3)))
        .dropDuplicates(["mmsi", "cell"])
        .drop("cell")
        .toPandas()
    )
    assert isinstance(points, pd.DataFrame)
    ports = load_ports()
    features: list[dict[str, Any]] = []
    per_port: dict[str, int] = {}
    for port_id, group in points.groupby("port_id"):
        per_port[str(port_id)] = len(group)
        if len(group) >= MIN_POINTS:
            features += cluster_port(spark, group.reset_index(drop=True), ports[str(port_id)])
    out = data_dir / "geo" / "learned_anchorages.geojson"
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".tmp")
    tmp.write_text(json.dumps({"type": "FeatureCollection", "features": features}, indent=1))
    os.replace(tmp, out)
    return {"points_per_port": per_port, "zones": [f["properties"] for f in features], "file": str(out)}


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--date", type=date.fromisoformat, help="use history up to and including this date")
    parser.add_argument(
        "--include-known", action="store_true", help="also cluster inside hand-drawn anchorages"
    )
    args = parser.parse_args()
    spark = session("harbourwatch-kmeans", driver_memory="2g")
    summary = learn(spark, settings().data_dir, args.date, args.include_known)
    log.info("anchorages learned", extra={"data": summary})


if __name__ == "__main__":
    main()
