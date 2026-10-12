import random

import pytest

from common import topics
from common.kafka import JsonProducer
from storage.hbase.client import HBaseClient
from storage.hbase.tables import VESSEL_STATE, VESSEL_TRACK, unrev
from streaming.job import JobConfig, start
from tests.integration.conftest import end_offsets

pytestmark = pytest.mark.integration


def run(spark, tmp_path, offsets, name):
    cfg = JobConfig(
        data_dir=tmp_path,
        checkpoint_dir=tmp_path / name,
        available_now=True,
        starting_offsets={topics.POSITIONS: offsets, topics.STATIC: offsets},
    )
    for q in start(spark, ["track", "static"], cfg):
        q.awaitTermination(240)
        assert q.exception() is None


def test_track_and_static_into_hbase_idempotently(spark, tmp_path):
    mmsi = str(random.randint(200_000_000, 790_000_000))
    t0 = 1_735_689_600_000 + random.randint(0, 10**9)
    offsets = end_offsets(topics.POSITIONS, topics.STATIC)
    producer = JsonProducer()
    producer.send(topics.POSITIONS, mmsi, {"tag": "not a position"})  # must be skipped, not crash the query
    producer.send(topics.STATIC, mmsi, {"ts": t0})
    for i in range(120):
        producer.send(
            topics.POSITIONS,
            mmsi,
            dict(
                mmsi=mmsi,
                ts=t0 + i * 10_000,
                lat=33.7 + i * 1e-4,
                lon=-118.2,
                sog=0.2,
                nav_status=1,
                source="noaa",
            ),
        )
    producer.send(topics.STATIC, mmsi, {"mmsi": mmsi, "ts": t0, "name": "OLD NAME", "ship_type": "cargo"})
    newer = dict(mmsi=mmsi, ts=t0 + 5, name="NEW NAME", ship_type="cargo", length_m=229.0)
    producer.send(topics.STATIC, mmsi, newer)
    producer.flush()

    run(spark, tmp_path, offsets, "first")
    client = HBaseClient()
    rows = client.scan_prefix(VESSEL_TRACK, mmsi, limit=1000)
    assert len(rows) == 120
    newest_key, newest = rows[0]
    assert unrev(newest_key[9:]) == t0 + 119 * 10_000  # newest first
    assert newest["pos:nav"] == "1" and "pos:cog" not in newest
    assert client.get(VESSEL_STATE, mmsi) == {
        "info:name": "NEW NAME",
        "info:type": "cargo",
        "info:length": "229.0",
    }

    # Re-processing the same messages from a fresh checkpoint must not create duplicates.
    run(spark, tmp_path, offsets, "second")
    assert len(client.scan_prefix(VESSEL_TRACK, mmsi, limit=1000)) == 120
