import random

import pytest
from confluent_kafka import Producer

from common import topics
from common.config import settings
from common.kafka import JsonProducer
from streaming.job import JobConfig, start
from tests.integration.conftest import end_offsets, read_topic_since
from tests.samples import aisstream_position, aisstream_static, envelope

pytestmark = pytest.mark.integration


def test_normalize_rejects_and_archive(spark, tmp_path):
    base_mmsi = random.randint(200_000_000, 790_000_000)
    start_ms = 1_735_689_600_000 + random.randint(0, 10**9)
    raw_offsets = end_offsets(topics.RAW)
    out_offsets = end_offsets(topics.POSITIONS, topics.STATIC)
    archive_offsets = end_offsets(topics.RAW, topics.WEATHER)

    producer = JsonProducer()
    sent = 0
    for i in range(20):
        msg = envelope(aisstream_position(mmsi=base_mmsi + i, ts_ms=start_ms + i * 1000))
        producer.send(topics.RAW, str(base_mmsi + i), msg)
        sent += 1
    for i in range(5):  # exact duplicates of the first five positions
        dup = envelope(aisstream_position(mmsi=base_mmsi + i, ts_ms=start_ms + i * 1000))
        producer.send(topics.RAW, str(base_mmsi + i), dup)
        sent += 1
    producer.send(topics.RAW, str(base_mmsi), envelope(aisstream_static(mmsi=base_mmsi, ts_ms=start_ms)))
    producer.send(topics.RAW, str(base_mmsi), envelope(aisstream_position(mmsi=base_mmsi, lat=91.0)))
    producer.send(topics.RAW, "x", envelope(aisstream_position(mmsi=12345, ts_ms=start_ms)))
    sent += 3
    producer.flush()
    garbage = Producer({"bootstrap.servers": settings().kafka_bootstrap})
    garbage.produce(topics.RAW, value=b"\xff not json")
    garbage.flush()
    sent += 1

    cfg = JobConfig(
        data_dir=tmp_path,
        available_now=True,
        starting_offsets={topics.RAW: raw_offsets, f"{topics.RAW},{topics.WEATHER}": archive_offsets},
    )
    for q in start(spark, ["normalize", "rejects", "archive"], cfg):
        q.awaitTermination(240)
        assert q.exception() is None

    positions = read_topic_since(spark, topics.POSITIONS, out_offsets)
    mine = [p for p in positions if base_mmsi <= int(p["mmsi"]) < base_mmsi + 20]
    assert len(mine) == 20  # 25 sent, 5 duplicates dropped
    assert {p["source"] for p in mine} == {"aisstream"}
    statics = [s for s in read_topic_since(spark, topics.STATIC, out_offsets) if s["mmsi"] == str(base_mmsi)]
    assert len(statics) == 1 and statics[0]["ship_type"] == "tanker"

    rejects = spark.read.parquet(str(tmp_path / "rejects")).collect()
    assert sorted(r.reason for r in rejects) == ["latitude out of range", "mmsi must be 9 digits", "not json"]

    cold = spark.read.parquet(str(tmp_path / "cold"))
    assert cold.filter("topic = 'ais.raw'").count() == sent
