from pathlib import Path

import pytest
from confluent_kafka import Consumer, TopicPartition

from common import topics
from common.config import settings
from common.kafka import JsonProducer
from ingest.replay import noaa_records, replay

pytestmark = pytest.mark.integration
FIXTURE = str(Path(__file__).parent.parent / "fixtures" / "noaa_la_sample.csv")


def raw_offsets() -> int:
    consumer = Consumer({"bootstrap.servers": settings().kafka_bootstrap, "group.id": "offsets-probe"})
    total = 0
    for p in range(topics.PARTITIONS[topics.RAW]):
        _, high = consumer.get_watermark_offsets(TopicPartition(topics.RAW, p), timeout=10)
        total += high
    consumer.close()
    return total


def test_replay_fixture_into_kafka():
    topics.ensure_topics()
    before = raw_offsets()
    sent = replay(noaa_records(FIXTURE), JsonProducer(), speed=0, limit=2000)
    assert sent == 2000
    assert raw_offsets() - before == 2000
