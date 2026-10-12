import json
import time
import uuid

import pytest
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

from common import topics
from common.config import settings
from common.kafka import JsonProducer
from tests.integration.conftest import end_offsets

pytestmark = pytest.mark.integration


def test_topics_created_with_partitions():
    topics.ensure_topics()
    assert topics.ensure_topics() == []  # second call is a no-op
    meta = AdminClient({"bootstrap.servers": settings().kafka_bootstrap}).list_topics(timeout=10)
    for name, partitions in topics.PARTITIONS.items():
        assert len(meta.topics[name].partitions) == partitions


def test_same_key_lands_in_same_partition():
    topics.ensure_topics()
    tag = uuid.uuid4().hex
    starts = json.loads(end_offsets(topics.POSITIONS))[topics.POSITIONS]
    producer = JsonProducer()
    for i in range(5):
        producer.send(topics.POSITIONS, "419000123", {"tag": tag, "i": i})
    assert producer.flush() == 0
    assert producer.failed == 0

    consumer = Consumer({"bootstrap.servers": settings().kafka_bootstrap, "group.id": f"t-{tag}"})
    consumer.assign([TopicPartition(topics.POSITIONS, int(p), off) for p, off in starts.items()])
    seen: list[tuple[int, int]] = []
    deadline = time.monotonic() + 30
    while len(seen) < 5 and time.monotonic() < deadline:
        msg = consumer.poll(0.5)
        if msg is None or msg.error():
            continue
        value = json.loads(msg.value())
        if value.get("tag") == tag:
            seen.append((msg.partition(), value["i"]))
    consumer.close()
    assert len({p for p, _ in seen}) == 1
    assert [i for _, i in seen] == [0, 1, 2, 3, 4]
