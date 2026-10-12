import random

import pytest

from common import topics
from common.kafka import JsonProducer
from storage.hbase.client import HBaseClient
from storage.hbase.tables import PORT_QUEUE, port_queue_key
from streaming.job import JobConfig, start
from streaming.sinks import iso
from tests.integration.conftest import end_offsets, read_topic_since
from tests.scenarios import HOUR, MIN, queue_growth

pytestmark = pytest.mark.integration


def test_queue_windows_and_growth_alert(spark, tmp_path):
    t0 = 1_893_456_000_000 + random.randint(0, 2_000_000) * HOUR  # window-aligned: whole hours
    first = random.randint(200_000, 789_000) * 1000
    offsets = end_offsets(topics.POSITIONS)
    alert_offsets = end_offsets(topics.ALERTS)
    producer = JsonProducer()
    for p in queue_growth(t0, first):
        producer.send(topics.POSITIONS, p["mmsi"], p)
    producer.flush()

    cfg = JobConfig(data_dir=tmp_path, available_now=True, starting_offsets={topics.POSITIONS: offsets})
    (query,) = start(spark, ["queue"], cfg)
    query.awaitTermination(300)
    assert query.exception() is None

    client = HBaseClient()
    before = client.get(PORT_QUEUE, port_queue_key("USLGB", t0 + 30 * MIN))
    after = client.get(PORT_QUEUE, port_queue_key("USLGB", t0 + 60 * MIN))
    assert before is not None and int(before["q:ships"]) == 2
    assert after is not None and int(after["q:ships"]) == 12

    alerts = [
        a
        for a in read_topic_since(spark, topics.ALERTS, alert_offsets)
        if a["type"] == "queue_growth" and a["port_id"] == "USLGB" and a["event_time"].startswith("2")
    ]
    assert iso(t0 + 60 * MIN) in {a["event_time"] for a in alerts}
    alert = next(a for a in alerts if a["event_time"] == iso(t0 + 60 * MIN))
    assert alert["details"] == {"ships": 12, "previous": 2, "growth_pct": 500.0}
