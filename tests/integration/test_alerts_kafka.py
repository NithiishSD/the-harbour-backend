import threading
import time
import uuid
from functools import partial

import pytest

from common import topics
from common.kafka import JsonProducer
from dashboard.alerts import AlertFeed, add_alert, consume

pytestmark = pytest.mark.integration


def test_feed_rebuilds_from_the_alerts_topic_after_a_restart():
    alert = {"alert_id": f"it-{uuid.uuid4()}", "type": "queue_growth", "port_id": "USLAX"}
    producer = JsonProducer()
    producer.send(topics.ALERTS, "queue_growth", alert)
    producer.send(topics.ALERTS, "queue_growth", alert)  # redelivery
    assert producer.flush() == 0

    for _ in range(2):  # two "dashboard starts": each reads the topic from the beginning
        feed, stop = AlertFeed(), threading.Event()
        reader = threading.Thread(target=consume, args=(topics.ALERTS, partial(add_alert, feed), stop))
        reader.start()
        try:
            deadline = time.time() + 30
            while time.time() < deadline and alert not in feed.latest(500):
                time.sleep(0.2)
            assert [a for a in feed.latest(500) if a["alert_id"] == alert["alert_id"]] == [alert]
        finally:
            stop.set()
            reader.join(10)
