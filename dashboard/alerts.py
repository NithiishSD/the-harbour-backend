"""Alert feed for the dashboard: the newest alerts from Kafka, as a list and as a Server-Sent Events stream.

The consumer has no committed offsets. On every start it re-reads the `alerts` topic from the beginning
(7 days of retention), so a restarted dashboard rebuilds the same feed. Kafka is at-least-once, so alerts
are deduped by `alert_id`.
"""

import asyncio
import json
import threading
import time
from collections import deque
from collections.abc import AsyncIterator, Callable
from typing import Any

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.cimpl import OFFSET_BEGINNING

from common import topics
from common.config import settings
from common.log import get_logger

log = get_logger("alerts")

FEED_SIZE = 500  # the API caps `limit` at 500
KEEPALIVE_SECONDS = 15


class AlertFeed:
    """The last FEED_SIZE alerts in arrival order. The consumer thread writes, request handlers read."""

    def __init__(self, size: int = FEED_SIZE) -> None:
        self._items: deque[dict[str, Any]] = deque(maxlen=size)
        self._ids: set[str] = set()
        self._lock = threading.Lock()
        self.total = 0  # alerts ever accepted; lets a stream ask for "everything after N"

    def add(self, alert: dict[str, Any]) -> bool:
        """Returns False for a duplicate."""
        with self._lock:
            if alert["alert_id"] in self._ids:
                return False
            if len(self._items) == self._items.maxlen:
                self._ids.discard(self._items[0]["alert_id"])
            self._items.append(alert)
            self._ids.add(alert["alert_id"])
            self.total += 1
        return True

    def latest(self, limit: int) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._items)[::-1][:limit]

    def since(self, seen: int) -> list[dict[str, Any]]:
        """Alerts accepted after the first `seen` ones, oldest first."""
        with self._lock:
            items = list(self._items)
            return items[len(items) - max(0, min(self.total - seen, len(items))) :]


def parse_alert(raw: bytes | None) -> dict[str, Any] | None:
    try:
        alert = json.loads(raw or b"")
    except ValueError:
        return None
    if not isinstance(alert, dict) or not isinstance(alert.get("alert_id"), str) or "type" not in alert:
        return None
    return alert


def add_alert(
    feed: AlertFeed, raw: bytes | None, is_test: Callable[[dict[str, Any]], bool] | None = None
) -> bool:
    """is_test says whether an alert comes from test data; those are not shown (common.testdata)."""
    alert = parse_alert(raw)
    if alert is None:
        log.warning("skipped malformed alert")
        return False
    if is_test is not None and is_test(alert):
        return False
    return feed.add(alert)


def consume(topic: str, handle: Callable[[bytes | None], object], stop: threading.Event) -> None:
    """Read `topic` from the beginning and pass every message value to `handle` until `stop` is set."""
    consumer = Consumer(
        {
            "bootstrap.servers": settings().kafka_bootstrap,
            "group.id": f"dashboard-{topic}",
            "enable.auto.commit": False,
        }
    )
    consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in range(topics.PARTITIONS[topic])])
    try:
        while not stop.is_set():
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                log.warning("consumer error", extra={"data": {"topic": topic, "error": str(msg.error())}})
                time.sleep(2)  # topic not created yet, or broker away: librdkafka reconnects by itself
                continue
            handle(msg.value())
    finally:
        consumer.close()


async def alert_events(
    feed: AlertFeed, poll: float = 0.5, keepalive: float = KEEPALIVE_SECONDS
) -> AsyncIterator[str]:
    """SSE body: one `data:` JSON line per alert that arrives from now on, a comment line when idle."""
    seen, quiet = feed.total, 0.0
    while True:
        fresh = feed.since(seen)
        seen = feed.total
        for alert in fresh:
            yield f"data: {json.dumps(alert, separators=(',', ':'))}\n\n"
        quiet = 0.0 if fresh else quiet + poll
        if quiet >= keepalive:
            quiet = 0.0
            yield ": keepalive\n\n"
        await asyncio.sleep(poll)
