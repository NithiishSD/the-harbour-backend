"""Thin Kafka producer wrapper used by every producer process."""

import json
from typing import Any

from confluent_kafka import KafkaError, Message, Producer

from common.config import settings
from common.log import get_logger

log = get_logger("kafka")


class JsonProducer:
    def __init__(self, bootstrap: str | None = None) -> None:
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap or settings().kafka_bootstrap,
                # Idempotence + acks=all: broker retries never write a message twice.
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": 20,
                "compression.type": "lz4",
            }
        )
        self.failed = 0

    def _on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            self.failed += 1
            log.error("delivery failed", extra={"data": {"topic": msg.topic(), "error": str(err)}})

    def send(self, topic: str, key: str | None, value: dict[str, Any]) -> None:
        payload = json.dumps(value, separators=(",", ":"), default=str).encode()
        while True:
            try:
                self._producer.produce(
                    topic, key=key.encode() if key else None, value=payload, on_delivery=self._on_delivery
                )
                break
            except BufferError:
                # Local queue full: let librdkafka drain it, then retry.
                self._producer.poll(0.5)
        self._producer.poll(0)

    def flush(self, timeout: float = 30) -> int:
        """Block until queued messages are delivered; returns how many are still pending."""
        return self._producer.flush(timeout)
