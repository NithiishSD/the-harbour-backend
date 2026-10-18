"""Kafka topic definitions and idempotent creation."""

from confluent_kafka.admin import AdminClient, NewTopic

from common.config import settings
from common.log import get_logger

RAW = "ais.raw"
POSITIONS = "ais.positions"
STATIC = "ais.static"
WEATHER = "weather.marine"
WEATHER_GRID = "weather.grid"
PORT_EVENTS = "port.events"
ALERTS = "alerts"
DLQ = "ais.dlq"

# Ship topics are keyed by MMSI, so one ship's messages stay ordered inside one partition.
PARTITIONS = {
    RAW: 6,
    POSITIONS: 6,
    STATIC: 6,
    PORT_EVENTS: 6,
    DLQ: 1,
    WEATHER: 1,
    WEATHER_GRID: 1,
    ALERTS: 1,
}
RETENTION_MS = 7 * 24 * 3600 * 1000

log = get_logger("topics")


def ensure_topics(bootstrap: str | None = None) -> list[str]:
    """Create any missing topics. Returns the names that were created."""
    admin = AdminClient({"bootstrap.servers": bootstrap or settings().kafka_bootstrap})
    existing = set(admin.list_topics(timeout=10).topics)
    missing = [
        NewTopic(name, num_partitions=n, replication_factor=1, config={"retention.ms": str(RETENTION_MS)})
        for name, n in PARTITIONS.items()
        if name not in existing
    ]
    if not missing:
        return []
    for name, future in admin.create_topics(missing).items():
        future.result()
        log.info("created topic", extra={"data": {"topic": name}})
    return [t.topic for t in missing]


if __name__ == "__main__":
    ensure_topics()
