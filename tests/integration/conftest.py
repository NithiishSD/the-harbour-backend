import json

import pytest
from confluent_kafka import Consumer, TopicPartition

from common import topics
from common.config import settings
from storage.hbase.tables import create_tables


@pytest.fixture(scope="session", autouse=True)
def infrastructure():
    topics.ensure_topics()
    create_tables()


def end_offsets(*names: str) -> str:
    """Current end of each topic as a Spark startingOffsets JSON map (start a query 'from now')."""
    consumer = Consumer({"bootstrap.servers": settings().kafka_bootstrap, "group.id": "offsets-probe"})
    offsets: dict[str, dict[str, int]] = {}
    for name in names:
        offsets[name] = {}
        for p in range(topics.PARTITIONS[name]):
            _, high = consumer.get_watermark_offsets(TopicPartition(name, p), timeout=10)
            offsets[name][str(p)] = high
    consumer.close()
    return json.dumps(offsets)


def read_topic_since(spark, name: str, offsets_json: str) -> list[dict]:
    """Every message on `name` after the given offsets, as parsed JSON values."""
    df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", settings().kafka_bootstrap)
        .option("subscribe", name)
        .option("startingOffsets", json.dumps({name: json.loads(offsets_json)[name]}))
        .option("endingOffsets", "latest")
        .load()
    )
    return [json.loads(r.value) for r in df.selectExpr("CAST(value AS STRING) AS value").collect()]
