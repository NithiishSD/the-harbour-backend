import pytest

from common import topics
from common.kafka import JsonProducer
from storage.hbase.client import HBaseClient
from storage.hbase.tables import PORT_CALLS, VESSEL_STATE
from streaming.job import JobConfig, start
from tests.integration.conftest import end_offsets, read_topic_since
from tests.scenarios import HOUR, Scenario

pytestmark = pytest.mark.integration


def test_state_machine_end_to_end(spark, tmp_path):
    sc = Scenario()
    offsets = end_offsets(topics.POSITIONS)
    out_offsets = end_offsets(topics.PORT_EVENTS, topics.ALERTS)
    producer = JsonProducer()
    for p in sc.positions():
        producer.send(topics.POSITIONS, p["mmsi"], p)
    producer.flush()

    cfg = JobConfig(data_dir=tmp_path, available_now=True, starting_offsets={topics.POSITIONS: offsets})
    (query,) = start(spark, ["state"], cfg)
    query.awaitTermination(300)
    assert query.exception() is None

    alerts = [a for a in read_topic_since(spark, topics.ALERTS, out_offsets) if a.get("mmsi") == sc.dark]
    assert len(alerts) == 1
    assert alerts[0]["type"] == "dark_ship" and alerts[0]["port_id"] == "USLGB"
    assert alerts[0]["details"]["active_ships"] >= 3

    events = read_topic_since(spark, topics.PORT_EVENTS, out_offsets)
    mine = {(e["mmsi"], e["type"]) for e in events}
    assert (sc.gap, "coverage_gap") in mine  # nobody else reported near Los Angeles
    assert not [a for a in read_topic_since(spark, topics.ALERTS, out_offsets) if a.get("mmsi") == sc.gap]
    assert {(sc.call, "anchored"), (sc.call, "berthed")} <= mine
    assert (sc.call, "coverage_gap") not in mine  # still reporting: never a candidate

    client = HBaseClient()
    history = client.get_versions(VESSEL_STATE, sc.call, "cur:state", 10)
    assert [state for _, state in history] == ["berthed", "anchored"]
    calls = client.scan_prefix(PORT_CALLS, "USLGB", limit=100_000)
    call = next(row for key, row in calls if key.endswith(sc.call))
    assert float(call["call:wait_hours"]) == 2.0
    assert int(call["call:anchored_at"]) == sc.t0
    last = client.get(VESSEL_STATE, sc.call)
    assert last is not None and int(last["last:ts"]) == sc.t0 + 9 * HOUR - 3 * 60_000
