from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from common import topics
from common.schema import normalize
from ingest.replay import Pacer, cold_records, noaa_records, replay

FIXTURE = str(Path(__file__).parent.parent / "fixtures" / "noaa_la_sample.csv")


class FakeSender:
    def __init__(self):
        self.sent = []

    def send(self, topic, key, value):
        self.sent.append((topic, key, value))

    def flush(self, timeout=30):
        return 0


def test_noaa_records_sorted_and_valid():
    records = list(noaa_records(FIXTURE))
    assert len(records) == len(Path(FIXTURE).read_text().splitlines()) - 1
    times = [ts for ts, _ in records]
    assert times == sorted(times)
    ts, env = records[0]
    assert env["source"] == "noaa"
    n = normalize(env)
    assert n.reject is None and n.position is not None and n.position["ts"] == ts


def test_replay_sends_keyed_raw_messages_and_respects_limit():
    sender = FakeSender()
    sent = replay(noaa_records(FIXTURE), sender, speed=0, limit=50)
    assert sent == 50 == len(sender.sent)
    topic, key, value = sender.sent[0]
    assert topic == topics.RAW
    assert key == value["payload"]["MMSI"]


class FakeClock:
    def __init__(self):
        self.now = 100.0
        self.slept = []

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.now += seconds


def test_pacer_compresses_event_gaps_by_speed():
    clock = FakeClock()
    pacer = Pacer(60, clock=clock, sleep=clock.sleep)
    pacer.wait(0)
    pacer.wait(60_000)  # one event-minute later -> one wall second at 60x
    pacer.wait(120_000)
    assert clock.slept == [1.0, 1.0]


def test_pacer_does_not_sleep_when_behind_or_at_max_speed():
    clock = FakeClock()
    pacer = Pacer(1, clock=clock, sleep=clock.sleep)
    pacer.wait(0)
    clock.now += 5  # producer fell behind
    pacer.wait(1_000)
    assert clock.slept == []
    fast = Pacer(0, clock=clock, sleep=clock.sleep)
    fast.wait(0)
    fast.wait(10**9)
    assert clock.slept == []


def test_cold_records_filters_dates_and_orders(tmp_path):
    root = tmp_path / "ais.raw"
    for day, offset in (("2026-10-01", 2), ("2026-10-02", 1), ("2026-10-03", 0)):
        part = root / f"date={day}"
        part.mkdir(parents=True)
        ts = datetime(2026, 10, int(day[-2:]), 12, offset, tzinfo=UTC)
        value = f'{{"source": "aisstream", "payload": {{"d": "{day}"}}}}'.encode()
        table = pa.table({"value": [value, b"\xff garbage"], "kafka_ts": [ts, ts]})
        pq.write_table(table, part / "p.parquet")
    got = list(cold_records(root, date(2026, 10, 1), date(2026, 10, 2)))
    assert [env["payload"]["d"] for _, env in got] == ["2026-10-01", "2026-10-02"]
