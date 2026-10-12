import json
import os
import random
import signal
import subprocess
import sys
import time

import pytest

from common import topics
from common.config import ROOT
from common.kafka import JsonProducer
from storage.hbase.client import HBaseClient
from storage.hbase.tables import VESSEL_TRACK, track_key
from tests.integration.conftest import end_offsets, read_topic_since
from tests.samples import aisstream_position, envelope

pytestmark = pytest.mark.integration

SHIPS = 3
PER_WAVE = 200  # positions per ship per wave

# The job runs in its own process so it can be killed like a real crash (SIGKILL, no clean shutdown).
JOB = """
import json, sys
from pathlib import Path
from common.spark import session
from streaming.job import JobConfig, start

cfg = JobConfig(data_dir=Path(sys.argv[1]), starting_offsets=json.loads(sys.argv[2]),
                available_now=sys.argv[3] == "once", trigger_seconds=2)
spark = session("recovery-test", master="local[2]", kafka=True)
queries = start(spark, ["normalize", "archive", "track"], cfg)
for q in queries:
    q.awaitTermination()
sys.exit(1 if any(q.exception() for q in queries) else 0)
"""


def job(data_dir, offsets, mode):
    return subprocess.Popen(  # noqa: S603 - fixed script, test arguments
        [sys.executable, "-c", JOB, str(data_dir), json.dumps(offsets), mode],
        cwd=ROOT,
        start_new_session=True,  # own process group: the kill takes the Spark JVM down too
    )


def run_once(data_dir, offsets):
    assert job(data_dir, offsets, "once").wait(300) == 0


def send_wave(mmsis, t0, wave):
    producer = JsonProducer()
    for mmsi in mmsis:
        for i in range(wave * PER_WAVE, (wave + 1) * PER_WAVE):
            producer.send(
                topics.RAW, str(mmsi), envelope(aisstream_position(mmsi=mmsi, ts_ms=t0 + i * 10_000))
            )
    producer.flush()


def track_rows(client, mmsis):
    return sum(len(client.scan_prefix(VESSEL_TRACK, str(m), limit=10_000)) for m in mmsis)


def wait_for(check, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if check():
            return True
        time.sleep(0.2)
    return False


def test_crash_recovery_and_idempotent_replay(spark, tmp_path):
    base = random.randint(200_000_000, 790_000_000)
    mmsis = [base + i for i in range(SHIPS)]
    t0 = 1_735_689_600_000 + random.randint(0, 10**9)
    positions_offsets = end_offsets(topics.POSITIONS)
    offsets = {
        topics.RAW: end_offsets(topics.RAW),
        f"{topics.RAW},{topics.WEATHER}": end_offsets(topics.RAW, topics.WEATHER),
        topics.POSITIONS: positions_offsets,
    }
    client = HBaseClient()
    wave_size = SHIPS * PER_WAVE

    send_wave(mmsis, t0, 0)
    running = job(tmp_path, offsets, "forever")
    try:
        assert wait_for(lambda: track_rows(client, mmsis) == wave_size, 240), "first wave never landed"
        send_wave(mmsis, t0, 1)
        # Kill while the second wave is being written (or after 20 s if it is still waiting for a trigger).
        wait_for(lambda: track_rows(client, mmsis) > wave_size, 20)
    finally:
        os.killpg(running.pid, signal.SIGKILL)
        running.wait()
    assert running.returncode == -signal.SIGKILL

    # Restart from the same checkpoints: everything lands exactly once.
    run_once(tmp_path, offsets)
    assert track_rows(client, mmsis) == 2 * wave_size
    keys = [str(m) for m in mmsis]
    cold = (
        spark.read.parquet(str(tmp_path / "cold")).filter("topic = 'ais.raw'").filter(f"key IN {tuple(keys)}")
    )
    assert cold.count() == 2 * wave_size  # the Parquet sink commits each batch once
    # The Kafka sink is at-least-once: a batch cut short is written again, so ais.positions may hold
    # repeats, but never misses one. HBase absorbs repeats because the row key is MMSI + event time.
    mine = [
        p for p in read_topic_since(spark, topics.POSITIONS, positions_offsets) if int(p["mmsi"]) in mmsis
    ]
    assert len({(p["mmsi"], p["ts"]) for p in mine}) == 2 * wave_size

    # Replaying the first wave changes nothing in HBase: same keys, same cell timestamps, one version.
    send_wave(mmsis, t0, 0)
    run_once(tmp_path, offsets)
    assert track_rows(client, mmsis) == 2 * wave_size
    versions = client.get_versions(VESSEL_TRACK, track_key(str(base), t0), "pos:lat", 10)
    assert [ts for ts, _ in versions] == [t0]
