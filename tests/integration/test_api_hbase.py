import random
import time

import pytest
from fastapi.testclient import TestClient

from dashboard.app import create_app
from dashboard.live import iso
from storage.hbase.client import HBaseClient
from storage.hbase.tables import (
    PORT_CALLS,
    PORT_QUEUE,
    VESSEL_STATE,
    VESSEL_TRACK,
    port_call_key,
    port_queue_key,
    track_key,
)

pytestmark = pytest.mark.integration

HOUR = 3_600_000
# Event times in year 2500+ are newer than the rows other tests leave behind (2030-2260), so newest-first
# reads see ours. Cell timestamps stay in the past: a delete cannot hide cells stamped in the future.
CELL = 1_000_000_000_000
T0 = 16_725_225_600_000 + random.randint(0, 10**6) * HOUR
# The ship itself reports in the present: the dashboard ignores ships stamped in the future (a broken clock).
NOW = int(time.time()) // 60 * 60_000 - HOUR


def test_api_reads_what_the_streaming_job_wrote():
    mmsi = str(random.randint(200_000_000, 790_000_000))
    client = HBaseClient()
    keys = {
        VESSEL_STATE: [mmsi],
        VESSEL_TRACK: [track_key(mmsi, T0 + i * 1000) for i in range(3)],
        PORT_CALLS: [port_call_key("USLAX", T0, mmsi)],
        PORT_QUEUE: [port_queue_key("USLAX", T0 + HOUR)],
    }
    try:
        anchored = {"cur:state": "anchored", "cur:port": "USLAX", "cur:since": str(NOW)}
        berthed = {"cur:state": "berthed", "cur:port": "USLAX", "cur:since": str(NOW + HOUR)}
        last = {"last:ts": str(NOW + HOUR), "last:lat": "33.75", "last:lon": "-118.26", "last:area": "USLAX"}
        client.put(VESSEL_STATE, mmsi, anchored, CELL)
        client.put(VESSEL_STATE, mmsi, berthed, CELL + 1)  # a second version of the cur: cells
        client.put(
            VESSEL_STATE, mmsi, {**last, "last:state": "berthed", "info:name": "INTEGRATION"}, CELL + 1
        )
        for i, key in enumerate(keys[VESSEL_TRACK]):
            client.put(
                VESSEL_TRACK, key, {"pos:lat": f"33.7{i}", "pos:lon": "-118.2", "pos:sog": "0.0"}, CELL
            )
        call = {"call:anchored_at": str(T0), "call:berthed_at": str(T0 + HOUR), "call:wait_hours": "1.0"}
        client.put(PORT_CALLS, keys[PORT_CALLS][0], call, CELL)
        window = {"q:window_start": str(T0 + HOUR // 2), "q:window_end": str(T0 + HOUR), "q:ships": "7"}
        client.put(PORT_QUEUE, keys[PORT_QUEUE][0], window, CELL)

        api = TestClient(create_app(client))
        health = api.get("/health")
        assert health.status_code == 200 and health.json()["status"] == "ok"
        assert health.json()["checks"]["kafka"] == "ok" and health.json()["checks"]["hbase"] == "ok"

        ship = next(s for s in api.get("/api/ships?port_id=USLAX&state=berthed").json() if s["mmsi"] == mmsi)
        assert ship["name"] == "INTEGRATION" and ship["lat"] == 33.75 and ship["since"] == iso(NOW + HOUR)
        assert [t["lat"] for t in api.get(f"/api/ships/{mmsi}/track").json()] == [
            33.72,
            33.71,
            33.70,
        ]  # newest first
        assert [s["state"] for s in api.get(f"/api/ships/{mmsi}/states").json()] == ["berthed", "anchored"]
        assert api.get("/api/ports/USLAX/calls?limit=1").json()[0]["mmsi"] == mmsi
        assert api.get("/api/ports/USLAX/queue?limit=1").json()[0]["ships"] == 7
        port = next(p for p in api.get("/api/ports").json() if p["port_id"] == "USLAX")
        assert port["window_count"] == 7 and port["window_end"] == iso(T0 + HOUR)
    finally:
        for table, rows in keys.items():
            for key in rows:
                client.delete_row(table, key)


def test_health_degraded_when_hbase_is_unreachable():
    api = TestClient(create_app(HBaseClient("http://127.0.0.1:9", timeout=1)))
    r = api.get("/health")
    assert r.status_code == 503 and r.json()["checks"]["hbase"] == "unavailable"
    assert api.get("/api/ports").json() == {
        "error": {"code": "HBASE_UNAVAILABLE", "message": "live data store is unavailable"}
    }
