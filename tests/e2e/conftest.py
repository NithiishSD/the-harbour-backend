"""Real dashboard process, real Kafka and HBase, a seeded warehouse and headless Chromium.

Needs `docker compose -f infra/docker-compose.yml up -d kafka hbase` and `playwright install chromium`.
The page loads Leaflet from unpkg, so the machine needs internet; map tiles are blocked on purpose.
"""

import os
import random
import socket
import subprocess
import sys
import time
import urllib.request
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import pytest
from playwright.sync_api import Browser, BrowserContext, Page, sync_playwright

from common import topics
from storage.hbase.client import HBaseClient
from storage.hbase.tables import (
    PORT_QUEUE,
    VESSEL_STATE,
    VESSEL_TRACK,
    create_tables,
    port_queue_key,
    track_key,
)
from storage.warehouse.load import connect, date_key
from tests.scenarios import LB_ANCHORAGE, LB_BERTH

ROOT = Path(__file__).resolve().parents[2]
CELL = 1_000_000_000_000  # cell timestamps stay in the past so a delete can remove them
# Now, to the minute: newer than replayed days in the dev stack, so the map rules keep the seeded ships,
# and not in the future, which the dashboard ignores as a broken clock.
T0 = int(time.time()) // 60 * 60_000


@dataclass
class Seed:
    anchored: str
    berthed: str
    anchored_name: str
    berthed_name: str
    queue: int
    queue_at: int
    history_name: str = "E2E HISTORY"  # a ship in the offline history of 2023-01-01
    history_mmsi: str = "367999001"


@dataclass
class Dashboard:
    url: str
    seed: Seed


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _after_newest_window(client: HBaseClient) -> int:
    """One hour after the newest Long Beach window (keys sort newest first), whatever other runs left."""
    newest = client.scan_prefix(PORT_QUEUE, "USLGB", 1)
    return (int(newest[0][1]["q:window_end"]) if newest else T0) + 3_600_000


def _seed_hbase(client: HBaseClient) -> Seed:
    create_tables()
    tag = f"{random.randint(1000, 9999)}"
    seed = Seed(
        anchored=str(random.randint(200_000_000, 399_999_999)),
        berthed=str(random.randint(400_000_000, 790_000_000)),
        anchored_name=f"E2E ANCHOR {tag}",
        berthed_name=f"E2E BERTH {tag}",
        queue=7,
        queue_at=_after_newest_window(client),
    )
    for mmsi, name, state, spot, since in [
        (seed.anchored, seed.anchored_name, "anchored", LB_ANCHORAGE, T0),
        (seed.berthed, seed.berthed_name, "berthed", LB_BERTH, T0 + 3_600_000),
    ]:
        client.put(
            VESSEL_STATE,
            mmsi,
            {
                "info:name": name,
                "info:type": "cargo",
                "cur:state": state,
                "cur:port": "USLGB",
                "cur:since": str(since),
                "last:ts": str(since + 60_000),
                "last:first": str(since),  # newest arrival: it outlasts the replay ships under the map cap
                "last:lat": str(spot[0]),
                "last:lon": str(spot[1]),
                "last:sog": "0.0",
                "last:area": "USLGB",
                "last:state": state,
            },
            CELL + 1,
        )
        # An earlier state (a second version of the cur: cells) and three reports for the track.
        client.put(
            VESSEL_STATE,
            mmsi,
            {"cur:state": "underway", "cur:port": "", "cur:since": str(since - 7_200_000)},
            CELL,
        )
        for i in range(3):
            cells = {"pos:lat": str(spot[0] + i * 0.001), "pos:lon": str(spot[1]), "pos:sog": "0.0"}
            client.put(VESSEL_TRACK, track_key(mmsi, since + i * 60_000), cells, CELL)
    client.put(
        PORT_QUEUE,
        port_queue_key("USLGB", seed.queue_at),
        {
            "q:window_start": str(seed.queue_at - 1_800_000),
            "q:window_end": str(seed.queue_at),
            "q:ships": str(seed.queue),
        },
        # Wall-clock cell time: this row key repeats between runs, and the last run's delete marker (stamped
        # with the server clock) would hide cells written with the old CELL time.
        int(time.time() * 1000),
    )
    return seed


def _seed_warehouse(path: Path) -> None:
    con = connect(path)
    for d, queue in [(date(2022, 12, 20), 2), (date(2023, 1, 1), 4), (date(2023, 1, 2), 9)]:
        key = date_key(d)
        con.execute("INSERT INTO dim_date VALUES (?, ?, ?, ?, ?, ?)", [key, d, d.year, d.month, d.day, 1])
        con.execute("INSERT INTO fact_port_daily VALUES ('USLGB', ?, 3, 6.5, 12.0, ?)", [key, queue])
    for wait, berthed in [
        (10.0, "2022-12-20 06:00:00"),
        (5.0, "2023-01-01 06:00:00"),
        (7.0, "2023-01-02 06:00:00"),
    ]:
        con.execute(
            "INSERT INTO fact_port_call "
            "VALUES ('USLGB', ?, 20230101, ?, NULL, ?, NULL, ?, NULL, 'cargo', true)",
            [f"{wait:.0f}00", berthed, berthed, wait],
        )
    con.close()


HISTORY_DAY = "2023-01-01"
HISTORY_T0 = 1_672_531_200_000  # 2023-01-01T00:00Z


def _seed_history(data: Path) -> None:
    """Offline history of one day: a ship sails in, waits at anchor and berths; another stays at a berth."""
    sea, near = (33.30, -118.30), (33.60, -118.20)

    def row(mmsi: str, hours: float, spot: tuple[float, float], sog: float, nav: int | None) -> dict:
        return {
            "mmsi": mmsi, "ts": HISTORY_T0 + int(hours * 3_600_000), "lat": spot[0], "lon": spot[1],
            "sog": sog, "cog": 90.0, "heading": 90, "nav_status": nav, "source": "noaa",
        }  # fmt: skip

    ship, quay = "367999001", "367999002"
    rows = [row(ship, 6, sea, 12, None), row(ship, 7, near, 10, None)]
    rows += [row(ship, h, LB_ANCHORAGE, 0.1, 1) for h in (8, 9, 10)]
    rows += [row(ship, 10.5, LB_BERTH, 0.0, 5), row(ship, 11, LB_BERTH, 0.0, 5)]
    rows += [row(quay, h, LB_BERTH, 0.0, 5) for h in range(0, 13)]
    vessels = [
        {"mmsi": ship, "ts": HISTORY_T0, "name": "E2E HISTORY", "ship_type": "cargo", "length_m": 190.0,
         "draught_m": None, "imo": None, "callsign": None},
        {"mmsi": quay, "ts": HISTORY_T0, "name": "E2E QUAYSIDE", "ship_type": "tug", "length_m": 30.0,
         "draught_m": None, "imo": None, "callsign": None},
    ]  # fmt: skip
    con = duckdb.connect()
    for name, data_rows in (("positions", rows), ("vessels", vessels)):
        folder = data / "history" / name / f"date={HISTORY_DAY}"
        folder.mkdir(parents=True)
        con.register("rows", pd.DataFrame(data_rows))
        con.execute(f"COPY (SELECT * FROM rows) TO '{folder / 'part-0.parquet'}' (FORMAT parquet)")
        con.unregister("rows")
    con.close()


@pytest.fixture(scope="session")
def dashboard(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Dashboard]:
    topics.ensure_topics()
    client = HBaseClient()
    seed = _seed_hbase(client)
    data = tmp_path_factory.mktemp("data")
    _seed_warehouse(data / "warehouse.duckdb")
    _seed_history(data)
    port = _free_port()
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "dashboard.app:app", "--host", "127.0.0.1", "--port", str(port)],
        cwd=ROOT,
        env={
            **os.environ,
            "DATA_DIR": str(data),
            "SHOW_TEST_DATA": "1",
            "MONITOR_COUNTRY": "US",
            "MONITOR_PORTS": "USLGB,USLAX",
        },  # fmt: skip
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    url = f"http://127.0.0.1:{port}"
    try:
        for _ in range(100):
            try:
                urllib.request.urlopen(f"{url}/api/zones", timeout=2).close()
                break
            except OSError:
                time.sleep(0.3)
        else:
            raise RuntimeError("dashboard did not start")
        yield Dashboard(url, seed)
    finally:
        proc.terminate()
        proc.wait(10)
        for mmsi in (seed.anchored, seed.berthed):
            client.delete_row(VESSEL_STATE, mmsi)
            for i in range(3):
                client.delete_row(VESSEL_TRACK, track_key(mmsi, T0 + i * 60_000))
                client.delete_row(VESSEL_TRACK, track_key(mmsi, T0 + 3_600_000 + i * 60_000))
        client.delete_row(PORT_QUEUE, port_queue_key("USLGB", seed.queue_at))


@pytest.fixture(scope="session")
def browser() -> Iterator[Browser]:
    with sync_playwright() as p:
        b = p.chromium.launch()
        yield b
        b.close()


@pytest.fixture
def context(browser: Browser) -> Iterator[BrowserContext]:
    ctx = browser.new_context(viewport={"width": 1280, "height": 900})
    ctx.route("**/*openstreetmap.org/**", lambda route: route.abort())  # no load on the tile servers
    ctx.route("**/*arcgisonline.com/**", lambda route: route.abort())  # nor on the satellite tiles
    yield ctx
    ctx.close()


@pytest.fixture
def page(context: BrowserContext, dashboard: Dashboard) -> Iterator[Page]:
    p = context.new_page()
    errors: list[str] = []
    p.on("pageerror", lambda e: errors.append(str(e)))
    p.on(
        "console",
        lambda m: (
            errors.append(m.text) if m.type == "error" and "Failed to load resource" not in m.text else None
        ),
    )
    p.goto(dashboard.url)
    yield p
    assert errors == [], "the page logged errors"
