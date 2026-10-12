from datetime import date
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest
from fastapi.testclient import TestClient

from common.geo import load_ports
from dashboard import app as dashboard_app
from dashboard.alerts import AlertFeed
from storage.warehouse import reports
from storage.warehouse.load import connect, load_date
from tests.fakes import FakeHBase

H = 3_600_000
JAN2 = 1_672_617_600_000  # 2023-01-02T00:00Z
DEC20 = JAN2 - 13 * 24 * H


def parquet(path: Path, select: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    duckdb.sql(f"COPY ({select}) TO '{path}' (FORMAT parquet)")  # noqa: S608 - test fixture


def call(mmsi: str, arrival: int, wait: float, kind: str = "cargo", seen: bool = True) -> str:
    return (
        f"SELECT 'USLGB' AS port_id, '{mmsi}' AS mmsi, '{kind}' AS ship_type, "
        f"{arrival}::BIGINT AS arrival_at, "
        f"{arrival}::BIGINT AS anchored_at, {arrival + int(wait * H)}::BIGINT AS berthed_at, "
        f"NULL::BIGINT AS departed_at, {wait}::DOUBLE AS wait_hours, NULL::DOUBLE AS turnaround_hours, "
        f"{str(seen).lower()} AS arrival_seen"
    )


def write_day(
    data: Path, day: str, start: int, calls: list[tuple[str, float, str, bool]], queue: int
) -> None:
    selects = " UNION ALL ".join(call(m, start + i * H, w, k, s) for i, (m, w, k, s) in enumerate(calls))
    parquet(data / "batch/port_calls" / f"date={day}" / "p.parquet", selects)
    median = sorted(w for _, w, k, s in calls if s and k in ("cargo", "tanker"))[0]
    parquet(
        data / "batch/port_daily" / f"date={day}" / "p.parquet",
        f"SELECT 'USLGB' AS port_id, {len(calls)} AS calls, {median}::DOUBLE AS median_wait_hours, "
        f"NULL::DOUBLE AS avg_turnaround_hours, {queue} AS max_hourly_queue",
    )
    parquet(
        data / "history/vessels" / f"date={day}" / "p.parquet",
        "SELECT '367000001' AS mmsi, 'SAMPLE' AS name, 'cargo' AS ship_type, 200.0 AS length_m",
    )


def weather(data: Path) -> None:
    rec = '{"port_id": "USLGB", "hour": "2023-01-02T05:00Z", "wind_speed_kn": %s, "wave_height_m": 1.5}'
    parquet(
        data / "cold/topic=weather.marine/date=2023-01-02" / "p.parquet",
        f"SELECT '{rec % 10.0}'::BLOB AS value, TIMESTAMP '2023-01-02 05:00:00' AS kafka_ts UNION ALL "
        f"SELECT '{rec % 12.0}'::BLOB, TIMESTAMP '2023-01-02 06:00:00' UNION ALL "  # newer forecast wins
        f"SELECT '{rec.replace('01-02T05', '01-03T05') % 3.0}'::BLOB, TIMESTAMP '2023-01-02 06:00:00'",
    )


@pytest.fixture
def data(tmp_path: Path) -> Path:
    write_day(tmp_path, "2022-12-20", DEC20, [("367000001", 10.0, "cargo", True)], 2)
    write_day(
        tmp_path,
        "2023-01-02",
        JAN2,
        [
            ("367000001", 6.0, "cargo", True),
            ("367000002", 8.0, "tanker", True),
            ("367000003", 1.0, "tug", True),
            ("367000004", 2.0, "cargo", False),
        ],
        4,
    )
    weather(tmp_path)
    return tmp_path


def test_load_is_idempotent_and_complete(data):
    con = connect(data / "warehouse.duckdb")
    ports = load_ports()
    load_date(con, date(2022, 12, 20), data, ports)
    counts = load_date(con, date(2023, 1, 2), data, ports)
    assert counts == {"fact_port_call": 4, "fact_port_daily": 1, "fact_weather_hourly": 1}
    first = {
        t: con.execute(f"SELECT * FROM {t} ORDER BY ALL").fetchall()
        for t in ("fact_port_call", "fact_weather_hourly")
    }
    assert load_date(con, date(2023, 1, 2), data, ports) == counts  # same date again: no duplicates
    for t, rows in first.items():
        assert con.execute(f"SELECT * FROM {t} ORDER BY ALL").fetchall() == rows
    assert con.execute("SELECT wind_speed_kn, wave_height_m FROM fact_weather_hourly").fetchone() == (
        12.0,
        1.5,
    )
    assert con.execute("SELECT count(*) FROM fact_port_call").fetchone() == (5,)
    assert con.execute("SELECT date_key, weekday FROM dim_date WHERE date_key = 20230102").fetchone() == (
        20230102,
        1,
    )
    assert con.execute("SELECT name, ship_type FROM dim_vessel").fetchone() == ("SAMPLE", "cargo")
    assert con.execute("SELECT count(*) FROM dim_port").fetchone() == (len(ports),)


def test_load_without_files_keeps_empty_tables(tmp_path):
    con = connect(tmp_path / "w.duckdb")
    assert load_date(con, date(2023, 1, 2), tmp_path, load_ports()) == {
        "fact_port_call": 0,
        "fact_port_daily": 0,
        "fact_weather_hourly": 0,
    }


def test_reports(data):
    path = data / "warehouse.duckdb"
    con = connect(path)
    for d in (date(2022, 12, 20), date(2023, 1, 2)):
        load_date(con, d, data, load_ports())
    con.close()
    assert reports.ports_daily(path, 30) == [
        {
            "port_id": "USLGB",
            "date": "2022-12-20",
            "calls": 1,
            "median_wait_hours": 10.0,
            "avg_turnaround_hours": None,
            "max_hourly_queue": 2,
        },
        {
            "port_id": "USLGB",
            "date": "2023-01-02",
            "calls": 4,
            "median_wait_hours": 6.0,
            "avg_turnaround_hours": None,
            "max_hourly_queue": 4,
        },
    ]
    assert [r["date"] for r in reports.ports_daily(path, 5)] == [
        "2023-01-02"
    ]  # counted back from the newest day
    # January: cargo/tanker with an observed arrival waited 6 and 8 h (median 7); December 10 h.
    assert reports.compare(path) == [
        {"port_id": "USLGB", "this_month_median": 7.0, "last_month_median": 10.0, "change_pct": -30.0}
    ]


def test_reports_are_held_to_the_countrys_ports_and_days(data):
    path = data / "warehouse.duckdb"
    con = connect(path)
    for d in (date(2022, 12, 20), date(2023, 1, 2)):
        load_date(con, d, data, load_ports())
    con.close()
    assert reports.ports_daily(path, 30, ports=["SGSIN"]) == []  # Long Beach is all it holds
    assert reports.compare(path, ports=["SGSIN"]) == []
    assert [r["date"] for r in reports.ports_daily(path, 30, ports=["USLGB"], dates=["2023-01-02"])] == [
        "2023-01-02"
    ]  # the December day was received for another country
    assert reports.ports_daily(path, 30, ports=["USLGB"], dates=[]) == []
    only = reports.compare(path, ports=["USLGB"], dates=["2023-01-02"])
    assert only[0]["this_month_median"] == 7.0 and only[0]["last_month_median"] is None


@pytest.fixture
def client(data, monkeypatch):
    path = data / "warehouse.duckdb"
    seen = SimpleNamespace(warehouse_path=path, monitor_max_partners=10)
    monkeypatch.setattr(dashboard_app, "settings", lambda: seen)
    return TestClient(dashboard_app.create_app(FakeHBase(), load_ports(), {}, AlertFeed())), path, data


def test_report_endpoints(client):
    api, path, data = client
    assert api.get("/api/reports/ports").json() == []  # nightly job has not run: empty, not an error
    assert api.get("/api/reports/compare").json() == []
    con = connect(path)
    load_date(con, date(2023, 1, 2), data, load_ports())
    con.close()
    rows = api.get("/api/reports/ports", params={"days": 7}).json()
    assert rows[0]["port_id"] == "USLGB" and rows[0]["median_wait_hours"] == 6.0
    assert api.get("/api/reports/compare").json()[0]["this_month_median"] == 7.0


@pytest.mark.parametrize("days", ["0", "366", "x", "1; DROP TABLE dim_port"])
def test_report_days_validated(client, days):
    r = client[0].get("/api/reports/ports", params={"days": days})
    assert r.status_code == 422 and r.json()["error"]["code"] == "INVALID_PARAM"


def test_corrupt_warehouse_is_503(client):
    api, path, _ = client
    path.write_bytes(b"not a duckdb file" * 100)
    r = api.get("/api/reports/ports")
    assert r.status_code == 503 and r.json()["error"]["code"] == "WAREHOUSE_UNAVAILABLE"


def test_the_dashboard_reports_only_for_its_country(data, monkeypatch):
    from common import scopes
    from common.config import settings
    from common.geo import load_world_ports

    con = connect(data / "warehouse.duckdb")
    load_date(con, date(2023, 1, 2), data, load_ports())
    con.close()
    monkeypatch.setenv("DATA_DIR", str(data))
    monkeypatch.setenv("MONITOR_PORTS", "")
    checks = {"kafka": lambda: "ok", "hbase": lambda: "ok", "warehouse": lambda: "ok"}

    def reports_for(country: str) -> list:
        monkeypatch.setenv("MONITOR_COUNTRY", country)
        settings.cache_clear()
        app = dashboard_app.create_app(
            FakeHBase(), load_ports(), checks, AlertFeed(), world=load_world_ports(), scoped=True
        )  # type: ignore[arg-type]
        return TestClient(app).get("/api/reports/ports").json()

    try:
        assert [r["port_id"] for r in reports_for("US")] == ["USLGB"]  # no ledger: nothing is held back
        assert reports_for("SG") == []  # deployed for Singapore: no port of it is in the warehouse
        # a NOAA history file for that day: it is the US's, whichever country the feed was set to since
        day = data / "history" / "positions" / "date=2023-01-02"
        day.mkdir(parents=True)
        duckdb.connect().execute(f"COPY (SELECT 'x' AS mmsi) TO '{day / 'part-0.parquet'}' (FORMAT parquet)")
        scopes.record(data, "SG", ("SGSIN",), ((1.0, 103.0, 2.0, 105.0),), first_data_ms=0, now_ms=0)
        assert [r["port_id"] for r in reports_for("US")] == ["USLGB"]  # NOAA days stay the US's
    finally:
        settings.cache_clear()
