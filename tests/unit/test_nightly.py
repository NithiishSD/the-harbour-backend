from datetime import date
from pathlib import Path

from batch import nightly as n


def test_steps_run_in_order_and_an_existing_csv_is_not_downloaded(tmp_path: Path, monkeypatch):
    csv = tmp_path / "noaa" / "csv" / "AIS_2023_01_01_bbox.csv"
    csv.parent.mkdir(parents=True)
    csv.write_text("x")
    calls = []
    monkeypatch.setattr(n.noaa, "download", lambda *a, **k: calls.append("download"))
    monkeypatch.setattr(n, "session", lambda *a, **k: "spark")
    monkeypatch.setattr(n, "backfill", lambda s, p, d: calls.append(("backfill", s, p)) or {"rows": 1})
    monkeypatch.setattr(n, "port_calls", lambda s, day, d: calls.append("port_calls") or {"calls": 2})
    monkeypatch.setattr(n, "learn", lambda s, d, until: calls.append(("learn", until)) or {})
    monkeypatch.setattr(
        n, "load_date", lambda con, day, d, ports: calls.append("load") or {"fact_port_call": 2}
    )

    out = n.nightly(date(2023, 1, 1), tmp_path, tmp_path / "w.duckdb")

    assert calls == [("backfill", "spark", str(csv)), "port_calls", ("learn", date(2023, 1, 1)), "load"]
    assert out["warehouse"] == {"fact_port_call": 2}
    assert (tmp_path / "w.duckdb").is_file()


def test_available_and_loaded_dates(tmp_path: Path):
    noaa_dir = tmp_path / "noaa" / "csv"
    noaa_dir.mkdir(parents=True)
    (noaa_dir / "AIS_2023_01_01_bbox.csv").write_text("x")
    hist_dir = tmp_path / "history" / "positions" / "date=2026-10-10"
    hist_dir.mkdir(parents=True)

    avail = n.available_dates(tmp_path)
    assert avail == [(date(2023, 1, 1), False), (date(2026, 10, 10), True)]

    # Initially empty warehouse
    w_path = tmp_path / "test_w.duckdb"
    assert n.loaded_dates(w_path) == set()
