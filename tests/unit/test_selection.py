from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from common import selection
from common.config import settings
from common.geo import load_ports, load_world_ports
from common.monitor import Monitor, suggest_partners
from dashboard.alerts import AlertFeed
from dashboard.app import create_app
from tests.fakes import FakeHBase

WORLD = load_world_ports()
SINGAPORE = (1.26, 103.84)
SHANGHAI = (31.23, 121.47)
YOKOHAMA = (35.45, 139.65)


def test_the_choice_is_kept_for_its_country_only(tmp_path: Path):
    assert selection.load(tmp_path, "SG") is None
    selection.save(tmp_path, "SG", ["SGSIN"], ["CN", "JP"])
    assert selection.load(tmp_path, "SG") == {"country": "SG", "ports": ["SGSIN"], "partners": ["CN", "JP"]}
    assert selection.load(tmp_path, "NL") is None  # another country starts from its env
    (tmp_path / selection.FILE).write_text("{not json")
    assert selection.load(tmp_path, "SG") is None


def ship(src, dest, lat=20.0, lon=119.0, state="underway"):
    return {"lat": lat, "lon": lon, "destination": dest, "state": state, "src_lat": src[0], "src_lon": src[1]}


def test_voyages_count_ships_between_the_monitored_ports_and_each_country():
    sg = Monitor.build(WORLD, "SG", partners=("CN", "JP"))
    ships = [
        ship(SHANGHAI, "SGSIN"), ship(SHANGHAI, "SG SIN"), ship(YOKOHAMA, "SINGAPORE"),
        ship(SINGAPORE, "JP YOK"), ship(SINGAPORE, "SGSIN"), ship(YOKOHAMA, "USLAX"),
    ]  # fmt: skip
    assert dict(sg.voyages(ships, WORLD)) == {"CN": 2, "JP": 2}
    assert suggest_partners(WORLD, sg, ships)[:2] in (("CN", "JP"), ("JP", "CN"))  # seen traffic first


def test_suggestions_are_topped_up_from_coverage_when_few_ships_were_seen(monkeypatch):
    monkeypatch.setattr("common.monitor.coverage_cells", lambda: None)
    sg = Monitor.build(WORLD, "SG")
    picked = suggest_partners(WORLD, sg, [ship(SHANGHAI, "SGSIN")])
    assert picked[0] == "CN" and 5 <= len(picked) <= 10 and len(set(picked)) == len(picked)


@pytest.fixture
def api(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("MONITOR_COUNTRY", "US")
    monkeypatch.setenv("MONITOR_PORTS", "")
    monkeypatch.setenv("MONITOR_PARTNERS", "JP,CN")
    monkeypatch.setattr("common.monitor.coverage_cells", lambda: None)
    monkeypatch.setattr("dashboard.app.coverage_cells", lambda: None)
    settings.cache_clear()
    checks = {"kafka": lambda: "ok", "hbase": lambda: "ok", "warehouse": lambda: "ok"}
    client = TestClient(
        create_app(FakeHBase(), load_ports(), checks, AlertFeed(), world=WORLD, scoped=True)  # type: ignore[arg-type]
    )
    yield client
    settings.cache_clear()


def test_ports_and_source_countries_are_chosen_and_kept_not_the_country(api: TestClient, tmp_path: Path):
    first = api.get("/api/monitor").json()
    assert first["country"] == "US" and first["partners"] == ["CN", "JP"] and "USLAX" in first["ports"]
    done = api.put("/api/monitor/selection", json={"ports": ["USLAX"], "partners": ["KR", "TW", "JP"]})
    assert done.status_code == 200
    now = api.get("/api/monitor").json()
    assert now["ports"] == ["USLAX"] and now["partners"] == ["JP", "KR", "TW"]
    assert now["defaults"]["partners"] == ["CN", "JP"]  # the environment's, for "back to the default"
    kept = selection.load(tmp_path, "US")
    assert kept == {"country": "US", "ports": ["USLAX"], "partners": ["JP", "KR", "TW"]}
    found = api.get("/api/monitor/suggestions").json()
    assert set(found) == {"suggested", "ships"} and 5 <= len(found["suggested"]) <= 10
    assert api.get("/api/monitor", params={"country": "SG"}).status_code == 422  # not a choice


def test_a_choice_is_refused_when_it_is_not_one(api: TestClient):
    assert api.put("/api/monitor/selection", json={"ports": ["SGSIN"], "partners": []}).status_code == 422
    assert api.put("/api/monitor/selection", json={"ports": [], "partners": ["ZZ"]}).status_code == 422
    too_many = ["CN", "JP", "KR", "TW", "HK", "MY", "ID", "TH", "VN", "NL", "DE"]
    assert api.put("/api/monitor/selection", json={"ports": [], "partners": too_many}).status_code == 422
    none = api.put("/api/monitor/selection", json={"ports": [], "partners": []})
    assert none.status_code == 200 and api.get("/api/monitor").json()["partners"] == []  # none is allowed
