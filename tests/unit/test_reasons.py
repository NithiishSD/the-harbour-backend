import json

from fastapi.testclient import TestClient

from common.geo import load_ports
from dashboard import reasons
from dashboard.app import create_app
from dashboard.live import iso
from dashboard.weather import WeatherFeed, condition
from storage.hbase.tables import VESSEL_STATE
from tests.fakes import FakeHBase

H = 3_600_000
T0 = 1_672_617_600_000  # 2023-01-02T00:00Z


def record(port: str, hour_ms: int, wind: float | None, wave: float | None) -> bytes:
    rec = {"port_id": port, "hour": iso(hour_ms), "wind_speed_kn": wind, "wave_height_m": wave}
    return json.dumps(rec).encode()


def anchored(mmsi: str, since: int, seen: int, port: str = "USLGB") -> dict:
    return {"mmsi": mmsi, "state": "anchored", "port": port, "since": iso(since), "last_seen": iso(seen)}


def test_condition_thresholds():
    assert condition(None, None) is None
    assert condition(10, 1.0) == "calm"
    assert condition(22, None) == condition(5, 2.5) == "rough"
    assert condition(34, 0.5) == condition(None, 4.0) == "storm"


def test_weather_feed_keeps_the_newest_record_and_finds_the_worst_hour():
    w = WeatherFeed()
    assert not w.add_raw(b"not json") and not w.add_raw(record("USLGB", T0, None, None))
    assert w.add_raw(record("USLGB", T0, 30, 1.0)) and w.add_raw(record("USLGB", T0, 12, 1.0))  # re-forecast
    w.add_raw(record("USLGB", T0 + H, 40, 5.0))
    assert w.at("USLGB", T0 + 20 * 60_000)["condition"] == "calm"  # the later record for 00:00 won
    assert w.at("USLAX", T0) is None
    worst = w.worst("USLGB", T0, T0 + 2 * H)
    assert worst["condition"] == "storm" and worst["hour_ms"] == T0 + H


def test_feed_time_ignores_a_few_wrong_clocks_and_stale_ships_do_not_wait_now():
    ships = [anchored(str(367000000 + i), T0, T0 + i * 60_000) for i in range(40)]
    ships.append(anchored("367999999", T0, T0 + 10_000 * 24 * H))  # clock in the far future
    ships.append(anchored("367888888", T0 - 30 * 24 * H, T0 - 29 * 24 * H))  # last heard a month earlier
    now = reasons.feed_time(ships)
    assert T0 <= now <= T0 + 40 * 60_000
    assert reasons.waiting_counts(ships, now) == {
        "USLGB": 41
    }  # 40 now + the future one; the stale one is out


def test_wait_reasons():
    w = WeatherFeed()
    w.add_raw(record("USLGB", T0 + H, 36, 2.0))
    ship = anchored("367000001", T0, T0 + 3 * H)
    codes = lambda waiting, s=ship: [r["code"] for r in reasons.wait_reasons(s, waiting, w, "Long Beach")]  # noqa: E731
    assert codes({"USLGB": 3}) == ["weather"]
    assert codes({"USLGB": 12}) == ["weather", "congestion"]
    calm = WeatherFeed()
    long_wait = anchored("367000002", T0, T0 + 50 * H)
    assert [r["code"] for r in reasons.wait_reasons(long_wait, {}, calm, "Long Beach")] == ["long_wait"]
    assert [r["code"] for r in reasons.wait_reasons(ship, {}, calm, "Long Beach")] == ["unknown"]
    text = reasons.wait_reasons(ship, {}, w, "Long Beach")[0]["text"]
    assert text == "Storm weather at Long Beach while waiting: wind 36 kn, waves 2.0 m"
    assert reasons.wait_reasons({**ship, "state": "berthed"}, {}, w, "Long Beach") == []


def test_ports_api_reports_weather_and_congestion():
    h = FakeHBase()
    for i in range(10):
        cells = {"last:ts": str(T0 + H), "last:lat": "33.74", "last:lon": "-118.165", "last:area": "USLGB"}
        h.put(VESSEL_STATE, str(367100000 + i), {**cells, "last:state": "anchored"}, T0 + H)
    w = WeatherFeed()
    w.add_raw(record("USLGB", T0 + H, 25, 1.0))
    checks = {"kafka": lambda: "ok"}
    api = TestClient(create_app(h, load_ports(), checks, weather=w))  # type: ignore[arg-type]
    ports = {p["port_id"]: p for p in api.get("/api/ports").json()}
    assert ports["USLGB"]["waiting_now"] == 10 and ports["USLGB"]["congested"] is True
    assert ports["USLGB"]["weather"] == {
        "hour_ms": T0 + H,
        "wind_speed_kn": 25.0,
        "wave_height_m": 1.0,
        "condition": "rough",
    }
    assert ports["USLAX"]["congested"] is False and ports["USLAX"]["weather"] is None
    ship = api.get("/api/ships").json()[0]
    # no state change seen, so the wait is just the last report's hour, which was rough
    assert [r["code"] for r in ship["reasons"]] == ["weather", "congestion"]
