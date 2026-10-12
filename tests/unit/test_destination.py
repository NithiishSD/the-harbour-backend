from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from common.geo import load_ports
from common.schema import ais_eta, normalize
from dashboard import reasons
from dashboard.alerts import AlertFeed
from dashboard.app import create_app
from dashboard.destination import is_orders, match
from dashboard.live import iso
from dashboard.weather import WeatherFeed
from storage.hbase.tables import VESSEL_STATE
from streaming.sinks import info_put
from tests.fakes import FakeHBase
from tests.samples import aisstream_static, envelope

PORTS = load_ports()
H = 3_600_000


def at(*args: int) -> int:
    return int(datetime(*args, tzinfo=UTC).timestamp() * 1000)


@pytest.mark.parametrize(
    "text, port",
    [
        ("LONG BEACH", "USLGB"), ("USLGB", "USLGB"), ("US LGB", "USLGB"), ("LGB ANCH", "USLGB"),
        ("long beach,ca", "USLGB"), ("CNSHA>USLAX", "USLAX"), ("CN SHA => LOS ANGELES", "USLAX"),
        ("LA", "USLAX"), ("SINGAPORE", "SGSIN"), ("S'PORE", "SGSIN"), ("SG SIN EOPL", "SGSIN"),
        ("LA PAZ", None), ("CNSHA", None), ("FOR ORDERS", None), ("", None), (None, None),
    ],
)  # fmt: skip
def test_declared_destination_matches_our_ports(text, port):
    assert match(text, PORTS) == port


def test_orders():
    assert is_orders("FOR ORDERS") and is_orders("for orders.") and is_orders("TBA")
    assert not is_orders("LONG BEACH") and not is_orders(None)


def test_eta_gets_the_year_nearest_the_report():
    sent = at(2023, 12, 30, 12)
    assert ais_eta({"Month": 1, "Day": 2, "Hour": 6, "Minute": 30}, sent) == at(
        2024, 1, 2, 6, 30
    )  # next year
    assert ais_eta({"Month": 12, "Day": 20, "Hour": 0, "Minute": 0}, sent) == at(2023, 12, 20)  # late ship
    not_set = {"Month": 0, "Day": 0, "Hour": 24, "Minute": 60}
    feb_30 = {"Month": 2, "Day": 30, "Hour": 1, "Minute": 0}
    for unset in (not_set, feb_30, {"Month": "x"}, None):
        assert ais_eta(unset, sent) is None


def test_static_message_keeps_destination_and_eta_and_noaa_has_none():
    msg = aisstream_static(ts_ms=at(2023, 1, 1, 9))
    msg["Message"]["ShipStaticData"] |= {
        "Destination": "us lgb@@@@",
        "Eta": {"Month": 1, "Day": 3, "Hour": 14, "Minute": 0},
    }
    s = normalize(envelope(msg)).static
    assert s is not None and s["destination"] == "US LGB" and s["eta"] == at(2023, 1, 3, 14)
    cells = info_put(dict(s))[1]
    assert cells["info:dest"] == "US LGB" and cells["info:eta"] == str(at(2023, 1, 3, 14))
    assert "info:dest" not in info_put({"mmsi": "367000001", "ts": 0, "destination": None})[1]


def anchored(since: int, **extra: object) -> dict:
    return {"mmsi": "367000001", "state": "anchored", "port": "USLGB", "since": iso(since),
            "last_seen": iso(since + 2 * H), "destination": None, "eta": None} | extra  # fmt: skip


def codes(ship: dict) -> list[str]:
    return [r["code"] for r in reasons.wait_reasons(ship, {}, WeatherFeed(), "Long Beach")]


def test_orders_and_early_arrival_reasons():
    t = at(2023, 1, 2)
    assert codes(anchored(t, destination="FOR ORDERS")) == ["orders"]
    assert codes(anchored(t, destination="USLGB", eta=iso(t + 20 * H))) == ["early"]
    assert codes(anchored(t, eta=iso(t + H))) == ["unknown"]  # within 3 h of the ETA: on time
    assert codes(anchored(t, eta=iso(t + 30 * 24 * H))) == ["unknown"]  # a month off: stale text


def test_route_prefers_a_declared_port_and_keeps_the_text():
    h = FakeHBase()
    cells = {"last:ts": str(at(2023, 1, 2)), "last:lat": "1.20", "last:lon": "103.70",
             "last:state": "underway", "last:sog": "12", "last:area": ""}  # fmt: skip
    h.put(
        VESSEL_STATE, "563000001", {**cells, "info:dest": "LONG BEACH", "info:eta": str(at(2023, 1, 20, 8))}
    )
    h.put(VESSEL_STATE, "563000002", {**cells, "info:dest": "FOR ORDERS"})
    api = TestClient(create_app(h, PORTS, {"kafka": lambda: "ok"}, AlertFeed()))  # type: ignore[arg-type]
    r = api.get("/api/ships/563000001/route").json()
    assert (
        r["destination"]["port_id"] == "USLGB" and r["destination"]["basis"] == "declared"
    )  # across the Pacific
    assert r["declared"] == {"text": "LONG BEACH", "eta": "2023-01-20T08:00:00.000Z", "port_id": "USLGB",
                             "orders": False}  # fmt: skip
    r = api.get("/api/ships/563000002/route").json()
    assert r["destination"]["port_id"] == "SGSIN" and r["destination"]["basis"] == "nearest port"
    assert r["declared"]["orders"] is True
