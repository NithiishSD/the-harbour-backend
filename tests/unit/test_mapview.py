from fastapi.testclient import TestClient

from common.geo import load_ports
from dashboard import mapview
from dashboard.alerts import AlertFeed
from dashboard.app import create_app
from dashboard.live import iso
from storage.hbase.tables import VESSEL_STATE
from tests.fakes import FakeHBase

H = 3_600_000
NOW = 1_767_225_600_000  # 2026-01-01T00:00Z


def ship(
    mmsi: str, state: str = "underway", seen_h: float = 0, since_h: float | None = None, first_h: float = 50
):
    return {
        "mmsi": mmsi,
        "name": f"SHIP {mmsi}",
        "state": state,
        "port": "USLGB" if state != "underway" else None,
        "last_seen": iso(int(NOW - seen_h * H)),
        "since": None if since_h is None else iso(int(NOW - since_h * H)),
        "first_seen": iso(int(NOW - first_h * H)),
    }


def drawn(ships, pinned=(), shown=(), dark=(), limit=100):
    return {
        s["mmsi"]: s["map"] for s in mapview.on_map(ships, NOW, set(dark), set(pinned), set(shown), limit)
    }


def test_travelling_ships_stay_until_they_go_silent():
    assert drawn([ship("1", seen_h=1), ship("2", seen_h=23)]) == {"1": "live", "2": "live"}
    assert drawn([ship("3", seen_h=25)]) == {}  # left our coverage: history only


def test_ships_only_passing_through_the_waters_go_first_and_go_early():
    def passing(mmsi, **kw):
        return ship(mmsi, **kw) | {"roles": ["in"]}

    def tracked(mmsi, **kw):
        return ship(mmsi, **kw) | {"roles": ["to"]}

    # a ship tracked for its voyage is kept a day of silence, a passing one only a few hours
    assert drawn([tracked("1", seen_h=20), passing("2", seen_h=20), passing("3", seen_h=2)]) == {
        "1": "live",
        "3": "live",
    }
    # under the cap the passing ships leave first, even when they arrived on the map later
    fleet = [tracked("a", first_h=40), tracked("b", first_h=30)]
    fleet += [passing("c", first_h=1), passing("d", first_h=2)]
    assert set(drawn(fleet, limit=2)) == {"a", "b"}
    assert set(drawn(fleet, limit=3)) == {"a", "b", "c"}  # then the newer of them
    # an anchored passing ship is not "a problem" to keep for a week
    assert drawn([passing("e", state="anchored", seen_h=10)]) == {}
    assert drawn([passing("f", state="anchored", seen_h=1)]) == {"f": "live"}


def test_arrived_ships_leave_a_day_after_arriving():
    ships = [
        ship("1", "berthed", since_h=23),
        ship("2", "berthed", since_h=25),
        ship("3", "departed", since_h=30),
    ]
    assert drawn(ships) == {"1": "live"}


def test_waiting_and_dark_ships_are_always_drawn():
    ships = [ship("1", "anchored", seen_h=150, since_h=300), ship("2", "berthed", seen_h=40, since_h=48)]
    assert drawn(ships, dark={"2"}) == {"1": "problem", "2": "problem"}
    assert drawn([ship("3", "anchored", seen_h=8 * 24, since_h=9 * 24)]) == {}  # forgotten after a week


def test_pinned_ships_stay_after_silence_and_arrival():
    ships = [ship("1", seen_h=100), ship("2", "berthed", since_h=30), ship("3", "anchored", seen_h=8 * 24)]
    assert drawn(ships, pinned={"1", "2", "3"}) == {"1": "pinned", "2": "pinned", "3": "pinned"}
    assert drawn(ships) == {}


def test_shown_history_ships_are_always_drawn():
    assert drawn([ship("1", "berthed", seen_h=500, since_h=500)], shown={"1"}) == {"1": "shown"}


def test_size_cap_drops_the_first_arrivals_but_never_pinned_ones():
    ships = [ship(str(i), first_h=10 - i) for i in range(6)]  # ship 5 arrived on the map last
    assert set(drawn(ships, limit=3)) == {"3", "4", "5"}
    assert set(drawn(ships, pinned={"0"}, limit=3)) == {"0", "3", "4", "5"}  # pinned do not count


def test_size_cap_keeps_the_waiting_queue():
    ships = [ship(str(i), first_h=10 - i) for i in range(6)]
    ships[0]["state"] = "anchored"  # the first to arrive, and still waiting for a berth
    assert set(drawn(ships, limit=3)) == {"0", "4", "5"}


def test_dark_ships_need_an_alert_after_the_last_report():
    ships = [ship("367000001", seen_h=8), ship("367000002", seen_h=1)]
    alerts = [
        {"type": "dark_ship", "mmsi": "367000001", "event_time": iso(NOW - 7 * H)},
        {"type": "dark_ship", "mmsi": "367000002", "event_time": iso(NOW - 2 * H)},  # reported since
    ]
    assert mapview.dark_ships(alerts, ships) == {"367000001"}


def put(h: FakeHBase, mmsi: str, state: str, seen: int, name: str) -> None:
    cells = {
        "last:ts": str(seen),
        "last:lat": "33.0",
        "last:lon": "-118.0",
        "last:area": "",
        "last:state": state,
    }
    h.put(VESSEL_STATE, mmsi, {**cells, "info:name": name, "last:first": str(seen - H)}, seen)


def test_ships_api_params_and_search():
    h = FakeHBase()
    for i in range(30):
        put(h, f"36700{i:04d}", "underway", NOW - i * 60_000, f"FLEET {i}")
    put(h, "367999999", "underway", NOW - 100 * H, "OLD TIMER")  # silent for days: history only
    api = TestClient(create_app(h, load_ports(), {"kafka": lambda: "ok"}, alerts=AlertFeed()))  # type: ignore[arg-type]
    assert len(api.get("/api/ships").json()) == 30
    assert len(api.get("/api/ships?limit=5").json()) == 5
    assert "367999999" in {s["mmsi"] for s in api.get("/api/ships?pinned=367999999&limit=5").json()}
    assert api.get("/api/ships?show=367999999").json()[-1]["map"] == "shown"
    assert api.get("/api/ships?pinned=12345").status_code == 422
    hits = api.get("/api/ships/search?q=old").json()
    assert [(s["mmsi"], s["on_map"]) for s in hits] == [("367999999", False)]
    assert api.get("/api/ships/search?q=3670000").json()[0]["on_map"] is True
    assert api.get("/api/ships/search?q=x").status_code == 422


def test_ships_stamped_in_the_future_do_not_move_the_map_clock():
    h = FakeHBase()
    for i in range(10):
        put(h, f"36700{i:04d}", "underway", NOW - i * 60_000, f"FLEET {i}")
    for i in range(5):  # a third of the rows from a broken clock, years ahead
        put(h, f"39340{i:04d}", "anchored", NOW + 2_000_000 * H, "FUTURE")
    api = TestClient(create_app(h, load_ports(), {"kafka": lambda: "ok"}, alerts=AlertFeed()))  # type: ignore[arg-type]
    shown = api.get("/api/ships").json()
    assert len(shown) == 10 and all(s["name"].startswith("FLEET") for s in shown)
