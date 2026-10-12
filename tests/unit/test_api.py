import pytest
from fastapi.testclient import TestClient

from common.geo import load_ports
from dashboard.app import create_app
from storage.hbase.tables import (
    PORT_CALLS,
    PORT_QUEUE,
    VESSEL_STATE,
    VESSEL_TRACK,
    port_call_key,
    port_queue_key,
    track_key,
)
from tests.fakes import FakeHBase

T0 = 1_672_617_600_000  # 2023-01-02T00:00:00Z
HOUR = 3_600_000
A, B, C = "367000001", "367000002", "367000003"


@pytest.fixture
def hbase() -> FakeHBase:
    h = FakeHBase()
    # A: anchored off Long Beach since 02:00 (state change at 02:00, then reporting every batch).
    h.put(
        VESSEL_STATE,
        A,
        {"cur:state": "anchored", "cur:port": "USLGB", "cur:since": str(T0 + 2 * HOUR)},
        T0 + 2 * HOUR,
    )
    h.put(
        VESSEL_STATE,
        A,
        {
            "last:ts": str(T0 + 5 * HOUR),
            "last:lat": "33.74",
            "last:lon": "-118.165",
            "last:area": "USLGB",
            "last:state": "anchored",
            "info:name": "ALPHA",
            "info:type": "cargo",
        },
        T0 + 5 * HOUR,
    )
    # B: berthed at Los Angeles; history anchored -> berthed.
    h.put(VESSEL_STATE, B, {"cur:state": "anchored", "cur:port": "USLAX", "cur:since": str(T0)}, T0)
    h.put(
        VESSEL_STATE, B, {"cur:state": "berthed", "cur:port": "USLAX", "cur:since": str(T0 + HOUR)}, T0 + HOUR
    )
    h.put(
        VESSEL_STATE,
        B,
        {
            "last:ts": str(T0 + 4 * HOUR),
            "last:lat": "33.75",
            "last:lon": "-118.26",
            "last:area": "USLAX",
            "last:state": "berthed",
        },
        T0 + 4 * HOUR,
    )
    # C: sailing in open water, never changed state; only static details besides the position.
    h.put(
        VESSEL_STATE,
        C,
        {
            "last:ts": str(T0 + 3 * HOUR),
            "last:lat": "33.0",
            "last:lon": "-118.0",
            "last:area": "",
            "last:state": "underway",
        },
        T0 + 3 * HOUR,
    )
    h.put(VESSEL_STATE, "367000099", {"info:name": "NO POSITION"}, T0)  # static details only: not listed
    for i in range(3):
        ts = T0 + i * 60_000
        h.put(
            VESSEL_TRACK,
            track_key(A, ts),
            {"pos:lat": f"33.7{i}", "pos:lon": "-118.1", "pos:sog": "0.1", "pos:nav": "1"},
            ts,
        )
    for arrival, mmsi, cells in [
        (T0 + 2 * HOUR, A, {"call:anchored_at": str(T0 + 2 * HOUR)}),
        (T0, B, {"call:anchored_at": str(T0), "call:berthed_at": str(T0 + HOUR), "call:wait_hours": "1.0"}),
    ]:
        port = "USLGB" if mmsi == A else "USLAX"
        h.put(PORT_CALLS, port_call_key(port, arrival, mmsi), cells, arrival)
    for end in (T0 + HOUR, T0 + 2 * HOUR):
        h.put(
            PORT_QUEUE,
            port_queue_key("USLGB", end),
            {
                "q:window_start": str(end - HOUR // 2),
                "q:window_end": str(end),
                "q:ships": "4" if end == T0 + HOUR else "9",
            },
            end,
        )
    return h


@pytest.fixture
def api(hbase: FakeHBase) -> TestClient:
    checks = {"kafka": lambda: "ok", "hbase": lambda: "ok", "warehouse": lambda: "empty"}
    return TestClient(create_app(hbase, load_ports(), checks), raise_server_exceptions=False)  # type: ignore[arg-type]


def error(response, status: int, code: str) -> None:
    assert response.status_code == status
    body = response.json()
    assert list(body) == ["error"] and body["error"]["code"] == code and body["error"]["message"]


def test_health_ok_and_degraded(hbase):
    def app(**overrides):
        checks = {"kafka": lambda: "ok", "hbase": lambda: "ok", "warehouse": lambda: "ok"} | overrides
        return TestClient(create_app(hbase, load_ports(), checks))  # type: ignore[arg-type]

    r = app().get("/health")
    assert r.status_code == 200 and r.json() == {
        "status": "ok",
        "checks": {"kafka": "ok", "hbase": "ok", "warehouse": "ok"},
    }
    r = app(warehouse=lambda: "empty").get("/health")  # no nightly run yet: not an error
    assert r.status_code == 200 and r.json()["checks"]["warehouse"] == "empty"
    r = app(hbase=lambda: "unavailable").get("/health")
    assert (
        r.status_code == 503
        and r.json()["status"] == "degraded"
        and r.json()["checks"]["hbase"] == "unavailable"
    )


def test_ports_summary(api):
    ports = {p["port_id"]: p for p in api.get("/api/ports").json()}
    assert set(ports) == set(load_ports())
    lgb = ports["USLGB"]
    assert lgb["name"] and lgb["waiting_now"] == 1  # A anchored; B berthed and C underway do not count
    assert lgb["window_count"] == 9 and lgb["window_end"] == "2023-01-02T02:00:00.000Z"  # newest window
    assert ports["USLAX"]["waiting_now"] == 0 and ports["USLAX"]["window_count"] is None


def test_port_calls_and_queue_newest_first(api):
    calls = api.get("/api/ports/USLAX/calls").json()
    assert calls == [
        {
            "mmsi": B,
            "anchored_at": "2023-01-02T00:00:00.000Z",
            "berthed_at": "2023-01-02T01:00:00.000Z",
            "departed_at": None,
            "wait_hours": 1.0,
        }
    ]
    queue = api.get("/api/ports/USLGB/queue").json()
    assert [q["ships"] for q in queue] == [9, 4]
    assert api.get("/api/ports/USLGB/queue?limit=1").json() == queue[:1]
    assert api.get("/api/ports/SGSIN/calls").json() == []  # known port without data


def test_ships_and_filters(api):
    ships = {s["mmsi"]: s for s in api.get("/api/ships").json()}
    assert set(ships) == {A, B, C}  # the static-only row is not a ship on the map
    assert ships[A] == {
        "mmsi": A,
        "name": "ALPHA",
        "ship_type": "cargo",
        "length_m": None,  # the fixture row has no length
        "state": "anchored",
        "port": "USLGB",
        "since": "2023-01-02T02:00:00.000Z",
        "last_seen": "2023-01-02T05:00:00.000Z",
        "first_seen": None,  # rows written before first_seen existed
        "src_lat": None,  # nor was the first position kept
        "src_lon": None,
        "flag": "US",  # from the MMSI: 367 is a US ship
        "followed": False,  # not on the list ingest.follow works from (there is none in this test)
        "lat": 33.74,
        "lon": -118.165,
        "sog": None,  # the fixture row has no speed, course or heading; test_api_ui covers a ship with them
        "cog": None,
        "heading": None,
        "destination": None,  # NOAA-style fixture: no declared destination or ETA
        "eta": None,
        "map": "problem",  # waiting at anchor: always on the map
        "reasons": [  # 3 h at anchor, the only ship waiting, no weather known
            {
                "code": "unknown",
                "text": "No clear cause in the data: berth booking, pilot, tide or paperwork are not visible",
            }
        ],
    }
    assert ships[B]["reasons"] == [] and ships[C]["reasons"] == []  # not waiting
    assert ships[C]["port"] is None and ships[C]["since"] is None and ships[C]["name"] is None
    assert [s["mmsi"] for s in api.get("/api/ships?port_id=USLAX").json()] == [B]
    assert [s["mmsi"] for s in api.get("/api/ships?state=anchored").json()] == [A]
    assert api.get("/api/ships?port_id=USLAX&state=anchored").json() == []
    assert [s["mmsi"] for s in api.get("/api/ships").json()] == [A, B, C][:1] + [
        B,
        C,
    ]  # latest last_seen first


def test_track_and_states(api):
    track = api.get(f"/api/ships/{A}/track").json()
    assert [t["event_time"] for t in track] == [f"2023-01-02T00:0{i}:00.000Z" for i in (2, 1, 0)]
    assert track[0] == {
        "event_time": "2023-01-02T00:02:00.000Z",
        "lat": 33.72,
        "lon": -118.1,
        "sog": 0.1,
        "cog": None,
        "heading": None,
        "nav": 1,
    }
    assert len(api.get(f"/api/ships/{A}/track?limit=2").json()) == 2
    states = api.get(f"/api/ships/{B}/states").json()
    assert [(s["state"], s["port"], s["since"]) for s in states] == [
        ("berthed", "USLAX", "2023-01-02T01:00:00.000Z"),
        ("anchored", "USLAX", "2023-01-02T00:00:00.000Z"),
    ]
    assert states[0]["ts"] == "2023-01-02T01:00:00.000Z"
    assert api.get(f"/api/ships/{C}/states").json() == []  # a known ship that never changed state


@pytest.mark.parametrize(
    "path",
    [
        "/api/ships/12345/track",
        "/api/ships/12345678a/track",
        "/api/ships/1234567890/states",
        "/api/ships/%27%3B%20DROP/track",
    ],
)
def test_bad_mmsi_is_422(api, path):
    error(api.get(path), 422, "INVALID_MMSI")


@pytest.mark.parametrize(
    "path",
    [
        "/api/ports/USLAX/calls?limit=0",
        "/api/ports/USLAX/calls?limit=501",
        "/api/ports/USLAX/queue?limit=x",
        "/api/ships/367000001/track?limit=1001",
        "/api/ships?state=sunk",
        "/api/ports/USLAX/calls?limit=-1",
    ],
)
def test_bad_params_are_422(api, path):
    error(api.get(path), 422, "INVALID_PARAM")


def test_limit_bounds_are_accepted(api):
    assert api.get("/api/ports/USLAX/calls?limit=1").status_code == 200
    assert api.get("/api/ports/USLAX/calls?limit=500").status_code == 200
    assert api.get(f"/api/ships/{A}/track?limit=1000").status_code == 200


@pytest.mark.parametrize(
    "path",
    [
        "/api/ports/XXXXX/calls",
        "/api/ports/uslax/queue",
        "/api/ships?port_id=nowhere",
    ],
)
def test_unknown_port_is_404(api, path):
    error(api.get(path), 404, "UNKNOWN_PORT")


def test_unknown_ship_is_404(api):
    error(api.get("/api/ships/999999999/track"), 404, "NOT_FOUND")
    error(api.get("/api/ships/999999999/states"), 404, "NOT_FOUND")


def test_unknown_route_and_method_use_the_error_shape(api):
    error(api.get("/api/nothing"), 404, "NOT_FOUND")
    error(api.get("/api/ports/US%2FLAX/queue"), 404, "NOT_FOUND")  # an encoded slash is not a port id
    error(api.post("/api/ports"), 405, "METHOD_NOT_ALLOWED")


def test_hbase_down_is_503_and_leaks_nothing(api, hbase):
    hbase.down = True
    for path in [
        "/api/ports",
        "/api/ships",
        "/api/ports/USLAX/calls",
        f"/api/ships/{A}/track",
        f"/api/ships/{A}/states",
    ]:
        r = api.get(path)
        error(r, 503, "HBASE_UNAVAILABLE")
        assert "refused" not in r.text and "Traceback" not in r.text


def test_unexpected_error_is_500_without_internals(hbase):
    def boom() -> str:
        raise RuntimeError("secret internal detail")

    client = TestClient(create_app(hbase, load_ports(), {"kafka": boom}), raise_server_exceptions=False)  # type: ignore[arg-type]
    r = client.get("/health")
    error(r, 500, "INTERNAL_ERROR")
    assert "secret" not in r.text
