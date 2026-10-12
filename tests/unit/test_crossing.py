from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pandas as pd

from common.geo import ZoneIndex, haversine_km, load_ports, load_zones
from dashboard import crossing, mapview
from dashboard.history import HistoryStore

PORTS = load_ports()
H = 3_600_000
T0 = int(datetime(2026, 10, 10, 6, tzinfo=UTC).timestamp() * 1000)


def iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).isoformat().replace("+00:00", "Z")


def ship(**kw) -> dict:
    base = {
        "mmsi": "244850097", "state": "underway", "port": None, "sog": 14.0, "cog": 80.0, "lat": 33.0,
        "lon": 142.0, "last_seen": iso(T0), "destination": "LOS ANGELES",
    }  # fmt: skip
    return base | kw


def test_a_ship_out_of_range_is_placed_along_the_sea_route():
    found = crossing.estimate(ship(), T0 + 100 * H, PORTS)
    assert found and found["estimated"] is True
    travelled = haversine_km(33.0, 142.0, found["lat"], found["lon"])
    assert 0.9 * 14 * 1.852 * 100 <= travelled <= 1.02 * 14 * 1.852 * 100  # about 2,600 km along the route
    assert found["lon"] > 140 or found["lon"] < -170  # across the date line, on the way to Los Angeles
    assert found["estimated_from"] == {"lat": 33.0, "lon": 142.0, "last_seen": iso(T0)}


def test_only_a_ship_on_a_crossing_is_estimated():
    now = T0 + 100 * H
    assert crossing.estimate(ship(destination=None), now, PORTS) is None  # nowhere to go as far as we know
    assert crossing.estimate(ship(destination="ROTTERDAM"), now, PORTS) is None  # not a port on this route
    assert crossing.estimate(ship(sog=3.0), now, PORTS) is None  # not at sea speed
    assert crossing.estimate(ship(state="anchored"), now, PORTS) is None
    assert crossing.estimate(ship(port="USLGB"), now, PORTS) is None  # in a port area
    assert crossing.estimate(ship(), T0 + 10 * 60_000, PORTS) is None  # the report is the position
    assert crossing.estimate(ship(), T0 + 30 * 24 * H, PORTS) is None  # longer than any crossing
    assert crossing.estimate(ship(), T0 + 24 * 24 * H, PORTS) is None  # it should have arrived by now
    assert crossing.estimate(ship(lat=85.0, lon=100.0), now, PORTS) is None  # nowhere near a lane


def test_apply_replaces_positions_and_the_map_keeps_the_ship():
    ships = [ship(), ship(mmsi="244850098", destination=None)]
    assert crossing.apply(ships, T0 + 100 * H, PORTS) == 1
    assert ships[0]["estimated"] and "estimated" not in ships[1]
    ships[0]["first_seen"] = ships[0]["last_seen"]
    ships[0]["since"] = None
    # 100 h silent is far beyond the 24 h rule that sends a ship to history, but an estimated ship stays
    kept = mapview.on_map(ships, T0 + 100 * H, set(), set(), set(), 100)
    assert [s["mmsi"] for s in kept] == ["244850097"]
    assert crossing.apply(ships, None, PORTS) == 0


def test_offline_snapshot_estimates_ships_that_left_range(tmp_path: Path):
    def put(name: str, rows: list[dict]) -> None:
        day = datetime.fromtimestamp(T0 / 1000, UTC).date().isoformat()
        folder = tmp_path / name / f"date={day}"
        folder.mkdir(parents=True)
        con = duckdb.connect()
        con.register("rows", pd.DataFrame(rows))
        con.execute(f"COPY (SELECT * FROM rows) TO '{folder / 'aisstream.parquet'}' (FORMAT parquet)")

    put("positions", [
        {"mmsi": "244850097", "ts": T0, "lat": 33.0, "lon": 142.0, "sog": 14.0, "cog": 80.0, "heading": 80,
         "nav_status": 0, "source": "aisstream"},
    ])  # fmt: skip
    put("destinations", [{"mmsi": "244850097", "ts": T0, "destination": "LOS ANGELES", "eta": None}])
    store = HistoryStore(tmp_path, ZoneIndex(load_zones()), PORTS)
    near = store.snapshot(T0 + 20 * 60_000)
    assert [(s["mmsi"], s.get("estimated")) for s in near] == [("244850097", None)]  # still reporting
    far = store.snapshot(T0 + 100 * H)
    assert [(s["mmsi"], s.get("estimated")) for s in far] == [("244850097", True)]
    assert far[0]["last_seen"] == "2026-10-10T06:00:00.000Z"  # the real last report is kept
    assert store.snapshot(T0 + 30 * 24 * H) == []
