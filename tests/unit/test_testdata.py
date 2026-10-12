from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from batch.archive_history import prune_cold, prune_history
from batch.purge_test_data import purge_hbase
from common.testdata import FIRST_SEED_MMSI, SEED_SHIPS, RealData
from storage.hbase.tables import PORT_CALLS, PORT_QUEUE, VESSEL_STATE, VESSEL_TRACK
from tests.fakes import FakeHBase

NOW = int(datetime(2026, 10, 12, tzinfo=UTC).timestamp() * 1000)
HOUR = 3_600_000


def ms(y: int, m: int, d: int, h: int = 12) -> int:
    return int(datetime(y, m, d, h, tzinfo=UTC).timestamp() * 1000)


@pytest.fixture
def real(tmp_path: Path) -> RealData:
    folder = tmp_path / "history" / "positions"
    (folder / "date=2023-01-01").mkdir(parents=True)
    (folder / "date=2023-01-01" / "part-0.parquet").write_bytes(b"noaa")
    (folder / "date=2026-10-10").mkdir()
    (folder / "date=2026-10-10" / "aisstream.parquet").write_bytes(b"live")  # not a NOAA day
    return RealData.load(tmp_path, date(2026, 10, 10))


def test_real_time_is_a_noaa_day_or_the_live_feeds_life(real: RealData):
    assert real.noaa_days == {"2023-01-01"}
    assert real.real_time(ms(2023, 1, 1), NOW)  # a NOAA day
    assert not real.real_time(ms(2023, 1, 2), NOW)  # no NOAA file for this day
    assert real.real_time(ms(2026, 10, 10, 0), NOW) and real.real_time(NOW, NOW)  # the live feed
    assert not real.real_time(ms(2026, 10, 8), NOW)  # before the live feed: the measurement probes
    assert not real.real_time(ms(2025, 1, 6), NOW)  # the test suites' days
    assert not real.real_time(NOW + 2 * 24 * HOUR, NOW)  # stamped ahead
    assert real.real_time(None, NOW)  # no time: cannot be judged


def test_test_ships_are_told_by_id_name_place_or_time(real: RealData):
    live = ms(2026, 10, 11)
    assert not real.is_test_ship("367000001", "REAL SHIP", 33.7, -118.2, live)
    assert real.is_test_ship(str(FIRST_SEED_MMSI), None, None, None, live)  # the demo fleet
    assert real.is_test_ship(str(FIRST_SEED_MMSI + SEED_SHIPS - 1), None, None, None, live)
    assert not real.is_test_ship(str(FIRST_SEED_MMSI + SEED_SHIPS), "OTHER", None, None, live)
    for name in ("TEST TANKER", "e2e anchor 12", "STUB MOVER", "MEASURE", "NEW NAME"):
        assert real.is_test_ship("367000002", name, None, None, live), name
    assert real.is_test_ship("340654001", None, 20.0, -150.0, live)  # a probe's place
    assert real.is_test_ship("367000003", "REAL", 33.7, -118.2, ms(2025, 1, 6))  # a day nobody had data for


def test_purge_counts_then_removes_only_test_rows(real: RealData):
    h = FakeHBase()

    def ship(mmsi: str, ts: int, name: str = "", lat: str = "33.7", lon: str = "-118.2") -> None:
        h.put(VESSEL_STATE, mmsi, {"last:ts": str(ts), "last:lat": lat, "last:lon": lon, "info:name": name})
        h.put(VESSEL_TRACK, f"{mmsi}9999", {"pos:lat": lat}, 1)

    ship("367000001", ms(2023, 1, 1), "NOAA SHIP")
    ship("367000002", ms(2026, 10, 11), "LIVE SHIP")
    ship("340654001", ms(2026, 10, 8), "", "20.0", "-150.0")  # probe
    ship(str(FIRST_SEED_MMSI), ms(2026, 10, 11), "PACIFIC ARROW 1")  # demo fleet
    ship("307836001", NOW + 400 * 24 * HOUR)  # stamped ahead
    ship("251607462", ms(2025, 1, 6))  # test suite day
    h.put(PORT_QUEUE, "USLGB-a", {"q:window_end": str(ms(2023, 1, 1))})
    h.put(PORT_QUEUE, "USLGB-b", {"q:window_end": str(ms(2105, 8, 14))})
    h.put(PORT_CALLS, "USLAX-a", {"call:berthed_at": str(ms(2023, 1, 1))})
    h.put(PORT_CALLS, "USLAX-b", {"call:berthed_at": str(ms(2231, 2, 10))})

    dry = purge_hbase(h, real, apply=False)  # type: ignore[arg-type]
    assert dry == {"ships": 4, "track_rows": 4, "queue_rows": 1, "call_rows": 1}
    assert len(h.tables[VESSEL_STATE]) == 6  # nothing changed

    assert purge_hbase(h, real, apply=True) == dry  # type: ignore[arg-type]
    assert sorted(h.tables[VESSEL_STATE]) == ["367000001", "367000002"]
    assert sorted(h.tables[VESSEL_TRACK]) == ["3670000019999", "3670000029999"]
    assert list(h.tables[PORT_QUEUE]) == ["USLGB-a"] and list(h.tables[PORT_CALLS]) == ["USLAX-a"]


def _day(root: Path, name: str, day: str, mb: float = 1.0) -> Path:
    folder = root / f"{name}" / f"date={day}"
    folder.mkdir(parents=True)
    f = folder / "aisstream.parquet"
    f.write_bytes(b"x" * int(mb * 1_048_576))
    return f


def test_history_is_cut_to_its_size_oldest_day_first_and_never_touches_noaa(tmp_path: Path):
    for day in ("2026-10-10", "2026-10-11", "2026-10-12"):
        for name in ("positions", "vessels"):
            _day(tmp_path, name, day)  # 2 MB a day
    noaa = tmp_path / "positions" / "date=2023-01-01" / "part-0.parquet"
    noaa.parent.mkdir(parents=True)
    noaa.write_bytes(b"x" * 5_000_000)
    assert prune_history(tmp_path, 100) == []  # under the cap
    assert prune_history(tmp_path, 4.5) == ["2026-10-10"]  # 6 MB -> 4 MB
    assert not (tmp_path / "positions" / "date=2026-10-10").exists()
    assert prune_history(tmp_path, 0) == ["2026-10-11"]  # the newest day always stays
    assert (tmp_path / "positions" / "date=2026-10-12" / "aisstream.parquet").exists() and noaa.exists()


def test_raw_archive_is_cut_to_its_size_oldest_day_first(tmp_path: Path):
    for day in ("2026-10-10", "2026-10-11", "2026-10-12"):
        folder = tmp_path / "topic=ais.raw" / f"date={day}"
        folder.mkdir(parents=True)
        (folder / "part.parquet").write_bytes(b"x" * 1_048_576)
    assert prune_cold(tmp_path, 1.5) == ["2026-10-10", "2026-10-11"]
    assert [p.name for p in (tmp_path / "topic=ais.raw").iterdir()] == ["date=2026-10-12"]


def test_alerts_of_tests_and_of_impossible_times_are_not_real(real: RealData):
    live = "2026-10-11T08:00:00.000Z"
    assert not real.is_test_alert({"alert_id": "6ff72bef-1", "event_time": live})
    assert not real.is_test_alert(
        {"alert_id": "6ff72bef-2", "event_time": "2023-01-01T08:00:00.000Z"}
    )  # NOAA day
    assert real.is_test_alert({"alert_id": "e2e-1", "event_time": live})
    assert real.is_test_alert({"alert_id": "it-1"})
    assert real.is_test_alert({"alert_id": "6ff72bef-3", "event_time": "2231-02-10T20:57:00.000Z"})
    assert real.is_test_alert({"alert_id": "6ff72bef-4", "event_time": "2025-01-06T08:00:00.000Z"})
