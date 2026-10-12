"""What is real data and what is test or demo data in the dev stack.

Real data is NOAA (the days held in data/history) and the live feed from LIVE_SINCE on. Everything else that
carries a time is not: the test days of 2025, the measurement probes of 2026-10-08, rows stamped decades
ahead. Demo ships (ingest.seed) and the probes and fixtures of the test suites are told apart by id, name
or place.

The live map, the history archive and the clean-up tool all ask the same questions here, so they agree.
"""

import time
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

FIRST_SEED_MMSI = 538_070_001  # ingest.seed: the demo fleet is this MMSI and the next SEED_SHIPS - 1
SEED_SHIPS = 24
PROBE_SPOT = (20.0, -150.0)  # measure.run puts every probe ship here
TEST_NAME_PREFIXES = ("TEST", "E2E", "STUB", "MEASURE", "NEW NAME")
TEST_ALERT_PREFIXES = ("e2e-", "it-")  # ids the end-to-end and integration tests give the alerts they send
DAY_MS = 86_400_000


def day_of(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).date().isoformat()


@dataclass(frozen=True)
class RealData:
    noaa_days: frozenset[str]  # YYYY-MM-DD of every NOAA day in data/history
    live_since_ms: int  # the live feed has existed since this moment

    @classmethod
    def load(cls, data_dir: Path, live_since: date) -> "RealData":
        folder = data_dir / "history" / "positions"
        days = {
            part.parent.name.removeprefix("date=")
            for part in folder.glob("date=*/*.parquet")
            if part.name != "aisstream.parquet"  # the NOAA backfill writes part-*.parquet
        }
        since = datetime(live_since.year, live_since.month, live_since.day, tzinfo=UTC)
        return cls(frozenset(days), int(since.timestamp() * 1000))

    def real_time(self, ts_ms: int | None, now_ms: int | None = None) -> bool:
        """A time that data can honestly have: a NOAA day, or the live feed's life. None cannot be judged."""
        if ts_ms is None:
            return True
        now = int(time.time() * 1000) if now_ms is None else now_ms
        if ts_ms > now + DAY_MS:
            return False
        return ts_ms >= self.live_since_ms or day_of(ts_ms) in self.noaa_days

    def is_test_alert(self, alert: dict[str, Any]) -> bool:
        """An alert sent by a test, or about a time real data cannot have."""
        if str(alert.get("alert_id", "")).startswith(TEST_ALERT_PREFIXES):
            return True
        try:
            when: int | None = int(datetime.fromisoformat(str(alert["event_time"])).timestamp() * 1000)
        except (KeyError, ValueError):
            when = None
        return not self.real_time(when)

    def is_test_ship(
        self,
        mmsi: str,
        name: str | None = None,
        lat: float | None = None,
        lon: float | None = None,
        ts_ms: int | None = None,
    ) -> bool:
        if mmsi.isdigit() and FIRST_SEED_MMSI <= int(mmsi) < FIRST_SEED_MMSI + SEED_SHIPS:
            return True
        if name and name.strip().upper().startswith(TEST_NAME_PREFIXES):
            return True
        if (
            lat is not None
            and lon is not None
            and abs(lat - PROBE_SPOT[0]) < 1e-6
            and abs(lon - PROBE_SPOT[1]) < 1e-6
        ):
            return True
        return not self.real_time(ts_ms)
