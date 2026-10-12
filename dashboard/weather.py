"""Port weather for the dashboard: hourly wind and waves per port from the weather.marine topic.

Like the alert feed, the topic is re-read from the beginning on every start (7 days of retention). Forecasts
are re-published every hour, so the record that arrived last for a port and hour wins.
"""

import json
import threading
from datetime import UTC, datetime
from typing import Any

# Beaufort 6 (strong breeze) and 8 (gale); 2.5 m and 4 m waves are where pilot boarding and crane work stop.
ROUGH_WIND_KN, STORM_WIND_KN = 22.0, 34.0
ROUGH_WAVE_M, STORM_WAVE_M = 2.5, 4.0
HOUR_MS = 3_600_000
RANK = {"calm": 0, "rough": 1, "storm": 2}


def condition(wind_kn: float | None, wave_m: float | None) -> str | None:
    """storm, rough or calm; None when neither value is known."""
    if wind_kn is None and wave_m is None:
        return None
    wind, wave = wind_kn or 0.0, wave_m or 0.0
    if wind >= STORM_WIND_KN or wave >= STORM_WAVE_M:
        return "storm"
    if wind >= ROUGH_WIND_KN or wave >= ROUGH_WAVE_M:
        return "rough"
    return "calm"


def _hour_ms(text: str) -> int | None:
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    return int((dt if dt.tzinfo else dt.replace(tzinfo=UTC)).timestamp() * 1000)


def _number(value: Any) -> float | None:
    return float(value) if isinstance(value, int | float) and not isinstance(value, bool) else None


class WeatherFeed:
    """(port_id, hour in epoch ms) -> {wind_speed_kn, wave_height_m, condition}. Thread-safe."""

    def __init__(self) -> None:
        self._hours: dict[tuple[str, int], dict[str, Any]] = {}
        self._lock = threading.Lock()

    def add_raw(self, raw: bytes | None) -> bool:
        """One weather.marine message; False when it is not a usable record."""
        try:
            rec = json.loads(raw or b"")
        except ValueError:
            return False
        if not isinstance(rec, dict) or not isinstance(rec.get("port_id"), str):
            return False
        hour = _hour_ms(str(rec.get("hour", "")))
        wind, wave = _number(rec.get("wind_speed_kn")), _number(rec.get("wave_height_m"))
        state = condition(wind, wave)
        if hour is None or state is None:
            return False
        with self._lock:
            self._hours[(rec["port_id"], hour)] = {
                "wind_speed_kn": wind,
                "wave_height_m": wave,
                "condition": state,
            }
        return True

    def at(self, port_id: str, ms: int) -> dict[str, Any] | None:
        """The record for the hour containing ms (or the hour after, while it is the newest)."""
        with self._lock:
            for hour in (ms - ms % HOUR_MS, ms - ms % HOUR_MS + HOUR_MS):
                rec = self._hours.get((port_id, hour))
                if rec:
                    return {"hour_ms": hour, **rec}
        return None

    def worst(self, port_id: str, start_ms: int, end_ms: int) -> dict[str, Any] | None:
        """The roughest hour at the port between start and end (both inclusive, hour resolution)."""
        best: dict[str, Any] | None = None
        hour = start_ms - start_ms % HOUR_MS
        with self._lock:
            while hour <= end_ms:
                rec = self._hours.get((port_id, hour))
                if rec and (best is None or RANK[rec["condition"]] > RANK[best["condition"]]):
                    best = {"hour_ms": hour, **rec}
                hour += HOUR_MS
        return best


# Beaufort 10 (storm force) and up is a cyclone; only the strongest point within 10 degrees counts, so
# one storm over several neighbouring points gets one swirl.
CYCLONE_WIND_KN = 48.0
CYCLONE_SPACING_DEG = 10.0


class GridFeed:
    """Weather points over the open ocean (topic weather.grid): hour in epoch ms -> point id -> record.

    Same rules as WeatherFeed: the topic is re-read on every start and the record that arrived last wins.
    """

    def __init__(self) -> None:
        self._hours: dict[int, dict[str, dict[str, Any]]] = {}
        self._lock = threading.Lock()

    def add_raw(self, raw: bytes | None) -> bool:
        try:
            rec = json.loads(raw or b"")
        except ValueError:
            return False
        if not isinstance(rec, dict) or not isinstance(rec.get("point_id"), str):
            return False
        lat, lon = _number(rec.get("lat")), _number(rec.get("lon"))
        wind, wave = _number(rec.get("wind_speed_kn")), _number(rec.get("wave_height_m"))
        hour, state = _hour_ms(str(rec.get("hour", ""))), condition(wind, wave)
        if lat is None or lon is None or not -90 <= lat <= 90 or not -180 <= lon <= 180:
            return False
        if hour is None or state is None:
            return False
        with self._lock:
            self._hours.setdefault(hour, {})[rec["point_id"]] = {
                "point_id": rec["point_id"],
                "lat": lat,
                "lon": lon,
                "wind_speed_kn": wind,
                "wave_height_m": wave,
                "condition": state,
            }
        return True

    def at(self, ms: int) -> list[dict[str, Any]]:
        """The points for the hour containing ms (or the hour after, while it is the newest), each with
        hour_ms and `cyclone`."""
        with self._lock:
            for hour in (ms - ms % HOUR_MS, ms - ms % HOUR_MS + HOUR_MS):
                if hour in self._hours:
                    points = [{"hour_ms": hour, **p} for p in self._hours[hour].values()]
                    break
            else:
                return []
        for p in points:
            p["cyclone"] = (p["wind_speed_kn"] or 0) >= CYCLONE_WIND_KN and not any(
                (q["wind_speed_kn"] or 0) > p["wind_speed_kn"]
                and abs(q["lat"] - p["lat"]) <= CYCLONE_SPACING_DEG
                and abs((q["lon"] - p["lon"] + 180) % 360 - 180) <= CYCLONE_SPACING_DEG
                for q in points
            )
        return points
