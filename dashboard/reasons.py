"""Why is a ship waiting? Rules over what the dashboard knows: queue size, port weather, time at anchor.

These are likely reasons, not facts: berth bookings, pilots, tides and paperwork are not visible in AIS or
weather data, so a wait with none of the signals below says so instead of guessing.
"""

from datetime import datetime
from typing import Any

from dashboard import destination
from dashboard.weather import WeatherFeed

ACTIVE_HOURS = 24  # a ship last heard more than this before the feed time is not waiting "now"
# ponytail: one threshold for all ports; use each port's usual queue from the warehouse if they differ a lot
CONGESTED_SHIPS = 10
LONG_WAIT_HOURS = 48
EARLY_HOURS = 3  # anchored at least this long before the declared ETA counts as an early arrival
EARLY_MAX_DAYS = 14  # a declared ETA further off than this is stale text, not a booking
HOUR_MS = 3_600_000


def ms(iso: str | None) -> int | None:
    return None if iso is None else int(datetime.fromisoformat(iso).timestamp() * 1000)


def feed_time(ships: list[dict[str, Any]]) -> int | None:
    """95th percentile of report times, as in the browser: one ship with a wrong clock cannot move 'now'."""
    times = sorted(t for t in (ms(s["last_seen"]) for s in ships) if t is not None)
    return times[int((len(times) - 1) * 0.95)] if times else None


def active(ship: dict[str, Any], now: int | None) -> bool:
    seen = ms(ship["last_seen"])
    return now is not None and seen is not None and seen >= now - ACTIVE_HOURS * HOUR_MS


def waiting_counts(ships: list[dict[str, Any]], now: int | None) -> dict[str, int]:
    counts: dict[str, int] = {}
    for s in ships:
        if s["state"] == "anchored" and s["port"] and active(s, now):
            counts[s["port"]] = counts.get(s["port"], 0) + 1
    return counts


def wait_reasons(
    ship: dict[str, Any], waiting: dict[str, int], weather: WeatherFeed, port_name: str
) -> list[dict[str, str]]:
    """Likely reasons for an anchored ship's wait, most specific first; [] for a ship that is not waiting."""
    if ship["state"] != "anchored" or not ship["port"]:
        return []
    end = ms(ship["last_seen"]) or 0
    start = ms(ship["since"]) or end
    out = []
    worst = weather.worst(ship["port"], start, end)
    if worst and worst["condition"] != "calm":
        wind, wave = worst["wind_speed_kn"], worst["wave_height_m"]
        measured = ", ".join(
            part
            for part in (
                None if wind is None else f"wind {wind:.0f} kn",
                None if wave is None else f"waves {wave:.1f} m",
            )
            if part
        )
        out.append(
            {
                "code": "weather",
                "level": worst["condition"],
                "text": f"{worst['condition'].capitalize()} weather at {port_name} while waiting: {measured}",
            }
        )
    queued = waiting.get(ship["port"], 0)
    if queued >= CONGESTED_SHIPS:
        out.append({"code": "congestion", "text": f"Berth queue: {queued} ships waiting at {port_name}"})
    if destination.is_orders(ship.get("destination")):
        out.append(
            {
                "code": "orders",
                "text": f"Declared destination '{ship['destination']}': waiting for the charterer's orders",
            }
        )
    eta = ms(ship.get("eta"))
    if eta is not None and EARLY_HOURS * HOUR_MS <= eta - start <= EARLY_MAX_DAYS * 24 * HOUR_MS:
        out.append(
            {
                "code": "early",
                "text": f"Arrived {(eta - start) / HOUR_MS:.0f} h before its declared ETA: waits for a slot",
            }
        )
    hours = (end - start) / HOUR_MS
    if hours >= LONG_WAIT_HOURS:
        out.append(
            {
                "code": "long_wait",
                "text": f"At anchor {hours / 24:.1f} days: often waiting for orders, cargo or a booked berth",
            }
        )
    if not out:
        out.append(
            {
                "code": "unknown",
                "text": "No clear cause in the data: berth booking, pilot, tide or paperwork are not visible",
            }
        )
    return out
