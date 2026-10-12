"""Which ships belong on the live map. Every ship stays in HBase and the warehouse; this only picks the map.

- A ship joins the map when it is first seen and stays while it travels.
- Arrived (berthed, or departed from a berth): kept MAP_ARRIVED_HOURS, then history only.
- Silent for MAP_SILENT_HOURS (no report, not waiting, not dark): it has left our coverage, history only.
- Waiting at anchor or dark: on the map whatever happened before (the problems to watch), until silent for
  PROBLEM_KEEP_DAYS, when the streaming job forgets the ship too.
- Pinned: always on the map (never removed for silence, arrival, age or the size cap) while the store has it.
- Shown: opened from the history search by the user; always on the map.
- Passing ships (tracked only because they are in the country's waters: neither end of their voyage is
  known to fit): lowest priority, they leave the map first under the size cap, never count as a problem, and
  go after MAP_TRANSIT_HOURS without a report.
- Size cap: when more ships qualify than `limit`, the passing ships leave first, then the ones that arrived
  on the map first; waiting and dark ships only after every other ship.
Times are feed time (see dashboard.reasons.feed_time), so a replayed day behaves like a live one.
"""

from typing import Any

from common.config import settings
from dashboard.reasons import HOUR_MS, ms

PROBLEM_KEEP_DAYS = 7  # streaming.state.FORGET_AFTER_MS
ARRIVED = ("berthed", "departed")


def dark_ships(alerts: list[dict[str, Any]], ships: list[dict[str, Any]]) -> set[str]:
    """Ships with a dark_ship alert and no report since it (as the browser decides)."""
    seen = {s["mmsi"]: ms(s["last_seen"]) or 0 for s in ships}
    return {
        a["mmsi"]
        for a in alerts
        if a.get("type") == "dark_ship"
        and a.get("mmsi") in seen
        and seen[a["mmsi"]] <= (ms(a.get("event_time")) or 0)
    }


def passing(ship: dict[str, Any]) -> bool:
    """Tracked for being in the country's waters and for nothing else (see common.monitor)."""
    return ship.get("roles") == ["in"]


def status(ship: dict[str, Any], now: int, dark: set[str], pinned: bool) -> str | None:
    """Why the ship is on the map ('problem', 'pinned' or 'live'), or None when it is history only."""
    if pinned:
        return "pinned"
    seen = ms(ship["last_seen"]) or 0
    if passing(ship):
        return "live" if now - seen <= settings().map_transit_hours * HOUR_MS else None
    if ship.get("estimated"):  # out of AIS range on a crossing: kept for as long as the estimate holds
        return "live"
    if ship["state"] == "anchored" or ship["mmsi"] in dark:
        return "problem" if now - seen <= PROBLEM_KEEP_DAYS * 24 * HOUR_MS else None
    if ship["state"] in ARRIVED:
        arrived_for = now - (ms(ship["since"]) or seen)
        if arrived_for > settings().map_arrived_hours * HOUR_MS:
            return None
    return "live" if now - seen <= settings().map_silent_hours * HOUR_MS else None


def on_map(
    ships: list[dict[str, Any]],
    now: int | None,
    dark: set[str],
    pinned: set[str],
    shown: set[str],
    limit: int,
) -> list[dict[str, Any]]:
    """The ships to draw, each with `map` set to problem, pinned, live or shown."""
    if now is None:
        return []
    keep: list[dict[str, Any]] = []
    rest: list[dict[str, Any]] = []
    for s in ships:
        why = "shown" if s["mmsi"] in shown else status(s, now, dark, s["mmsi"] in pinned)
        if why is None:
            continue
        s["map"] = why
        (keep if why in ("pinned", "shown") else rest).append(s)
    # FIFO: newest arrivals on the map are kept; pinned and shown ships do not count against the cap.
    rest.sort(key=lambda s: s["first_seen"] or s["since"] or s["last_seen"] or "", reverse=True)
    # Waiting and dark ships go last: they arrived long ago, yet they are the queue the map is for.
    rest.sort(key=lambda s: s["map"] != "problem")
    rest.sort(key=passing)  # passing ships last: the first to go when there are more than `limit`
    drawn = keep + rest[:limit]
    return sorted(
        drawn, key=lambda s: s["last_seen"] or "", reverse=True
    )  # the API's order: newest report first
