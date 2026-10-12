"""Demo data for the open ocean: ships sailing the Singapore -> Los Angeles route and a weather map with a
typhoon and a Pacific storm, all timed from now.

    python -m ingest.seed          # last 6 h of tracks, weather for -6 h .. +24 h, then keep reporting
    python -m ingest.seed --once   # publish that and exit

Ships go into ais.raw as aisstream messages and weather into weather.grid, the same topics and shapes as the
live producers, so the whole pipeline runs. The map's clock is the newest ship report, so do not mix this
with a replay of an old day: the replayed ships would age out of the map.
"""

import argparse
import math
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from common import topics
from common.kafka import JsonProducer
from common.log import get_logger
from common.route import LENGTH_NM, along, lattice
from common.testdata import FIRST_SEED_MMSI, SEED_SHIPS
from ingest.replay import Sender
from ingest.weather import grid_record

log = get_logger("ingest.seed")
SHIPS = SEED_SHIPS
HISTORY_H, STEP_MIN = 6, 30
FIRST_MMSI = FIRST_SEED_MMSI  # Marshall Islands flag block
NAMES = (
    "PACIFIC ARROW",
    "ORIENT STAR",
    "BLUE HORIZON",
    "GOLDEN GATE",
    "SUNDA SPIRIT",
    "LUZON EXPRESS",
    "NORTH WIND",
    "SILK ROUTE",
)
# (centre lat, centre lon when seeded, drift lat/lon per hour, peak wind kn, radius deg): a typhoon east of
# Luzon curving north-east, and a winter storm in the Gulf of Alaska side of the route.
STORMS = (
    (20.5, 128.0, 0.30, 0.45, 78.0, 4.5),
    (45.5, 192.0, 0.0, 0.35, 34.0, 5.0),
)
BASE_WIND_KN = 11.0


def ais_time(at: datetime) -> str:
    return at.strftime("%Y-%m-%d %H:%M:%S.%f") + "000 +0000 UTC"


def ship_at(i: int, at: datetime, start: datetime) -> tuple[float, float, int, float]:
    """(lat, lon, heading, speed kn) of seed ship i (even: bound for Los Angeles, odd: Singapore)."""
    speed = 12.0 + (i * 7) % 8
    east = i % 2 == 0
    spot = ((i * 0.618) % 1) * 0.9 + 0.05  # spread the fleet over the route
    nm = spot * LENGTH_NM + (1 if east else -1) * speed * (at - start).total_seconds() / 3600
    lat, lon, heading = along(nm)
    return lat, lon, heading if east else (heading + 180) % 360, speed


def ship_messages(i: int, at: datetime, start: datetime) -> list[dict[str, Any]]:
    mmsi = FIRST_MMSI + i
    lat, lon, heading, speed = ship_at(i, at, start)
    name = f"{NAMES[i % len(NAMES)]} {i // len(NAMES) + 1}"
    meta = {"MMSI": mmsi, "ShipName": name, "latitude": lat, "longitude": lon, "time_utc": ais_time(at)}
    position = {
        "UserID": mmsi,
        "Latitude": lat,
        "Longitude": lon,
        "Sog": speed,
        "Cog": heading,
        "TrueHeading": heading,
        "NavigationalStatus": 0,
        "Valid": True,
    }
    return [{"MessageType": "PositionReport", "MetaData": meta, "Message": {"PositionReport": position}}]


def static_message(i: int, at: datetime) -> dict[str, Any]:
    mmsi = FIRST_MMSI + i
    name = f"{NAMES[i % len(NAMES)]} {i // len(NAMES) + 1}"
    body = {
        "UserID": mmsi,
        "Name": name,
        "Type": 80 if i % 3 == 0 else 70,
        "Dimension": {"A": 150 + 10 * (i % 5), "B": 40, "C": 16, "D": 16},
        "Destination": "LOS ANGELES" if i % 2 == 0 else "SINGAPORE",
        "Valid": True,
    }
    meta = {"MMSI": mmsi, "ShipName": name, "time_utc": ais_time(at)}
    return {"MessageType": "ShipStaticData", "MetaData": meta, "Message": {"ShipStaticData": body}}


def wind_at(lat: float, lon: float, hours: float) -> float:
    """Wind in knots: a light background plus each storm as a bump that moves with the hours since now."""
    wind = BASE_WIND_KN + 3 * math.sin(lat / 7 + lon / 23)
    for s_lat, s_lon, d_lat, d_lon, peak, radius in STORMS:
        dlat = lat - (s_lat + d_lat * hours)
        dlon = (lon - (s_lon + d_lon * hours) + 180) % 360 - 180
        dist = math.hypot(dlat, dlon * math.cos(math.radians(lat)))
        wind += peak * math.exp(-0.5 * (dist / radius) ** 2)
    return round(wind, 1)


def weather_records(now: datetime, anchor: datetime) -> list[dict[str, Any]]:
    """Every weather point from 6 h ago to 24 h ahead; the storms are where they were seeded at `anchor`."""
    top = now.replace(minute=0, second=0, microsecond=0)
    out = []
    for h in range(-6, 25):
        at = top + timedelta(hours=h)
        for point_id, lat, lon in lattice():
            wind = wind_at(lat, lon, (at - anchor).total_seconds() / 3600)
            wave = round(0.0045 * wind**2, 1)  # fully grown sea: about 4.5 m at 30 kn, 9 m at 45 kn
            out.append(grid_record(point_id, lat, lon, at.strftime("%Y-%m-%dT%H:00Z"), wind, wave))
    return out


def publish_ships(sender: Sender, at: datetime, start: datetime, with_static: bool = False) -> None:
    for i in range(SHIPS):
        messages = ship_messages(i, at, start)
        if with_static:
            messages.append(static_message(i, at))
        for msg in messages:
            envelope = {"source": "aisstream", "received_at": at.isoformat(), "payload": msg}
            sender.send(topics.RAW, str(FIRST_MMSI + i), envelope)


def seed(sender: Sender, now: datetime) -> None:
    start = now - timedelta(hours=HISTORY_H)
    publish_ships(sender, start, start, with_static=True)
    for step in range(1, HISTORY_H * 60 // STEP_MIN + 1):
        publish_ships(sender, start + timedelta(minutes=STEP_MIN * step), start)
    for rec in weather_records(now, now):
        sender.send(topics.WEATHER_GRID, rec["point_id"], rec)
    sender.flush()
    log.info("seeded", extra={"data": {"ships": SHIPS, "weather_points": len(lattice())}})


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--once", action="store_true", help="publish the history and the weather, then exit")
    parser.add_argument("--interval", type=float, default=60, help="seconds between position reports")
    args = parser.parse_args()
    topics.ensure_topics()
    producer = JsonProducer()
    now = datetime.now(UTC)
    seed(producer, now)
    if args.once:
        return
    start = now - timedelta(hours=HISTORY_H)
    anchor, last_weather = now, now
    while True:
        time.sleep(args.interval)
        at = datetime.now(UTC)
        publish_ships(producer, at, start)
        if at - last_weather > timedelta(hours=1):  # keep the forecast window ahead of the clock
            for rec in weather_records(at, anchor):
                producer.send(topics.WEATHER_GRID, rec["point_id"], rec)
            last_weather = at
        producer.flush(5)


if __name__ == "__main__":
    main()
