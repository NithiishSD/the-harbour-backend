"""Hourly marine weather per port from Open-Meteo into weather.marine.

    python -m ingest.weather                    # live: every hour, last 24 h + next 24 h
    python -m ingest.weather --date 2023-01-01  # one historical day, then exit

Wind is requested in knots so it matches the AIS speed unit. Waves come from the marine API, which snaps
to the nearest sea grid cell. Live runs also fetch the weather points along the sea route
(the lane nodes of the monitoring: Monitor.weather_points) into weather.grid, which draws the weather at sea.
"""

import argparse
import json
import time
import urllib.error
import urllib.request
from datetime import date
from typing import Any
from urllib.parse import urlencode

from common import topics
from common.geo import Port, load_ports, load_world_ports
from common.kafka import JsonProducer
from common.log import get_logger
from common.monitor import Monitor
from ingest.replay import Sender

MARINE_URL = "https://marine-api.open-meteo.com/v1/marine"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
log = get_logger("ingest.weather")


def _get_json(url: str, params: dict[str, Any], timeout: float) -> dict[str, Any]:
    full = f"{url}?{urlencode(params)}"
    with urllib.request.urlopen(full, timeout=timeout) as resp:  # noqa: S310 - fixed https endpoints
        body: dict[str, Any] = json.loads(resp.read())
    return body


def _hourly(body: dict[str, Any], field: str) -> dict[str, float | None]:
    hourly = body.get("hourly") or {}
    return dict(zip(hourly.get("time", []), hourly.get(field, []), strict=False))


def fetch_port(
    port: Port,
    day: date | None,
    timeout: float = 20,
    urls: tuple[str, str, str] = (MARINE_URL, FORECAST_URL, ARCHIVE_URL),
) -> list[dict[str, Any]]:
    """Hourly records for one port: a historical day, or the last and next 24 h when day is None."""
    marine_url, forecast_url, archive_url = urls
    where = {"latitude": port.lat, "longitude": port.lon, "timezone": "UTC"}
    span = (
        {"start_date": day.isoformat(), "end_date": day.isoformat()}
        if day
        else {"past_days": 1, "forecast_days": 1}
    )
    waves = _hourly(_get_json(marine_url, {**where, **span, "hourly": "wave_height"}, timeout), "wave_height")
    wind_params = {**where, **span, "hourly": "wind_speed_10m", "wind_speed_unit": "kn"}
    wind = _hourly(_get_json(archive_url if day else forecast_url, wind_params, timeout), "wind_speed_10m")
    return [
        {
            "port_id": port.port_id,
            "hour": f"{hour}:00Z",
            "wind_speed_kn": wind.get(hour),
            "wave_height_m": waves.get(hour),
        }
        for hour in sorted(set(waves) | set(wind))
    ]


def grid_record(
    point_id: str, lat: float, lon: float, hour: str, wind: float | None, wave: float | None
) -> dict[str, Any]:
    return {
        "point_id": point_id,
        "lat": lat,
        "lon": lon,
        "hour": hour,
        "wind_speed_kn": wind,
        "wave_height_m": wave,
    }


def fetch_grid(
    points: list[tuple[str, float, float]],
    timeout: float = 30,
    urls: tuple[str, str, str] = (MARINE_URL, FORECAST_URL, ARCHIVE_URL),
) -> list[dict[str, Any]]:
    """Hourly records (last and next 24 h) for the weather points: one request per API, all points in it."""
    where = {
        "latitude": ",".join(str(p[1]) for p in points),
        "longitude": ",".join(str(p[2]) for p in points),
        "timezone": "UTC",
        "past_days": 1,
        "forecast_days": 1,
    }
    marine = _get_json(urls[0], {**where, "hourly": "wave_height"}, timeout)
    wind = _get_json(urls[1], {**where, "hourly": "wind_speed_10m", "wind_speed_unit": "kn"}, timeout)
    # one location answers with an object, several with a list in the order asked
    waves_by_point = marine if isinstance(marine, list) else [marine]
    wind_by_point = wind if isinstance(wind, list) else [wind]
    out = []
    for (point_id, lat, lon), wave_body, wind_body in zip(points, waves_by_point, wind_by_point, strict=True):
        waves, winds = _hourly(wave_body, "wave_height"), _hourly(wind_body, "wind_speed_10m")
        out += [
            grid_record(point_id, lat, lon, f"{hour}:00Z", winds.get(hour), waves.get(hour))
            for hour in sorted(set(waves) | set(winds))
        ]
    return out


def publish_grid(sender: Sender, **kwargs: Any) -> int:
    try:
        world = load_world_ports()
        records = fetch_grid(Monitor.selected(world).weather_points(world), **kwargs)
    except (urllib.error.URLError, TimeoutError, ValueError) as err:
        log.warning("grid weather fetch failed", extra={"data": {"error": str(err)[:200]}})
        return 0
    for rec in records:
        sender.send(topics.WEATHER_GRID, rec["point_id"], rec)
    sender.flush()
    log.info("grid weather published", extra={"data": {"records": len(records)}})
    return len(records)


def publish(ports: list[Port], sender: Sender, day: date | None, **kwargs: Any) -> int:
    sent = 0
    for port in ports:
        try:
            records = fetch_port(port, day, **kwargs)
        except (urllib.error.URLError, TimeoutError, ValueError) as err:
            # One port failing must not stop the others; the next cycle retries.
            log.warning(
                "weather fetch failed", extra={"data": {"port_id": port.port_id, "error": str(err)[:200]}}
            )
            continue
        for rec in records:
            sender.send(topics.WEATHER, port.port_id, rec)
        sent += len(records)
    sender.flush()
    log.info("weather published", extra={"data": {"records": sent, "ports": len(ports)}})
    return sent


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--date", type=date.fromisoformat, help="fetch one historical day and exit")
    parser.add_argument("--interval", type=float, default=3600)
    args = parser.parse_args()
    topics.ensure_topics()
    ports = list(load_ports().values())
    producer = JsonProducer()
    if args.date:
        publish(ports, producer, args.date)
        return
    while True:
        publish(ports, producer, None)
        publish_grid(producer)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
