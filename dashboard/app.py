"""Dashboard backend.

    uvicorn dashboard.app:app --host 127.0.0.1 --port 8000

Every error leaves as {"error": {"code": "<STABLE_CODE>", "message": "..."}}; stack traces stay in the log.
"""

import json
import os
import threading
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any, Literal

import duckdb
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient
from fastapi import FastAPI, Query, Request
from fastapi import Path as PathParam
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from starlette.exceptions import HTTPException as StarletteHTTPException

from common import coverage, lanes, scopes, selection, topics
from common.config import settings
from common.flags import flag_country
from common.geo import Port, ZoneIndex, load_ports, load_world_ports, load_zones
from common.log import get_logger
from common.monitor import Monitor, coverage_cells, recommend_partners, suggest_partners, timezone_of
from common.testdata import RealData, day_of
from dashboard import crossing, destination, live, mapview, reasons, waterway
from dashboard.alerts import AlertFeed, add_alert, alert_events, consume
from dashboard.history import HistoryStore
from dashboard.weather import GridFeed, WeatherFeed
from storage.hbase.client import HBaseClient, HBaseError
from storage.warehouse import reports

log = get_logger("dashboard")

# In production Nginx serves frontend/src/ directly (see infra/docker-compose.yml).
# For local dev, FastAPI falls back to mounting it itself.
STATIC = Path(os.environ.get("STATIC_DIR", str(Path(__file__).parents[2] / "frontend" / "src")))
MmsiPath = Annotated[str, PathParam(pattern=r"^\d{9}$")]
MmsiList = Annotated[str, Query(pattern=r"^(\d{9}(,\d{9}){0,199})?$")]  # up to 200 comma-separated MMSIs
MAP_LIMIT = 2000  # default size cap of the map
DAY_MS = 86_400_000
GOING_TTL_S = 120  # how long the countries the monitored ships sail to are kept
State = Literal["underway", "anchored", "berthed", "departed"]
Checks = dict[str, Callable[[], str]]


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str) -> None:
        self.status, self.code, self.message = status, code, message


def _ms(text: str | None) -> int | None:
    try:
        return int(datetime.fromisoformat(text).timestamp() * 1000) if text else None
    except ValueError:
        return None


def error_body(code: str, message: str) -> dict[str, Any]:
    return {"error": {"code": code, "message": message}}


def kafka_check() -> Callable[[], str]:
    admin: list[AdminClient] = []

    def check() -> str:
        if not admin:
            admin.append(AdminClient({"bootstrap.servers": settings().kafka_bootstrap}))
        try:
            admin[0].list_topics(timeout=3)
        except KafkaException:
            return "unavailable"
        return "ok"

    return check


def hbase_check(client: HBaseClient) -> Callable[[], str]:
    def check() -> str:
        try:
            client.version()
        except HBaseError:
            return "unavailable"
        return "ok"

    return check


def warehouse_check(path: Path) -> Callable[[], str]:
    def check() -> str:
        if not path.is_file():
            return "empty"  # the nightly job has not run yet: nothing is broken
        try:
            duckdb.connect(str(path), read_only=True).close()
        except duckdb.Error:
            return "unavailable"
        return "ok"

    return check


class NotForStreams:
    """GZipMiddleware for everything except the alert stream.

    The gzip middleware holds a response's headers back until its first chunk. The alert stream may have
    none for 15 seconds (its keepalive); the browser's EventSource stays "connecting" till it has them."""

    def __init__(self, app: Any, minimum_size: int = 1024) -> None:
        self.app = app
        self.gzipped = GZipMiddleware(app, minimum_size=minimum_size)

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        stream = scope["type"] == "http" and scope["path"].startswith("/api/alerts/stream")
        await (self.app if stream else self.gzipped)(scope, receive, send)


def create_app(
    client: HBaseClient | None = None,
    known_ports: dict[str, Port] | None = None,
    checks: Checks | None = None,
    alerts: AlertFeed | None = None,
    weather: WeatherFeed | None = None,
    grid: GridFeed | None = None,
    history: HistoryStore | None = None,
    world: dict[str, Port] | None = None,
    scoped: bool | None = None,
) -> FastAPI:
    """`scoped` (default: when the real port lists are used) holds the dashboard to the country in
    MONITOR_COUNTRY: only its ships and ports, and only the data received while the feed was set to it."""
    hbase = client or HBaseClient(timeout=5)
    port_list = known_ports if known_ports is not None else load_ports()
    # every port a ship may name: the watched ones first, then geo/world_ports.csv (tests pass their own list)
    world_ports = world or ({**load_world_ports(), **port_list} if known_ports is None else port_list)
    zone_index = ZoneIndex(load_zones())
    health_checks = checks or {
        "kafka": kafka_check(),
        "hbase": hbase_check(hbase),
        "warehouse": warehouse_check(settings().warehouse_path),
    }
    feed = alerts or AlertFeed()
    port_weather = weather or WeatherFeed()
    sea_weather = grid or GridFeed()
    locked = known_ports is None if scoped is None else scoped
    most = settings().monitor_max_partners
    stores: list[HistoryStore] = [history] if history else []
    home: list[Monitor] = []  # the monitoring of the environment, worked out once
    going: dict[Any, tuple[float, tuple[str, ...]]] = {}  # monitoring -> (when, countries its ships sail to)
    truth: list[RealData] = []

    def is_test_alert(alert: dict[str, Any]) -> bool:
        """Alerts of test data are not shown (SHOW_TEST_DATA=1 shows them: the end-to-end tests send some)."""
        if locked and out_of_scope(alert):
            return True
        if os.environ.get("SHOW_TEST_DATA"):
            return False
        return real().is_test_alert(alert)

    def out_of_scope(alert: dict[str, Any]) -> bool:
        """An alert of another country's port, or of a time the feed was not set to this country."""
        port = world_ports.get(str(alert.get("port_id")))
        if port is not None and port.country != home_monitor().country:
            return True
        return not in_spans(spans(), _ms(str(alert.get("event_time") or "")))

    def real() -> RealData:
        if not truth:
            truth.append(RealData.load(settings().data_dir, settings().live_since))
        return truth[0]

    def home_monitor() -> Monitor:
        if not home:
            home.append(Monitor.from_settings(world_ports))
        return home[0]

    def spans() -> list[scopes.Span] | None:
        """When the feed was set to the monitored country (None: no ledger, nothing to tell apart)."""
        return scopes.eras(scopes.read(settings().data_dir), home_monitor().country)

    def in_spans(found: list[scopes.Span] | None, ts_ms: int | None) -> bool:
        """Was this time one at which data of the monitored country was being received? NOAA's days are US
        waters: theirs only when the country is the US."""
        if found is None:
            return True
        if ts_ms is None:
            return False
        noaa = home_monitor().country == "US" and day_of(ts_ms) in real().noaa_days
        return noaa or scopes.in_eras(found, ts_ms)

    def report_scope() -> tuple[list[str] | None, list[str] | None]:
        """(ports, days) a report may use: the monitored country's ports, and the days received while the feed
        was set to it (NOAA's days for the US). The warehouse holds every country's rows."""
        if not locked:
            return None, None
        country = home_monitor().country
        ids = [p.port_id for p in world_ports.values() if p.country == country]
        found = spans()
        if found is None:
            return ids, None
        days = set(real().noaa_days) if country == "US" else set()
        today = datetime.now(UTC).timestamp() * 1000
        for start, end in found:
            t = start
            while t < min(end, today + DAY_MS):
                days.add(day_of(int(t)))
                t += DAY_MS
        return ids, sorted(days)

    def history_scope() -> str | None:
        found = spans()
        return None if found is None else scopes.predicate(found, home_monitor().country == "US")

    def past() -> HistoryStore:
        """The offline history, opened on first use (the live views never need it)."""
        if not stores:
            stores.append(
                HistoryStore(
                    settings().data_dir / "history",
                    zone_index,
                    world_ports,
                    scope=history_scope if locked else None,
                )
            )
        return stores[0]

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        stop = threading.Event()
        # A feed passed in by the caller is filled by the caller; otherwise read its topic.
        readers = [
            (topics.ALERTS, lambda raw: add_alert(feed, raw, is_test_alert)) if alerts is None else None,
            (topics.WEATHER, port_weather.add_raw) if weather is None else None,
            (topics.WEATHER_GRID, sea_weather.add_raw) if grid is None else None,
        ]
        for reader in filter(None, readers):
            threading.Thread(target=consume, args=(*reader, stop), daemon=True).start()
        yield
        stop.set()

    def following() -> dict[str, Any] | None:
        """What ingest.follow last wrote: how many ships it follows worldwide and which."""
        try:
            found = json.loads((settings().data_dir / "follow_status.json").read_text())
        except (OSError, ValueError):
            return None
        return found if isinstance(found, dict) else None

    scan_lock = threading.Lock()
    scanned: list[tuple[float, list[dict[str, Any]], int | None, dict[str, int]]] = []

    def ship_rows() -> tuple[list[dict[str, Any]], int | None, dict[str, int]]:
        """Every ship with its wait reasons, the feed time and the ships waiting now per port.

        The scan of HBase and the work on every ship is shared: one answer is kept for SHIPS_CACHE_S and
        handed (as copies) to every request in that time, so the cost does not grow with the viewers."""
        ttl = settings().ships_cache_s if locked else 0.0
        if ttl <= 0:
            return scan_ships()
        with scan_lock:  # concurrent requests wait for one scan instead of each doing their own
            if not scanned or time.monotonic() - scanned[0][0] > ttl:
                rows, now, waiting = scan_ships()
                scanned[:] = [(time.monotonic(), rows, now, waiting)]
            _, rows, now, waiting = scanned[0]
        return [dict(s) for s in rows], now, waiting

    def scan_ships() -> tuple[list[dict[str, Any]], int | None, dict[str, int]]:
        rows = live.ships(hbase)
        followed = set((following() or {}).get("mmsis", []))
        for s in rows:
            s["followed"] = s["mmsi"] in followed  # also heard from stations anywhere in the world
        if locked:  # what the store holds from another country's time is not this monitoring's
            found = spans()
            rows = [s for s in rows if in_spans(found, _ms(s["last_seen"]))]
        now = reasons.feed_time(rows)
        crossing.apply(rows, now, world_ports)  # ships out of AIS range get an estimated position
        for s in rows:
            s["flag"] = flag_country(s["mmsi"])
        waiting = reasons.waiting_counts(rows, now)
        for s in rows:
            name = port_list[s["port"]].name if s["port"] in port_list else str(s["port"])
            s["reasons"] = reasons.wait_reasons(s, waiting, port_weather, name)
        return rows, now, waiting

    app = FastAPI(title="HarbourWatch", docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_middleware(NotForStreams, minimum_size=1024)  # the ship list is repeated keys: compresses well

    def port_or_404(port_id: str) -> str:
        if port_id not in port_list:
            raise ApiError(404, "UNKNOWN_PORT", f"unknown port {port_id[:10]!r}")
        return port_id

    @app.exception_handler(ApiError)
    async def api_error(_: Request, err: ApiError) -> JSONResponse:
        return JSONResponse(error_body(err.code, err.message), err.status)

    @app.exception_handler(HBaseError)
    async def hbase_down(_: Request, err: HBaseError) -> JSONResponse:
        log.error("hbase unavailable", extra={"data": {"error": str(err)}})
        return JSONResponse(error_body("HBASE_UNAVAILABLE", "live data store is unavailable"), 503)

    @app.exception_handler(duckdb.Error)
    async def warehouse_down(_: Request, err: duckdb.Error) -> JSONResponse:
        log.error("warehouse unavailable", extra={"data": {"error": str(err)[:200]}})
        return JSONResponse(error_body("WAREHOUSE_UNAVAILABLE", "reports store is unavailable"), 503)

    @app.exception_handler(RequestValidationError)
    async def invalid(_: Request, err: RequestValidationError) -> JSONResponse:
        first = err.errors()[0]
        name = str(first["loc"][-1])
        code = "INVALID_MMSI" if name == "mmsi" else "INVALID_PARAM"
        return JSONResponse(error_body(code, f"{name}: {first['msg']}"), 422)

    @app.exception_handler(StarletteHTTPException)
    async def http_error(_: Request, err: StarletteHTTPException) -> JSONResponse:
        code = {404: "NOT_FOUND", 405: "METHOD_NOT_ALLOWED"}.get(err.status_code, "HTTP_ERROR")
        return JSONResponse(error_body(code, str(err.detail)), err.status_code)

    @app.exception_handler(Exception)
    async def unexpected(_: Request, err: Exception) -> JSONResponse:
        log.error("unhandled error", exc_info=err)
        return JSONResponse(error_body("INTERNAL_ERROR", "internal error"), 500)

    @app.get("/health")
    def health() -> JSONResponse:
        results = {name: check() for name, check in health_checks.items()}
        ok = "unavailable" not in results.values()
        return JSONResponse({"status": "ok" if ok else "degraded", "checks": results}, 200 if ok else 503)

    def at_ms(text: str) -> int:
        """ISO time (or epoch ms) from the query string; the offline views take their time from it."""
        try:
            value = int(text) if text.isdigit() else int(datetime.fromisoformat(text).timestamp() * 1000)
        except ValueError as err:
            raise ApiError(422, "INVALID_PARAM", "at: not an ISO time or epoch milliseconds") from err
        if not 946_684_800_000 <= value <= 4_102_444_800_000:  # years 2000..2100
            raise ApiError(422, "INVALID_PARAM", "at: out of range")
        return value

    AtQuery = Annotated[str | None, Query(max_length=40)]

    def monitor_of(country: str | None, ports: str, partners: str | None) -> Monitor:
        """The monitoring the viewer chose (common.selection) over the environment's (common.monitor),
        narrowed to the ports and source countries asked for. The country is fixed by MONITOR_COUNTRY:
        another one is a 422, as are ports that are not the country's."""
        env = home_monitor()
        if locked and country and country.upper() != env.country:
            fixed = f"country: fixed by MONITOR_COUNTRY ({env.country}); redeploy to change"
            raise ApiError(422, "INVALID_PARAM", fixed)
        chosen = env.country if locked else (country or env.country).upper()
        base = Monitor.selected(world_ports, env) if chosen == env.country else env
        wanted = tuple(p for p in ports.upper().split(",") if p)
        if not wanted and chosen == env.country:
            wanted = base.ports
        try:
            Monitor.validate(world_ports, chosen, wanted)
        except ValueError as err:
            raise ApiError(422, "INVALID_PARAM", f"country: {err}") from err
        chosen_partners = base.partners
        if partners is not None:  # the source countries asked for (at most MONITOR_MAX_PARTNERS)
            codes = {c for c in partners.upper().split(",") if c}
            known = {p.country for p in world_ports.values()}
            if not codes <= known or len(codes) > most:
                raise ApiError(
                    422, "INVALID_PARAM", f"partners: up to {most} countries that have a port in geo/"
                )
            chosen_partners = frozenset(codes) - {chosen}
        return Monitor.build(world_ports, chosen, wanted, tuple(chosen_partners), env.region_km)

    voyages_seen: list[tuple[float, Any]] = []  # (when, ships) for the suggestions

    def suggestions(watch: Monitor) -> tuple[tuple[str, ...], dict[str, int]]:
        """The source countries to follow, and how many ships were seen on voyages with each."""
        if not voyages_seen or time.monotonic() - voyages_seen[0][0] > GOING_TTL_S:
            voyages_seen[:] = [(time.monotonic(), live.ships(hbase))]
        ships = voyages_seen[0][1]
        return suggest_partners(world_ports, watch, ships), dict(watch.voyages(ships, world_ports))

    @app.get("/api/monitor")
    def monitor_config(
        country: Annotated[str | None, Query(max_length=2)] = None,
        ports: Annotated[str, Query(max_length=200)] = "",
    ) -> dict[str, Any]:
        """What is watched: the country of the environment (fixed until the next deployment), the ports and
        source countries chosen (else the environment's), and for each candidate source country how many ships
        were seen sailing between it and the monitored ports and how far coastal AIS follows the voyage
        (common.coverage), with the countries suggested for following."""
        env = home_monitor()
        watch = monitor_of(country, ports, None)
        chosen = watch.country
        if chosen == env.country:
            partners = tuple(sorted(watch.partners))
        else:  # library mode: another country, its partners are the ones it is best followed to
            partners = recommend_partners(world_ports, chosen, watch.ports)
        countries: dict[str, list[dict[str, Any]]] = {}
        for p in world_ports.values():
            if not locked or p.country == chosen:
                countries.setdefault(p.country, []).append(
                    {"port_id": p.port_id, "name": p.name, "watched": p.port_id in port_list}
                )
        entries = scopes.read(settings().data_dir)
        cells = coverage_cells()
        boxes = [list(b) for b in watch.ais_boxes(world_ports)]
        return {
            "country": chosen,
            "timezone": timezone_of(chosen),
            "locked": locked,
            "ports": list(watch.ports),
            # which country the running feed was started for: a different one means the ingest was not
            # redeployed with the environment, and what it receives is not this country's
            "feed": {
                "country": entries[-1]["country"] if entries else None,
                "matches": not entries or entries[-1]["country"] == chosen,
                "follows_selection": bool(entries) and entries[-1].get("boxes") == boxes,
            },
            "follow": None
            if (status := following()) is None
            else {k: status.get(k) for k in ("ships", "max", "at")},
            "partners": list(partners),
            "defaults": {"ports": list(env.ports), "partners": sorted(env.partners)},
            "max_partners": most,
            "waters": watch.waters(world_ports),
            "region_km": env.region_km,
            "countries": [{"code": c, "ports": v} for c, v in sorted(countries.items())],
            "coverage": [
                asdict(r) for r in coverage.country_coverage(world_ports, chosen, cells, watch.ports)
            ],
            "coverage_basis": "measured"
            if cells
            else "estimated from the lane graph (run the coverage survey)",
        }

    @app.get("/api/monitor/suggestions")
    def monitor_suggestions(
        country: Annotated[str | None, Query(max_length=2)] = None,
        ports: Annotated[str, Query(max_length=200)] = "",
    ) -> dict[str, Any]:
        """The source countries worth following and how many ships were seen on voyages with each: from
        the ships in the store, so asked for apart from /api/monitor (which must answer at once)."""
        suggested, seen = suggestions(monitor_of(country, ports, None))
        return {"suggested": list(suggested), "ships": seen}

    @app.get("/api/coverage")
    def coverage_picture(
        country: Annotated[str | None, Query(max_length=2)] = None,
        ports: Annotated[str, Query(max_length=200)] = "",
        partners: Annotated[str | None, Query(max_length=60)] = None,
    ) -> dict[str, Any]:
        """What the monitoring can see, to draw: the waters whose ships are shown, the area the live feed
        listens to, where AIS was heard at all (the survey) and the stretches of the voyages to the source
        countries that no station hears."""
        watch = monitor_of(country, ports, partners)
        cells = coverage_cells()
        heard = []
        if cells is not None:
            for (r, c), ships in sorted(cells.counts.items()):
                lat, lon = r * cells.size + cells.size / 2, c * cells.size - 180 + cells.size / 2
                heard.append({"lat": lat, "lon": lon, "size": cells.size, "ships": ships})
        there = [coverage.principal_port(world_ports, c) for c in sorted(watch.partners)]
        found = coverage.country_coverage(world_ports, watch.country, cells, watch.ports)
        rows = {r.country: r for r in found}
        blind = coverage.blind_stretches(world_ports, watch.country, cells, watch.ports, set(watch.partners))
        # the ships the map shows now: the roles, then the keep times (a passing ship silent is gone)
        everyone, now, _ = ship_rows()
        for s in everyone:
            s["roles"] = watch.roles(s, world_ports)
            s["kind"] = watch.kind(s, world_ports, s["roles"])
        tracked = [s for s in everyone if s["roles"]]
        dark = mapview.dark_ships(feed.latest(500), tracked)
        shown: dict[str, int] = {"in": 0, "from": 0, "to": 0}
        for s in mapview.on_map(tracked, now, dark, set(), set(), 20_000):
            for role in s["roles"]:
                shown[role] += 1
        return {
            "country": watch.country,
            "waters": watch.waters(world_ports),
            "tracked": shown,
            "reach": [] if cells is None else coverage.reach_outline(cells),
            "listening": [
                {"min_lat": b[0], "min_lon": b[1], "max_lat": b[2], "max_lon": b[3]}
                for b in watch.ais_boxes(world_ports)
            ],
            "heard": heard,
            "blind": blind,
            "monitored": [
                {"port_id": p, "name": w.name, "lat": w.lat, "lon": w.lon}
                for p, w in ((p, world_ports[p]) for p in watch.ports)
            ],
            "sources": [
                {
                    "country": p.country,
                    "port_id": p.port_id,
                    "name": p.name,
                    "lat": p.lat,
                    "lon": p.lon,
                    "covered": rows[p.country].covered if p.country in rows else None,
                    "gap_hours": rows[p.country].gap_hours if p.country in rows else None,
                }
                for p in there
                if p is not None
            ],
            "survey": None
            if cells is None
            else {"minutes": cells.minutes, "cell_deg": cells.size, "cells": len(cells.counts)},
        }

    class Selection(BaseModel):
        ports: list[str] = Field(default_factory=list, max_length=60)
        partners: list[str] = Field(default_factory=list, max_length=most)

    @app.put("/api/monitor/selection")
    def choose(body: Selection) -> dict[str, Any]:
        """Save the monitored ports (none: all of the country's) and the source countries. The live feed
        reads them and listens to those coasts within a minute; the country itself cannot be set here."""
        env = home_monitor()
        ids = [p.upper() for p in body.ports] or [
            p.port_id for p in world_ports.values() if p.country == env.country
        ]
        watch = monitor_of(None, ",".join(ids), ",".join(c.upper() for c in body.partners))
        selection.save(settings().data_dir, watch.country, list(watch.ports), sorted(watch.partners))
        return monitor_config(None, "")

    @app.get("/api/ports")
    def ports(
        at: AtQuery = None, ids: Annotated[str, Query(max_length=200, alias="ports")] = ""
    ) -> list[dict[str, Any]]:
        base = home_monitor()
        pool = {k: v for k, v in world_ports.items() if v.country == base.country} if locked else world_ports
        watched = {k: v for k, v in port_list.items() if k in pool} or (pool if locked else port_list)
        shown = {p: pool[p] for p in ids.upper().split(",") if p in pool} or watched
        if at is not None:  # offline: ships waiting and weather at that time, from the history
            t = at_ms(at)
            waiting_then = reasons.waiting_counts(past().snapshot(t), t)
            return [
                {
                    "port_id": p.port_id, "name": p.name, "lat": p.lat, "lon": p.lon,
                    "waiting_now": waiting_then.get(p.port_id, 0), "window_count": None, "window_end": None,
                    "congested": waiting_then.get(p.port_id, 0) >= reasons.CONGESTED_SHIPS,
                    "weather": port_weather.at(p.port_id, t),
                }
                for p in shown.values()
            ]  # fmt: skip
        _, now, waiting = ship_rows()
        return [
            p
            | {
                "congested": p["waiting_now"] >= reasons.CONGESTED_SHIPS,
                "weather": port_weather.at(p["port_id"], now) if now is not None else None,
            }
            for p in live.ports(hbase, shown, waiting)
        ]

    @app.get("/api/weather/grid")
    def weather_grid(at: AtQuery = None) -> list[dict[str, Any]]:
        """Open-ocean weather points for the feed's current hour, or the hour of `at` (offline)."""
        if at is not None:
            return sea_weather.at(at_ms(at))
        _, now, _ = ship_rows()
        return sea_weather.at(now) if now is not None else []

    @app.get("/api/history/dates")
    def history_dates() -> list[dict[str, Any]]:
        """The days with data of the monitored country (the history holds other countries' too: not these)."""
        return past().dates()

    @app.get("/api/history/ships")
    def history_ships(
        at: Annotated[str, Query(max_length=40)],
        window_min: Annotated[int, Query(ge=1, le=1440)] = 60,
        limit: Annotated[int, Query(ge=1, le=MAP_LIMIT)] = MAP_LIMIT,
        mmsi: Annotated[str | None, Query(pattern=r"^\d{9}$")] = None,
        tracked: bool = False,
        country: Annotated[str | None, Query(max_length=2)] = None,
        ports: Annotated[str, Query(max_length=200)] = "",
        partners: Annotated[str | None, Query(max_length=60)] = None,
    ) -> list[dict[str, Any]]:
        """Where the ships were at a past time: the newest report of each within `window_min` before it, or
        the newest report of the one ship `mmsi` whatever its age. `tracked` as in /api/ships (the history
        has no first position, so a ship's source is unknown and only 'to' and 'in' apply)."""
        when = at_ms(at)
        everything = tracked or locked
        if locked and not in_spans(spans(), when):
            raise ApiError(
                409,
                "NOT_MONITORED",
                f"the live feed was not set to {home_monitor().country} at this time: nothing of it was "
                "received, so the replay would show an empty sea",
            )
        found = past().snapshot(when, window_min * 60_000, limit, mmsi)
        for s in found:
            s["flag"] = flag_country(s["mmsi"])
        if everything and not mmsi:
            watch = monitor_of(country, ports, partners)
            for s in found:
                s["roles"] = watch.roles(s, world_ports)
                s["kind"] = watch.kind(s, world_ports, s["roles"])
            found = [s for s in found if s["roles"]]
        return found

    @app.get("/api/route")
    def plan_route(
        lat: Annotated[float, Query(ge=-90, le=90)],
        lon: Annotated[float, Query(ge=-180, le=180)],
        to: Annotated[str, Query(max_length=10)],
    ) -> dict[str, Any]:
        """The path a ship at (lat, lon) would take to port `to` (a port of ports.csv or world_ports.csv):
        along the sea lanes, then the approach lanes of the port."""
        if to not in world_ports:
            raise ApiError(404, "UNKNOWN_PORT", f"unknown port {to[:10]!r}")
        port = world_ports[to]
        path = live.path_to(lat, lon, port)
        return {"path": path, "distance_nm": round(waterway.length_km(path) / live.KM_PER_NM, 1)}

    @app.get("/api/history/ships/{mmsi}/journey")
    def history_journey(mmsi: MmsiPath, until: AtQuery = None, full: bool = False) -> dict[str, Any]:
        found = past().journey(mmsi, at_ms(until) if until else None, stop_at_arrival=not full)
        if found is None:
            raise ApiError(404, "UNKNOWN_SHIP", "no history for this ship")
        return found

    @app.get("/api/sea-lanes")
    def sea_lanes(
        country: Annotated[str | None, Query(max_length=2)] = None,
        ports: Annotated[str, Query(max_length=200)] = "",
        partners: Annotated[str | None, Query(max_length=60)] = None,
    ) -> dict[str, Any]:
        """The sea lanes and ports of this monitoring's voyages, and no others: the monitored ports, the
        partner countries' ports and the countries the country's own ships were seen to sail to."""
        watch = monitor_of(country, ports, partners)
        key = (watch.country, watch.ports, watch.partners)
        cached = going.get(key)
        if cached is None or time.monotonic() - cached[0] > GOING_TTL_S:
            rows, _, _ = ship_rows()
            seen: set[str] = set()
            for s in rows:  # where the country's own and departed ships say they are going
                if watch.roles(s, world_ports):
                    code = destination.match(s.get("destination"), world_ports)
                    if code and world_ports[code].country not in watch.partners | {watch.country}:
                        seen.add(world_ports[code].country)
            cached = going[key] = (time.monotonic(), tuple(sorted(seen)))
        extra = cached[1]
        shown = {p.port_id: p for p in watch.far_ports(world_ports, extra)}
        shown |= {p: world_ports[p] for p in watch.ports}
        return {
            "lanes": lanes.lanes(watch.edges(world_ports, extra)),
            "also_sailing_to": list(extra),
            "ports": [
                {
                    "port_id": p.port_id,
                    "name": p.name,
                    "lat": p.lat,
                    "lon": p.lon,
                    "watched": p.port_id in watch.ports,
                }
                for p in shown.values()
            ],
        }

    @app.get("/api/ports/{port_id}/calls")
    def port_calls(port_id: str, limit: Annotated[int, Query(ge=1, le=500)] = 50) -> list[dict[str, Any]]:
        return live.port_calls(hbase, port_or_404(port_id), limit)

    @app.get("/api/ports/{port_id}/queue")
    def port_queue(port_id: str, limit: Annotated[int, Query(ge=1, le=500)] = 48) -> list[dict[str, Any]]:
        return live.port_queue(hbase, port_or_404(port_id), limit)

    @app.get("/api/alerts")
    def alert_list(limit: Annotated[int, Query(ge=1, le=500)] = 100) -> list[dict[str, Any]]:
        return feed.latest(limit)

    @app.get("/api/alerts/stream")
    def alert_stream() -> StreamingResponse:
        return StreamingResponse(
            alert_events(feed),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.get("/api/ships")
    def ships(
        port_id: str | None = None,
        state: State | None = None,
        pinned: MmsiList = "",
        show: MmsiList = "",
        limit: Annotated[int, Query(ge=1, le=20_000)] = MAP_LIMIT,
        tracked: bool = False,
        country: Annotated[str | None, Query(max_length=2)] = None,
        ports: Annotated[str, Query(max_length=200)] = "",
        partners: Annotated[str | None, Query(max_length=60)] = None,
    ) -> list[dict[str, Any]]:
        """The ships on the live map (see dashboard.mapview): pinned and shown ones are kept regardless.

        Only the ships the monitoring cares about are listed (common.monitor): those that left a monitored
        port, are bound for one, or are in the country's waters; each carries its `roles`. `tracked` asks for
        the same where the dashboard is not held to its country (tests, library use)."""
        if port_id is not None:
            port_or_404(port_id)
        rows, now, _ = ship_rows()
        if tracked or locked:  # the dashboard shows only the monitoring's ships: never "everyone"
            watch = monitor_of(country, ports, partners)
            # a pinned or searched ship stays on the map whatever its roles are now
            keep = set(filter(None, pinned.split(","))) | set(filter(None, show.split(",")))
            for s in rows:
                s["roles"] = watch.roles(s, world_ports)
                s["kind"] = watch.kind(s, world_ports, s["roles"])
            rows = [s for s in rows if s["roles"] or s["mmsi"] in keep]
        dark = mapview.dark_ships(feed.latest(500), rows)
        drawn = mapview.on_map(
            rows, now, dark, set(filter(None, pinned.split(","))), set(filter(None, show.split(","))), limit
        )
        return [
            s
            for s in drawn
            if (port_id is None or s["port"] == port_id) and (state is None or s["state"] == state)
        ]

    @app.get("/api/ships/search")
    def ship_search(q: Annotated[str, Query(min_length=2, max_length=40)]) -> list[dict[str, Any]]:
        """Current and past ships by name or MMSI prefix; `on_map` says whether the live map has it."""
        text = q.strip().lower()
        rows, now, _ = ship_rows()
        dark = mapview.dark_ships(feed.latest(500), rows)
        if locked:
            watch = monitor_of(None, "", None)
            rows = [s for s in rows if watch.roles(s, world_ports)]
        hits = [s for s in rows if s["mmsi"].startswith(text) or text in (s["name"] or "").lower()][:25]
        return [
            {
                **{k: s[k] for k in ("mmsi", "name", "ship_type", "state", "port", "last_seen")},
                "on_map": now is not None and mapview.status(s, now, dark, False) is not None,
            }
            for s in hits
        ]

    @app.get("/api/ships/{mmsi}/track")
    def ship_track(mmsi: MmsiPath, limit: Annotated[int, Query(ge=1, le=1000)] = 100) -> list[dict[str, Any]]:
        rows = live.track(hbase, mmsi, limit)
        if not rows:
            raise ApiError(404, "NOT_FOUND", "no track for this ship")
        return rows

    @app.get("/api/ships/{mmsi}/states")
    def ship_states(mmsi: MmsiPath) -> list[dict[str, Any]]:
        if not live.known_ship(hbase, mmsi):
            raise ApiError(404, "NOT_FOUND", "unknown ship")
        return live.states(hbase, mmsi)

    @app.get("/api/zones")
    def zones() -> dict[str, Any]:
        """Geofence polygons as GeoJSON, for drawing on the map."""
        features = [
            {
                "type": "Feature",
                "properties": {"port_id": z.port_id, "kind": z.kind, "name": z.name},
                "geometry": {"type": "Polygon", "coordinates": [[list(p) for p in z.ring]]},
            }
            for z in load_zones()
        ]
        return {"type": "FeatureCollection", "features": features}

    @app.get("/api/ships/{mmsi}/route")
    def ship_route(mmsi: MmsiPath) -> dict[str, Any]:
        found = live.route(hbase, mmsi, world_ports, zone_index, port_list)
        if found is None:
            raise ApiError(404, "NOT_FOUND", "unknown ship")
        return found

    @app.get("/api/reports/ports")
    def report_ports(days: Annotated[int, Query(ge=1, le=365)] = 30) -> list[dict[str, Any]]:
        return reports.ports_daily(settings().warehouse_path, days, *report_scope())

    @app.get("/api/reports/compare")
    def report_compare() -> list[dict[str, Any]]:
        return reports.compare(settings().warehouse_path, *report_scope())

    @app.get("/", include_in_schema=False)
    def index() -> Any:
        idx = Path(STATIC) / "index.html"
        if idx.is_file():
            return FileResponse(idx)
        return {"status": "ok", "service": "HarbourWatch API", "docs": "/docs"}

    # Mount static assets for local development.
    # In production these are served by Nginx (infra/docker-compose.yml).
    if Path(STATIC).is_dir():
        app.mount("/static", StaticFiles(directory=STATIC), name="static")
    return app


app = create_app()
