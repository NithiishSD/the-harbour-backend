"""Runtime settings, read from environment variables (and an optional .env file)."""

import os
from dataclasses import dataclass
from datetime import date
from functools import cache
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _load_dotenv(path: Path) -> None:
    """Copy KEY=VALUE lines from .env into os.environ without overriding real env vars."""
    if not path.is_file():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str
    hbase_rest_url: str
    aisstream_api_key: str
    data_dir: Path
    geo_dir: Path
    # Live areas as (min_lat, min_lon, max_lat, max_lon); empty means the boxes along the sea route
    # (common.lanes.corridor_boxes).
    live_bboxes: tuple[tuple[float, float, float, float], ...]
    dark_timeout_hours: float
    min_active_ships: int
    queue_growth_pct: float
    queue_growth_min: int
    spark_master: str
    live_since: date  # the live feed has existed since this day: older non-NOAA data is test data
    history_max_mb: int  # offline history of the live feed (data/history/*/aisstream.parquet)
    cold_max_mb: int  # raw archive of the live feed (data/cold/topic=ais.raw)
    monitor_country: str  # the country this dashboard watches (common.monitor)
    monitor_ports: tuple[str, ...]  # its ports to watch; empty means all of them
    monitor_partners: tuple[str, ...]  # countries whose ships bound for those ports are tracked
    monitor_region_km: float  # how far its waters reach from a port when it has no box
    # its waters as boxes; empty: geo/national_waters.csv, else circles of monitor_region_km
    monitor_waters: tuple[tuple[float, float, float, float], ...]
    monitor_max_partners: int  # most source countries followed at once
    monitor_min_partners: int  # fewest the suggestions are topped up to
    monitor_timezone: str  # IANA zone the dashboard shows times in; empty: geo/timezones.csv for the country
    monitor_ais_pad: int  # squares of 5 degrees added round every lane the live feed listens along
    monitor_ais_region_km: float  # the feed listens this far round the monitored ports
    monitor_ais_source_km: float  # ... and this far round every port of a chosen source country
    monitor_followable_hours: float  # a blind stretch up to this long is bridged by the estimate
    monitor_source_port_km: float  # a first report this close to a port is "at" it
    monitor_stop_km: float  # a ship standing this close to a port is stopped at it
    monitor_left_km: float  # ... and has left it once a later report is this far away
    monitor_slow_kn: float  # standing still: no faster than this to or from the neighbouring report
    spark_driver_memory: str  # the streaming job's JVM heap
    spark_shuffle_partitions: int  # partitions of a shuffle: a handful is plenty on one machine
    spark_ui: bool  # the Spark web UI (a few threads and some heap)
    stream_trigger_s: int  # seconds between the streaming queries' micro-batches
    ships_cache_s: float  # the dashboard keeps its list of ships this long and shares it between requests
    history_every_s: float  # seconds between archive runs of the replay history
    follow_max: int  # ships followed worldwide at once (aisstream allows 200 per subscription)
    follow_days: float  # a voyage is followed this long at most
    follow_refresh_s: float  # how often the list of followed ships is worked out again
    map_transit_hours: float  # ships tracked only for being in the waters leave the map after this silence
    map_arrived_hours: float  # arrived ships stay on the map this long
    map_silent_hours: float  # a travelling ship silent this long leaves the map

    @property
    def warehouse_path(self) -> Path:
        return self.data_dir / "warehouse.duckdb"


def _bboxes(raw: str) -> tuple[tuple[float, float, float, float], ...]:
    """'min_lat,min_lon,max_lat,max_lon' boxes separated by ';'."""
    boxes = []
    for box in filter(None, (b.strip() for b in raw.split(";"))):
        parts = [float(p) for p in box.split(",")]
        if len(parts) != 4:
            raise ValueError(f"a box is 4 comma-separated numbers (LIVE_BBOX, MONITOR_WATERS), got {box!r}")
        boxes.append((parts[0], parts[1], parts[2], parts[3]))
    return tuple(boxes)


@cache
def settings() -> Settings:
    _load_dotenv(ROOT / ".env")
    env = os.environ.get
    return Settings(
        kafka_bootstrap=env("KAFKA_BOOTSTRAP", "localhost:9092"),
        hbase_rest_url=env("HBASE_REST_URL", "http://localhost:8080").rstrip("/"),
        aisstream_api_key=env("AISSTREAM_API_KEY", ""),
        data_dir=Path(env("DATA_DIR", str(ROOT / "data"))),
        geo_dir=Path(env("GEO_DIR", str(ROOT / "geo"))),
        live_bboxes=_bboxes(env("LIVE_BBOX", "")),
        dark_timeout_hours=float(env("DARK_TIMEOUT_HOURS", "6")),
        min_active_ships=int(env("MIN_ACTIVE_SHIPS", "3")),
        queue_growth_pct=float(env("QUEUE_GROWTH_PCT", "50")),
        queue_growth_min=int(env("QUEUE_GROWTH_MIN", "5")),
        spark_master=env("SPARK_MASTER", "local[*]"),
        live_since=date.fromisoformat(env("LIVE_SINCE", "2026-10-10")),
        history_max_mb=int(env("HISTORY_MAX_MB", "1024")),
        cold_max_mb=int(env("COLD_MAX_MB", "2048")),
        monitor_country=env("MONITOR_COUNTRY", "SG").strip().upper(),
        monitor_ports=tuple(p.strip().upper() for p in env("MONITOR_PORTS", "").split(",") if p.strip()),
        monitor_partners=tuple(
            c.strip().upper() for c in env("MONITOR_PARTNERS", "auto").split(",") if c.strip()
        ),
        monitor_region_km=float(env("MONITOR_REGION_KM", "150")),
        monitor_waters=_bboxes(env("MONITOR_WATERS", "")),
        monitor_max_partners=int(env("MONITOR_MAX_PARTNERS", "10")),
        monitor_min_partners=int(env("MONITOR_MIN_PARTNERS", "5")),
        monitor_timezone=env("MONITOR_TIMEZONE", "").strip(),
        monitor_ais_pad=int(env("MONITOR_AIS_PAD", "2")),
        monitor_ais_region_km=float(env("MONITOR_AIS_REGION_KM", "800")),
        monitor_ais_source_km=float(env("MONITOR_AIS_SOURCE_KM", "500")),
        monitor_followable_hours=float(env("MONITOR_FOLLOWABLE_HOURS", "48")),
        monitor_source_port_km=float(env("MONITOR_SOURCE_PORT_KM", "60")),
        monitor_stop_km=float(env("MONITOR_STOP_KM", "15")),
        monitor_left_km=float(env("MONITOR_LEFT_KM", "30")),
        monitor_slow_kn=float(env("MONITOR_SLOW_KN", "1.5")),
        spark_driver_memory=env("SPARK_DRIVER_MEMORY", "2g"),
        spark_shuffle_partitions=int(env("SPARK_SHUFFLE_PARTITIONS", "8")),
        spark_ui=env("SPARK_UI", "1") not in ("0", "false", "no"),
        stream_trigger_s=int(env("STREAM_TRIGGER_S", "10")),
        ships_cache_s=float(env("SHIPS_CACHE_S", "2")),
        history_every_s=float(env("HISTORY_EVERY_S", "600")),
        follow_max=int(env("FOLLOW_MAX", "200")),
        follow_days=float(env("FOLLOW_DAYS", "30")),
        follow_refresh_s=float(env("FOLLOW_REFRESH_S", "300")),
        map_transit_hours=float(env("MAP_TRANSIT_HOURS", "6")),
        map_arrived_hours=float(env("MAP_ARRIVED_HOURS", "24")),
        map_silent_hours=float(env("MAP_SILENT_HOURS", "24")),
    )
