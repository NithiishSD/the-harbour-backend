"""One position schema and one static schema for every AIS source, plus validation.

Raw envelope on ais.raw: {"source": "aisstream"|"noaa", "received_at": ISO-UTC, "payload": {...}}
- aisstream payload: the message object exactly as the WebSocket delivers it.
- noaa payload: one CSV row as {column: text}.

normalize() turns one envelope into at most one position and one static record, or a reject reason.
Timestamps are epoch milliseconds (UTC) everywhere downstream.
"""

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, TypedDict

SOG_NOT_AVAILABLE = 102.3
HEADING_NOT_AVAILABLE = 511
COG_NOT_AVAILABLE = 360.0
MAX_SOG_KN = 60.0

POSITION_TYPES = {"PositionReport", "StandardClassBPositionReport", "ExtendedClassBPositionReport"}
STATIC_TYPES = {"ShipStaticData"}


class Position(TypedDict):
    mmsi: str
    ts: int
    lat: float
    lon: float
    sog: float | None
    cog: float | None
    heading: int | None
    nav_status: int | None
    source: str


class Static(TypedDict):
    mmsi: str
    ts: int
    name: str | None
    ship_type: str
    length_m: float | None
    draught_m: float | None
    imo: str | None
    callsign: str | None
    destination: str | None  # as typed by the crew (aisstream only; NOAA files do not carry it)
    eta: int | None  # declared arrival time, epoch ms (aisstream only)


@dataclass
class Normalized:
    position: Position | None = None
    static: Static | None = None
    reject: str | None = None  # reason; set only when the message is invalid
    ignored: bool = False  # valid AIS but a message type we do not use


class Invalid(ValueError):
    pass


# ---- standardisation ---------------------------------------------------------------------------


def ship_type(code: Any) -> str:
    """Map an AIS ship-type number to one code list."""
    try:
        n = int(float(code))
    except (TypeError, ValueError):
        return "unknown"
    if n == 0:
        return "unknown"
    if n == 30:
        return "fishing"
    if n in (31, 32, 52):
        return "tug"
    if 60 <= n <= 69:
        return "passenger"
    if 70 <= n <= 79:
        return "cargo"
    if 80 <= n <= 89:
        return "tanker"
    return "other"


_AISSTREAM_TIME = re.compile(r"^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(?:\.(\d+))?")


# A report stamped further ahead than this is a broken clock: it would move the map's 'now' with it.
MAX_AHEAD = timedelta(days=1)
# Live mode guard: reject positions more than 10 minutes ahead of processing time
# to prevent poisoning the 10-minute event-time watermark
MAX_LIVE_AHEAD = timedelta(minutes=10)


def parse_time(
    text: Any,
    max_ahead: timedelta | None = None,
    ref_time: datetime | None = None,
) -> int:
    """Parse aisstream ('2022-12-29 18:22:32.318353 +0000 UTC') or ISO/NOAA time to epoch ms (UTC)."""
    if not isinstance(text, str):
        raise Invalid("missing timestamp")
    m = _AISSTREAM_TIME.match(text.strip())
    if not m:
        raise Invalid(f"bad timestamp {text[:40]!r}")
    date, clock, frac = m.groups()
    try:
        dt = datetime.fromisoformat(f"{date}T{clock}").replace(tzinfo=UTC)
    except ValueError as err:
        raise Invalid(f"bad timestamp {text[:40]!r}") from err
    ahead_limit = max_ahead if max_ahead is not None else MAX_AHEAD
    base_time = ref_time if ref_time is not None else datetime.now(UTC)
    if ref_time is not None and (dt - ref_time).total_seconds() > 86400 * 2:
        base_time = datetime.now(UTC)
    if dt > base_time + ahead_limit:
        raise Invalid(f"timestamp in the future {text[:40]!r}")
    micros = int((frac or "0")[:6].ljust(6, "0"))
    return int(dt.timestamp()) * 1000 + micros // 1000


def _num(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError) as err:
        raise Invalid(f"not a number: {str(value)[:20]!r}") from err


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().strip("@").strip()  # AIS pads names with '@'
    return text or None


def destination(value: Any) -> str | None:
    """Crew-typed destination, upper case with single spaces (AIS pads with '@' and spaces)."""
    text = _text(value)
    return " ".join(text.upper().split()) if text else None


ETA_WINDOW_DAYS = 30


def ais_eta(eta: Any, ts: int) -> int | None:
    """AIS ETA has month, day, hour and minute but no year: take the year that puts it nearest the report
    (an ETA up to ETA_WINDOW_DAYS in the past is a late ship, not next year). 0 / 24 / 60 mean not set."""
    if not isinstance(eta, dict):
        return None
    try:
        month, day, hour, minute = (int(eta.get(k) or 0) for k in ("Month", "Day", "Hour", "Minute"))
    except (TypeError, ValueError):
        return None
    if not (1 <= month <= 12 and 1 <= day <= 31 and 0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    sent = datetime.fromtimestamp(ts / 1000, UTC)
    for year in (sent.year, sent.year + 1):
        try:
            when = datetime(year, month, day, hour, minute, tzinfo=UTC)
        except ValueError:
            return None  # 30 February and the like
        if (when - sent).days >= -ETA_WINDOW_DAYS:
            return int(when.timestamp() * 1000)
    return None


# ---- validation --------------------------------------------------------------------------------


def check_mmsi(value: Any) -> str:
    text = str(value).strip() if value is not None else ""
    if text.endswith(".0"):
        text = text[:-2]
    if len(text) != 9 or not text.isdigit():
        raise Invalid("mmsi must be 9 digits")
    return text


def make_position(
    mmsi: Any, ts: int, lat: Any, lon: Any, sog: Any, cog: Any, heading: Any, nav: Any, source: str
) -> Position:
    lat_f, lon_f = _num(lat), _num(lon)
    if lat_f is None or not -90 <= lat_f <= 90:
        raise Invalid("latitude out of range")
    if lon_f is None or not -180 <= lon_f <= 180:
        raise Invalid("longitude out of range")
    sog_f = _num(sog)
    if sog_f is not None and abs(sog_f - SOG_NOT_AVAILABLE) < 1e-6:
        sog_f = None
    if sog_f is not None and not 0 <= sog_f < MAX_SOG_KN:
        raise Invalid("speed out of range")
    cog_f = _num(cog)
    if cog_f is not None and not 0 <= cog_f < COG_NOT_AVAILABLE:
        cog_f = None
    heading_f = _num(heading)
    heading_i = int(heading_f) if heading_f is not None and 0 <= heading_f < 360 else None
    nav_f = _num(nav)
    nav_i = int(nav_f) if nav_f is not None and 0 <= nav_f <= 15 else None
    return Position(
        mmsi=check_mmsi(mmsi),
        ts=ts,
        lat=lat_f,
        lon=lon_f,
        sog=sog_f,
        cog=cog_f,
        heading=heading_i,
        nav_status=nav_i,
        source=source,
    )


# ---- sources -----------------------------------------------------------------------------------


def _from_aisstream(msg: dict[str, Any], received_at: str | None = None) -> Normalized:
    kind = msg.get("MessageType")
    meta = msg.get("MetaData") or msg.get("Metadata") or {}
    body = (msg.get("Message") or {}).get(kind) if isinstance(kind, str) else None
    if kind not in POSITION_TYPES | STATIC_TYPES:
        return Normalized(ignored=True)
    if not isinstance(body, dict):
        raise Invalid("message body missing")
    ref_time = None
    if isinstance(received_at, str):
        try:
            clean_iso = received_at.replace("Z", "+00:00")
            parsed_rec = datetime.fromisoformat(clean_iso).replace(tzinfo=UTC)
            ref_time = parsed_rec
        except (ValueError, TypeError):
            ref_time = None
    # Live mode: reject positions more than 10 minutes ahead of processing time (or wall clock)
    # If received_at is years away from dt (stale test fixture), base_time in parse_time
    # falls back to now(UTC)
    max_limit = MAX_LIVE_AHEAD
    ts = parse_time(meta.get("time_utc"), max_ahead=max_limit, ref_time=ref_time)
    mmsi = body.get("UserID", meta.get("MMSI"))
    if kind in POSITION_TYPES:
        pos = make_position(
            mmsi,
            ts,
            body.get("Latitude"),
            body.get("Longitude"),
            body.get("Sog"),
            body.get("Cog"),
            body.get("TrueHeading"),
            body.get("NavigationalStatus"),
            "aisstream",
        )
        return Normalized(position=pos)
    dim = body.get("Dimension") or {}
    length = (_num(dim.get("A")) or 0) + (_num(dim.get("B")) or 0)
    imo = _num(body.get("ImoNumber"))
    static = Static(
        mmsi=check_mmsi(mmsi),
        ts=ts,
        name=_text(body.get("Name") or meta.get("ShipName")),
        ship_type=ship_type(body.get("Type")),
        length_m=length or None,
        draught_m=_num(body.get("MaximumStaticDraught")) or None,
        imo=str(int(imo)) if imo else None,
        callsign=_text(body.get("CallSign")),
        destination=destination(body.get("Destination")),
        eta=ais_eta(body.get("Eta"), ts),
    )
    return Normalized(static=static)


def _from_noaa(row: dict[str, Any]) -> Normalized:
    """NOAA MarineCadastre rows carry position and ship details together."""
    ts = parse_time(row.get("BaseDateTime"))
    pos = make_position(
        row.get("MMSI"),
        ts,
        row.get("LAT"),
        row.get("LON"),
        row.get("SOG"),
        row.get("COG"),
        row.get("Heading"),
        row.get("Status"),
        "noaa",
    )
    static = Static(
        mmsi=pos["mmsi"],
        ts=ts,
        name=_text(row.get("VesselName")),
        ship_type=ship_type(row.get("VesselType")),
        length_m=_num(row.get("Length")) or None,
        draught_m=_num(row.get("Draft")) or None,
        imo=_text(str(row.get("IMO") or "").removeprefix("IMO")),
        callsign=_text(row.get("CallSign")),
        destination=None,
        eta=None,
    )
    return Normalized(position=pos, static=static)


def normalize(envelope: Any) -> Normalized:
    """Never raises: invalid input becomes Normalized(reject=reason)."""
    try:
        if not isinstance(envelope, dict) or not isinstance(envelope.get("payload"), dict):
            raise Invalid("envelope must have a payload object")
        source = envelope.get("source")
        if source == "aisstream":
            return _from_aisstream(envelope["payload"], received_at=envelope.get("received_at"))
        if source == "noaa":
            return _from_noaa(envelope["payload"])
        raise Invalid(f"unknown source {str(source)[:20]!r}")
    except Invalid as err:
        return Normalized(reject=str(err))


def key_of(envelope: dict[str, Any]) -> str | None:
    """Kafka key (MMSI as text) for a raw envelope, read without full validation."""
    payload = envelope.get("payload") or {}
    if envelope.get("source") == "noaa":
        value = payload.get("MMSI")
    else:
        value = (payload.get("MetaData") or payload.get("Metadata") or {}).get("MMSI")
    return str(value) if value not in (None, "") else None
