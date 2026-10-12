"""Live AIS from aisstream.io into ais.raw.

    AISSTREAM_API_KEY=... python -m ingest.aisstream

aisstream keeps nothing and cannot resend, so every message goes straight to Kafka. On disconnect the
producer reconnects with exponential backoff. The API key is sent once per connection and never logged.
"""

import asyncio
import json
import random
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import Any

from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from common import scopes, topics
from common.config import settings
from common.geo import load_world_ports
from common.kafka import JsonProducer
from common.log import get_logger
from common.monitor import Monitor
from common.schema import key_of
from ingest.replay import Sender

URL = "wss://stream.aisstream.io/v0/stream"
MESSAGE_TYPES = [
    "PositionReport",
    "StandardClassBPositionReport",
    "ExtendedClassBPositionReport",
    "ShipStaticData",
]
MAX_MESSAGE_BYTES = 1 << 20
log = get_logger("ingest.aisstream")


class FatalStreamError(RuntimeError):
    """The server refused us (for example a bad API key); retrying will not help."""


MAX_FOLLOWED = 200  # ships one subscription may name (aisstream's limit)
WORLD = ((-90.0, -180.0, 90.0, 180.0),)


def subscription(
    api_key: str,
    bboxes: Sequence[tuple[float, float, float, float]],
    mmsis: Sequence[str] | None = None,
) -> dict[str, Any]:
    """`mmsis`: only these ships (at most MAX_FOLLOWED), wherever in the boxes they are heard."""
    sub: dict[str, Any] = {
        "APIKey": api_key,
        "BoundingBoxes": [[[lat1, lon1], [lat2, lon2]] for lat1, lon1, lat2, lon2 in bboxes],
        "FilterMessageTypes": MESSAGE_TYPES,
    }
    if mmsis is not None:
        sub["FiltersShipMMSI"] = list(mmsis)[:MAX_FOLLOWED]
    return sub


def backoff(attempt: int, cap: float = 60) -> float:
    return min(cap, 2.0**attempt) * (0.5 + random.random() / 2)  # noqa: S311 - jitter, not crypto


class Stats:
    def __init__(self) -> None:
        self.received = 0
        self.unparseable = 0


CHECK_EVERY_S = 10  # how often a running connection asks whether the wanted boxes changed

Boxes = Sequence[tuple[float, float, float, float]]


async def stream_once(
    url: str,
    sub: dict[str, Any],
    sender: Sender,
    stats: Stats,
    stop_after: int | None,
    changed: Callable[[], bool] | None = None,
    update: Callable[[], dict[str, Any] | None] | None = None,
) -> None:
    """One connection. `changed` says the subscription is out of date: the connection ends (and the caller
    connects again with the new boxes). `update` returns a new subscription (or None): it is sent on the open
    connection, which aisstream applies in place of the old one (at most once a second)."""
    checked = time.monotonic()
    async with connect(url, max_size=MAX_MESSAGE_BYTES, open_timeout=10) as ws:
        await ws.send(json.dumps(sub))  # aisstream closes the socket if this is not sent within 3 s
        log.info(
            "subscribed", extra={"data": {"bbox": sub["BoundingBoxes"], "types": sub["FilterMessageTypes"]}}
        )
        async for frame in ws:
            try:
                msg = json.loads(frame)
            except (ValueError, UnicodeDecodeError):
                stats.unparseable += 1
                continue
            if isinstance(msg, dict) and "error" in msg:
                raise FatalStreamError(str(msg["error"])[:200])
            envelope = {"source": "aisstream", "received_at": datetime.now(UTC).isoformat(), "payload": msg}
            sender.send(topics.RAW, key_of(envelope), envelope)
            stats.received += 1
            if stats.received % 1000 == 0:
                log.info("progress", extra={"data": {"received": stats.received}})
            if stop_after is not None and stats.received >= stop_after:
                return
            if (changed or update) is not None and time.monotonic() - checked > CHECK_EVERY_S:
                checked = time.monotonic()
                if changed is not None and changed():
                    log.info("subscription changed: reconnecting with the new boxes")
                    return
                newer = update() if update is not None else None
                if newer is not None:
                    await ws.send(json.dumps(newer))
                    ships = len(newer.get("FiltersShipMMSI", []))
                    log.info("subscription updated", extra={"data": {"ships": ships}})


def _updater(
    api_key: str, boxes: Boxes, follow: Callable[[], Sequence[str]], current: Sequence[str] | None
) -> Callable[[], dict[str, Any] | None]:
    """A function giving the subscription to send when the list of followed ships has changed, else None."""
    state = [list(current or [])]

    def update() -> dict[str, Any] | None:
        latest = list(follow())
        if set(latest) == set(state[0]):  # the same ships in another order is not a change
            return None
        state[0] = latest
        return subscription(api_key, boxes, latest)

    return update


async def run(
    url: str,
    api_key: str,
    bboxes: Boxes | Callable[[], Boxes],
    sender: Sender,
    stop_after: int | None = None,
    max_backoff: float = 60,
    follow: Callable[[], Sequence[str]] | None = None,
) -> Stats:
    """Stream until stop_after messages (forever when None), reconnecting on network errors. `bboxes` may be
    a function: it is asked for the boxes at every connection and every CHECK_EVERY_S while connected, and a
    change makes the connection start again (the viewer chose other ports or countries). `follow` gives the
    ships to follow (MMSIs); the list is asked for every CHECK_EVERY_S and a change is sent on the open
    connection."""
    stats = Stats()
    attempt = 0
    while stop_after is None or stats.received < stop_after:
        before = stats.received
        wanted = bboxes() if callable(bboxes) else bboxes
        ships = list(follow()) if follow is not None else None
        sub = subscription(api_key, wanted, ships)
        changed = (lambda w=wanted: bboxes() != w) if callable(bboxes) else None
        update = _updater(api_key, wanted, follow, ships) if follow is not None else None
        try:
            await stream_once(url, sub, sender, stats, stop_after, changed, update)
        except (OSError, WebSocketException, TimeoutError) as err:
            log.warning("connection lost", extra={"data": {"error": type(err).__name__}})
        finally:
            sender.flush(5)
        if stop_after is not None and stats.received >= stop_after:
            break
        attempt = 0 if stats.received > before else attempt + 1
        if attempt == 5:
            # aisstream drops the socket without a reason for a bad key, so this is the only signal.
            log.error("5 connections in a row closed without data; check AISSTREAM_API_KEY and LIVE_BBOX")
        delay = backoff(attempt, max_backoff)
        log.info("reconnecting", extra={"data": {"in_seconds": round(delay, 2)}})
        await asyncio.sleep(delay)
    return stats


def _live_since_ms(s: Any) -> int:
    d = s.live_since
    return int(datetime(d.year, d.month, d.day, tzinfo=UTC).timestamp() * 1000)


def main() -> None:
    s = settings()
    if not s.aisstream_api_key:
        raise SystemExit("AISSTREAM_API_KEY is not set (see .env.example)")
    topics.ensure_topics()
    world = load_world_ports()
    environment = Monitor.from_settings(world)

    def boxes() -> tuple[tuple[float, float, float, float], ...]:
        """What to listen to now: the viewer's ports and source countries (common.selection) over the
        environment's. A new subscription is a new line of the scope ledger (common.scopes)."""
        monitor = Monitor.selected(world, environment)
        found = tuple(s.live_bboxes) or monitor.ais_boxes(world)
        scopes.record(s.data_dir, monitor.country, monitor.ports, found, first_data_ms=_live_since_ms(s))
        return found

    try:
        asyncio.run(run(URL, s.aisstream_api_key, boxes, JsonProducer()))
    except FatalStreamError as err:
        log.error("aisstream refused the connection", extra={"data": {"error": str(err)}})
        raise SystemExit(1) from err


if __name__ == "__main__":
    main()
