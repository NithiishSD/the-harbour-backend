"""Where is a ship ever heard? A short worldwide survey of the live feed, to choose countries by.

    python -m ingest.coverage_survey --minutes 10

Subscribes to the whole world for a few minutes (position reports only), counts the distinct ships heard in
every 2-degree square and writes data/coverage/cells.json. Nothing goes to Kafka. The squares with ships are
where coastal AIS reaches; the blank ocean between them is where a voyage has to be estimated
(common.coverage).
Needs AISSTREAM_API_KEY. Run it again to refresh the picture; a longer survey hears the quieter coasts.
"""

import argparse
import asyncio
import json
import math
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from common.config import settings
from common.log import get_logger
from ingest.aisstream import MAX_MESSAGE_BYTES, URL

log = get_logger("ingest.coverage_survey")
POSITIONS = ["PositionReport", "StandardClassBPositionReport", "ExtendedClassBPositionReport"]


def cell_key(lat: float, lon: float, size: float) -> str:
    return f"{math.floor(lat / size)}:{math.floor((lon + 180) / size)}"


def tally(frame: str | bytes, seen: dict[str, set[str]], size: float) -> bool:
    """Add one aisstream message to the survey; False when it is not a usable position."""
    try:
        msg = json.loads(frame)
        meta = msg["MetaData"]
        lat, lon, mmsi = float(meta["latitude"]), float(meta["longitude"]), str(meta["MMSI"])
    except (ValueError, KeyError, TypeError):
        return False
    if not (-90 <= lat <= 90 and -180 <= lon <= 180) or (lat == 0 and lon == 0):
        return False
    seen.setdefault(cell_key(lat, lon, size), set()).add(mmsi)
    return True


async def survey(api_key: str, minutes: float, size: float, url: str = URL) -> dict[str, set[str]]:
    seen: dict[str, set[str]] = {}
    deadline = time.monotonic() + minutes * 60
    sub = {
        "APIKey": api_key,
        "BoundingBoxes": [[[-90.0, -180.0], [90.0, 180.0]]],
        "FilterMessageTypes": POSITIONS,
    }
    heard = 0
    while time.monotonic() < deadline:
        try:
            async with connect(url, max_size=MAX_MESSAGE_BYTES, open_timeout=15) as ws:
                await ws.send(json.dumps(sub))
                while (left := deadline - time.monotonic()) > 0:
                    try:
                        frame = await asyncio.wait_for(ws.recv(), timeout=min(left, 30))
                    except TimeoutError:
                        continue
                    heard += tally(frame, seen, size)
                    if heard and heard % 20_000 == 0:
                        log.info("heard", extra={"data": {"messages": heard, "squares": len(seen)}})
        except (OSError, WebSocketException) as err:
            log.warning("connection lost", extra={"data": {"error": type(err).__name__}})
            await asyncio.sleep(5)
    return seen


def write(path: Path, seen: dict[str, set[str]], size: float, minutes: float) -> dict[str, Any]:
    """Write the survey; with an earlier one of the same square size the two are merged (a square heard in
    either stays heard, the busier count is kept), so surveys at different hours add up."""
    counts = {k: len(v) for k, v in seen.items()}
    before_minutes = 0.0
    if path.is_file():
        old = json.loads(path.read_text())
        if float(old.get("cell", 0)) == size:
            before_minutes = float(old.get("minutes", 0))
            counts = {k: max(counts.get(k, 0), int(old["cells"].get(k, 0))) for k in {*counts, *old["cells"]}}
    doc = {
        "cell": size,
        "minutes": round(before_minutes + minutes, 1),
        "at": datetime.now(UTC).isoformat(timespec="seconds"),
        "ships": len({m for ships in seen.values() for m in ships}),
        "cells": dict(sorted(counts.items())),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, separators=(",", ":")))
    return doc


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--minutes", type=float, default=10)
    parser.add_argument("--cell", type=float, default=2.0, help="square size in degrees")
    args = parser.parse_args()
    s = settings()
    if not s.aisstream_api_key:
        raise SystemExit("AISSTREAM_API_KEY is not set (see .env.example)")
    seen = asyncio.run(survey(s.aisstream_api_key, args.minutes, args.cell))
    doc = write(s.data_dir / "coverage" / "cells.json", seen, args.cell, args.minutes)
    log.info(
        "survey written",
        extra={"data": {k: doc[k] for k in ("ships", "minutes", "at")} | {"squares": len(seen)}},
    )


if __name__ == "__main__":
    main()
