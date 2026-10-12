"""Follow the ships on a voyage to or from a monitored port, wherever in the world they are heard.

    AISSTREAM_API_KEY=... python -m ingest.follow

A second aisstream connection with the whole world as its area and a list of ships (common.follow): only
those ships are sent, from any station anywhere, so a ship that left Singapore is updated again the moment
some coast hears it. The list is worked out from HBase every FOLLOW_REFRESH_S and sent on the open
connection. Messages go to ais.raw like the regional feed's.
"""

import asyncio
import json
from pathlib import Path

from common import follow, topics
from common.config import settings
from common.geo import load_world_ports
from common.kafka import JsonProducer
from common.log import get_logger
from common.monitor import Monitor
from dashboard import live
from ingest.aisstream import URL, WORLD, FatalStreamError, run
from storage.hbase.client import HBaseClient

log = get_logger("ingest.follow")
STATUS = "follow_status.json"


def ships_to_follow(client: HBaseClient, monitor: Monitor, world: dict) -> list[str]:
    return follow.voyage_mmsis(live.ships(client), monitor, world, follow.now_ms())


def main() -> None:
    s = settings()
    if not s.aisstream_api_key:
        raise SystemExit("AISSTREAM_API_KEY is not set (see .env.example)")
    topics.ensure_topics()
    world = load_world_ports()
    environment = Monitor.from_settings(world)
    client = HBaseClient(timeout=30)
    last: list[tuple[float, list[str]]] = []

    def listing() -> list[str]:
        """The ships to follow now, kept for FOLLOW_REFRESH_S (the scan of every ship is not free)."""
        import time

        if last and time.monotonic() - last[0][0] < s.follow_refresh_s:
            return last[0][1]
        monitor = Monitor.selected(world, environment)
        ships = ships_to_follow(client, monitor, world)
        last[:] = [(time.monotonic(), ships)]
        path: Path = s.data_dir / STATUS
        status = {"ships": len(ships), "max": s.follow_max, "at": follow.now_ms(), "mmsis": ships}
        path.write_text(json.dumps(status))
        log.info("following", extra={"data": {"ships": len(ships)}})
        return ships

    try:
        asyncio.run(run(URL, s.aisstream_api_key, WORLD, JsonProducer(), follow=listing))
    except FatalStreamError as err:
        log.error("aisstream refused the connection", extra={"data": {"error": str(err)}})
        raise SystemExit(1) from err


if __name__ == "__main__":
    main()
