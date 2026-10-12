"""Give every ship already in HBase its source: the oldest position stored for it.

    python -m batch.backfill_sources            # dry run: how many ships would get one
    python -m batch.backfill_sources --apply

New ships get `src:*` cells from the streaming job (streaming.sinks.state_puts). A ship the job was already
following when that began has none, so its first position since then would be taken for its source. This reads
the oldest track row of each such ship and writes the cells the same way, with the timestamp that makes the
earliest report win, so it is safe to run again and never overwrites an earlier source.
"""

import argparse
import time
from typing import Any

from common.log import get_logger
from storage.hbase.client import HBaseClient
from storage.hbase.tables import VESSEL_STATE, VESSEL_TRACK, unrev
from streaming.sinks import SRC_TS_BASE

log = get_logger("batch.backfill_sources")
MAX_TRACK_ROWS = 100_000


def _num(row: dict[str, str], column: str) -> float | None:
    try:
        return float(row[column])
    except (KeyError, ValueError):
        return None


def run(client: Any, apply: bool = False) -> dict[str, int]:
    out = {"ships": 0, "without_source": 0, "with_track": 0, "written": 0}
    horizon = int(time.time() * 1000) + 86_400_000
    for mmsi, row in client.scan_all(VESSEL_STATE, 1_000_000):
        out["ships"] += 1
        if row.get("src:lat") is not None:
            continue
        if len(mmsi) != 9 or not mmsi.isdigit():  # a shorter id is a prefix of other ships' track rows
            continue
        out["without_source"] += 1
        rows = client.scan_prefix(VESSEL_TRACK, mmsi, MAX_TRACK_ROWS)
        track = [(k, c) for k, c in rows if len(k) == 28 and unrev(k[9:]) <= horizon]  # no clock-skewed rows
        if not track:
            continue
        out["with_track"] += 1
        key, cells = track[-1]  # newest first: the last row is the oldest
        lat, lon, ts = _num(cells, "pos:lat"), _num(cells, "pos:lon"), unrev(key[len(mmsi) :])
        if lat is None or lon is None:
            continue
        if apply:
            src = {"src:lat": str(lat), "src:lon": str(lon), "src:ts": str(ts)}
            client.put(VESSEL_STATE, mmsi, src, SRC_TS_BASE - ts)
            out["written"] += 1
    log.info("sources", extra={"data": {**out, "applied": apply}})
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--apply", action="store_true", help="write the cells (without it only counts)")
    run(HBaseClient(timeout=60), parser.parse_args().apply)


if __name__ == "__main__":
    main()
