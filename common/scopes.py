"""Which sea the live feed was subscribed to, and since when: data/monitor_scopes.jsonl.

The monitored country is fixed by MONITOR_COUNTRY when the stack is deployed. The stores (HBase, the history,
the warehouse) hold whatever was received, under whichever country was set then, so a redeploy for another
country must not show the old country's data in a replay or a report. The feed writes one line when it starts
with a subscription that differs from the last; the dashboard reads the spans in which the feed was set to
its country (`eras`) and shows nothing from outside them.

  {"since": epoch ms, "country": "SG", "ports": ["SGSIN"], "boxes": [[min_lat, min_lon, max_lat, max_lon]]}

`boxes` is null for the line written for data that predates the ledger: it covers the country it names.
"""

import json
import time
from pathlib import Path
from typing import Any

LEDGER = "monitor_scopes.jsonl"
Scope = dict[str, Any]
Span = tuple[int, int]
INF = 1 << 62  # an era that has not ended


def read(data_dir: Path) -> list[Scope]:
    path = data_dir / LEDGER
    if not path.is_file():
        return []
    out = []
    for line in path.read_text().splitlines():
        try:
            out.append(json.loads(line))
        except ValueError:
            continue  # a half-written line: the ledger is only ever appended to
    return sorted(out, key=lambda e: e["since"])


def record(
    data_dir: Path,
    country: str,
    ports: tuple[str, ...],
    boxes: tuple[tuple[float, float, float, float], ...],
    first_data_ms: int | None = None,
    now_ms: int | None = None,
) -> bool:
    """Append the subscription when it differs from the last one. `first_data_ms`: when the ledger is new,
    the line that stands for the data already there (it covers `country`). True when a line was added."""
    entries = read(data_dir)
    wanted = [list(b) for b in boxes]
    if entries and entries[-1]["country"] == country and entries[-1]["boxes"] == wanted:
        return False
    lines: list[Scope] = []
    if not entries and first_data_ms is not None:
        lines.append({"since": first_data_ms, "country": country, "ports": list(ports), "boxes": None})
    lines.append(
        {
            "since": now_ms if now_ms is not None else int(time.time() * 1000),
            "country": country,
            "ports": list(ports),
            "boxes": wanted,
        }
    )
    data_dir.mkdir(parents=True, exist_ok=True)
    with (data_dir / LEDGER).open("a") as f:
        f.writelines(json.dumps(line) + "\n" for line in lines)
    return True


def eras(entries: list[Scope], country: str, fallback_since_ms: int | None = None) -> list[Span] | None:
    """The spans of time (start, end) when the feed was subscribed for `country`; the last one is open
    (end = INF). None when there is no ledger and no fallback: nothing separates one country's data from
    another's (a demo, a replay of NOAA, tests), so nothing is held back."""
    if not entries:
        return None if fallback_since_ms is None else [(fallback_since_ms, INF)]
    out: list[Span] = []
    for i, e in enumerate(entries):
        end = entries[i + 1]["since"] if i + 1 < len(entries) else INF
        if e["country"] == country:
            if out and out[-1][1] == e["since"]:
                out[-1] = (out[-1][0], end)  # the same country again with other boxes: one span
            else:
                out.append((e["since"], end))
    return out


def in_eras(spans: list[Span], ts_ms: int) -> bool:
    return any(a <= ts_ms < b for a, b in spans)


def overlaps(spans: list[Span], start_ms: int, end_ms: int) -> bool:
    return any(a <= end_ms and start_ms < b for a, b in spans)


def predicate(spans: list[Span], noaa: bool) -> str:
    """SQL over the history's `source` and `ts` columns: the rows that belong to the country. NOAA's files are
    US waters, so they belong only when the country is the US."""
    parts = ["source = 'noaa'"] if noaa else []
    times = " OR ".join(f"(ts >= {a} AND ts < {b})" if b != INF else f"ts >= {a}" for a, b in spans)
    if times:
        parts.append(f"(COALESCE(source, '') <> 'noaa' AND ({times}))")
    return " OR ".join(parts) or "FALSE"
