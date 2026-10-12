from pathlib import Path

import duckdb
import pandas as pd

from measure.timing import gaps, intervals

H = 3_600_000


def history(tmp_path: Path) -> str:
    rows = []
    for i in range(0, 5):  # ship A stands still, one report every 3 minutes
        rows.append({"mmsi": "100000001", "ts": i * 180_000, "sog": 0.0, "source": "aisstream"})
    rows += [{"mmsi": "100000002", "ts": 0, "sog": 0.0, "source": "aisstream"}]  # ship B: 7 h of silence
    rows += [{"mmsi": "100000002", "ts": 7 * H, "sog": 0.0, "source": "aisstream"}]
    folder = tmp_path / "positions" / "date=2026-10-10"
    folder.mkdir(parents=True)
    duckdb.connect().register("r", pd.DataFrame(rows)).execute(
        f"COPY (SELECT * FROM r) TO '{folder / 'aisstream.parquet'}' (FORMAT parquet)"
    )
    return str(tmp_path / "positions" / "date=*" / "*.parquet")


def test_intervals_and_gaps(tmp_path: Path):
    glob, con = history(tmp_path), duckdb.connect()
    (row,) = [r for r in intervals(con, glob, None) if r[1] == "standing still"]
    assert row[0] == "aisstream" and row[2] == 5 and row[3] == 180.0  # A's four 3-minute steps and B's gap
    ((source, ships, over1, over3, over6, over12, over24),) = gaps(con, glob, None)
    assert (source, ships, over1, over6, over12, over24) == ("aisstream", 2, 1, 1, 0, 0)
    assert over3 == 1
