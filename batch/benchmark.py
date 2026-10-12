"""Batch scaling: the RDD backfill on the same NOAA files with 4, 2 and 1 Spark cores.

    python -m batch.benchmark --input 'data/noaa/csv/AIS_2023_01_0[123].csv'

Each run uses a fresh local[N] session and a throw-away output folder; the files are read once first so
every run starts with the same warm OS cache. Prints a table and writes data/batch/benchmark.json.
All runs must agree on the counts, otherwise the timing compares different work.
"""

import argparse
import glob
import json
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any

from batch.backfill_rdd import backfill
from common.config import settings
from common.log import get_logger
from common.spark import session

log = get_logger("batch.benchmark")
DRIVER_MEMORY = "6g"  # 23 M rows: 2 and 4 GB run out of heap; needs ~7 GB of free host memory
CHECKED = ("rows", "positions", "duplicates", "unique_positions", "ships", "vessel_days")


def warm(pattern: str) -> int:
    total = 0
    for name in sorted(glob.glob(pattern)):
        with open(name, "rb") as f:
            while chunk := f.read(1 << 24):
                total += len(chunk)
    return total


def run_once(pattern: str, cores: int) -> dict[str, Any]:
    out = Path(tempfile.mkdtemp(prefix="hw-bench-"))
    spark = session(f"harbourwatch-bench-{cores}", master=f"local[{cores}]", driver_memory=DRIVER_MEMORY)
    try:
        start = time.perf_counter()
        summary = backfill(spark, pattern, out)
        seconds = time.perf_counter() - start
    finally:
        spark.stop()
        shutil.rmtree(out, ignore_errors=True)
    return {"cores": cores, "seconds": round(seconds, 1)} | {k: summary[k] for k in CHECKED}


def benchmark(pattern: str, cores: list[int]) -> dict[str, Any]:
    size = warm(pattern)
    runs = []
    for n in cores:
        runs.append(run_once(pattern, n))
        log.info("run finished", extra={"data": runs[-1]})  # kept in the log if a later run dies
    reference = {k: runs[0][k] for k in CHECKED}
    if any({k: r[k] for k in CHECKED} != reference for r in runs):
        raise SystemExit(f"runs disagree on the counts: {runs}")
    base = min(runs, key=lambda r: r["cores"])["seconds"]
    for r in runs:
        r["speedup"] = round(base / r["seconds"], 2)
        r["rows_per_second"] = round(r["rows"] / r["seconds"])
    return {"input": pattern, "megabytes": round(size / 1e6), "runs": runs}


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--input", required=True, help="NOAA CSV glob")
    parser.add_argument("--cores", default="4,2,1", help="fastest first; speedup is against the fewest cores")
    args = parser.parse_args()
    result = benchmark(args.input, [int(c) for c in args.cores.split(",")])
    out = settings().data_dir / "batch" / "benchmark.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
