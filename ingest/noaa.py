
"""Download daily NOAA AIS files and extract CSV, optionally clipped to a bounding box."""

import argparse
import csv
import io
import shutil
import urllib.request
import zipfile
from contextlib import ExitStack
from datetime import date, timedelta
from pathlib import Path

import zstandard

from common.config import settings
from common.log import get_logger

BASE_URL = "https://noaaocm.blob.core.windows.net/ais/csv2"
log = get_logger("ingest.noaa")

BBox = tuple[float, float, float, float]


def file_name(day: date) -> str:
    return f"AIS_{day:%Y_%m_%d}"


def download(
    day: date, dest: Path, base_url: str = BASE_URL, timeout: float = 60
) -> Path:
    """Download the day's compressed AIS CSV, using an atomic temporary file."""
    dest.mkdir(parents=True, exist_ok=True)

    target = dest / f"ais-{day:%Y-%m-%d}.csv.zst"
    if target.is_file():
        return target

    url = f"{base_url}/csv{day.year}/{target.name}"
    part = target.with_suffix(target.suffix + ".part")
    log.info("downloading", extra={"data": {"url": url}})

    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            with part.open("wb") as out:
                shutil.copyfileobj(response, out, length=1 << 20)
        part.replace(target)
    finally:
        part.unlink(missing_ok=True)

    return target


def in_bbox(row: dict[str, str], bbox: BBox) -> bool:
    try:
        lat, lon = float(row["LAT"]), float(row["LON"])
    except (KeyError, TypeError, ValueError):
        return False
    return bbox[0] <= lat <= bbox[2] and bbox[1] <= lon <= bbox[3]


def extract(source_path: Path, out_csv: Path, bbox: BBox | None = None) -> int:
    """Extract a ZIP or Zstandard-compressed CSV, optionally filtering coordinates."""
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    with ExitStack() as stack:
        if source_path.name.endswith(".zst"):
            compressed = stack.enter_context(source_path.open("rb"))
            stream = stack.enter_context(
                zstandard.ZstdDecompressor().stream_reader(compressed)
            )
            text = stack.enter_context(
                io.TextIOWrapper(stream, encoding="utf-8", newline="")
            )
        else:
            archive = stack.enter_context(zipfile.ZipFile(source_path))
            member = next(
                (name for name in archive.namelist() if name.lower().endswith(".csv")),
                None,
            )
            if member is None:
                raise ValueError(f"No CSV file found inside {source_path}")
            raw = stack.enter_context(archive.open(member))
            text = stack.enter_context(
                io.TextIOWrapper(raw, encoding="utf-8", newline="")
            )

        with out_csv.open("w", newline="", encoding="utf-8") as out:
            if bbox is None:
                reader = csv.reader(text)
                writer = csv.writer(out)
                rows = 0
                for row in reader:
                    writer.writerow(row)
                    rows += 1
                return max(rows - 1, 0)
            reader = csv.DictReader(text)
            writer = csv.DictWriter(out, fieldnames=reader.fieldnames or [])
            writer.writeheader()
            rows = 0

            for row in reader:
                if in_bbox(row, bbox):
                    writer.writerow(row)
                    rows += 1

            return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--days", type=int, default=1)
    parser.add_argument("--bbox", help="min_lat,min_lon,max_lat,max_lon")
    parser.add_argument("--keep-zip", action="store_true")
    args = parser.parse_args()

    if args.days < 1:
        parser.error("--days must be at least 1")

    bbox: BBox | None = None
    if args.bbox:
        parts = [float(part) for part in args.bbox.split(",")]
        if len(parts) != 4:
            parser.error("--bbox must contain exactly four numbers")
        bbox = (parts[0], parts[1], parts[2], parts[3])

    root = settings().data_dir / "noaa"

    for offset in range(args.days):
        day = args.date + timedelta(days=offset)
        source_path = download(day, root / "zip")
        suffix = "_bbox" if bbox else ""
        out_csv = root / "csv" / f"{file_name(day)}{suffix}.csv"

        rows = extract(source_path, out_csv, bbox)
        log.info(
            "extracted",
            extra={"data": {"file": str(out_csv), "rows": rows}},
        )

        if not args.keep_zip:
            source_path.unlink()


if __name__ == "__main__":
    main()
