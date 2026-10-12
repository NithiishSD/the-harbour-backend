"""Download NOAA MarineCadastre daily AIS files and extract them, optionally clipped to a bounding box.

python -m ingest.noaa --date 2023-01-01 --days 3
python -m ingest.noaa --date 2023-01-01 --bbox 33.4,-118.6,33.9,-117.9
"""

import argparse
import csv
import io
import shutil
import urllib.request
import zipfile
from datetime import date, timedelta
from pathlib import Path

from common.config import settings
from common.log import get_logger

BASE_URL = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler"
log = get_logger("ingest.noaa")

BBox = tuple[float, float, float, float]  # min_lat, min_lon, max_lat, max_lon


def file_name(day: date) -> str:
    return f"AIS_{day:%Y_%m_%d}"


def download(day: date, dest: Path, base_url: str = BASE_URL, timeout: float = 60) -> Path:
    """Fetch the day's zip into dest (skipped when already present). Writes .part first, then renames."""
    dest.mkdir(parents=True, exist_ok=True)
    target = dest / f"{file_name(day)}.zip"
    if target.exists():
        return target
    url = f"{base_url}/{day.year}/{file_name(day)}.zip"
    part = target.with_suffix(".zip.part")
    log.info("downloading", extra={"data": {"url": url}})
    with urllib.request.urlopen(url, timeout=timeout) as resp, part.open("wb") as out:  # noqa: S310
        shutil.copyfileobj(resp, out, length=1 << 20)
    part.rename(target)
    return target


def in_bbox(row: dict[str, str], bbox: BBox) -> bool:
    try:
        lat, lon = float(row["LAT"]), float(row["LON"])
    except (KeyError, TypeError, ValueError):
        return False
    return bbox[0] <= lat <= bbox[2] and bbox[1] <= lon <= bbox[3]


def extract(zip_path: Path, out_csv: Path, bbox: BBox | None = None) -> int:
    """Write the CSV inside zip_path to out_csv (rows outside bbox dropped). Returns data rows written."""
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        member = next(n for n in zf.namelist() if n.endswith(".csv"))
        with zf.open(member) as raw, out_csv.open("w", newline="") as out:
            text = io.TextIOWrapper(raw, encoding="utf-8", newline="")
            if bbox is None:
                lines = 0
                for line in text:
                    out.write(line)
                    lines += 1
                return max(lines - 1, 0)
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
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--days", type=int, default=1)
    parser.add_argument("--bbox", help="min_lat,min_lon,max_lat,max_lon")
    parser.add_argument("--keep-zip", action="store_true")
    args = parser.parse_args()
    bbox: BBox | None = None
    if args.bbox:
        parts = [float(p) for p in args.bbox.split(",")]
        bbox = (parts[0], parts[1], parts[2], parts[3])
    root = settings().data_dir / "noaa"
    for offset in range(args.days):
        day = args.date + timedelta(days=offset)
        zip_path = download(day, root / "zip")
        suffix = "_bbox" if bbox else ""
        out = root / "csv" / f"{file_name(day)}{suffix}.csv"
        rows = extract(zip_path, out, bbox)
        log.info("extracted", extra={"data": {"file": str(out), "rows": rows}})
        if not args.keep_zip:
            zip_path.unlink()


if __name__ == "__main__":
    main()
