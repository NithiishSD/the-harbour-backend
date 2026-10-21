import csv
import threading
import zipfile
from datetime import date
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
import zstandard

from ingest.noaa import download, extract, file_name

FIXTURE = Path(__file__).parent.parent / "fixtures" / "noaa_la_sample.csv"
LA_BBOX = (33.70, -118.30, 33.80, -118.20)


def make_zip(tmp_path: Path) -> Path:
    zip_path = tmp_path / "AIS_2023_01_01.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(FIXTURE, "AIS_2023_01_01.csv")
    return zip_path


def make_zst(tmp_path: Path) -> Path:
    zst_path = tmp_path / "ais-2023-01-01.csv.zst"
    cctx = zstandard.ZstdCompressor()
    zst_path.write_bytes(cctx.compress(FIXTURE.read_bytes()))
    return zst_path


def test_fixture_is_real_noaa_layout():
    with FIXTURE.open() as f:
        header = next(csv.reader(f))
    assert header[:6] == ["MMSI", "BaseDateTime", "LAT", "LON", "SOG", "COG"]
    assert "TransceiverClass" in header


def test_extract_full(tmp_path):
    out = tmp_path / "out.csv"
    rows = extract(make_zip(tmp_path), out)
    assert rows == sum(1 for _ in FIXTURE.open()) - 1
    assert out.read_text() == FIXTURE.read_text()


def test_extract_zst_full(tmp_path):
    out = tmp_path / "out.csv"
    rows = extract(make_zst(tmp_path), out)
    assert rows == sum(1 for _ in FIXTURE.open()) - 1
    assert out.read_text() == FIXTURE.read_text()


def test_extract_bbox_filters_rows(tmp_path):
    out = tmp_path / "out.csv"
    rows = extract(make_zip(tmp_path), out, LA_BBOX)
    kept = list(csv.DictReader(out.open()))
    assert 0 < rows == len(kept) < sum(1 for _ in FIXTURE.open()) - 1
    for r in kept:
        assert 33.70 <= float(r["LAT"]) <= 33.80 and -118.30 <= float(r["LON"]) <= -118.20


@pytest.fixture
def file_server(tmp_path):
    year = tmp_path / "srv" / "2023"
    year.mkdir(parents=True)
    make_zip(year)
    handler = partial(SimpleHTTPRequestHandler, directory=str(tmp_path / "srv"))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


def test_download_then_skip_when_present(tmp_path, file_server):
    dest = tmp_path / "zips"
    path = download(date(2023, 1, 1), dest, base_url=file_server)
    assert path.name == f"{file_name(date(2023, 1, 1))}.zip"
    assert zipfile.is_zipfile(path)
    assert not list(dest.glob("*.part"))
    mtime = path.stat().st_mtime_ns
    assert download(date(2023, 1, 1), dest, base_url="http://127.0.0.1:9") == path  # no network needed
    assert path.stat().st_mtime_ns == mtime


def test_download_missing_day_raises(tmp_path, file_server):
    with pytest.raises(OSError):
        download(date(2023, 1, 2), tmp_path / "zips", base_url=file_server)
    assert not list((tmp_path / "zips").glob("*"))
