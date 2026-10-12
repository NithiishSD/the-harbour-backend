import json
import threading
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import pytest

from common import topics
from common.geo import Port
from ingest.weather import fetch_grid, publish, publish_grid

LAX = Port("USLAX", "Los Angeles", "US", 33.729, -118.262)
SIN = Port("SGSIN", "Singapore", "SG", 1.264, 103.84)


class FakeSender:
    def __init__(self):
        self.sent = []

    def send(self, topic, key, value):
        self.sent.append((topic, key, value))

    def flush(self, timeout=30):
        return 0


@pytest.fixture
def open_meteo():
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            url = urlparse(self.path)
            q = {k: v[0] for k, v in parse_qs(url.query).items()}
            calls.append((url.path, q))
            if q["latitude"].startswith("1."):  # Singapore: simulate an outage
                self.send_response(500)
                self.end_headers()
                return
            times = ["2023-01-01T00:00", "2023-01-01T01:00"]
            field = q["hourly"]
            values = [0.9, 1.1] if field == "wave_height" else [5.0, None]
            body = json.dumps({"hourly": {"time": times, field: values}}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    yield (f"{base}/marine", f"{base}/forecast", f"{base}/archive"), calls
    server.shutdown()


def test_historical_day_uses_archive_and_knots(open_meteo):
    urls, calls = open_meteo
    sender = FakeSender()
    assert publish([LAX], sender, date(2023, 1, 1), urls=urls, timeout=5) == 2
    assert [path for path, _ in calls] == ["/marine", "/archive"]
    assert calls[1][1]["wind_speed_unit"] == "kn"
    assert calls[0][1]["start_date"] == "2023-01-01"
    topic, key, rec = sender.sent[0]
    assert (topic, key) == (topics.WEATHER, "USLAX")
    assert rec == {
        "port_id": "USLAX",
        "hour": "2023-01-01T00:00:00Z",
        "wind_speed_kn": 5.0,
        "wave_height_m": 0.9,
    }
    assert sender.sent[1][2]["wind_speed_kn"] is None


def test_live_uses_forecast_and_one_failing_port_does_not_stop_others(open_meteo):
    urls, calls = open_meteo
    sender = FakeSender()
    assert publish([SIN, LAX], sender, None, urls=urls, timeout=5) == 2
    assert {key for _, key, _ in sender.sent} == {"USLAX"}
    assert calls[-1][0] == "/forecast"
    assert calls[-1][1]["past_days"] == "1"


def test_grid_fetch_asks_once_for_all_points_and_keeps_their_order():
    asked = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            q = {k: v[0] for k, v in parse_qs(urlparse(self.path).query).items()}
            asked.append(q)
            n = len(q["latitude"].split(","))
            field = q["hourly"]
            one = {"hourly": {"time": ["2026-10-10T00:00"], field: [3.0]}}
            body = json.dumps([one] * n if n > 1 else one).encode()
            self.send_response(200)
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_address[1]}"
    sender = FakeSender()
    points = [("SEA-00", 5.0, 108.0), ("SEA-01", 12.0, 113.5)]
    try:
        assert publish_grid(sender, urls=(f"{base}/marine", f"{base}/forecast", ""), timeout=5) > 0
        asked.clear()
        records = fetch_grid(points, urls=(f"{base}/marine", f"{base}/forecast", ""), timeout=5)
    finally:
        server.shutdown()
    assert len(asked) == 2 and asked[0]["latitude"] == "5.0,12.0" and asked[1]["wind_speed_unit"] == "kn"
    assert records[1] == {
        "point_id": "SEA-01",
        "lat": 12.0,
        "lon": 113.5,
        "hour": "2026-10-10T00:00:00Z",
        "wind_speed_kn": 3.0,
        "wave_height_m": 3.0,
    }
    assert {t for t, _, _ in sender.sent} == {topics.WEATHER_GRID}


def test_grid_outage_is_logged_and_publishes_nothing():
    sender = FakeSender()
    assert publish_grid(sender, urls=("http://127.0.0.1:9/m", "http://127.0.0.1:9/f", ""), timeout=1) == 0
    assert sender.sent == []
