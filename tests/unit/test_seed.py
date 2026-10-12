import json
from datetime import UTC, datetime, timedelta

from common.lanes import corridor_boxes
from common.route import lattice
from common.schema import normalize
from dashboard.weather import CYCLONE_WIND_KN, GridFeed
from ingest.seed import FIRST_MMSI, SHIPS, publish_ships, seed, ship_at, static_message, weather_records

NOW = datetime(2026, 10, 10, 6, 30, tzinfo=UTC)


class FakeSender:
    def __init__(self):
        self.sent = []

    def send(self, topic, key, value):
        self.sent.append((topic, key, value))

    def flush(self, timeout=30):
        return 0


def test_ships_sail_in_opposite_directions_along_the_route():
    (la0, lo0, _, _), (la1, lo1, _, _) = ship_at(0, NOW, NOW), ship_at(0, NOW + timedelta(hours=10), NOW)
    assert lo1 != lo0 or la1 != la0
    east_heading, west_heading = ship_at(0, NOW, NOW)[2], ship_at(1, NOW, NOW)[2]
    assert east_heading != west_heading


def test_every_message_passes_the_pipeline_validation():
    sender = FakeSender()
    publish_ships(sender, NOW, NOW, with_static=True)
    assert len(sender.sent) == 2 * SHIPS
    for _, key, envelope in sender.sent:
        result = normalize(envelope)
        assert result.reject is None and not result.ignored
        assert key.isdigit() and int(key) >= FIRST_MMSI
    static = normalize({"source": "aisstream", "payload": static_message(0, NOW)}).static
    assert static["destination"] == "LOS ANGELES" and static["name"].startswith("PACIFIC ARROW")


def test_seed_sends_history_and_weather_and_nothing_in_the_future():
    sender = FakeSender()
    seed(sender, NOW)
    raw = [e for t, _, e in sender.sent if t == "ais.raw"]
    grid = [r for t, _, r in sender.sent if t == "weather.grid"]
    assert len(raw) == SHIPS * (2 + 12) and grid  # 13 reports and one static each, plus the first report
    assert (
        max(normalize(e).position["ts"] for e in raw if normalize(e).position) <= NOW.timestamp() * 1000 + 1
    )


def test_weather_has_one_moving_cyclone_and_calm_water():
    feed = GridFeed()
    for rec in weather_records(NOW, NOW):
        assert feed.add_raw(json.dumps(rec).encode())
    now_ms = int(NOW.timestamp() * 1000)
    at = {h: feed.at(now_ms + h * 3_600_000) for h in range(-6, 25)}
    for points in at.values():
        assert len(points) == len(lattice())
        assert sum(p["cyclone"] for p in points) == 1  # always drawn, once
    assert [p["point_id"] for p in at[0] if p["cyclone"]] == ["SEA-15"]  # east of Luzon
    assert [p["point_id"] for p in at[12] if p["cyclone"]] != ["SEA-15"]  # 12 h later it has moved on
    assert any(p["condition"] == "calm" for p in at[0])
    assert max(p["wind_speed_kn"] for p in at[0]) > CYCLONE_WIND_KN


def test_default_live_boxes_cover_the_seed_ships():
    boxes = corridor_boxes()
    for i in range(SHIPS):
        lat, lon, _, _ = ship_at(i, NOW, NOW)
        assert any(a - 0.5 <= lat <= b + 0.5 and c - 0.5 <= lon <= d + 0.5 for a, c, b, d in boxes), i
