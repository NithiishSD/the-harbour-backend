import pytest

from common.schema import Invalid, key_of, normalize, parse_time, ship_type
from tests.samples import aisstream_position, aisstream_static, envelope, noaa_row

T0 = 1_672_563_600_000  # 2023-01-01 09:00:00 UTC


def test_aisstream_position_maps_to_schema():
    n = normalize(envelope(aisstream_position(ts_ms=T0 + 318)))
    assert n.reject is None and n.static is None
    assert n.position == {
        "mmsi": "419000123",
        "ts": T0 + 318,
        "lat": 1.25,
        "lon": 103.9,
        "sog": 0.1,
        "cog": 308.0,
        "heading": 235,
        "nav_status": 1,
        "source": "aisstream",
    }


def test_class_b_position_has_no_nav_status():
    n = normalize(envelope(aisstream_position(kind="StandardClassBPositionReport")))
    assert n.position is not None and n.position["nav_status"] is None


def test_aisstream_static_maps_and_cleans_name():
    n = normalize(envelope(aisstream_static()))
    assert n.static is not None
    assert n.static["name"] == "TEST TANKER"
    assert n.static["ship_type"] == "tanker"
    assert n.static["length_m"] == 183
    assert n.static["draught_m"] == 11.2
    assert n.static["imo"] == "9123456"


def test_noaa_row_gives_position_and_static():
    n = normalize(envelope(noaa_row(), source="noaa"))
    assert n.position is not None and n.static is not None
    assert n.position["ts"] == T0
    assert n.position["heading"] is None  # 511 = not available
    assert n.position["nav_status"] == 1
    assert n.static["ship_type"] == "cargo"
    assert n.static["imo"] == "9000001"


def test_unused_message_type_is_ignored_not_rejected():
    n = normalize(envelope({"MessageType": "BaseStationReport", "Message": {}, "MetaData": {}}))
    assert n.ignored and n.reject is None


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"lat": 91.0}, "latitude"),
        ({"lat": -90.5}, "latitude"),
        ({"lon": 181.0}, "longitude"),
        ({"mmsi": 12345678}, "mmsi"),
        ({"mmsi": 1234567890}, "mmsi"),
        ({"sog": 75.0}, "speed"),
    ],
)
def test_validation_rejects(change, reason):
    n = normalize(envelope(aisstream_position(**change)))
    assert n.position is None
    assert n.reject is not None and reason in n.reject


def test_sog_not_available_becomes_null():
    n = normalize(envelope(aisstream_position(sog=102.3)))
    assert n.position is not None and n.position["sog"] is None


def test_boundaries_are_valid():
    n = normalize(envelope(aisstream_position(lat=-90.0, lon=180.0, sog=59.9)))
    assert n.position is not None


@pytest.mark.parametrize(
    "bad",
    [
        None,
        "nope",
        {"source": "noaa"},
        {"source": "x", "payload": {}},
        {"source": "noaa", "payload": {"MMSI": "367000001", "BaseDateTime": "yesterday"}},
        {"source": "noaa", "payload": {**noaa_row(), "LAT": "abc"}},
        {"source": "aisstream", "payload": {"MessageType": "PositionReport", "Message": {}}},
    ],
)
def test_garbage_is_rejected_never_raises(bad):
    n = normalize(bad)
    assert n.reject


def test_parse_time_formats():
    assert parse_time("2023-01-01 09:00:00.5 +0000 UTC") == T0 + 500
    assert parse_time("2023-01-01T09:00:00") == T0
    assert parse_time("2023-01-01 09:00:00.123456789 +0000 UTC") == T0 + 123


def test_a_timestamp_far_in_the_future_is_rejected():
    with pytest.raises(Invalid, match="future"):
        parse_time("2238-09-03 19:57:00 +0000 UTC")


def test_live_positions_more_than_10_minutes_ahead_are_rejected():
    # Live envelope with time_utc 15 minutes ahead of received_at processing time
    msg = aisstream_position()
    msg["MetaData"]["time_utc"] = "2026-10-17 12:15:00.000000 +0000 UTC"
    env = envelope(msg, source="aisstream")
    env["received_at"] = "2026-10-17T12:00:00Z"
    result = normalize(env)
    assert result.reject is not None and "future" in result.reject

    # Live envelope within 5 minutes of received_at processing time is accepted
    msg2 = aisstream_position()
    msg2["MetaData"]["time_utc"] = "2026-10-17 12:05:00.000000 +0000 UTC"
    env2 = envelope(msg2, source="aisstream")
    env2["received_at"] = "2026-10-17T12:00:00Z"
    result2 = normalize(env2)
    assert result2.reject is None
    assert result2.position is not None


def test_replay_positions_exempt_from_10_minute_guard():
    # Historical NOAA replay is from 2023, far in the past relative to wall-clock now, but accepted
    row = noaa_row()
    row["BaseDateTime"] = "2023-01-01T09:00:00"
    env = envelope(row, source="noaa")
    result = normalize(env)
    assert result.reject is None
    assert result.position is not None


@pytest.mark.parametrize(
    ("code", "expected"),
    [(30, "fishing"), (52, "tug"), (65, "passenger"), (70, "cargo"), (89, "tanker"), (0, "unknown"),
     (None, "unknown"), ("", "unknown"), (99, "other"), ("80.0", "tanker")],
)  # fmt: skip
def test_ship_type_code_list(code, expected):
    assert ship_type(code) == expected


def test_kafka_key():
    assert key_of(envelope(aisstream_position())) == "419000123"
    assert key_of(envelope(noaa_row(), source="noaa")) == "367000001"
    assert key_of({"source": "noaa", "payload": {}}) is None
