import json

import pandas as pd

from streaming.job import NORMALIZED_COLUMNS, STATIC_FIELDS, normalize_batches
from tests.samples import aisstream_position, aisstream_static, envelope, noaa_row


def run(values):
    pdf = pd.DataFrame({"value": values, "kafka_ts": [pd.Timestamp("2023-01-01", tz="UTC")] * len(values)})
    return pd.concat(list(normalize_batches(iter([pdf]))))


def test_one_row_per_record_kind():
    out = run(
        [
            json.dumps(envelope(aisstream_position())),
            json.dumps(envelope(aisstream_static())),
            json.dumps(envelope(noaa_row(), source="noaa")),  # position + static
            json.dumps(envelope(aisstream_position(lat=95))),
            "{not json",
            json.dumps(envelope({"MessageType": "BaseStationReport", "Message": {}})),  # ignored: no row
        ]
    )
    assert list(out.columns) == NORMALIZED_COLUMNS
    assert out["kind"].tolist() == ["position", "static", "position", "static", "reject", "reject"]
    assert out.loc[out["kind"] == "reject", "reason"].tolist() == ["latitude out of range", "not json"]
    assert out.loc[out["kind"] == "reject", "raw"].iloc[1] == "{not json"
    static = out[out["kind"] == "static"].iloc[0]
    assert static["destination"] == "SINGAPORE"  # declared destination travels with the ship details
    assert set(STATIC_FIELDS) <= set(NORMALIZED_COLUMNS)  # every static field is in the normalized rows


def test_nullable_integer_columns():
    out = run([json.dumps(envelope(aisstream_position(kind="StandardClassBPositionReport")))])
    assert str(out["nav_status"].dtype) == "Int32"
    assert out["nav_status"].isna().all()
    assert str(out["ts"].dtype) == "Int64"


def test_empty_batch():
    out = run([])
    assert out.empty and list(out.columns) == NORMALIZED_COLUMNS


def test_empty_string_columns_are_none_not_nan():
    # Arrow refuses float NaN in a string column; Spark then fails the whole micro-batch.
    out = run([json.dumps(envelope(aisstream_position()))])
    for col in ("name", "ship_type", "imo", "callsign", "destination", "reason", "raw"):
        assert out[col].tolist() == [None], col


def test_invalid_utf8_bytes_are_rejected_not_fatal():
    out = run([b"\xff not json", json.dumps(envelope(aisstream_position())).encode()])
    assert out["kind"].tolist() == ["reject", "position"]
    assert out["raw"].iloc[0] == "\ufffd not json"


def test_offsets_map_is_cut_to_subscribed_topics():
    from streaming.job import JobConfig, starting_offsets

    both = '{"ais.positions": {"0": 5}, "ais.static": {"0": 7}}'
    cfg = JobConfig(starting_offsets={"ais.positions": both})
    assert json.loads(starting_offsets(cfg, "ais.positions")) == {"ais.positions": {"0": 5}}
    assert starting_offsets(JobConfig(), "ais.raw") == "earliest"
    assert (
        starting_offsets(JobConfig(starting_offsets={"ais.raw": "latest"}), "ais.raw,weather.marine")
        == "latest"
    )
