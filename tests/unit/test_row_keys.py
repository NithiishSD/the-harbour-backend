import pytest

from storage.hbase.tables import port_call_key, port_queue_key, rev, track_key, unrev


def test_reversed_timestamp_sorts_newest_first():
    keys = [track_key("419000123", ts) for ts in (1_000, 5_000, 3_000)]
    assert sorted(keys) == [track_key("419000123", 5_000), track_key("419000123", 3_000), keys[0]]


def test_track_key_fixed_width_and_roundtrip():
    key = track_key("419000123", 1_700_000_000_000)
    assert len(key) == 9 + 19
    assert key.startswith("419000123")
    assert unrev(key[9:]) == 1_700_000_000_000


def test_mmsi_prefix_keeps_ships_apart():
    # One ship's rows never interleave with another ship's rows in key order.
    a = [track_key("200000001", t) for t in (1, 2**40)]
    b = [track_key("200000002", t) for t in (1, 2**40)]
    assert max(a) < min(b)


@pytest.mark.parametrize("bad", ["12345678", "1234567890", "41900012x", ""])
def test_invalid_mmsi_rejected(bad):
    with pytest.raises(ValueError):
        track_key(bad, 1)


def test_port_keys():
    assert port_call_key("uslax", 10, "419000123").startswith("USLAX")
    assert len(port_call_key("USLAX", 10, "419000123")) == 5 + 19 + 9
    assert port_queue_key("SGSIN", 20) < port_queue_key("SGSIN", 10)
    with pytest.raises(ValueError):
        port_queue_key("LA", 1)


def test_negative_timestamp_rejected():
    with pytest.raises(ValueError):
        rev(-1)
