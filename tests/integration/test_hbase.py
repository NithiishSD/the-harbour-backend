import uuid

import pytest

from storage.hbase.client import HBaseClient
from storage.hbase.tables import TABLES, create_tables, track_key

pytestmark = pytest.mark.integration


@pytest.fixture
def client():
    return HBaseClient()


@pytest.fixture
def scratch(client):
    name = f"t_{uuid.uuid4().hex[:8]}"
    client.create_table(name, {"c": 3, "d": 1})
    yield name
    client.drop_table(name)


def test_create_tables_idempotent(client):
    create_tables(client)
    assert create_tables(client) == []
    for name in TABLES:
        assert client.table_exists(name)


def test_put_get_and_missing(client, scratch):
    client.put(scratch, "row1", {"c:a": "1", "d:b": "two", "c:none": None})  # type: ignore[dict-item]
    assert client.get(scratch, "row1") == {"c:a": "1", "d:b": "two"}
    assert client.get(scratch, "nope") is None


def test_versions_capped_and_same_timestamp_overwrites(client, scratch):
    for ts in (100, 200, 300, 400):
        client.put(scratch, "s", {"c:state": f"v{ts}"}, ts)
    client.put(scratch, "s", {"c:state": "v400"}, 400)  # replay of the same event
    versions = client.get_versions(scratch, "s", "c:state", 10)
    assert versions == [(400, "v400"), (300, "v300"), (200, "v200")]  # family keeps 3


def test_prefix_scan_newest_first(client, scratch):
    rows = [(track_key("419000123", ts), {"c:lat": str(ts)}, ts) for ts in (1000, 3000, 2000)]
    rows.append((track_key("419000124", 5000), {"c:lat": "other"}, 5000))
    assert client.put_rows(scratch, rows) == 4
    got = client.scan_prefix(scratch, "419000123", limit=2)
    assert [r["c:lat"] for _, r in got] == ["3000", "2000"]
    assert len(client.scan_all(scratch)) == 4
    assert client.scan_prefix(scratch, "999") == []


def test_keys_with_special_characters(client, scratch):
    client.put(scratch, "a/b c", {"c:x": "ok"})
    assert client.get(scratch, "a/b c") == {"c:x": "ok"}
    client.delete_row(scratch, "a/b c")
    assert client.get(scratch, "a/b c") is None


def test_missing_family_is_added_without_touching_others(client, scratch):
    client.put(scratch, "r", {"c:a": "1"})
    assert client.create_table(scratch, {"c": 3, "d": 1, "e": 2}) is True
    assert client.families(scratch) == {"c": 3, "d": 1, "e": 2}
    assert client.get(scratch, "r") == {"c:a": "1"}
    assert client.create_table(scratch, {"c": 3, "e": 2}) is False


def test_count_rows_pages_through_table(client, scratch):
    client.put_rows(scratch, [(f"k{i:03d}", {"c:a": str(i)}, None) for i in range(25)])
    assert client.count_rows(scratch, "c:a", page=10) == 25
    assert client.count_rows(scratch, "c:a", page=25) == 25
    assert client.count_rows(scratch, "c:a", page=100) == 25
