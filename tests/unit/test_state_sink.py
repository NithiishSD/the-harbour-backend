from common import topics
from storage.hbase.tables import PORT_CALLS, VESSEL_STATE, port_call_key
from streaming.sinks import event_id, handle_events, state_puts

H = 3_600_000


class FakeHBase:
    def __init__(self):
        self.tables = {}

    def put_rows(self, table, rows):
        rows = list(rows)
        for key, cells, _ in rows:
            self.tables.setdefault(table, {}).setdefault(key, {}).update(cells)
        return len(rows)

    def scan_all(self, table, limit=10000):
        return sorted(self.tables.get(table, {}).items())


class FakeSender:
    def __init__(self):
        self.sent = []

    def send(self, topic, key, value):
        self.sent.append((topic, key, value))

    def flush(self, timeout=30):
        return 0


def ev(kind, mmsi="419000001", ts=0, port="USLGB", **data):
    return {"type": kind, "mmsi": mmsi, "ts": ts, "port_id": port, "lat": 33.74, "lon": -118.16, "data": data}


def last(mmsi, ts, area="USLGB"):
    return ev("last", mmsi, ts, area, sog=0.1, cog=181.5, heading=180.0, state="anchored")


def test_state_and_last_rows_and_port_call_key():
    events = [
        ev("state", ts=5, state="anchored", since=5),
        last("419000001", 9),
        ev("port_call", ts=7, arrival_at=5, anchored_at=5, berthed_at=7, departed_at=None, wait_hours=2.0),
    ]
    state_rows, call_rows = state_puts(events)
    assert state_rows[0] == ("419000001", {"cur:state": "anchored", "cur:port": "USLGB", "cur:since": "5"}, 5)
    assert state_rows[1][1]["last:ts"] == "9" and state_rows[1][1]["last:area"] == "USLGB"
    assert state_rows[1][1]["last:cog"] == "181.5" and state_rows[1][1]["last:heading"] == "180.0"
    key, cells, _ = call_rows[0]
    assert key == port_call_key("USLGB", 5, "419000001")
    assert cells == {"call:anchored_at": "5", "call:berthed_at": "7", "call:wait_hours": "2.0"}


def test_dark_candidate_becomes_alert_when_neighbours_report():
    hb, out = FakeHBase(), FakeSender()
    neighbours = [last(f"41900001{i}", 3 * H) for i in range(3)]
    counts = handle_events(
        [*neighbours, ev("dark_candidate", ts=H, state="anchored")], out, hb, 6 * H, 3, now_ms=0
    )
    assert counts["alerts"] == 1
    topic, key, alert = out.sent[-1]
    assert (topic, key, alert["type"], alert["mmsi"]) == (
        topics.ALERTS,
        "dark_ship",
        "dark_ship",
        "419000001",
    )
    assert alert["details"]["active_ships"] == 3
    assert alert["alert_id"] == event_id("dark_ship", "419000001", H)


def test_dark_candidate_is_coverage_gap_when_area_quiet():
    hb, out = FakeHBase(), FakeSender()
    others = [last("419000011", H - 1), last("419000012", 3 * H, area="USLAX"), last("419000013", 900 * H)]
    handle_events([*others, ev("dark_candidate", ts=H, state="anchored")], out, hb, 6 * H, 1)
    topic, _, gap = out.sent[-1]
    assert topic == topics.PORT_EVENTS and gap["type"] == "coverage_gap"
    assert gap["details"]["active_ships"] == 0  # older, other area, and far-future ships do not count


def test_port_events_only_for_port_states_and_reappearance():
    hb, out = FakeHBase(), FakeSender()
    events = [
        ev("state", ts=1, state="underway", since=1),
        ev("state", ts=2, state="anchored", since=2),
        ev("reappeared", ts=3, silent_hours=7.0),
    ]
    handle_events(events, out, hb, 6 * H, 3)
    assert [v["type"] for t, _, v in out.sent if t == topics.PORT_EVENTS] == ["anchored", "reappeared"]
    assert hb.tables[VESSEL_STATE]["419000001"]["cur:state"] == "anchored"
    assert PORT_CALLS not in hb.tables
