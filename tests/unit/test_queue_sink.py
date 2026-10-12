from common import topics
from storage.hbase.tables import PORT_QUEUE, port_queue_key
from streaming.sinks import WINDOW_MS, handle_queue, queue_alerts, queue_puts

M = 60_000


def window(end, ships, port="USLGB"):
    return {"port_id": port, "window_start": end - WINDOW_MS, "window_end": end, "ships": ships}


def test_queue_rows_keyed_by_port_and_reversed_window_end():
    ((key, cells, ts),) = queue_puts([window(60 * M, 7)])
    assert key == port_queue_key("USLGB", 60 * M) and ts == 60 * M
    assert cells == {"q:window_start": str(30 * M), "q:window_end": str(60 * M), "q:ships": "7"}


def test_growth_alert_needs_both_percent_and_absolute_rise():
    prev = {("USLGB", 30 * M): 10, ("USLAX", 30 * M): 2, ("SGSIN", 30 * M): 10}
    rows = [
        window(60 * M, 15),
        window(60 * M, 6, "USLAX"),
        window(60 * M, 14, "SGSIN"),
        window(90 * M, 50, "USLGB"),
    ]
    alerts = queue_alerts(rows, lambda p, e: prev.get((p, e)), pct=50, min_ships=5, now_ms=0)
    # USLGB +5 (+50%) fires; USLAX +4 is under 5 ships; SGSIN +40% is under 50%; no window before 90 min.
    assert [(a["port_id"], a["details"]["growth_pct"]) for a in alerts] == [("USLGB", 50.0)]
    assert alerts[0]["type"] == "queue_growth" and alerts[0]["details"]["previous"] == 10


class FakeHBase:
    def __init__(self, rows=None):
        self.rows = rows or {}
        self.puts = []

    def put_rows(self, table, rows):
        rows = list(rows)
        self.puts += rows
        return len(rows)

    def get(self, table, key):
        return self.rows.get(key)


class FakeSender:
    def __init__(self):
        self.sent = []

    def send(self, topic, key, value):
        self.sent.append((topic, key, value))

    def flush(self, timeout=30):
        return 0


def test_previous_window_from_same_batch_or_hbase():
    stored = {port_queue_key("USLAX", 30 * M): {"q:ships": "2"}}
    hb, out = FakeHBase(stored), FakeSender()
    n = handle_queue([window(30 * M, 3), window(60 * M, 12), window(60 * M, 9, "USLAX")], out, hb, 50, 5)
    assert n == 2
    assert {(t, v["port_id"]) for t, _, v in out.sent} == {(topics.ALERTS, "USLGB"), (topics.ALERTS, "USLAX")}
    assert len(hb.puts) == 3 and PORT_QUEUE
