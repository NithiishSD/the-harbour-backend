from pathlib import Path

from common import scopes

SG_BOX = (1.0, 103.0, 2.0, 105.0)
DAY = 86_400_000


def test_record_adds_a_line_only_when_the_subscription_changed(tmp_path: Path):
    assert scopes.record(tmp_path, "SG", ("SGSIN",), (SG_BOX,), first_data_ms=0, now_ms=DAY)
    assert not scopes.record(tmp_path, "SG", ("SGSIN",), (SG_BOX,), now_ms=2 * DAY)
    assert scopes.record(tmp_path, "SG", ("SGSIN",), ((10.0, 10.0, 11.0, 11.0),), now_ms=3 * DAY)
    got = scopes.read(tmp_path)
    assert [e["since"] for e in got] == [0, DAY, 3 * DAY]  # the line for the data before the ledger first
    assert got[0]["boxes"] is None


def test_eras_are_the_spans_the_feed_was_set_to_the_country(tmp_path: Path):
    scopes.record(tmp_path, "SG", ("SGSIN",), (SG_BOX,), first_data_ms=0, now_ms=DAY)
    scopes.record(tmp_path, "NL", ("NLRTM",), ((51.0, 3.0, 52.0, 5.0),), now_ms=3 * DAY)
    scopes.record(tmp_path, "SG", ("SGSIN",), (SG_BOX,), now_ms=6 * DAY)
    entries = scopes.read(tmp_path)
    sg = scopes.eras(entries, "SG")
    assert sg == [(0, 3 * DAY), (6 * DAY, scopes.INF)]  # SG, then NL's time is not its, then SG again
    assert scopes.in_eras(sg, 2 * DAY) and not scopes.in_eras(sg, 4 * DAY) and scopes.in_eras(sg, 9 * DAY)
    assert scopes.eras(entries, "NL") == [(3 * DAY, 6 * DAY)]
    assert scopes.eras(entries, "US") == []  # a country the feed was never set to has no time at all
    assert scopes.overlaps(sg, 2 * DAY, 4 * DAY) and not scopes.overlaps(sg, 4 * DAY, 5 * DAY)


def test_the_same_country_with_other_boxes_is_one_span_and_no_ledger_holds_nothing_back(tmp_path: Path):
    scopes.record(tmp_path, "SG", ("SGSIN",), (SG_BOX,), first_data_ms=0, now_ms=DAY)
    scopes.record(tmp_path, "SG", ("SGSIN",), ((0.0, 0.0, 1.0, 1.0),), now_ms=2 * DAY)
    assert scopes.eras(scopes.read(tmp_path), "SG") == [(0, scopes.INF)]
    assert scopes.eras([], "SG") is None  # nothing to tell one country's data from another's


def test_predicate_keeps_noaa_only_for_the_us_and_live_rows_only_in_the_spans():
    spans = [(10, 20), (30, scopes.INF)]
    us = scopes.predicate(spans, noaa=True)
    assert "source = 'noaa'" in us and "ts >= 10 AND ts < 20" in us and "ts >= 30" in us
    assert "source = 'noaa'" not in scopes.predicate(spans, noaa=False).split(" OR ")[0]
    assert scopes.predicate([], noaa=False) == "FALSE"
