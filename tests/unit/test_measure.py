from common.schema import normalize
from measure.run import raw_report, summarise


def test_summary_percentiles():
    s = summarise([float(i) for i in range(1, 21)])
    assert s == {"n": 20, "min_s": 1.0, "median_s": 10.5, "p95_s": 20.0, "max_s": 20.0}


def test_probe_report_is_a_valid_position():
    n = normalize(raw_report(371000001, 1_767_225_600_000))
    assert n.reject is None and n.position is not None
    assert n.position["mmsi"] == "371000001" and n.position["ts"] == 1_767_225_600_000
