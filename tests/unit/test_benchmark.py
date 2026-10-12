import pytest

from batch import benchmark as b


def fake_run(counts):
    def run(pattern, cores):
        return {"cores": cores, "seconds": 10.0 / cores} | counts(cores)

    return run


SAME = {k: 1 for k in b.CHECKED}


def test_speedup_is_relative_to_the_fewest_cores(tmp_path, monkeypatch):
    f = tmp_path / "a.csv"
    f.write_text("abc")
    monkeypatch.setattr(b, "run_once", fake_run(lambda c: SAME))
    out = b.benchmark(str(f), [4, 2, 1])
    assert [r["speedup"] for r in out["runs"]] == [4.0, 2.0, 1.0]


def test_runs_with_different_counts_are_rejected(tmp_path, monkeypatch):
    f = tmp_path / "a.csv"
    f.write_text("abc")
    monkeypatch.setattr(b, "run_once", fake_run(lambda c: SAME | {"rows": c}))
    with pytest.raises(SystemExit):
        b.benchmark(str(f), [1, 2])
