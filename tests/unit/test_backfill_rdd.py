from pathlib import Path

from pyspark.sql import functions as F

from batch.backfill_rdd import backfill, parse_row

FIXTURE = Path(__file__).parent.parent / "fixtures" / "noaa_la_sample.csv"


def write_csv(path: Path, rows: list[str]) -> str:
    header = FIXTURE.read_text().splitlines()[0]
    path.write_text("\n".join([header, *rows]) + "\n")
    return str(path)


def test_parse_row():
    header = FIXTURE.read_text().splitlines()[0].split(",")
    good = FIXTURE.read_text().splitlines()[1].split(",")
    n = parse_row(header, good)
    assert n.position and n.position["mmsi"] == "368126190" and n.position["source"] == "noaa"
    assert n.static and n.static["name"] == "GERONIMO" and n.static["ship_type"] == "tug"
    assert parse_row(header, good[:5]).reject == "wrong number of fields"
    bad = ["91.0" if c == "LAT" else v for c, v in zip(header, good, strict=True)]
    assert parse_row(header, bad).reject == "latitude out of range"


def test_backfill_dedupes_counts_and_writes_history(spark, tmp_path):
    real = FIXTURE.read_text().splitlines()[1:201]
    next_day = real[0].replace("2023-01-01T00:00:04", "2023-01-02T00:00:04")
    bad_lat = real[1].replace("33.73477", "91.0")
    rows = [*real, real[0], real[5], next_day, bad_lat, "123,not,enough"]
    summary = backfill(spark, write_csv(tmp_path / "AIS_test.csv", rows), tmp_path, top=3)

    assert summary["rows"] == 205
    assert summary["rejects"] == {"latitude out of range": 1, "wrong number of fields": 1}
    assert summary["positions"] == 203
    assert summary["duplicates"] == 2
    assert summary["unique_positions"] == 201
    counts = {
        line.split("\t")[0]: int(line.split("\t")[1])
        for f in (tmp_path / "batch" / "ship_counts" / "AIS_test").glob("part-*")
        for line in f.read_text().splitlines()
    }
    assert sum(counts.values()) == 201 and len(counts) == summary["ships"]
    assert summary["busiest"][0] == max(counts.items(), key=lambda kv: (kv[1], kv[0]))  # largest first

    history = tmp_path / "history" / "positions"
    df = spark.read.parquet(str(history))
    assert df.count() == 201
    assert {r.date.isoformat(): r["count"] for r in df.groupBy("date").count().collect()} == {
        "2023-01-01": 200,
        "2023-01-02": 1,
    }
    assert df.select("mmsi", "ts").distinct().count() == 201

    # Ship details: the newest row per ship per day.
    vessels = spark.read.parquet(str(tmp_path / "history" / "vessels"))
    ships_day1 = {r.split(",")[0] for r in real}
    assert summary["vessel_days"] == vessels.count() == len(ships_day1) + 1
    newest = df.where("date = '2023-01-01'").groupBy("mmsi").agg(F.max("ts").alias("ts"))
    day1 = vessels.where("date = '2023-01-01'").select("mmsi", "ts")
    assert day1.exceptAll(newest).count() == 0 and newest.exceptAll(day1).count() == 0

    # Re-running only 2023-01-02 replaces that date and leaves 2023-01-01 alone.
    backfill(spark, write_csv(tmp_path / "AIS_day2.csv", [next_day, next_day]), tmp_path)
    df = spark.read.parquet(str(history))
    assert df.count() == 201
    # Re-running the whole input gives the same history (no appended copies).
    backfill(spark, str(tmp_path / "AIS_test.csv"), tmp_path)
    assert spark.read.parquet(str(history)).count() == 201
