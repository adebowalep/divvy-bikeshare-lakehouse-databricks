"""Tests of the real PySpark transformations in src/divvy_transforms.py, on small synthetic fixtures.

Run locally with:  pytest tests/ -v      (needs pyspark and delta-spark; no Azure access)
"""
import datetime as dt
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

import divvy_transforms as T

D = dt.date


def ts(s):
    return dt.datetime.fromisoformat(s)


# ------------------------------------------------------------------------------------------ age
AGE_CASES = [
    # (label, event, birth, expected)
    ("birthday already occurred this year", D(2021, 6, 15), D(1990, 3, 10), 31),
    ("birthday not yet occurred this year", D(2021, 2, 1), D(1990, 3, 10), 30),
    ("event exactly on birthday", D(2021, 3, 10), D(1990, 3, 10), 31),
    ("day before birthday", D(2021, 3, 9), D(1990, 3, 10), 30),
    ("leap-day birthday, leap year, day before (28 Feb)", D(2024, 2, 28), D(2000, 2, 29), 23),
    ("leap-day birthday, leap year, on 29 Feb", D(2024, 2, 29), D(2000, 2, 29), 24),
    ("leap-day birthday, non-leap year, 27 Feb", D(2021, 2, 27), D(2000, 2, 29), 20),
    ("leap-day birthday, non-leap year, 28 Feb counts as the birthday", D(2021, 2, 28), D(2000, 2, 29), 21),
    ("leap-day birthday, non-leap year, 1 Mar", D(2021, 3, 1), D(2000, 2, 29), 21),
    ("28 Feb birthday is unaffected in a leap year", D(2024, 2, 28), D(2000, 2, 28), 24),
    ("missing birth date", D(2021, 6, 15), None, None),
    ("missing event date", None, D(1990, 3, 10), None),
    ("event before birth", D(1990, 1, 1), D(1995, 1, 1), None),
    ("event on the birth date", D(1995, 1, 1), D(1995, 1, 1), 0),
]


@pytest.mark.parametrize("label,event,birth,expected", AGE_CASES, ids=[c[0] for c in AGE_CASES])
def test_age_years(spark, label, event, birth, expected):
    df = spark.createDataFrame([(event, birth)], "event date, birth date")
    got = df.select(T.age_years("event", "birth").alias("a")).first()["a"]
    assert got == expected


# ----------------------------------------------------------------------------------- durations
DURATION_CASES = [(-5, "NEGATIVE"), (0, "ZERO"), (1, "OK"), (600, "OK"), (86400, "OK"),
                  (86401, "OVER_24H"), (400000, "OVER_24H"), (None, "MISSING")]


@pytest.mark.parametrize("secs,expected", DURATION_CASES)
def test_duration_quality(spark, secs, expected):
    df = spark.createDataFrame([(secs,)], "s long")
    assert df.select(T.duration_quality(F.col("s")).alias("q")).first()["q"] == expected


def test_duration_categories_are_exhaustive(spark):
    df = spark.createDataFrame([(c,) for c, _ in DURATION_CASES], "s long")
    got = {r["q"] for r in df.select(T.duration_quality(F.col("s")).alias("q")).collect()}
    assert got == set(T.DURATION_CATEGORIES)


def test_time_band(spark):
    df = spark.createDataFrame([(h,) for h in (0, 5, 6, 11, 12, 17, 18, 23)], "h int")
    got = [r["b"] for r in df.select(T.time_band(F.col("h")).alias("b")).collect()]
    assert got == ["Night (00-05)", "Night (00-05)", "Morning (06-11)", "Morning (06-11)",
                   "Afternoon (12-17)", "Afternoon (12-17)", "Evening (18-23)", "Evening (18-23)"]


# -------------------------------------------------------------------------------- fixtures
@pytest.fixture()
def bronze(spark):
    rider = spark.createDataFrame([
        (1000, D(1989, 2, 13), D(2019, 4, 23), None, True),
        (1001, D(2000, 2, 29), D(2021, 2, 28), None, False),     # leap-day birthday
        (1002, D(1998, 8, 10), D(2022, 2, 4), D(2022, 6, 1), True),
        (1002, D(1998, 8, 10), D(2021, 1, 1), None, True),       # duplicate id: later start wins
    ], "rider_id int, birthday date, account_start_date date, account_end_date date, is_member boolean")
    station = spark.createDataFrame([("S1", "Alpha St", 41.1, -87.1), ("KA2", "Beta Ave", 41.2, -87.2)],
                                    "station_id string, name string, latitude double, longitude double")
    trip = spark.createDataFrame([
        ("t_ok", "classic_bike", ts("2021-02-12 16:14:56"), ts("2021-02-12 16:21:43"), "S1", "KA2", 1000),
        ("t_neg", "classic_bike", ts("2021-02-12 10:00:00"), ts("2021-02-12 09:00:00"), "S1", "S1", 1000),
        ("t_zero", "classic_bike", ts("2021-02-12 10:00:00"), ts("2021-02-12 10:00:00"), "S1", "S1", 1001),
        ("t_24h", "classic_bike", ts("2021-02-12 10:00:00"), ts("2021-02-13 10:00:00"), "S1", "S1", 1001),
        ("t_over", "electric_bike", ts("2021-02-12 10:00:00"), ts("2021-02-13 10:00:01"), "S1", "KA2", 1002),
        ("t_nostation", "electric_bike", ts("2021-02-28 08:00:00"), ts("2021-02-28 08:10:00"), "UNKNOWN", None, 1001),
        ("t_norider", "classic_bike", ts("2021-03-01 08:00:00"), ts("2021-03-01 08:05:00"), "S1", "S1", 9999),
    ], "trip_id string, rideable_type string, started_at timestamp, ended_at timestamp, "
       "start_station_id string, end_station_id string, rider_id int")
    payment = spark.createDataFrame([
        (1, D(2019, 5, 1), Decimal("9.00"), 1000), (2, D(2019, 6, 1), Decimal("9.00"), 1000),
        (3, D(2021, 3, 1), Decimal("12.50"), 1001), (4, D(2021, 3, 1), Decimal("3.25"), 8888),
    ], "payment_id int, date date, amount decimal(10,2), rider_id int")
    return rider, station, trip, payment


@pytest.fixture()
def gold(bronze):
    rider, station, trip, payment = bronze
    dim_rider, _ = T.build_dim_rider(rider)
    dim_station, _ = T.build_dim_station(station)
    return dict(dim_rider=dim_rider, dim_station=dim_station, dim_date=T.build_dim_date(trip, payment),
                fact_trip=T.build_fact_trip(trip, dim_rider, dim_station),
                fact_payment=T.build_fact_payment(payment, dim_rider))


# --------------------------------------------------------------------------- fact_trip rules
def test_fact_trip_duration_quality_per_trip(gold):
    got = {r["trip_id"]: r["duration_quality"] for r in gold["fact_trip"].collect()}
    assert got == {"t_ok": "OK", "t_neg": "NEGATIVE", "t_zero": "ZERO", "t_24h": "OK",
                   "t_over": "OVER_24H", "t_nostation": "OK", "t_norider": "OK"}


def test_fact_trip_keeps_every_trip_and_keys_stay_unique(bronze, gold):
    ft = gold["fact_trip"]
    assert ft.count() == bronze[2].count() == 7
    assert ft.select("trip_id").distinct().count() == 7           # joins did not multiply rows


def test_fact_trip_unmatched_references_are_null_not_dropped(gold):
    rows = {r["trip_id"]: r for r in gold["fact_trip"].collect()}
    assert rows["t_nostation"]["start_station_key"] is None        # station id not in the station list
    assert rows["t_nostation"]["end_station_key"] is None          # NULL end station id
    assert rows["t_norider"]["rider_key"] is None and rows["t_norider"]["rider_age_at_trip"] is None
    assert rows["t_ok"]["start_station_key"] is not None and rows["t_ok"]["rider_key"] is not None


def test_fact_trip_duration_values(gold):
    r = {x["trip_id"]: x for x in gold["fact_trip"].collect()}
    assert r["t_ok"]["ride_duration_seconds"] == 407
    assert r["t_ok"]["ride_duration_minutes"] == pytest.approx(6.783333, abs=1e-5)
    assert r["t_neg"]["ride_duration_seconds"] == -3600


def test_fact_trip_age_uses_ride_start_date_and_leap_rule(gold):
    r = {x["trip_id"]: x for x in gold["fact_trip"].collect()}
    assert r["t_ok"]["rider_age_at_trip"] == 31                    # born 1989-02-13, ride 2021-02-12: birthday not yet
    assert r["t_nostation"]["rider_age_at_trip"] == 21             # born 2000-02-29, ride 2021-02-28: counts as birthday


def test_valid_trip_count_matches_category_arithmetic(gold):
    ft = gold["fact_trip"]
    by = {r["q"]: r["n"] for r in ft.groupBy(F.col("duration_quality").alias("q")).count().withColumnRenamed("count", "n").collect()}
    assert sum(by.values()) == ft.count()
    assert by["OK"] == ft.count() - sum(v for k, v in by.items() if k != "OK")


# ------------------------------------------------------------------ dimensions and date keys
def test_dim_rider_dedupes_and_keys_are_unique_and_deterministic(bronze):
    rider = bronze[0]
    d1, conflicts = T.build_dim_rider(rider)
    d2, _ = T.build_dim_rider(rider)
    assert conflicts == 1                                          # rider 1002 appears twice with different data
    assert d1.count() == 3 and d1.select("rider_key").distinct().count() == 3
    assert [(r["rider_id"], r["rider_key"]) for r in d1.orderBy("rider_id").collect()] == [(1000, 1), (1001, 2), (1002, 3)]
    assert T.table_fingerprint(d1) == T.table_fingerprint(d2)
    kept = d1.filter("rider_id = 1002").first()
    assert kept["account_start_date"] == D(2022, 2, 4)             # the later account start wins the tie-break


def test_dim_rider_age_at_account_start_leap_rule(gold):
    ages = {r["rider_id"]: r["age_at_account_start"] for r in gold["dim_rider"].collect()}
    assert ages == {1000: 30, 1001: 21, 1002: 23}


def test_dim_date_is_contiguous_and_keys_are_yyyymmdd(gold):
    dd = gold["dim_date"]
    lo, hi, n = dd.agg(F.min("date"), F.max("date"), F.count(F.lit(1))).first()
    assert (hi - lo).days + 1 == n
    assert dd.select("date_key").distinct().count() == n
    row = dd.filter("date = DATE'2021-02-12'").first()
    assert row["date_key"] == 20210212 and row["day_of_week_name"] == "Friday" and row["day_of_week_num"] == 5
    assert dd.filter("date = DATE'2021-02-13'").first()["is_weekend"] is True


def test_every_fact_date_key_exists_in_dim_date(gold):
    keys = gold["dim_date"].select(F.col("date_key").alias("k"))
    ft, fp = gold["fact_trip"], gold["fact_payment"]
    assert ft.join(keys, ft.start_date_key == keys.k, "left_anti").count() == 0
    assert ft.join(keys, ft.end_date_key == keys.k, "left_anti").count() == 0
    assert fp.join(keys, fp.payment_date_key == keys.k, "left_anti").count() == 0


# -------------------------------------------------------------------------------- payments
def test_payment_amounts_reconcile_and_no_row_multiplication(bronze, gold):
    payment = bronze[3]
    fp = gold["fact_payment"]
    assert fp.count() == payment.count()
    assert fp.agg(F.sum("amount")).first()[0] == payment.agg(F.sum("amount")).first()[0] == Decimal("33.75")
    assert fp.filter("rider_key IS NULL").count() == 1             # payment of unknown rider 8888 is kept, key NULL


# ------------------------------------------------------------------------ repeatable writes
def test_delta_overwrite_rebuild_gives_identical_fingerprint(spark, gold, tmp_path):
    pytest.importorskip("delta")
    path = str(tmp_path / "fact_trip")
    fps = []
    for _ in range(2):                                             # build and overwrite twice
        gold["fact_trip"].write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
        fps.append(T.table_fingerprint(spark.read.format("delta").load(path)))
    assert fps[0] == fps[1] and fps[0][0] == 7
    assert spark.read.format("delta").load(path).count() == 7      # overwrite did not append


def test_fingerprint_detects_a_content_change_with_equal_count(spark):
    a = spark.createDataFrame([(1, "x"), (2, "y")], "k int, v string")
    b = spark.createDataFrame([(1, "x"), (2, "z")], "k int, v string")
    assert a.count() == b.count() and T.table_fingerprint(a) != T.table_fingerprint(b)
