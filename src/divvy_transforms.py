"""PySpark transformations for the Divvy lakehouse (Bronze -> Gold).

This file is the single source of truth for the transformation logic. `tools/build_notebook.py`
copies the marked regions below, unchanged, into the Databricks notebook cells, and the tests in
`tests/` import this module, so what is tested is exactly what the notebook runs.

Nothing here reads or writes storage: every function takes DataFrames and returns DataFrames.
"""
from pyspark.sql import functions as F, Window

# --- region: common ---------------------------------------------------------------------------
MAX_VALID_SECONDS = 86400          # a ride longer than 24 hours is not a plausible single ride
DURATION_CATEGORIES = ["OK", "NEGATIVE", "ZERO", "OVER_24H", "MISSING"]


def age_years(event_date, birth_date):
    """Completed years at event_date. NULL when either date is NULL or the event precedes birth.

    Convention for a 29 February birthday: the anniversary falls on 28 February in non-leap years
    (the same rule as T-SQL DATEADD, used by the related Synapse warehouse).
    """
    e = F.col(event_date) if isinstance(event_date, str) else event_date
    b = F.col(birth_date) if isinstance(birth_date, str) else birth_date
    last_day_of_month = F.dayofmonth(F.last_day(F.make_date(F.year(e), F.month(b), F.lit(1))))
    anniversary = F.make_date(F.year(e), F.month(b), F.least(F.dayofmonth(b), last_day_of_month))
    return (F.when(e.isNull() | b.isNull() | (e < b), F.lit(None))
             .otherwise(F.year(e) - F.year(b) - F.when(e < anniversary, 1).otherwise(0))).cast("int")


def duration_quality(secs):
    """Category for a ride duration in seconds. Only OK rides are used in duration averages.

    MISSING (NULL), NEGATIVE (< 0), ZERO (= 0), OVER_24H (> 86400) and OK (0 < secs <= 86400).
    """
    return (F.when(secs.isNull(), "MISSING").when(secs < 0, "NEGATIVE").when(secs == 0, "ZERO")
             .when(secs > MAX_VALID_SECONDS, "OVER_24H").otherwise("OK"))


def time_band(hour):
    return (F.when(hour < 6, "Night (00-05)").when(hour < 12, "Morning (06-11)")
             .when(hour < 18, "Afternoon (12-17)").otherwise("Evening (18-23)"))


def dedupe(df, key, order_cols):
    """One row per key. Tie-break = order_cols (first wins). Returns (df, number of keys with conflicting rows)."""
    conflicts = df.dropDuplicates().groupBy(key).count().filter("count > 1").count()
    w = Window.partitionBy(key).orderBy(*order_cols)
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn"), conflicts


def table_fingerprint(df):
    """(row count, order-independent content hash). Equal fingerprints for two builds mean equal row
    counts and, with overwhelming probability, equal content in every column (xxhash64 over all columns,
    summed so row order does not matter). It is a strong consistency check, not a cryptographic proof."""
    h = df.select(F.xxhash64(*df.columns).cast("decimal(38,0)").alias("h"))
    r = h.agg(F.count(F.lit(1)).alias("n"), F.sum("h").alias("s")).first()
    return (r["n"], str(r["s"]))
# --- end region: common -----------------------------------------------------------------------


# --- region: dimensions -----------------------------------------------------------------------
def build_dim_rider(rider):
    r, conflicts = dedupe(rider, "rider_id",
                          [F.col("account_start_date").desc_nulls_last(), F.col("account_end_date").desc_nulls_last()])
    dim = r.select(
        F.row_number().over(Window.orderBy("rider_id")).alias("rider_key"),
        "rider_id",
        F.col("birthday").alias("birth_date"),
        "account_start_date", "account_end_date",
        F.col("is_member"),                       # current snapshot flag, not historical status
        age_years("account_start_date", "birthday").alias("age_at_account_start"))
    return dim, conflicts


def build_dim_station(station):
    s, conflicts = dedupe(station, "station_id", [F.col("name").asc_nulls_last(), F.col("latitude").asc_nulls_last()])
    dim = s.select(F.row_number().over(Window.orderBy("station_id")).alias("station_key"),
                   "station_id", F.col("name").alias("station_name"), "latitude", "longitude")
    return dim, conflicts


def build_dim_date(trip, payment):
    b = (trip.agg(F.min(F.to_date("started_at")).alias("t0"), F.max(F.to_date("ended_at")).alias("t1"))
             .crossJoin(payment.agg(F.min("date").alias("p0"), F.max("date").alias("p1")))
             .select(F.least("t0", "p0").alias("lo"), F.greatest("t1", "p1").alias("hi")).first())
    d = (trip.sparkSession.range(1)
         .select(F.explode(F.sequence(F.lit(b["lo"]), F.lit(b["hi"]), F.expr("interval 1 day"))).alias("date")))
    return d.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        (F.expr("weekday(date)") + 1).alias("day_of_week_num"),          # 1 = Monday ... 7 = Sunday
        F.date_format("date", "EEEE").alias("day_of_week_name"),
        F.dayofmonth("date").alias("day_of_month"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.quarter("date").alias("quarter"),
        F.year("date").alias("year"),
        F.date_format("date", "yyyy-MM").alias("year_month"),
        (F.expr("weekday(date)") >= 5).alias("is_weekend"))
# --- end region: dimensions -------------------------------------------------------------------


# --- region: facts ----------------------------------------------------------------------------
def build_fact_trip(trip, dim_rider, dim_station):
    rider = F.broadcast(dim_rider.select("rider_key", "rider_id", "birth_date"))
    s_start = F.broadcast(dim_station.select(F.col("station_id").alias("start_station_id"), F.col("station_key").alias("start_station_key")))
    s_end = F.broadcast(dim_station.select(F.col("station_id").alias("end_station_id"), F.col("station_key").alias("end_station_key")))
    secs = F.col("ended_at").cast("long") - F.col("started_at").cast("long")
    hr = F.hour("started_at")
    return (trip
        .join(rider, "rider_id", "left")
        .join(s_start, "start_station_id", "left")
        .join(s_end, "end_station_id", "left")
        .select(
            "trip_id", "rider_key",
            F.date_format("started_at", "yyyyMMdd").cast("int").alias("start_date_key"),
            F.date_format("ended_at", "yyyyMMdd").cast("int").alias("end_date_key"),
            "start_station_key", "end_station_key", "rideable_type", "started_at", "ended_at",
            hr.alias("start_hour"),
            time_band(hr).alias("time_band"),
            secs.alias("ride_duration_seconds"),
            (secs / 60.0).alias("ride_duration_minutes"),
            duration_quality(secs).alias("duration_quality"),
            age_years(F.to_date("started_at"), "birth_date").alias("rider_age_at_trip")))


def build_fact_payment(payment, dim_rider):
    rider = F.broadcast(dim_rider.select("rider_key", "rider_id"))
    return (payment.join(rider, "rider_id", "left")
        .select("payment_id", "rider_key",
                F.date_format("date", "yyyyMMdd").cast("int").alias("payment_date_key"),
                F.col("date").alias("payment_date"),
                F.col("amount").cast("decimal(10,2)").alias("amount")))
# --- end region: facts ------------------------------------------------------------------------
