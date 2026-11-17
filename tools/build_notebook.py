"""Generates the SOURCE-ONLY Databricks notebook (no outputs) from this script and src/divvy_transforms.py.

Usage:  python tools/build_notebook.py [--out PATH]
Default output: notebooks/Divvy_Databricks_Lakehouse.ipynb (relative to the repository root).

The generator refuses to overwrite a notebook that contains saved outputs, so an executed notebook
(Azure run evidence) can never be destroyed by regenerating the source.
"""
import argparse
import json
import re
import sys
from pathlib import Path

import nbformat as nbf

ROOT = Path(__file__).resolve().parents[1]
ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
ap.add_argument("--out", type=Path, default=ROOT / "notebooks" / "Divvy_Databricks_Lakehouse.ipynb")
OUT = ap.parse_args().out.resolve()

if OUT.exists():
    existing = json.loads(OUT.read_text())
    if any(c.get("outputs") for c in existing.get("cells", []) if c.get("cell_type") == "code"):
        sys.exit(f"Refusing to overwrite {OUT}: it contains saved outputs (execution evidence). Choose another --out.")

_MODULE = (ROOT / "src" / "divvy_transforms.py").read_text()


def region(*names):
    """Return the named '# --- region: x ---' blocks of src/divvy_transforms.py, verbatim."""
    out = []
    for n in names:
        m = re.search(rf"# --- region: {n} -+\n(.*?)# --- end region: {n} -+", _MODULE, re.S)
        if not m:
            sys.exit(f"region {n!r} not found in src/divvy_transforms.py")
        out.append(m.group(1).strip("\n"))
    return "\n\n\n".join(out)


cells = []
md = lambda s: cells.append(nbf.v4.new_markdown_cell(s.strip("\n")))
code = lambda s: cells.append(nbf.v4.new_code_cell(s.strip("\n")))

# ----------------------------------------------------------------------------- 0
md("""
# Divvy Bike-Share Data Lakehouse on Azure Databricks

**Flow:** Unity Catalog Volume (source CSV) → Bronze Delta files (volume) → registered Bronze Delta tables → Gold Delta star schema → analytics

**Business questions**
1. Ride time by weekday / time of day, start & end station, rider age at trip, member vs casual.
2. Payments per month / quarter / year, and per member by age at account start.
3. *(Extra credit)* Spend per member vs. average rides per month and monthly riding minutes.

**Star schema** (see `docs/Divvy_Star_Schema.pdf` in the repository): `fact_trip`, `fact_payment`; dimensions `dim_rider`, `dim_station` (role-played as start/end), `dim_date`.

**Before running:** upload `riders.csv`, `payments.csv`, `stations.csv`, `trips.csv` to `<volume>/raw/` in Catalog Explorer, attach a Unity Catalog compute (single node, DBR 13.3 LTS+), edit the three names in the next cell, then *Run all*.

**Source note:** the four CSV files have **no header row**, so column names come from the ERD and are applied explicitly below.
""")

md("## 0. Configuration")
code('''
from pyspark.sql import functions as F, Window

# ---- EDIT THESE THREE to match Catalog Explorer (catalog > schema > volume) --------------
CATALOG       = "main"      # catalog that holds your volume; tables are created here too
VOLUME_SCHEMA = "default"   # schema that holds the volume
VOLUME        = "divvy"     # volume name
# -------------------------------------------------------------------------------------------
BRONZE_SCHEMA = "divvy_bronze"   # registered Bronze tables
GOLD_SCHEMA   = "divvy_gold"     # Gold star schema tables

VOLUME_BASE = f"/Volumes/{CATALOG}/{VOLUME_SCHEMA}/{VOLUME}"
RAW_DIR     = f"{VOLUME_BASE}/raw"            # source CSVs (never written to by this notebook)
BRONZE_DIR  = f"{VOLUME_BASE}/bronze_delta"   # Bronze Delta *files* (one folder per entity)

SOURCE_FILES = {"rider": "riders.csv", "payment": "payments.csv",
                "station": "stations.csv", "trip": "trips.csv"}

# Column names + target types. The CSVs have no header, so this is the single source of truth.
BRONZE_COLS = {
    "rider":   [("rider_id", "INT"), ("first", "STRING"), ("last", "STRING"), ("address", "STRING"),
                ("birthday", "DATE"), ("account_start_date", "DATE"), ("account_end_date", "DATE"),
                ("is_member", "BOOLEAN")],
    "payment": [("payment_id", "INT"), ("date", "DATE"), ("amount", "DECIMAL(10,2)"), ("rider_id", "INT")],
    "station": [("station_id", "STRING"), ("name", "STRING"), ("latitude", "DOUBLE"), ("longitude", "DOUBLE")],
    "trip":    [("trip_id", "STRING"), ("rideable_type", "STRING"), ("started_at", "TIMESTAMP"),
                ("ended_at", "TIMESTAMP"), ("start_station_id", "STRING"), ("end_station_id", "STRING"),
                ("rider_id", "INT")],
}

def tbl(schema, name):
    """Fully qualified table name catalog.schema.table."""
    return f"{CATALOG}.{schema}.{name}" if CATALOG else f"{schema}.{name}"

def ls(path):
    return [f.name for f in dbutils.fs.ls(path)]

try:
    display
except NameError:                      # lets the same code run outside Databricks (local test)
    display = lambda df: df.show(20, False)

print("Volume base :", VOLUME_BASE)
print("Spark       :", spark.version)
''')

code('''
# Validate that the four source files exist in the volume before doing anything else.
found = ls(RAW_DIR)
missing = [f for f in SOURCE_FILES.values() if f not in found]
assert not missing, f"Missing in {RAW_DIR}: {missing}. Found: {found}. Check CATALOG / VOLUME_SCHEMA / VOLUME and the upload."
print("OK - all source files found:", sorted(SOURCE_FILES.values()))
''')

md("""
**Source → Bronze → Gold mapping**

| Source file | Bronze table | Columns (from ERD; files have no header) | Gold use |
|---|---|---|---|
| `riders.csv` | `rider` | rider_id, first, last, address, birthday, account_start_date, account_end_date, is_member | `dim_rider` (names/address are dropped: not needed) |
| `stations.csv` | `station` | station_id *(string: values like `KA1503000012`)*, name, latitude, longitude | `dim_station` |
| `trips.csv` | `trip` | trip_id, rideable_type, started_at, ended_at, start_station_id, end_station_id, rider_id | `fact_trip` |
| `payments.csv` | `payment` | payment_id, date, amount, rider_id | `fact_payment` |

ERD correction: `station_id` / `start_station_id` / `end_station_id` are **strings**, not INT as drawn in the ERD. There is no separate Account file; account fields live on the rider.
""")

# ----------------------------------------------------------------------------- 1
md("""
## 1. Source inspection
Read every CSV as **strings** with an explicit column list (no schema inference), then convert with `try_cast`, which yields NULL instead of failing. Any value that was present in the file but became NULL is counted as a **parse failure**.
""")
code('''
def read_raw(name):
    ddl = ", ".join(f"`{c}` STRING" for c, _ in BRONZE_COLS[name])
    return (spark.read.option("header", "false").option("mode", "PERMISSIVE")
            .schema(ddl).csv(f"{RAW_DIR}/{SOURCE_FILES[name]}"))

def type_and_check(name):
    """Return (typed DataFrame, {column: parse_failure_count})."""
    cols = BRONZE_COLS[name]
    typed = [c if t == "STRING" else f"try_cast(`{c}` AS {t}) AS `{c}`" for c, t in cols]
    nonstr = [c for c, t in cols if t != "STRING"]
    both = read_raw(name).selectExpr(*typed, *[f"`{c}` AS `_raw_{c}`" for c in nonstr])
    fails = both.agg(*[F.sum(F.when(F.col(f"_raw_{c}").isNotNull() & F.col(c).isNull(), 1).otherwise(0)).alias(c)
                       for c in nonstr]).first().asDict()
    return both.select(*[c for c, _ in cols]), {k: int(v or 0) for k, v in fails.items()}
''')
code('''
KEYS = {"rider": "rider_id", "payment": "payment_id", "station": "station_id", "trip": "trip_id"}
PII_COLS = {"rider": ["first", "last", "address"]}   # never displayed: not needed for any analysis
inspection = {}
for name in BRONZE_COLS:
    df, fails = type_and_check(name)
    cols = [c for c, _ in BRONZE_COLS[name]]
    stats = df.agg(F.count(F.lit(1)).alias("rows"),
                   F.countDistinct(KEYS[name]).alias("distinct_keys"),
                   *[F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}") for c in cols]).first().asDict()
    inspection[name] = (stats, fails)
    print(f"\\n=== {name}  ({SOURCE_FILES[name]}) ===")
    df.printSchema()
    display(df.drop(*PII_COLS.get(name, [])).limit(5))
    print("rows:", stats["rows"], "| distinct keys:", stats["distinct_keys"],
          "| duplicate keys:", stats["rows"] - stats["distinct_keys"])
    print("nulls per column:", {c: stats[f"null_{c}"] for c in cols if stats[f"null_{c}"]})
    print("parse failures  :", {c: n for c, n in fails.items() if n} or "none")

bad = {n: {c: v for c, v in f.items() if v} for n, (_, f) in inspection.items() if any(f.values())}
assert not bad, f"Type conversion failures (header row present? wrong format?): {bad}"
''')
md("""
*What to look for:* `rider.account_end_date` is legitimately NULL for active accounts; every other column should have 0 nulls. Parse failures must be `none`. If a file turns out to have a header row, the failure counts will point at the ID column.
""")

# ----------------------------------------------------------------------------- 2
md("""
## 2. Extract: write Bronze Delta **files** to the volume
Each entity is written to its own folder under `bronze_delta/`. The `raw/` folder with the CSVs is never touched. `mode("overwrite")` makes re-runs idempotent.
""")
code('''
CSV_COUNTS = {n: inspection[n][0]["rows"] for n in BRONZE_COLS}
for name in BRONZE_COLS:
    df, _ = type_and_check(name)
    (df.write.format("delta").mode("overwrite").save(f"{BRONZE_DIR}/{name}"))
    print("wrote", f"{BRONZE_DIR}/{name}")
''')
code('''
# Verify: Delta transaction log exists, then read back and compare counts with the CSV.
FILE_COUNTS = {}
for name in BRONZE_COLS:
    path = f"{BRONZE_DIR}/{name}"
    assert "_delta_log/" in ls(path), f"No _delta_log in {path}"
    FILE_COUNTS[name] = spark.read.format("delta").load(path).count()
    print(f"{name:8s} csv={CSV_COUNTS[name]:>10,}  delta_files={FILE_COUNTS[name]:>10,}  match={CSV_COUNTS[name] == FILE_COUNTS[name]}")
assert CSV_COUNTS == FILE_COUNTS
print(ls(f"{BRONZE_DIR}/trip")[:5])
''')

# ----------------------------------------------------------------------------- 3
md("""
## 3. Load: registered Bronze Delta tables (Spark SQL)
Unity Catalog does **not** allow a table `LOCATION` inside a volume (path overlap), so the Bronze *tables* are managed Delta tables in Unity Catalog storage. They are created with `CREATE TABLE` and filled with `INSERT OVERWRITE` from the Delta files written in section 2 (via temporary views).
""")
code('''
for schema in (BRONZE_SCHEMA, GOLD_SCHEMA):
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}" if CATALOG else f"CREATE SCHEMA IF NOT EXISTS {schema}")
    except Exception as e:   # no CREATE SCHEMA privilege: use an existing schema instead
        raise RuntimeError(f"Cannot create schema {schema}. Pick an existing writable schema and set "
                           f"BRONZE_SCHEMA / GOLD_SCHEMA above. Original error: {e}")
print("schemas ready:", BRONZE_SCHEMA, GOLD_SCHEMA)
''')
code('''
for name, cols in BRONZE_COLS.items():
    t = tbl(BRONZE_SCHEMA, name)
    col_ddl = ", ".join(f"`{c}` {ty}" for c, ty in cols)
    col_list = ", ".join(f"`{c}`" for c, _ in cols)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {t} ({col_ddl}) USING DELTA")
    spark.read.format("delta").load(f"{BRONZE_DIR}/{name}").createOrReplaceTempView(f"v_bronze_{name}")
    spark.sql(f"INSERT OVERWRITE {t} SELECT {col_list} FROM v_bronze_{name}")
    print("loaded", t)
''')
code('''
TABLE_COUNTS = {}
for name in BRONZE_COLS:
    t = tbl(BRONZE_SCHEMA, name)
    detail = spark.sql(f"DESCRIBE DETAIL {t}").select("format", "numFiles", "sizeInBytes").first()
    TABLE_COUNTS[name] = spark.sql(f"SELECT COUNT(*) AS n FROM {t}").first()["n"]
    print(f"{t:40s} format={detail['format']}  files={detail['numFiles']}  rows={TABLE_COUNTS[name]:,}")
    assert detail["format"] == "delta" and TABLE_COUNTS[name] == FILE_COUNTS[name]
display(spark.sql(f"SELECT * FROM {tbl(BRONZE_SCHEMA, 'trip')} LIMIT 5"))
''')

# ----------------------------------------------------------------------------- 4
md("""
## 4. Gold dimensions (PySpark)
* **Keys:** `rider_key` / `station_key` = `row_number()` over the business ID (deterministic for the same data); `date_key` = `yyyyMMdd`.
* **Duplicates:** deduplicated by business key with an explicit tie-break; keys whose duplicate rows *disagree* are counted and reported.
* **Age** = completed years (birthday anniversary respected), NULL if a date is missing or the event is before birth. A 29 February birthday has its anniversary on 28 February in non-leap years.
* These functions are embedded unchanged from `src/divvy_transforms.py`, where they are unit-tested with synthetic fixtures.
* Dimensions hold attributes only, no facts. Names/addresses are intentionally left out (PII not needed).
""")
code("def bronze(name):\n    return spark.table(tbl(BRONZE_SCHEMA, name))\n\n\n" + region("common", "dimensions"))


# ----------------------------------------------------------------------------- 5
md("""
## 5. Gold facts (PySpark)
* `fact_trip` — grain: one row per `trip_id`. Left joins to the dimensions so no trip is dropped; unmatched station/rider keys stay NULL and are reported in validation.
* `fact_payment` — grain: one row per `payment_id`.
* Trip duration keeps fractional minutes. `duration_quality` is `OK` only when `0 < seconds <= 86400`; otherwise `NEGATIVE`, `ZERO`, `OVER_24H` (longer than 24 hours) or `MISSING` (NULL). Such rows are flagged, not deleted. Timestamps are used exactly as stored in the source (no timezone assumption).
* Trips and payments are **never joined to each other** row-by-row; for the extra-credit analysis each is aggregated to rider-month first.
""")
code(region("facts"))


# ----------------------------------------------------------------------------- 6
md("""
## 6. Gold Delta managed tables
Every Gold table is written with `write.format("delta").mode("overwrite").saveAsTable(...)`. Re-running replaces the content (no appends, nothing is dropped). Facts are built from the *saved* dimension tables so keys always match.
""")
code('''
GOLD_TABLES = ["dim_rider", "dim_station", "dim_date", "fact_trip", "fact_payment"]
DIM_CONFLICTS = {}

def gold(name):
    return spark.table(tbl(GOLD_SCHEMA, name))

def write_gold(df, name):
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(tbl(GOLD_SCHEMA, name)))

def build_gold():
    dim_rider, DIM_CONFLICTS["rider"] = build_dim_rider(bronze("rider"))
    dim_station, DIM_CONFLICTS["station"] = build_dim_station(bronze("station"))
    write_gold(dim_rider, "dim_rider")
    write_gold(dim_station, "dim_station")
    write_gold(build_dim_date(bronze("trip"), bronze("payment")), "dim_date")
    write_gold(build_fact_trip(bronze("trip"), gold("dim_rider"), gold("dim_station")), "fact_trip")
    write_gold(build_fact_payment(bronze("payment"), gold("dim_rider")), "fact_payment")
    return {n: gold(n).count() for n in GOLD_TABLES}

GOLD_COUNTS_RUN1 = build_gold()
FINGERPRINT_RUN1 = {n: table_fingerprint(gold(n)) for n in GOLD_TABLES}
print(GOLD_COUNTS_RUN1)
print("keys with conflicting duplicate source rows:", DIM_CONFLICTS)
''')
code('''
for n in GOLD_TABLES:
    t = tbl(GOLD_SCHEMA, n)
    fmt = spark.sql(f"DESCRIBE DETAIL {t}").first()["format"]
    assert fmt == "delta" and GOLD_COUNTS_RUN1[n] > 0
    print(f"{t:38s} format={fmt} rows={GOLD_COUNTS_RUN1[n]:,}")
display(gold("fact_trip").limit(5)); display(gold("fact_payment").limit(5))
display(gold("dim_rider").limit(5)); display(gold("dim_station").limit(5)); display(gold("dim_date").limit(5))
''')

# ----------------------------------------------------------------------------- 7
md("## 7. Validation")
code('''
# ASSERT = a real test that fails the run. INFO = a reported diagnostic that is never counted as a pass.
results = []
def check(name, ok, detail=""):
    results.append(("ASSERT", name, "PASS" if ok else "FAIL", str(detail)))
def info(name, detail):
    results.append(("INFO", name, "-", str(detail)))

ft, fp, dr, ds, dd = (gold(n) for n in ["fact_trip", "fact_payment", "dim_rider", "dim_station", "dim_date"])
n = lambda df, cond: df.filter(cond).count()

# Row-count lineage: CSV -> Bronze files -> Bronze tables -> Gold
check("trip: csv = bronze files = bronze table = fact_trip",
      CSV_COUNTS["trip"] == FILE_COUNTS["trip"] == TABLE_COUNTS["trip"] == GOLD_COUNTS_RUN1["fact_trip"], GOLD_COUNTS_RUN1["fact_trip"])
check("payment: csv = bronze files = bronze table = fact_payment",
      CSV_COUNTS["payment"] == FILE_COUNTS["payment"] == TABLE_COUNTS["payment"] == GOLD_COUNTS_RUN1["fact_payment"], GOLD_COUNTS_RUN1["fact_payment"])
check("rider: csv = ... = dim_rider", CSV_COUNTS["rider"] == TABLE_COUNTS["rider"] == GOLD_COUNTS_RUN1["dim_rider"], GOLD_COUNTS_RUN1["dim_rider"])
check("station: csv = ... = dim_station", CSV_COUNTS["station"] == TABLE_COUNTS["station"] == GOLD_COUNTS_RUN1["dim_station"], GOLD_COUNTS_RUN1["dim_station"])

# Unique keys (also proves joins did not multiply fact rows)
for df, key, nm in [(ft, "trip_id", "fact_trip.trip_id"), (fp, "payment_id", "fact_payment.payment_id"),
                    (dr, "rider_key", "dim_rider.rider_key"), (dr, "rider_id", "dim_rider.rider_id"),
                    (ds, "station_key", "dim_station.station_key"), (ds, "station_id", "dim_station.station_id"),
                    (dd, "date_key", "dim_date.date_key")]:
    dups = df.groupBy(key).count().filter("count > 1").count()
    check(f"unique {nm}", dups == 0, f"duplicate keys: {dups}")
check("no conflicting duplicate source rows in dimensions", sum(DIM_CONFLICTS.values()) == 0, DIM_CONFLICTS)

# Required keys / referential integrity
check("fact_trip.rider_key not NULL", n(ft, "rider_key IS NULL") == 0, n(ft, "rider_key IS NULL"))
check("fact_payment.rider_key not NULL", n(fp, "rider_key IS NULL") == 0, n(fp, "rider_key IS NULL"))
check("fact_trip station keys not NULL", n(ft, "start_station_key IS NULL OR end_station_key IS NULL") == 0,
      f"start NULL={n(ft, 'start_station_key IS NULL')}, end NULL={n(ft, 'end_station_key IS NULL')}  (any NULL = station id absent from stations.csv)")
check("fact_trip.rider_age_at_trip not NULL", n(ft, "rider_age_at_trip IS NULL") == 0, n(ft, "rider_age_at_trip IS NULL"))
check("rider_age_at_trip in 0..110", n(ft, "rider_age_at_trip < 0 OR rider_age_at_trip > 110") == 0)
check("every date key exists in dim_date",
      ft.join(dd.select(F.col("date_key").alias("k")), ft.start_date_key == F.col("k"), "left_anti").count() == 0 and
      ft.join(dd.select(F.col("date_key").alias("k")), ft.end_date_key == F.col("k"), "left_anti").count() == 0 and
      fp.join(dd.select(F.col("date_key").alias("k")), fp.payment_date_key == F.col("k"), "left_anti").count() == 0)
lo, hi, n_days = dd.agg(F.min("date"), F.max("date"), F.count(F.lit(1))).first()
check("dim_date has no gaps between its first and last date", (hi - lo).days + 1 == n_days, f"{lo} .. {hi}, {n_days} days")

# Duration quality: a rule that can fail (OK means 0 < seconds <= 86400)
dq = {r["duration_quality"]: r["n"] for r in ft.groupBy("duration_quality").agg(F.count("*").alias("n")).collect()}
check("duration_quality only uses the defined categories", set(dq) <= set(DURATION_CATEGORIES), sorted(dq))
check("duration_quality categories add up to fact_trip", sum(dq.values()) == GOLD_COUNTS_RUN1["fact_trip"], sum(dq.values()))
check("every OK trip has 0 < seconds <= 86400, no other trip does",
      n(ft, "duration_quality = 'OK' AND NOT (ride_duration_seconds > 0 AND ride_duration_seconds <= 86400)") == 0 and
      n(ft, "duration_quality <> 'OK' AND ride_duration_seconds > 0 AND ride_duration_seconds <= 86400") == 0)
valid_from_bronze = n(bronze("trip").select((F.col("ended_at").cast("long") - F.col("started_at").cast("long")).alias("s")),
                      "s > 0 AND s <= 86400")
check("valid trip count = independent count from Bronze", dq.get("OK", 0) == valid_from_bronze, f"OK={dq.get('OK', 0):,} bronze={valid_from_bronze:,}")
info("duration_quality counts (flagged trips stay in fact_trip, excluded from duration averages)", dq)

# Payment amounts: numeric and reconcile with the CSV
csv_sum = type_and_check("payment")[0].agg(F.sum("amount")).first()[0]
fact_sum = fp.agg(F.sum("amount")).first()[0]
check("payment amount: CSV sum = fact_payment sum", csv_sum == fact_sum, f"csv={csv_sum} fact={fact_sum}")
check("payment amount: no NULL / non-positive", n(fp, "amount IS NULL OR amount <= 0") == 0, n(fp, "amount IS NULL OR amount <= 0"))

# Delta format
check("Gold tables are Delta",
      all(spark.sql(f"DESCRIBE DETAIL {tbl(GOLD_SCHEMA, t)}").first()["format"] == "delta" for t in GOLD_TABLES))

# Rebuild safety: run the whole Gold build a second time; compare row counts AND content fingerprints
GOLD_COUNTS_RUN2 = build_gold()
FINGERPRINT_RUN2 = {t: table_fingerprint(gold(t)) for t in GOLD_TABLES}
check("Gold re-run (overwrite) gives identical row counts", GOLD_COUNTS_RUN1 == GOLD_COUNTS_RUN2, GOLD_COUNTS_RUN2)
check("Gold re-run gives identical content fingerprints (all columns, all 5 tables)",
      FINGERPRINT_RUN1 == FINGERPRINT_RUN2, {t: FINGERPRINT_RUN2[t][1][-8:] for t in GOLD_TABLES})

res = spark.createDataFrame(results, "kind STRING, check STRING, result STRING, detail STRING")
res.show(100, False)
asserts = [r for r in results if r[0] == "ASSERT"]
failed = [r for r in asserts if r[2] != "PASS"]
print(f"{len(asserts) - len(failed)} of {len(asserts)} assertions passed; {len(results) - len(asserts)} informational diagnostic(s) not counted.")
assert not failed, f"Validation failed: {[r[1] for r in failed]}"
print("ALL VALIDATION ASSERTIONS PASSED")
''')
md("""
*Reading the result:* only `ASSERT` rows can fail the run; `INFO` rows report a finding and are never counted as passes. An unmatched station key would not be an error in itself (stations can be retired or missing from the station list), which is why that check prints how many are NULL. Trips that are `NEGATIVE`, `ZERO`, `OVER_24H` or `MISSING` stay in the fact table but are excluded from the duration analytics below. The re-run check compares row counts and an order-independent hash of every column; it is a strong consistency check, not a cryptographic proof.
""")

# ----------------------------------------------------------------------------- 8
md("""
## 8. Analytics
Duration queries use only `duration_quality = 'OK'` trips (`0 < duration <= 24 hours`). `is_member` is the rider's **current** flag in the source.
""")
code('''
FT, FP, DR, DS, DD = (tbl(GOLD_SCHEMA, n) for n in ["fact_trip", "fact_payment", "dim_rider", "dim_station", "dim_date"])

def age_band(col):
    return (f"CASE WHEN {col} IS NULL THEN 'unknown' WHEN {col} < 18 THEN '<18' WHEN {col} < 25 THEN '18-24' "
            f"WHEN {col} < 35 THEN '25-34' WHEN {col} < 45 THEN '35-44' WHEN {col} < 55 THEN '45-54' "
            f"WHEN {col} < 65 THEN '55-64' ELSE '65+' END")
''')
md("### 8.1 Ride time by weekday and time of day")
code('''
display(spark.sql(f"""
SELECT d.day_of_week_num, d.day_of_week_name AS weekday, f.time_band,
       COUNT(*) AS trips, ROUND(SUM(f.ride_duration_minutes), 1) AS total_minutes,
       ROUND(AVG(f.ride_duration_minutes), 2) AS avg_minutes
FROM {FT} f JOIN {DD} d ON f.start_date_key = d.date_key
WHERE f.duration_quality = 'OK'
GROUP BY d.day_of_week_num, d.day_of_week_name, f.time_band
ORDER BY d.day_of_week_num, f.time_band"""))
''')
md("### 8.2 Ride time by start and end station (top 15 by average minutes, stations with ≥ 200 trips)")
code('''
for role, key in [("start", "start_station_key"), ("end", "end_station_key")]:
    print(f"--- by {role} station ---")
    display(spark.sql(f"""
    SELECT s.station_name AS {role}_station, COUNT(*) AS trips, ROUND(AVG(f.ride_duration_minutes), 2) AS avg_minutes,
           ROUND(SUM(f.ride_duration_minutes), 0) AS total_minutes
    FROM {FT} f JOIN {DS} s ON f.{key} = s.station_key
    WHERE f.duration_quality = 'OK'
    GROUP BY s.station_name HAVING COUNT(*) >= 200
    ORDER BY avg_minutes DESC LIMIT 15"""))
''')
md("### 8.3 Ride time by rider age at trip and member vs casual")
code('''
display(spark.sql(f"""
SELECT {age_band('f.rider_age_at_trip')} AS age_band_at_trip,
       CASE WHEN r.is_member THEN 'member' ELSE 'casual' END AS rider_type,
       COUNT(*) AS trips, ROUND(AVG(f.ride_duration_minutes), 2) AS avg_minutes, ROUND(SUM(f.ride_duration_minutes), 0) AS total_minutes
FROM {FT} f JOIN {DR} r ON f.rider_key = r.rider_key
WHERE f.duration_quality = 'OK'
GROUP BY 1, 2 ORDER BY 1, 2"""))
''')
md("### 8.4 Money spent per year, quarter and month")
code('''
display(spark.sql(f"""
SELECT d.year, d.quarter, d.month, d.month_name, COUNT(*) AS payments, ROUND(SUM(p.amount), 2) AS total_paid
FROM {FP} p JOIN {DD} d ON p.payment_date_key = d.date_key
GROUP BY ROLLUP (d.year, d.quarter, (d.month, d.month_name))
ORDER BY d.year, d.quarter, d.month""").limit(200))
''')
md("### 8.5 Money spent per member by age at account start")
code('''
display(spark.sql(f"""
WITH per_rider AS (SELECT rider_key, SUM(amount) AS total_paid, COUNT(*) AS payments FROM {FP} GROUP BY rider_key)
SELECT {age_band('r.age_at_account_start')} AS age_band_at_account_start,
       COUNT(*) AS riders_with_payments, ROUND(SUM(p.total_paid), 2) AS total_paid,
       ROUND(AVG(p.total_paid), 2) AS avg_paid_per_rider
FROM per_rider p JOIN {DR} r ON p.rider_key = r.rider_key
GROUP BY 1 ORDER BY 1"""))
''')
md("""
### 8.6 Extra credit: spend vs. rides per month and minutes per month
Payments and trips are aggregated **separately** to rider × month, then combined, so payment amounts are never repeated per trip. Trips only exist for a 12-month window, so payments are limited to that same window; riders with no trips or no payments in a month are kept (FULL OUTER JOIN).
Rider averages = total over the window ÷ number of months in the window.
""")
code('''
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW rider_month AS
WITH win AS (SELECT MIN(start_date_key) AS lo, MAX(start_date_key) AS hi FROM {FT}),
trip_m AS (
  SELECT f.rider_key, d.year_month, COUNT(*) AS rides, SUM(f.ride_duration_minutes) AS ride_minutes
  FROM {FT} f JOIN {DD} d ON f.start_date_key = d.date_key
  WHERE f.duration_quality = 'OK' GROUP BY f.rider_key, d.year_month),
pay_m AS (
  SELECT p.rider_key, d.year_month, SUM(p.amount) AS spend
  FROM {FP} p JOIN {DD} d ON p.payment_date_key = d.date_key CROSS JOIN win
  WHERE p.payment_date_key BETWEEN win.lo AND win.hi GROUP BY p.rider_key, d.year_month)
SELECT COALESCE(t.rider_key, p.rider_key) AS rider_key, COALESCE(t.year_month, p.year_month) AS year_month,
       COALESCE(t.rides, 0) AS rides, COALESCE(t.ride_minutes, 0.0) AS ride_minutes, COALESCE(p.spend, 0) AS spend
FROM trip_m t FULL OUTER JOIN pay_m p ON t.rider_key = p.rider_key AND t.year_month = p.year_month""")

n_months = spark.sql(f"SELECT COUNT(DISTINCT d.year_month) AS n FROM {FT} f JOIN {DD} d ON f.start_date_key = d.date_key").first()["n"]
print("months in trip window:", n_months)

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW rider_summary AS
SELECT r.rider_key, r.is_member, COALESCE(SUM(m.rides), 0) / {n_months} AS avg_rides_per_month,
       COALESCE(SUM(m.ride_minutes), 0) / {n_months} AS avg_minutes_per_month, COALESCE(SUM(m.spend), 0) AS spend_in_window
FROM {DR} r LEFT JOIN rider_month m ON r.rider_key = m.rider_key GROUP BY r.rider_key, r.is_member""")

print("--- spend by average rides per month ---")
display(spark.sql("""
SELECT CASE WHEN avg_rides_per_month = 0 THEN '0' WHEN avg_rides_per_month < 1 THEN '<1' WHEN avg_rides_per_month < 3 THEN '1-3'
            WHEN avg_rides_per_month < 6 THEN '3-6' ELSE '6+' END AS rides_per_month_band,
       COUNT(*) AS riders, ROUND(AVG(spend_in_window), 2) AS avg_spend, ROUND(SUM(spend_in_window), 2) AS total_spend
FROM rider_summary GROUP BY 1 ORDER BY 1"""))
print("--- spend by average riding minutes per month ---")
display(spark.sql("""
SELECT CASE WHEN avg_minutes_per_month = 0 THEN '0' WHEN avg_minutes_per_month < 10 THEN '<10' WHEN avg_minutes_per_month < 30 THEN '10-30'
            WHEN avg_minutes_per_month < 60 THEN '30-60' ELSE '60+' END AS minutes_per_month_band,
       COUNT(*) AS riders, ROUND(AVG(spend_in_window), 2) AS avg_spend, ROUND(SUM(spend_in_window), 2) AS total_spend
FROM rider_summary GROUP BY 1 ORDER BY 1"""))
''')
md("*How to read it:* compare `avg_spend` across the bands. Spend is the rider's payments inside the trip window; the source does not link payments to individual trips, so this is a rider-level comparison, not a per-ride cost.")

# ----------------------------------------------------------------------------- 9
md("""
## 9. Summary and re-run guidance
""")
code('''
print("Bronze files :", BRONZE_DIR, "->", list(BRONZE_COLS))
print("Bronze tables:", [tbl(BRONZE_SCHEMA, n) for n in BRONZE_COLS])
print("Gold tables  :", [tbl(GOLD_SCHEMA, n) for n in GOLD_TABLES])
display(spark.createDataFrame([(n, c) for n, c in GOLD_COUNTS_RUN2.items()], "gold_table STRING, rows LONG"))
''')
md("""
**Caveats**
* `is_member` is the rider's *current* flag; the data has no membership history, so member-vs-casual results describe today's status.
* Trips cover one year (Feb 2021 – Jan 2022); payments span 2013–2022. Time-based trip analytics and the extra-credit comparison use that overlap only.
* The source links payments to riders, not to trips.
* Negative, zero, over-24-hour and missing-duration trips are flagged in `duration_quality` and excluded from duration averages; they stay in `fact_trip`.
* Age uses completed years at the event date. Station IDs are strings.

**Re-running:** every write is `overwrite` (Bronze files, Bronze tables via `INSERT OVERWRITE`, Gold via `saveAsTable`), so running the notebook again replaces the data instead of adding to it; section 7 runs the Gold build twice and compares row counts and content fingerprints. Only the project's own tables/folders are touched.
""")

nb = nbf.v4.new_notebook(cells=cells)
nb.metadata = {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
               "language_info": {"name": "python"}}
nbf.write(nb, OUT)
print("wrote", OUT, len(cells), "cells")
