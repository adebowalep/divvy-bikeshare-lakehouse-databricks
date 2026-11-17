"""Exports small summary tables (JSON) from the Gold Delta tables for the static dashboard.
Usage: export_dashboard_data.py <gold_db_dir> <out.json>
<gold_db_dir> holds the Gold tables as Delta folders (dim_*, fact_*), e.g. a local run's warehouse/divvy_gold.db.
Same SQL logic as notebook section 8; no raw rows are exported."""
import sys, os, json
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

gold, out = sys.argv[1:3]
b = (SparkSession.builder.master("local[4]").appName("dash-export").config("spark.driver.memory", "6g")
     .config("spark.sql.shuffle.partitions", "16").config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC").config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
     .config("spark.pyspark.python", sys.executable))
spark = configure_spark_with_delta_pip(b).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
for t in ["dim_rider", "dim_station", "dim_date", "fact_trip", "fact_payment"]:
    spark.read.format("delta").load(os.path.join(gold, t)).createOrReplaceTempView(t)

def q(sql):
    return [r.asDict() for r in spark.sql(sql).collect()]

AGE = lambda c: (f"CASE WHEN {c} IS NULL THEN 'unknown' WHEN {c} < 18 THEN '<18' WHEN {c} < 25 THEN '18-24' "
                 f"WHEN {c} < 35 THEN '25-34' WHEN {c} < 45 THEN '35-44' WHEN {c} < 55 THEN '45-54' "
                 f"WHEN {c} < 65 THEN '55-64' ELSE '65+' END")
D = {}
D["kpi"] = q("""SELECT (SELECT COUNT(*) FROM fact_trip) trips,
  (SELECT COUNT(*) FROM fact_trip WHERE duration_quality='OK') valid_trips,
  (SELECT COUNT(*) FROM fact_payment) payments, (SELECT CAST(SUM(amount) AS DOUBLE) FROM fact_payment) total_paid,
  (SELECT COUNT(*) FROM dim_rider) riders, (SELECT COUNT(*) FROM dim_station) stations,
  (SELECT AVG(CASE WHEN is_member THEN 1.0 ELSE 0.0 END) FROM dim_rider) member_share,
  (SELECT AVG(ride_duration_minutes) FROM fact_trip WHERE duration_quality='OK') avg_minutes,
  (SELECT MEDIAN(ride_duration_minutes) FROM fact_trip WHERE duration_quality='OK') median_minutes""")[0]
D["quality"] = q("SELECT duration_quality q, COUNT(*) n FROM fact_trip GROUP BY 1")
D["weekday_band"] = q("""SELECT d.day_of_week_num dow, d.day_of_week_name weekday, f.time_band band, COUNT(*) trips,
  AVG(f.ride_duration_minutes) avg_min, SUM(f.ride_duration_minutes) total_min
  FROM fact_trip f JOIN dim_date d ON f.start_date_key=d.date_key WHERE f.duration_quality='OK'
  GROUP BY 1,2,3 ORDER BY 1,3""")
D["hourly"] = q("""SELECT start_hour h, COUNT(*) trips, AVG(ride_duration_minutes) avg_min
  FROM fact_trip WHERE duration_quality='OK' GROUP BY 1 ORDER BY 1""")
D["monthly_trips"] = q("""SELECT d.year_month ym, COUNT(*) trips, AVG(f.ride_duration_minutes) avg_min
  FROM fact_trip f JOIN dim_date d ON f.start_date_key=d.date_key WHERE f.duration_quality='OK' GROUP BY 1 ORDER BY 1""")
D["top_start"] = q("""SELECT s.station_name name, COUNT(*) trips, AVG(f.ride_duration_minutes) avg_min
  FROM fact_trip f JOIN dim_station s ON f.start_station_key=s.station_key WHERE f.duration_quality='OK'
  GROUP BY 1 ORDER BY trips DESC LIMIT 10""")
D["longest_end"] = q("""SELECT s.station_name name, COUNT(*) trips, AVG(f.ride_duration_minutes) avg_min
  FROM fact_trip f JOIN dim_station s ON f.end_station_key=s.station_key WHERE f.duration_quality='OK'
  GROUP BY 1 HAVING COUNT(*)>=200 ORDER BY avg_min DESC LIMIT 5""")
D["age_member"] = q(f"""SELECT {AGE('f.rider_age_at_trip')} age_band, CASE WHEN r.is_member THEN 'member' ELSE 'casual' END rider_type,
  COUNT(*) trips, AVG(f.ride_duration_minutes) avg_min
  FROM fact_trip f JOIN dim_rider r ON f.rider_key=r.rider_key WHERE f.duration_quality='OK' GROUP BY 1,2 ORDER BY 1,2""")
D["pay_year"] = q("""SELECT d.year, COUNT(*) payments, CAST(SUM(p.amount) AS DOUBLE) total
  FROM fact_payment p JOIN dim_date d ON p.payment_date_key=d.date_key GROUP BY 1 ORDER BY 1""")
D["pay_quarter"] = q("""SELECT CONCAT(d.year,' Q',d.quarter) yq, CAST(SUM(p.amount) AS DOUBLE) total
  FROM fact_payment p JOIN dim_date d ON p.payment_date_key=d.date_key GROUP BY d.year,d.quarter ORDER BY d.year,d.quarter""")
D["pay_age"] = q(f"""WITH pr AS (SELECT rider_key, SUM(amount) total FROM fact_payment GROUP BY 1)
  SELECT {AGE('r.age_at_account_start')} age_band, COUNT(*) riders, CAST(AVG(pr.total) AS DOUBLE) avg_paid, CAST(SUM(pr.total) AS DOUBLE) total
  FROM pr JOIN dim_rider r ON pr.rider_key=r.rider_key GROUP BY 1 ORDER BY 1""")

# Extra credit: rider x month aggregated separately, then combined (no join-inflation)
spark.sql("""CREATE OR REPLACE TEMP VIEW rider_month AS
WITH win AS (SELECT MIN(start_date_key) lo, MAX(start_date_key) hi FROM fact_trip),
trip_m AS (SELECT f.rider_key, d.year_month, COUNT(*) rides, SUM(f.ride_duration_minutes) ride_minutes
  FROM fact_trip f JOIN dim_date d ON f.start_date_key=d.date_key WHERE f.duration_quality='OK' GROUP BY 1,2),
pay_m AS (SELECT p.rider_key, d.year_month, SUM(p.amount) spend FROM fact_payment p JOIN dim_date d ON p.payment_date_key=d.date_key
  CROSS JOIN win WHERE p.payment_date_key BETWEEN win.lo AND win.hi GROUP BY 1,2)
SELECT COALESCE(t.rider_key,p.rider_key) rider_key, COALESCE(t.year_month,p.year_month) year_month,
  COALESCE(t.rides,0) rides, COALESCE(t.ride_minutes,0.0) ride_minutes, COALESCE(p.spend,0) spend
FROM trip_m t FULL OUTER JOIN pay_m p ON t.rider_key=p.rider_key AND t.year_month=p.year_month""")
nm = spark.sql("SELECT COUNT(DISTINCT d.year_month) n FROM fact_trip f JOIN dim_date d ON f.start_date_key=d.date_key").first()["n"]
spark.sql(f"""CREATE OR REPLACE TEMP VIEW rider_summary AS
SELECT r.rider_key, COALESCE(SUM(m.rides),0)/{nm} avg_rides, COALESCE(SUM(m.ride_minutes),0)/{nm} avg_min,
  CAST(COALESCE(SUM(m.spend),0) AS DOUBLE) spend FROM dim_rider r LEFT JOIN rider_month m ON r.rider_key=m.rider_key GROUP BY 1""")
D["months_in_window"] = nm
D["spend_by_rides"] = q("""SELECT CASE WHEN avg_rides=0 THEN '0' WHEN avg_rides<1 THEN '<1' WHEN avg_rides<3 THEN '1-3' WHEN avg_rides<6 THEN '3-6' ELSE '6+' END band,
  COUNT(*) riders, AVG(spend) avg_spend FROM rider_summary GROUP BY 1 ORDER BY MIN(avg_rides)""")
D["spend_by_minutes"] = q("""SELECT CASE WHEN avg_min=0 THEN '0' WHEN avg_min<10 THEN '<10' WHEN avg_min<30 THEN '10-30' WHEN avg_min<60 THEN '30-60' ELSE '60+' END band,
  COUNT(*) riders, AVG(spend) avg_spend FROM rider_summary GROUP BY 1 ORDER BY MIN(avg_min)""")
json.dump(D, open(out, "w"), default=lambda o: float(o), indent=1)
print("wrote", out, {k: (len(v) if isinstance(v, list) else v) for k, v in D.items()})
