"""Runs the notebook's code cells on a LOCAL Spark+Delta session against the real CSVs (test harness only).
Usage: python tools/local_test.py NOTEBOOK DATA_DIR WORK_DIR
Replaces only the config lines (catalog/volume paths) and dbutils.ls; all transformation code is the notebook's own."""
import argparse, sys, os, json, time
from pathlib import Path
ap = argparse.ArgumentParser(description=__doc__)
ap.add_argument("notebook", type=Path, help="notebook to run, e.g. notebooks/Divvy_Databricks_Lakehouse.ipynb")
ap.add_argument("data_dir", type=Path, help="folder holding riders.csv, payments.csv, stations.csv, trips.csv")
ap.add_argument("work_dir", type=Path, help="scratch folder for the local volume and warehouse (needs ~1.5 GB free)")
args = ap.parse_args()
nb, data, work = str(args.notebook.resolve()), args.data_dir.resolve(), args.work_dir.resolve()
if not args.notebook.is_file(): sys.exit(f"Notebook not found: {args.notebook}")
missing = [f"{f}.csv" for f in ("riders", "payments", "stations", "trips") if not (data / f"{f}.csv").is_file()]
if missing: sys.exit(f"Missing in {data}: {missing}")
work = str(work); data = str(data)
os.environ["PYSPARK_PYTHON"] = os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.makedirs(f"{work}/vol/raw", exist_ok=True)
for f in ("riders", "payments", "stations", "trips"):
    dst = f"{work}/vol/raw/{f}.csv"
    if not os.path.exists(dst): os.symlink(f"{data}/{f}.csv", dst)
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
b = (SparkSession.builder.master("local[4]").appName("divvy-local-test")
     .config("spark.driver.memory", "6g").config("spark.sql.shuffle.partitions", "16")
     .config("spark.sql.warehouse.dir", f"{work}/warehouse")
     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
     .config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC").config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
     .config("spark.pyspark.python", sys.executable).config("spark.pyspark.driver.python", sys.executable))
spark = configure_spark_with_delta_pip(b).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
class _F:  # minimal dbutils.fs.ls stand-in
    def __init__(s, n): s.name = n
class _FS:
    def ls(self, p): return [_F(n + ("/" if os.path.isdir(os.path.join(p, n)) else "")) for n in sorted(os.listdir(p))]
class _DB: fs = _FS()
dbutils = _DB()
cells = [c for c in json.load(open(nb))["cells"] if c["cell_type"] == "code"]
ns = dict(spark=spark, dbutils=dbutils)
for i, c in enumerate(cells):
    src = "".join(c["source"])
    if "CATALOG       = " in src:   # config cell: point at local folders, no catalog
        import re; src = re.sub(r'CATALOG\s*= "[^"]*"', 'CATALOG = ""', src, count=1)
        src = src.replace('VOLUME_BASE = f"/Volumes/{CATALOG}/{VOLUME_SCHEMA}/{VOLUME}"', f'VOLUME_BASE = "{work}/vol"')
    t = time.time(); print(f"\n######## cell {i} ########", flush=True)
    exec(compile(src, f"cell{i}", "exec"), ns)
    print(f"[cell {i} took {time.time()-t:.0f}s]", flush=True)
print("\nLOCAL TEST COMPLETE")
