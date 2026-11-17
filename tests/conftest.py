"""Shared fixtures: a small local Spark session (UTC, Delta enabled) used by every Spark test."""
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "tools"))


@pytest.fixture(scope="session")
def spark():
    pyspark = pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession
    py = sys.executable
    os.environ["PYSPARK_PYTHON"] = py
    os.environ["PYSPARK_DRIVER_PYTHON"] = py
    builder = (SparkSession.builder.master("local[2]").appName("divvy-tests")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.shuffle.partitions", "4")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
               .config("spark.pyspark.python", py)
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    try:
        from delta import configure_spark_with_delta_pip
        builder = configure_spark_with_delta_pip(builder)
    except ImportError:
        pass
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
