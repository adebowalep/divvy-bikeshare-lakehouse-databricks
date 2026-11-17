# Reproduction guide

There are three ways to reproduce parts of this project, in increasing order of effort and cost.

| Goal | Needs | Cost |
|---|---|---|
| Run the unit tests | Python, Java 8 or newer, `pip install -r requirements-dev.txt` | none |
| Run the notebook on your own machine | the same, plus the four CSV files | none |
| Run the notebook on Azure Databricks | an Azure Databricks workspace with Unity Catalog | compute time while the cluster runs |

## 1. Tests (no Azure account)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pytest
```

The Spark tests start a small local session and take a few minutes. The dashboard browser tests are skipped if no Chromium-based browser is installed. The tests were run with Python 3.13, PySpark 3.5.3, Delta Lake 3.2.1 and Java 8.

## 2. Run the notebook locally

The notebook is written for Databricks, but [`tools/local_test.py`](../tools/local_test.py) runs its cells on a local Spark and Delta Lake session. It changes only the volume path and `dbutils.fs.ls`; all transformation code is the notebook's own.

```bash
python tools/local_test.py notebooks/Divvy_Databricks_Lakehouse.ipynb /path/to/folder/with/csvs /path/to/workdir
```

Allow roughly 1.5 GB of free disk space and several minutes for the 4.6 million trips. The Gold tables are left in `<workdir>/warehouse/divvy_gold.db`.

## 3. Run on Azure Databricks

**Prerequisites.** A workspace with Unity Catalog, permission to create a schema and tables in a catalog, and a volume to hold the files. The Divvy data files are not in this repository: they come with the Udacity project and are described in the README.

**Compute.** A single-node cluster is enough. The run used Databricks Runtime 15.4 LTS, access mode Dedicated (single user), Photon off, on a 4-core `Standard_D4ds_v5`. Any Unity Catalog capable runtime from 13.3 LTS should work, but only 15.4 LTS was tested. In the lab, the compute form defaulted to autoscaling workers and exceeded the 8-core regional quota until Single node was ticked first.

**Steps.**

1. In Catalog, create a managed volume (for example `divvy`) in a schema you can write to. Create a folder `raw` and upload `riders.csv`, `payments.csv`, `stations.csv` and `trips.csv`. `trips.csv` is about 440 MB.
2. Import `notebooks/Divvy_Databricks_Lakehouse.ipynb` (Workspace, Import, File) and attach the cluster.
3. In the configuration cell set `CATALOG`, `VOLUME_SCHEMA` and `VOLUME`. Leave the rest. The three words after `/Volumes/` in a file's copied path are these values.
4. Run the cells in order, or Run all. The notebook stops with a clear message if the source files are missing or any type conversion fails.
5. Confirm the validation cell ends with `ALL VALIDATION ASSERTIONS PASSED`, and read the `INFO` rows separately.
6. In Catalog, check `divvy_bronze` (4 tables) and `divvy_gold` (5 tables). Each should show format Delta. The History tab of a Gold table shows one `CREATE OR REPLACE TABLE` version per build, and the notebook builds Gold twice. History needs a running cluster attached.

**Cost and cleanup.** Compute is billed while the cluster runs, so set auto-termination (30 to 60 minutes) and terminate the cluster when you finish. Tables and files created by the notebook live in the catalog and volume; drop the `divvy_bronze` and `divvy_gold` schemas and the volume folders `bronze_delta/` and `raw/` if you want to remove them. The notebook never writes to `raw/`.

**Troubleshooting.**

| Symptom | Likely cause and fix |
|---|---|
| `Missing in ...: [...]` in the first cell | Wrong `CATALOG`, `VOLUME_SCHEMA`, `VOLUME`, or the files are not in `raw/`. Use Copy path in Catalog. |
| Permission error creating a schema | Set `BRONZE_SCHEMA` and `GOLD_SCHEMA` to existing schemas you can write to. |
| Error about overlapping paths or `LOCATION` | Do not create tables with a location inside the volume. The notebook does not. |
| `Type conversion failures` | The file has a header row or a different format. The failure counts name the column. |
| Cluster quota error | Use Single node and a 4-core node type; do not request a quota increase. |
| Cluster is not Unity Catalog capable | Use a Dedicated or Standard access mode and Runtime 13.3 LTS or newer. |

## Rebuilding the documentation assets

```bash
python tools/build_notebook.py                      # source notebook (refuses to overwrite one with outputs)
python tools/export_dashboard_data.py <workdir>/warehouse/divvy_gold.db
python tools/build_dashboard.py
python tools/shoot_dashboard.py                     # needs Edge, Chrome or Chromium; or --browser PATH
python tools/draw_architecture.py
python tools/draw_star_schema.py
```

Every script resolves paths from the repository root, so they can be run from any directory.
