# Divvy Bike-Share Lakehouse on Azure Databricks

Bronze → Gold lakehouse (Spark + Delta Lake) for the Udacity *Data Lakes* project.

## Deliverables
| File | Purpose |
|---|---|
| `Divvy_Databricks_Lakehouse.ipynb` | The notebook. Import into Azure Databricks and run top to bottom. |
| `Divvy_Star_Schema.pdf` | Star schema: `fact_trip`, `fact_payment`, `dim_rider`, `dim_station` (start/end roles), `dim_date`. |
| `RUNBOOK.md` | Click-by-click guide for the Udacity lab. |
| `tools/` | Generators and the local test harness (not part of the submission). |

## Input files (exact names, no header row)
`riders.csv`, `payments.csv`, `stations.csv`, `trips.csv` (from the Udacity dataset ZIP). Upload them to `/Volumes/<catalog>/<schema>/<volume>/raw/`.

## Architecture
```
Volume /raw/*.csv ──PySpark──▶ Volume /bronze_delta/<entity>/ (Delta files)
                                   │ temp view + INSERT OVERWRITE (Spark SQL)
                                   ▼
                     <catalog>.divvy_bronze.{rider,payment,station,trip}   (managed Delta)
                                   │ PySpark
                                   ▼
        <catalog>.divvy_gold.{dim_rider,dim_station,dim_date,fact_trip,fact_payment}
                         (write.format("delta").mode("overwrite").saveAsTable)
```
Unity Catalog cannot register a table whose `LOCATION` is inside a volume, hence the separate managed Bronze tables.

## Notebook order
0 Config → 1 Inspect → 2 Bronze Delta files → 3 Bronze tables (Spark SQL) → 4 Dimensions → 5 Facts → 6 Write Gold → 7 Validation → 8 Analytics → 9 Summary.

## Prerequisites
Unity Catalog–enabled compute, Databricks Runtime 13.3 LTS or newer, single node `Standard_DS3_v2`/`Standard_DS4_v2` (lab limit 8 cores). A writable catalog/schema/volume.

## Validation
Section 7 asserts: CSV = Bronze files = Bronze tables = Gold counts, unique keys, no NULL rider/date keys, payment sum reconciliation, Delta format, and that a second Gold build gives identical counts.

## Export evidence
In Databricks: **File ▸ Export ▸ IPython Notebook (.ipynb)** after a successful run so cell outputs are included. Put it and the PDF in one ZIP.

## Known source limitations
* Files have no header row; column names come from the ERD.
* `station_id` is a string (ERD shows INT).
* `is_member` is a current snapshot, not history.
* Trips cover Feb 2021–Jan 2022; payments 2013–2022. Payments are not linked to trips.
* A small number of trips have zero or negative duration; they are flagged (`duration_quality`) and excluded from duration analytics.

## Verified run (Azure Databricks, Unity Catalog, DBR 15.4 LTS, single node)
All 23 validation checks passed. Counts: 4,584,921 trips, 1,946,607 payments, 75,000 riders, 838 stations, 3,288 dates.
Payment total 19,457,105.25 reconciles with the CSV. 116 negative and 81 zero-duration trips are flagged (`duration_quality`), not deleted.
`fact_trip` shows two `CREATE OR REPLACE TABLE` versions in Catalog Explorer History (Gold overwrite re-run). Screenshots are in `evidence/` (01-14, 25-32 are Databricks UI captures; 15-24 are rendered from the exported notebook's stored outputs by `tools/render_evidence.py`).
