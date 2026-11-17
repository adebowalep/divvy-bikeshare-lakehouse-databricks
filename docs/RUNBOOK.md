# Runbook: running the Divvy lakehouse in the Udacity Azure Databricks lab

Menu names can differ slightly between Databricks UI versions. If a step doesn't match what you see, screenshot it and send it to me.

## 1. Before "Access Lab" (do this at home, not on the clock)
1. Have these ready in one folder: `riders.csv`, `payments.csv`, `stations.csv`, `trips.csv` (unzipped, in `data/`), `Divvy_Databricks_Lakehouse.ipynb`, `Divvy_Star_Schema.pdf`.
2. The lab session is timed and you only get **10 attempts**. Open the lab only when you are ready to work for the whole session. Don't click Access Lab "just to look".
3. `trips.csv` is about 440 MB. Make sure your internet is stable; upload is the slowest step.
4. Skim the notebook once so you know the three names you must edit: `CATALOG`, `VOLUME_SCHEMA`, `VOLUME`.

## 2. Open the lab and the workspace
1. Udacity classroom → *Cloud Lab – Data Lakes Project* → **Access Lab**. Wait about 5–6 minutes for credentials.
2. Copy the Azure username/password shown (use the copy icons). Click the **Microsoft Azure** link and sign in. Keep the credentials private.
3. In the Azure portal search bar type **Azure Databricks** → open the one workspace that already exists (inside the pre-made resource group). Don't create a new workspace.
4. On the workspace Overview click **Launch Workspace**. You should land on a Databricks page with a left sidebar (Workspace, Catalog, Compute …) and the Microsoft Azure + databricks header.

## 3. Check compute BEFORE creating anything
1. Sidebar → **Compute**. Look at the existing list.
2. If a cluster already exists and shows a green/running state, **use it** if it is Unity Catalog capable (Access mode "Single user" or "Shared", Runtime 13.3 LTS or newer) and is single node. Click it to check Configuration.
3. If you must create one: **Create compute** →
   * Policy: Unrestricted / default (whatever the lab allows)
   * **Single node**
   * Access mode: **Single user** (your user)
   * Databricks Runtime: an **LTS** version, **13.3 or newer** (e.g. 13.3 LTS / 14.3 LTS / 15.4 LTS). Photon off.
   * Node type: **Standard_DS3_v2** (4 cores) preferred; Standard_DS4_v2 (8 cores) only if nothing else uses cores.
   * Set auto-termination to 30–60 minutes. Click **Create compute**.
4. **Error "OperationNotAllowed … Total Regional Cores quota … Limit: 8, Current Usage: 8"** means all 8 cores are used.
   * Look at Compute for clusters you created (not ones you did not create) and **Terminate** the unused ones, wait about 2 minutes, retry with Standard_DS3_v2 single node.
   * Do **not** request a quota increase.
   * If it still fails with nothing of yours running, send udacity-labsupport@udacity.com (subject "Error in Lab") the course/lesson/page name, steps, and a screenshot of the error and of the Compute list, as the lab page asks.

## 4. Catalog Explorer: find/create a volume and upload the CSVs
1. Sidebar → **Catalog**. Expand catalogs (e.g. `main`, `workspace`, or a catalog named for your lab).
2. Look for an existing **Volume** (the icon under a schema → "Volumes"). If one exists and you can open it, use it. Write down **catalog.schema.volume**.
3. If none: choose a schema you can write to (often `default`) → **Create** → **Create volume** → name `divvy`, type **Managed** → Create. If "Create" is greyed out or denied, send me a screenshot of the Catalog page.
4. Open the volume → **Upload to this volume** → create a folder named `raw` (the "Create directory" button), open it, then upload the four CSV files into `raw`.
5. Verify: you should see exactly `riders.csv`, `payments.csv`, `stations.csv`, `trips.csv` in `raw`.
6. Click `riders.csv` (or the `raw` folder's ⋮ menu) → **Copy path**. It looks like `/Volumes/<catalog>/<schema>/<volume>/raw/riders.csv`. The three words after `/Volumes/` are your `CATALOG`, `VOLUME_SCHEMA`, `VOLUME`.

## 5. Import the notebook
1. Sidebar → **Workspace** → your user folder → ⋮ (or **Create**/ right-click) → **Import**.
2. Choose **File**, drop `Divvy_Databricks_Lakehouse.ipynb`, click **Import**. Open it.
3. Top right: **Connect** → select your running cluster.
4. In cell "0. Configuration" edit `CATALOG`, `VOLUME_SCHEMA`, `VOLUME` to the values from step 4.6. Leave the rest.
5. Run the next cell (file check). It must print `OK - all source files found`. If it asserts, your names or upload are wrong (see section 8).

## 6. Execute cell by cell
Use **Shift+Enter** per cell the first time; don't "Run all" until you've seen section 3 succeed.
| Section | What happens | Confirm before continuing |
|---|---|---|
| 0 config + file check | names set, files found | `OK - all source files found` |
| 1 inspection | CSVs read as strings, typed, profiled (a minute or two for trips) | `parse failures: none` for all four; `duplicate keys: 0`; rider `account_end_date` is the only column with nulls |
| 2 Bronze Delta files | 4 Delta folders in `bronze_delta/` | each line shows `match=True` |
| 3 Bronze tables | schemas + 4 tables created and loaded | each table `format=delta` and rows equal the file counts. If CREATE SCHEMA is denied, set `BRONZE_SCHEMA`/`GOLD_SCHEMA` to existing schemas you can write to |
| 4–5 | function definitions only | no errors |
| 6 Gold | 5 tables written (overwrite) | counts printed; format `delta` for all five |
| 7 Validation | table of checks, then second Gold build | final line `ALL VALIDATION CHECKS PASSED`. Read the `detail` column for station NULL counts and `duration_quality` counts |
| 8 Analytics | tables to read | results appear; results make sense |
| 9 Summary | table list | |
Expected numbers are not pre-written: trust the counts your own run prints, and check they agree with each other.

## 7. Verify in Catalog Explorer
1. Catalog → your catalog → schema `divvy_bronze` (4 tables) and `divvy_gold` (5 tables).
2. Click a table → **Details**: *Data source format / Provider* = **DELTA**. **Sample Data** tab shows rows. **History** tab lists writes (`WRITE`/`CREATE OR REPLACE TABLE`).
3. Row count: in a new cell run `spark.table("<catalog>.divvy_gold.fact_trip").count()`.
4. Overwrite proof: section 7 already runs the Gold build twice and compares counts. You can also re-run section 6 and re-check the count; it must not grow, and History shows an extra overwrite version.

## 8. Troubleshooting
| Problem | Check | Safe fix |
|---|---|---|
| Path wrong / file not found | run `display(dbutils.fs.ls("/Volumes/<c>/<s>/<v>/raw"))` | Fix the three names; use Copy path from Catalog Explorer |
| Permission denied (UC) | error mentions privilege/`USE SCHEMA`/`CREATE TABLE` | use a schema you own; set `BRONZE_SCHEMA`/`GOLD_SCHEMA`; show me the error |
| Compute not UC-compatible / `/Volumes` unsupported | cluster shows "No isolation shared" or runtime < 13.3 | create a new single-node cluster with Single user access mode, LTS 13.3+ |
| Unsupported runtime | `spark.version` below 3.4 | choose 13.3 LTS or newer |
| Cluster core/quota error | see section 3 step 4 | terminate your own idle cluster, retry DS3_v2 single node |
| CSV parsing | section 1 reports parse failures per column | open the CSV in Catalog preview; confirm no header row and same columns; show me the output |
| Unknown station IDs | validation detail shows NULL station keys | not an error by itself; those trips stay in the fact with NULL station key |
| Table-name collision | "table already exists" with another schema/owner | change `BRONZE_SCHEMA`/`GOLD_SCHEMA` names (don't drop shared tables) |
| Delta path overlap | error about "overlap" / LOCATION in a volume | never create tables with LOCATION inside the volume; this notebook doesn't |
| Gold write not allowed | `saveAsTable` permission error | use a schema where you have CREATE TABLE |
| Session/credentials expired | login page / cluster gone | all lab data is deleted at expiry; start a new attempt, re-upload, re-run; export early |

## 9. Collect the submission
1. After a successful run, with outputs visible: notebook menu **File → Export → IPython Notebook (.ipynb)**. (Do **not** pick Source file/HTML/DBC; the rubric wants Jupyter.)
2. Verify the export: open the `.ipynb` in a text editor or Jupyter and confirm cells contain `"outputs"` with your results. If outputs are missing, re-export after Run all.
3. Put the exported `.ipynb` and `Divvy_Star_Schema.pdf` into one folder and ZIP it (Mac: right-click → Compress; Windows: Send to → Compressed folder).
4. Upload the ZIP in Udacity → **Submit project → Zip File**.

## 10. Clean up
1. Export everything first (the lab deletes it at expiry).
2. Compute → **Terminate** your own cluster (don't delete shared/pre-created resources).
3. Final tick-list:
   - [ ] PDF has 2 facts, rider/station/date dims, keys readable
   - [ ] `fact_trip` has duration + age at trip; `fact_payment` has amount
   - [ ] Notebook reads CSV from a UC volume and writes Bronze Delta files to the volume
   - [ ] Spark SQL creates/loads Bronze tables
   - [ ] Gold written with `format("delta").mode("overwrite").saveAsTable`
   - [ ] Validation shows `ALL VALIDATION CHECKS PASSED`; Bronze vs Gold distinct
   - [ ] Exported `.ipynb` includes outputs; ZIP contains `.ipynb` + `.pdf`

---
## Notes from the real lab run (what differed from the generic steps above)
* The compute form defaults to 2–8 autoscaling workers. Tick **Single node** first, or the 8-core quota is exceeded.
* `Standard_DS3_v2` and `Standard_DS4_v2` were greyed out in West US. A single-node `Standard_D4ds_v5` (4 cores) created without a quota error, with Runtime 15.4 LTS, Photon off, access mode Dedicated.
* The sidebar uses newer names (Catalog, Compute, Jobs & Pipelines). Volume path used: `/Volumes/<catalog>/default/divvy/raw`; the catalog was named after the workspace.
* Catalog Explorer **History** needs a running cluster attached ("Select compute") before it shows versions.
* After a full run, **File > Export > IPython Notebook (.ipynb)** kept every cell output (text and result tables).
