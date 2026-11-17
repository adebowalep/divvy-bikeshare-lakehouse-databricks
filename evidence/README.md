# Evidence

What is here, what it proves, and what is deliberately not published.

## 1. The executed Azure Databricks notebook (authentic)

[`databricks_run_v1/Divvy_Databricks_Lakehouse_executed_v1.ipynb`](databricks_run_v1/Divvy_Databricks_Lakehouse_executed_v1.ipynb) is the notebook exported from Azure Databricks after a full run (Runtime 15.4 LTS, Unity Catalog). Every code cell has an execution record and the saved outputs are the ones Databricks produced.

- It is the **first version** of the notebook. It ran under the original duration rule, which counted a trip longer than 24 hours as valid, and the original age rule (see [`../docs/validation.md`](../docs/validation.md)). Its recorded counts are historical Azure results and are not edited.
- **One change was made to it, for privacy:** the five-row preview of the riders file showed first names, last names and addresses. Those three columns were replaced with `[redacted]` in the saved outputs (both the HTML table and the raw result data). Nothing else was changed. The synthetic names and addresses remain in earlier Git commits; see the privacy section of the validation report.
- Databricks truncated its HTML result tables to about ten rows when it exported them. That is how the file was produced.

## 2. Renders of the saved outputs (images 15 to 24)

Images `15_` to `24_` in this folder are **renders of cells from the notebook above, produced by [`tools/render_evidence.py`](../tools/render_evidence.py)**. They show the code and output exactly as stored in the notebook. They are **not screenshots of the Databricks interface.** Image 16 was re-rendered after the redaction described above; the others are unchanged.

| File | Notebook section |
|---|---|
| `15_config_and_file_check.png` | Configuration and source file check |
| `16_section1_inspection.png` | Source inspection (rider names and addresses redacted) |
| `17_section2_bronze_delta_files.png` | Bronze Delta files, counts match the CSV |
| `18_section3_bronze_tables.png` | Registered Bronze tables |
| `19_section6_gold_tables.png` | Gold tables written |
| `20_section7_validation.png` | Validation table (original 23-check version) |
| `21_analytics_weekday_station.png` | Analytics 8.1 and 8.2 |
| `22_analytics_age_payments.png` | Analytics 8.3 to 8.5 |
| `23_analytics_extra_credit.png` | Analytics 8.6 |
| `24_summary.png` | Summary |

## 3. Databricks interface screenshots (not published)

During the lab run, 21 screenshots of the Azure and Databricks interfaces were taken (resource list, workspace, compute settings, catalog, volume, schemas, Delta details and history). They exist only on the author's computer. They were never committed and are excluded by `.gitignore`, because they show the temporary lab account name. An earlier version of the README referred to them by number; those references were wrong for the published repository and have been removed.

## 4. Local verification (new, not from Azure)

The corrected notebook in [`../notebooks/`](../notebooks) was run end to end on a local Spark and Delta Lake session against the same four CSV files, and the unit tests in [`../tests/`](../tests) were run. These results are recorded in [`../docs/validation.md`](../docs/validation.md), clearly separated from the Azure results above. The literal printed validation table of that local run is in [`local_run_v2/validation_output.txt`](local_run_v2/validation_output.txt); it is a local Spark and Delta Lake run, not an Azure one. The corrected notebook has **not** been re-run on Azure Databricks, so it has no Azure outputs yet.
