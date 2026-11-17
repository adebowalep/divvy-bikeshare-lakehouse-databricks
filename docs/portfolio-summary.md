# Portfolio summary

Copy and adapt. Every figure below is backed by [validation.md](validation.md).

## Title

**Divvy Bike-Share | Azure Databricks Lakehouse & Analytics**

## One-paragraph description

A Bronze-to-Gold lakehouse for 4.6 million Chicago bike-share trips and 1.9 million payments, built with PySpark, Spark SQL and Delta Lake on Azure Databricks with Unity Catalog. Four headerless CSV files become a star schema with two facts and three dimensions. The notebook checks its own work with 28 assertions, the transformation logic has 62 automated tests that run without Azure, and a static dashboard presents the findings. The project began as a Udacity course brief; the engineering, testing and documentation are my own.

## What it shows

- **Lakehouse design on Azure Databricks.** Unity Catalog volumes, Bronze Delta files and managed Bronze tables, Gold dimensional tables, and a clear reason for having no Silver layer.
- **Careful ingestion.** Explicit schemas for headerless files, `try_cast` with parse-failure counts, and a workaround for the Unity Catalog rule that tables cannot sit inside a volume.
- **Dimensional modelling.** Two fact tables, a role-playing station dimension, deterministic surrogate keys and left joins that keep unmatched references visible.
- **Data quality as code.** A five-category duration rule, tested age logic including leap-day birthdays, assertions that are kept apart from diagnostics, and a rebuild check that compares row counts and content hashes.
- **Honest verification.** I found two defects by comparing this project with a related one on the same dataset, fixed them with tests, re-ran the full data locally, and recorded exactly what was and was not run on Azure.
- **Communication.** A dashboard with themes and palettes, an architecture diagram, a star schema, and a validation report that separates Azure evidence from local evidence.

## Key numbers

| | |
|---|---|
| Trips / payments | 4,584,921 / 1,946,607 |
| Gold tables | 5 (2 facts, 3 dimensions) |
| Valid trips | 4,583,447 (116 negative, 81 zero and 1,277 over 24 hours are flagged and kept) |
| Payment total | $19,457,105.25, equal in CSV and Gold |
| Validation | 28 of 28 assertions on the full data, local run of the corrected notebook |
| Tests | 62 passed |

## A talking point for interviews

The first version counted every positive duration as valid. When I compared its results with my Synapse warehouse on the same data, the valid-trip counts differed by exactly 1,277 trips. Those were rides longer than 24 hours. Excluding them lowered the mean ride from 21.8 to 19.8 minutes and shrank the apparent gap between members and casual riders from 0.6 to 0.1 minutes. I fixed the rule in code, tested it, and kept the original Azure outputs, unchanged apart from redacting synthetic rider names, and labelled them as the first version.

## Links

- Repository: https://github.com/adebowalep/divvy-bikeshare-lakehouse-databricks
- Live dashboard: https://adebowalep.github.io/divvy-bikeshare-lakehouse-databricks/dashboard/ (verified live)
- Related project (Azure Synapse and SQL, same dataset): https://github.com/adebowalep/divvy-azure-data-warehouse

## Skills

Azure Databricks · PySpark · Spark SQL · Delta Lake · Unity Catalog · dimensional modelling · data quality and validation · pytest · technical documentation
