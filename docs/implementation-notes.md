# Implementation notes

The engineering decisions behind the notebook, and what each one protects against.

## Explicit schemas, no inference

The four CSV files have no header row, so Spark's header and schema inference would have used the first data row as column names and guessed types from a sample. Instead each file is read as all strings with a column list taken from the ERD, then converted with `try_cast`. `try_cast` returns NULL where `cast` would fail the job, and the notebook counts every value that was present in the file but became NULL. Any such failure stops the run, so a bad type can never reach a Delta table as a silent NULL. Station IDs came out as strings (`KA1503000012`), which the ERD had drawn as integers.

## Delta format and overwrite

Every layer is written in Delta format with overwrite semantics: Bronze files with `mode("overwrite")`, Bronze tables with `INSERT OVERWRITE`, and Gold with `saveAsTable` in overwrite mode with `overwriteSchema`. Running the notebook again replaces content and never appends, which is what makes a partial or repeated run safe. The Delta history shows each Gold rebuild as a `CREATE OR REPLACE TABLE` version.

## Unity Catalog

The source files and Bronze files live in a volume, and the tables are managed tables in a catalog and schema. Unity Catalog rejects a table whose `LOCATION` is inside a volume, so the Bronze tables are loaded from the Bronze files instead of pointing at them (see [architecture.md](architecture.md)).

## Surrogate keys

`rider_key` and `station_key` come from `row_number()` over the business ID. The ordering makes them repeatable for a full rebuild, which is what the rebuild fingerprint check relies on. The window has no partitioning, so Spark moves all rows through one task for that step. That is fine for 75,000 riders and 838 stations and would need a different approach (hashing, or an identity column) for a large dimension.

## Deduplication

Dimensions are deduplicated by business key with an explicit tie-break (the latest account start for riders, the first name for stations). The number of keys whose duplicate rows disagreed is returned and asserted to be zero. In this data there were no duplicate keys at all, so the tie-break is a safeguard that was not exercised on the real files. The unit tests exercise it with a fabricated duplicate.

## Joins

Facts use left joins to the dimensions, so a missing reference shows up as a NULL key and is counted instead of dropping the fact row. The small dimensions are broadcast. Trips and payments are never joined to each other: the source links payments to riders only, so a direct join would repeat each payment once per ride. The extra-credit analysis sums each to rider and month first, and only then combines them.

## Duration and age rules

The rules are in [`src/divvy_transforms.py`](../src/divvy_transforms.py) and described in [data-model.md](data-model.md). Two decisions are worth stating.

1. **Over 24 hours is invalid.** The first version treated every positive duration as valid. A ride longer than a day is not a plausible single ride, and the related Synapse warehouse already excluded them, so the lakehouse now flags them as `OVER_24H` too. The rows are kept.
2. **29 February.** The first age function compared month and day numbers, so 28 February counted as before a 29 February birthday even in a non-leap year. The function now builds the anniversary date in the event year and clamps it to the last day of the month, so the anniversary is 28 February in non-leap years.

Neither rule was changed by editing the documented figures. Both were changed in code, with tests, and the full dataset was re-run (see [validation.md](validation.md)).

## Timezone

Timestamps are used exactly as stored, with no timezone conversion. A local test run on a laptop in a daylight-saving timezone once reported 124 negative durations instead of 116, because Spark converted the stored UTC timestamps to local time. Databricks runs in UTC and gave 116. The local scripts therefore set the Spark session timezone and the JVM timezone to UTC.

## One source of truth for the logic

The transformation functions live in `src/divvy_transforms.py`. [`tools/build_notebook.py`](../tools/build_notebook.py) copies them into the notebook cells verbatim, and the tests import the same module, so what is tested is what the notebook runs. The generator writes `notebooks/Divvy_Databricks_Lakehouse.ipynb` (source only). It refuses to overwrite any notebook that contains saved outputs, so the executed Azure notebook in `evidence/databricks_run_v1/` cannot be destroyed by regenerating the source.
