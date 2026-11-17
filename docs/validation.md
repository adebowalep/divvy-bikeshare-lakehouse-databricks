# Validation report

This report separates four kinds of evidence, because they are not equally strong.

| Kind | What it is | Where the proof is |
|---|---|---|
| **Azure, historical (v1)** | The first notebook version, run on Azure Databricks | [`evidence/databricks_run_v1/`](../evidence/databricks_run_v1) and images 15 to 24 |
| **Local, full dataset (v2)** | The corrected notebook, all of its cells, run on a local Spark and Delta Lake session against the same four CSV files | Output reproduced below. The Azure outputs for v2 do not exist yet. |
| **Unit tests (v2)** | 62 automated tests on small synthetic fixtures and on the committed dashboard files | [`tests/`](../tests) |
| **Not verified** | Listed at the end | |

## 1. What the original Azure run proved, and what it did not

The v1 notebook printed 23 passing checks. Reading the code shows that one of the 23 was not a test: the duration-quality entry passed unconditionally (`check(..., True, counts)`), so it only reported counts. The honest description of v1 is **22 assertions plus one informational diagnostic**. The 22 assertions were row-count lineage from CSV to Gold, key uniqueness, no NULL rider or station keys, valid date keys, age range, payment reconciliation, no conflicting duplicates, Delta format and equal row counts after a second Gold build.

Two things v1 did not check:

- **A rebuild was compared by row count only.** Equal counts do not show equal content.
- **Nothing tested the duration rule or the age rule.** Both had defects (next section).

The Azure outputs themselves were not changed, apart from the privacy redaction described in section 6.

## 2. Defects found and fixed

| Defect | Evidence | Fix |
|---|---|---|
| Trips longer than 24 hours counted as valid (`OK`) | v1 `build_fact_trip` used `.otherwise("OK")`. The related Synapse warehouse flags `over_24h`. | New category `OVER_24H` for `seconds > 86400` |
| A NULL duration would also be classed `OK` | `NULL < 0` and `NULL = 0` are not true, so `otherwise` caught it. The data has none, so no result changed. | New category `MISSING`, checked first |
| 29 February birthday | v1 compared month and day numbers, so on 28 February 2021 someone born on 29 February 2000 was 20. The Synapse warehouse (T-SQL `DATEADD`) and the new code give 21. | Anniversary built as a real date and clamped to the last day of the month |

The tests in `tests/test_transforms.py` cover these cases. I ran the v1 expressions on the same inputs by hand: an 86,401 second ride and a NULL duration both came back `OK`, and the leap-day case came back 20. I did not run the whole test file against the v1 code.

The corrected figures were not copied from the Synapse project. They come from running the corrected notebook on the data, and they agree with the Synapse counts:

| Category | Azure v1 | Local v2 |
|---|---:|---:|
| Total trips | 4,584,921 | 4,584,921 |
| `NEGATIVE` | 116 | 116 |
| `ZERO` | 81 | 81 |
| `OVER_24H` | not separated | 1,277 |
| `OK` (valid) | 4,584,724 | 4,583,447 |

## 3. Local run of the corrected notebook, full dataset

Every one of the 22 code cells of the 44-cell notebook ran to completion on the real files. Spark 3.5.3 and Delta Lake 3.2.1, session timezone UTC, 4,584,921 trips. The validation cell printed the following summary; the full table is saved verbatim in [`evidence/local_run_v2/validation_output.txt`](../evidence/local_run_v2/validation_output.txt):

```
28 of 28 assertions passed; 1 informational diagnostic(s) not counted.
ALL VALIDATION ASSERTIONS PASSED
```

| # | Assertion | Observed |
|---|---|---|
| 1 to 4 | Row-count lineage CSV = Bronze files = Bronze table = Gold, for trips, payments, riders, stations | 4,584,921 / 1,946,607 / 75,000 / 838 |
| 5 to 11 | Unique keys in both facts, `rider_key`, `rider_id`, `station_key`, `station_id`, `date_key` | 0 duplicates each |
| 12 | No conflicting duplicate source rows in dimensions | rider 0, station 0 |
| 13 to 14 | `rider_key` not NULL in both facts | 0 NULL |
| 15 | Start and end station keys not NULL | 0 NULL |
| 16 to 17 | `rider_age_at_trip` not NULL and within 0 to 110 | 0 outside |
| 18 | Every fact date key exists in `dim_date` | pass |
| 19 | `dim_date` has no gaps | 2013-02-01 to 2022-02-01, 3,288 days |
| 20 | Duration categories are from the defined set | NEGATIVE, OK, OVER_24H, ZERO |
| 21 | Categories add up to `fact_trip` | 4,584,921 |
| 22 | Every `OK` trip has 0 < seconds <= 86,400 and no other trip does | pass |
| 23 | Valid trips equal an independent count taken from Bronze | 4,583,447 = 4,583,447 |
| 24 to 25 | Payment total CSV equals `fact_payment`; no NULL or non-positive amounts | $19,457,105.25 on both sides |
| 26 | All Gold tables are Delta | pass |
| 27 | Second Gold build gives identical row counts | pass |
| 28 | Second Gold build gives identical content fingerprints, all five tables | pass |

The one informational row reports `{'OK': 4583447, 'ZERO': 81, 'NEGATIVE': 116, 'OVER_24H': 1277}` and is not counted. The content fingerprint is an order-independent hash of every column, so it detects a changed value that an equal row count would hide. It is a strong consistency check, not a cryptographic proof, and it was run twice in the same session, not across sessions.

Median ride length is unchanged at 12.2 minutes. Other results moved because 1,277 very long rides no longer count:

| Result | Azure v1 | Local v2 |
|---|---:|---:|
| Mean ride | 21.8 min | 19.8 min |
| Longest weekday on average | Sunday 27.8 min | Sunday 24.9 min |
| Member vs casual average ride | 21.9 vs 21.3 min | 19.9 vs 19.8 min |
| Busiest hour (17:00) trips | 470,147 | 470,046 |
| Busiest start station trips | 80,336 | 80,299 |
| Longest average ending station (200+ trips) | Base - 2132 W Hubbard Warehouse, 3,750 min over 468 trips | the same station, 171 min over 338 trips |
| Riders with no trips in the window | 37,291 | 37,297 |

Payment totals, the yearly and quarterly payment figures and the payment bands by age at account start do not depend on duration and are identical to v1. The age fix changed one trip: the only trip by a rider born on 29 February that falls on 28 February of a non-leap year (measured on the full data).

## 4. Age function tests

14 cases in `tests/test_transforms.py` run the real PySpark expression `age_years`:

| Case | Event / birth | Expected | Result |
|---|---|---:|---|
| Birthday already occurred | 2021-06-15 / 1990-03-10 | 31 | pass |
| Birthday not yet occurred | 2021-02-01 / 1990-03-10 | 30 | pass |
| Exactly on birthday | 2021-03-10 / 1990-03-10 | 31 | pass |
| Day before birthday | 2021-03-09 / 1990-03-10 | 30 | pass |
| 29 Feb birth, leap year, 28 Feb | 2024-02-28 / 2000-02-29 | 23 | pass |
| 29 Feb birth, leap year, 29 Feb | 2024-02-29 / 2000-02-29 | 24 | pass |
| 29 Feb birth, non-leap year, 27 Feb | 2021-02-27 / 2000-02-29 | 20 | pass |
| 29 Feb birth, non-leap year, 28 Feb | 2021-02-28 / 2000-02-29 | 21 | pass |
| 29 Feb birth, non-leap year, 1 Mar | 2021-03-01 / 2000-02-29 | 21 | pass |
| 28 Feb birth, leap year | 2024-02-28 / 2000-02-28 | 24 | pass |
| Missing birth date | | NULL | pass |
| Missing event date | | NULL | pass |
| Event before birth | 1990-01-01 / 1995-01-01 | NULL | pass |
| Event on the birth date | 1995-01-01 / 1995-01-01 | 0 | pass |

## 5. Other unit tests

62 tests passed in about two minutes on Python 3.13, PySpark 3.5.3, Delta Lake 3.2.1 and Java 8.

| File | Tests | Covers |
|---|---:|---|
| `test_transforms.py` | 37 | The 14 age cases above; duration categories for negative, zero, 1 s, 600 s, exactly 86,400 s, 86,401 s, 400,000 s and NULL; time bands; one-row-per-trip fact with unmatched station and rider references kept as NULL keys; deduplication and conflict counting; deterministic surrogate keys; `date_key` format, weekday and weekend flags; contiguous `dim_date`; payment total reconciliation; a Delta overwrite rebuilt twice with equal fingerprints; fingerprint sensitivity to a changed value with an equal row count |
| `test_dashboard.py` | 20 | `data.json` sections and reconciliation (quality categories, every breakdown equals valid trips, payments equal the total); the HTML is rebuilt identically from the template by the script, run from another directory; clear failure on a missing input; sections present; no unresolved placeholder; no fetch or XHR; the only external reference is optional Google Fonts; no rider names or addresses in published files; a real headless-browser render with the title, trip count, at least 10 SVG charts, no `undefined` or `NaN` in visible text and no JavaScript errors in the console |
| `test_notebook_generation.py` | 5 | The generator reproduces the committed notebook; the notebook embeds the tested functions verbatim; the generator refuses to overwrite a notebook with outputs; the executed Azure notebook still has its 23 executed cells and results |

## 6. Dashboard interaction checks (manual, in a real browser)

Done by hand in the built-in browser, not automated, on the final `dashboard/index.html`:

- All six sections render, 11 SVG charts, no console errors.
- Every segmented control produced a distinct view: the four colour palettes, heatmap metric, station metric, Top 5 or Top 10, age metric, age group (Both, Members, Casual), payments (per quarter, cumulative).
- The theme button cycles automatic, light, dark and back.
- At a 375 px wide phone viewport the page has no horizontal overflow (`scrollWidth` equals `innerWidth`). I could not capture a screenshot of the phone layout in that session, so its visual quality was not inspected.

## 6a. Live deployment check

On 17 November 2026, after GitHub Pages was enabled (branch `main`, folder `/`), the public URL `https://adebowalep.github.io/divvy-bikeshare-lakehouse-databricks/dashboard/` returned HTTP 200, as did `/` and `/dashboard/data.json`. In a real browser the page showed the title, 11 SVG charts, the six sections and all KPI and quality figures, with no `undefined` or `NaN` in the text and no console errors on a clean load. The seven controls produced distinct views, and the served `data.json` equals the committed file (4,583,447 valid trips, 1,277 over 24 hours). Not checked on the live site: other browsers, and the phone layout beyond the earlier overflow measurement.

## 7. Privacy review

| Item | Finding | Action |
|---|---|---|
| Executed notebook, riders preview | Five riders' first name, last name and address in the saved output, in the HTML table and in the raw result data | Replaced with `[redacted]`; nothing else changed |
| Evidence image 16 | Showed the same five riders | Re-rendered from the redacted notebook |
| Other evidence images, `dashboard/data.json`, `dashboard/index.html` | No names or addresses. The other images are renders of other notebook cells, and a search of the whole notebook found these fields only in the riders preview. The dashboard files are checked by an automated test. | None |
| New notebook | Previews drop `first`, `last`, `address` | Done in the notebook code |
| Git history | Public history still contains the original values: commit `81673d8` (notebook) and `7c7dcfb` (image 16) | **Not rewritten.** Needs your decision. |
| Bronze layer | Bronze keeps every source column, including names and addresses, as a faithful copy; Gold leaves them out | Documented |

The riders file is synthetic (generated names and addresses), which lowers the risk. It does not remove the value of keeping real-looking personal fields out of a public repository.

**Provenance and redistribution.** The CSV files come with the Udacity course project and are not in this repository (they are excluded by `.gitignore`). The trip records are derived from the public Divvy bike-share data for Chicago. I did not verify the licence terms of the course copy or of the original Divvy data, so I cannot state that redistribution is permitted. Keeping the files out of the repository avoids the question.

## 8. Not verified

- **The corrected notebook has not run on Azure Databricks.** Its v2 results come from a local Spark and Delta Lake session (same code, same CSV files, UTC). A Databricks run could differ in storage and Unity Catalog behaviour. The Azure screenshots and the executed notebook still show v1.
- Unity Catalog permission behaviour, managed table placement and Catalog Explorer history for v2.
- Behaviour on Databricks runtimes other than 15.4 LTS.
- Phone layout appearance (only overflow was measured), and browsers other than Chromium.
- Dashboard performance, accessibility audit and screen reader behaviour.
- The licence terms of the source data.
