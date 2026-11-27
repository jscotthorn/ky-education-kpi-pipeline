# 120 - CRDC collection year for out-of-school suspension

Base `feature/2025-26-kyrc26` at `1aef27b4a43dffb9757b4651a4dc687945479b52`. The branch had not moved. Issue #122. Record 0048 stays `accepted, code differs (follow-up open)` until this pull request merges.

## Rule

Record 0048: "Use the CRDC collection year as the KPI year everywhere. Move suspensions over in a later pull request."

Issue #122, Scott, 2026-10-25 12:05 PM ET: "Record 0048. Use the CRDC collection year as the KPI year everywhere; move suspensions over in a later PR (this issue)."

`etl/out_of_school_suspension.py` now stores the last four digits of `Collected School Year`. Values are unchanged. The output still has no collected-year column. `config/kde_sources.yaml` is unchanged. No new metric.

## Source files

Downloaded 27 Nov 2026, 1:00 PM ET, with `python3 data/prepare_kde_data.py out_of_school_suspension`. These are the files the module already reads. No mirror addition.

| File | URL | Bytes | MD5 | SHA256 |
| --- | --- | --- | --- | --- |
| `KYRC24_OVW_Student_Suspensions.csv` | `https://www.education.ky.gov/Open-House/data/HistoricalDatasets/KYRC24_OVW_Student_Suspensions.csv` | 2,281,971 | `0a8685cfc993b688b6307a75d6a6764f` | `0880b763565a6edbd03f9353ba7188e1eda71d92a24f4e1e3750c267a2a8a20f` |
| `KYRC26_OVW_Student_Suspensions.csv` | `https://kdeschoolreportcard.blob.core.windows.net/datasets/KYRC26_OVW_Student_Suspensions.csv` | 2,292,780 | `7b59e410b3453f5ae006fd7e4f91234f` | `33d03b14a1d003a683430c3106e97ed5520dccf0d2021db55f49bd210cb2d7b1` |

Both hashes match the 18 October 2026 record in notes/104. The KYRC26 `Last-Modified` on this fetch was `Wed, 04 Nov 2026 13:21:02 GMT`. The bytes match the October file. Content-MD5 `e1nkELNFP1rgBv1+T5EjTw==`.

| File | Source rows | `School Year` | `Collected School Year` | KPI year before | KPI year after |
| --- | --- | --- | --- | --- | --- |
| KYRC24 | 14,960 | `20232024` on every row | `20202021` on every row | 2024 | 2021 |
| KYRC26 | 14,949 | `20252026` on every row | `20212022` on every row | 2026 | 2022 |

Neither file has a blank collected year. Neither file has a district named `State` or `All Districts`, a school named `All Schools` or `---District Total---`, or a school code ending in `000`. There is no state row and no district total. Fayette County is school rows: 68 schools and 748 source rows in KYRC24, 69 schools and 759 source rows in KYRC26. District number is `165` in both files. Those school rows are not averaged into a district row.

## Before and after

The module was run on the base code, then again after `extract_year` was changed. Output is `out_of_school_suspension.csv`. Other module files were not regenerated. `kpi_master.csv` is not in the tree and was not rebuilt. Generated files were not committed.

Rows: 299,090 before and 299,090 after. Dropping `year` and `last_updated`, the two files are identical in row order. Value changes: 0. Suppressed changes: 0. Duplicate keys on `(district, school_id, school_name, year, student_group, metric)`: 0 before and 0 after inside this file.

Each collection lands in one KPI year. Each metric has the same row count in that year.

| Metric | 2024 before | 2026 before | 2021 after | 2022 after |
| --- | ---: | ---: | ---: | ---: |
| `in_school_suspension_total_count` | 14,960 | 14,949 | 14,960 | 14,949 |
| `in_school_suspension_with_disabilities_count` | 14,960 | 14,949 | 14,960 | 14,949 |
| `in_school_suspension_without_disabilities_count` | 14,960 | 14,949 | 14,960 | 14,949 |
| `out_of_school_suspension_multiple_total_count` | 14,960 | 14,949 | 14,960 | 14,949 |
| `out_of_school_suspension_multiple_with_disabilities_count` | 14,960 | 14,949 | 14,960 | 14,949 |
| `out_of_school_suspension_multiple_without_disabilities_count` | 14,960 | 14,949 | 14,960 | 14,949 |
| `out_of_school_suspension_single_total_count` | 14,960 | 14,949 | 14,960 | 14,949 |
| `out_of_school_suspension_single_with_disabilities_count` | 14,960 | 14,949 | 14,960 | 14,949 |
| `out_of_school_suspension_single_without_disabilities_count` | 14,960 | 14,949 | 14,960 | 14,949 |
| `out_of_school_suspension_total_count` | 14,960 | 14,949 | 14,960 | 14,949 |
| All ten metrics | 149,600 | 149,490 | 149,600 | 149,490 |

Per year, treating the move as add and remove: 2024 removed 149,600 and added 0. 2026 removed 149,490 and added 0. 2021 added 149,600 and removed 0. 2022 added 149,490 and removed 0. Value-changed counts are 0 in every year. No other module's output changed.

These ten metric names are emitted only by this module. `safe_schools_discipline` emits `out_of_school_suspension_count`, which is a different metric and is not in this file (0 rows before and after). A year move cannot collide with another module on this key. The base `kpi_master.csv` duplicate count cited for this branch is 1,813 (1,810 in 2019, 1 in 2022, 2 in 2023). This run did not rebuild that file. This module adds no new duplicate key.

Fayette County rows move with the same years: 7,480 from 2024 to 2021, and 7,590 from 2026 to 2022. Both destination years are 2021 or later.

## Spot checks

Source cell and output row. Totals are the sum of the published components. There is no state cell and no district-total cell to place beside an output row.

Bryan Station High School `165170`, Fayette County, All Students, KYRC26. `Collected School Year` `20212022`. `School Year` `20252026`. Source single 7 and 28, multiple 35 and 146. Output `out_of_school_suspension_total_count` 216, suppressed N, year 2022, district `Fayette County`, school id `165170`.

The same school, KYRC24. `Collected School Year` `20202021`. `School Year` `20232024`. Source in-school 11 and 36, single 5 and 2, multiple 3 and 20. Output total 30, suppressed N, year 2021. Single total 7. Multiple total 23. In-school total 47.

Adair County High School `001010`, All Students, KYRC26. Source single 8 and 61, multiple 1 and 17. Output total 87, suppressed N, year 2022, district `Adair County`. This is a school row. It is not an Adair County district total.

Camp Taylor Elementary `275004`, Jefferson County, All Students, KYRC24. All six count cells are `*`. Output is 10 rows, each suppressed Y with an empty value, year 2021.

## Audit of other CRDC readers

| Module | CRDC file | Year rule | Result |
| --- | --- | --- | --- |
| `out_of_school_suspension` | `KYRC24_OVW_Student_Suspensions.csv`, `KYRC26_OVW_Student_Suspensions.csv` | Collection year. `20202021` is 2021. `20212022` is 2022. | Pass. This change. |
| `advanced_coursework` | `KYRC23_OVW_Advanced_Coursework.csv`, `KYRC26_OVW_Advanced_Coursework.csv` | Overview rows use the collection year. The KYRC23 fixture is `School Year` `20222023` and `Collected School Year` `20202021`, stored as 2021. The KYRC26 fixture is `School Year` `20252026` and `Collected School Year` `20212022`, stored as 2022. Participation and courses offered are EDOP files, not CRDC, and keep the report-card year. KYRC24 and KYRC25 overview are the 2020-21 collection and are skipped. | Pass. Not changed here. |
| `chronic_absenteeism` | `KYRC26_OVW_Chronic_Absenteeism_CRDC.csv` is not registered | `accepts_data_file` skips a name that contains both `crdc` and `chronic_absenteeism`. The overview file this module reads is not that CRDC extract. | Pass. It does not load a CRDC file. Not changed here. |
| none | `OVW_Arrests_and_Referrals_to_Law_Enforcement`, `OVW_Harassment_and_Bullying`, `OVW_Preschool_Enrollment`, `OVW_Preschool_Suspensions_and_Expulsions`, `OVW_Student_Expulsions`, `OVW_Violent_Offenses` | Not registered in `config/kde_sources.yaml`. No ETL reads them. | Report only. No module is off the rule, because no module loads them. Not fixed here. |

`safe_schools_discipline` reads KDE Safe Schools files, not CRDC. It still owns `out_of_school_suspension_count`.

## Dashboard

`jscotthorn/fcps-equity-dashboard` `scripts/extract-fayette-kpi.js`, read from the default branch. The file was last changed in commit `8c929966` on 4 Aug 2026, which is the copy present at pipeline pin `477f763` (25 Oct 2026). `477f763` is a pipeline commit, not a dashboard commit.

`TARGET_INDICATORS` contains `out_of_school_suspension_count`. It does not contain `out_of_school_suspension_total_count`, the `_single_*` metrics, the `_multiple_*` metrics, or the `in_school_suspension_*` metrics. `COVARIATE_METRICS` does not contain them either. The dashboard suspension indicator is the Safe Schools count. The year filter keeps `year >= 2021`. The moved rows land in 2021 and 2022, so they would stay above that cutoff if they were extracted. They are not extracted. Fayette rows of `out_of_school_suspension_count` in this module's output: 0 before and 0 after.

The in-repo `html/generate_dashboard_data.py` selects metrics matching `out_of_school_suspension.*_count` and then keeps names containing `total_count`. That script would see the year shift on the three out-of-school total metrics. It is not the FCPS extract script, and it was not changed.

## Flags and mirror

No cells were flagged. No `notes/flags/` list. No KPI column. Mirror additions: none.
