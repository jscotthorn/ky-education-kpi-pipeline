# Remove calculated climate and safety indexes

Scott Horn decided on 2026-10-20 to remove `climate_index_score_calculated` and `safety_index_score_calculated`.

The pipeline averaged survey question indexes and then dropped duplicate keys on district, school, school name, year, student group, and metric. School level was not part of that key. The group-by sorted level, and the file list sorted the elementary survey file ahead of high and middle, so a key that had an elementary row kept the elementary value. District rows and schools that publish more than one level were elementary-only. That is the same flaw PR #103 fixed for the published index by putting the level on the metric name.

KDE's published indexes replace them: `climate_index_score_{elementary|middle|high}` and `safety_index_score_{elementary|middle|high}`, loaded for 2022-23 onward. There is no unsuffixed alias. Scott accepts losing 2021-22 for this series. The 2022 accountability number stays `climate_safety_indicator_rate_{level}`. The 2023 combined rate stays `climate_safety_combined_indicator_rate_{level}`. Those are not this series.

`notes/104--kyrc26-file-to-etl-mapping.md` and `notes/105--kyrc26-completeness-audit.md` already use 104 and 105, so this decision is 106. The climate section of notes/104 points here.

## What was removed

`etl/safe_schools_climate.py` no longer calls `calculate_aggregate_index_scores` or `format_calculated_scores_as_kpi`. The survey-file patterns, the question-metadata join, and the chunked survey read are gone. Survey item files are not selected. Published index rows, the two accountability-rate series, the compliance rate, and outside doors are unchanged.

The covariate catalog already used the published per-level names (high school for graduation and postsecondary readiness, elementary for chronic absenteeism, all three for discipline). The note that the calculated average remained is gone. No alias was added. `scripts/generate_report_drafts.py` had used `climate_index_score_calculated` as one indicator. It now reads the three published climate indexes, one indicator per level.

## Rows the old code emitted

Counted on 20 October 2026 from the survey files `get_files_to_process` selected when they were on disk. The registered download does not include those files. On the registered files the old code wrote 400,581 rows and 0 calculated rows.

The 2021 elementary survey file is 0 bytes. The 2021 middle and high files have a question type and no question index, so the old code did not average them. The 2025 response names in the old selector are 404. `KYRC25_ACCT_Survey_Results.csv` and `KYRC26_ACCT_Survey_Results.csv` were not in the selector.

| Year | `climate_index_score_calculated` | `safety_index_score_calculated` |
| --- | ---: | ---: |
| 2021 | 0 | 0 |
| 2022 | 27,451 | 27,455 |
| 2023 | 28,404 | 28,456 |
| 2024 | 28,452 | 28,554 |
| 2025 | 0 | 0 |
| 2026 | 0 | 0 |
| Total | 84,307 | 84,465 |

168,772 rows after the level-blind dedupe. Before that dedupe the same files produced 218,264 level rows. 31,471 keys were present at more than one level. Every one of those keys that had an elementary row kept the elementary value (27,959). The other 3,512 had no elementary row. District and state `All Schools` rows are in that elementary-only set: 20,676 multi-level district or state keys, 19,533 equal to the elementary value.

Duplicate keys after the old dedupe: 0. The registered run, before and after this removal, has 0 duplicate keys on year, school, student group, and metric. No other metric's row count changes on the registered files.
