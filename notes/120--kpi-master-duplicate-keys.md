# 120 KPI master duplicate keys

Checked against `feature/2025-26-kyrc26` at `1aef27b4a43dffb9757b4651a4dc687945479b52`. The branch had not moved. The four modules were regenerated with `etl_runner.run_etl_module` into a scratch directory. `data/processed` is not committed. `config/kde_sources.yaml` was not edited. No mirror file was added.

The duplicate keys in these four outputs were 1,813 before this change and 0 after. Other modules were not regenerated. Issue #128 counted the same 1,813 keys in the combined file at an earlier commit, and these four modules are the ones that issue names.

## What the 1,813 keys were

Key: `district`, `school_id`, `school_name`, `year`, `student_group`, `metric`.

| Year | Duplicate keys |
| --- | --- |
| 2019 | 1,810 |
| 2022 | 1 |
| 2023 | 2 |

| Metric | Duplicate keys |
| --- | --- |
| `graduation_rate_4_year` | 54 |
| `graduation_rate_5_year` | 39 |
| `graduation_count_4_year` | 54 |
| `graduation_count_5_year` | 39 |
| `graduation_total_4_year` | 406 |
| `graduation_total_5_year` | 406 |
| `postsecondary_readiness_rate` | 406 |
| `postsecondary_readiness_rate_with_bonus` | 406 |
| `teacher_average_years_experience` | 1 |
| `student_teacher_ratio` | 2 |

Fayette County accounts for 56 of the 2019 keys, all of them year 2019. Seven school ids (`165`, `165039`, `165060`, `165105`, `165125`, `165170`, `165200`) and eight metrics. Graduation contributes 42 and postsecondary readiness contributes 14. No other Fayette year has a duplicate key. Issue #128 said none of the keys were Fayette. That was not true of the output at `1aef27b`.

## Cause, checked in the source files

2019 graduation and transition readiness are not identical source rows and not two overlapping files. `GRADUATION_RATE_18-19.xlsx` and `TRANSITION_READINESS_ACCOUNTABILITY_18-19.xlsx` use coversheet codes. Both coversheets say `LEP` is English Learners and `LEN` is Non-English Learners. The readiness coversheet says `ELN` is not English Learners including monitored students. The shared map sends `LEP` and `LEN` both to English Learner, and sends `ELN` to Non-English Learner. Record 0047 says to read the workbook label before the shared map and to leave `demographic_mappings.yaml` unchanged. The file order wrote `LEN` before `LEP`, so the first of the two English Learner values was the non-English-learner cell.

State graduation, 4-year rate, school `999`: `LEP` 74.3, `LEN` 91.0, All Students 90.6. After the change English Learner is 74.3 and Non-English Learner is 91.0. Bryan Station `165170`: English Learner 60.0 (was the `LEP` cell) and Non-English Learner 89.7 (was the `LEN` cell). Fayette district `165`: English Learner 72.3 and Non-English Learner 88.7.

State readiness, school `999`: `LEP` 9.2 (bonus 9.4), `LEN` 67.6 (bonus 69.7), `ELN` 67.5 (bonus 69.6). After the change English Learner is 9.2, Non-English Learner is 67.6, and Non-English Learner or monitored is 67.5. Bryan Station: English Learner 6.8, Non-English Learner 56.2, Non-English Learner or monitored 55.6.

`ACD` and `ACO` stay filtered. The graduation coversheet names `ACD` as disabilities and the shared map already sends `CSG` to Students with Disabilities (IEP). Mapping `ACD` there would add a new collision. That part of 0047 is still open.

2022 teacher experience is an identical source row. `average_years_school_experience_2022.csv` rows 1564 and 1565 are both Kentucky Tech System, school code `777`, district total, educator count 104, average 10.5. One copy is stored.

2023 student-teacher ratio is the adjacent-year rule in record 0023. `student_teacher_ratio_2023.csv` repeats Corbin `133` and Floyd `175`. The kept cells are Corbin 15 (2022 is 16, 2024 school `133000` is 15) and Floyd 14 (2022 is 14, 2024 school `175000` is 16). Fayette `165` in that file is three `0:01` copies and one `13:01`. Only 13 is stored, before and after. It is not a duplicate key. Zeros stay omitted by the existing `> 0` rule.

KYRC23's report-card file publishes one district total for each of those districts, and those cells are the copies this pipeline drops. `https://kdeschoolreportcard.blob.core.windows.net/datasets/KYRC23_OVW_Student_to_Teacher_Ratio.csv` school year `20222023`: Corbin Independent, All Schools, school code `133000`, `Student Teacher Ratio` `412:01:00`. Floyd County, All Schools, school code `175000`, `5249:01:00`. The #56 rule is unchanged: the stored 2023 rows are still Corbin 15 and Floyd 14. Those two kept rows are on `notes/flags/G-flagged-keys.csv` with label `kde-recheck`. The file is not registered. `accepts_data_file` skips that name, so loading it would be a second 2023 copy.

## Rows not stored

These are the rows this change stops storing. The `0:01` copies were already omitted by `ratio > 0`. They are listed because the 2023 filter sees them.

| File | Source row | Key | Cell | What happened |
| --- | --- | --- | --- | --- |
| `student_teacher_ratio_2023.csv` | 315 | Corbin Independent, `133`, `---District Total---`, 2023, All Students, `student_teacher_ratio` | `0:01` | Already omitted by `ratio > 0`. Not the adjacent-year row |
| `student_teacher_ratio_2023.csv` | 317 | same key | `3:01` | Was stored as 3.0. Dropped. 2022 is 16 and 2024 is 15 |
| `student_teacher_ratio_2023.csv` | 318 | same key | `412:01` | Was stored as 412.0. Dropped |
| `student_teacher_ratio_2023.csv` | 316 | same key | `15:01` | Kept. Stored as 15.0 |
| `student_teacher_ratio_2023.csv` | 514 | Floyd County, `175`, `---District Total---`, 2023, All Students, `student_teacher_ratio` | `5249:01` | Was stored as 5249.0. Dropped. 2022 is 14 and 2024 is 16 |
| `student_teacher_ratio_2023.csv` | 516 | same key | `0:01` | Already omitted by `ratio > 0` |
| `student_teacher_ratio_2023.csv` | 517 | same key | `3:01` | Was stored as 3.0. Dropped |
| `student_teacher_ratio_2023.csv` | 515 | same key | `14:01` | Kept. Stored as 14.0 |
| `average_years_school_experience_2022.csv` | 1565 | Kentucky Tech System, `777`, `---District Total---`, 2022, All Students, `teacher_average_years_experience` | 10.5, educator count 104 | Second identical copy. Row 1564 is the copy that is stored |

No graduation or readiness row was dropped. `LEN` cells moved from English Learner to Non-English Learner. Readiness `ELN` cells moved from Non-English Learner to Non-English Learner or monitored. Where `LEN` and `ELN` differ, the Non-English Learner value changes. Bourbon County High School `041110` is one of those: `ELN` is 66.7 and `LEN` is suppressed, so Non-English Learner changes from 66.7 unsuppressed to suppressed, and 66.7 is stored on Non-English Learner or monitored.

## Output diff against the base at 1aef27b

Compared excluding `last_updated`. Years with no row in this table are unchanged.

File totals, excluding `last_updated` from the comparison:

| Module | Rows before | Rows after |
| --- | --- | --- |
| graduation_rates | 167,216 | 167,216 |
| postsecondary_readiness | 102,730 | 102,730 |
| teacher_experience | 12,638 | 12,657 |
| student_teacher_ratio | 11,882 | 11,878 |

Keys added, removed, or whose stored value list changed. Years absent from this table are unchanged.

| Module | Year | Keys added | Keys removed | Keys whose value list changed |
| --- | --- | --- | --- | --- |
| graduation_rates | 2019 | 2,368 | 1,370 | 998 |
| postsecondary_readiness | 2019 | 812 | 0 | 1,008 |
| teacher_experience | 2019 | 5 | 0 | 0 |
| teacher_experience | 2020 | 1 | 0 | 0 |
| teacher_experience | 2021 | 3 | 0 | 0 |
| teacher_experience | 2022 | 4 | 0 | 1 |
| teacher_experience | 2023 | 3 | 0 | 0 |
| teacher_experience | 2024 | 2 | 0 | 0 |
| teacher_experience | 2025 | 2 | 0 | 0 |
| student_teacher_ratio | 2023 | 0 | 0 | 2 |

Graduation row count is unchanged because the 1,370 removed English Learner keys are `LEN` rate or count cells that had no `LEP` partner, and those cells are the new Non-English Learner rows. The 998 value changes are English Learner keys that had both cells; the remaining value is the `LEP` cell. The 2,368 added keys are all Non-English Learner.

Readiness adds 812 Non-English Learner or monitored keys (the `ELN` cells). The 1,008 value-list changes are 812 English Learner keys collapsing to the `LEP` cell and 196 Non-English Learner keys whose number changes from the `ELN` cell to the `LEN` cell. 88 of those also change `suppressed`, for the same reason as Bourbon. Keys that are one row on both sides and are not part of that relabel have no column difference.

Teacher experience adds the 20 zeros, by year 5, 1, 3, 4, 3, 2, 2. The 2022 value-list change is Kentucky Tech `777` going from two 10.5 rows to one 10.5 row. Every other single key is unchanged, including state `999000` in 2026 (12.2) and Fayette `165000` in 2026 (13.38).

Student-teacher ratio drops four stored cells: Corbin 3 and 412, Floyd 5249 and 3. Corbin stays 15 and Floyd stays 14. Fayette `165` in 2023 stays one row at 13. State `999000` in 2025 stays 15. The three outliers stay: Corbin `133000` 2025 is 483, Cloverport `132000` 2024 is 86, Cloverport school `132030` 2026 is 115.

On keys with one row on both sides, graduation, teacher experience, and student-teacher ratio have no column differences. Readiness differs only on the Non-English Learner relabel above.

## Fayette, year 2021 and later

`district == Fayette County` and `year >= 2021`, for the metrics these four modules emit:

| Module | Rows before | Rows after | Added | Removed | Value changed |
| --- | --- | --- | --- | --- | --- |
| graduation_rates | 2,284 | 2,284 | 0 | 0 | 0 |
| postsecondary_readiness | 1,646 | 1,646 | 0 | 0 | 0 |
| teacher_experience | 425 | 426 | 1 | 0 | 0 |
| student_teacher_ratio | 403 | 403 | 0 | 0 | 0 |

The added row is Fayette County, school `165009`, UK Early Childhood Lab, 2021, All Students, `teacher_average_years_experience`, value 0.0, suppressed N, source `average_years_school_experience_2021.csv`. Educator count in that file is 1 and total years is 0.

The dashboard extract in `fcps-equity-dashboard` `scripts/extract-fayette-kpi.js` keeps Fayette rows with year at least 2021. The 56 duplicate keys are 2019, so they are below that cutoff. The new 2021 lab row is a Fayette covariate and would enter that extract. This repository does not change the dashboard.

## Flags

`notes/flags/G-flagged-keys.csv` has 25 rows, label `kde-recheck`. Twenty are the zero averages. Three are the ratio outliers. Two are the kept 2023 district totals, Corbin 15 and Floyd 14. Values are unchanged from the published cells that are stored. Record DNNNN.

## Combine guard

`etl_runner.combine_kpi_files` raises `DuplicateKpiKeyError` when the written file repeats a key that is not in `DUPLICATE_KEY_ALLOWLIST`. The allowlist is empty. Every group in the 1,813 was covered by the identical-row rule, the adjacent-year rule, or record 0047. Nothing was left allowlisted. The check counts an 8-byte hash of each key while streaming the file, then reads the real key text only for hashes that occur more than once. A missing master file is an error. The combined file is written to a temporary file in the same directory and renamed after the check passes. Source rows are rewritten into `KPI_COLUMNS` order.

A full combine of the 31 processed files, 48,822,403 rows, was timed with `/usr/bin/time -v python3 -u etl_runner.py --skip-etl`. Wall clock was 5:00.08. Maximum resident set size was 920,740 kbytes (899 MB). Exit status 0. The earlier checker, which kept every key in a Python dict, was OOM-killed on this file.
