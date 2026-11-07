# 119 - KYRC26 chronic absenteeism, district 602's published name

The base is `feature/2025-26-kyrc26`. `develop` and `master` are not touched. Teacher working conditions is not touched. This note records the accepted source choice. It does not download the refreshed overview, does not replace `data/raw/`, and does not regenerate `data/processed/`. That load and the regen are pending and separate.

## File comparison

The comparison is the 7 November 2026 check of the two overview files. It was not re-run in this workspace. `data/raw/` is gitignored.

| Copy | md5 |
| --- | --- |
| 15 October datasets copy, and the UAT copy | `07d218e589daaac167e0faacabecfe8e` |
| Refreshed datasets copy | `2c317303f3f136bc102deee832c3cc95` |

Both files have 19 columns and 430,848 data rows. There is no duplicate on School Code, Grade, and Demographic. Sorted on every column except District Name, the rows are identical. Row order changed, so a positional diff is not a data diff.

The only cell change is District Name on district 602. That is 576 rows:

| School code | School | 15 October District Name | Refreshed District Name |
| --- | --- | --- | --- |
| `602000` | All Schools | `Kentucky School for the Blind` | `Kentucky School for the Deaf District` |
| `602176` | Kentucky School for the Blind | `Kentucky School for the Blind` | `Kentucky School for the Deaf District` |

The district-school list, old and new the same for these two districts, names 602 `Kentucky School for the Blind District` and 603 `Kentucky School for the Deaf District`. Other refreshed files still label district 602 as the School for the Blind. This note does not inventory those files and does not state a count. Fayette (`165`) is 19,296 rows and matches, and the state rows match. No measure cell changes.

Scott approved the refreshed copy as the source to load as published, and approved keeping this warning. The published name is not replaced with the list's name. Loading the bytes and regenerating output are pending and separate. `docs/decisions/0056-kyrc26-chronic-absenteeism-district-602-name.md`.

## How the ETL keys the row

`ChronicAbsenteeismETL` does not override `extract_school_id` or `create_kpi_template`.

`BaseETL.extract_school_id` (`etl/base_etl.py`) reads `school_code` and nothing else. `602000` and `602176` stay those school ids. A missing school code raises. District Name is not consulted.

`BaseETL.create_kpi_template` copies `district_name` into `district` and copies `district_number` into its own column. There is no lookup against the district-school list in this module. `All Schools` is stored as `---District Total---`. That is the existing school-name rule. Fayette `165000` already takes the same rewrite. It does not change district 602's label.

`convert_to_kpi_format` writes the rate, count, and enrollment from the same source row. The district string is not a join key inside the conversion. `etl_runner.combine_kpi_files` appends processed files. It does not join on `district`.

A shared district string does not move 602's measures onto a 603 school code. The labeled example in `tests/test_chronic_absenteeism.py` checks that. Its measure cells are markers, not KDE's published 602 or 603 counts. The real overview is not in the fixture directory.

## Where a name-only join would matter

`analysis/scripts/chronic_absenteeism_analysis.py` returns merge strategy `school_id` and keeps years 2023-2025. The 2026 rows, including district 602, are excluded before the covariate merges. The grade-span merge is `school_id` plus `year`. District totals are dropped. A school stays only when `high_grade` is 5.

`BaseAnalysisDataset.merge_covariates`, on the school-id strategy:

- demographics join on `year` and `school_id`
- teacher metrics join on `year`, `school_id`, and `district`
- financial metrics join on `year` and `district`

If a later run included 2026 and kept `602176`, the teacher join would miss that school when the teacher file still says `Kentucky School for the Blind District`. The chronic numbers would stay on `602176`. They would not move to `603390`, because school id is still in that key. The financial join would attach spending stored under `Kentucky School for the Deaf District` and would miss spending stored under the Blind name. That is covariate attribution by the label. It does not change the chronic rate. The current chronic analysis does not take that path. This change does not rewrite the merge.

`html/generate_dashboard_data.py` keeps districts whose name contains `Fayette`. The Deaf label does not match, so district 602 stays out of the Fayette extract, as the Blind label would. The dashboard is not edited.

## What this change does not do

It does not load the refreshed bytes, does not correct the source name, does not add a district crosswalk, does not regenerate processed output, and does not change teacher working conditions.
