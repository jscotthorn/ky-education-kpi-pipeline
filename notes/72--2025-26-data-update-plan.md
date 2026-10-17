# 72 - Plan to incorporate KDE 2025-26 data

Planning only. No pipeline code is changed in this document.

Checked on 17 October 2026 against the live files, not the catalog text. Statewide figures in the temporary files match the 15 October 2026 Kentucky Teacher release (4-year graduation 94.1, 5-year 94.5, SAT 470.9 / 437.0 / 59.3 / 908.0, 1,261 schools, CSI 53, TSI 77, ATSI 81).

## Recommendation

Ingest the 15 October temporary assessment files for the indicators they actually contain, and backfill the missing 2024-25 `KYRC25` year at the same time. Do not wait for `KYRC26` to start, and do not load the temporary files through the current parsers unchanged.

`KYRC26` historical extracts for 2025-26 are not published. The files that are published use a thinner layout than `KYRC24` / `KYRC25`: fewer identifier columns, different headers, district and state rows stored as 3-digit codes, and two assessment files with no school-year column. Dropping them in `data/raw/` today would either skip the rows or label them as 2024.

## What is published for 2025-26

Catalog page, published 14 October 2026 at 3:55 PM:

https://www.education.ky.gov/Open-House/data/Pages/Assessment_Accountability_Datasets_2025-2026.aspx

Files live under `OAA Temporary Datasets`, not `HistoricalDatasets`. HTTP `Last-Modified` on the data files is 15 October 2026, 04:36–04:51 UTC. The cooperative zip was modified later the same day at 13:59 UTC. News release: [KDE releases 2025-2026 assessment and accountability data](https://www.kentuckyteacher.org/news/2026/10/kde-releases-2025-2026-assessment-and-accountability-data/) (15 October 2026).

`All_Datasets_Archive.Zip` contains the 16 data files below and nothing else. Use the individual files or that zip. Do not use the cooperative zip as the statewide source: the same names are in it, but the files are coop-filtered and much smaller, and the CSI workbook is absent.

| File | Bytes | Pipeline | 2025-26 status |
| --- | ---: | --- | --- |
| [Graduation_Rate_2026.CSV](https://www.education.ky.gov/Open-House/data/OAA%20Temporary%20Datasets/Graduation_Rate_2026.CSV) | 1,073,777 | `graduation_rates` | Usable for rates only, after column and ID mapping |
| [Postsecondary_Readiness_2026.CSV](https://www.education.ky.gov/Open-House/data/OAA%20Temporary%20Datasets/Postsecondary_Readiness_2026.CSV) | 842,564 | `postsecondary_readiness` | Usable after ID mapping. Rate headers already match |
| [English_Language_Proficiency_2026.CSV](https://www.education.ky.gov/Open-House/data/OAA%20Temporary%20Datasets/English_Language_Proficiency_2026.CSV) | 1,665,216 | `english_learner_progress` | Usable after ID mapping. Score headers already match |
| [English_Learners_Attainment_2026.CSV](https://www.education.ky.gov/Open-House/data/OAA%20Temporary%20Datasets/English_Learners_Attainment_2026.CSV) | 462,223 | none | New file. Not a substitute for the proficiency file |
| [Kindergarten_Screen_2026.CSV](https://www.education.ky.gov/Open-House/data/OAA%20Temporary%20Datasets/Kindergarten_Screen_2026.CSV) | 10,251,508 | `kindergarten_readiness` | Usable for the overall percent. Prior-setting column name will not map |
| [Accountable_Assessment_Performance_2026.CSV](https://www.education.ky.gov/Open-House/data/OAA%20Temporary%20Datasets/Accountable_Assessment_Performance_2026.CSV) | 27,448,141 | `kentucky_summative_assessment` | Usable after a year fix and one header alias. No school-year column |
| [Assessment_Performance_by_Grade_2026.CSV](https://www.education.ky.gov/Open-House/data/OAA%20Temporary%20Datasets/Assessment_Performance_by_Grade_2026.CSV) | 55,715,316 | `kentucky_summative_assessment` | Same year gap, plus a truncated subject label and numeric grades |
| [Quality_of_School_Climate_and_Safety_Survey_Index_Scores_2026.CSV](https://www.education.ky.gov/Open-House/data/OAA%20Temporary%20Datasets/Quality_of_School_Climate_and_Safety_Survey_Index_Scores_2026.CSV) | 3,985,101 | `safe_schools_climate` | Index columns match. Current filename rules will not pick the file up |
| [Quality_of_School_Climate_and_Safety_Survey_Questions_2026.CSV](https://www.education.ky.gov/Open-House/data/OAA%20Temporary%20Datasets/Quality_of_School_Climate_and_Safety_Survey_Questions_2026.CSV) | 6,730 | `safe_schools_climate` | 81 question rows (27 questions × elementary, middle, high). Question type is only in this file |
| [Quality_of_School_Climate_and_Safety_Survey_Elementary_School_2026.CSV](https://www.education.ky.gov/Open-House/data/OAA%20Temporary%20Datasets/Quality_of_School_Climate_and_Safety_Survey_Elementary_School_2026.CSV) | 60,240,437 | `safe_schools_climate` | Responses only. No question text or question type |
| [Quality_of_School_Climate_and_Safety_Survey_Middle_School_2026.CSV](https://www.education.ky.gov/Open-House/data/OAA%20Temporary%20Datasets/Quality_of_School_Climate_and_Safety_Survey_Middle_School_2026.CSV) | 33,013,254 | `safe_schools_climate` | Same split as the elementary file |
| [Quality_of_School_Climate_and_Safety_Survey_High_School_2026.CSV](https://www.education.ky.gov/Open-House/data/OAA%20Temporary%20Datasets/Quality_of_School_Climate_and_Safety_Survey_High_School_2026.CSV) | 26,078,263 | `safe_schools_climate` | Same split as the elementary file |
| [Accountable_Profile_2026.CSV](https://www.education.ky.gov/Open-House/data/OAA%20Temporary%20Datasets/Accountable_Profile_2026.CSV) | 331,210 | climate reads older profile files by name | Same layout as the Feb 2026 supplemental `Accountable_Profile_2025.CSV`, not the `KYRC` profile |
| [Accountable_Summary_2026.CSV](https://www.education.ky.gov/Open-House/data/OAA%20Temporary%20Datasets/Accountable_Summary_2026.CSV) | 2,662,288 | not a current source | Same layout as supplemental `Accountable_Summary_2025.CSV`. Subgroup indicator scores |
| [ACCT_CSI_TSI_ATSI_2026.xlsx](https://www.education.ky.gov/Open-House/data/OAA%20Temporary%20Datasets/ACCT_CSI_TSI_ATSI_2026.xlsx) | 35,000 | none (`KYRC*_ACCT_School_Improvement.csv` is the older shape and is not wired) | Sheets CSI (53), TSI (77), ATSI (81) |
| [College Admissions Exam_2026.CSV](https://www.education.ky.gov/Open-House/data/OAA%20Temporary%20Datasets/College%20Admissions%20Exam_2026.CSV) | 1,243,383 | none (`act_scores` is listed in `kde_sources.yaml` only) | SAT results with stale ACT column titles. Do not append to ACT history |
| [All_Datasets_Archive.Zip](https://www.education.ky.gov/Open-House/data/OAA%20Temporary%20Datasets/All_Datasets_Archive.Zip) | 17,145,212 | downloader | Convenience copy of the 16 files above |

Two other 2025-26 files exist and are outside the current ETL:

- CTE grant allocations: [2025-2026 District State Grant Allocation.xls](https://www.education.ky.gov/Open-House/data/OCTE%20Data%20Files/2025-2026%20District%20State%20Grant%20Allocation.xls)
- Economically disadvantaged qualifying data: [2025-2026FinalQualifyingData.xlsx](https://www.education.ky.gov/Open-House/data/OFO/2025-2026FinalQualifyingData.xlsx)

Supplemental enrollment, attendance, finance, educator, school-profile, and IDEA pages do not list 2025-26 files. Finance on the supplemental page stops at 2024-25.

## What is not published yet

The historical School Report Card list has **zero** items with school year `2025-2026` (SharePoint list `Historical SRC Datasets`, field `Year`, queried 17 October 2026). These URLs return 404:

- `KYRC26_ACCT_4_Year_High_School_Graduation.csv`
- `KYRC26_OVW_Chronic_Absenteeism.csv`
- `KYRC26_OVW_Student_Enrollment.csv`
- `KYRC26_OVW_Student_Suspensions.csv`
- `KYRC26_CTE_Participation.csv`
- `KYRC26_SAFE_Behavior_Events_by_Type.csv`
- `KYRC26_ADLF_Transition_to_In_State_Postsecondary_Education.csv`

`KYRC25` (school year 2024-25) is the newest historical bundle: 99 files, created 9–19 February 2026. The pipeline config still stops at `KYRC24` (school year 2023-24). A 2025-26-only load would skip 2024-25.

Blocked current pipelines until a `KYRC26` (or equivalent) extract exists:

| Pipeline | Last file the config knows | Why 2025-26 is blocked |
| --- | --- | --- |
| `chronic_absenteeism` | `KYRC24_OVW_Chronic_Absenteeism.csv` | No 2025-26 attendance extract |
| `student_enrollment` | `KYRC24_OVW_Student_Enrollment.csv` | No 2025-26 membership extract |
| `out_of_school_suspension` | `KYRC24_OVW_Student_Suspensions.csv` | No 2025-26 suspension extract |
| `safe_schools_events` | `KYRC24_SAFE_Behavior_Events_by_*.csv` | No 2025-26 behavior files |
| `safe_schools_discipline` | `KYRC24_SAFE_Discipline_Resolutions.csv`, `KYRC24_SAFE_Legal_Sanctions.csv` | No 2025-26 discipline files |
| `cte_participation` | `KYRC24_CTE_Participation.csv` | Grant spreadsheet only. No participation extract |
| `postsecondary_enrollment` | `KYRC24_ADLF_Transition_to_In_State_Postsecondary_Education.csv` | No 2025-26 transition file. Readiness is a different indicator and is available |
| Precautionary-measure half of `safe_schools_climate` | `KYRC24_SAFE_Precautionary_Measures.csv` | Survey and index scores are available. The yes/no safety-practice file is not |

The October release is the accountability bundle (assessment, graduation, readiness, English learners, kindergarten screen, climate survey, classifications). Attendance, enrollment, behavior, and CTE participation have historically followed in the `KYRC` dump. `KYRC25` landed in February 2026, about four months after the prior fall release. Expect the same lag for `KYRC26`.

House Bill 257 changes the accountability system in 2026-27, not in this file set. This release is the last year for editing and mechanics, on-demand writing, and the climate-and-safety accountability indicator. Parsers for this year should not assume those columns continue next year.

## How the pipeline gets a new year today

`data/prepare_kde_data.py` reads `config/kde_sources.yaml`. Every filename is fetched from one prefix:

`https://www.education.ky.gov/Open-House/data/HistoricalDatasets/`

There is no per-file URL, no second folder, and no refresh: if the destination file exists, the download is skipped. `etl_runner.py` then runs whatever `*.csv` files are in each `data/raw/<source>/` directory. `safe_schools_climate` is the exception. It ignores unknown CSVs and looks for hardcoded `KYRC24_...` names plus lowercase globs such as `quality_of_school_climate_and_safety_survey_*.csv`. The published 2026 names are `Quality_...CSV`, so a Linux glob will not see them.

`BaseETL.extract_year` keeps the last four digits of an 8-character year (`20252026` → `2026`) and a 4-character year. Any other shape, including a missing year, becomes **`2024`**. That default is the main silent-corruption risk in this release.

School identity today is the 6-digit `School Code` (`999000` state, `001000` district total, `001010` school). `standardize_school_name` turns a blank name into `Unknown School`.

## Schema comparison

Headers were read from the 2026 files (full-file checks for graduation, readiness, kindergarten, both assessment files, college exam, and profile) and from the first rows of the matching `KYRC24` and `KYRC25` files.

### Shared 2026 layout changes

Almost every temporary file has only:

`School Year`, `School Code`, `District Name`, `School Name`, plus the measure columns.

Missing relative to `KYRC24` / `KYRC25`: county number and name, district number, school number, state school id, NCES id, co-op, co-op code, and usually school type. Those KPI columns will be blank unless they are joined back from `KYRC25_OVW_District_School_List.csv` (or the later `KYRC26` list). New schools for 2025-26 will not be in the 2024-25 list.

Organization rows are encoded differently:

| Grain | `KYRC25` | 2026 temporary files |
| --- | --- | --- |
| State | code `999000`, district `All Districts`, school `All Schools` | code `999`, district `State`, school blank |
| District total | code `001000`, school `All Schools` | code `001` (3 digits), school blank |
| School | 6-digit code | 6-digit code, unchanged |
| Education cooperative | co-op columns on each row | extra rows such as `909` / `ED COOP - JEFF CO`, classification `SP` |

Graduation has 13,248 rows and 552 codes: 8,904 six-digit and 4,344 three-digit. Profile has 1,261 six-digit codes, matching the published school count, plus 542 three-digit aggregate rows. Left-padding `001` to six digits produces `000001`, which is the wrong code. The safe rule is: 6-digit codes stay; a 3-digit district or state code gains `000` on the right (`001` → `001000`, `999` → `999000`); a blank school name on those rows becomes `All Schools`; district `State` becomes `All Districts`.

Cooperative rows (`ED COOP - ...`) are a new grain. Keep them out of the school and district KPI grain unless the owner wants cooperative rollups. Their 3-digit codes must not be rewritten into the district-code series without a check.

Graduation district names are cut at 45 characters (`Kentucky National Guard Youth Challenge Acade`). Join on school code, not name.

Suppression in the 2026 files is `Y`, `N`, or blank in a flag column. Suppressed rates are blank, not `*`. `KYRC` files use `Y` plus `*` in the value. Blank flag plus blank value means the cohort or level does not apply; do not store those as suppressed. `Y` must still be kept with a null value, which the current suppressed-row path already does once the column is mapped.

`School Year` is `20252026` wherever the column exists. `extract_year` already turns that into `2026`.

### File-by-file: 2026 versus what the parsers expect

**Graduation.** One file holds both rates. Headers are `4-Year Graduation Rate` and `5-Year Graduation Rate`, with `Suppressed 4-Year` and `Suppressed 5-Year`. The parser looks for `4 Year Cohort Graduation Rate`, `4-YEAR GRADUATION RATE`, and the older count columns. None of those headers are in this file, so a raw drop-in produces no graduation KPIs. `KYRC24` and `KYRC25` are also rates-only (the cohort counts live in `graduation_rate_2023.csv` and earlier). 2026 does not remove counts that `KYRC24` still had. State row checks out: 94.1 and 94.5.

**Postsecondary readiness.** `Postsecondary Rate` and `Postsecondary Rate With Bonus` already match the parser. School classification is in a column named `type`, not `School Type`. Full file: 10,962 rows, base rate max 100, bonus max 125 (2,189 rows over 100). The existing 0–150 bonus check holds. No row exceeds 150.

**English learner progress.** The four `PERCENTAGE OF VALUE TABLE SCORE OF ...` headers and `Level` (`ES` / `MS` / `HS`) already match. `KYRC25` spells level as `Elementary School` / `Middle School` / `High School`; both forms are already mapped.

**English learner attainment.** New columns: `Grade`, `Number Tested`, `Number of Students Reaching Attainment`, `Attainment Rate`. Grades include `00`, `01`–`12`, and `TL`. This is not the accountability value-table file. Leave it out of `english_learner_progress` unless a new metric is requested.

**Kindergarten.** 2026 is a percent file (`Total Percent Ready`, domain percents), closer to `kindergarten_screen_2023.csv` than to the `KYRC24` / `KYRC25` count file (`Ready With Interventions`, `Ready`, `Ready With Enrichments`, `Total Ready`). There is no `Number Tested`, so `kindergarten_readiness_total` cannot be built. The prior-setting column is `PRIOR_SETTING`. The parser maps `Prior Setting` and `PRIOR SETTING` only. If that column is left unmapped, every prior-setting row is treated as the all-students row and the readiness rate is written once per setting. Values, which match `KYRC25`, are `All Students`, `Home`, `State Funded`, `Child Care`, `Other`, `Head Start`, `Kindergarten`, `Unknown`. There is no `Preschool` category in either year. `State Funded` is the category that replaced it.

**Kentucky Summative Assessment, by level.** `Accountable_Assessment_Performance_2026.CSV` has no school-year column. Unchanged, `extract_year` stores the year as 2024. Subjects are `Reading`, `Mathematics`, `Science`, `Social Studies`, `Combined Writing` (same subjects as `KYRC25_ACCT_Kentucky_Summative_Assessment.csv`). The combined column is `Proficient/Distinguished`. The parser maps `Proficient / Distinguished` and `PROFICIENT/DISTINGUISHED`, not the no-space form, so that rate would be dropped. `Novice`, `Apprentice`, `Proficient`, `Distinguished`, `Content Index`, `Level`, and `Subject` already match. Level is `ES` / `MS` / `HS`. School classification is `TYPE`. One demographic is cut at 50 characters: `Students with Disabilities/IEP without Accomodatio`. The mapper already knows the longer misspelling `...without Accomodations`, not this shorter value. `Combined Writing` is not in the subject map, so the fallback slug is `combined_writing` rather than `writing`. That is already true of `KYRC25`, not a new 2026 split.

**Kentucky Summative Assessment, by grade.** Also no school-year column. Grades are `03`, `04`, `05`, `06`, `07`, `08`, `10`, `11`. The parser turns a numeric grade into `grade_3`. `KYRC25_ASMT_Kentucky_Summative_Assessment.csv` uses `8th`, `10th`, `11th`, which the same function leaves as `8th`. Loading both without a shared normalizer creates two metric families. Subjects match `KYRC25` except the label is `Editing and Mechanic` (66,371 rows) instead of `Editing and Mechanics`. No content index, same as the older by-grade file.

**Climate index.** `Climate Index` and `Safety Index` already match. The file has one row per level. District aggregates share a blank school name, and climate dedupes on district, school id, school name, year, student group, and metric, without level. District ES, MS, and HS indexes would collapse to one row. Keep level on the metric or on the school name before dedupe.

**Climate survey responses.** Response files have `Question Number`, agreement counts, `Agree and Strongly Agree`, and `Question Index`. They do not have `Question` or `Question Type`. Those are in the 81-row questions file and must be joined on level plus question number. `KYRC25_ACCT_Survey_Results.csv` still carries question text and type on every row. The questions file uses type `C` and `S`, which the parser already accepts.

**Accountability profile and summary.** 2026 profile headers are identical to supplemental `Accountable_Profile_2025.CSV` and different from `KYRC25_ACCT_Profile.csv` (title case, `Reading_Mathematics` instead of `READING_MATH`, typos `Statue Rating` and `Reason for Federal Classifcation` in both the 2025 and 2026 supplemental files). New columns versus `KYRC`: `Accountable School or Program` (constant `A` in this file) and `Overall Combined Federal Rate`. Summary uses `Overall Combined Federal Indicator Rate` for the federal column. The profile’s federal classification counts match the workbook: CSI 53, TSI 77, ATSI 81, and the rest blank. The climate parser looks for the old all-caps profile headers and for filenames `accountability_profile_2022.csv` and `accountability_profile_2023.csv`, so this file will not be read.

**CSI workbook.** Columns are `Code`, `District`, `School`, `Type`, `School Level`, `Title 1`, `Federal Classification`, `Number of years identified` (not on the TSI sheet), `Reason for Federal Classification`. `Code` is not zero-padded (`41090` and `111026` both occur). Do not treat it as `School Code` until it is checked against the school list.

**College admissions exam.** The page description still says ACT. The state row is the SAT release: reading and writing 470.9, mathematics 437.0, analysis in science 59.3, total 908.0, with benchmark percents 43 and 23. Column titles still say `Average ACT Score` for mathematics, science, and the composite. Observed ranges are SAT scales (math about 271–694, science about 52–66, composite about 591–1368), not ACT scales. There is no current ETL. `KYRC25_ASMT_The_ACT.csv` is still ACT (`Mathematics`, `Reading`, `Science`, `English`, `Composite Score`) and must stay a separate series.

### `KYRC25` versus `KYRC24` (the missing 2024-25 year)

These active-pipeline headers are identical, including school-year `20242025` and 6-digit school codes. Adding the `KYRC25_` filename to `kde_sources.yaml` is enough:

- Graduation, 4-year and 5-year
- Postsecondary readiness
- English learner proficiency rate
- Kindergarten composite (count layout)
- Accountability profile, summary, and school improvement
- Summative assessment, both the accountability file and the by-grade file
- Climate index and survey results
- The ACT file (download list only)
- Chronic absenteeism
- Student enrollment
- Student suspensions
- Behavior events by type, location, and context
- Legal sanctions
- Transition to in-state postsecondary education
- District and school list

These `KYRC25` files will load and then drop or misread measures if only the filename changes:

| File | What changed | What the parser does if the new headers are ignored |
| --- | --- | --- |
| `KYRC25_CTE_Participation.csv` | `CTE Participants in All Grades`, `Total Number of Student`, `Grade 12 CTE Eligible Completer`, `Grade 12 CTE Completers` replaced by `All High School CTE Participants`, `All High School Enrollments`, `12th Grade Participants`, `12th Grade Completers` | No CTE metrics. The old participation and completer columns were rates (`50.0`, `75.0`). The new columns are counts (`3`, `4`, and `*` for suppression). A rename that keeps the old “is it a rate?” check will treat school-level counts under 100 as percents |
| `KYRC25_SAFE_Discipline_Resolutions.csv` | Same measures, different casing: `Out-of-School Suspensions (SSP3)` vs `Out-Of-School Suspensions (SSP3)`, `Expelled, Not Receiving Services (Ssp2)`, `Removal by Hearing Officer (IAES2)`, and `In-School Removal (INSR) or In-District Removal (INDR) >=.5` | Those counts are dropped. Matching is case-sensitive |
| `KYRC25_SAFE_Behavior_Events_by_Grade_Level.csv` | `Preschool`, `Grade 1` … `Grade 14` replaced by `PS`, `1st` … `14th`. `All Grades` and `K` are unchanged | Grade-level event counts disappear. The all-grades total remains |
| `KYRC25_SAFE_Precautionary_Measures.csv` | Questions reworded from `Are visitors...` to statements such as `Visitors are required to sign-in to the building`. New column: `All outside doors are locked during school hours` | Policy-compliance rate is not computed. The new door question has no mapping |

`KYRC25` also adds files the pipeline does not read: `KYRC25_SAFE_Approaches_that_are_actively_implemented_school_wide.csv` (404 for the `KYRC24` name) and `KYRC25_EDOP_School_Courses_Summary.csv`.

Demographic labels in `KYRC25` still match the 2024 mapper inputs (`White (non-Hispanic)`, `Non Economically Disadvantaged`, `Non-Foster`, `Student without Disabilities (IEP)`, `Native Hawaiian or Pacific Islander`). The 2026 temporary files mostly use the already-standard hyphenated labels. Two 2026 labels do not resolve:

- `Native Hawaiian or Other Pacific Islander` (the word `Other` is new)
- `Students with Disabilities/IEP without Accomodatio` (50-character cutoff)

`White (Non-Hispanic)` and `Two or more races` match through the case-insensitive map. `config/demographic_mappings.yaml` `year_specific` stops at `2024`. Years `2025` and `2026` need entries or the demographic audit marks every group unexpected.

## Implementation plan

Sizes are the amount of code and test work, not a schedule. Do them in this order. Each parser change needs a fixture-sized unit test before the full file is downloaded.

### 1. Stop the silent year and ID failures

**Size: medium.** `etl/base_etl.py`, demographic config, tests.

- Remove the `extract_year` fallback to `2024`. Fail the row, or take the year from the filename, when `school_year` is missing or not 4 or 8 characters.
- For the two 2026 assessment files, set year `2026` from the filename before that helper runs.
- Normalize temporary-file organization codes as described above, and set district aggregate school names to `All Schools`.
- Add demographic maps for `Native Hawaiian or Other Pacific Islander` and the 50-character IEP label.
- Add `year_specific` blocks for `2025` and `2026`.
- Tests: a 2026 row must not come out as year 2024; `001` must become `001000` and must not become `000001`; `999` / `State` must land on the existing state key; the two new demographic strings must map.

### 2. Let the downloader fetch more than `HistoricalDatasets`

**Size: small.** `config/kde_sources.yaml`, `data/prepare_kde_data.py`, tests.

- Allow a full URL, or a folder prefix, per file. The 2026 folder name contains spaces.
- Keep original filenames.
- Add the 2026 files only under the sources that will actually parse them after step 3. Do not drop the CSI workbook, college file, or attainment file into an existing raw directory until a parser owns them.
- Add every identical-header `KYRC25_` file from the table above.
- Decide whether skip-if-exists stays. It will hide a later `KYRC26` replacement of a temporary file with the same name. Prefer a checksum or `Last-Modified` check for this year’s files.
- Do not commit the raw downloads. `*.csv` is gitignored; the xlsx is not, so keep it out of the repo on purpose.

### 3. Map the 2026 files that can produce current KPIs

**Size: medium**, spread across the modules below. One fixture per file, built from a handful of real rows (state, one district, one school, one suppressed group).

| Module | Change |
| --- | --- |
| `graduation_rates` | Map `4-Year Graduation Rate`, `5-Year Graduation Rate`, `Suppressed 4-Year`, `Suppressed 5-Year`. Emit rate KPIs only. Do not synthesize counts from rates |
| `postsecondary_readiness` | Map `type` to school classification. Existing rate columns can stay |
| `english_learner_progress` | ID normalization only, unless attainment is promoted to its own metrics |
| `kindergarten_readiness` | Map `PRIOR_SETTING`. Keep prior-setting rows from taking the all-students path. Accept percent inputs. Leave `kindergarten_readiness_total` null for this year. Slug `State Funded` and the other non-`All Students` settings only for `All Students` demographic rows, which is what the module already does when the column is mapped |
| `kentucky_summative_assessment` | Map `Proficient/Distinguished`. Alias `Editing and Mechanic` to the same slug as `Editing and Mechanics`. Normalize `03` and `8th` to one grade token. Alias `Combined Writing` only if the owner wants it folded into `writing`; otherwise document `combined_writing` as the continuing `KYRC25` name |
| `safe_schools_climate` | Discover `KYRC25_` and `2026` index files, case-insensitively. Read index scores first. Join survey responses to the questions file on level and question number before computing question metrics. Preserve level when deduping. Point profile reading at the new profile only after its title-case headers are mapped; until then, do not let it overwrite index scores |

After this step, run each module on the real file and check the state row (`School Code` `999`, demographic `All Students`):

- Graduation file: 4-year 94.1 and 5-year 94.5. These match the news release.
- Assessment file, proficient/distinguished: reading 50 / 48 / 49 and combined writing 41 / 47 / 45 for elementary, middle, and high. These match the news release.
- Profile, state high school: postsecondary combined indicator rate 98.7 and graduation combined indicator rate 94.7. The news story said the readiness indicator moved to 98.5. Use the file values above as the regression checks, and do not expect the profile graduation column to equal the 4-year rate.

### 4. Backfill 2024-25 where the header changed

**Size: medium.** Required before `KYRC25` filenames from step 2 are processed for these four files.

- CTE: map the four new columns to counts. Participation rate = participants / enrollments. Completion rate = 12th-grade completers / 12th-grade participants. Keep `*` as suppressed. Do not reuse the “value ≤ 100 means percent” heuristic on these columns.
- Discipline resolutions: accept the `KYRC25` casing next to the `KYRC24` strings.
- Behavior events by grade: map `PS`, `1st` … `14th` onto the existing grade fields.
- Precautionary measures: map the reworded statements onto the current policy fields, and decide whether the new locked-outside-doors question enters the compliance numerator. Default is to store it and leave the existing eight-item rate unchanged until the owner says otherwise.

The identical-header `KYRC25` files from step 2 need tests that a 2024-25 row survives with year `2025` and the same metric names as 2023-24. Kindergarten prior settings in `KYRC25` already include `State Funded`; confirm the slug does not collide with an existing metric name.

### 5. Tests, docs, and the year-gap check

**Size: medium.**

- Unit fixtures under `tests/` for every alias in steps 3 and 4. Follow `tests/test_graduation_rates.py` and the end-to-end pattern only for a trimmed file, not the 55 MB grade file.
- Assert KPI names still follow `{indicator}_rate_{period}`, `{indicator}_count_{period}`, `{indicator}_total_{period}`.
- Assert suppressed `Y` rows remain, with null values, and blank suppression does not.
- Update `KPIS.md` only if a metric is added or a year is intentionally missing a count. Update the README source list when the new files are wired.
- Re-run `python3 -m pytest tests/` for the touched modules.
- After the first full run, confirm `data/kpi` contains year `2025` for the `KYRC25` sources and year `2026` for the temporary sources, and that year `2024` did not gain a second copy of the 2026 assessment rows.

### 6. Leave these blocked until KDE posts the files

**Size: small once the files exist, except CTE, discipline, grade events, and precautionary measures, which need the step 4 work first.**

Re-query the historical list for `Year eq '2025-2026'`. When `KYRC26_*` appears, diff its header against `KYRC25` before adding the filename. Prefer `KYRC26` over the temporary file for the same year, school, group, and metric, because it should restore county, NCES, co-op, and the separate 4-year and 5-year files. Do not average the two.

Blocked: chronic absenteeism, enrollment, suspensions, behavior events, discipline, CTE participation, postsecondary enrollment (transition), precautionary measures.

### 7. Optional, only if the owner wants the new series

**Size: medium each.** Not required to refresh current KPIs.

- English learner attainment (`Attainment Rate`, tested count), by grade including `TL`.
- CSI / TSI / ATSI workbook, after `Code` is matched to `School Code`.
- A new SAT metric family from `College Admissions Exam_2026.CSV`. Store the measures under SAT names. Do not write them into the ACT composite series. The page description is wrong; the state numbers and the score ranges are SAT.
- Subgroup indicator scores from `Accountable_Summary_2026.CSV` (status, change, and combined rate by indicator). This is the accountability color inputs, not the student-level assessment percents already planned in step 3.

## Risks

- Loading the two assessment files before the year fix writes 2025-26 proficiency into year 2024.
- Loading kindergarten before `PRIOR_SETTING` is mapped multiplies readiness rates.
- Padding school codes on the left breaks the link to every prior year.
- CTE’s old rate-versus-count heuristic will invent participation rates above the real rate if it is pointed at the new count columns.
- Climate dedupe drops two of the three district index levels when the school name is blank.
- The 50-character IEP label and `Native Hawaiian or Other Pacific Islander` will show up as their own student groups and split trends.
- `Editing and Mechanic` will split from `Editing and Mechanics` if it is not aliased.
- Numeric grades (`08`) and ordinal grades (`8th`) will split the by-grade assessment metrics.
- Skip-if-exists will keep a temporary file after `KYRC26` is published under a different name, and both will load, duplicating 2026.
- The cooperative zip is easy to mistake for the full release.
- SAT columns titled “ACT” will corrupt the ACT trend if they are appended.

## Open questions

1. Load the temporary 2025-26 assessment files now, or wait for `KYRC26`? This plan assumes load now for the indicators in step 3, and wait for everything in step 6.
2. Backfill 2024-25 (`KYRC25`) in the same change? The config currently jumps from 2023-24 to nothing. This plan assumes yes.
3. For 2025-26 graduation, publish the rate KPIs only and leave count and cohort-total metrics absent for that year? That matches `KYRC24` and `KYRC25`, which are also rates-only. Confirm before anyone computes counts as `rate × enrollment`.
4. Fill county, NCES, and co-op for 2025-26 by joining the 2024-25 district/school list, or leave those fields blank until `KYRC26`? New and closed schools will be wrong in a join against the older list.
5. Exclude `ED COOP - ...` rows from school and district KPIs?
6. Add English learner attainment, the CSI workbook, or a separate SAT series in this round, or hold them?
7. For climate, are index scores enough for 2025-26, or should the 60 MB elementary response file be joined to the questions file and loaded too?
8. Should `Combined Writing` stay as its own subject slug (as `KYRC25` already implies), or be aliased to `writing`?
9. Should the new precautionary question (outside doors locked) change the compliance rate when that 2024-25 file is loaded?

## Verification notes

- Historical list filter `Year eq '2025-2026'` returned 0 items. `Year eq '2024-2025'` returned 99 files, modified 9–19 February 2026.
- `KYRC26` names above returned HTTP 404. `KYRC25` and `KYRC24` comparison files returned HTTP 206 with `Last-Modified` in February 2026 (`KYRC25`) or May 2025 / February 2026 (`KYRC24`).
- 2026 temporary files returned HTTP 206, `Last-Modified` 15 October 2026. The archive zip listing matches the catalog (16 files).
- Full reads from `All_Datasets_Archive.Zip` produced the row counts, code lengths, subject frequencies, demographic strings, SAT ranges, kindergarten prior settings, and the state graduation, assessment, profile, and SAT rows cited above. State reading proficient/distinguished is 50 / 48 / 49 and combined writing is 41 / 47 / 45. The profile’s state high-school postsecondary combined indicator rate is 98.7, while the news story said 98.5.
- Header comparisons for behavior location, context, legal sanctions, and the grade-level file were taken from the first 2.5 KB of each `KYRC24` and `KYRC25` file.
- Supplemental pages for enrollment, CTE, finance, educator data, school profiles, IDEA, and student health were fetched the same day. Only the CTE grant file and the economically disadvantaged qualifying file added a 2025-26 link, apart from the assessment catalog and the cooperative zip.
