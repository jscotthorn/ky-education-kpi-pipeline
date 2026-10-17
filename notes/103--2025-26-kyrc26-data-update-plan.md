# 103 - Plan for the permanent KYRC26 (2025-26) files

Revised 17 October 2026 against `develop` (`b14ec3c`). Develop already has journal 72 for the 2024-25 incorporation, so this plan is journal 103.

Shared prep for this plan now lives on `feature/2025-26-kyrc26`. The file-by-file map, and the list of which script still has to change, is journal 104. The downloader points at the live `datasets` container, KYRC26 names are registered, demographic years 2025 and 2026 are configured, and `BaseETL` will not read a `KYRC26_` file until that module sets `include_kyrc26 = True`. Parser edits stay in the per-script pull requests.

## Recommendation

Load 2025-26 from the permanent `KYRC26_*.csv` files on the public Azure container `datasets`. Leave the OAA temporary 2026 files unused. The temporary bundle is an optional later path, described at the end, and it is the wrong shape for this pipeline.

The files to load are already online. On `datasets` they were last modified 15 October 2026 at 10:00 UTC, the morning of the public accountability release. The report-card download page publishes the same bytes from `datasetsuat6051276` (earlier Last-Modified, matching Content-MD5 on 17 October 2026). The downloader HEADs both. The Open House historical catalog still has no 2025-26 entries, and KDE has not published a date for when that catalog will list them. `https://openhouse.education.ky.gov/Home/SRCData` failed TLS verification (the server sends the leaf only), so that older catalog was not read. The 10-day regulatory review runs through 26 October 2026, so re-HEAD both containers after that date before a production download. A changed Last-Modified, ETag, or Content-MD5 replaces the local file.

## What changed from the previous plan

The previous draft was written against `main` and recommended two things develop has already overtaken:

- Backfill 2024-25 from `KYRC25`. Develop already configures those files, and the parsers for the temporary 2025 accountability files are in place. That backfill is done.
- Ingest the 15 October temporary 2026 files first. Those files use 3-digit organization codes, thin identifier columns, and several truncated headers. The permanent `KYRC26` files use the historical layout (6-digit `999000` / `001000` codes, county, NCES, co-op, `All Districts` / `All Schools`). Loading the temporary files would fight the parsers develop already has.

Checked again on develop, these earlier mapping worries are already handled and are not KYRC26 work:

| Earlier worry | Where develop handles it |
| --- | --- |
| `4-Year Graduation Rate`, `Suppressed 4-Year` | `etl/graduation_rates.py` also still maps `4 Year Cohort Graduation Rate`, which is the KYRC26 header |
| `Proficient/Distinguished` with no spaces | `etl/kentucky_summative_assessment.py`. KYRC26 uses the spaced form `Proficient / Distinguished`, which is mapped too |
| `PRIOR_SETTING` | `etl/kindergarten_readiness.py` |
| CTE count columns and rate math | `etl/cte_participation.py` (`All High School CTE Participants`, enrollments, 12th-grade counts) |
| 2025 climate survey filenames and the questions-file join | `etl/safe_schools_climate.py` patterns and `Quality_of_School_Climate_and_Safety_Survey_Questions_2025.CSV` |
| Filename year fallback, blank school name → district total | `BaseETL.extract_year`, `standardize_school_name` |
| `Native Hawaiian or Other Pacific Islander` | general map in `config/demographic_mappings.yaml` |

KYRC26 does not contain the temporary-file hazards (missing school-year column, `Editing and Mechanic`, the 50-character IEP cutoff, numeric grades `03`). Those stay in the deferred section. The only 3-digit organization codes in the permanent files are `901`–`909` on the four subject crosstabs, and they are educational-cooperative totals (CKEC through GLEC), not schools. No ETL reads those files. See `notes/kyrc26-code-domain-diff.md`.

## Where the files are

Public container, confirmed by HTTP 200 on 17 October 2026:

`https://kdeschoolreportcard.blob.core.windows.net/datasets/KYRC26_<name>.csv`

`Last-Modified` is `Thu, 15 Oct 2026 10:00:09 GMT` through `10:00:11 GMT`.

Of the 99 `KYRC25_` names on the Open House historical list, the same stem exists as `KYRC26_` for 97. The other two were renamed, not dropped: `KYRC26_ASMT_The_ACT.csv` is 404 and the SAT file is `KYRC26_ASMT_CAE.csv`; `KYRC26_CRDC_Chronic_Absenteeism.csv` is 404 and the file is `KYRC26_OVW_Chronic_Absenteeism_CRDC.csv`. The ACT rename stays out of `act_scores`. The CRDC rename is unconsumed. The overview chronic-absenteeism file remains the KPI source.

The download page links `https://kdeschoolreportcard.blob.core.windows.net/datasetsuat6051276/`. Byte MD5 matched `datasets` on all 99 KYRC26 CSVs on 17 October 2026. Last-Modified and ETag differ because the objects were written twice. `datasets` is the later copy (15 October 2026 10:00:09–10:00:11 GMT).

Finance files that exist for KYRC25 have no KYRC26 twin. That is the two files the pipeline consumes (`KYRC25_FT_Spending_per_Student.csv`, `KYRC25_FT_Financial_Summary.csv`) plus `FT_Facilities_Debt_Services`, `FT_Grants`, `FT_Learning_Environment`, `FT_Operations`, `FT_State_Funding_SEEK`, `FT_Statewide_Funding_Sources`, `FT_Taxes`, and five more spending variants: `FT_Personnel_Spending_per_Student_Federal_Funds`, `FT_Personnel_Spending_per_Student_State_Local_Funds`, `FT_Total_Spending_per_Student_All_Fund_Sources`, `FT_Total_Spending_per_Student_Federal_Funds`, and `FT_Total_Spending_per_Student_State_Local_Funds`. The extra five do not change the wait for the two consumed names.

The ACCT group is 19 CSVs, including `Survey_Results`. A count of 22 on the download page is those 19 files plus the package zip and two anchors that are not datasets. The four EDOP files for advanced courses and dual credit are in the 2025-26 tree and in `KYRC26_EDOP_Datasets.zip`. They are 2025-26 extracts. Dual-credit courses offered, state `All Enrollments`: enrollment 143,848 / completers 132,208 / qualifying 124,596.

These locations do not have the files:

- Open House `HistoricalDatasets` returns 404 for `KYRC26_OVW_Chronic_Absenteeism.csv` and `KYRC26_ACCT_4_Year_High_School_Graduation.csv`. The same folder returns 206 for the KYRC25 chronic-absenteeism file (`Last-Modified` 10 February 2026).
- SharePoint list `Historical SRC Datasets` (`Year eq '2025-2026'`) returned 0 items. `Year eq '2024-2025'` still returns rows.
- `https://kdeschoolreportcard.blob.core.windows.net/datasetsdev/` returns 404, including for `KYRC25_OVW_Chronic_Absenteeism.csv`. That is the base URL develop uses for `url: kyrc25` (`config/kde_sources.yaml` `base_url_kyrc25`). The live container name is `datasets`.

The temporary catalog remains the other 2025-26 publication. It was published 14 October 2026 at 3:55 PM and points readers at the historical page without giving a date:

https://www.education.ky.gov/Open-House/data/Pages/Assessment_Accountability_Datasets_2025-2026.aspx

## What KYRC26 actually contains

Headers below were read from the blobs. Statewide checks use `School Code` `999000` and demographic `All Students`.

Identity matches every recent KYRC year: school year `20252026` (the pipeline year is `2026`), state row `999` / `All Districts` / `All Schools` / `999000`, district totals `001000`, schools 6-digit. County, NCES, co-op, and school type are present. Suppression on the accountability files is `Y` / `N`, with `*` in suppressed values.

| Check | KYRC26 value |
| --- | --- |
| 4-year graduation rate | 94.1 |
| 5-year graduation rate | 94.5 |
| Reading proficient/distinguished, elementary / middle / high | 50 / 48 / 49 |
| Combined writing proficient/distinguished, elementary / middle / high | 41 / 47 / 45 |
| Profile, state high school, postsecondary combined indicator rate | 98.7 |
| Profile, state high school, graduation combined indicator rate | 94.7 |
| Postsecondary readiness, base rate / rate with bonus | 85.3 / 93.9 |
| Kindergarten, state, ready with interventions / ready / enrichments / total ready | 55 / 38 / 7 / 45 |
| CTE, state, participants / enrollments | 144,661 / 201,255 |

Reading and combined writing match the 15 October Kentucky Teacher release. The profile postsecondary combined rate in the file is 98.7. The news story said the readiness indicator moved to 98.5. Use 98.7. The profile graduation column is the combined indicator (94.7), not the 4-year rate (94.1).

https://www.kentuckyteacher.org/news/2026/10/kde-releases-2025-2026-assessment-and-accountability-data/

A header diff of the 99 KYRC26 CSVs against their KYRC25 twins found 73 identical headers, 12 co-op spelling changes, the discipline `(Ssp2)` to `(SSP2)` change, and 13 measure-column changes. The three kinds below are the ones that matter for parsers. The full table is `notes/kyrc26-schema-diff-report.md`.

**Co-op spelling only.** `Co-Op` / `Co-Op Code` became `CO-OP` / `CO-OP Code` on the assessment crosstabs, the by-grade summative file, profile, summary, and the district/school list. `BaseETL` already maps `CO-OP`. Measure columns on those files are unchanged. By-grade subjects and `Proficient / Distinguished` match KYRC25, including ordinal grades such as `10th`.

**Small column edits on files develop already parses:**

- Discipline resolutions: `Expelled, Not Receiving Services (Ssp2)` became `Expelled, Not Receiving Services (SSP2)`. The other KYRC25 casings are unchanged (`Out-of-School Suspensions (SSP3)`, `Removal by Hearing Officer (IAES2)`, `Unilateral Removal by School Personnel (IAES1)`, `In-School Removal (INSR) or In-District Removal (INDR) >=.5`).
- Precautionary measures: `All classroom doors lock from the inside` is gone. `All outside doors are locked during school hours` remains, along with the other KYRC25 statements (`Visitors are required to sign-in to the building`, and the rest). Column order differs from KYRC25.
- Dual credit courses offered: the KYRC25 typo `Couse Completers` is now `Course Completers`, which `etl/dual_credit.py` already maps. The alias `'Couse Completers '` in that module has a trailing space the live KYRC25 header does not. That is a develop mismatch for the old file. KYRC26 does not need it.
- Gifted participation by grade dropped `Grade 14`.
- Teacher certification gained Rank I and Rank II count and percent columns. Teacher turnover gained instructional-staff count, turnover count, and turnover percent. Existing columns remain.
- Student membership dropped `Preschool`. Develop does not parse that file.

**Rewritten education-opportunity files**, not used by the current accountability KPIs: career studies, health and physical education, visual and performing arts, world languages, and access to technology. Diff those modules before anyone adds the KYRC26 filename. Benchmark replaced `Reading` and `English` with `Reading and Writing`. There is no benchmark ETL; the file is download-only today.

Files whose headers match KYRC25 exactly, including the measure columns the parsers use: both graduation files, English learner proficiency, postsecondary readiness, kindergarten composite, accountability index scores, survey results, CTE participation, chronic absenteeism, enrollment, suspensions, transition to in-state postsecondary, behavior events by type, location, context, and grade, and legal sanctions.

Graduation is two files again, with `4 Year Cohort Graduation Rate` and `5 Year Cohort Graduation Rate`. Kindergarten keeps `Prior Setting` plus `Ready With Interventions`, `Ready`, `Ready With Enrichments`, and `Total Ready`. On the state row and on Adair County Primary Center those three components sum to 100, and the same is true of the KYRC24 and KYRC25 composite files. The current count branch therefore turns them back into the published percents and stores the percents in the count metrics. There is no tested-count column. That is the existing KYRC composite behavior. Develop's 2024-25 kindergarten KPIs come from `Kindergarten_Screen_2025.CSV` (percent headers) instead.

Suspensions carry `Collected School Year` `20212022` on every one of the 14,949 rows, while `School Year` is `20252026`. The KYRC25 file does the same thing one year earlier (`School Year` `20242025`, collected `20202021`). The suspension module maps the collected year and takes the KPI year from `School Year`. Keep that.

## What develop already loads for 2024-25

`data/prepare_kde_data.py` accepts a string filename (historical Open House prefix) or `{url: "2025"|"kyrc25"|"historical", file: "..."}`.

`url: kyrc25` is how chronic absenteeism, CTE, enrollment, secondary enrollment, transition, the teacher-quality family, spending, coursework, dual credit, gifted, career pathways, school courses, the district/school list, and financial summary reach 2024-25. On `develop` those entries point at `datasetsdev`, which answers 404. The shared prep retargets `base_url_kyrc25` and `base_url_kyrc26` at `datasets`. The same filenames return 200 there.

`url: "2025"` still points graduation, English learner proficiency, by-grade assessment, kindergarten, and postsecondary readiness at the OAA temporary 2025 files. Climate discovers the 2025 temporary survey names in code. Safe-schools behavior, discipline, precautionary measures, suspensions, and the ACT file are still KYRC24 only in `kde_sources.yaml`, even though the KYRC25 safe-schools files have been on the blob since May and June 2026.

`year_specific` in `config/demographic_mappings.yaml` stops at `"2024"`. Years 2025 and 2026 fall through to the general map, and the demographic audit then treats every group as unexpected. That is a report, not a failed load.

## Work to do for KYRC26

Sizes are the amount of code and tests. KYRC26 filenames are already registered. Turn a module on only in the pull request that makes its parser accept the file (`include_kyrc26 = True` for `BaseETL` modules). Safe-schools events and climate do not use that flag; they keep their own filename lists. Each new alias needs a fixture from a real row (state, one district, one school, one suppressed group). `tests/fixtures/kyrc26_files.yaml` and `tests/fixtures/kyrc26_state_rows.yaml` are the shared fixtures.

### 1. Point the downloader at the live container

**Done** in the shared prep (PR #36). `base_url_kyrc25` and `base_url_kyrc26` are `https://kdeschoolreportcard.blob.core.windows.net/datasets/`. `base_url_kyrc_uat` is `https://kdeschoolreportcard.blob.core.windows.net/datasetsuat6051276/`. For `KYRC25_` and `KYRC26_` names the downloader HEADs both. Matching or missing Content-MD5 keeps `datasets`. Differing Content-MD5 keeps the newer Last-Modified and records that URL. A 404 on `datasets` uses the UAT container. A local file is kept only when its stored Last-Modified, ETag (when both sides have one), and Content-MD5 (when both sides have one) still match. A file with no stamp is downloaded again. Journal 104 is the list of registered names and the per-module checklist. `BaseETL.include_kyrc26` stays false until the script PR that accepts the file.

Keep the existing `url: "2025"` entries for 2024-25. Moving those indicators onto `KYRC25_` files would change kindergarten's layout and the by-grade token, and it is a separate decision. Do not also configure a temporary 2026 filename for a metric that has a `KYRC26_` file.

### 2. Turn on filenames whose parsers already accept the header

The filenames below are registered. **Size: small** per module, except where journal 104 says otherwise: set `include_kyrc26 = True` and add the year-`2026` smoke check. Journal 104 is the checklist (exact files, header changes, fixtures, gotchas, size). Do not add the temporary 2026 file alongside them. `student_teacher_ratio` fixtures a school row, not `999000`. `advanced_coursework` expects 257 overview schools and collected year `20212022`.

| Module | File |
| --- | --- |
| `graduation_rates` | `KYRC26_ACCT_4_Year_High_School_Graduation.csv`, `KYRC26_ACCT_5_Year_High_School_Graduation.csv` |
| `english_learner_progress` | `KYRC26_ACCT_English_Learners_Progress_Proficiency_Rate.csv` |
| `postsecondary_readiness` | `KYRC26_ACCT_Postsecondary_Readiness.csv` |
| `kentucky_summative_assessment` | `KYRC26_ACCT_Kentucky_Summative_Assessment.csv`, `KYRC26_ASMT_Kentucky_Summative_Assessment.csv` |
| `kindergarten_readiness` | `KYRC26_ASMT_Kindergarten_Screen_Composite.csv` |
| `cte_participation` | `KYRC26_CTE_Participation.csv` |
| `chronic_absenteeism` | `KYRC26_OVW_Chronic_Absenteeism.csv` |
| `student_enrollment` | `KYRC26_OVW_Student_Enrollment.csv` |
| `postsecondary_enrollment` | `KYRC26_ADLF_Transition_to_In_State_Postsecondary_Education.csv` |
| `out_of_school_suspension` | `KYRC26_OVW_Student_Suspensions.csv` |
| events, type / location / context | `KYRC26_SAFE_Behavior_Events_by_Type.csv`, `_by_Location.csv`, `_by_Context.csv` |
| discipline, legal sanctions only | `KYRC26_SAFE_Legal_Sanctions.csv` |
| dual credit, gifted, teacher certification, teacher turnover, district/school list, and the other KYRC25 overview files whose headers matched | the `KYRC26_` twin |

Smoke checks on the real file, state row:

- Graduation 94.1 and 94.5, year `2026`.
- Reading proficient/distinguished 50 / 48 / 49 and combined writing 41 / 47 / 45. Subject slug for combined writing stays `combined_writing`. Grades stay `10th`-style, which is what `_normalize_grade` already does for non-numeric tokens.
- Kindergarten readiness rate 45 for the state all-students row. Count metrics will hold 55 / 38 / 7 / 45, the same way the KYRC24 composite is treated.
- CTE participation rate from 144,661 / 201,255. The count columns already go through `clean_numeric_with_commas`.
- Chronic absenteeism state rate 23.5. The suppression value on that file is `No`, which `standardize_suppression_field` already maps to `N`.
- Suspension KPI year `2026`, taken from `School Year`, with `Collected School Year` left as an attribute.

`Combined Writing` is already the KYRC25 slug. Leave it.

### 3. Parser changes the KYRC26 safe-schools files still need

**Size: medium.** These files match KYRC25, and develop never mapped the KYRC25 wording. The same aliases cover both years.

- Discipline resolutions, case-sensitive today (`etl/safe_schools_discipline.py`): add `Out-of-School Suspensions (SSP3)`, `Removal by Hearing Officer (IAES2)`, `Unilateral Removal by School Personnel (IAES1)`, and `In-School Removal (INSR) or In-District Removal (INDR) >=.5` next to the KYRC24 spellings. `Expelled, Not Receiving Services (SSP2)` already matches KYRC26. Keep the KYRC25 `Ssp2` spelling too so that year can be loaded later.
- Behavior by grade (`etl/safe_schools_events.py`): map `PS`, `1st` … `14th` onto the existing grade fields. `All Grades` and `K` already match. Enrollment's `Preschool` / `Grade 1` headers are a different file and already match.
- Precautionary measures (`etl/safe_schools_climate.py`): map the KYRC25/KYRC26 statements onto the current policy fields. The strings in code (`Visitors required to sign-in`, `All classroom doors lock from the inside.`) are not the column titles (`Visitors are required to sign-in to the building`). KYRC26 has no classroom-door column. `All outside doors are locked during school hours` has no mapping. Store the door question. Leave the existing compliance rate's item list as it is until the owner decides whether the rate should drop classroom doors and add outside doors.
- Climate discovery: `get_files_to_process` names `KYRC24_ACCT_Index_Scores.csv` and the 2025 temporary survey files. Add `KYRC26_ACCT_Index_Scores.csv` (7.1 MB). The index columns `Climate Index` and `Safety Index` already map. Dedup keys are district, school id, school name, year, student group, and metric, and the metric name `climate_index_score` does not include level. State and district rows share `All Schools` across elementary, middle, and high, so three index rows would collapse. Put level on the dedupe key or on the metric name.
- Survey microdata is `KYRC26_ACCT_Survey_Results.csv` (297,011,485 bytes). Question text and question type are on each row, so the 2025 questions-file join is not required. Load it only if question-level KPIs are wanted for 2026. The filename pattern has to include it; the lowercase `quality_of_school_climate_and_safety_survey_*.csv` glob will not.

`etl/safe_schools_events.py` `extract_year_from_school_year` still defaults unrecognized values to `2024` and has no filename fallback. KYRC26's `20252026` is 8 characters, so the KPI year will be 2026. The default is a leftover hazard for any row that loses its school year.

### 4. Demographic audit config

**Done.** `year_specific` for `2025` and `2026` is the 2024 block. A sample of KYRC26 `Demographic` columns on 17 October 2026 had no label outside that set. This stops the audit from flagging them as unexpected. It does not change metric values.

### 5. Renamed, or still absent

| Series | What exists today | What to do |
| --- | --- | --- |
| SAT, not ACT | `KYRC26_ASMT_The_ACT.csv` is 404. `KYRC26_ASMT_CAE.csv` is the permanent file and it is SAT (state reading and writing 470.9, mathematics 437.0, science 59.3, total 908.0). For KYRC25, `ASMT_CAE` and `ASMT_The_ACT` are the same ACT file. The repo still downloads only `KYRC24_ASMT_The_ACT.csv` | Keep it out of `act_scores`. SAT is a separate decision, not the next S script. Do not append the temporary college-exam file either |
| Finance | The two consumed KYRC25 files, and twelve other `FT_` names, have no KYRC26 twin | Wait until `KYRC26_FT_Spending_per_Student.csv` and `KYRC26_FT_Financial_Summary.csv` return 200. The SRC finance calendar puts the 2024-26 financial-transparency release in May 2027. The extra spending variants do not change that wait |
| CRDC chronic absenteeism | `KYRC26_CRDC_Chronic_Absenteeism.csv` is 404 because the file was renamed to `KYRC26_OVW_Chronic_Absenteeism_CRDC.csv`. Collected year on every KYRC26 row is `20242025` | Unconsumed. The overview file `KYRC26_OVW_Chronic_Absenteeism.csv` is the KPI source. Do not add a CRDC script unless a CRDC KPI is in scope |

English learner attainment, the CSI / TSI / ATSI workbook, and `Accountable_Summary_2026.CSV` exist only in the temporary bundle. The permanent profile and summary files are the KYRC-shaped ones already on the blob. Subgroup indicator scores from the temporary summary are a new series, not required to refresh current KPIs.

## Release timeline

No KDE page gives a date for the Open House historical KYRC26 listing. The pages that were checked:

- Historical datasets page. It says datasets begin with 2011-12 and that 2024-25 is available on the SRC Suite dashboard. It does not mention 2025-26 files. https://www.education.ky.gov/Open-House/data/Pages/Historical-SRC-Datasets.aspx
- 2025-26 temporary catalog, published 14 October 2026. One sentence points historical readers at the historical page and gives no date. https://www.education.ky.gov/Open-House/data/Pages/Assessment_Accountability_Datasets_2025-2026.aspx
- School Report Card Suite page, published 5 March 2026. It describes Open House as the downloadable history through "the most recent" and does not date the next dump. https://education.ky.gov/Open-House/Pages/School-Report-Card-Suite.aspx
- Supplemental assessment page, published 25 March 2026. Its table stops at 2024-25. https://www.education.ky.gov/Open-House/data/Pages/Supplemental-Data-Assessment-and-Accountability.aspx
- Public-release notices for 15 October 2026. They date the embargo, the public release, and the review window. They do not mention historical CSV files. https://content.govdelivery.com/accounts/KYDE/bulletins/42d1068 and https://www.kentuckyteacher.org/news/2026/10/kde-releases-2025-2026-assessment-and-accountability-data/
- KSIS School Report Card timeline for the 2025-26 suite. Public release is listed as 15 October 2026. Financial transparency opens to districts in March 2027 and is public in May 2027 for 2024-26 finance data. That page is the portal calendar, not a KYRC file calendar. https://www.education.ky.gov/districts/tech/sis/Pages/School_Report_Card_Resources.aspx

What the previous years did:

| School year | Public accountability release | Later Open House / historical refresh | Historical-list `Created` timestamps |
| --- | --- | --- | --- |
| 2023-24 (KYRC24) | 3 October 2024, first as Open House datasets, dashboard later because of the vendor change. https://content.govdelivery.com/accounts/KYDE/bulletins/3b8401f | 10 December 2024 commissioner note: dashboard soft launch hoped the week of 16 December; prior-year files stay in the historical datasets. https://content.govdelivery.com/accounts/KYDE/bulletins/3c68ec8 | 106 files, 28 May 2025 through 23 July 2025 |
| 2024-25 (KYRC25) | Delayed to 19 November 2025 for the new science cut scores. https://content.govdelivery.com/accounts/KYDE/bulletins/3fa7990 and https://content.govdelivery.com/accounts/KYDE/bulletins/3eae4b6 | 10-day review 19 November through 4/5 December 2025. https://content.govdelivery.com/accounts/KYDE/bulletins/3fbfe1b Post-review files announced for 3 February 2026 on the School Report Card and Open House. https://content.govdelivery.com/accounts/KYDE/bulletins/4075ddd | 99 files, 9 February 2026 through 19 February 2026 |

The gap from the fall release to the Open House list was roughly seven to nine months for KYRC24 and about two and a half to three months for KYRC25. KYRC25's fall release was itself delayed, so February is not a rule. Nothing in the 2026 notices promises either lag.

This year is different in one observed way. The historical-shaped `KYRC26_` blobs were written on the public `datasets` container the morning of the release, while the Open House list is still empty. That is earlier than either prior catalog update. It is also inside the review window (15 October through 26 October 2026). The 2024-25 post-review refresh landed about two months after the review closed. A similar refresh could replace these blobs without any change to the Open House page. Re-HEAD the container after 26 October, and again if a post-review bulletin appears.

House Bill 257 changes accountability in 2026-27. This file set is the last year for editing and mechanics, on-demand writing, and the climate-and-safety accountability indicator. Parsers for 2025-26 should keep those columns, and next year's plan should expect them to disappear.

## Deferred path: OAA temporary files

Use this only if a series is required before its `KYRC26` file exists. The SAT college-exam file and the English learner attainment file are the two pieces that have no blob twin. Everything else in the temporary zip is a thinner copy of a file that is already on `datasets`.

Temporary folder: `https://www.education.ky.gov/Open-House/data/OAA%20Temporary%20Datasets/`. `All_Datasets_Archive.Zip` is the statewide 16-file archive (17,145,212 bytes, 15 October 2026). The educational cooperative zip is a filtered subset and is missing the CSI workbook. Do not use it as the statewide source.

If this path is taken later, the parsers need the temporary identity rules, which KYRC26 does not need:

- Append `000` to a 3-digit code (`001` → `001000`, `999` → `999000`). Left-padding produces `000001`.
- Blank school name on those rows is the district or state total. `standardize_school_name` already maps a blank name to `---District Total---`.
- Keep `ED COOP - ...` rows (classification `SP`) out of the school and district grain.
- The two assessment files have no school-year column. `extract_year` can take `_2026.` from the filename; a KYRC-style name such as `KYRC26_....csv` does not match that regex and would otherwise become `2024`.
- Map the 50-character label `Students with Disabilities/IEP without Accomodatio` and alias `Editing and Mechanic` to `Editing and Mechanics`.
- Join county, NCES, and co-op from the school list. KYRC26 already carries those columns.
- The temporary kindergarten file is percents (`Total Percent Ready`, `PRIOR_SETTING`) with no tested count. The temporary by-grade file uses numeric grades (`03`), which `_normalize_grade` turns into `grade_3`, a different family from KYRC `10th`.

## Risks

- `datasetsdev` 404s. A KYRC26 entry copied onto that base URL will silently skip or fail the download.
- An update that lands only on `datasetsuat6051276` is invisible to a downloader that watches `datasets` alone. This branch probes both. If the hashes diverge after the review window, take the newer stamp and record which container it came from.
- `student_teacher_ratio` has no `999000` or `All Schools` row. A state-row smoke test will fail. Do not rebuild district and state totals in the opt-in pull request.
- `OVW_Advanced_Coursework` covers 257 schools and collected year `20212022`. An overview fixture copied from the KYRC25 total will fail.
- Configuring both the temporary 2026 assessment file and `KYRC26_ACCT_Kentucky_Summative_Assessment.csv` writes two 2026 proficiency series (`grade_3` versus `10th`, and a second copy of the level rates).
- Discipline casing drops out-of-school suspensions and three other counts if the KYRC26 file is parsed with the KYRC24 strings only.
- Grade-level behavior events disappear for `PS` and `1st`–`14th` while `All Grades` still loads.
- Climate index rows for elementary, middle, and high collapse when they share a school name and the dedupe key omits level.
- The precautionary compliance rate loses the classroom-door item, because that column is not in the KYRC26 file.
- Kindergarten count metrics for this file family store percentages. Dividing them again as if they were headcounts would halve the rate.
- Using `Collected School Year` as the suspension KPI year would file 2025-26 suspensions under 2022.
- Appending the temporary college-exam file to ACT history would mix SAT scale scores into the ACT series.

## Open questions

1. Download the 15 October blobs now, or wait until after 26 October 2026 and re-HEAD `datasets` and `datasetsuat6051276` first? The schema work can start on the current headers either way. A production load should follow the later stamp if the hashes diverge, and should record which container it came from.
2. Should 2024-25 graduation, English learners, kindergarten, by-grade assessment, and readiness stay on the temporary 2025 files? Moving them to `KYRC25_` makes the year comparable to KYRC26 and changes kindergarten's source layout and the by-grade token.
3. Should the outside-doors question enter the precautionary compliance rate, and should that rate drop the classroom-door item that KYRC26 no longer reports?
4. Are climate index scores enough for 2025-26, or should the 297 MB survey file be loaded too?
5. Add the new teacher-turnover and teacher-certification columns (instructional-staff turnover, Rank I and Rank II) as KPIs in this round?
6. `KYRC26_ASMT_CAE.csv` is the permanent SAT file. Keep it out of `act_scores` unless a separate SAT series is in scope. English learner attainment and the CSI workbook still have no KYRC26 twin.

## Verification

Checked 17 October 2026.

- Blob HEAD/GET against `datasets` and `datasetsuat6051276` for the report-card tree: 99 KYRC26 CSVs returned 200 on both, with matching Content-MD5. `KYRC26_ASMT_The_ACT.csv` and `KYRC26_CRDC_Chronic_Absenteeism.csv` are 404 because those series were renamed to `KYRC26_ASMT_CAE.csv` and `KYRC26_OVW_Chronic_Absenteeism_CRDC.csv`. Finance names, including the five additional spending variants, returned 404 for KYRC26 and 200 for KYRC25.
- School and district codes were compared without the 80-value cap. `901`–`909` on the subject crosstabs are cooperative totals. See `notes/kyrc26-code-domain-diff.md`.
- `https://openhouse.education.ky.gov/Home/SRCData` failed TLS verification (leaf certificate only). That catalog was not read.
- `datasetsdev` returned 404 for `KYRC25_OVW_Chronic_Absenteeism.csv`.
- Open House `HistoricalDatasets` GET returned 206 for `KYRC25_OVW_Chronic_Absenteeism.csv` and 404 for the two KYRC26 names above.
- SharePoint `Year eq '2025-2026'` returned an empty page. `Year eq '2024-2025'` returned a page with a next link.
- Full-file reads: suspensions (14,949 rows, one collected year), kindergarten composite (state and Adair County Primary Center), accountability assessment (state reading, mathematics, and combined writing by level), accountability profile (state high school). Range reads: graduation, readiness, English learners, CTE, chronic absenteeism, enrollment, both safe-schools event grains, discipline, legal sanctions, precautionary measures, index scores, survey header, district/school list.
- Header diff of all 97 KYRC26 files against KYRC25 produced the co-op rename, the discipline `Ssp2` → `SSP2` change, the dropped classroom-door column, the dual-credit typo fix, and the education-opportunity rewrites listed above.
- Develop code checked for each of those headers: `etl/graduation_rates.py`, `etl/kentucky_summative_assessment.py`, `etl/kindergarten_readiness.py`, `etl/cte_participation.py`, `etl/english_learner_progress.py`, `etl/safe_schools_discipline.py`, `etl/safe_schools_events.py`, `etl/safe_schools_climate.py`, `etl/out_of_school_suspension.py`, `etl/chronic_absenteeism.py`, `etl/dual_credit.py`, `etl/base_etl.py` `extract_year`, `config/kde_sources.yaml`, `config/demographic_mappings.yaml`.
