# 104 - KYRC26 file-to-ETL mapping

Verified 17 October 2026 against the live container `https://kdeschoolreportcard.blob.core.windows.net/datasets/`. Names came from the Open House historical page (99 `KYRC25_` files) plus the two finance names the pipeline already configures.

https://www.education.ky.gov/Open-House/data/Pages/Historical-SRC-Datasets.aspx

The plan for this release is journal 103. This note is the map each per-script pull request should follow. Shared prep (downloader, year config, demographic years, the opt-in gate, fixtures) is already on `feature/2025-26-kyrc26`. Parser changes are not.

## What was checked

- HEAD of every `KYRC25_` name on that page, the `KYRC26_` twin, and `KYRC25_` / `KYRC26_` spending and financial summary. 97 of 99 twins returned 200. `Last-Modified` is `Thu, 15 Oct 2026 10:00:09 GMT` through `10:00:11 GMT`.
- Header of every 200 response, compared with the KYRC25 twin.
- `Demographic` values from the accountability, enrollment, safety, kindergarten, CTE, gifted, and chronic-absenteeism files. No label sits outside the 2024 `year_specific` set. `Total Events` is already in `filter_out`.

Still 404, so they are not registered:

| File | KYRC25 on the same container |
| --- | --- |
| `KYRC26_ASMT_The_ACT.csv` | present (`Last-Modified` Thu, 14 May 2026 19:11:31 GMT) |
| `KYRC26_CRDC_Chronic_Absenteeism.csv` | present |
| `KYRC26_FT_Spending_per_Student.csv` | present |
| `KYRC26_FT_Financial_Summary.csv` | present |

`datasetsdev` is not the live host. `base_url_kyrc25` and `base_url_kyrc26` both point at `datasets`.

## How a script turns 2026 on

`data/prepare_kde_data.py` downloads a `url: kyrc26` entry from `datasets`. It re-downloads when the stored `Last-Modified` differs from the blob, and when a file on disk has no `.kde-source.yaml` stamp. An unchanged stamp is left in place.

`BaseETL.accepts_data_file` skips `KYRC26_*` until the module sets `include_kyrc26 = True`. Set that flag in the same pull request that teaches the parser the new headers, not before. Two modules do not use the flag:

- `etl/safe_schools_events.py` reads a hardcoded filename list under `data/raw/safe_schools`.
- `etl/safe_schools_climate.py` reads the patterns in `get_files_to_process`.

`etl/kyrc.py` has `release_year_from_filename` (already used by `BaseETL.extract_year`) and `normalize_kyrc_grade_token` (`PS` → `Preschool`, `1st` → `Grade 1`). The grade helper is not applied inside any ETL. Chronic absenteeism already lowercases `PS` / `1st` for KYRC25, and those tokens are the same in KYRC26, so do not remap that metric family as part of a shared change.

`tests/fixtures/kyrc26_files.yaml` holds the probed inventory, the header diffs for registered files, and the column lists. `tests/fixtures/kyrc26_state_rows.yaml` holds one statewide `All Students` row for the files that were read in full.

## Header differences versus KYRC25

72 of the KYRC26 files that exist match the KYRC25 header exactly. The rest:

**Co-op spelling only** (`Co-Op` / `Co-Op Code` became `CO-OP` / `CO-OP Code`). `BaseETL` maps both. Measure columns are unchanged. This is the by-grade summative file, the assessment crosstabs, profile, summary, and `KYRC26_OVW_District_School_List.csv`.

**Registered files whose measure columns changed:**

| File | Change | Who has to handle it |
| --- | --- | --- |
| `KYRC26_SAFE_Discipline_Resolutions.csv` | `Expelled, Not Receiving Services (Ssp2)` became `(SSP2)`. The other KYRC25 casings are unchanged | `safe_schools_discipline`. `(SSP2)` already matches the mapper. The other four KYRC25/KYRC26 titles do not match the KYRC24 titles in code |
| `KYRC26_SAFE_Precautionary_Measures.csv` | Dropped `All classroom doors lock from the inside`. `All outside doors are locked during school hours` was already on the KYRC25 file | `safe_schools_climate` |
| `KYRC26_EDOP_Dual_Credit_Courses_Offered.csv` | `Couse Completers` became `Course Completers` | Already mapped in `etl/dual_credit.py` |
| `KYRC26_EDOP_Gifted_Participation_by_Grade_Level.csv` | Dropped `Grade 14` | `gifted_talented`. That grade will be absent for 2026 |
| `KYRC26_OVW_Teacher_Turnover.csv` | Added instructional-staff count, turnover count, and turnover percent | Existing columns still match. New columns are optional |
| `KYRC26_OVW_Teacher_Certification_Data.csv` | Added Rank I and Rank II count and percent | Existing columns still match. New columns are optional |
| `KYRC26_ASMT_Benchmark.csv` | `Reading` and `English` became `Reading and Writing` | Download only. No benchmark ETL |

**On the blob, not configured, not consumed by an ETL.** Do not add these in a script PR unless a new pipeline is in scope. Versus KYRC25 they were rewritten or trimmed: career studies, health and physical education, visual and performing arts, world languages (immersion column prefix `WL_`), access to technology, and student membership (dropped `Preschool`).

`KYRC26_ACCT_Survey_Results.csv` is 297,011,485 bytes. It is not registered. Question text is on each row. Load it only if question-level climate KPIs are wanted.

## ETL scripts

"Consumes today" is the file the current code and `config/kde_sources.yaml` actually use for the newest year. The KYRC26 column is the file now registered beside it. "Turn on" is what the script pull request still has to do.

State checks below are `School Code` `999000`, `Demographic` `All Students`, school year `20252026` (pipeline year `2026`).

### Opt in and add a 2026 fixture. Headers already match what the parser accepts

| Script | Consumes today | KYRC26 file | Schema versus KYRC25 | Size |
| --- | --- | --- | --- | --- |
| `chronic_absenteeism` | `KYRC25_OVW_Chronic_Absenteeism.csv` | same twin | identical. State rate 23.5, suppression `No`, grade tokens `All Grades` / `PS` / `K` / `1st` (same as KYRC25) | S |
| `graduation_rates` | `Graduation_Rate_2025.CSV` plus the two KYRC24 cohort files | `KYRC26_ACCT_4_Year_High_School_Graduation.csv`, `KYRC26_ACCT_5_Year_High_School_Graduation.csv` | identical. `4 Year Cohort Graduation Rate` is already mapped. State 94.1 and 94.5 | S |
| `english_learner_progress` | `English_Language_Proficiency_2025.CSV` | `KYRC26_ACCT_English_Learners_Progress_Proficiency_Rate.csv` | identical. Value-table headers already mapped. Level is `Elementary School` / `Middle School` / `High School` | S |
| `postsecondary_readiness` | `Postsecondary_Readiness_2025.CSV` | `KYRC26_ACCT_Postsecondary_Readiness.csv` | identical. State base 85.3, with bonus 93.9 | S |
| `postsecondary_enrollment` | `KYRC25_ADLF_Transition_to_In_State_Postsecondary_Education.csv` | same twin | identical | S |
| `cte_participation` | `KYRC25_CTE_Participation.csv` | same twin | identical. State participants 144,661 / enrollments 201,255 | S |
| `student_enrollment` | `KYRC25_OVW_Student_Enrollment.csv` | same twin | identical | S |
| `secondary_enrollment` | `KYRC25_OVW_Secondary_Enrollment.csv` | same twin | identical | S |
| `out_of_school_suspension` | `KYRC24_OVW_Student_Suspensions.csv` only. The KYRC25 twin exists and was not configured | `KYRC26_OVW_Student_Suspensions.csv` | identical to KYRC25, and those count columns are already in `module_column_mappings`. `Collected School Year` is `20212022` on every row; keep the KPI year on `School Year` | S |
| `teacher_experience` | `KYRC25_OVW_Average_Years_School_Experience.csv` | same twin | identical | S |
| `student_teacher_ratio` | `KYRC25_OVW_Student_to_Teacher_Ratio.csv` | same twin | identical | S |
| `novice_teachers` | `KYRC25_OVW_Inexperienced_Teachers.csv` and `KYRC25_OVW_Students_Taught_by_Inexperienced_Teachers.csv` | both twins | identical | S |
| `teacher_working_conditions` | `KYRC25_OVW_Teacher_Working_Conditions.csv` | same twin | identical. No `Demographic` column | S |
| `students_taught_by_ineffective_teachers` | `KYRC25_OVW_Students_Taught_by_Ineffective_Teachers.csv` | same twin | identical | S |
| `students_taught_by_out_of_field_teachers` | `KYRC25_OVW_Students_Taught_by_Out_of_Field_Teachers.csv` | same twin | identical | S |
| `teacher_turnover` | `KYRC25_OVW_Teacher_Turnover.csv` | same twin | three new instructional-staff columns. Existing turnover columns are unchanged | S |
| `teacher_certification` | `KYRC25_OVW_Teacher_Certification_Data.csv` | same twin | Rank I and Rank II columns added. Existing columns unchanged | S |
| `dual_credit` | the two `KYRC25_EDOP_Dual_Credit_*` files | both twins | courses-offered fixed `Couse Completers` → `Course Completers`, which is already mapped. Participation file is identical | S |
| `gifted_talented` | both `KYRC25_EDOP_Gifted_Participation_by_*` files | both twins | by-grade dropped `Grade 14`. Category file is identical | S |
| `advanced_coursework` | the three KYRC25 advanced-coursework files | three twins | identical | S |
| `school_courses` | `KYRC25_EDOP_School_Courses_Summary.csv` | same twin | identical | S |
| `cte_pathways` | `KYRC25_CTE_Career_Pathways.csv` | same twin | identical | S |
| `career_readiness_indicators` | `KYRC25_CTE_Career_Readiness_Indicators.csv` | same twin | identical | S |
| `district_school_list` | `KYRC25_OVW_District_School_List.csv` | same twin | co-op spelling only. `CO-OP` and `Co-Op` both map | S |
| `kindergarten_readiness` | `Kindergarten_Screen_2025.CSV` (percent headers). KYRC24 composite is also configured | `KYRC26_ASMT_Kindergarten_Screen_Composite.csv` | identical to the KYRC25 composite. State `All Students` / prior setting `All Students`: 55 / 38 / 7 / 45. Those columns are named like counts and the values are percents, which is how the KYRC24 composite already behaves. Do not also register `Kindergarten_Screen_2026` | S |

`kentucky_summative_assessment` is the same kind of change and a larger check. It consumes `Assessment_Performance_by_Grade_2025.CSV` plus the two KYRC24 summative files. Registering adds `KYRC26_ACCT_Kentucky_Summative_Assessment.csv` and `KYRC26_ASMT_Kentucky_Summative_Assessment.csv`. Versus KYRC25 the accountability file is identical and the by-grade file changes co-op spelling only. `Proficient / Distinguished` and ordinal grades such as `10th` are already mapped. Size M, because the pull request has to show level rates and one grade, not because a new header is expected. No dependency on the other scripts.

### Parser work, then turn the file on

| Script | Consumes today | KYRC26 file | What the parser does not yet accept | Size |
| --- | --- | --- | --- | --- |
| `safe_schools_discipline` | `KYRC24_SAFE_Discipline_Resolutions.csv` and `KYRC24_SAFE_Legal_Sanctions.csv` in `data/raw/safe_schools_discipline`. KYRC25 twins exist and are not configured | both twins, already registered in that directory and in `safe_schools` | Legal-sanction headers match the mapper (`Arrests`, `Charges`, and the rest). Resolution headers that still need aliases next to the KYRC24 titles: `Out-of-School Suspensions (SSP3)`, `Removal by Hearing Officer (IAES2)`, `Unilateral Removal by School Personnel (IAES1)`, `In-School Removal (INSR) or In-District Removal (INDR) >=.5`. Keep `Ssp2` if KYRC25 is loaded in the same change. `(SSP2)` already matches KYRC26 | S |
| `safe_schools_events` | hardcoded `KYRC24_SAFE_Behavior_Events_by_{Type,Grade_Level,Location,Context}.csv` under `data/raw/safe_schools` | the four twins, registered in `safe_schools`. The module will ignore them until the list names them | Type, location, and context headers match the KYRC24 strings in code. Grade headers are `PS`, `K`, `1st` … `14th`, not `Preschool` / `Grade 1`. Use `normalize_kyrc_grade_token`. `include_kyrc26` does not apply | M |
| `safe_schools_climate` | `KYRC24_ACCT_Index_Scores.csv`, `KYRC24_SAFE_Precautionary_Measures.csv`, and, when present, the 2025 temporary survey names hardcoded in `get_files_to_process` | `KYRC26_ACCT_Index_Scores.csv` and `KYRC26_SAFE_Precautionary_Measures.csv` are registered. The survey file is not | The index columns `Climate Index` and `Safety Index` already map, but the filename pattern will not see `KYRC26_ACCT_Index_Scores.csv`. Dedup omits level, so elementary, middle, and high rows that share `All Schools` collapse. Precautionary code maps shortened questions (`Visitors required to sign-in`), not `Visitors are required to sign-in to the building`. The classroom-door column is gone. Outside doors has no mapping; store it, and leave the compliance-rate item list until the owner decides | M |

### Blocked on a file that is still 404

| Script | Consumes today | Missing KYRC26 file | Size when the file appears |
| --- | --- | --- | --- |
| `spending_per_student` | `KYRC25_FT_Spending_per_Student.csv` | `KYRC26_FT_Spending_per_Student.csv` | S, after a header check |
| `financial_summary` | `KYRC25_FT_Financial_Summary.csv` | `KYRC26_FT_Financial_Summary.csv` | S, after a header check |

There is no ACT ETL module. `act_scores` in `kde_sources.yaml` still downloads `KYRC24_ASMT_The_ACT.csv`. `KYRC25_ASMT_The_ACT.csv` is on the container and is not configured. `KYRC26_ASMT_The_ACT.csv` is 404. The temporary `College Admissions Exam_2026.CSV` is SAT under ACT titles (journal 103). Do not append it.

CRDC chronic absenteeism has no ETL. The overview chronic-absenteeism file is the KPI source, and its KYRC26 twin is registered.

### No KYRC26 parser work

`census_acs` and `census_saipe` read Census APIs. `school_tract_geocoding` does not read a KYRC extract. Rerun it after `district_school_list` has 2026 rows if new schools need coordinates. That is a refresh, not a parser change.

These configured directories have a KYRC26 file registered and no module in `config/mappings.yaml`: `benchmark_assessment` (subject columns changed), `student_retention`, `dropout_rate`, `homeless_students`, `migrant_students`, `students_with_disabilities`, `educator_qualifications`, and `students_taught_by_inexperienced_teachers` (the novice-teacher ETL reads its own copy of that file). No script pull request.

## Suggested order

The first script should be small, should not be summative assessment, and should not need a new column alias.

1. `chronic_absenteeism`. One file, headers identical to the KYRC25 file develop already parses, state rate 23.5. Set `include_kyrc26` and add a fixture test. Nothing else depends on it.
2. The other opt-in rows in the first table, in the order listed. None of them block each other. Graduation, English learners, readiness, and kindergarten still also read the temporary 2025 file for 2024-25; leave those entries in place.
3. `kentucky_summative_assessment` after one opt-in pull request has landed, so the flag-and-fixture pattern is already visible.
4. `safe_schools_discipline`, then `safe_schools_events`, then `safe_schools_climate`. Discipline is aliases only. Events needs the grade helper and its own filename list. Climate needs discovery, the dedupe key, and the precautionary wording.
5. `spending_per_student` and `financial_summary` when the KYRC26 names return 200. ACT stays out until a real `KYRC26_ASMT_The_ACT.csv` exists.

Re-HEAD `datasets` after 26 October 2026 before a production download. The stamps above were taken inside the 10-day review. Skip-if-exists is no longer the rule; a later stamp replaces the local file.
