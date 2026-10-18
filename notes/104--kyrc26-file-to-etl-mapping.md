# 104 - KYRC26 file-to-ETL mapping

Verified 17 October 2026 against `https://kdeschoolreportcard.blob.core.windows.net/datasets/` and the download-page container `https://kdeschoolreportcard.blob.core.windows.net/datasetsuat6051276/`. The plan is journal 103. The completeness audit is journal 105. The uncapped school and district code diff is `notes/kyrc26-code-domain-diff.md`.

This note is the source of truth for later per-script agents. Follow the module checklist below. Do not invent a different file list, and do not treat a develop test failure on the journal 105 list as a regression.

Shared prep is on `cursor/kyrc26-shared-prep-61fb` (PR #36), which targets `feature/2025-26-kyrc26`. Parser changes stay in the per-script pull requests that target that integration branch. Do not push to `develop`, `master`, or `feature/2025-26-kyrc26` from a script PR's work branch.

## Test baseline

Unchanged `origin/develop` at `b14ec3c`, this environment, pandas 3.0.6, 17 October 2026:

`42 failed, 375 passed, 104 skipped`

The node ids are in journal 105 (census live API, pandas `StringDtype` versus `object`, climate `test_get_files_to_process` expects 4 files and finds 6, discipline rates, financial metric counts, and the rest of that list). A later script pull request should treat a failure on that list as pre-existing unless the assertion changed.

## Script progress

| Module | Status |
| --- | --- |
| `chronic_absenteeism` | Merged in PR #38 (`cursor/chronic-absenteeism-kyrc26-1b59`). Opt-in, plus a hygiene filename guard for the CRDC extract. Shared changes: `.gitignore` keeps `tests/fixtures/**/*.csv`, and `PyYAML` is a project dependency because tests import it. Details are under that module |
| `graduation_rates` | Merged in PR #39 (`cursor/graduation-rates-kyrc26-05b6`). Opt-in, the 5-year cohort alias, and a source swap in `config/kde_sources.yaml`: `Graduation_Rate_2025.CSV` is replaced by the two permanent KYRC25 cohort files. The parser skips leftover `Graduation_Rate_2024.CSV`, `Graduation_Rate_2025.CSV`, and `Graduation_Rate_2026.CSV` so those years are not written twice. No shared parser or dependency changes. Details are under that module |
| `english_learner_progress` | In review in PR #40 (`cursor/english-learner-progress-kyrc26-26cf`). Opt-in, and a source swap in `config/kde_sources.yaml`: `English_Language_Proficiency_2025.CSV` is replaced by `KYRC25_ACCT_English_Learners_Progress_Proficiency_Rate.csv`. The parser skips leftover `English_Language_Proficiency_2024.CSV`, `English_Language_Proficiency_2025.CSV`, and `English_Language_Proficiency_2026.CSV` so those years are not written twice. No shared parser or dependency changes. Details are under that module |
| `postsecondary_readiness` | In review in PR #41 (`cursor/postsecondary-readiness-kyrc26-37b0`). Opt-in, and a source swap in `config/kde_sources.yaml`: `Postsecondary_Readiness_2025.CSV` is replaced by `KYRC25_ACCT_Postsecondary_Readiness.csv`. The parser skips leftover `Postsecondary_Readiness_2024.CSV`, `Postsecondary_Readiness_2025.CSV`, and `Postsecondary_Readiness_2026.CSV` so those years are not written twice. A source row that is not `Y` and has both rates empty is skipped. That removes the empty `suppressed` `N` rows journal 75 emitted, so KYRC24 changes from 21,438 KPI rows to 15,908. 2022 and 2023 stay 18,262. No shared parser or dependency changes. Details are under that module |
| `postsecondary_enrollment` | In review in PR #43 (`cursor/postsecondary-enrollment-kyrc26-5640`). Opt-in only. Every registered prior-year URL returned 200, and no temporary OAA name was ever registered, so there is no source swap and no leftover-file skip. Headers match KYRC25. There is no Suppressed column. Known deviation from notes/8, tracked in issue #44: a `*` cell is dropped and a fully starred group emits no KPI row. That predates KYRC26, so prior-year KPI counts stay 18,737 / 7,781 / 9,509 / 9,625 / 9,679. KYRC26 writes 10,657. No shared parser or dependency changes. Details are under that module |
| `cte_participation` | In review in PR #45 (`cursor/cte-participation-kyrc26-4ad7`). Opt-in only. Every registered prior-year URL returned 200, and no temporary OAA name was ever registered, so there is no source swap and no leftover-file skip. Headers match KYRC25. There is no Suppressed column. Known deviation from notes/8, tracked in issue #46: a `*` cell is dropped and a fully starred group emits no KPI row. That predates KYRC26, so prior-year KPI counts stay 9,772 / 9,270 / 9,935 / 14,075. KYRC26 writes 14,424. A second pre-existing deviation, tracked in issue #47: the two columns the parser maps to rates (participants, completers) hold counts, as do all four measure columns. None of the headers is named like a rate. The base parser publishes a count of 100 or below as the rate and nulls a count above 100. KYRC24 has 42 genuine participation rates above 100 that the same cleaner nulls. Not changed here. No shared parser or dependency changes. Details are under that module |
| `student_enrollment` | In review in PR #48 (`cursor/student-enrollment-kyrc26-6f5d`). Opt-in, plus a local comma cleaner so pandas 3 StringDtype counts such as `669,865` survive. That cleaner restores the pandas 2.3.3 output for 2019-2025. The comma bug appears only on pandas 3, and pandas is unpinned. A filename skip for `OVW_Student_Membership` because the remaining grade headers would emit different totals. That file is not preschool membership: KYRC26 has no Preschool column on it. Every registered prior-year URL returned 200. No temporary OAA name was ever registered, so there is no source swap and no leftover-temp skip. Headers match KYRC25. There is no Suppressed column and no `*` cell. KYRC26 writes 180,076 KPI rows. `openpyxl` is not a declared dependency, so without it the 2019 workbook is skipped and the run continues. No shared parser or dependency changes. Details are under that module |
| `secondary_enrollment` | In review in PR #50 (`cursor/secondary-enrollment-kyrc26-a164`). Opt-in, plus a local comma cleaner so pandas 3 StringDtype counts such as `29,360` survive. That cleaner restores the pandas 2.3.3 KYRC25 file (6,619 KPI rows, state total 29,360). The unfixed pandas 3 run aborts that file: the missing comma total becomes `pd.NA` after the Int64 cast, and `or` raises `boolean value of NA is ambiguous`. Ignoring the abort, the same missing commas leave 6,541 rows and drop 29,360. pandas is unpinned. Every registered prior-year URL returned 200. No temporary OAA name was ever registered, so there is no source swap and no leftover-temp skip. Headers match KYRC25. There is no Suppressed column and no `*` cell. KYRC26 writes 6,955 KPI rows. KYRC24 is registered on the datasets host and writes 6,378 KPI rows, state total 28,978. The series is students with a secondary (concurrent) enrollment at this school, by grade. All Grades can exceed the published grade cells in KYRC25 and KYRC26. `openpyxl` is not a declared dependency, so without it the 2019 workbook is skipped and the run continues. No shared parser or dependency changes. Details are under that module |
| `out_of_school_suspension` | In review in PR #51 (`cursor/out-of-school-suspension-kyrc26-9246`). Opt-in. The KPI year is the report-card year: 2026 is CRDC 2021-22 and 2024 is CRDC 2020-21, and the output has no collected-year column. `KYRC25_OVW_Student_Suspensions.csv` is not registered. It republishes CRDC 2020-21 (same collected year `20202021`, same stars) under `School Year` `20242025`, and `accepts_data_file` skips that name. This module does not read `safe_schools_discipline_YYYY.csv`. `safe_schools_discipline` owns SSP3 since `e7d815a`, so 2020-2023 KPI rows from this module are 0. A total is suppressed when any component is starred: 7,173 KYRC24 totals change from 0 / `N` to empty / `Y`. KYRC24 stays 149,600 KPI rows. KYRC26 writes 149,490. pandas 2.3.3 and pandas 3.0.6 write the same 299,090 rows. No shared parser or dependency changes. Details are under that module |
| `teacher_experience` | In review in PR #53 (`cursor/teacher-experience-kyrc26-9f6f`). Opt-in, plus a local comma cleaner so an educator count such as `39,242` survives the production Int64 cast on pandas 2.3.3 and pandas 3. The old parser stripped commas only inside `should_skip_row`, which runs after that cast, so both pandas versions dropped the state row and the other comma-formatted district totals. Restored rows: 2021 +3, 2024 +4, 2025 +5. Previously published values are unchanged. The unfixed base dropped those 12 rows on pandas 2.3.3 and on pandas 3, so this is not a pandas-3-only bug. `KYRC23_OVW_Average_Years_School_Experience.csv` returns 200 and is not registered. It republishes 2022-23, and `accepts_data_file` skips that name. Every other registered URL returned 200. No temporary OAA name was ever registered. Headers match KYRC25. There is no Suppressed column and no `*` cell. KYRC26 writes 1,587 KPI rows. `openpyxl` is not a declared dependency, so without it the 2019 workbook is skipped and the run continues. No shared parser or dependency changes. Details are under that module |
| `student_teacher_ratio` | In review in PR #55 (`cursor/student-teacher-ratio-kyrc26-535c`). Opt-in. KYRC26 is school rows only: 1,322 KPI rows, no `999000` and no district total. The parser stores the published students-per-teacher figure and does not rebuild those totals. Every registered URL returned 200. No temporary OAA name was ever registered. `KYRC23_OVW_Student_to_Teacher_Ratio.csv` returns 200 and republishes 2022-23, and `accepts_data_file` skips that name. Headers match KYRC25. There is no Suppressed column and no `*` cell. Seven 2019-20 ratios stored as `48:01:00` were dropped by the old parser and are now kept, so 2020 goes from 1,521 to 1,528. The other prior-year rows are unchanged. No registered ratio contains a comma. pandas 2.3.3 and pandas 3.0.6 write the same 11,882 rows. `openpyxl` is not a declared dependency, so without it the 2019 workbook is skipped and the run continues. No shared parser or dependency changes. Details are under that module |
| `novice_teachers` | In review in PR #57 (`cursor/novice-teachers-kyrc26-8cd5`). Opt-in. `NEW_TEACHER_COUNT_18-19.xlsx` is registered. A blank `NEWPCT` with zero new teachers and a positive teacher count is 0 percent, so 2019 writes 1,636 first-year rates. `KYRC23_OVW_Inexperienced_Teachers.csv` and `KYRC23_OVW_Students_Taught_by_Inexperienced_Teachers.csv` return 200 and republish 2022-23, and `accepts_data_file` skips those names. Every other registered URL returned 200. No temporary OAA name was registered. Headers match KYRC25. There is no Suppressed column and no `*` cell. KYRC26 writes 21,244 KPI rows. The total is 143,209. A negative equity-gap cell is still dropped. 2020-2023 and KYRC26 publish a signed gap. KYRC23, KYRC24, and KYRC25 publish the absolute value. Issue #59 tracks a signed gap. Issue #58 tracks the registered KYRC24 equity copy. Neither behavior changes here. The comma strip does not change a published rate. It only keeps a comma-formatted count in the unit test. pandas 2.3.3 and pandas 3.0.6 write the same 143,209 rows. `openpyxl` is now a project dependency so the 2019 workbook is read. Shared change: `analysis/scripts/base_analysis_dataset.py` requires both novice rates (`sum(min_count=2)`), so 2019 is NaN in the combined series. Schools with neither rate had been a fabricated 0. Those 3,544 rows are now NaN (2020 1,478, 2021 1,478, 2022 260, 2023 102, 2024 71, 2025 82, 2026 73) and are mean-imputed, which changes the 2021 inputs to the graduation and postsecondary-enrollment analyses. The ETL output stays 143,209 rows. `BaseETL` was not edited. The `students_taught_by_inexperienced_teachers` download registration was left as it is. Details are under that module |
| `teacher_working_conditions` | In review in PR #60 (`cursor/teacher-working-conditions-kyrc26-abe9`). Opt-in, plus registration of `teacher_working_conditions_2020.csv`. `KYRC23_OVW_Teacher_Working_Conditions.csv` returns 200 and republishes 2022-23. It turns 945 `N/A` cells into 0, and `accepts_data_file` skips that name. A literal `N/A` has no count column, so it stays missing. A published 0 is kept. KYRC26 writes 3,282 KPI rows. 2020 writes 4,131. Prior years stay 4,113 / 3,684 / 3,675 / 3,684 / 4,302. pandas 2.3.3 and pandas 3.0.6 write the same 26,871 rows. `openpyxl` is already a project dependency from PR #57. This module does not read a workbook. No shared parser or dependency changes. Details are under that module |
| `students_taught_by_ineffective_teachers` | In review in PR #63 (`cursor/students-taught-by-ineffective-teachers-kyrc26-7131`). Opt-in, plus registration of `students_taught_by_ineffective_teachers_2020.csv`. `KYRC23_OVW_Students_Taught_by_Ineffective_Teachers.csv` returns 200 and republishes 2022-23, and `accepts_data_file` skips that name. These files publish percents and have no count column, so `N/A`, `#N/A`, and a blank stay missing. A published 0 is kept. The datasets KYRC24 object is a 39-row file and is not swapped for the historical extract. KYRC26 writes 15,255 KPI rows. 2020 writes 13,288 after skipping 188 placeholder rows whose only number is students with disabilities 0.00 or 0.000%. Prior years stay 15,687 / 16,775 / 11,959 / 12,457 / 15,219. pandas 2.3.3 and pandas 3.0.6 write the same 100,640 rows. No shared parser or dependency changes. Details are under that module |
| `students_taught_by_out_of_field_teachers` | In review in PR #65 (`cursor/out-of-field-teachers-kyrc26-bef2`). Opt-in, plus registration of `students_taught_by_out_of_field_teachers_2020.csv`. `KYRC23_OVW_Students_Taught_by_Out_of_Field_Teachers.csv` returns 200 and republishes 2022-23, and `accepts_data_file` skips that name. These files publish percents and have no count column. No registered year has a blank, `N/A`, or `#N/A` measure cell, so a published 0 is kept and the 2020 disability-only placeholder rule from ineffective teachers does not apply. The group columns are within-group rates in every year. A negative equity-gap cell is stored. The datasets KYRC24 object repairs the headers and changes school cells. It is not swapped. KYRC26 writes 18,171 KPI rows. 2020 writes 18,072. Prior years stay 18,072 / 18,090 / 18,135 / 18,198 / 18,225. pandas 2.3.3 and pandas 3.0.6 write the same 126,963 rows. No shared parser or dependency changes. Details are under that module |

`chronic_absenteeism` was re-tested on this branch after the review fixes: `42 failed, 394 passed, 104 skipped`. The 42 failure node ids match the develop baseline above. Run `python3 scripts/compare_pytest_to_journal105.py` and paste the report into the pull request. Do not commit a per-script diff file. The extra passes are the shared-prep tests already on `feature/2025-26-kyrc26` plus the chronic-absenteeism tests added here. Do not treat 375 passed as the number to match once shared prep is in the base.

`graduation_rates` compare run after the 2024 temporary-file skip, pandas 3.0.6: `42 failed, 400 passed, 104 skipped`, errors 0. The 42 failure node ids match the develop baseline. The extra pass beyond the KYRC25 source swap is `test_leftover_temporary_file_does_not_duplicate_2024`. Do not treat 394 passed as the number to match once this module merges.

`english_learner_progress` compare run, pandas 3.0.6: `42 failed, 406 passed, 104 skipped`, errors 0. The 42 failure node ids match the develop baseline. The six passes beyond the graduation-rates total are the KYRC26 acceptance test, the KYRC25 source-swap test, the leftover-temporary-file test, the blank-and-partial-blank test, the sibling-file test, and the real-row fixture test. Do not treat 400 passed as the number to match once this module merges.

`postsecondary_readiness` compare run, pandas 3.0.6: `42 failed, 411 passed, 104 skipped`, errors 0. The 42 failure node ids match the develop baseline. The five passes beyond the english-learner total are the KYRC26 acceptance test, the KYRC25 source-swap test, the leftover-temporary-file test, the blank-row skip test, and the real-row fixture test. Do not treat 406 passed as the number to match once this module merges.

`postsecondary_enrollment` compare run, pandas 3.0.6: `42 failed, 415 passed, 104 skipped`, errors 0. The 42 failure node ids match the develop baseline. The four passes beyond the readiness total are the KYRC26 acceptance test, the registered-source test, the blank-row and sibling-file test, and the real-row fixture test. Do not treat 411 passed as the number to match once this module merges.

`cte_participation` compare run, pandas 3.0.6: `42 failed, 419 passed, 104 skipped`, errors 0. The 42 failure node ids match the develop baseline. The four passes beyond the enrollment total are the KYRC26 acceptance test, the registered-source test, the blank-row and sibling-file test, and the real-row fixture test. Do not treat 415 passed as the number to match once this module merges.

`student_enrollment` compare run, pandas 3.0.6: `40 failed, 426 passed, 104 skipped`, errors 0. The 40 failure node ids are the develop baseline minus `tests/test_student_enrollment.py::TestStudentEnrollmentETL::test_clean_enrollment_data` and `tests/test_student_enrollment.py::TestCleanEnrollmentData::test_comma_removal`. Those two now pass because the StringDtype comma cleaner keeps `669,865`. The other two student-enrollment baseline failures stay. The five passes beyond the CTE total, plus those two, are the KYRC26 acceptance test, the registered-source test, the blank-row test, the membership-filename test, the real-row fixture test, and the two comma tests. The 2024 state primary anchor is an assertion inside that real-row test, so the pass count does not move. The 2024 and 2025 row difference versus a pandas 3 base run is the pandas 2.3.3 output being restored. pandas is unpinned. Do not treat 419 passed as the number to match once this module merges.

`secondary_enrollment` compare run, pandas 3.0.6: `38 failed, 433 passed, 104 skipped`, errors 0. The 38 failure node ids are the develop baseline minus the two `student_enrollment` comma tests already removed on this branch, and minus `tests/test_secondary_enrollment.py::TestSecondaryEnrollmentETL::test_clean_enrollment_data` and `tests/test_secondary_enrollment.py::TestCleanEnrollmentData::test_comma_removal`. Those two now pass because the StringDtype comma cleaner keeps `29,360`. The other two student-enrollment baseline failures stay. The five passes beyond the student-enrollment total, plus those two, are the KYRC26 acceptance test, the registered-source test, the blank-row test, the real-row fixture test, the explicit StringDtype comma test, and the two baseline comma tests. The 2025 state total, and the 2024 state total of 28,978, are assertions inside that real-row test, so the pass count does not move for them. The KYRC25 row difference versus a pandas 3 base run is the pandas 2.3.3 output being restored. pandas is unpinned. Do not treat 426 passed as the number to match once this module merges.

`out_of_school_suspension` compare run after the review fixes, pandas 3.0.6: `35 failed, 443 passed, 104 skipped`, errors 0. The 35 failure node ids are the develop baseline minus the two `student_enrollment` comma tests, the two `secondary_enrollment` comma tests, and `tests/test_out_of_school_suspension.py::TestOutOfSchoolSuspensionETL::test_convert_to_kpi_format_safe_schools`, `tests/test_out_of_school_suspension.py::TestOutOfSchoolSuspensionETL::test_negative_values_suppressed`, and `tests/test_out_of_school_suspension_end_to_end.py::TestOutOfSchoolSuspensionE2E::test_kyrc24_and_safe_schools`. Those three now pass because this module emits nothing for the annual Safe Schools files. `safe_schools_discipline` owns SSP3 since `e7d815a`. The passes beyond the secondary-enrollment total are the KYRC26 acceptance test, the registered-source test, the KYRC25 filename-skip test, the blank-row test, the StringDtype comma test, the starred-total test, the real-row fixture test, and the three baseline tests. Do not treat 433 passed as the number to match once this module merges.

`teacher_experience` compare run, pandas 3.0.6: `35 failed, 450 passed, 104 skipped`, errors 0. The 35 failure node ids are the develop baseline minus the two `student_enrollment` comma tests, the two `secondary_enrollment` comma tests, and the three `out_of_school_suspension` tests already removed on this branch. No new failures. The integration branch at `a357281` collects 583 tests. This run collects 589. The six added passes are the KYRC26 acceptance test, the KYRC23 republish test, the blank-row test, the StringDtype comma test, the registered-source test, and the real-row fixture test. Do not treat 444 passed as the number to match once this module merges.

`student_teacher_ratio` compare run, pandas 3.0.6: `35 failed, 459 passed, 104 skipped`, errors 0. The 35 failure node ids are the develop baseline minus the two `student_enrollment` comma tests, the two `secondary_enrollment` comma tests, and the three `out_of_school_suspension` tests already removed on this branch. No new failures. The integration branch at `564095d` collects 589 tests. This run collects 598. The nine added passes are the KYRC26 acceptance test, the KYRC23 republish test, the blank-row test, the StringDtype ratio test, the registered-source test, the real-row fixture test, the two parser tests for `48:01:00` and a comma numerator, and the test that rejects `15:00`, `15:2`, and `15:01:30`. Do not treat 450 passed as the number to match once this module merges.

`novice_teachers` compare run, pandas 3.0.6: `35 failed, 469 passed, 104 skipped`, errors 0. The 35 failure node ids are the develop baseline minus the two `student_enrollment` comma tests, the two `secondary_enrollment` comma tests, and the three `out_of_school_suspension` tests already removed on this branch. No new failures. The integration branch at `c68a33c` collects 598 tests. This run collects 608. The ten added passes are the KYRC26 acceptance test, the KYRC23 republish test, the blank-row test, the suppressed-row test, the StringDtype comma test, the negative-equity-gap test, the registered-source test, the real-row fixture test, the 2019 zero-fill boundary test, and the combined-rate test. Adair Learning Academy `001012` is 0.0 inside the real-row test. The follow-up adds those two tests, so the pass count is 469. Do not treat 459 passed as the number to match once this module merges.

`teacher_working_conditions` clean checkout, pandas 3.0.6: `35 failed, 476 passed, 104 skipped`, errors 0. The base before this module, the `novice_teachers` compare above, is `35 failed, 469 passed, 104 skipped`. The seven added passes are the KYRC26 acceptance test, the KYRC23 republish test, the `N/A`-and-zero test, the suppressed-row test, the StringDtype percent test, the registered-source test, and the real-row fixture test. 469 + 7 = 476. The 35 failure node ids are the develop baseline minus the two `student_enrollment` comma tests, the two `secondary_enrollment` comma tests, and the three `out_of_school_suspension` tests already removed on this branch. No new failures. The ten end-to-end tests in `tests/test_teacher_working_conditions_end_to_end.py` skip unless `data/processed/teacher_working_conditions.csv` exists. A checkout that already has that file runs them, which is why the compare script on a machine with the processed file prints `35 failed, 486 passed, 94 skipped`. 476 + 10 = 486. 104 − 10 = 94. The clean-checkout number to carry forward is 476 passed and 104 skipped. Do not treat 469 passed as the number to match once this module merges.

`students_taught_by_ineffective_teachers` clean checkout, pandas 3.0.6: `35 failed, 485 passed, 104 skipped`, errors 0. The base before this module, the `teacher_working_conditions` compare above, is `35 failed, 476 passed, 104 skipped`. The nine added passes are the KYRC26 acceptance test, the KYRC23 republish test, the `N/A`-and-zero test, the disability-only-zero test, the suppressed-row test, the StringDtype percent test, the negative-equity-gap test, the registered-source test, and the real-row fixture test. 476 + 9 = 485. The 35 failure node ids are the develop baseline minus the two `student_enrollment` comma tests, the two `secondary_enrollment` comma tests, and the three `out_of_school_suspension` tests already removed on this branch. No new failures. The seven end-to-end tests already in `tests/test_students_taught_by_ineffective_teachers_end_to_end.py` skip unless `data/processed/students_taught_by_ineffective_teachers.csv` exists. The new real-row test reads fixtures and does not skip. Do not treat 476 passed as the number to match once this module merges.

`students_taught_by_out_of_field_teachers` clean checkout, pandas 3.0.6: `35 failed, 493 passed, 104 skipped`, errors 0. The base before this module, the `students_taught_by_ineffective_teachers` compare above, is `35 failed, 485 passed, 104 skipped`. The eight added passes are the KYRC26 acceptance test, the KYRC23 republish test, the `N/A`-and-zero test, the disability-only-zero-is-kept test, the suppressed-row test, the StringDtype percent test, the registered-source test, and the real-row fixture test. 485 + 8 = 493. The existing negative-equity-gap test already passed on this branch, so the pass count does not move for it. The 35 failure node ids are the develop baseline minus the two `student_enrollment` comma tests, the two `secondary_enrollment` comma tests, and the three `out_of_school_suspension` tests already removed on this branch. No new failures. The eight end-to-end tests already in `tests/test_students_taught_by_out_of_field_teachers_end_to_end.py` skip unless `data/processed/students_taught_by_out_of_field_teachers.csv` exists. The new real-row test reads fixtures and does not skip. Do not treat 485 passed as the number to match once this module merges.

## What was checked

- The report-card with-tree API for 2025-26 has 106 active items: 99 CSVs and 7 package zips. Finance (`FT_`) is absent. 2024-25 through 2021-22 have 121 items, the same tree plus `FT_`.
- All 99 KYRC26 CSVs exist on both containers. Content-MD5 matches. Zip lengths match. `datasets` Last-Modified is `Thu, 15 Oct 2026 10:00:09 GMT` through `10:00:11 GMT`. The UAT container is earlier (6–15 October) because the objects were written twice.
- 73 KYRC26 headers match the KYRC25 twin exactly. 12 change only `Co-Op` / `Co-Op Code` to `CO-OP` / `CO-OP Code`. One changes `Expelled, Not Receiving Services (Ssp2)` to `(SSP2)`. The other 13 change measure columns. The schema table is `notes/kyrc26-schema-diff-report.md`.
- School code, school number, and district number were compared in full, without the 80-value cap. See `notes/kyrc26-code-domain-diff.md`.
- `https://openhouse.education.ky.gov/Home/SRCData` was not read. TLS verification fails because the server sends only the leaf certificate (`SSLCertVerificationError`, unable to get local issuer certificate). That is not evidence of a missing file.

## How a script turns 2026 on

`data/prepare_kde_data.py` downloads a `url: kyrc26` entry. For `KYRC25_` and `KYRC26_` names it HEADs `base_url_kyrc26` (`datasets`) and `base_url_kyrc_uat` (`datasetsuat6051276`).

- Matching or missing Content-MD5 keeps `datasets`.
- Differing Content-MD5 keeps the newer Last-Modified and stores that URL.
- A 404 on `datasets` uses the UAT container.
- A local file is replaced when Last-Modified, ETag (when both sides have one), or Content-MD5 (when both sides have one) differs, and when the file has no `.kde-source.yaml` stamp.

Re-HEAD both containers after 26 October 2026 before a production download. If the hashes diverge, take the newer stamp and record which container it came from.

`BaseETL.accepts_data_file` skips `KYRC26_*` until the module sets `include_kyrc26 = True`. Set that flag in the same pull request that accepts the file. Two modules do not use the flag:

- `etl/safe_schools_events.py` reads a hardcoded filename list under `data/raw/safe_schools`.
- `etl/safe_schools_climate.py` reads the patterns in `get_files_to_process`.

`etl/kyrc.py` has `release_year_from_filename` (already used by `BaseETL.extract_year`) and `normalize_kyrc_grade_token` (`PS` → `Preschool`, `1st` → `Grade 1`). The grade helper is not applied inside any ETL. Chronic absenteeism already lowercases `PS` / `1st` for KYRC25, and those tokens are the same in KYRC26, so do not remap that metric family.

`tests/fixtures/kyrc26_files.yaml` holds the probed inventory. `tests/fixtures/kyrc26_state_rows.yaml` holds one statewide `All Students` row where one exists. `student_teacher_ratio` cannot use a `999000` row from that fixture.

Do not register `KYRC26_ACCT_Survey_Results.csv` (297,011,485 bytes) unless question-level climate KPIs are wanted. Do not load temporary 2026 OAA files. Do not point `KYRC26_ASMT_CAE.csv` at `act_scores`.

## Renames and files that are not the next script

| KYRC25 name | KYRC26 name | Decision |
| --- | --- | --- |
| `ASMT_The_ACT` | `ASMT_CAE` | SAT, not ACT. State All Students mathematics 437.0, science 59.3, reading and writing 470.9, total score 908.0. Headers replace `Reading`, `English`, and `Composite Score` with `Reading and Writing` and `Total Score`. `KYRC26_ASMT_The_ACT.csv` and `KYRC26_ASMT_SAT.csv` are 404. For KYRC25 the two names are the same ACT file. Keep SAT out of `act_scores`. A separate SAT decision, not the next S script |
| `CRDC_Chronic_Absenteeism` | `OVW_Chronic_Absenteeism_CRDC` | Unconsumed. `KYRC26_CRDC_Chronic_Absenteeism.csv` is 404 because of the rename. School year `20252026`. Collected school year `20242025` on every row (KYRC25 collected year was `20202021`). Overview `KYRC26_OVW_Chronic_Absenteeism.csv` remains the KPI source |

The ACCT group is exactly 19 CSVs: Summary, Profile, Assessment_Participation, Kentucky_Summative_Assessment, School_Improvement, Crosstab_Reading, Crosstab_Mathematics, Crosstab_Science, Crosstab_SocialStudies, Crosstab_EditingMechanics, Crosstab_OnDemandWriting, Survey_Results, Index_Scores, English_Learners_Progress_Proficiency_Rate, 4_Year_High_School_Graduation, 5_Year_High_School_Graduation, Crosstab_Cohort_4_Year, Crosstab_Cohort_5_Year, Postsecondary_Readiness. A DOM count of 22 is those 19 files plus the package zip and two non-dataset anchors.

The four EDOP files (advanced courses offered, advanced courses participation and performance, dual credit courses offered, dual credit participation and performance) are in the 2025-26 tree and in `KYRC26_EDOP_Datasets.zip`. They are 2025-26 data. Dual-credit courses offered, state `All Enrollments`: enrollment 143,848 / completers 132,208 / qualifying 124,596, against KYRC25 136,572 / 124,028 / 117,355. School year `20252026`. Keep `dual_credit` and `advanced_coursework` as S. Do not hold them for a missing file.

These KYRC25 finance names are 404 as KYRC26. Only the first two have modules. Wait until those two names return 200. The other thirteen do not change that wait:

- Consumed: `FT_Spending_per_Student`, `FT_Financial_Summary`
- Already noted, not consumed: `FT_Facilities_Debt_Services`, `FT_Grants`, `FT_Learning_Environment`, `FT_Operations`, `FT_State_Funding_SEEK`, `FT_Statewide_Funding_Sources`, `FT_Taxes`
- Additional spending variants with no KYRC26 twin: `FT_Personnel_Spending_per_Student_Federal_Funds`, `FT_Personnel_Spending_per_Student_State_Local_Funds`, `FT_Total_Spending_per_Student_All_Fund_Sources`, `FT_Total_Spending_per_Student_Federal_Funds`, `FT_Total_Spending_per_Student_State_Local_Funds`

## Codes that are not schools

On `ACCT_Crosstab_Reading`, `ACCT_Crosstab_Mathematics`, `ACCT_Crosstab_Science`, and `ACCT_Crosstab_SocialStudies`, school codes `901` through `909` are educational-cooperative totals. `District Number` equals the code, `School Name` is blank, and level is elementary, middle, or high school.

| Code | Cooperative |
| --- | --- |
| `901` | CKEC |
| `902` | GRREC |
| `903` | KEDC |
| `904` | KVEC |
| `905` | NKCES |
| `906` | OVEC |
| `907` | SESC |
| `908` | WKEC |
| `909` | GLEC |

KYRC25 leaves the GLEC co-op cell blank. KYRC26 fills it with `GLEC`. Editing-and-mechanics and on-demand-writing crosstabs do not have these codes. No ETL reads the crosstabs. Do not pad `901` to `901000`.

## Module checklist

State checks use `School Code` `999000`, `Demographic` `All Students`, and school year `20252026` (pipeline year `2026`) unless the gotcha says that row is absent. Set `include_kyrc26 = True` in the same pull request, except events and climate, which edit their own filename lists.

### `chronic_absenteeism` — S — pilot — merged

| | |
| --- | --- |
| KYRC26 files | `KYRC26_OVW_Chronic_Absenteeism.csv` |
| Header changes | None. Same as the KYRC25 file develop already parses. No new column alias was required |
| Fixtures / assertions | State rate 23.5, suppression `No`, year `2026`. Grade tokens `All Grades`, `PS`, `K`, `1st` stay as they are for KYRC25. Checked against the blob file, not a rewritten row |
| Gotchas | One overview file. Do not read `KYRC26_OVW_Chronic_Absenteeism_CRDC.csv`. Do not remap `PS` / `1st` in shared code |

Pilot result, checked 17 October 2026 against the public `datasets` container. `include_kyrc26 = True` is the parser change. `etl/kyrc.py`, `BaseETL`, and the demographic config were not edited. Re-HEAD both files after 26 October 2026. If the hashes diverge from the stamps below, take the newer stamp and record which container it came from.

| File | Bytes | MD5 | Last-Modified |
| --- | --- | --- | --- |
| `KYRC26_OVW_Chronic_Absenteeism.csv` | 60,964,965 | `07d218e589daaac167e0faacabecfe8e` | Thu, 15 Oct 2026 10:00:10 GMT |
| `KYRC25_OVW_Chronic_Absenteeism.csv` | 61,818,307 | `86a98e257708df11a2bb5824690f76c7` | Fri, 09 Oct 2026 19:40:56 GMT |

KYRC26 has 430,848 data rows. The MD5s are of those downloaded bytes.

All Students, All Grades, KPI `year` `2026` (source `School Year` `20252026`). `All Schools` is stored as `---District Total---`.

| Row | School code | Rate | Count | Enrollment | Suppressed |
| --- | --- | --- | --- | --- | --- |
| State (`All Districts`) | `999000` | 23.5 | 149,637 | 635,917 | N |
| Fayette County district | `165000` | 25.3 | 10,561 | 41,708 | N |
| Bryan Station High School | `165170` | 43.2 | 902 | 2,088 | N |

The 2024-25 control, same organizations, All Students, All Grades: state rate 25.0, count 163,177, enrollment 651,776; Fayette `165000` rate 27.0, count 11,575, enrollment 42,878; Bryan Station `165170` rate 43.0, count 925, enrollment 2,153. Those counts and enrollments are fixed assertions. A same-branch pair of full-file runs also matched on 1,177,946 KPI rows, which only shows the new file does not clobber the old one. Review of this pull request compared the 2025 output with the base branch and found it identical.

Suppression in the file is `Yes` / `No`. `standardize_suppression_field` already maps that to `Y` / `N`, and a `Y` row is kept with an empty value. Real 2026 rows used for that check: state All Grades `Military Dependent`; state `PS` All Students; Adair County High School `001010`, All Grades, `American Indian or Alaska Native`.

Grade tokens are unchanged from KYRC25. `normalize_grade_field` lowercases tokens it does not know, so the metric suffixes are `k`, `1st`, and `ps`, not `kindergarten`, `grade_1`, or `preschool`. Do not call `normalize_kyrc_grade_token` from this module. State All Students counts: kindergarten 11,051 (2026) and 12,402 (2025); 1st 9,229 (2026) and 10,298 (2025). Preschool is suppressed at the state row, so those rows do emit `chronic_absenteeism_rate_ps` as an empty placeholder.

`All Grades` is the row with a numeric rate (14,258 in KYRC26, 14,482 in KYRC25). An offered grade that is not suppressed has a count and a blank enrollment and rate, so only the count metric is emitted. That is the state All Students kindergarten row. It is not a rule that `chronic_absenteeism_rate_k` or `chronic_absenteeism_rate_1st` are absent. A suppressed grade row emits all three metrics as empty placeholders, and a full-file run contains those names.

KYRC26 has 121,408 school-level grade rows with `Suppressed` `No` and all three measure cells empty. KYRC25 has zero such rows (those cells were `*` and `Suppressed` `Yes`). Review confirmed these are grade-not-offered, not suppression. Of the 11,531 school-grades in that blank set, 11,245 fall outside the school's served grade span. The remainder are at schools with no published grade values. The parser skips an unsuppressed empty measure, which is what notes/8 requires: `suppressed=Y` means the value was withheld, and a missing grade is the absence of a row. Emitting these as suppressed would mark a grade the school does not offer as a privacy suppression. 2026 therefore writes 809,792 KPI rows against 1,177,946 for 2025. State and district rows are not in the blank set, and `All Grades` is not in it. The locked example is Bryan Station High School `165170`, grade `K`, All Students: KYRC26 is blank / `No` and emits no `_k` row; the KYRC25 twin is `*` / `Yes` and emits the three suppressed `_k` rows.

`CO-OP Code` `901` on the Fayette rows is the CKEC cooperative code. The school code stays `165170`. That is not the crosstab `901`–`909` total.

The CRDC file is a different shape (`Demographic` values such as `Total Student Count`, measures `Total` / `Female` / `Male`, `Collected School Year` `20242025` on a `School Year` of `20252026`). It is not registered. None of its columns are the overview measures this module maps, so a parse emits no KPI rows. `accepts_data_file` still rejects a name that contains both `crdc` and `chronic_absenteeism`. That is hygiene for this directory, not a required parser change. Before adding a filename guard on another module, check whether the sibling file would actually produce rows.

Both years' demographic columns are the same 18 labels, all already in the 2026 demographic config. The full-file run reported 18 found and no unexpected labels. `KPIS.md` still describes the older `Grade N` suffixes. This file does not add those names; it uses the same ordinal suffixes KYRC25 already produced, so the KPI list was left as it is.

Fixtures are nine real rows from each year, in `tests/fixtures/chronic_absenteeism/`, including the Bryan Station kindergarten pair. `tests/test_fixture_sizes.py` fails if any CSV under `tests/fixtures/` exceeds 50 KB. `.gitignore` ignores every `*.csv`, which hid these fixtures. The exception `!tests/fixtures/**/*.csv` lets later agents commit the same kind of excerpt without `git add -f`. Raw downloads are still ignored. `PyYAML` is in `pyproject.toml` because `tests/test_kyrc26_registration.py` and `tests/test_prepare_kde_data.py` import `yaml`.

## Next-agent steps

Do these for every later script. The chronic-absenteeism pilot is the example, not a template to copy line for line.

1. Compare suppression and blank patterns, and the output row counts, with the prior year. Explain every difference. A smaller file is not automatically a dropped column. For this module the difference is the grade-not-offered blanks above.
2. "Prior year unchanged" means fixed values from the prior-year file, or a diff against output produced by the base branch. Two runs of the same branch only show that the new file does not clobber the old one.
3. Put one real fixture row in the test for each new suppression or blank pattern. This module's new pattern is school `165170`, grade `K`, All Students.
4. Before adding a filename guard, check whether the sibling file would actually produce rows. The CRDC guard is hygiene: that file maps none of the overview measure columns, so it already yields no KPI rows.
5. Re-read the module gotcha before requiring a `999000` row. `out_of_school_suspension` has no `999000` row. The KPI year is the report-card year, so 2026 is CRDC 2021-22 and 2024 is CRDC 2020-21, and the output has no collected-year column. `KYRC25_OVW_Student_Suspensions.csv` republishes CRDC 2020-21 and is not registered. `student_teacher_ratio` has no state row and no district `All Schools` row in KYRC26; fixture a school row. `novice_teachers` does have a `999000` row on each KYRC26 file. The student file is also registered under `students_taught_by_inexperienced_teachers` for download only; this ETL reads the `novice_teachers` copy. A blank 2018-19 percent with zero new teachers is 0.0. A negative equity-gap cell is dropped by the existing `>= 0` check. 2020-2023 and KYRC26 publish a signed gap. KYRC24 and KYRC25 publish the absolute value. Issue #59 tracks the signed gap. Issue #58 tracks the registered KYRC24 equity copy. Do not add a shared helper for a token or header that only one script needs. `teacher_working_conditions` has a `999000` row and no demographic column. The value is percent favorable, not an index to rescale. A literal `N/A` has no response count, so it is not a zero. A published 0 is kept. `teacher_working_conditions_2020.csv` is registered. `KYRC23_OVW_Teacher_Working_Conditions.csv` republishes 2022-23 and is skipped. `TELL_EQUITY_17-18.xlsx` and `TELL_EQUITY_18-19.xlsx` are school year `20172018` and use `EQUITY_MEASURE`, so this parser emits nothing from them. Do not register them as this metric. `students_taught_by_ineffective_teachers` has a `999000` row. The value is the published percent. In 2020-2023 and in KYRC25 and KYRC26 the group columns are shares of all students. The registered historical KYRC24 file behaves like a within-group rate. These files have no count column, so a blank, `N/A`, or `#N/A` percent stays missing. A published 0 is kept. A 2020 row whose every percent other than students with disabilities is missing is skipped. Sunshine School `236021` writes nothing. `students_taught_by_ineffective_teachers_2020.csv` is registered and writes 13,288 KPI rows. `KYRC23_OVW_Students_Taught_by_Ineffective_Teachers.csv` republishes 2022-23 and is skipped. The datasets KYRC24 object is 39 rows and is not the registered historical file. A negative equity-gap cell is still dropped. In 2021-2023 the gap is Not Title I minus Title I. In 2020 it is Title I minus Not Title I on 4 of 5 differing rows, and the state −1.19 matches neither. Issue #59 tracks the signed gap. Issue #64 tracks the subtracted complement columns and the 2023 `N/A` rows that are 0 in 2024. `students_taught_by_out_of_field_teachers` has a `999000` row. The value is the published percent, and the group columns are within-group rates in every registered year, including historical KYRC24. These files have no count column and no blank measure cell, so a published 0 is kept. There is no 2020 disability-only placeholder row. A negative equity-gap cell is stored. In 2020-2023 the gap is Title I minus Not Title I on every paired cell. KYRC26 follows that difference, with 32 cells off by 0.1. KYRC24 and KYRC25 are mixed. Issue #59 tracks the signed gap. `students_taught_by_out_of_field_teachers_2020.csv` is registered and writes 18,072 KPI rows. `KYRC23_OVW_Students_Taught_by_Out_of_Field_Teachers.csv` republishes 2022-23 and is skipped. The datasets KYRC24 object repairs the headers and changes school cells. It is not the registered historical file. The UAT KYRC25 object negates some equity-gap cells. datasets has no Content-MD5, so the downloader keeps datasets.
6. Run `python3 scripts/compare_pytest_to_journal105.py` and paste its report into the pull request body. Do not commit a per-script diff file. The script exits nonzero when a failure node id is new or any test errors. A failure already on the journal 105 list is not a regression.

### `graduation_rates` — S — in review

| | |
| --- | --- |
| KYRC26 files | `KYRC26_ACCT_4_Year_High_School_Graduation.csv`, `KYRC26_ACCT_5_Year_High_School_Graduation.csv` |
| Header changes | None versus KYRC25. `4 Year Cohort Graduation Rate` was already mapped. `5 Year Cohort Graduation Rate` was not, so the 5-year file emitted no rows until this alias. School-code sets are identical to KYRC25 |
| Fixtures / assertions | State 94.1 and 94.5, year `2026`. Checked against the blob file, not a rewritten row |
| Gotchas | `Graduation_Rate_2025.CSV` is 404. The permanent KYRC25 cohort files are the 2024-25 source. Skip leftover `Graduation_Rate_2024.CSV`, `Graduation_Rate_2025.CSV`, and `Graduation_Rate_2026.CSV` so those years are not written twice. `analysis/scripts/graduation_analysis.py` now sees 2025 A1 rows. `scripts/generate_report_drafts.py` still looks for district `State` |

Result, checked 17 October 2026 against the public `datasets` container. `include_kyrc26 = True`, the 5-year cohort alias, and the `kde_sources.yaml` source swap are the changes. `etl/kyrc.py`, `BaseETL`, and the demographic config were not edited. Re-HEAD the cohort files after 26 October 2026. If the hashes diverge from the stamps below, take the newer stamp and record which container it came from.

`datasets` HEAD for the KYRC26 files did not return Content-MD5. The UAT Content-MD5 matches the bytes downloaded from `datasets`, and `datasets` Last-Modified is newer, so these are the `datasets` objects.

| File | Bytes | MD5 | Last-Modified |
| --- | --- | --- | --- |
| `KYRC26_ACCT_4_Year_High_School_Graduation.csv` | 1,175,641 | `21244adbcc429ba9158d4e2f790431de` | Thu, 15 Oct 2026 10:00:09 GMT |
| `KYRC26_ACCT_5_Year_High_School_Graduation.csv` | 1,175,588 | `99fbcad12c03186b5733f609eddcfc97` | Thu, 15 Oct 2026 10:00:09 GMT |
| `KYRC25_ACCT_4_Year_High_School_Graduation.csv` | 1,175,161 | `7e4fb3432624c4a8681a46862418e618` | Thu, 06 Aug 2026 17:29:55 GMT |
| `KYRC25_ACCT_5_Year_High_School_Graduation.csv` | 1,174,850 | `ddcd0df0de09e52291c98a78353b03ac` | Thu, 14 May 2026 19:11:31 GMT |
| `KYRC24_ACCT_4_Year_High_School_Graduation.csv` | 1,148,155 | `57a926b137aedabf29343e2cbc4dc5e3` | Thu, 14 May 2026 19:11:28 GMT |
| `KYRC24_ACCT_5_Year_High_School_Graduation.csv` | 1,148,527 | `e7a9890825ada247494fbbeee09e759b` | Thu, 14 May 2026 19:11:28 GMT |

Each cohort file has 9,528 data rows and the same 397 school codes. The MD5s are of those downloaded bytes. `Graduation_Rate_2025.CSV` returned 404 on the OAA temporary URL, on both blob containers, on the historical-datasets host, and on the Wayback lookup used here. It is no longer registered. `Graduation_Rate_2024.CSV` and `Graduation_Rate_2026.CSV` are not registered either. `accepts_data_file` skips all three temporary names, because those files use `4-Year Graduation Rate` and `5-Year Graduation Rate`, which this module already maps. A leftover copy would write a second year of rows next to the permanent cohort files. Historical `graduation_rate_2023.csv` and earlier names are still accepted.

All Students, KPI `year` `2026` (source `School Year` `20252026`). `All Schools` is stored as `---District Total---`.

| Row | School code | 4-year | 5-year | Suppressed |
| --- | --- | --- | --- | --- |
| State (`All Districts`) | `999000` | 94.1 | 94.5 | N |
| Fayette County district | `165000` | 93.9 | 93.7 | N |
| Bryan Station High School | `165170` | 93.8 | 94.0 | N |
| Adair County High School | `001010` | 97.6 | 97.5 | N |

Bryan Station `state_school_id` `034165170`, `nces_id` `210186000388`. Fayette district `nces_id` `2101860`, `co_op` `CKEC`, `co_op_code` `901`. That `901` is the cooperative code. The school code stays `165000`. KYRC25 stored Bryan Station's NCES id as `210186000000`; KYRC24 and KYRC26 both use `210186000388`.

The configured 2023-24 file is `KYRC24_ACCT_4_Year_High_School_Graduation.csv`: state 92.3, Fayette 92.8, Bryan Station 94.1. A base-branch run of that file wrote 7,606 KPI rows. This branch matches that output exactly, ignoring `last_updated`. The KYRC25 4-year file was not a configured production source on the base branch. Parsed here it writes 7,622 rows: state 93.6, Fayette `165000` 92.6, Bryan Station `165170` 93.7. The KYRC25 5-year file writes 7,648 rows: state 94.2, Fayette 94.4, Bryan Station 95.2.

`config/kde_sources.yaml` did not register either KYRC25 cohort file. 2024-25 production came from `Graduation_Rate_2025.CSV`, and that temporary file already mapped `5-Year Graduation Rate`. The alias therefore does not add a new production 2025 series on top of an existing one. The only already-configured file that gained rows is `KYRC24_ACCT_5_Year_High_School_Graduation.csv`: the base branch wrote no rows from it, and this branch writes 7,565 (state 93.4). No 4-year value the base parser already emitted changed.

The temporary file itself could not be re-read. There is no cached copy and no git fixture. Journal 74 (19 November 2025) is the surviving record: 13,200 source rows became 19,134 KPI rows, 9,692 of them `graduation_rate_4_year` and 9,442 `graduation_rate_5_year`. The permanent KYRC25 files are a 24-by-397 grid, 9,528 rows each, and together they write 15,270 KPI rows (7,622 + 7,648). Only the KYRC25 names are registered, so those 15,270 rows replace the temporary file's 19,134. They are not added to them. The November 2025 media packet printed a 4-year All Students rate of 93.5 and a 5-year rate of 94.2. The permanent file's state cells, which this pipeline stores, are 93.6 and 94.2.

The 3,864-row drop is not a value revision, and no groups or metrics are lost. The temporary `Graduation_Rate_YYYY.CSV` format includes entities the permanent cohort files leave out: alternative and special-program schools (A3 and A5–A8), KSB/KSD, National Guard Youth, and ED co-op totals 901–909. It also carried fake 5-year suppressed placeholders, because the old parser copied the 4-year suppression flag onto the whole row. The cohort output, in turn, has some suppressed rows the temporary format lacked. The 2025 temporary file cannot be recovered, so the breakdown below is the 2023-24 analog (temporary extract versus the KYRC24 cohort files): 18,890 rows versus 15,171. That gap is +4,011 from 144 entities missing from the cohort files, +478 fake 5-year placeholders, and −770 cohort-only suppressed rows. For 2025, (550 − 397) = 153 extra entities × 24 groups = 3,672 rows, which is consistent with the 3,864 drop. The swap also fixes identifier consistency. The temporary format used 3-digit ids with district `State`. Cohort files use the 6-digit school code and district `All Districts`. The old path stored 172 real 5-year rates as suppressed.

Journal 74's Adair County district sample was 4-year 96.5 / Female 98.0 / Male 95.0 and 5-year 98.3 / 100.0 / 97.0. Adair County district `001000`, which is the same numbers as Adair County High School `001010`, matches the 5-year sample exactly (98.3, 100.0, 97.0). The 4-year Male rate matches 95.0. All Students is 97.0 rather than 96.5, and Female is 99.0 rather than 98.0. That movement is consistent with KDE's routine temporary-to-permanent 4-year revisions. On the 2023-24 pair, 15.4% of 4-year values changed, by up to 5.6 points, and 3.2% of 5-year values changed.

The temporary 2025 file had no School Type column, so its rows failed `analysis/scripts/graduation_analysis.py`'s `school_type == 'A1'` filter and never entered the analysis. The KYRC25 files set School Type. 8,730 year-2025 A1 KPI rows now pass that filter: 4,355 from the 4-year file and 4,375 from the 5-year file (suppression `N` or `Y`). Model results will change.

Follow-up, not fixed here. `scripts/generate_report_drafts.py` selects state rows with `district == 'State'`. Every KYRC state row uses `All Districts` (graduation 2024–26, chronic absenteeism, English learner progress 2024–26, and postsecondary readiness 2024–26). The graduation state average in those drafts will be missing until that lookup is updated. For English learner progress that lookup now finds no state row in 2024, 2025, or 2026: KYRC24 already stored `All Districts`, and the 2024-25 temporary file that used `State` is no longer registered. The 2022 and 2023 historical proficiency files still have district `State` (33 rows, school code `999`), so those two years still match. For postsecondary readiness the same `district == 'State'` lookup finds no state row in 2024, 2025, or 2026. KYRC24 already stored `All Districts`, and the 2024-25 temporary file that used `State` is no longer registered. The 2022 and 2023 historical readiness files still have district `State`.

Suppression in the file is `N`, `Y`, or blank. `Y` is `*` and is kept with an empty value. Blank means the rate cell is empty too, and that row is not emitted. There is no new code. KYRC24 stores the blank rate as one space; `to_numeric` already drops it, so do not add a space to the shared missing-value list for this file.

| File | N | Y | Blank | KPI rows |
| --- | --- | --- | --- | --- |
| KYRC25 4-year | 5,733 | 1,889 | 1,906 | 7,622 |
| KYRC26 4-year | 5,738 | 1,985 | 1,805 | 7,723 |
| KYRC25 5-year | 5,607 | 2,041 | 1,880 | 7,648 |
| KYRC26 5-year | 5,733 | 1,889 | 1,906 | 7,622 |

KYRC26 writes 15,345 KPI rows (7,723 + 7,622). The 4-year file gains 101 rows and the 5-year file loses 26 against its own prior-year file. Same 9,528 keys in each pair. The 4-year net is 461 cells moving blank to `Y` and 360 moving `Y` to blank. The 5-year file's own prior year nets 412 cells moving blank to `N` or `Y` and 438 moving `Y` to blank.

The KYRC26 5-year `N` / `Y` / blank counts are exactly the KYRC25 4-year counts (5,733 / 1,889 / 1,906). That is the same cohort one year later, not a copied file. The 9,528 keys match. 9,502 of them (99.73%) keep the same suppression code: 5,728 stay `N`, 1,898 stay blank, and 1,876 stay `Y`. The 26 codes that change cancel in pairs, 8 blank to `Y` against 8 `Y` to blank, and 5 `N` to `Y` against 5 `Y` to `N`, so the three totals do not move. Among the 5,728 rows with a numeric rate in both cells, 5,554 (96.96%) have a 5-year rate at or above the prior 4-year rate. The mean change is +0.82 points.

The locked blank example is Bryan Station `165170`, 4-year, `American Indian or Alaska Native`: KYRC26 is blank and emits no row; the KYRC25 twin is `*` / `Y` and emits the suppressed placeholder. The reverse, same school, 4-year `Military Dependent`, is blank in KYRC25 and `Y` in KYRC26. Adair County High School `001010`, `American Indian or Alaska Native`, is blank in both years. The only KYRC26 state suppression is 5-year `American Indian or Alaska Native` (`*`); that group's KYRC25 5-year rate is 88.0. School-level suppressions used for the check: Bryan Station `Asian` and Adair County High School `Foster Care`, both `Y` / `*`.

`CO-OP` `GREC` is the value on 528 rows in every KYRC24 and KYRC25 graduation file, both the 4-year and the 5-year extracts. KYRC26 replaces those 528 rows with `GLEC` in each file and has no `GREC` rows. The school code does not change, and nothing in this pipeline groups or joins on the co-op name. A later cross-year grouping on `co_op` would split that cooperative in two.

Both years' demographic columns are the same 24 labels, all already in the 2026 demographic config. The full-file run reported 24 found and no unexpected labels. No new KPI names. `graduation_rate_4_year` and `graduation_rate_5_year` are the only metrics these files emit. There are no count or total columns.

`KYRC26_ACCT_Crosstab_Cohort_4_Year.csv` and `KYRC26_ACCT_Crosstab_Cohort_5_Year.csv` are not registered under this directory. Their measure is `Year Cohort Rate`, and they have no `Demographic` column, so a parse emits no KPI rows. No filename guard. `KYRC26_ASMT_Measurements_of_Interim_Progress_Graduation_Rate.csv` is a different series and is not this module.

Fixtures are real rows in `tests/fixtures/graduation_rates/`: nine KYRC26 4-year rows, seven KYRC26 5-year rows, and the prior-year anchors, including the Bryan Station American Indian pair and one KYRC24 space-blank row. `tests/test_fixture_sizes.py` passes.

Lessons for later agents:

1. "Header changes: none" means the KYRC26 header matches KYRC25. It does not mean every measure column is already mapped. Check the twin of the column the checklist names. This module's 4-year cohort header was mapped and its 5-year cohort header was not, so the base branch emitted no rows from the KYRC24 5-year file. The temporary 2025 file used `5-Year Graduation Rate` and did emit 5-year rows.
2. Compare the suppression value set, then the counts. Here the codes are still `N`, `Y`, and blank. Output row counts moved because cells changed code on the same keys. `N` plus `Y` equals the KPI rows, and blank emits nothing. Confirm that before adding a skip rule.
3. A blank suppression cell is not `Y`. Emitting it as suppressed would mark a cohort the file did not report as a privacy suppression. notes/8 still applies.
4. Before adding a filename guard, parse one sibling row. The cohort crosstabs emit nothing, so they have no guard. `Graduation_Rate_2024.CSV`, `Graduation_Rate_2025.CSV`, and `Graduation_Rate_2026.CSV` do emit rows, because their hyphenated headers were already mapped. The guard exists so a leftover copy cannot sit beside the KYRC cohort files and write that year twice. The match is case-insensitive. The hyphenated-format unit tests use `graduation_rate_2024_format.csv` so they still exercise those headers. Historical `graduation_rate_2023.csv` and earlier names stay.
5. Prior-year identity is a diff of the files the base parser already emitted. This module's KYRC24 4-year file matches that output. The production rows added by the new alias are the KYRC24 5-year rows only. KYRC25 was not registered. Its 2024-25 rates already came from `Graduation_Rate_2025.CSV`, which mapped `5-Year Graduation Rate`. Replacing that dead file is a source swap, and the two sources must not both be registered.
6. Do not add a shared helper for a header only this file uses. The 5-year alias lives on this module. Do not add a space to the shared missing-value list; KYRC24's space-blank rate is already skipped.

### `english_learner_progress` — S — in review

| | |
| --- | --- |
| KYRC26 files | `KYRC26_ACCT_English_Learners_Progress_Proficiency_Rate.csv` |
| Header changes | None versus KYRC25. Value-table headers were already mapped, including the uppercase temporary-file headers |
| Fixtures / assertions | State elementary 28 / 33 / 24 / 15, year `2026`. Levels `Elementary School`, `Middle School`, `High School`. Checked against the blob file, not a rewritten row |
| Gotchas | `English_Language_Proficiency_2025.CSV` is 404. The permanent KYRC25 file is the 2024-25 source. Skip leftover `English_Language_Proficiency_2024.CSV`, `English_Language_Proficiency_2025.CSV`, and `English_Language_Proficiency_2026.CSV`. `KYRC26_ACCT_English_Learners_Attainment.csv` is 404 and is not this module. `scripts/generate_report_drafts.py` still looks for district `State`. The three EL analysis scripts still cap the year at 2025 |

Result, checked 17 October 2026 against the public `datasets` container. `include_kyrc26 = True` and the `kde_sources.yaml` source swap are the parser changes. `etl/kyrc.py`, `BaseETL`, and the demographic config were not edited. Re-HEAD the proficiency files after 26 October 2026. If the hashes diverge from the stamps below, take the newer stamp and record which container it came from.

`datasets` HEAD for the KYRC26 file did not return Content-MD5. The UAT Content-MD5 matches the bytes downloaded from `datasets`, and `datasets` Last-Modified is newer, so this is the `datasets` object. The KYRC25 UAT Content-MD5 also matches the `datasets` download. Matching hashes keep `datasets` even though that UAT Last-Modified is later.

| File | Bytes | MD5 | Last-Modified |
| --- | --- | --- | --- |
| `KYRC26_ACCT_English_Learners_Progress_Proficiency_Rate.csv` | 2,928,044 | `de16ecb7b399744797e2b122c487c5cb` | Thu, 15 Oct 2026 10:00:09 GMT |
| `KYRC25_ACCT_English_Learners_Progress_Proficiency_Rate.csv` | 2,928,882 | `4e8d2db903f91dc908d642bc402c19af` | Thu, 14 May 2026 19:11:31 GMT |
| `KYRC24_ACCT_English_Learners_Progress_Proficiency_Rate.csv` | 2,935,267 | `d38c9d255fdf3b1985359f8a75a59353` | Thu, 14 May 2026 19:11:28 GMT |

The historical-datasets host serves the same KYRC24 bytes. Its KYRC25 copy is the same length and a different file (`7712d55bfc102b12b3bf2a625c730aa9`, Last-Modified Tue, 10 Feb 2026 20:37:51 GMT). KDE revised that file between the February historical copy and the May `datasets` file this config registers. The two copies differ in 51 rows, by up to 4 points. The revised rows are Jefferson County (16), Scott (8), Warren (7), Henderson (6), Shelby (6), Fayette (5), and Boone (3). The 4-point change is Tates Creek High School, White (non-Hispanic). The registered `kyrc25` URL is `datasets`. The MD5s above are of the downloaded bytes.

KYRC26 has 19,316 data rows. KYRC25 has 19,338. KYRC24 has 19,360. Each year has 11 demographic labels, all already in the 2026 demographic config. The full-file run reported 11 found and no unexpected labels. No new KPI names. The twelve metrics are the four value-table scores at elementary, middle, and high.

All Students, KPI `year` `2026` (source `School Year` `20252026`). `All Schools` is stored as `---District Total---`. Score order is 0, 60-80, 100, 140.

| Row | School code | Level | Scores | Suppressed |
| --- | --- | --- | --- | --- |
| State (`All Districts`) | `999000` | Elementary | 28, 33, 24, 15 | N |
| State (`All Districts`) | `999000` | Middle | 54, 29, 12, 5 | N |
| State (`All Districts`) | `999000` | High | 62, 25, 10, 3 | N |
| Fayette County district | `165000` | Elementary | 24, 33, 25, 18 | N |
| Fayette County district | `165000` | High | 66, 23, 9, 2 | N |
| Bryan Station High School | `165170` | High | 70, 19, 8, 2 | N |
| Adair County district | `001000` | Elementary | 30, 39, 30, blank | N |

Bryan Station `state_school_id` `034165170`, `nces_id` `210186000388`, `school_type` `A1`. Fayette district `nces_id` `2101860`, `co_op` `CKEC`, `co_op_code` `901`. That `901` is the cooperative code. The school code stays `165000`. KYRC25 stored Bryan Station's NCES id as `210186000000`. KYRC24 and KYRC26 both use `210186000388`.

The configured 2023-24 file is `KYRC24_ACCT_English_Learners_Progress_Proficiency_Rate.csv`. A base-branch run wrote 31,196 KPI rows. This branch matches that output exactly, ignoring `last_updated`. State elementary All Students is 29, 35, 23, 13. Fayette elementary is 25, 36, 24, 15. Bryan Station high is 64, 29, 7, 0. Adair County elementary is 38, 46, 15, and score 140 is blank.

`config/kde_sources.yaml` did not register the KYRC25 proficiency file. 2024-25 production came from `English_Language_Proficiency_2025.CSV`. That temporary file returned 404 on the OAA URL, on both blob containers, on the historical-datasets host, and on the Wayback lookup used here. It is no longer registered. The permanent KYRC25 file, parsed with the unchanged parser, writes 32,351 KPI rows. State elementary All Students is 30, 35, 22, 13. Fayette elementary is 29, 35, 23, 14. Journal 77's Adair County elementary sample is 33, 47, 14, 5, and the permanent file matches those four cells.

Journal 77 (19 November 2025) is the surviving record of the temporary file: 19,635 source rows became 33,353 KPI rows, split 17,708 elementary, 8,300 middle, and 7,345 high. The permanent KYRC25 file has 19,338 source rows and writes 32,351 KPI rows: 17,319 elementary, 7,980 middle, and 7,052 high. Only the KYRC25 name is registered, so those 32,351 rows replace the temporary file's 33,353. They are not added to them.

The 297 figure is source rows, not KPI rows. It is 9 educational cooperatives, codes 901–909, times 11 demographic groups times 3 levels. Those cooperative rows are what the permanent file leaves out, and they produce the 1,002 KPI-row gap: 17,708 − 17,319 = 389 elementary, 8,300 − 7,980 = 320 middle, and 7,345 − 7,052 = 293 high (389 + 320 + 293 = 1,002).

Two other years have the same shape, and the 2025 temporary file itself cannot be re-read. The live `English_Language_Proficiency_2026.CSV` (1,665,216 bytes, not registered) versus KYRC26 differs by +1,031 KPI rows (34,708 versus 33,677), and every one of those rows is a cooperative total. After the 3-digit district codes are padded, the other 19,316 keys match. Suppression codes match. The 13,521 published numeric score cells match. Each of the four score columns also differs in 5,039 cells, all `*` in KYRC26 versus empty in the temporary file, on `Suppressed` `Y`. The temporary file stores the state district as `State` and the state school code as `999`. A Wayback copy of the 2023-24 temporary file versus KYRC24 differs by +1,003 KPI rows, again all cooperative totals.

Suppression in the file is `N`, `Y`, or blank. `Y` is `*` and is kept as four empty metrics. Blank means every score cell is empty too, and that row is not emitted. An `N` row with one empty score emits the other scores and omits that one. There is no new code. About 97% of KYRC24's empty score cells are a single space (44,832 of 46,244). Those are the blank-suppression rows, and every score on those rows is one space. The other 1,412 empty cells are truly empty and sit on `Suppressed` `N` rows. `to_numeric` already drops the space, so do not add a space to the shared missing-value list.

| File | N | Y | Blank | KPI rows |
| --- | --- | --- | --- | --- |
| KYRC24 | 3,390 | 4,762 | 11,208 | 31,196 |
| KYRC25 | 3,603 | 4,771 | 10,964 | 32,351 |
| KYRC26 | 3,673 | 5,039 | 10,604 | 33,677 |

`N` plus `Y` is not the KPI count. A `Y` row emits four metrics. An `N` row emits one metric per filled score cell. Blank emits nothing. KYRC26 writes 1,326 more KPI rows than KYRC25. The source-row change is six schools, not a dropped column. Removed: Wurtland Middle `221024` and Wurtland Elementary `221110` (KEDC), Young Elementary `275374` (GREC), and Arlie Boggs Elementary `331210` (KVEC), 55 rows. Added: Mary E. Britton Middle `165220` (CKEC) and Greenup County School of Innovation `221015` (KEDC), 33 rows. On the 19,283 shared keys, 584 cells move `Y` to blank and 910 move blank to `Y`. 190 move `N` to `Y` and 244 move `Y` to `N`. 10 move blank to `N`. None move `N` to blank.

The locked blank example is Bryan Station `165170`, high school, `American Indian or Alaska Native`: KYRC26 and KYRC25 are blank and emit no row. The KYRC24 twin stores the empty scores as one space and also emits no row. The partial blank is Adair County district `001000`, elementary, All Students: KYRC26 score 140 is empty and `Suppressed` is `N`, so the other three scores are kept (30, 39, 30). KYRC24 already omits that score (38, 46, 15). KYRC25 publishes it as 5. State suppressions, all three years, are middle and high `American Indian or Alaska Native`. Elementary for that group is published. In KYRC26 those scores are 28, 25, 33, 15. School-level suppression used for the check: Bryan Station high `Asian`, `Y` / `*`.

`CO-OP` `GREC` is 1,529 rows and 133 school codes in the KYRC25 file. KYRC26 has no `GREC` rows. 132 of those codes are `GLEC` (1,518 rows). The 133rd is Young Elementary `275374`, which left the file. Nothing in this pipeline groups or joins on the co-op name. A later cross-year grouping on `co_op` would split that cooperative in two.

`KYRC26_ACCT_English_Learners_Attainment.csv` is 404. The live temporary attainment file, `KYRC26_ASMT_English_Language_Proficiency.csv`, and `KYRC26_OVW_English_Learners.csv` do not use the value-table headers, so a parse emits no KPI rows. No filename guard. `English_Language_Proficiency_2026.CSV` does emit rows, because its uppercase headers were already mapped. The guard exists so a leftover copy cannot sit beside the KYRC file and write that year twice. The match is case-insensitive. The 2024-shaped unit test uses `english_language_proficiency_2024_format.csv` so it still exercises those headers. Historical `english_language_proficiency_2023.csv` and `english_language_proficiency_2022.csv` stay. `english_learner_proficiency_2021.csv` is a 0-byte 200, and `english_learner_proficiency_2020.csv` is a 121,925,616-byte domain file, not the value table. Neither is a 404. Both stay.

Follow-up, not fixed here. `scripts/generate_report_drafts.py` selects state rows with `district == 'State'`. The EL progress metrics (`english_learner_score_60_80_elementary`, `_middle`, `_high`, and the combined proficiency report) use that lookup. That lookup now finds no EL state row in 2024, 2025, or 2026, the same `All Districts` gap as graduation. KYRC24 already stored `All Districts`, and the 2024-25 temporary file that used `State` is no longer registered. The 2022 and 2023 historical files still have district `State` (33 rows, school code `999`). `analysis/scripts/el_progress_elementary_analysis.py`, `el_progress_middle_analysis.py`, and `el_progress_high_analysis.py` keep year range `(2022, 2025)` and drop `---District Total---`. 2026 school rows will not enter those datasets until the year cap moves. School Type `A1` is already on the school rows (13,618 in KYRC26).

Fixtures are real rows in `tests/fixtures/english_learner_progress/`: thirteen KYRC26 rows and twelve rows each from KYRC25 and KYRC24, including the Bryan Station American Indian pair, the KYRC24 space-blank row, and the Adair partial blank. `tests/test_fixture_sizes.py` passes.

Lessons for later agents:

1. "Leave the temporary file in place" was the checklist before the HEAD. `English_Language_Proficiency_2025.CSV` is 404. Switch that year to the permanent KYRC file and do not register both. The 2026 temporary file is still live. Do not register it next to `KYRC26_`.
2. "Header changes: none" means the KYRC26 header matches KYRC25. It also means the uppercase temporary headers were already mapped, so a leftover `English_Language_Proficiency_2024.CSV`, `_2025.CSV`, or `_2026.CSV` emits a second copy of that year. The guard is case-insensitive. Historical `english_language_proficiency_2022.csv` and `_2023.csv` stay. Rename a unit-test file that used the skipped name.
3. Before adding a filename guard, parse one sibling row. Attainment, the assessment proficiency file, and the overview English-learner counts emit nothing. The temporary proficiency extract does emit rows.
4. Compare the suppression value set, then the counts. Here the codes are still `N`, `Y`, and blank. Blank emits nothing. `Y` emits four empty metrics. `N` emits one metric per filled score cell. A blank score on an `N` row is not a privacy suppression. KYRC24 already does this. About 97% of its empty score cells are one space (the blank-suppression rows). The rest are truly empty and sit on `N` rows. `to_numeric` already drops the space.
5. Prior-year identity is a diff of the files the base parser already emitted. This module's KYRC24 file matches that output (31,196 KPI rows). KYRC25 was not registered. Its 2024-25 rates already came from `English_Language_Proficiency_2025.CSV`. Replacing that dead file is a source swap.
6. Do not add a shared helper for a header or token only this file uses. The level names were already mapped. Do not add a space to the shared missing-value list.
7. State district is `All Districts`. `scripts/generate_report_drafts.py` still selects `State`. That lookup now finds no EL state row in 2024, 2025, or 2026, the same gap as graduation. The 2022 and 2023 historical files still use `State`. The three EL analysis scripts still stop at 2025.

### `postsecondary_readiness` — S — in review

| | |
| --- | --- |
| KYRC26 files | `KYRC26_ACCT_Postsecondary_Readiness.csv` |
| Header changes | None versus KYRC25. `Postsecondary Rate` and `Postsecondary Rate With Bonus` were already mapped, including the temporary-file headers. School-code set is identical to KYRC25 |
| Fixtures / assertions | State base rate 85.3, rate with bonus 93.9, year `2026`. Checked against the blob file, not a rewritten row |
| Gotchas | `Postsecondary_Readiness_2025.CSV` is 404. The permanent KYRC25 file is the 2024-25 source. Skip leftover `Postsecondary_Readiness_2024.CSV`, `Postsecondary_Readiness_2025.CSV`, and `Postsecondary_Readiness_2026.CSV`. The 2026 temporary file is still live. Do not register it. `scripts/generate_report_drafts.py` still looks for district `State`. `analysis/scripts/postsecondary_readiness_analysis.py` still caps the year at 2025 |

Result, checked 17 October 2026 against the public `datasets` container. `include_kyrc26 = True` and the `kde_sources.yaml` source swap are the changes. `etl/kyrc.py`, `BaseETL`, and the demographic config were not edited. Re-HEAD the readiness files after 26 October 2026. If the hashes diverge from the stamps below, take the newer stamp and record which container it came from.

`datasets` HEAD for the KYRC26 file did not return Content-MD5. The UAT Content-MD5 matches the bytes downloaded from `datasets`, and `datasets` Last-Modified is newer, so this is the `datasets` object. The KYRC25 `datasets` Content-MD5 matches the download. KYRC24 `datasets`, the historical-datasets host, and the UAT container are the same bytes.

| File | Bytes | MD5 | Last-Modified |
| --- | --- | --- | --- |
| `KYRC26_ACCT_Postsecondary_Readiness.csv` | 1,366,274 | `b5893752e1eefeabc05ab37caa40b7ab` | Thu, 15 Oct 2026 10:00:09 GMT |
| `KYRC25_ACCT_Postsecondary_Readiness.csv` | 1,365,309 | `1ee5f118a10ecd6f9408c85a1c939ded` | Fri, 09 Oct 2026 19:39:31 GMT |
| `KYRC24_ACCT_Postsecondary_Readiness.csv` | 1,337,876 | `85eb6af528ac3026cb1f7ad9746d93d4` | Thu, 14 May 2026 19:11:28 GMT |

The historical-datasets host serves the same KYRC24 bytes. Its KYRC25 copy is a different file (1,371,527 bytes, `54c8b0bd22b81beebfeefdeec25af9b1`, Last-Modified Tue, 10 Feb 2026 20:37:59 GMT). The two copies differ in 6,218 rate cells, every one a suppressed `*` in the `datasets` file and ` *` (a leading space) in the February copy. Published numeric rates match. Both copies produce the same 16,130 KPI rows. The registered `kyrc25` URL is `datasets`.

Each KYRC file has 10,719 data rows, 397 school codes, and the same 27 demographic labels. The full-file run reported 27 found and no unexpected labels. No new KPI names. The two metrics are `postsecondary_readiness_rate` and `postsecondary_readiness_rate_with_bonus`.

All Students, KPI `year` `2026` (source `School Year` `20252026`). `All Schools` is stored as `---District Total---`.

| Row | School code | Base | With bonus | Suppressed |
| --- | --- | --- | --- | --- |
| State (`All Districts`) | `999000` | 85.3 | 93.9 | N |
| Fayette County district | `165000` | 86.4 | 96.0 | N |
| Bryan Station High School | `165170` | 80.7 | 90.9 | N |
| Adair County district | `001000` | 97.5 | 109.9 | N |
| State `Asian` | `999000` | 92.8 | 101.3 | N |
| Bryan Station `Asian` | `165170` | empty | empty | Y |
| Bryan Station `American Indian or Alaska Native` | `165170` | empty | empty | blank in the file |

Bryan Station `state_school_id` `034165170`, `nces_id` `210186000388`, `school_type` `A1`, county `FAYETTE`. Fayette district `nces_id` `2101860`, `co_op` `CKEC`, `co_op_code` `901`. That `901` is the cooperative code. The school code stays `165000`. KYRC25 stored Bryan Station's NCES id as `210186000000`. KYRC24 and KYRC26 both use `210186000388`. Adair County High School `001010` is `210003000001` in KYRC24 and KYRC26 and `210003000000` in KYRC25. The state `Asian` bonus rate is 101.3. Bonus rates above 100 are kept. The validation ceiling is 150.

The configured 2023-24 file is `KYRC24_ACCT_Postsecondary_Readiness.csv`. A base-branch run wrote 21,438 KPI rows. This branch writes 15,908. The difference is intentional. Journal 75 stored a blank suppression row as `suppressed` `N` with an empty value. That design was wrong. notes/8 treats a blank unsuppressed measure as the absence of a row, and in 2022-23 KDE coded these same cells as `Y` / `*`. Of the 2,765 KYRC24 blank source rows, 2,753 were `Y` / `*` in both 2022 and 2023, 11 were `Y` / `*` in 2023 only, and 1 was `Y` / `*` in 2022 and a published rate in 2023. None were a published number in both earlier years. The 5,530 removed KPI rows are those blank rows, two empty metrics each. Every published rate and every `Y` row is unchanged, ignoring `last_updated`. State All Students stays 81.0 / 86.0. Fayette stays 77.5 / 82.6. Bryan Station stays 68.6 / 77.7. Adair County district stays 86.5 / 94.9.

`config/kde_sources.yaml` did not register the KYRC25 readiness file. 2024-25 production came from `Postsecondary_Readiness_2025.CSV`. That temporary file returned 404 on the OAA URL, on the historical-datasets host, and it is not in the Wayback index used here. It is no longer registered. The permanent KYRC25 file writes 16,130 KPI rows. The old parser wrote 21,438, and the 5,308 removed rows are the 2,654 blank source rows. Published cells are unchanged. State All Students is 83.3 / 89.1. Fayette is 84.2 / 91.3. Bryan Station is 87.9 / 98.5. Journal 75's Adair County district sample is 90.2 / 98.0, Female 93.7 / 102.6, Male 87.3 / 94.1, and the permanent file matches those six cells.

Journal 75 (19 November 2025) is the surviving record of the temporary file: 10,962 source rows became 21,924 KPI rows under the old parser, which emitted both metrics for every source row. The permanent KYRC25 file has 10,719 source rows. Under that same parser it wrote 21,438 KPI rows. After the blank-row skip it writes 16,130. Only the KYRC25 name is registered, so those 16,130 rows replace the temporary file's 21,924. They are not added to them. The 486-row gap between 21,924 and 21,438 is the nine cooperatives, counted under the old parser. It is separate from the blank-row drop.

The 486-row drop is 9 educational cooperatives, codes 901–909, times 27 demographic groups times 2 metrics. 10,962 / 27 = 406 entities. 10,719 / 27 = 397. The difference is 9 entities. The 2025 temporary file itself cannot be re-read. The live `Postsecondary_Readiness_2026.CSV` (842,564 bytes, not registered) has the same 10,962-row shape and the same nine-column header. Its nine extra entities are the cooperative totals: 901 CKEC, 902 GRREC, 903 KEDC, 904 KVEC, 905 NKCES, 906 OVEC, 907 SESC, 908 WKEC, and 909 JEFF CO, each with `type` `SP`. That is 9 × 27 = 243 source rows, and 243 × 2 = 486 KPI rows. After the 3-digit district and state codes are padded to six digits, the other 10,719 keys match KYRC26. Suppression codes match: 4,920 `N`, 3,232 `Y`, and 2,567 blank. Every published rate matches. 1,423 cells differ only by a trailing `.0`, such as `80` versus `80.0` and `110` versus `110.0`.

The temporary file does not add alternative schools. It has 228 six-digit school codes, the same 228 KYRC school codes. It has 169 three-digit district and state codes, which pad to the 169 KYRC codes that end in `000`, including state `999` to `999000`. The only codes KYRC leaves out are 901–909. `type` on the temporary file is `A1` (10,665 rows), `B1` (54), or `SP` (243). The second `B1` entity is district `606`, Model Laboratory Schools at EKU. KYRC stores that district total as `606000` with a blank school type. The school `606460` is `B1` in both files. 228 + 169 + 9 = 406. 228 + 169 = 397.

The November 2025 and August 2026 snapshots were not the same rates. Journal 102 records the November state cells as 83.0 / 88.8 and Fayette as 82.2 / 89.2, and the 3 August 2026 re-download as 83.3 / 89.1 and 84.2 / 91.3. The permanent file matches the August state and Fayette cells. Henry Clay High School `165039` is 72.1 / 73.5. Journal 102 named 72.1 as the August base rate; the November base rate was 67.3 and the November bonus was 68.7. Lafayette is 88.6 / 93.8 against November 84.9 / 90.1. Paul Laurence Dunbar is 83.6 / 90.6 against November 79.8 / 86.9. Tates Creek (87.0 / 97.4), Frederick Douglass (88.7 / 97.4), and Bryan Station (87.9 / 98.5) match the November printout. Adair County matches the November sample above. Those are the cells that can be checked. The rest of the November file is gone. Journal 102's sentence that every FCPS high school shifted upward by 2–7 points is wrong: those three schools are unchanged. The revised rates are already in the historical-datasets KYRC25 file (Last-Modified Tue, 10 Feb 2026 20:37:59 GMT), so the revision happened between 19 November 2025 and 10 February 2026. The 9 October 2026 `datasets` copy matches those published rates.

Suppression in the KYRC file is `N`, `Y`, or blank. There is no new code. `Y` is `*` in KYRC25 and KYRC26 and ` *` in KYRC24. A `Y` row emits both metrics empty. `to_numeric` already drops the spaced star. Do not add a space to the shared missing-value list. A row that is not `Y` and has both rates empty is not reported. `convert_to_kpi_format` skips it. That is the same rule as notes/8 and as chronic absenteeism, graduation, and English learner progress. There is no partial blank. The two rate cells always share a status. Keeping the blank row as `suppressed` `N` with an empty value was journal 75's design, and it was wrong. `scripts/generate_report_drafts.py` keeps rows where `suppressed` is not `Y` and calls `float(value)`, so those empty values became NaN in 79–101 districts a year.

This skip changes KYRC24 on purpose, and it would change 2022 and 2023 if those files had blank rows. They do not. Their withheld cells are `Y` / `*`, so both years stay 18,262 KPI rows. Published rates in every year stay identical. Only the empty blank rows go away.

| Year | Before | After | Removed |
| --- | --- | --- | --- |
| 2022 | 18,262 | 18,262 | 0 |
| 2023 | 18,262 | 18,262 | 0 |
| 2024 | 21,438 | 15,908 | 5,530 |
| 2025 | 21,438 | 16,130 | 5,308 |
| 2026 | 21,438 | 16,304 | 5,134 |

| File | N | Y | Blank | KPI rows |
| --- | --- | --- | --- | --- |
| KYRC24 | 4,760 | 3,194 | 2,765 | 15,908 |
| KYRC25 | 4,956 | 3,109 | 2,654 | 16,130 |
| KYRC26 | 4,920 | 3,232 | 2,567 | 16,304 |

`N` plus `Y`, times two metrics, is the KPI count. Blank emits nothing. 4,760 + 3,194 = 7,954, and 7,954 × 2 = 15,908. 4,956 + 3,109 = 8,065, and 8,065 × 2 = 16,130. 4,920 + 3,232 = 8,152, and 8,152 × 2 = 16,304. A full-file run of the updated parser wrote those three counts, and 18,262 for each of 2022 and 2023. The historical-datasets KYRC25 copy writes the same 16,130 rows as the `datasets` copy.

KYRC26 against KYRC25, on the same 10,719 keys: 4,423 stay `N`, 2,192 stay `Y`, and 2,137 stay blank. 527 move `N` to `Y` and 493 move `Y` to `N`. 513 move blank to `Y` and 424 move `Y` to blank. 6 move `N` to blank and 4 move blank to `N`. The net is −36 `N`, +123 `Y`, and −87 blank (4,956 − 36 = 4,920, 3,109 + 123 = 3,232, 2,654 − 87 = 2,567). The output count moves because blank rows are no longer emitted.

The locked blank example is Bryan Station `165170`, `American Indian or Alaska Native`: KYRC26, KYRC25, and KYRC24 are blank and emit no row. State `Alternate Assessment` is blank in all three years and emits no row. School-level suppression used for the check: Bryan Station `Asian`, `Y` / `*` in KYRC25 and KYRC26, and `Y` / ` *` in KYRC24. That row still emits two empty metrics with `suppressed` `Y`.

`CO-OP` `GREC` is 594 rows and 22 school codes in the KYRC25 file, all Jefferson County codes beginning `275`. KYRC26 labels those same 22 codes `GLEC` and has no `GREC` rows. Nothing in this pipeline groups or joins on the co-op name. A later cross-year grouping on `co_op` would split that cooperative in two.

`Postsecondary_Readiness_2026.CSV` does emit rows, because `Postsecondary Rate` was already mapped. The guard exists so a leftover copy cannot sit beside the KYRC file and write that year twice. The match is case-insensitive. The 2024-shaped unit test uses `postsecondary_readiness_2024_format.csv` so it still exercises those headers. Historical `postsecondary_readiness_2022.csv` and `postsecondary_readiness_2023.csv` stay. `Postsecondary_Readiness_2024.CSV` is also 404. The temporary file stores the state district as `State` and the state school code as `999`.

Follow-up, not fixed here. Skipping the blank row stops the unsuppressed empty value that `scripts/generate_report_drafts.py` was passing to `float(value)`. That script still selects state rows with `district == 'State'`. The 2022 and 2023 historical files still have district `State`, school name `---State Total---`, and school code `999` (9,131 source rows each). KYRC24 already stored `All Districts`. The 2024-25 temporary file that used `State` is no longer registered, so the lookup finds no postsecondary state row in 2024, 2025, or 2026. `analysis/scripts/postsecondary_readiness_analysis.py` keeps year range `(2022, 2025)` and `school_type == 'A1'`. The temporary file's `type` column is not mapped to `school_type`, so its 2025 rows never entered that dataset. The KYRC25 file sets School Type. 227 A1 schools have a published All Students base rate, and those 2025 rows now pass the filter. 2026 school rows will not enter until the year cap moves. School Type `A1` is already on the school rows (6,129 source rows, 227 schools).

Fixtures are real rows in `tests/fixtures/postsecondary_readiness/`: eleven rows each from KYRC26, KYRC25, and KYRC24, including the Bryan Station American Indian blank, the Bryan Station Asian suppression, and the KYRC24 spaced star. `tests/test_fixture_sizes.py` passes.

Lessons for later agents:

1. "Leave the temporary file in place" was the checklist before the HEAD. `Postsecondary_Readiness_2025.CSV` is 404. Switch that year to the permanent KYRC file and do not register both. The 2026 temporary file is still live (842,564 bytes). Do not register it next to `KYRC26_`.
2. "Header changes: none" means the KYRC26 header matches KYRC25. It also means the temporary rate headers were already mapped, so a leftover `Postsecondary_Readiness_2024.CSV`, `_2025.CSV`, or `_2026.CSV` emits a second copy of that year. The guard is case-insensitive. Historical `postsecondary_readiness_2022.csv` and `_2023.csv` stay. Rename a unit-test file that used the skipped name.
3. Skip a source row when it is not `Y` and both rates are empty. That row was not reported. notes/8 is the rule, and chronic absenteeism, graduation, and English learner progress already skip it. A `Y` row still emits two empty metrics with `suppressed` `Y`. Do not emit the blank as `suppressed` `N`. Journal 75 did that, and `scripts/generate_report_drafts.py` then kept `suppressed != 'Y'` and called `float(value)`. There is no partial blank. KYRC24 stores the star as ` *`. `to_numeric` already drops it. Do not add a space to the shared missing-value list.
4. Compare the suppression value set, then the counts. Here the codes are still `N`, `Y`, and blank. `N` plus `Y`, times two metrics, is the KPI count. Blank emits nothing. The output count moves when cells change between blank and `N` or `Y`.
5. The temporary-to-permanent gap for this file is the nine cooperative totals, not alternative schools. 406 entities versus 397 is 9 × 27 source rows, and × 2 metrics is 486 KPI rows. The 2025 temporary file is 404. Journal 75's 10,962 rows are the same 406 × 27 shape, and the live 2026 temporary file is where the nine codes can be counted.
6. Prior-year identity for published rates is a diff against the rows the base parser already emitted, after the empty `suppressed` `N` rows are dropped. Those empty rows are an intentional change. Journal 75's design was wrong, and in 2022-23 KDE coded the same cells as `Y` / `*`. KYRC24 goes from 21,438 to 15,908. 2022 and 2023 have no blank rows and stay 18,262. KYRC25 was not registered. Replacing the dead temporary file is a source swap. Its current output is 16,130.
7. State district is `All Districts`. `scripts/generate_report_drafts.py` still selects `State`. That lookup finds the 2022 and 2023 rows and finds no KYRC state row in 2024, 2025, or 2026. The readiness analysis script still stops at 2025 and requires `school_type == 'A1'`. The temporary `type` column was not mapped, so the KYRC25 School Type column is what lets 2025 A1 schools into that dataset.
8. Do not add a shared helper for a header only this file uses. The rate headers were already mapped. `GREC` became `GLEC` on 22 Jefferson County school codes (594 rows). Nothing in this pipeline joins on the co-op name.

### `postsecondary_enrollment` — S — in review in PR #43

| | |
| --- | --- |
| KYRC26 files | `KYRC26_ADLF_Transition_to_In_State_Postsecondary_Education.csv` |
| Header changes | None versus KYRC25. `Percentage College Enrolled In State Table` was already mapped. The 2021-2023 files use `PERCENTAGE COLLEGE ENROLLED IN STATE`, also already mapped |
| Fixtures / assertions | State cohort 48,798, public 18,769, private 3,563, total enrolled 22,332, rates 38.5 / 7.3 / 45.8, year `2026`. Checked against the blob file, not a rewritten row |
| Gotchas | District `602` is new versus KYRC25. That is a code-domain change, not a header change. No Suppressed column in any year. `scripts/generate_report_drafts.py` still looks for district `State`. The enrollment analysis script still caps the year at 2025 |

Result, checked 17 October 2026 against the public `datasets` container. `include_kyrc26 = True` is the parser change. `etl/kyrc.py`, `BaseETL`, `config/kde_sources.yaml`, and the demographic config were not edited. Re-HEAD the transition file after 26 October 2026. If the hashes diverge from the stamps below, take the newer stamp and record which container it came from.

`datasets` HEAD for the KYRC26 file did not return Content-MD5. The UAT Content-MD5 matches the bytes downloaded from `datasets`, and `datasets` Last-Modified is newer, so this is the `datasets` object. The KYRC25 file is the same bytes on `datasets`, on the UAT container, and on the historical-datasets host.

| File | Bytes | MD5 | Last-Modified |
| --- | --- | --- | --- |
| `KYRC26_ADLF_Transition_to_In_State_Postsecondary_Education.csv` | 887,543 | `ba19e9d0c848fbc06715a005727d3318` | Thu, 15 Oct 2026 10:00:09 GMT |
| `KYRC25_ADLF_Transition_to_In_State_Postsecondary_Education.csv` | 1,311,339 | `a0646ff1b21da0d6bb3b8a9f1ec97c0e` | Thu, 14 May 2026 19:11:31 GMT |
| `KYRC24_ADLF_Transition_to_In_State_Postsecondary_Education.csv` | 847,759 | `55af3896a4342a0521fcca43fc9a1d3b` | Thu, 29 May 2025 23:24:56 GMT |
| `transition_in_state_postsecondary_education_2023.csv` | 863,283 | `41ae848e2f82a9922bf4adf1b3582ae9` | Wed, 07 Feb 2024 21:27:32 GMT |
| `transition_in_state_postsecondary_education_2022.csv` | 834,976 | `6f99eda044316536d5eb12d8abc4603a` | Thu, 08 Feb 2024 20:14:55 GMT |
| `transition_in_state_postsecondary_education_2021.csv` | 831,315 | `61b505fcff3a7d9f8dfe16ebf297345e` | Fri, 09 Feb 2024 19:35:35 GMT |

The historical KYRC25 copy is Last-Modified Thu, 19 Feb 2026 20:18:27 GMT and matches the `datasets` MD5. The UAT KYRC25 Content-MD5 is `oGRv8bIdoNa7O4qfHsl8Dg==` and the UAT KYRC26 Content-MD5 is `uhnp0MhI+8BnFaAFcn0zGA==`. Both match the `datasets` downloads.

The registered KYRC24 file is the historical copy above (Last-Modified Thu, 29 May 2025). KDE re-posted the same name on `datasets` (Last-Modified Thu, 14 May 2026, 849,631 bytes). The seven measure columns match on all 6,320 keys, so the KPI `value` column is unaffected. The re-post fixes co-op columns: 480 blank `CO-OP` cells become `GREC`, and 5 rows whose co-op was `-84.21926253` and whose co-op code was `CKEC` become co-op code `901`. Those columns are copied onto the KPI row, so pointing the registration at the May 2026 copy would change `co_op` on 485 rows and `co_op_code` on those 5. The registered source stays the May 2025 historical file.

Every registered prior-year URL returned 200 with the browser headers in `config/kde_sources.yaml`. A bare HEAD of the education.ky.gov host returns 403. These names are 404, and none of them were registered: `transition_in_state_postsecondary_education_2024.csv`, `_2025.csv`, and `_2026.csv` on the historical host; `Transition_to_In_State_Postsecondary_Education_2024.CSV`, `_2025.CSV`, and `_2026.CSV` on the OAA temporary host and on `datasets`; `Postsecondary_Enrollment_2025.CSV` and `_2026.CSV` on the OAA host. There is no leftover temporary file to skip. The 2021-2023 historical names stay.

KYRC26 has 6,512 data rows. KYRC25 has 9,522. The MD5s are of those downloaded bytes.

All Students, KPI `year` `2026` (source `School Year` `20252026`). `All Schools` is stored as `---District Total---`. Rates in this file have no percent sign. KYRC24 and KYRC25 store rates as `39.10%`. `clean_percentage_values` already strips the sign.

| Row | School code | Cohort | Public | Private | Total | Rates | Suppressed cells |
| --- | --- | --- | --- | --- | --- | --- | --- |
| State (`All Districts`) | `999000` | 48,798 | 18,769 | 3,563 | 22,332 | 38.5 / 7.3 / 45.8 | none |
| Fayette County district | `165000` | 3,340 | 1,407 | 187 | 1,594 | 42.1 / 5.6 / 47.7 | none |
| Bryan Station High School | `165170` | 577 | 225 | 26 | 251 | 39.0 / 4.5 / 43.5 | none |
| Adair County district | `001000` | 215 | 36 | 47 | 83 | 16.7 / 21.9 / 38.6 | none |
| Adair County High School | `001010` | 201 |  |  |  |  | six enrollment cells `*` |

Bryan Station `state_school_id` `034165170`, `nces_id` `210186000388`, `school_type` `A1`, county `FAYETTE`. Fayette district `nces_id` `2101860`, `co_op` `CKEC`, `co_op_code` `901`. That `901` is the cooperative code. The school code stays `165000`. KYRC25 stored Bryan Station's NCES id as `210186000000`. KYRC24 and KYRC26 both use `210186000388`. Adair County High School `001010` is `210003000001` in KYRC24 and KYRC26 and `210003000000` in KYRC25.

The configured prior-year files were already the permanent KYRC files. A run of this branch before `include_kyrc26 = True` wrote the baseline. After the opt-in, years 2021-2025 match that file exactly, ignoring `last_updated`.

| Year | KPI rows |
| --- | --- |
| 2021 | 18,737 |
| 2022 | 7,781 |
| 2023 | 9,509 |
| 2024 | 9,625 |
| 2025 | 9,679 |
| 2026 | 10,657 |

State All Students stays 46.3 in 2024 and 45.5 in 2025. Fayette stays 50.1 and 47.9. Bryan Station stays 44.6 and 40.6. Adair County district stays 39.2 and 38.5. Those are the total in-state rates. The counts behind them are unchanged too.

KYRC25 is a complete grid: 529 school codes × 18 demographics = 9,522. KYRC26 has 535 school codes and is sparse, the same shape as KYRC24. Code change versus KYRC25: 20 added, 14 removed, net +6. 529 − 14 + 20 = 535. None of the removed codes are cooperative totals 901-909.

| Piece | Rows |
| --- | --- |
| KYRC25 | 9,522 |
| 14 removed schools × 18 | −252 |
| All-star demographics dropped on the 515 shared codes | −2,920 |
| 20 new codes | +162 |
| KYRC26 | 6,512 |

9,522 − 252 − 2,920 + 162 = 6,512. The 2,920 dropped rows are 820 district-total rows and 2,100 school rows. All 2,920 were `*` in every measure cell in KYRC25, so they already emitted no KPI row. The 162 new rows are 8 on district `602000` and 154 on 19 new schools.

All Schools falls from 3,060 to 2,248. No district code was removed. District `602000` adds 8 rows. The 170 shared district codes drop 820 all-star demographics. 3,060 − 820 + 8 = 2,248. School rows fall from 6,462 to 4,264: 6,462 − 252 − 2,100 + 154 = 4,264. 2,248 + 4,264 = 6,512.

The 14 removed schools are a mix, not a block of alternative schools and not co-op totals. Three are `A1` (Collins Lane `181025`, Conway Middle `275164`, Frederick Law Olmsted Academy North `275620`). The other eleven are `A5`, `A6`, or `A8`. Those 252 source rows held 11 published cells. The 162 new-code rows hold 45 published cells. On the 6,350 shared keys, 1,928 cells move `*` to a number and 984 move a number to `*`. 1,928 − 984 = 944. KPI rows: 9,679 − 11 + 45 + 944 = 10,657.

| File | All `*` | Mixed | All numeric | Blank | KPI rows |
| --- | --- | --- | --- | --- | --- |
| KYRC24 | 3,213 | 2,517 | 590 | 0 | 9,625 |
| KYRC25 | 6,358 | 2,602 | 562 | 0 | 9,679 |
| KYRC26 | 2,827 | 3,035 | 650 | 0 | 10,657 |

3,213 + 2,517 + 590 = 6,320. 6,358 + 2,602 + 562 = 9,522. 2,827 + 3,035 + 650 = 6,512. A blank cell does not appear in any of these files, or in the 2021-2023 files. The only non-numeric tokens are `*` (KYRC24, KYRC25, KYRC26), plus `**` in 2022 and 2023 and `<10` in 2021. All three are already in `MISSING_VALUE_INDICATORS`. There is no Suppressed column. Under the current parser a `*` metric is absent and a fully starred group emits nothing, so the output has no `suppressed` `Y` row. Known deviation from notes/8: this module drops `*` cells and fully starred groups instead of emitting `suppressed=Y`. The behavior predates KYRC26 and is out of scope here; follow-up issue #44 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/44). It needs a design call because KYRC25 pads non-existent cohorts with stars, while KYRC24 and KYRC26 omit fully starred groups. Zero blank source rows means the prior-year counts do not move under the current parser.

The locked full-suppression example is Bryan Station `165170`, `Asian`: `*` in KYRC24, KYRC25, and KYRC26, and no KPI row in any year. Fayette `165000`, `American Indian or Alaska Native`, is the same in KYRC25 and KYRC26. Adair County High School `001010`, All Students, is fully starred in KYRC24 and KYRC25 (no KPI row) and in KYRC26 publishes only the cohort, 201. The six enrollment cells stay `*`.

The new KYRC26 pattern is a private-college star beside published public and total cells. Seven rows. The locked one is Bowling Green High School `042070`, All Students: cohort 301, public 151, total enrolled 167, public rate 50.2, total rate 55.5. Private count and private rate are `*` and are not emitted. The other five metrics are emitted with `suppressed` `N`.

District `602` is Kentucky School for the Blind. It is absent from KYRC25. KYRC24 already had `602000` (6 all-star rows) and `602176` (5 all-star rows, school type `D1`). KYRC26 has both codes again, 8 demographics each, all `*`, school type `D1` on `602176`. Those 16 rows emit no KPI row. The new code does not change the KPI counts. County is `JEFFERSON`. District NCES id `2100094`. School NCES id `210009401779`.

`CO-OP` `GREC` is 774 rows and 43 school codes in KYRC25, all Jefferson County codes beginning `275`. KYRC26 labels 40 codes `GLEC` (486 rows) and has no `GREC` rows. 39 codes overlap. The four GREC codes that do not return are removed schools `275164`, `275463`, `275620`, and `275768`. The new GLEC code is `275191`, W.E.B. DuBois Academy. Nothing in this pipeline groups or joins on the co-op name.

`KYRC26_ADLF_Current_Year_Graduates.csv` and `KYRC26_ADLF_Graduate_Outcomes.csv` are not registered under this directory. Graduates has `Graduate Count`. Outcomes has `College`, `Military`, `Working`, and percent columns, and no `Demographic`. Neither header is one of the seven mapped measures, so a parse emits no KPI rows. No filename guard.

Follow-up, not fixed here. Issue #42 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/42) and its [17 October 2026 comment](https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/42#issuecomment-6242106920): `scripts/generate_report_drafts.py` selects state rows with `district == 'State'`, and `get_state_average` also requires `school_name == '---District Total---'`. The 2021-2023 files store district `State` and school name `---State Total---` (school code `999`). KYRC24, KYRC25, and KYRC26 store district `All Districts` and school name `All Schools`, which this parser stores as `---District Total---` (school code `999000`). No year matches both conditions, so this module's state average is missing in every year. Matching `school_code == '999000'` alone does not cover 2021-2023. This module emits no empty value, so the NaN guard in issue #42 is not reached by these rows. `analysis/scripts/postsecondary_enrollment_analysis.py` keeps year range `(2021, 2025)` and `school_type == 'A1'`. 2026 school rows will not enter until the year cap moves. School type `A1` is already on the school rows (2,999 source rows, 229 schools). 169 of those schools publish an All Students total rate.

Both years' demographic columns are the same 18 labels, all already in the 2026 demographic config. The full-file run reported 18 found and no unexpected labels. No new KPI names.

Fixtures are real rows in `tests/fixtures/postsecondary_enrollment/`: ten KYRC26 rows, seven KYRC25 rows, and six KYRC24 rows, including the Bryan Station Asian suppression, the Fayette American Indian suppression, the Adair County High School partial row, the Bowling Green private-college star, and both district `602` rows. `tests/test_fixture_sizes.py` passes.

Lessons for later agents:

1. HEAD the registered URLs before assuming a temporary file went 404. This module's KYRC25 entry was already the permanent file, and it returns 200 on `datasets`, on UAT, and on the historical host, with one MD5. The guessed OAA names are 404 and were never registered. Do not add a skip for a filename this directory has never used. The 2021-2023 names stay.
2. "Header changes: none" means the KYRC26 header matches KYRC25. The total-rate header is `Percentage College Enrolled In State Table` on the KYRC files and `PERCENTAGE COLLEGE ENROLLED IN STATE` on the 2021-2023 files. Both were already mapped. A rate without a percent sign still goes through `clean_percentage_values`.
3. This file has no Suppressed column. `*` is a cell, not a row flag. Known deviation from notes/8: this module drops `*` cells and fully starred groups instead of emitting `suppressed=Y`. The behavior predates KYRC26 and is out of scope here; follow-up issue #44 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/44). It needs a design call because KYRC25 pads non-existent cohorts with stars, while KYRC24 and KYRC26 omit fully starred groups. A blank cell, which these files do not contain, is skipped by the base converter today.
4. Compare the suppression token set, then the counts. The KYRC token is still `*`. KYRC26 is smaller because it omits all-star demographic rows KYRC25 included, and it writes more KPI rows because more of the remaining cells are numbers. Under the current parser, `*` cells are not KPI rows. The source-row drop and the KPI-row increase are not a contradiction. See issue #44 before treating that as the intended design.
5. The temporary-to-permanent gap does not apply. There is no temporary file. The 14 schools KYRC26 drops are not co-op totals. Three are `A1` and eleven are `A5`, `A6`, or `A8`. 14 × 18 = 252 source rows, and those rows held 11 published cells.
6. Prior-year identity is a diff against output the base parser already wrote, before `include_kyrc26` was set. 2021-2025 match that file. Published state, Fayette, Bryan Station, and Adair County rates are unchanged.
7. State district is `All Districts` for 2024-2026. Issue #42 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/42#issuecomment-6242106920): `scripts/generate_report_drafts.py` selects `State`, and `get_state_average` also requires school name `---District Total---`. The 2021-2023 state rows are district `State` and school name `---State Total---` (school code `999`), so the state average for this metric is missing in every year. The enrollment analysis script still stops at 2025 and requires `school_type == 'A1'`.
8. District `602` is a code-domain change versus KYRC25, not a header change. KYRC24 already had the Kentucky School for the Blind codes, all starred. KYRC26's 16 rows are starred too and emit nothing. Do not add a shared helper for that code.
9. Before adding a filename guard, parse one sibling row. Current Year Graduates and Graduate Outcomes emit nothing. `GREC` became `GLEC` on the Jefferson codes that remain. Nothing in this pipeline joins on the co-op name.

### `cte_participation` — S — in review

| | |
| --- | --- |
| KYRC26 files | `KYRC26_CTE_Participation.csv` |
| Header changes | None |
| Fixtures / assertions | State participants 144,661 / enrollments 201,255. Counts already go through `clean_numeric_with_commas` |
| Gotchas | None beyond the usual opt-in. No Suppressed column. A `*` cell is dropped. See the result below |

Result, checked 17 October 2026 against the public `datasets` container. `include_kyrc26 = True` is the parser change. `etl/kyrc.py`, `BaseETL`, `config/kde_sources.yaml`, and the demographic config were not edited. Re-HEAD the participation file after 26 October 2026. If the hashes diverge from the stamps below, take the newer stamp and record which container it came from.

`datasets` HEAD for the KYRC26 file did not return Content-MD5. The UAT Content-MD5 matches the bytes downloaded from `datasets`, and `datasets` Last-Modified is newer, so this is the `datasets` object. The KYRC25 pair matches the same way.

| File | Bytes | MD5 | Last-Modified |
| --- | --- | --- | --- |
| `KYRC26_CTE_Participation.csv` | 1,091,622 | `82a36bff5ecbce9321898898ae2c6d22` | Thu, 15 Oct 2026 10:00:09 GMT |
| `KYRC25_CTE_Participation.csv` | 859,494 | `f357a9620827d8a5cfb711ed97c1f38a` | Thu, 14 May 2026 19:11:31 GMT |
| `KYRC24_CTE_Participation.csv` | 845,823 | `0b0d30c79ff6c5f3c039aafd5ceb8081` | Thu, 29 May 2025 23:02:15 GMT |
| `cte_by_student_group_2023.csv` | 1,193,029 | `1aa1a9adf4e2d055a229c1db12f84500` | Wed, 07 Feb 2024 15:08:37 GMT |
| `cte_by_student_group_2022.csv` | 1,211,533 | `171707b976e55359a523b600b95270b2` | Thu, 08 Feb 2024 17:36:10 GMT |

The MD5s are of those downloaded bytes. UAT Content-MD5 is `gqNr/17LzpMhiYiYrixtIg==` for KYRC26 and `81epYggn2KXPtxHtl8Hzig==` for KYRC25, which are those hashes. UAT Last-Modified is earlier (KYRC26 Sat, 10 Oct 2026 06:00:11 GMT; KYRC25 Tue, 06 Oct 2026 20:27:25 GMT).

Every registered prior-year URL returned 200 with the browser headers in `config/kde_sources.yaml`. These names are 404, and none of them were registered: `CTE_Participation_2024.CSV`, `_2025.CSV`, and `_2026.CSV` on the OAA temporary host, on the historical host, and on `datasets`; `cte_by_student_group_2024.csv`, `_2025.csv`, and `_2026.csv` on those three hosts; `CTE_by_Student_Group_2025.CSV` and `CTE_Participation_2025.csv` on those three hosts. There is no leftover temporary file to skip. The 2022 and 2023 historical names stay.

The historical host also has `KYRC25_CTE_Participation.csv` (859,710 bytes, MD5 `f222d497d0b4cefa7664d7d66e72f16b`, Last-Modified Thu, 19 Feb 2026 19:57:50 GMT). It is not the registered source. The registered `datasets` file is 216 bytes smaller. Both have 6,526 rows and the same headers. The only cell difference is `District Name` on 24 rows, school codes `603000` and `603390`: `datasets` says `Kentucky School for the Deaf` and the historical copy says `Kentucky School for the Deaf District`. Measure cells match. This pipeline keeps the registered `datasets` file.

KYRC26 has 8,245 data rows. KPI `year` `2026` comes from `School Year` `20252026`. `All Schools` is stored as `---District Total---`. The participation rate is `round(participants / enrollments * 100, 1)`. The grade-12 rate is `round(completers / participants * 100, 1)`, and both counts are stored.

| Row | School code | Participants | Enrollments | Rate | 12th completers | 12th participants | 12th rate | Suppressed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| State (`All Districts`) | `999000` | 144,661 | 201,255 | 71.9 | 19,990 | 44,038 | 45.4 | N |
| Fayette County district | `165000` | 9,972 | 13,244 | 75.3 | 1,404 | 2,946 | 47.7 | N |
| Bryan Station High School | `165170` | 1,358 | 2,241 | 60.6 | 144 | 482 | 29.9 | N |
| Adair County High School | `001010` | 454 | 818 | 55.5 | 37 | 183 | 20.2 | N |

Bryan Station `state_school_id` `034165170`, `nces_id` `210186000388`, `school_type` `A1`, county `FAYETTE`. Fayette district `nces_id` `2101860`, `co_op` `CKEC`, `co_op_code` `901`. That `901` is the cooperative code. The school code stays `165170`. KYRC25 stored Bryan Station's NCES id as `210186000000`. KYRC24 and KYRC26 both use `210186000388`. Adair County High School `001010` is `210003000001` in KYRC24 and KYRC26 and `210003000000` in KYRC25.

The configured prior-year files were already the permanent files. A run of this branch before `include_kyrc26 = True` wrote the baseline. After the opt-in, years 2022-2025 match that file exactly, ignoring `last_updated`. 43,052 KPI rows, no value, suppression, district, or school-name difference.

| Year | KPI rows |
| --- | --- |
| 2022 | 9,772 |
| 2023 | 9,270 |
| 2024 | 9,935 |
| 2025 | 14,075 |
| 2026 | 14,424 |

State All Students stays 68.7 in 2024 and 70.3 in 2025. Fayette stays 64.3 and 70.7. Bryan Station stays 61.3 and 65.6. Adair County High School stays 63.9 and 59.2. The 2024 grade-12 rates stay 42.3, 38.5, 29.5, and 26.2. The 2025 grade-12 rates stay 44.6, 43.6, 30.2, and 23.3.

KYRC26 is a complete grid: 485 school codes × 17 demographics = 8,245. KYRC25 has 476 school codes and is sparse, 6,526 rows. Code change versus KYRC25: 11 added, 2 removed, net +9. 476 − 2 + 11 = 485. None of the removed or added codes are cooperative totals 901-909. School numbers 901, 902, 905, and 908 are career-center numbers inside real districts. The school code stays six digits (`005901` for Allen County Career and Technical Center).

| Piece | Rows |
| --- | --- |
| KYRC25 | 6,526 |
| 2 removed schools | −32 |
| New demographics on the 474 shared codes | +1,564 |
| 11 new codes × 17 | +187 |
| KYRC26 | 8,245 |

6,526 − 32 + 1,564 + 187 = 8,245. The removed schools are Fayette County technical centers, both `A2`: Southside `165904` (17 rows) and Eastside `165905` (15 rows). 17 + 15 = 32. They are not co-op totals.

All Schools rises from 2,308 to 2,856. No district code was removed. Districts `354` (Ludlow Independent) and `606` (Model Laboratory Schools at EKU) add 34 rows. The 166 shared district codes add 514 demographics. 2,308 + 34 + 514 = 2,856. School rows rise from 4,218 to 5,389: 4,218 − 32 + 153 + 1,050 = 5,389. The 153 rows are 9 new schools × 17. The 1,050 rows are new demographics on schools that already existed. 2,856 + 5,389 = 8,245.

The 11 new codes are not a block of alternative schools. Two are district totals (`354000`, `606000`). The nine schools are `A1` (Lewis County High `335120`, Ludlow High `354020`, Union County High `565065`), `A2` (Locust Trace `165068`, The Hub `165137`), `A5` (Pathfinder `275951`, Commander Academy `575021`), `A8` (HCS Virtual Academy `231035`), and `B1` (Model High `606460`). `B1` is new versus KYRC25.

KPI rows are not one per source row. A participation rate is emitted when both count cells are numbers and enrollment is above 0. A grade-12 group emits three KPI rows (rate, completer count, participant count) when both 12th-grade cells are numbers and the participant count is above 0. KYRC25: 4,535 rates + 3 × 3,180 = 14,075. KYRC26: 4,536 rates + 3 × 3,296 = 14,424.

| Piece | Participation rates | Grade-12 groups | KPI rows |
| --- | --- | --- | --- |
| KYRC25 | 4,535 | 3,180 | 14,075 |
| Shared keys, cell changes | −44 | +111 | +289 |
| 1,564 new demographics on shared codes | +22 | +1 | +25 |
| 11 new codes | +23 | +29 | +110 |
| 32 removed rows | 0 | −25 | −75 |
| KYRC26 | 4,536 | 3,296 | 14,424 |

−44 + 3 × 111 = 289. 22 + 3 × 1 = 25. 23 + 3 × 29 = 110. 14,075 + 289 + 25 + 110 − 75 = 14,424. The 1,564 new demographic keys are 1,230 all-star, 247 star-and-blank, 84 mixed number and star, 2 mixed with a blank, and 1 number-and-blank. 1,230 + 247 + 84 + 2 + 1 = 1,564. The all-star and star-and-blank keys emit nothing under the current parser, which is why 1,564 source rows become 25 KPI rows.

| File | All `*` | Mixed number/`*` | All numeric | Number and blank | Number, `*`, and blank | `*` and blank | KPI rows |
| --- | --- | --- | --- | --- | --- | --- | --- |
| KYRC25 | 509 | 2,400 | 2,560 | 611 | 277 | 169 | 14,075 |
| KYRC26 | 1,829 | 2,428 | 2,679 | 603 | 282 | 424 | 14,424 |

509 + 2,400 + 2,560 + 611 + 277 + 169 = 6,526. 1,829 + 2,428 + 2,679 + 603 + 282 + 424 = 8,245. The cells counted as blank in that table are the text `N/A`. pandas reads `N/A` as missing. No measure cell in KYRC25 or KYRC26 is an empty string. A constructed all-blank row emits nothing, which is what notes/8 requires, and that rule does not move the prior-year counts.

Career-center enrollment is the text `N/A`: 1,057 cells in KYRC25 and 1,309 in KYRC26. Of the KYRC26 cells, 850 are school type `C2`, 442 are `A2`, and 17 have a blank school type. `N/A` means not applicable, because those students are counted at their home school. pandas reads it as missing, so the parser treats it as an empty denominator and emits no participation rate. That treatment stays correct. The locked example is Allen County Career and Technical Center `005901`, All Students: participants 739, enrollment `N/A`, 12th-grade completers 122, 12th-grade participants 241. The grade-12 rate is 50.6, with counts 122 and 241, `suppressed` `N`. The KYRC25 twin is participants 725, enrollment `N/A`, completers 107, participants 233, grade-12 rate 45.9. NCES id `210007000000` in KYRC25 and `210007002061` in KYRC26. The same school's `American Indian or Alaska Native` row is `*`, `N/A`, `*`, `*`, and the current parser emits nothing.

The locked full-suppression example is Bryan Station `165170`, `Native Hawaiian or Pacific Islander`: `*` in every measure cell in KYRC24 and KYRC26, and the row is absent from KYRC25. No KPI row in any year. Fayette `165000`, `American Indian or Alaska Native`, is the partial row: KYRC26 publishes 15 and 18 (rate 83.3) and stars both 12th-grade cells. KYRC25 publishes 13 and 21 (rate 61.9), stars the completer cell, and publishes 4 grade-12 participants, which is not enough for a grade-12 metric because the completer cell is `*`. Bryan Station Foster Care in KYRC26 publishes 21 and 49 (rate 42.9), stars completers, and publishes 6 grade-12 participants. The published rate is emitted with `suppressed` `N`. The starred grade-12 metrics are absent.

There is no Suppressed column. The only non-numeric tokens in KYRC25 and KYRC26 are `*` and `N/A`. KYRC24 also uses `--` (554 participation cells) and `N/A` (317 participation cells). Both are already in `MISSING_VALUE_INDICATORS`. Under the current parser a `*` metric is absent and a fully starred group emits nothing, so the output has no `suppressed` `Y` row. Known deviation from notes/8. The behavior predates KYRC26. It needs a design call, tracked in issue #46 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/46), because KYRC26 adds 1,230 all-star demographic rows that KYRC25 omitted. Mapping every `*` to `Y` would mark those groups as privacy suppression. Career-center rows also mix an `N/A` enrollment with starred counts. Issue #44 is the same kind of call for postsecondary enrollment. It does not cover this file. The tests that assert an absent starred metric point at #46.

The two columns the parser maps to rates (participants, completers) hold counts, as do all four measure columns: `TOTAL NUMBER OF STUDENT`, `CTE PARTICIPANTS IN ALL GRADES`, `GRADE 12 CTE ELIGIBLE COMPLETER`, and `GRADE 12 CTE COMPLETERS`. `TOTAL NUMBER OF STUDENT` and `GRADE 12 CTE ELIGIBLE COMPLETER` are already mapped as counts. None of the headers is named like a rate. None of those cells has a decimal. KYRC24's participation column has 3,416 values with a fractional decimal. Where both grade-12 cells are numeric, completers are at or below eligible in every row: 2,767 rows in 2022 and 2,671 in 2023. Bryan Station `165170` All Students in 2022 stores completers 77 and eligible 413. That 77 means 77 of 413 eligible, which is 18.6%. The base parser reads the participants and completers columns as rates. It publishes a count of 100 or below as the rate, and `clean_cte_data` nulls a value above 100. In 2022 that drops 2,415 participation values and 149 completion values. In 2023 it drops 2,162 and 115. The 2022 state row is participants 167,510, total `*`, eligible completers 43,229, completers 13,624. Only the eligible count is emitted. The 2023 state row drops participants 123,372 and completers 11,471 and keeps eligible completers 25,097. Bryan Station 2022 publishes 77 in the completion-rate metric and eligible 413, and drops participants 1,528. This is a pre-existing deviation that loses data. It is tracked in issue #47 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/47). It is not changed in this pull request. KYRC24 is the year whose participation column is a rate, and 42 of those rates are genuinely above 100. The same cleaner nulls them. That is pre-existing and is part of #47. KYRC25 and KYRC26 keep counts in the count columns and compute the rate.

`CO-OP` `GREC` is 353 rows and 25 school codes in KYRC25, all Jefferson County codes beginning `275`. KYRC26 labels all 25 `GLEC` and adds `275951`, Pathfinder School of Innovation. 26 codes, 442 rows. 25 × 17 + 17 = 442. Nothing in this pipeline groups or joins on the co-op name.

`KYRC26_CTE_Career_Pathways.csv`, `KYRC26_CTE_Career_Readiness_Indicators.csv`, `KYRC26_CTE_Perkins_Report.csv`, `KYRC26_CTE_Postsecondary_Opportunities.csv`, and `KYRC26_CTE_Student_Objectives.csv` are not registered under this directory. None of them has a `Demographic` column, and none of their measure headers is one of the mapped participation columns. A parse emits no KPI rows. No filename guard.

Follow-up, not fixed here. Issue #42 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/42): `scripts/generate_report_drafts.py` selects state rows with `district == 'State'`, and `get_state_average` also requires `school_name == '---District Total---'`. CTE is not in that script's indicator list. If it were added, no year would match both conditions. The 2022 and 2023 state rows are district `State` and school name `---State Total---` (school code `999`). KYRC24, KYRC25, and KYRC26 store district `All Districts` and school name `All Schools`, which this parser stores as `---District Total---` (school code `999000`). This module emits no empty value, so the NaN guard in issue #42 is not reached by these rows. `analysis/config/covariate_catalog.yaml` lists `cte_participation_rate` as a covariate. No analysis script selects these rows by district `State`.

Both years' demographic columns are the same 17 labels, all already in the 2026 demographic config. The label check reported 17 valid and no unexpected labels for 2022 through 2026. No new KPI names. `KPIS.md` still lists the three KYRC24 metrics and not `cte_completer_count_grade_12` or `cte_participant_count_grade_12`. Those two names were already emitted for KYRC25 before this change.

The 2023 file has 17 rows whose `SCHOOL YEAR` is `20022023` instead of `20222023`. `extract_year` keeps the last four digits, so they still file under 2023. That is a source typo, left as it is.

Fixtures are real rows in `tests/fixtures/cte_participation/`: nine KYRC26 rows, seven KYRC25 rows, six KYRC24 rows, and the state and Bryan Station rows from 2022 and 2023. They include the Bryan Station Native Hawaiian suppression, the Fayette American Indian partial row, the Bryan Station Foster Care partial row, and both Allen County career-center rows. `tests/test_fixture_sizes.py` passes.

Lessons for later agents:

1. HEAD the registered URLs before assuming a temporary file went 404. This module's KYRC25 entry was already the permanent file, and it returns 200 on `datasets` and on UAT, with one MD5. The guessed OAA names are 404 and were never registered. Do not add a skip for a filename this directory has never used. The 2022 and 2023 names stay. The historical-host KYRC25 copy is a different file: 24 district-name cells, and the measure cells match. It is not registered.
2. "Header changes: none" means the KYRC26 header matches KYRC25. The count headers were already mapped. A comma in `144,661` still goes through `clean_numeric_with_commas`. The state rate is 71.9, from 144,661 / 201,255.
3. This file has no Suppressed column. `*` is a cell, not a row flag. Known deviation from notes/8: this module drops `*` cells and fully starred groups instead of emitting `suppressed=Y`. The behavior predates KYRC26. It needs a design call, tracked in issue #46 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/46), because KYRC26 adds all-star demographic rows that KYRC25 left out, and career centers mix an `N/A` enrollment with starred counts. Issue #44 is the parallel call for postsecondary enrollment. It does not cover this file. Do not treat the drop as the intended design.
4. Career-center enrollment is the text `N/A` (1,057 cells in KYRC25, 1,309 in KYRC26), meaning not applicable because students count at their home school. There is no truly empty measure cell. pandas reads `N/A` as missing, so Allen County `005901` All Students emits the grade-12 metrics and no participation rate. Treating `N/A` as an empty denominator stays correct, and it does not change the prior-year counts.
5. Compare the token set, then the counts. KYRC26 is larger because it is a 485 × 17 grid and KYRC25 is sparse. The KPI increase is 349 rows, and the arithmetic above adds up. The two removed schools are Fayette `A2` technical centers, not co-op totals. 17 + 15 = 32 source rows, and those rows held 25 published grade-12 groups and no participation rate.
6. Prior-year identity is a diff against output the base parser already wrote, before `include_kyrc26` was set. 2022-2025 match that file: 9,772 / 9,270 / 9,935 / 14,075. Published state, Fayette, Bryan Station, and Adair County High School values are unchanged.
7. State district is `All Districts` for 2024-2026 and `State` for 2022-2023. Issue #42: `scripts/generate_report_drafts.py` selects `State`, and `get_state_average` also requires school name `---District Total---`. CTE is not in the indicator list. The 2022-2023 state rows are district `State` and school name `---State Total---` (school code `999`), so neither shape would match both conditions.
8. `GREC` became `GLEC` on the 25 Jefferson codes that remain, plus new school `275951`. Nothing in this pipeline joins on the co-op name. District `606` and school type `B1` are code-domain changes, not header changes.
9. Before adding a filename guard, parse one sibling row. Career Pathways, Career Readiness Indicators, the Perkins report, Postsecondary Opportunities, and Student Objectives emit nothing.
10. The two columns the parser maps to rates (participants, completers) hold counts, as do all four 2022 and 2023 measure columns. `TOTAL NUMBER OF STUDENT` and `GRADE 12 CTE ELIGIBLE COMPLETER` are already mapped as counts. None of the headers is named like a rate. There is no decimal in them, and completers are at or below eligible wherever both are numeric. Bryan Station 2022 stores completers 77 and eligible 413: 77 of 413 is 18.6%. The base parser publishes a count of 100 or below as the rate and nulls a count above 100. That loses data. It is pre-existing, tracked in issue #47 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/47), and not changed here. KYRC24 has 42 genuine participation rates above 100, and the same cleaner nulls those too (#47). KYRC25 and KYRC26 compute the rate from the count columns.

### `student_enrollment` — S — in review

| | |
| --- | --- |
| KYRC26 files | `KYRC26_OVW_Student_Enrollment.csv` |
| Header changes | None versus KYRC25. The grade headers were already mapped. On pandas 3, a comma in `669,865` has to be stripped on StringDtype or `to_numeric` drops it. pandas 2.3.3 already strips it, because `dtype=str` is still object |
| Fixtures / assertions | State `999000` total 669,865, year `2026`. Preschool 29,782 is on this file. 2024 state All Students primary is 331,857 |
| Gotchas | `OVW_Student_Membership` is not preschool membership and has no ETL. KYRC26 removed its Preschool column. State All Grades there is 624,645, which is not enrollment 669,865 and is not 669,865 − 29,782. The remaining grade headers are mapped and would emit `student_enrollment_*`, so this parser skips that filename. Preschool enrollment is on this file. `openpyxl` is not declared, so a missing install skips the 2019 workbook without failing the run |

Result, checked 17 October 2026 against the public `datasets` container. `include_kyrc26 = True`, the StringDtype comma cleaner in `clean_enrollment_data`, an NA-safe total lookup, and the membership filename skip are the changes. `etl/kyrc.py`, `BaseETL`, and the demographic config were not edited. Re-HEAD the enrollment file after 26 October 2026. If the hashes diverge from the stamps below, take the newer stamp and record which container it came from.

`datasets` HEAD for the KYRC25 and KYRC26 files did not return Content-MD5. The UAT Content-MD5 matches the bytes downloaded from `datasets`, and `datasets` Last-Modified is newer, so these are the `datasets` objects.

| File | Bytes | MD5 | Last-Modified |
| --- | --- | --- | --- |
| `KYRC26_OVW_Student_Enrollment.csv` | 3,586,080 | `34fb611ac6c7356089d79681a9748fdd` | Thu, 15 Oct 2026 10:00:10 GMT |
| `KYRC25_OVW_Student_Enrollment.csv` | 3,620,384 | `70f5e78cc6b68f7b1161e53562e227ca` | Sat, 18 Jul 2026 21:29:47 GMT |
| `KYRC24_OVW_Student_Enrollment.csv` (historical, the registered file) | 3,584,130 | `7bee3e93297164c1823f4aac6504a81c` | Wed, 28 May 2025 23:34:07 GMT |

Each KYRC file has 30 columns. The headers match from KYRC24 through KYRC26. KYRC26 has 22,923 data rows. KYRC25 has 23,136. KYRC24 has 23,092. The MD5s are of those downloaded bytes.

Every registered URL returned 200. `primary_enrollment_2023.csv`, `primary_enrollment_2022.csv`, `primary_enrollment_2021.csv`, `primary_enrollment_2020.csv`, and `STUDENT_PRIMARY_ENROLLMENT_18-19.xlsx` are on the historical host. `KYRC25_OVW_Student_Enrollment.csv` and `KYRC26_OVW_Student_Enrollment.csv` are on `datasets` and on UAT, with one MD5 each. The guessed OAA names `Student_Enrollment_2024.CSV`, `Student_Enrollment_2025.CSV`, `Student_Enrollment_2026.CSV`, and `primary_enrollment_2025.csv` are 404 and were never registered. There is no leftover-temp skip.

The historical-host KYRC25 copy is a different file: 3,588,121 bytes, MD5 `3235aa7a4fdc6f45ca44c07fdcbe6b69`, Last-Modified Tue, 10 Feb 2026 19:57:10 GMT. Same keys. The only measure difference is that 32,263 zero cells are blank. It is not registered. The parser already omits a zero, so that copy would not change KPI values. The blob copy of KYRC24 (3,593,935 bytes, MD5 `12282908c95cd741bd04d6edf0267e64`) is also not registered. Its measure cells match the historical file. `CO-OP` differs on 2,504 rows: 2,488 Jefferson rows are blank here and `GREC` on the blob, and school `121026` (Rev. Henry E. Baker Sr. Interm. School, 16 rows) stores `CO-OP` `-84.21926253` and `CO-OP Code` `CKEC` here, against blob `CKEC` / `901`. The historical file returns 200, so it stays the source. Switching would change `co_op` on those 2,504 rows.

All Students, KPI `year` `2026` (source `School Year` `20252026`). `All Schools` is stored as `---District Total---`. Counts are integers. No measure cell has a decimal. Where every grade cell is numeric, including zeros and excluding blanks, the grades sum to `All Grades`. That holds for every row in KYRC24, KYRC25, and KYRC26.

| Row | School code | Total | Preschool | Kindergarten | Grade 12 | Grade 14 | Suppressed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| State (`All Districts`) | `999000` | 669,865 | 29,782 | 46,806 | 49,115 | 634 | N |
| Fayette County district | `165000` | 43,057 | 1,251 | 3,026 | 3,136 | 26 | N |
| Bryan Station High School | `165170` | 2,241 | blank | blank | 567 | 2 | N |
| Adair County High School | `001010` | 818 | blank | blank | 204 | 2 | N |

State primary (preschool through grade 5) is 323,375. Middle (grades 6-8) is 145,235. Secondary (grades 9-12) is 200,621. 323,375 + 145,235 + 200,621 + 634 = 669,865. Fayette primary is 20,480, middle 9,333, secondary 13,218, grade 14 is 26. 20,480 + 9,333 + 13,218 + 26 = 43,057. Bryan Station grades 9-12 are 621, 529, 522, and 567. Secondary is 2,239. 2,239 + 2 = 2,241. Adair grades 9-12 are 194, 217, 201, and 204. Secondary is 816. 816 + 2 = 818.

Bryan Station `state_school_id` `034165170`, `nces_id` `210186000388`, `school_type` `A1`, county `FAYETTE`. Fayette district `nces_id` `2101860`, `co_op` `CKEC`, `co_op_code` `901`. That `901` is the cooperative code. The school code stays `165170`. KYRC25 stored Bryan Station's NCES id as `210186000000`. KYRC24 and KYRC26 both use `210186000388`. Adair County High School `001010` is `210003000001` in KYRC24 and KYRC26 and `210003000000` in KYRC25. The KYRC24 historical Fayette district NCES id is blank.

pandas is unpinned. On pandas 2.3.3, `read_csv(..., dtype=str)` returns object, so the existing `dtype == object` comma strip already runs. The base code on pandas 2.3.3 already writes this branch's 2019-2025 rows. The comma bug is pandas 3 only: `dtype=str` is StringDtype, the object check misses `669,865`, and `to_numeric` drops it. The StringDtype cleaner restores that pandas 2 output on pandas 3. It does not revise the published 2019-2025 counts. KYRC26 is new on both versions.

The table is a pandas 3.0.6 run of the base code, before the cleaner and before `include_kyrc26`, against this branch. On that version, 2019-2023 already matched, because the 2020-2023 primary files have no comma inside a measure cell and the 2019 workbook stores numbers. 2024 and 2025 lost the comma-formatted cells and the aggregates those cells complete. After the cleaner they match the pandas 2 output. No previously emitted row disappeared. On pandas 3 the CSV text of `value` no longer has a trailing `.0`. The number is the same.

| Year | pandas 3 base | pandas 2.3.3 and this branch |
| --- | --- | --- |
| 2019 | 164,810 | 164,810 |
| 2020 | 176,886 | 176,886 |
| 2021 | 174,798 | 174,798 |
| 2022 | 178,567 | 178,567 |
| 2023 | 179,785 | 179,785 |
| 2024 | 180,131 | 181,563 |
| 2025 | 180,375 | 181,803 |
| 2026 | 0 | 180,076 |

On pandas 3.0.6 the 2024 state All Students row kept only grade 14 (653), the one state cell with no comma. The 2025 state row kept only grade 14 (637). Counts of 1,000 and above are the cells with a comma: 1,354 in KYRC24 and 1,347 in KYRC25. State totals for the earlier years were already present on both versions: 2019 is 700,992 (school code `999000`), 2020 is 698,388, 2021 is 682,953, 2022 is 685,401, and 2023 is 687,294 (school code `999`).

`openpyxl` was not a declared dependency in that pull request. `pyproject.toml` then listed pandas, pydantic, PyYAML, ruamel-yaml, pytest, tabulate, and requests. PR #57 adds `openpyxl` so the 2018-19 workbooks can be read. `pd.read_excel` needs it for `STUDENT_PRIMARY_ENROLLMENT_18-19.xlsx`. `BaseETL.process` catches a per-file exception, logs it, and continues, so a missing openpyxl writes 2020-2026 and leaves 2019 out.

What pandas 3 drops, and what pandas 2.3.3 already wrote: 2024 is 1,432 rows, 1,354 comma-formatted cells plus 78 aggregate rows that had no under-1,000 grade to sum. 180,131 + 1,354 + 78 = 181,563. 2025 is 1,428 rows, 1,347 comma-formatted cells plus 81 aggregate rows. 180,375 + 1,347 + 81 = 181,803. Every restored value is at least 1,000. Fifty-six existing aggregate rows differ between the pandas 3 base and this output, 31 in 2024 and 25 in 2025, and all 56 are `student_enrollment_primary` (37), `student_enrollment_middle` (7), or `student_enrollment_secondary` (12). No previously emitted grade count or total differs. The 2024 state Asian primary on pandas 3 was 591, which is only the preschool cell (`591`). Kindergarten through grade 5 are `1,054`, `1,162`, `1,166`, `1,119`, `1,126`, and `1,157`. 591 + 1,054 + 1,162 + 1,166 + 1,119 + 1,126 + 1,157 = 7,375, which is the pandas 2 value. The fixture locks the same kind of sum for 2024 state All Students primary: 31,467 + 49,681 + 50,859 + 52,241 + 48,560 + 49,321 + 49,728 = 331,857. Published state totals are 686,224 (2024) and 682,013 (2025). Fayette stays 44,362 and 44,390. Bryan Station stays 2,266 and 2,262. Adair County High School stays 847 and 860.

KYRC26 is not a complete 18-demographic grid. KYRC25 has 1,572 school codes and 23,136 rows. KYRC26 has 1,561 school codes and 22,923 rows. Code change versus KYRC25: 5 added, 16 removed, net −11. 1,572 − 16 + 5 = 1,561. None of the removed or added codes are cooperative totals 901-909.

| Piece | Rows |
| --- | --- |
| KYRC25 | 23,136 |
| 16 removed schools | −214 |
| Demographic keys dropped on the 1,556 shared codes | −753 |
| Demographic keys added on those codes | +691 |
| 5 new codes | +63 |
| KYRC26 | 22,923 |

23,136 − 214 − 753 + 691 + 63 = 22,923. The 753 dropped keys are 41 district and 712 school. The 691 added keys are 39 district and 652 school. State keys stay 18. 41 + 712 = 753. 39 + 652 = 691.

District rows go from 2,823 to 2,821. No district code was added or removed (175 codes). 2,823 − 41 + 39 = 2,821. School rows go from 20,295 to 20,084. 20,295 − 214 − 712 + 652 + 63 = 20,084. School codes go from 1,396 to 1,385. 1,396 − 16 + 5 = 1,385. 18 + 2,821 + 20,084 = 22,923.

The 16 removed schools are not co-op totals. Seven are `A8` virtual academies and account for 103 of the 214 rows: Christian County Public Schools VLA `115012` (17), Cardinal Virtual Academy `121015` (16), HCS Virtual Academy `231035` (16), Liberty Academy `465035` (13), Cultivate Virtual Learning Academy `531025` (16), Todd County Virtual Academy `551023` (11), and Beacon Academy `571020` (14). 17 + 16 + 16 + 13 + 16 + 11 + 14 = 103. The other nine are Wurtland Middle `221024` (12), Wurtland Elementary `221110` (13), Young Elementary `275374` (13), Jefferson Regional Juvenile Detention Center `275463` (10), Arlie Boggs Elementary `331210` (10), GEO International High `571245` (13), Rivendell Elementary `571300` (16), Rivendell High `571305` (13), and Williamstown Head Start Preschool `593040` (11). 12 + 13 + 13 + 10 + 10 + 13 + 16 + 13 + 11 = 111. 103 + 111 = 214.

The 5 new codes are The Anderson County Learning Academy `011030` (11), Mary E. Britton Middle School `165220` (16), Greenup County School of Innovation `221015` (13), Metcalfe County Academy `425015` (8), and Early Learning Academy `571006` (15). 11 + 16 + 13 + 8 + 15 = 63.

KPI rows are not one per source row. A grade count is emitted when it is greater than 0. A total is emitted when `All Grades` is greater than 0. Primary, middle, and secondary sums are emitted when at least one of their grades is greater than 0. KYRC25 writes 181,803. KYRC26 writes 180,076.

| Piece | KPI rows |
| --- | --- |
| KYRC25 | 181,803 |
| 16 removed schools | −1,579 |
| Shared codes | −549 |
| 5 new schools | +401 |
| KYRC26 | 180,076 |

181,803 − 1,579 − 549 + 401 = 180,076. The shared-code change is state +1, district −62, and school −488. 1 − 62 − 488 = −549. The state +1 is Migrant grade 14: KYRC25 publishes 0, which this parser omits, and KYRC26 publishes 1. Level totals: state 339 to 340, district 43,300 to 43,238, school 138,164 to 136,498. 339 + 43,300 + 138,164 = 181,803. 340 + 43,238 + 136,498 = 180,076.

| File | Blank cells | Zero cells | Positive cells | All-blank rows | `*` cells | KPI rows |
| --- | --- | --- | --- | --- | --- | --- |
| KYRC24 | 188,275 | 32,352 | 148,845 | 0 | 0 | 181,563 |
| KYRC25 | 188,800 | 32,263 | 149,113 | 0 | 0 | 181,803 |
| KYRC26 | 187,508 | 31,527 | 147,733 | 0 | 0 | 180,076 |

188,275 + 32,352 + 148,845 = 369,472, and 23,092 × 16 = 369,472. 188,800 + 32,263 + 149,113 = 370,176, and 23,136 × 16 = 370,176. 187,508 + 31,527 + 147,733 = 366,768, and 22,923 × 16 = 366,768. There is no Suppressed column. The only non-numeric measure token is a blank. A blank grade is not part of `All Grades`. Bryan Station `165170` All Students leaves preschool through grade 8 blank, and 621 + 529 + 522 + 567 + 2 = 2,241. Those blank grades emit no KPI row, which is what notes/8 requires for a grade that is not offered. A zero is an offered grade with a count of 0, and it is inside the `All Grades` sum. The current parser drops that zero in two places: `extract_metrics` keeps a count only when it is greater than 0, and `convert_to_kpi_format` also skips a metric when the value is 0. Whether a published zero should be a KPI row is a pre-existing design question, not suppression. Counts of 1 and 2 are published, and the output has no `suppressed` `Y` row. The locked small-count row is Bryan Station Native Hawaiian or Pacific Islander: total 2, grades 9 and 10 are 0, grades 11 and 12 are 1, grade 14 is 0. The KPI rows are the total, grade 11, grade 12, and secondary 2. The same group's KYRC25 row publishes grade 10 and grade 11 as 1.

`KYRC26_OVW_Student_Membership.csv` is not registered under this directory and it is not preschool membership. KYRC26 removed the Preschool column from that file. Its state All Students `All Grades` is 624,645. 669,865 − 29,782 = 640,083, which is not 624,645. Preschool enrollment, 29,782, is on `OVW_Student_Enrollment`. The grade headers that file still has are mapped, so a parse emits `student_enrollment_*` rows. `accepts_data_file` skips a name that contains `student_membership`. That keeps a leftover membership file from writing a second total next to this one.

`CO-OP` `GREC` is 2,491 rows and 169 school codes in KYRC25, all Jefferson County codes beginning `275`. KYRC26 labels the remaining codes `GLEC`: 2,456 rows and 167 codes. The two removed Jefferson schools are Young Elementary `275374` (13) and the Jefferson Regional Juvenile Detention Center `275463` (10). 13 + 10 = 23. 2,491 − 23 = 2,468. Twelve demographic rows drop on the codes that remain. 2,468 − 12 = 2,456. Nothing in this pipeline groups or joins on the co-op name.

Both years' demographic columns are the same 18 labels, all already in the 2026 demographic config. The label check reported 18 valid and no unexpected labels for 2024, 2025, and 2026. The five optional labels that are absent (Alternate Assessment, Consolidated Student Group, Non Economically Disadvantaged, Non English Learner, Student without Disabilities (IEP)) are the same set KYRC24 and KYRC25 already missed. No new KPI names. `KPIS.md` lists the KYRC files, the primary files, the 2018-19 workbook, and years 2019-2026.

The 2023 file has 19 rows whose `SCHOOL YEAR` is `20022023` instead of `20222023`. `extract_year` keeps the last four digits, so they still file under 2023. That is a source typo, left as it is.

Fixtures are real rows in `tests/fixtures/student_enrollment/`: six KYRC26 rows, five KYRC25 rows, four KYRC24 rows, and the 2022 and 2023 state All Students rows. They include the Bryan Station blank elementary grades and the Bryan Station Native Hawaiian and American Indian rows, where a 0 sits beside a published 1 or 2. `tests/test_fixture_sizes.py` passes.

Follow-up, not fixed here. Issue #42 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/42): `scripts/generate_report_drafts.py` selects state rows with `district == 'State'`, and `get_state_average` also requires `school_name == '---District Total---'`. Student enrollment is not in that script's indicator list. If it were added, no year would match both conditions. The 2020-2023 state rows are district `State` and school name `---State Total---` (school code `999`; 2019 uses `999000` with the same district and school name). KYRC24, KYRC25, and KYRC26 store district `All Districts` and school name `All Schools`, which this parser stores as `---District Total---` (school code `999000`). `analysis/scripts/base_analysis_dataset.py` reads `student_enrollment_total` and drops `---District Total---`. It does not select district `State`. On pandas 3, before the StringDtype cleaner, an `All Grades` count at or above 1,000 was missing. pandas 2.3.3 already wrote it. A total under 1,000 was present on both.

Lessons for later agents:

1. HEAD the registered URLs before assuming a temporary file went 404. This module's KYRC25 entry was already the permanent file, and it returns 200 on `datasets` and on UAT, with one MD5. The guessed OAA names are 404 and were never registered. Do not add a skip for a filename this directory has never used. The 2020-2023 primary names and the 2018-19 workbook stay. The historical-host KYRC25 copy blanks the zero cells. It is not registered.
2. "Header changes: none" means the KYRC26 header matches KYRC25. The grade headers were already mapped. pandas is unpinned. On pandas 2.3.3, `dtype=str` is object and the existing comma strip already keeps `669,865`. On pandas 3, `dtype=str` is StringDtype, and a strip that checks only `dtype == object` drops it. The state total is 669,865. Preschool 29,782 is on this file.
3. This file has no Suppressed column and no `*` cell. A blank grade is not part of `All Grades` and emits nothing. A zero is an offered grade with a count of 0. The parser skips that zero in two places: `extract_metrics` (`value > 0`) and `convert_to_kpi_format` (`value != 0`). Whether a published zero should be a KPI row is a pre-existing design question, not suppression. Counts of 1 and 2 are published. There is no `suppressed` `Y` row.
4. `OVW_Student_Membership` is not preschool membership. KYRC26 has no Preschool column on that file. State All Grades is 624,645. 669,865 − 29,782 = 640,083, which is not 624,645. Preschool enrollment is on `OVW_Student_Enrollment`. The grade headers that file still has would emit `student_enrollment_*` rows, so this parser skips a `student_membership` filename. The file is not registered here and has no ETL.
5. Compare the code sets, then the counts. KYRC26 is smaller by 213 source rows and 1,727 KPI rows, and the arithmetic above adds up. The 16 removed schools are not co-op totals. Seven virtual academies account for 103 of the 214 removed source rows.
6. Prior-year identity against pandas 2.3.3 is the base code's own output. That version already writes this branch's 2019-2025 rows: 164,810 / 176,886 / 174,798 / 178,567 / 179,785 / 181,563 / 181,803. The smaller 2024 and 2025 counts, and the 56 partial aggregate sums, are a pandas 3 run of the base code. The StringDtype cleaner restores the pandas 2 sums. Published state, Fayette, Bryan Station, and Adair County High School values are the file values, including 686,224 and 682,013. The 2024 state All Students primary anchor is 331,857.
7. State district is `All Districts` for 2024-2026 and `State` for 2019-2023. Issue #42: `scripts/generate_report_drafts.py` selects `State`, and `get_state_average` also requires school name `---District Total---`. Student enrollment is not in the indicator list. Neither shape would match both conditions.
8. `GREC` became `GLEC` on the Jefferson codes that remain. Nothing in this pipeline joins on the co-op name. The KYRC24 historical file's `CO-OP` quirks (blank Jefferson rows, and `-84.21926253` on school `121026`) stay, because that file returns 200 and the blob copy is not registered.
9. The 2023 file has 19 rows dated `20022023`. `extract_year` keeps 2023. Leave the typo.
10. `openpyxl` is not a declared dependency. Without it, reading `STUDENT_PRIMARY_ENROLLMENT_18-19.xlsx` raises, `BaseETL.process` logs that error and continues, and 2019 is absent from the output. Dependencies were not changed.

### `secondary_enrollment` — S — in review in PR #50

| | |
| --- | --- |
| KYRC26 files | `KYRC26_OVW_Secondary_Enrollment.csv` |
| Header changes | None. KYRC24, KYRC25, and KYRC26 share the same 23-column grade headers. No new column alias was required |
| Fixtures / assertions | State All Students total 30,193, suppression `N`, year `2026`, district `All Districts`. Fayette `165000` total 97, the sum of its four schools. Ignite Institute `035160` total 1,300. American Indian at Ignite is a published 1, not a star. The 2024 state anchor is 28,978 |
| Gotchas | pandas 3 drops comma counts unless the cleaner accepts StringDtype. KYRC25 state total is 29,360. KYRC26 stores not-offered grades as blanks; KYRC25 stored them as zeros. Both already emit no KPI row. All Grades can exceed the published grade cells in KYRC25 (37 rows, state gap 3) and KYRC26 (47 rows, state gap 63). KYRC24 and 2020-2023 totals equal the grade sums |

`include_kyrc26` is set in this change. `clean_enrollment_data` now strips commas on object and on StringDtype, the same check as `etl/student_enrollment.py`. The helper stays in this module. `BaseETL.standardize_missing_values` is unchanged. pandas is unpinned.

This series is students with a secondary (concurrent) enrollment at this school, by grade. The 2018-19 workbook cover sheet defines `TOTAL_STUDENTS` as "Count of unique students with secondary enrollment". Our interpretation of the data is a concurrent second enrollment at a school other than the student's main school, mostly at career and technical centers and regional academies. KDE's catalogue pairs the file with primary enrollments. KYRC26 has 91 school codes. The state total is 30,193, beside 669,865 total enrollment. Ignite Institute, school type `A2`, is 1,300.

Every registered URL returned 200 on 17 October 2026. KYRC25 on `datasets` is Last-Modified `Sat, 18 Jul 2026 21:29:47 GMT`, 177,835 bytes, ETag `0x8DEE513B10C679E`. That host does not send Content-MD5. The UAT copy is Last-Modified `Tue, 06 Oct 2026 20:27:25 GMT`, the same length, Content-MD5 `X9F+SUhepKjuzHDUkbaxYw==`. The downloaded file matches that hash. KYRC26 on `datasets` is Last-Modified `Thu, 15 Oct 2026 10:00:10 GMT`, 184,131 bytes, ETag `0x8DF2AA31999B9A2`. The UAT copy is Last-Modified `Sat, 10 Oct 2026 06:02:11 GMT`, the same length, Content-MD5 `16x42gQflJiGcEhEwt/llA==`. The downloaded file matches that hash. The 2020-2023 CSVs and `STUDENT_SECONDARY_ENROLLMENT_18-19.xlsx` returned 200 on the historical host, at the lengths the downloader stored. Guessed temporary names `Secondary_Enrollment_2024.CSV`, `Secondary_Enrollment_2025.CSV`, and `Secondary_Enrollment_2026.CSV` are 404 on the OAA host, as are the lowercase twins. None of those names is registered. There is no leftover filename to skip.

`KYRC24_OVW_Secondary_Enrollment.csv` is registered on the datasets host. Other modules' KYRC24 files use the historical host. The config key `url: kyrc25` is the datasets host, not the school year. The downloader's UAT alternate only runs for names starting `KYRC25_` or `KYRC26_`, so it skips this file. That is harmless: the datasets and historical copies have identical cells. HEAD on 18 October 2026 (UTC): datasets returned 200, 166,278 bytes, Last-Modified `Thu, 14 May 2026 19:11:30 GMT`, ETag `0x8DEB1EC9B0085CF`, no Content-MD5. UAT returned 200, the same length, Last-Modified `Tue, 06 Oct 2026 20:27:26 GMT`, Content-MD5 `sA9mihukPqGnrieOebSzJg==`. The historical copy is 166,281 bytes, Last-Modified `Wed, 28 May 2025 23:34:08 GMT`, and its parsed cells match the datasets file. The file has the same 23 columns as KYRC25, 1,116 data rows, 83 school codes, no `*` cell, and 69 comma-formatted cells. Every row's grade cells sum to All Grades. The state All Students total is 28,978 (grades 6 / 107 / 709 / 6,273 / 7,673 / 7,542 / 6,652 / 16). Middle is 822 and high school is 28,140. 822 + 28,140 + 16 = 28,978. On pandas 3.0.6 and pandas 2.3.3 the file writes 6,378 KPI rows, and the other CSV years stay 4,858 / 4,636 / 5,629 / 6,056 / 6,619 / 6,955. The two outputs match cell for cell.

On pandas 2.3.3, `dtype=str` is object and the old comma strip already keeps `29,360`. That run writes 6,619 KYRC25 KPI rows. The state All Students total is 29,360. On pandas 3.0.6, `dtype=str` is StringDtype, the `dtype == object` strip does not run, and `to_numeric` turns `29,360` into a missing value. `config/mappings.yaml` then casts the column to Int64, so the missing total is `pd.NA`. `row.get('all_grades') or row.get('total_student_count')` raises `boolean value of NA is ambiguous`. `BaseETL.process` logs that error and writes no rows from the file. The same missing commas, if the abort is ignored, leave 6,541 KPI rows. The state row keeps only the counts under 1,000: grade 6 is 3, grade 7 is 146, grade 8 is 810, grade 14 is 13, and middle is 959. The total and the comma-formatted high-school grades are gone. This branch writes 6,619 again, and the state total is 29,360. A cell-by-cell compare of the pandas 2.3.3 base output and this branch's pandas 2.3.3 output is identical for 2020-2023 and 2025: 4,858 / 4,636 / 5,629 / 6,056 / 6,619, 27,798 rows. On pandas 3, 2020-2023 match the unfixed run. KYRC25 is the year that changes, from an aborted file to the pandas 2 file. KYRC26 writes 6,955.

There is no Suppressed column and no `*` cell in any registered file. Every KPI row is `suppressed` `N`. A count of 1 is published. KYRC25 has no blank grade cell. Its zeros are grade 6 1,144, grade 7 1,113, grade 8 949, grade 9 354, grade 10 261, grade 11 228, grade 12 247, and grade 14 1,145. KYRC26 blanks are 945, 867, 160, 34, 34, 22, 22, and 168 on those same grades. Its zeros are 267, 264, 778, 332, 264, 208, 264, and 1,007. All Grades is never blank and never zero (26 comma totals in KYRC25, 29 in KYRC26). A blank grade is not part of All Grades and emits nothing, which is what notes/8 requires for a grade that is not offered. A zero is an offered grade with a count of 0. The parser already drops that zero in `extract_metrics` (`value > 0`) and again in `convert_to_kpi_format` (`value != 0`). Whether a published zero should be a KPI row is a pre-existing design question, not suppression. It is not changed here. 2021, 2022, and 2023 have 60, 71, and 75 all-blank rows. Each of those rows is `Consolidated Student Group`, and every count cell is empty. They already emit no KPI row, so the prior-year counts stay 4,636 / 5,629 / 6,056. 2020 has no all-blank row. A constructed blank row, after the Int64 cast, emits nothing and does not raise.

All Grades counts unique students in every grade. KYRC files publish only grades 6-12 and 14. Older files had preschool through grade 5 columns, 0 or blank (2019 has blanks), and for 2019-2024 the All Grades total equals the sum of the grade cells. KYRC24 has no row where the total and the grade sum differ.

State All Students, school `999000`, district `All Districts`, school name stored as `---District Total---`, year `2026`: total 30,193, grades 9 / 95 / 829 / 6,287 / 7,707 / 8,192 / 6,985 / 26. Middle is 933 (9 + 95 + 829). High school is 29,171 (6,287 + 7,707 + 8,192 + 6,985). 933 + 29,171 + 26 = 30,130. The file total is 30,193. KYRC26 has 47 rows where All Grades is higher than the published grade cells. The state gap of 63 is East Jessamine Middle `281025` All Students 61 (total 68), Tates Creek Middle `165100` All Students 1, and Camden Station Elementary `465005` All Students 1. Those students are in grades the file does not publish; cause unverified. 62 of the 63 are at East Jessamine Middle and Tates Creek Middle, both grades 6-8 schools. On those rows, middle + high school + grade 14 will not equal the total. The parser stores All Grades. The 2025 state row is total 29,360, grades 3 / 146 / 810 / 6,064 / 7,699 / 7,951 / 6,671 / 13, middle 959, high school 28,385. 959 + 28,385 + 13 = 29,357. KYRC25 has 37 such rows, and the state gap is 3.

Fayette County `165000`, All Students, 2026: total 97. That total is Tates Creek Middle `165100` 1 + Tates Creek High `165105` 16 + Frederick Douglass High `165125` 70 + Success Academy `165133` 10. The district grade cells are 0 / 1 / 25 / 15 / 10 / 30 / 15 / 0, middle 26, high school 70. 26 + 70 = 96 because Tates Creek Middle's All Grades cell is 1 and its grade 6-8 cells are 0 (grades 9-14 are blank). Co-op `CKEC` / `901`, NCES `2101860`. The 2025 total is 87. Ignite Institute `035160` is new: total 1,300, grades blank / blank / 67 / 454 / 280 / 254 / 245 / blank. 67 + 454 + 280 + 254 + 245 = 1,300. Middle 67, high school 1,233. Co-op `NKCES` / `905`, NCES `210051002536`, school type `A2`. American Indian at that school is total 1 and grade 12 is 1, so high school is 1. Grades 8-11 are 0. Grades 6, 7, and 14 are blank. `suppressed` is `N`. Tates Creek Middle `165100` is also new: total 1. Grades 6-8 are 0 and grades 9-14 are blank, so the only KPI row is the total. Tates Creek High `165105` is total 16 in 2026 (grade 7 is 1, grade 8 is 15, middle 16, NCES `210186000383`) and total 7 in 2025 (grade 7 is 1, grade 8 is 6, middle 7, NCES `210186000000`). Grade 6 is blank in 2026 and 0 in 2025. Grades 9-12 and 14 are 0 in both years.

Source rows go from 1,185 to 1,235, which is 50. Six school codes are added and account for 66 rows: Boone County `035000` (16), Ignite Institute `035160` (16), Tates Creek Middle `165100` (4), East Jessamine Middle `281025` (11), Camden Station Elementary `465005` (3), and Trigg County Career and Technical Center `555025` (16). 16 + 16 + 4 + 11 + 3 + 16 = 66. Four codes are removed and account for 26 rows: Pembroke Elementary `115130` (5), Rosenwald Dunbar Elementary `281060` (5), Owensboro High `472110` (7), and Phoenix Horizon Community `525005` (9). 5 + 5 + 7 + 9 = 26. The other 85 codes change by 10 rows as demographic groups enter and leave. 66 − 26 + 10 = 50. 1,185 + 50 = 1,235. KPI rows go from 6,619 to 6,955, which is 336. The added codes contribute 107 + 107 + 4 + 32 + 3 + 91 = 344. The removed codes contribute 5 + 5 + 27 + 44 = 81. The shared codes contribute 73. 344 − 81 + 73 = 336. 6,619 + 336 = 6,955. `NKCES` / `905` is the new co-op, and it appears only on the two Boone County codes (32 source rows). Nothing in this pipeline joins on the co-op name.

Both years' demographic columns are the same 18 labels, all already in the 2026 demographic config. The label check reported 18 valid and no unexpected labels for 2025 and 2026. The five optional labels that are absent (Alternate Assessment, Consolidated Student Group, Non Economically Disadvantaged, Non English Learner, Student without Disabilities (IEP)) are the same set KYRC25 already missed. No new KPI names. `KPIS.md` lists KYRC24, KYRC25, KYRC26, the 2020-2023 files, the 2018-19 workbook, and years 2019-2026.

Fixtures are real rows in `tests/fixtures/secondary_enrollment/`: six KYRC26 rows, three KYRC25 rows, the KYRC24 state All Students row, the 2022 state All Students row, and the 2023 state All Students row plus the empty Consolidated Student Group row. They include the Ignite American Indian row and the Tates Creek Middle row, where a published 1 sits beside zeros and blanks. `tests/test_fixture_sizes.py` passes.

Follow-up, not fixed here. Issue #42 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/42): `scripts/generate_report_drafts.py` selects state rows with `district == 'State'`, and `get_state_average` also requires `school_name == '---District Total---'`. Secondary enrollment is not in that script's indicator list. If it were added, no year would match both conditions. The 2020-2023 state rows are district `State` and school name `---State Total---` (school code `999`). KYRC24, KYRC25, and KYRC26 store district `All Districts` and school name `All Schools`, which this parser stores as `---District Total---` (school code `999000`). No analysis script reads `secondary_enrollment_*`. A published zero is still omitted. That predates KYRC26.

`openpyxl` is not a declared dependency. Without it, reading `STUDENT_SECONDARY_ENROLLMENT_18-19.xlsx` raises, `BaseETL.process` logs that error and continues, and 2019 is absent from the output. The workbook has 1,140 rows, school year `20182019`, and no comma-formatted counts. Dependencies were not changed.

Lessons for later agents:

1. HEAD the registered URLs before assuming a temporary file went 404. This module's KYRC25 entry was already the permanent file, and it returns 200 on `datasets` and on UAT, with one MD5. The guessed OAA names are 404 and were never registered. Do not add a skip for a filename this directory has never used. The 2020-2023 names and the 2018-19 workbook stay. `KYRC24_OVW_Secondary_Enrollment.csv` is registered on the datasets host (`url: kyrc25` is the host, not the school year), unlike other modules' historical-host KYRC24 files. The downloader's UAT name check therefore skips it. The copies are identical. It writes 6,378 KPI rows and the state total is 28,978.
2. "Header changes: none" means the KYRC26 header matches KYRC25. pandas is unpinned. On pandas 2.3.3, `dtype=str` is object and the existing comma strip already keeps `29,360`. On pandas 3, `dtype=str` is StringDtype. A strip that checks only `dtype == object` loses the count, the Int64 cast stores `pd.NA`, and `or` aborts the file. The state total is 29,360. The restored KYRC25 file is 6,619 rows. The aborted file writes 0. Ignoring the abort leaves 6,541 and drops 29,360.
3. This file has no Suppressed column and no `*` cell. A blank grade is not part of All Grades and emits nothing. A zero is an offered grade with a count of 0. The parser skips that zero in two places. Whether a published zero should be a KPI row is a pre-existing design question, not suppression. Counts of 1 and 2 are published. There is no `suppressed` `Y` row.
4. Compare the code sets, then the counts. KYRC26 is larger by 50 source rows and 336 KPI rows, and the arithmetic above adds up. The six added codes are not co-op totals. Boone County is the new district, and `NKCES` is only those two codes.
5. Prior-year identity against pandas 2.3.3 is the base code's own output. That version already writes this branch's 2020-2023 and 2025 rows: 4,858 / 4,636 / 5,629 / 6,056 / 6,619. The empty Consolidated Student Group rows in 2021-2023 already emit nothing. Published state, Fayette, Tates Creek High, and Ignite values are the file values, including 30,193 and 29,360. KYRC24 was not registered on the base branch. This pull request adds it: 6,378 KPI rows, state total 28,978, and the other years stay put on both pandas versions.
6. State district is `All Districts` for 2024-2026 and `State` for 2020-2023. Issue #42: `scripts/generate_report_drafts.py` selects `State`, and `get_state_average` also requires school name `---District Total---`. Secondary enrollment is not in the indicator list. Neither shape would match both conditions.
7. All Grades counts unique students in every grade. KYRC files publish only grades 6-12 and 14, so All Grades can exceed those cells. For 2019-2024 the total equals the grade sum. KYRC25 has 37 rows (state gap 3: 29,360 against 29,357). KYRC26 has 47 rows (state gap 63: 30,193 against 30,130), and that state gap is East Jessamine Middle 61, Tates Creek Middle 1, and Camden Station Elementary 1. Those students are in grades the file does not publish; cause unverified. 62 of the 63 are at East Jessamine Middle and Tates Creek Middle, both grades 6-8 schools. On those rows middle + high school + grade 14 will not equal the total. Fayette's 97 is the sum of its four school totals. Store All Grades.
8. `openpyxl` is not a declared dependency. Without it, the 2019 workbook is skipped and the run continues. Dependencies were not changed.

### `out_of_school_suspension` — S — in review

| | |
| --- | --- |
| KYRC26 files | `KYRC26_OVW_Student_Suspensions.csv` |
| Header changes | None versus the CRDC header this module already maps. Count columns are already in `module_column_mappings` |
| Fixtures / assertions | KPI year `2026` from `School Year`. That is CRDC 2021-22. There is no collected-year column in the output. No `999000` row. Bryan Station High `165170` All Students total 216. Adair County High `001010` total 87. American Indian at Bryan Station is a published 0. Camp Taylor `275004` All Students is starred in every count, including the four totals. South Hopkins Middle `265185` Female suppresses the in-school total because one component is starred |
| Gotchas | The KPI year is the report-card year. 2026 is CRDC 2021-22 and 2024 is CRDC 2020-21. `KYRC25_OVW_Student_Suspensions.csv` republishes CRDC 2020-21 and is not registered. This module does not read `safe_schools_discipline_YYYY.csv`. A total is suppressed when any component is starred |

`include_kyrc26` is set in this change. `KYRC25_OVW_Student_Suspensions.csv` is not registered. `accepts_data_file` skips that filename because it republishes CRDC 2020-21. This module does not read `safe_schools_discipline_YYYY.csv`. An all-blank measure row is skipped. A total is suppressed when any component is starred. The title-case aliases for the other discipline columns are removed. `BaseETL.standardize_missing_values` is unchanged. pandas is unpinned.

**The KPI year is the report-card year.** KDE's report-card glossary calls this file Student Suspensions (CRDC): counts of in-school and out-of-school suspensions for students with and without disabilities, reported to the federal Office for Civil Rights in the biennial Civil Rights Data Collection. It is not the annual Safe Schools `SSP3` resolution count. Report-card year 2024 (`KYRC24`, `School Year` `20232024`) is the 2020-21 collection. Every row's `Collected School Year` is `20202021`. Report-card year 2026 (`KYRC26`, `School Year` `20252026`) is the 2021-22 collection. Every row's `Collected School Year` is `20212022`. `extract_year` reads `school_year`. The collected year is mapped and then dropped: `KPI_COLUMNS` has no collected-year column, so it is not in the output. Filing these rows under the collected year would put the 2025-26 report card in 2022.

The CRDC columns are student counts. Single means one out-of-school suspension. Multiple means more than one. Bryan Station High School `165170`, All Students, year `2026`: single 7 and 28, multiple 35 and 146. 7 + 28 = 35. 35 + 146 = 181. 35 + 181 = 216. In-school is 38 and 198, and 38 + 198 = 236. Adair County High School `001010` is 8 + 61 + 1 + 17 = 87. The Male multiple-with-disabilities cell is 1. There is no state row and no district total. School codes do not include `999000` or any code ending in `000`. `All Schools` does not appear. Fayette County in this file is 69 schools and 759 source rows, not one district row.

This module does not read `safe_schools_discipline_YYYY.csv`. Commit `e7d815a` (4 Aug 2025) deleted this module's SSP3 branch in the same commit that made `safe_schools_discipline` publish `out_of_school_suspension_count` from `OUT OF SCHOOL SUSPENSION SSP3`. `kpi_master` concatenates without deduping, so restoring the branch wrote a second copy of that count. The annual files stay on `safe_schools_discipline`, which publishes those resolution columns as counts. The title-case aliases in this module never matched the uppercase 2020-2023 headers, and they are removed. `KPIS.md` no longer lists metrics this module does not emit. On the 2023 state row of the annual file, which this module does not load, All Students SSP3 is 40,768 and Total Events is 75,620. Female plus Male equals Total Events, not All Students. That partition belongs to `safe_schools_discipline`.

Every registered URL returned 200 on 18 October 2026 (UTC). KYRC25 on `datasets` is Last-Modified `Sat, 18 Jul 2026 21:29:47 GMT`, 2,289,184 bytes, ETag `0x8DEE513B1179EC2`. That host does not send Content-MD5. The UAT copy is Last-Modified `Tue, 06 Oct 2026 20:27:29 GMT`, the same length, Content-MD5 `1DAQf/4ow22d/23qKbzaWw==`. The historical copy is Last-Modified `Tue, 10 Feb 2026 19:57:58 GMT`, the same length. All three hashes match `d430107ffe28c36d9dff6dea29bcda5b`. KYRC26 on `datasets` is Last-Modified `Thu, 15 Oct 2026 10:00:10 GMT`, 2,292,780 bytes, ETag `0x8DF2AA319B79578`. The UAT copy is Last-Modified `Mon, 12 Oct 2026 14:26:42 GMT`, the same length, Content-MD5 `e1nkELNFP1rgBv1+T5EjTw==`. The downloaded file matches that hash (`7b59e410b3453f5ae006fd7e4f91234f`). The registered KYRC24 file on the historical host is Last-Modified `Thu, 29 May 2025 22:13:24 GMT`, 2,281,971 bytes, MD5 `0a8685cfc993b688b6307a75d6a6764f`. `safe_schools_discipline_2020.csv` through `2023.csv` returned 200 on that host. `safe_schools_discipline_2019.csv` and `KYRC23_OVW_Student_Suspensions.csv` are 404. Guessed temporary names `Student_Suspensions_2024.CSV`, `Student_Suspensions_2025.CSV`, and `Student_Suspensions_2026.CSV`, and the lowercase and `OVW_` twins, are 404 on the OAA host. None of those names was registered. There is no leftover filename to skip.

`KYRC25_OVW_Student_Suspensions.csv` returns 200 and is not registered. It is cell-for-cell the datasets copy of KYRC24 apart from `School Year`. Both are the 2020-21 collection: collected year stays `20202021`, the six count cells match, and the same 1,793 rows are fully starred. `School Year` is `20242025`, so registering it republishes CRDC 2020-21 as report-card year 2025. Statewide All Students `out_of_school_suspension_total_count` is 6,447 in 2024 and 34,681 in 2026. A 2025 copy would repeat 6,447. The base branch never registered the file. `accepts_data_file` skips the filename even if it is dropped into the raw directory. A `datasets` copy of `KYRC24_OVW_Student_Suspensions.csv` also returns 200 (2,289,184 bytes, Last-Modified `Thu, 14 May 2026 19:11:30 GMT`, MD5 `5532ae6399748f4365358721046954c5`). Its count cells match the registered historical file and its `CO-OP` cells fill 1,804 blanks with `GREC`. The registered historical URL still returns 200, so this pull request does not swap it.

`SAFE_SCHOOLS_18-19.xlsx` returns 200 on the historical host (9,175,595 bytes, Last-Modified `Sat, 19 Sep 2026 14:26:42 GMT`). It is the shared multi-table workbook from journal 101, with wide race and gender columns and a `TABLE` column. It is not an `OVW_Student_Suspensions` file and it is not the same shape as `safe_schools_discipline_2020.csv`. It is not registered here.

There is no Suppressed column. KYRC24 uses `*` on 1,793 source rows where all six count cells are starred, plus one partial row: South Hopkins Middle School `265185`, Female, where only `In-School Without Disabilities` is `*` and the out-of-school cells are 0, 1, 0, and 1. There is no blank count cell and no comma. A count of 0 is published. KYRC26 has no `*` and no blank count cell. American Indian at Bryan Station is 0 in every count, and that 0 is `suppressed` `N`. A count of 1 is published (Adair County High School, Male, multiple with disabilities). KYRC26 is the 2021-22 collection, not the 2020-21 file with stars turned into zeros.

A total is the sum of its components only when every component is published. If any component is starred, the total is empty and `suppressed` `Y`. Summing the published parts is not the file's count: when every part is starred the sum of the remaining parts is 0, and when one part is starred the sum drops the withheld students. notes/8. On KYRC24 that changes 7,173 KPI rows from 0 / `N` to empty / `Y`. 1,793 fully starred rows × 4 totals = 7,172. The 7,173rd is South Hopkins Female's in-school total: the published component is 0 and the other component is starred, so the old sum was also 0. No total that had a nonzero published component changes. The out-of-school cells on that Female row are published, so the out-of-school total stays 2 / `N`. Component cells are unchanged. KYRC26 has no star, so its totals are unchanged. The row count stays 149,600.

An all-blank measure row is not in either registered file. The parser drops one before it can be labeled suppressed. A `*` stays. That drop removes zero rows. A lone blank component in an otherwise filled row is emitted as empty / `Y` and suppresses its total, which is not what notes/8 says (a blank emits nothing). KYRC24 and KYRC26 have no blank cells, so nothing changes today. Revisit that if KDE ever publishes blanks.

Stopping this module from reading the annual files changes 2020-2023 from the reviewed head (`3e14d71`) to 0. That head had restored SSP3 and written one `out_of_school_suspension_count` per demographic other than `Total Events`. The base run, before that restore, wrote 13 placeholder metrics for each starred SSP3 cell and dropped the published numbers. Both of those outputs are gone. `safe_schools_discipline` still publishes the count. KYRC24 stays 149,600 rows. KYRC25 is not written. KYRC26 is 149,490. pandas 2.3.3 and pandas 3.0.6 write the same 299,090 rows.

| Year | Base KPI rows | Reviewed head `3e14d71` | This fix | Published `N` | Starred `Y` |
| --- | --- | --- | --- | --- | --- |
| 2020 | 185,055 | 28,440 | 0 | 0 | 0 |
| 2021 | 187,889 | 28,458 | 0 | 0 | 0 |
| 2022 | 179,803 | 28,476 | 0 | 0 | 0 |
| 2023 | 178,165 | 28,512 | 0 | 0 | 0 |
| 2024 | 149,600 | 149,600 | 149,600 | 131,668 | 17,932 |
| 2025 | — | 149,600 | 0 | 0 | 0 |
| 2026 | — | 149,490 | 149,490 | 149,490 | 0 |

The 2024 `Y` count was 10,759 before this fix (1,793 fully starred rows × 6 component metrics, plus the one South Hopkins in-school cell). The four totals on those rows were 0 / `N` and were not in that 10,759. Adding the 7,173 totals gives 17,932 `Y` and 131,668 `N`. 10,759 + 7,173 = 17,932. 138,841 − 7,173 = 131,668. 17,932 + 131,668 = 149,600. The 7,173 empty totals are 1,794 in-school totals and 1,793 of each of the three out-of-school totals. Compared with the base output and with `3e14d71`, those are the only KYRC24 cells that change. KYRC26 matches `3e14d71` cell for cell apart from `last_updated`.

KYRC26 source rows go from 14,960 to 14,949, which is 11. Fifteen school codes are added and sixteen are removed. Each code has 11 demographic rows. 15 × 11 = 165. 16 × 11 = 176. 176 − 165 = 11. 14,960 − 176 + 165 = 14,949. The other 1,344 codes do not change their row count. 1,344 + 15 = 1,359. 1,344 + 16 = 1,360. KPI rows are 10 per source row. 14,960 × 10 = 149,600. 14,949 × 10 = 149,490. 11 × 10 = 110. 149,600 − 110 = 149,490. The added codes are `025012`, `035018`, `035135`, `115012`, `145015`, `165270`, `181012`, `331050`, `411013`, `415010`, `426035`, `491020`, `541043`, `545080`, and `555020`. The removed codes are `012005`, `115017`, `131016`, `135022`, `147120`, `221024`, `221110`, `275374`, `301140`, `331210`, `445045`, `515133`, `571245`, `571300`, `571305`, and `575071`. None of these are co-op totals `901`–`909`.

`CO-OP` on KYRC26 uses `GLEC` where KYRC25 used `GREC` and the school remains. KYRC26 also has 11 blank `CO-OP` cells. Nothing in this pipeline joins on the co-op name. KYRC25 itself is not an input.

Both registered years use the same 11 demographic labels, all already in the 2026 demographic config. The label check reported 11 found and no unexpected labels for 2024 and 2026. No new KPI names. `KPIS.md` lists the ten CRDC metrics and says the KPI year is the report-card year. Annual SSP3 and the other resolution counts stay under Safe Schools Discipline, where they are counts.

The registered files have no thousands separators. The largest KYRC26 cell is 646. `dtype=str` is StringDtype on pandas 3. `_numeric_clean` does not check `dtype == object`. `astype(str)` and a comma strip keep a synthetic `1,024` on pandas 2.3.3 and pandas 3.0.6. pandas is unpinned.

Fixtures are real rows in `tests/fixtures/out_of_school_suspension/`: four KYRC26 rows, three KYRC24 rows, and six 2023 rows. The 2023 file is present so the test can show this module emits nothing from it. The KYRC25 excerpt is removed. The KYRC24 rows are Bryan Station, Camp Taylor's fully starred row, and the South Hopkins partial star. `tests/test_fixture_sizes.py` passes.

Issue #42 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/42): `scripts/generate_report_drafts.py` selects state rows with `district == 'State'`, and `get_state_average` also requires `school_name == '---District Total---'`. Out-of-school suspension is not in that script's indicator list. This module's remaining years have no state row. `html/generate_dashboard_data.py` reads metrics matching `out_of_school_suspension.*_count` and does not require district `State`. The CRDC metrics come from this module. `out_of_school_suspension_count` comes from `safe_schools_discipline`.

Lessons for later agents:

1. Do not register `KYRC25_OVW_Student_Suspensions.csv`. It returns 200 and is the datasets KYRC24 file apart from `School Year`. Both are CRDC 2020-21. Registering it republishes 6,447 as 2025. `accepts_data_file` skips the name. The guessed OAA names are 404 and were never registered. Do not add a skip for a filename this directory has never used. The historical KYRC24 URL still returns 200. Leave it. The datasets KYRC24 copy only fills `CO-OP`.
2. The KPI year is the report-card year. 2026 is CRDC 2021-22 (`Collected School Year` `20212022` on all 14,949 rows). 2024 is CRDC 2020-21 (`Collected School Year` `20202021`). The output has no collected-year column. Do not file the extract under the collected year.
3. There is no `999000` row and no district total. Fixture a school. Bryan Station `165170` All Students is 216 in 2026 and 30 in 2024. Statewide All Students out-of-school total is 34,681 in 2026 and 6,447 in 2024.
4. KYRC26 has no `*` and no blank count. A 0 and a 1 are published, and every KPI row is `suppressed` `N`. KYRC24 stars 1,793 whole rows, plus one partial star at South Hopkins `265185` Female. A starred component suppresses the total. That is 7,173 KYRC24 rows changed from 0 / `N` to empty / `Y`. A blank row is not in these files. The parser skips one. A `*` stays.
5. Compare the code sets, then the counts. KYRC26 is smaller than KYRC24 by 11 source rows and 110 KPI rows, and the arithmetic above adds up. The fifteen added codes and sixteen removed codes are not co-op totals.
6. Do not emit SSP3 from this module. `safe_schools_discipline` has owned `out_of_school_suspension_count` since `e7d815a`. The base run wrote 185,055 / 187,889 / 179,803 / 178,165 placeholder rows. The reviewed head wrote 28,440 / 28,458 / 28,476 / 28,512. This fix writes 0 for 2020-2023. The other resolution columns are counts in `safe_schools_discipline`, not rates, and this module no longer maps them.
7. This module has no state row. Issue #42 does not list this indicator. A CRDC year would not match both of that script's conditions.
8. `SAFE_SCHOOLS_18-19.xlsx` returns 200 and is not this series. Journal 101 already recorded the wide multi-table schema. Do not register it on this module.
9. No shared parser or dependency changes. pandas is unpinned. pandas 2.3.3 and pandas 3.0.6 write the same 299,090 rows.

### `teacher_experience` — S — in review

| | |
| --- | --- |
| KYRC26 files | `KYRC26_OVW_Average_Years_School_Experience.csv` |
| Header changes | None versus KYRC25. `Educator Count` and `Average Years of Experience` were already mapped |
| Fixtures / assertions | State `999000` average 12.20, year `2026`. The educator count on that row is `39,242`. Fayette County `165000` is 13.38. Bryan Station High `165170` is 14.29. Adair County High `001010` is 13.37. Regional School Programs `147120` has one teacher and publishes 4.00. There is no suppressed group |
| Gotchas | `KYRC23_OVW_Average_Years_School_Experience.csv` returns 200 and republishes 2022-23. It is not registered. A comma in the educator count is dropped by the production Int64 cast unless it is stripped first. That cast runs on pandas 2.3.3 and on pandas 3. The measure is average years of teaching experience, not years at the building |

`include_kyrc26` is set in this change. Commas are stripped on object and on StringDtype before `config/mappings.yaml` casts `educator_count` to Int64. `KYRC23_OVW_Average_Years_School_Experience.csv` is not registered. `accepts_data_file` skips that filename because it republishes 2022-23. An all-blank measure row is skipped. `BaseETL.standardize_missing_values` is unchanged. pandas is unpinned.

**The measure is average years of teaching experience.** KDE's report-card glossary calls it Average School Experience: the total years of teaching experience divided by the number of teachers in the school. The 2018-19 cover sheet calls `AVGEXPERIENCEYEARS` "Average number of years' experience by teachers" and `CNTEXP` "Total number of teachers contributing to the Total Years of Experience". 703 KAR 5:140 defines average years of experience as the average professional experience of classroom teachers, excluding administrators, guidance counselors, and media specialists. It is not years employed at that building. The parser stores the published average. It does not recompute it from total years and the count. On the 2023 file, which still has `TOTAL YEARS EXPERIENCE`, the published average matches that quotient within 0.005 on every row. 2019, 2020, and 2021 match within 0.05, which is half of one displayed tenth. 2022 matches within 0.05 except four rows: Anchorage Independent district `006` and school `006010` publish 18.8 where the quotient is 17.89, the Model Laboratory district `606` publishes 12.5 where the quotient is 15.25, and the state total publishes 11.8 where 482,258 / 40,442 is 11.92. Those published averages are stored as printed.

The only KPI is `teacher_average_years_experience`. No other module emits that name. The educator count is not a KPI. `kpi_master` concatenates without deduping, so a second module must not emit this metric.

Every registered URL returned 200 on 17 October 2026 (Eastern). KYRC26 on `datasets` is Last-Modified `Thu, 15 Oct 2026 10:00:10 GMT`, 190,191 bytes, ETag `0x8DF2AA31962AE40`. That host does not send Content-MD5. The UAT copy is Last-Modified `Sat, 10 Oct 2026 06:00:45 GMT`, the same length, Content-MD5 `XzenUEtIqj0yND8acLvgMQ==`. The downloaded file matches that hash (`5f37a7504b48aa3d32343f1a70bbe031`). KYRC25 on `datasets` is Last-Modified `Thu, 14 May 2026 19:11:32 GMT`, 189,681 bytes, ETag `0x8DEB1EC9C702BBE`. The UAT copy is Last-Modified `Tue, 06 Oct 2026 20:27:29 GMT`, the same length, Content-MD5 `WLzmSxzIOUPw8mQJhdfsZA==`. The historical copy is the same 189,681 bytes and the same MD5 (`58bce64b1cc83943f0f2640985d7ec64`). The registered KYRC24 file on the historical host is Last-Modified `Thu, 29 May 2025 22:07:03 GMT`, 188,803 bytes, MD5 `664abd87ec09bf4cf74097f1c17cdffe`. `average_years_school_experience_2020.csv` through `2023.csv` and `SCHOOL_EXPERIENCE_18-19.xlsx` returned 200 on that host. `KYRC22_OVW_Average_Years_School_Experience.csv` is 404. Guessed temporary names `Average_Years_School_Experience_2024.CSV`, `Average_Years_School_Experience_2025.CSV`, and `Average_Years_School_Experience_2026.CSV`, and the lowercase, `OVW_`, and `School_Experience_` twins, are 404 on the OAA host, the historical host, and both blob containers. None of those names was registered. There is no leftover temporary filename to skip.

`KYRC23_OVW_Average_Years_School_Experience.csv` returns 200 and is not registered. It is the 2022-23 extract again. School year is `20222023` on all 1,587 rows. Educator counts and averages match `average_years_school_experience_2023.csv` after the historical 3-digit district codes are written as `NNN000`. School codes match, so registering both files would write each school twice for 2023. The historical file types Breathitt County `061` as school year `20022023`. KYRC23 corrects that cell to `20222023`. The average on that row is 12.21 either way, and `extract_year` already keeps the ending year `2023`. CO-OP blanks on the historical file are filled with `GREC` on 165 KYRC23 rows. The base branch never registered KYRC23. `accepts_data_file` skips the filename even if it is dropped into the raw directory. A `datasets` copy of `KYRC24_OVW_Average_Years_School_Experience.csv` also returns 200 (189,459 bytes, Last-Modified `Thu, 14 May 2026 19:11:30 GMT`, MD5 `38ed01f36eec256b258dbb6013d5a16b`). Its educator counts and averages match the registered historical file. Its `CO-OP` cells fill 167 blanks with `GREC` and replace one garbage value. School `121026`, Rev. Henry E. Baker Sr. Interm. School, has historical `CO-OP` `-84.21926253` and `CO-OP Code` `CKEC`. The datasets copy has `CKEC` and `901`. The count is 43 and the average is 12.16 on both copies. The registered historical URL still returns 200, so this pull request does not swap it.

There is no Suppressed column. No registered year has a `*` or a blank educator count or a blank average. A count of 0 is not in these files. A count of 1 is published. KYRC26 publishes 4.00 for Regional School Programs `147120`, which has one teacher, and 18.00 for Kentucky School for the Blind `602176`, which also has one teacher. Both are `suppressed` `N`. An all-blank measure row is not in any registered file. The parser drops one before it can be labeled suppressed. That drop removes zero rows.

An average of 0.00 is omitted by the existing `avg_years > 0` check. All 20 of those source rows have an educator count of 1 and total years of 0, and most of them are area technology centers and alternative schools. They look more like missing experience records than a measured average of zero. They are unexplained. Issue #54 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/54) tracks them. This predates KYRC26. Counts of those source rows: 2019 has 5, 2020 has 1, 2021 has 3, 2022 has 4, 2023 has 3, 2024 has 2, 2025 has 2, and 2026 has 0. Do not treat the omission as the rule to keep.

The comma bug is in the production path, on both pandas versions. `config/mappings.yaml` casts `educator_count` to Int64 inside `BaseETL.process`. That cast calls `to_numeric` before `should_skip_row`. A comma in `39,242` becomes missing, and the row is skipped. The old `_parse_numeric_with_commas` only runs inside `should_skip_row`, so it never sees the original text when the production config is used. The unfixed base dropped those same 12 rows on pandas 2.3.3 and on pandas 3. This is not a pandas-3-only bug. On pandas 3, `dtype=str` is also StringDtype, so a strip that checks only `dtype == object` would miss the same count. The cleaner in `standardize_missing_values` strips commas on object and on StringDtype before the cast. pandas is unpinned. Previously emitted averages are unchanged.

The rows restored by the comma strip, and no others:

| Year | School code | District | Average | Educator count in the file |
| --- | --- | --- | --- | --- |
| 2021 | `035` | Boone County | 13 | `1,229` |
| 2021 | `165` | Fayette County | 12.3 | `2,589` |
| 2021 | `275` | Jefferson County | 11.6 | `5,710` |
| 2024 | `035000` | Boone County | 13.35 | `1,262` |
| 2024 | `165000` | Fayette County | 13.15 | `2,646` |
| 2024 | `275000` | Jefferson County | 12.76 | `6,013` |
| 2024 | `999000` | All Districts | 12.17 | `40,598` |
| 2025 | `035000` | Boone County | 13.74 | `1,215` |
| 2025 | `165000` | Fayette County | 13.63 | `2,984` |
| 2025 | `275000` | Jefferson County | 12.93 | `6,115` |
| 2025 | `571000` | Warren County | 10.60 | `1,009` |
| 2025 | `999000` | All Districts | 12.28 | `41,230` |

KYRC26 comma rows, kept on the first run of this file: state `999000` is 12.20 from `39,242`, Boone `035000` is 14.02 from `1,238`, Fayette `165000` is 13.38 from `2,919`, and Jefferson `275000` is 12.85 from `6,007`.

| Year | Base KPI rows | This fix | Published `N` | Zeros still omitted |
| --- | --- | --- | --- | --- |
| 2019 | 1,580 | 1,580 | 1,580 | 5 |
| 2020 | 1,579 | 1,579 | 1,579 | 1 |
| 2021 | 1,559 | 1,562 | 1,562 | 3 |
| 2022 | 1,576 | 1,576 | 1,576 | 4 |
| 2023 | 1,584 | 1,584 | 1,584 | 3 |
| 2024 | 1,585 | 1,589 | 1,589 | 2 |
| 2025 | 1,576 | 1,581 | 1,581 | 2 |
| 2026 | — | 1,587 | 1,587 | 0 |

The base total with the 2019 workbook is 11,039. This fix is 12,638. 11,039 + 12 restored comma rows + 1,587 KYRC26 rows = 12,638. 3 + 4 + 5 = 12. There is no `suppressed` `Y` row in the base output or in this fix. A cell-by-cell compare of the shared keys, on pandas 2.3.3 and on pandas 3.0.6, found no value, district, school-name, or suppression change outside those 12 rows. pandas 2.3.3 and pandas 3.0.6 write the same 12,638 rows apart from `last_updated`. Without `openpyxl` the 2019 workbook is skipped and the total is 11,058. 12,638 − 1,580 = 11,058.

KYRC26 source rows go from 1,583 to 1,587, which is 4. Nineteen school codes are added and fifteen are removed. 19 − 15 = 4. 1,583 − 15 + 19 = 1,587. The added codes are `011030`, `035160`, `147120`, `165220`, `221015`, `301155`, `425015`, `426023`, `551023`, `551025`, `555025`, `571006`, `602000`, `602176`, `777116`, `777124`, `777129`, `777139`, and `777153`. The removed codes are `121015`, `131016`, `134011`, `221024`, `221110`, `275374`, `275463`, `275465`, `331210`, `523022`, `571245`, `571300`, `571305`, `593040`, and `606450`. None of these are co-op totals `901`–`909`. `602000` is a new district total, Kentucky School for the Blind District, so codes ending in `000` go from 174 to 175. `602176` is that school's only row, and its school type `D1` is new on this file. KPI rows are one per source row except those unexplained 0.00 averages. KYRC25 has 2 of them, so 1,583 − 2 = 1,581. KYRC26 has none, so it writes 1,587. 1,587 − 1,581 = 6, which is the 4 added codes plus those 2 KYRC25 rows.

`CO-OP` on KYRC26 uses `GLEC` where KYRC25 used `GREC` (165 rows) and the school remains. KYRC26 has 22 blank `CO-OP` cells. KYRC25 has 16. Nothing in this pipeline joins on the co-op name.

The file has no demographic column. `student_group` is `All Students` on every row. The base demographic check still warns that the race labels are absent. That warning is the shared required-label list. This file has never had those columns. No new KPI names. `KPIS.md` states the glossary definition, lists KYRC24, KYRC25, KYRC26, the 2020-2023 files, the 2018-19 workbook, and years 2019-2026.

The 2022 Kentucky Tech district total, school code `777`, is in the source file twice with the same count 104 and the same average 10.5. Both rows are kept. The 1,576 KPI rows for 2022 include both. That duplicate predates KYRC26.

Fixtures are real rows in `tests/fixtures/teacher_experience/`: six KYRC26 rows, three KYRC25 rows, two KYRC24 rows, the 2023 state row, the Bryan Station row, and the Breathitt `20022023` row, plus the 2022 state row, the 2021 state and Fayette rows, and the 2020 state and Bryan Station rows. `tests/test_fixture_sizes.py` passes.

Issue #42 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/42): `scripts/generate_report_drafts.py` selects state rows with `district == 'State'`, and `get_state_average` also requires `school_name == '---District Total---'`. Teacher experience is not in that script's indicator list. If it were added, no year would match both conditions. The 2019-2023 state rows are district `State` and school name `---State Total---` (school code `999`). KYRC24, KYRC25, and KYRC26 store district `All Districts` and school name `All Schools`, which this parser stores as `---District Total---` (school code `999000`). `analysis/scripts/base_analysis_dataset.py` reads `teacher_average_years_experience`, drops `---District Total---`, and does not select district `State`. A `---State Total---` row would pass that filter. `html/generate_dashboard_data.py` does not read this metric.

`openpyxl` is not a declared dependency. Without it, reading `SCHOOL_EXPERIENCE_18-19.xlsx` raises, `BaseETL.process` logs that error and continues, and 2019 is absent from the output. The workbook has 1,585 rows, school year `20182019`, and no comma-formatted counts. The state average is 12 from educator count 40,176. Dependencies were not changed.

Lessons for later agents:

1. HEAD the registered URLs before assuming a temporary file went 404. Every registered URL returned 200. The guessed OAA names are 404 and were never registered. Do not add a skip for a filename this directory has never used. `KYRC23_OVW_Average_Years_School_Experience.csv` returns 200 and is the 2022-23 file again. Do not register it. `accepts_data_file` skips the name. The historical KYRC24 URL still returns 200. Leave it. The datasets KYRC24 copy matches the measures and only repairs `CO-OP`.
2. The measure is average years of teaching experience, not years at the building. KDE's glossary and 703 KAR 5:140 say so, and the 2018-19 cover sheet says so. Store the published average. Do not divide total years by the count. On 2022, Anchorage, the Model Laboratory district, and the state total do not match that quotient. The stored state average is 11.8.
3. "Header changes: none" means the KYRC26 header matches KYRC25. pandas is unpinned. The production Int64 cast of `educator_count` runs before `should_skip_row` on pandas 2.3.3 and on pandas 3. A comma in `39,242` is dropped unless it is stripped first, on both versions. On pandas 3, `dtype=str` is also StringDtype. The state average is 12.20. The restored prior-year files are 1,562 / 1,589 / 1,581 for 2021 / 2024 / 2025. The unfixed files were 1,559 / 1,585 / 1,576 and were missing the state row in 2024 and 2025.
4. This file has no Suppressed column, no `*` cell, and no blank measure cell. A blank row is not in these files. The parser skips one. A count of 1 is published, and every KPI row is `suppressed` `N`. An average of 0.00 is omitted by `avg_years > 0`. All 20 of those rows have an educator count of 1 and total years of 0, mostly at area technology centers and alternative schools. They look more like missing experience records than a measured average of zero, and they are unexplained. Issue #54 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/54) tracks them. That is 5 / 1 / 3 / 4 / 3 / 2 / 2 / 0 source rows from 2019 through 2026. Do not tell a later agent to keep that omission.
5. Compare the code sets, then the counts. KYRC26 is larger than KYRC25 by 4 source rows and 6 KPI rows, and the arithmetic above adds up. The nineteen added codes and fifteen removed codes are not co-op totals. `602000` is the new Kentucky School for the Blind district.
6. Prior-year identity against the base code is 1,580 / 1,579 / 1,559 / 1,576 / 1,584 / 1,585 / 1,576 for 2019-2025, on pandas 2.3.3 and on pandas 3. The only cells that change are the 12 comma rows in the table above. Published state, Fayette, Bryan Station, and Adair values for the rows the base already wrote are the file values, including 12.0, 12.1, 11.8, and 12.06. KYRC26 is new: 1,587 rows, state average 12.20.
7. State district is `All Districts` for 2024-2026 and `State` for 2019-2023. School name `All Schools` is stored as `---District Total---`. `---State Total---` is left as written. Issue #42: `scripts/generate_report_drafts.py` selects `State` and also requires `---District Total---`. Teacher experience is not in the indicator list. Neither shape would match both conditions. The analysis dataset drops `---District Total---` and would keep `---State Total---`.
8. `openpyxl` is not a declared dependency. Without it, the 2019 workbook is skipped and the run continues. The total is then 11,058. Dependencies were not changed.
9. No shared parser or dependency changes. The comma cleaner stays in this module. pandas is unpinned. pandas 2.3.3 and pandas 3.0.6 write the same 12,638 rows. The 2022 source contains two identical Kentucky Tech district rows for school code `777`. Both are kept.
10. Do not emit the educator count as its own metric from this module or from another one. `teacher_average_years_experience` is already this module's KPI, and `kpi_master` does not dedupe.

### `student_teacher_ratio` — S — in review in PR #55

| | |
| --- | --- |
| KYRC26 files | `KYRC26_OVW_Student_to_Teacher_Ratio.csv` |
| Header changes | None |
| Fixtures / assertions | School rows only. Bryan Station High School `165170` is 15. School code is 6 digits and does not end in `000`. Do not assert `999000` or `School Name` `All Schools` |
| Gotchas | KYRC26 has 1,432 school rows and 1,322 KPI rows. District number `999` and the 173 KYRC25 codes that end in `000` are gone. That set is the state row (`999000`, ratio `15:01`) plus 172 `All Schools` district rows. Stays S because this pull request does not rebuild district or state totals |

Result, checked 18 October 2026 (UTC) against the public `datasets` container and the historical host. `include_kyrc26 = True` is the parser change, plus a read of `N:01:00` so seven 2019-20 ratios are no longer dropped. `etl/kyrc.py`, `BaseETL`, and the demographic config were not edited. Re-HEAD `KYRC26_OVW_Student_to_Teacher_Ratio.csv` after 26 October 2026. If the hash diverges from the stamp below, take the newer stamp and record which container it came from.

**The measure is students per one teacher.** KDE's report-card glossary defines Student to Teacher Ratio as the school's 6/30 SAAR membership divided by teachers on a full-time-equivalent basis, excluding administrators, guidance counselors, media specialists, and resource teachers (classification codes 2010, 2025, 2030, 2040, 2050, 2060, 2070, 2080, 2090, 2095, 2096, 2099, 2100, and 2211, object code 0110). 703 KAR 5:140 defines average student/teacher ratio as end-of-year membership divided by FTE teachers, excluding administrators, guidance counselors, and media specialists. The 2018-19 cover sheet calls `STDNT_TCH_RATIO` "Number of Students Enrolled compared to the number of Teachers on staff". The glossary says teacher counts for the Kentucky School for the Blind, the Kentucky School for the Deaf, and Model Laboratory Elementary and Model Laboratory High School are collected outside normal reporting and are not included in state totals. The parser stores the published figure, the number before the colon. It does not divide membership by FTE. These files do not contain those two components.

The only KPI is `student_teacher_ratio`. No other module emits that name. `kpi_master` concatenates without deduping, so a second module must not emit this metric.

Every registered URL returned 200 on 18 October 2026 (UTC). KYRC26 on `datasets` is Last-Modified `Thu, 15 Oct 2026 10:00:10 GMT`, 172,746 bytes, ETag `0x8DF2AA319BD0D95`, MD5 `c5dd6d64996e5d06720a3f2283e0d34d`. That host does not send Content-MD5. The UAT copy is the same length and the same MD5 (Content-MD5 `xd1tZJluXQZyCj8ig+DTTQ==`), Last-Modified `Sat, 10 Oct 2026 06:02:27 GMT`. KYRC25 on `datasets` is Last-Modified `Sat, 18 Jul 2026 21:29:47 GMT`, 188,244 bytes, ETag `0x8DEE513B119717D`, MD5 `a1b973ff98e6f18991f6793f8e0808ac`. The UAT copy matches that hash. The historical KYRC24 file is Last-Modified `Thu, 29 May 2025 22:03:56 GMT`, 185,796 bytes, MD5 `c5e86bf33742c629761b7e8e36d9be79`. `student_teacher_ratio_2020.csv` through `2023.csv` and `STUDENT_TEACHER_RATIO_18-19.xlsx` returned 200 on that host. `KYRC22_OVW_Student_to_Teacher_Ratio.csv` is 404. Guessed temporary names `Student_to_Teacher_Ratio_2024.CSV`, `Student_to_Teacher_Ratio_2025.CSV`, `Student_to_Teacher_Ratio_2026.CSV`, and the `Student_Teacher_Ratio_`, `OVW_Student_to_Teacher_Ratio_`, and lowercase twins, are 404 on the OAA host, the historical host, and both blob containers. None of those names was registered. There is no leftover temporary filename to skip.

`KYRC23_OVW_Student_to_Teacher_Ratio.csv` returns 200 and is not registered. It is the 2022-23 extract again. School year is `20222023` on all 1,602 rows. After the historical 3-digit district codes are written as `NNN000`, the school-code sets match. 1,599 ratio numerators match `student_teacher_ratio_2023.csv`. The other three codes are district totals the historical file repeats four times with conflicting ratios. KYRC23 keeps one of those values: Corbin Independent `133000` keeps `412:01:00`, Fayette County `165000` keeps `13:01`, and Floyd County `175000` keeps `5249:01:00`. Registering both files would write each school twice for 2023. The historical file types Breathitt County `061` as school year `20022023`. KYRC23 corrects that cell to `20222023`. The ratio on that row is 14 either way, and `extract_year` already keeps the ending year `2023`. CO-OP blanks on the historical file are filled with `GREC` on 167 KYRC23 rows. The base branch never registered KYRC23. `accepts_data_file` skips the filename even if it is dropped into the raw directory.

A `datasets` copy of `KYRC24_OVW_Student_to_Teacher_Ratio.csv` also returns 200 (186,446 bytes, Last-Modified `Thu, 14 May 2026 19:11:30 GMT`, MD5 `1e4d9b71ce4a209404dd0bbe248aedc9`). Its ratios match the registered historical file on all 1,603 school codes. Its `CO-OP` cells fill Jefferson County blanks with `GREC` (170 rows in the datasets copy, against 173 blanks on the historical file) and replace one garbage value. School `121026`, Rev. Henry E. Baker Sr. Interm. School, has historical `CO-OP` `-84.21926253` and `CO-OP Code` `CKEC`. The datasets copy has `CKEC` and `901`. The ratio is `0:01` on both copies. District name differs for Kentucky School for the Blind `602000` and `602176`: the datasets copy drops the word District. The registered historical URL still returns 200, so this pull request does not swap it.

`KYRC25_OVW_Student_to_Teacher_Ratio.csv` on the historical host returns 200 (188,226 bytes, Last-Modified `Tue, 10 Feb 2026 19:57:53 GMT`). Ratios match the registered datasets file on all 1,608 codes. Two district names differ: `602176` and `603390` are "Kentucky School for the Blind District" and "Kentucky School for the Deaf District" on `datasets`, and drop the word District on the historical host. 2024-25 is already registered. The historical copy is not a missing year.

There is no Suppressed column. No registered year has a `*` or a blank ratio. A blank row is not in these files. The parser drops one before it can be labeled suppressed. That drop removes zero rows from the registered files. Every KPI row is `suppressed` `N`. There is no suppressed group to fixture. A `*` in a ratio cell would be dropped silently: `BaseETL.standardize_missing_values` blanks it before this module sees it, so the row is skipped instead of emitted as `suppressed` `Y`. No registered file has that cell. Follow-up issue #56 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/56) tracks that drop. It is related to issues #49 and #52.

A ratio of `0:01`, `0:1`, `00:01`, or `0:00` is omitted by the existing `ratio > 0` check. A `:00` or `:00:00` denominator is accepted only when the numerator is 0, so `15:00` is not read as 15. The registered files contain only `N:1`, `N:01`, `N:01:00`, `0:00`, and `00:01` (KYRC24 through KYRC26 use `N:01` and `0:01`). The 110 KYRC26 zero rows are published as `0:01`. They are not applicable: zero membership, or teachers counted outside normal reporting. They are preschools, career and technical centers, alternative schools, and KSB, KSD, and Model, not regular A1 schools, so omitting them is correct under notes/8. A published 1 is kept: Lighthouse Academy `571016`, Warren County, school type `A5`, ratio `1:01`. Model Elementary `606450` and Model High School `606460` publish `0:01`, as do Kentucky School for the Blind `602176` and Kentucky School for the Deaf `603390`. The glossary says those four schools' teacher counts are outside the state total. This pull request does not rebuild that total. Counts of zero source rows: 2019 has 111, 2020 has 29, 2021 has 91, 2022 has 89, 2023 has 110, 2024 has 110, 2025 has 111, and 2026 has 110.

No registered ratio contains a comma. `config/mappings.yaml` casts `ratio` to `str`, not to Int64, so the pandas 3 comma bug does not drop a current cell. `parse_ratio` still strips a comma on object and on StringDtype before the match, and it does not use `bool(pd.NA)`. pandas is unpinned. pandas 2.3.3 and pandas 3.0.6 write the same 11,882 rows apart from `last_updated`. The production `mappings.yaml` config was the config for both runs.

The 2019-20 file stores seven ratios as `N:01:00`. The old parser did not match that shape, logged them as invalid, and wrote no KPI row. Neighboring years use `N:01` or `N:1` for the same schools, and the numerator is the published ratio. Those seven rows are the only prior-year change:

| School code | School | District | Published cell | Stored value |
| --- | --- | --- | --- | --- |
| `011030` | APEX Academy | Anderson County | `28:01:00` | 28 |
| `015070` | Ballard Co. Preschool/Headstart Center | Ballard County | `42:01:00` | 42 |
| `017009` | Bardstown Early Childhood Center | Bardstown Independent | `33:01:00` | 33 |
| `021012` | BAVEL | Barren County | `48:01:00` | 48 |
| `061031` | Breathitt Regional Juvenile Det Ctr | Breathitt County | `5:01:00` | 5 |
| `295150` | Cordia School | Knott County | `14:01:00` | 14 |
| `591225` | Whitley County Alternative School | Whitley County | `12:01:00` | 12 |

| Year | Base KPI rows | This change | Zeros still omitted |
| --- | --- | --- | --- |
| 2019 | 1,533 | 1,533 | 111 |
| 2020 | 1,521 | 1,528 | 29 |
| 2021 | 1,502 | 1,502 | 91 |
| 2022 | 1,506 | 1,506 | 89 |
| 2023 | 1,501 | 1,501 | 110 |
| 2024 | 1,493 | 1,493 | 110 |
| 2025 | 1,497 | 1,497 | 111 |
| 2026 | — | 1,322 | 110 |

The base total with the 2019 workbook is 10,553. This change is 11,882. 10,553 + 7 restored 2020 rows + 1,322 KYRC26 rows = 11,882. A cell-by-cell compare of the shared keys, on pandas 2.3.3 and on pandas 3.0.6, found no value, district, school-name, or suppression change outside those 7 rows. Without `openpyxl` the 2019 workbook is skipped and the total is 10,349. 11,882 − 1,533 = 10,349.

KYRC26 source rows are 1,432. KYRC25 source rows are 1,608. KYRC25 has 173 codes that end in `000`. All 173 have `School Name` `All Schools`, and all 173 have a positive ratio. One of them is the state row `999000`, ratio `15:01`. The other 172 are district totals. 173 − 1 = 172. Thirteen other KYRC25 school codes are absent from KYRC26, and ten school codes are new. None of the ten end in `000`. 1,608 − 173 − 13 + 10 = 1,432. The six removed schools with a positive ratio are Wurtland Middle `221024` (10), Wurtland Elementary `221110` (10), Young Elementary `275374` (9), Arlie Boggs Elementary `331210` (11), GEO International High `571245` (15), and Rivendell High `571305` (7). The four added schools with a positive ratio are The Anderson County Learning Academy `011030` (17), Mary E. Britton Middle `165220` (11), Greenup County School of Innovation `221015` (10), and Metcalfe County Academy `425015` (17). 1,497 − 173 − 6 + 4 = 1,322. The other seven removed schools and the other six added schools are zeros, so they do not move the KPI count. 111 − 7 + 6 = 110, and 1,432 − 110 = 1,322.

`CO-OP` on KYRC26 uses `GLEC` where KYRC25 used `GREC`. KYRC25 has 169 `GREC` rows. 167 of those school codes remain and become `GLEC`. The co-op code stays `909`. The other two `GREC` rows are removed: the Jefferson County district total `275000` and Young Elementary `275374`. 169 − 2 = 167. KYRC26 has 167 `GLEC` rows and no `GREC` rows. KYRC26 has 3 blank `CO-OP` cells. KYRC25 has 5. Nothing in this pipeline joins on the co-op name. Fern Creek High `275012` is 14 in both years. KYRC25 stores co-op `GREC` and NCES id `210299000000`. KYRC26 stores `GLEC` and `210299000628`.

Bryan Station High School `165170`, the KYRC26 school row: ratio `15:01`, district Fayette County, state school id `034165170`, NCES id `210186000388`, co-op `CKEC`, co-op code `901`, school type `A1`, county `FAYETTE`, KPI year `2026` from school year `20252026`. Adair County High School `001010` is `15:01`, NCES id `210003000001`, co-op `GRREC`, co-op code `902`. KYRC25 Bryan Station is also 15, and its NCES id is the published `210186000000`. KYRC25 Adair is 14, NCES id `210003000000`. KYRC24 state `999000` is 15, Bryan Station is 15, and Adair is 14. KYRC25 Fayette district `165000` is 13, NCES id `2101860`, co-op `CKEC`. There is no `165000` row and no `999000` row in KYRC26. `School Name` `All Schools` is stored as `---District Total---` when it is present. KYRC26 has no such name.

The 2019-2023 state row is district `State` and school name `---State Total---`, school code `999`. The ratios are 15, 16, 16, 15, and 15 for 2019 through 2023. `---State Total---` is left as written. Fayette County's district ratio in those files is 14, 15, 14, 14, and 13, on school code `165`. The 2023 Fayette total is in the source file four times: three copies are `0:01` and one is `13:01`. The output keeps 13. Corbin Independent `133` is in that file four times (`0:01`, `15:01`, `3:01`, `412:01`) and the output keeps 15, 3, and 412. Floyd County `175` is in it four times (`5249:01`, `14:01`, `0:01`, `3:01`) and the output keeps 5249, 14, and 3. Those duplicate district totals predate KYRC26. They are a source defect. KYRC23's choice of 412 and 5249 is one of the copies, not a reason to drop the others here.

The file has no demographic column. `student_group` is `All Students` on every row. The base demographic check still warns that the race labels are absent. That warning is the shared required-label list. This file has never had those columns. No new KPI names. `KPIS.md` states the glossary definition, lists KYRC24, KYRC25, KYRC26, the 2020-2023 files, the 2018-19 workbook, and years 2019-2026.

KYRC26's largest published ratio is Gateway Academy High School `205012`, Graves County, `116:01`. Cloverport Independent School `132030` is an A1 school at `115:01`. Both are stored. The 2023 Floyd County district total of 5,249 was already stored by the base parser and is unchanged. Follow-up issue #56 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/56) tracks the source outliers this pull request leaves as published: Corbin Independent district `133000` is `483:01` in 2025, and Cloverport Independent district `132000` is `86:01` in 2024. It also tracks the 2023 duplicate district totals and the silent `*` drop.

Fixtures are real rows in `tests/fixtures/student_teacher_ratio/`: three KYRC26 school rows, five KYRC25 rows, three KYRC24 rows, and the 2020-2023 anchors, including BAVEL `48:01:00` and the Breathitt `20022023` row. `tests/test_fixture_sizes.py` passes.

Issue #42 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/42): `scripts/generate_report_drafts.py` selects state rows with `district == 'State'`, and `get_state_average` also requires `school_name == '---District Total---'`. Student-teacher ratio is not in that script's indicator list. If it were added, no year would match both conditions. The 2019-2023 state rows are district `State` and school name `---State Total---` (school code `999`). KYRC24 and KYRC25 store district `All Districts` and school name `All Schools`, which this parser stores as `---District Total---` (school code `999000`). KYRC26 has no state row. `analysis/scripts/base_analysis_dataset.py` reads `student_teacher_ratio`, drops `---District Total---`, and does not select district `State`. A `---State Total---` row would pass that filter. KYRC26 adds school rows only. `html/generate_dashboard_data.py` does not read this metric.

`openpyxl` is not a declared dependency. Without it, reading `STUDENT_TEACHER_RATIO_18-19.xlsx` raises, `BaseETL.process` logs that error and continues, and 2019 is absent from the output. The workbook has 1,644 rows, school year `20182019`, and ratio text such as `15:1`. The state ratio is 15. Dependencies were not changed.

Lessons for later agents:

1. HEAD the registered URLs before assuming a temporary file went 404. Every registered URL returned 200. The guessed OAA names are 404 and were never registered. Do not add a skip for a filename this directory has never used. `KYRC23_OVW_Student_to_Teacher_Ratio.csv` returns 200 and is the 2022-23 file again. Do not register it. `accepts_data_file` skips the name. The historical KYRC24 URL still returns 200. Leave it. The datasets KYRC24 copy matches the ratios and only repairs `CO-OP`.
2. The measure is students per one teacher. KDE's glossary and 703 KAR 5:140 say membership divided by FTE teachers, excluding administrators, counselors, and media specialists. The glossary also excludes resource teachers, and it excludes the Kentucky School for the Blind, the Kentucky School for the Deaf, and the Model Laboratory schools from the state total. Store the published number before the colon. Do not divide membership by a teacher count. These files do not have those columns.
3. KYRC26 has no state row and no district `All Schools` row. Fixture a school whose code does not end in `000`. Bryan Station `165170` is 15. Do not rebuild the 172 district totals or the state total. This module stays S because those totals were not rebuilt.
4. "Header changes: none" means the KYRC26 header matches KYRC25. pandas is unpinned. No registered ratio contains a comma, and `ratio` is cast to `str`, not Int64. `dtype=str` is StringDtype on pandas 3. The `48:01:00` shape is a real ratio. A `:00` or `:00:00` denominator is accepted only when the numerator is 0, so `15:00` is not 15. 2020 is 1,528 rows. The unfixed file was 1,521 and was missing the seven schools in the table above. 2019, 2021, 2022, 2023, 2024, and 2025 are unchanged: 1,533 / 1,502 / 1,506 / 1,501 / 1,493 / 1,497.
5. This file has no Suppressed column and no blank ratio. A blank row is not in these files. The parser skips one. Every KPI row is `suppressed` `N`. A published zero is omitted by `ratio > 0`. That is 111 / 29 / 91 / 89 / 110 / 110 / 111 / 110 source rows from 2019 through 2026. The 110 KYRC26 zeros are published `0:01`. They are not applicable: zero membership, or teachers counted outside normal reporting. They are preschools, career and technical centers, alternative schools, and KSB, KSD, and Model, not regular A1 schools, so omitting them is correct under notes/8. A ratio of 1 is published. A `*` is not in any registered file. If one appeared, `BaseETL.standardize_missing_values` would blank it before this module sees it, and the row would be skipped instead of emitted as `suppressed` `Y`. That silent drop, Corbin Independent district 483 in 2025 (`133000`), and Cloverport Independent district 86 in 2024 (`132000`) are follow-up issue #56 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/56). The 2023 file repeats three district totals with conflicting ratios. Corbin `133` emits 15, 3, and 412. Floyd `175` emits 5,249, 14, and 3. That is a source defect. Do not treat KYRC23's single copy as the row to keep. Issue #56 tracks those duplicates as well.
6. Compare the code sets, then the counts. KYRC26 is smaller than KYRC25 by 176 source rows and 175 KPI rows, and the arithmetic above adds up. 1,608 − 173 − 13 + 10 = 1,432. 1,497 − 173 − 6 + 4 = 1,322. The 173 codes that end in `000` are the state row plus 172 district totals, not co-op totals `901`–`909`.
7. Prior-year identity against the base code is 1,533 / 1,521 / 1,502 / 1,506 / 1,501 / 1,493 / 1,497 for 2019-2025, on pandas 2.3.3 and on pandas 3. The only cells that change are the seven 2020 rows in the table above. Published state, Fayette, Bryan Station, and Adair values for the rows the base already wrote are the file values, including 15, 13, and 16. KYRC26 is new: 1,322 rows, Bryan Station 15.
8. State district is `All Districts` for 2024 and 2025 and `State` for 2019-2023. School name `All Schools` is stored as `---District Total---`. `---State Total---` is left as written. KYRC26 has neither row. Issue #42: `scripts/generate_report_drafts.py` selects `State` and also requires `---District Total---`. Student-teacher ratio is not in the indicator list. Neither shape would match both conditions, and 2026 has no state row. The analysis dataset drops `---District Total---` and would keep `---State Total---`.
9. `openpyxl` is not a declared dependency. Without it, the 2019 workbook is skipped and the run continues. The total is then 10,349. Dependencies were not changed.
10. No shared parser or dependency changes. pandas is unpinned. pandas 2.3.3 and pandas 3.0.6 write the same 11,882 rows.

### `novice_teachers` — S — in review in PR #57

| | |
| --- | --- |
| KYRC26 files | `KYRC26_OVW_Inexperienced_Teachers.csv`, `KYRC26_OVW_Students_Taught_by_Inexperienced_Teachers.csv` |
| Header changes | None |
| Fixtures / assertions | State `999000` row on each file, year `2026`. District `All Districts`. Institutional rates 5.60 and 15.30. Equity All Students 65.8 / 34.2 / 31.6 |
| Gotchas | The same inexperienced-teachers student file is also registered under `students_taught_by_inexperienced_teachers` for download only. This ETL reads the `novice_teachers` copy. Leave that other directory alone |

Result, checked 18 October 2026 (UTC) against the public `datasets` container and the historical host. `include_kyrc26 = True` is the parser change, plus the 2018-19 workbook and a filename skip for the two KYRC23 republishes. A blank 2018-19 percent with zero new teachers is stored as 0.0. `etl/kyrc.py`, `BaseETL`, and the demographic config were not edited. `openpyxl` is declared in `pyproject.toml`. `analysis/scripts/base_analysis_dataset.py` requires both split rates before it sums them. Re-HEAD both KYRC26 files after 26 October 2026. If a hash diverges from the stamp below, take the newer stamp and record which container it came from.

**An inexperienced teacher has 0-3 years of teaching experience.** KDE's report-card glossary says that. A teacher in the first year is a New Teacher. The 2021-22 report-card change note says the breakdown is less than one year and between one and three years. The quality-assurance worksheet says the counts come from the Kentucky Educator Credentialing System. The students-taught file is the comparison of students taught by those teachers, by Title I status. The two teacher rates are stored separately. This module does not add them into one total.

The KPI names are `novice_teacher_rate_less_than_1_year`, `novice_teacher_rate_1_to_3_years`, `students_taught_by_inexperienced_teachers_rate_title_1`, `students_taught_by_inexperienced_teachers_rate_not_title_1`, and `students_taught_by_inexperienced_teachers_rate_equity_gap`. No other module emits those names. `kpi_master` concatenates without deduping, so a second module must not emit them. `students_taught_by_inexperienced_teachers` in `config/kde_sources.yaml` is a download registration only. It has no ETL. This pull request does not change that directory.

Every registered URL returned 200 on 18 October 2026 (UTC). KYRC26 institutional on `datasets` is Last-Modified `Thu, 15 Oct 2026 10:00:10 GMT`, 205,136 bytes, ETag `0x8DF2AA3198BEDB6`, MD5 `92528e45b4aa5f29269bd5794be0d328`. That host does not send Content-MD5. The UAT copy is the same length and the same MD5 (Content-MD5 `klKORbSqXykmm9V5S+DTKA==`), Last-Modified `Sat, 10 Oct 2026 06:01:54 GMT`. KYRC26 equity on `datasets` is Last-Modified `Thu, 15 Oct 2026 10:00:11 GMT`, 321,011 bytes, ETag `0x8DF2AA319C58F85`, MD5 `b3d64712c06852ed7460b9f2b72306f1`. The UAT copy matches that hash (Content-MD5 `s9ZHEsBoUu10YLnytyMG8Q==`), Last-Modified `Sat, 10 Oct 2026 06:02:28 GMT`. KYRC25 institutional on `datasets` is Last-Modified `Sat, 18 Jul 2026 21:29:47 GMT`, 204,643 bytes, MD5 `63c43bdeb3402a340eaa55fe7be34019`. KYRC25 equity is the same Last-Modified, 321,878 bytes, MD5 `96156f26bf27793be2908ff0dd317e73`. The historical and UAT copies of both KYRC25 files match those hashes. The historical KYRC24 institutional file is Last-Modified `Thu, 29 May 2025 22:07:03 GMT`, 203,815 bytes, MD5 `7c946bab73e4f368ec5f9306102992db`. The historical KYRC24 equity file is Last-Modified `Thu, 29 May 2025 22:09:21 GMT`, 306,713 bytes, MD5 `9168e6d6572692925a269885dabdc7dd`. `inexperienced_teachers_2020.csv` through `2023.csv`, `students_taught_by_inexperienced_teachers_2020.csv` through `2023.csv`, and `NEW_TEACHER_COUNT_18-19.xlsx` (Last-Modified `Sat, 19 Sep 2026 14:08:32 GMT`, 167,610 bytes, MD5 `fcb2e75e745cdf447659aac8fed68888`) returned 200 on the historical host. `KYRC22_OVW_Inexperienced_Teachers.csv` is 404. The guessed temporary name `Inexperienced_Teachers_2026.CSV` is 404 on the OAA host. None of those names was registered. There is no leftover temporary filename to skip.

`KYRC23_OVW_Inexperienced_Teachers.csv` and `KYRC23_OVW_Students_Taught_by_Inexperienced_Teachers.csv` return 200 and are not registered. Both are the 2022-23 extract again. School year is `20222023` on every KYRC23 row (institutional 1,554 rows, equity 2,015 rows). After the historical 3-digit district codes are written as `NNN000`, the institutional school-code set matches `inexperienced_teachers_2023.csv`. New-teacher counts match on all 1,554 rows. Teacher counts differ on 336 rows, and the 1-3 year percent differs by more than 0.05 on 368 rows. The KYRC23 state row `999000` is 44,444 teachers, counts `6,684` and `3,079`, rates 15.00 and 6.90. The registered historical state row is district `State`, school code `999`, rates 15.48 and 7.13. The historical equity gap for Adair County `001000` All Students is `-69.53`. KYRC23 stores `69.5`. The historical file types Breathitt County `061` as school year `20022023` on the institutional row and on all three equity title rows. KYRC23 corrects those cells to `20222023`. `extract_year` already keeps the ending year `2023` from `20022023`. Registering both copies would write 2023 twice, with different rates. `accepts_data_file` skips both KYRC23 names even if they are dropped into the raw directory. A full run with those files present emitted no KYRC23 rows.

A `datasets` copy of `KYRC24_OVW_Inexperienced_Teachers.csv` also returns 200 (204,471 bytes, Last-Modified `Thu, 14 May 2026 19:11:30 GMT`, MD5 `1aba31d794bbc5de788b084d5c0ddfaa`). Rates, teacher counts, and the new-teacher counts match the registered historical file on all 1,591 school codes. `CO-OP` differs on 168 rows. The registered historical URL still returns 200, so this pull request does not swap it. The datasets copy of the KYRC24 equity file also returns 200 (317,938 bytes, same Last-Modified, MD5 `e0cf730225ef7ab3a010ec6142b7b080`). School codes and titles match. All Students differs on 1,581 of 2,022 rows, median absolute difference 36.8. State Title I / Not Title I / Equity Gap All Students is 11.3 / 1.3 / 9.9 on the registered historical file and 66.0 / 35.4 / 30.6 on the datasets copy. Bryan Station High `165170` Title I All Students is 0.0 on the historical file and 86.0 on the datasets copy. Journal 20's 66.0 and 35.4 figures are the datasets file. Swapping would change the published 2024 equity values. The historical URL still returns 200, so it stays. The tests anchor 11.3, 1.3, 9.9, and Bryan Station 0.0. The historical KYRC25 files match the registered datasets bytes, so 2024-25 is not a missing year.

There is no Suppressed column. No registered year has a `*` cell. Every KPI row is `suppressed` `N` (143,209 rows). There is no suppressed group in these files. A constructed `Y` row is kept with an empty value, which is the notes/8 shape. A blank institutional row, both rates empty and the row not suppressed, emits nothing. KYRC26 and KYRC25 have no blank rates. The 2022 file has four blank district totals, and the base parser already emitted nothing for them:

| School code | School | District | Teacher count | Rates |
| --- | --- | --- | --- | --- |
| `602` | `---District Total---` | Kentucky School for the Blind District | 26 | blank |
| `603` | `---District Total---` | Kentucky School for the Deaf District | 39 | blank |
| `606` | `---District Total---` | Model Laboratory Schools at EKU | 57 | blank on one of the two `606` rows |
| `777` | `---District Total---` | Kentucky Tech System | blank | blank |

The other `606` row publishes 14.04 and 5.26 and is kept once. 1,396 source rows minus 4 blank rows is 1,392, times 2 rates is 2,784. That is the 2022 institutional count before and after this change.

An equity-gap cell below zero is dropped by the existing `value >= 0` check. 2020-2023 and KYRC26 publish a signed gap, Title I minus Not Title I, so a negative cell is a real published difference. KYRC23, KYRC24, and KYRC25 publish the absolute value, which is why the registered 2024 and 2025 files have no negative cells. The drop predates KYRC26. This pull request leaves both behaviors in place. Issue #59 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/59) tracks storing a signed gap for every year, including the 804 dropped negatives (184 + 188 + 155 + 176 + 101) and the 157 absolute-value cells in 2024-25 whose direction is hidden. Issue #58 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/58) tracks switching the registered KYRC24 equity file to the datasets copy. The test records the current cells so a later change does not silently mark them `suppressed` `Y`. Dropped negative cells, all on Equity Gap rows: 2020 has 184 of 18,072 (emitted 17,888), 2021 has 188 of 18,072 (17,884), 2022 has 155 of 18,090 (17,935), 2023 has 176 of 18,135 (17,959), and 2026 has 101 of 18,171 (emitted 18,070). KYRC26 Adair County district `001000` is negative on all nine demographics (All Students `-35.5`), so that status emits nothing. Title I 57.6 and Not Title I 93.1 are kept. Bardstown Independent `017000` Equity Gap All Students is 46.6 and English Learner is `-3.6`. Only the English Learner cell is dropped, and the row stays `suppressed` `N`.

A published 0 is kept. James E Bazzell Middle School `005015`, Allen County, is 20.00 for 1-3 years and 0.00 for less than 1 year. KYRC26 institutional zeros are 214 of the 1-3 year percent and 497 of the less-than-1-year percent. Percent columns are rates: on KYRC26, the published percent matches count divided by teacher count within 0.15 on every row, no count exceeds the teacher count, and no percent exceeds 100.

The only commas in the registered files are the state new-teacher counts. KYRC26 state counts are `6,016` and `2,217` (teacher count `39242`). KYRC25 state counts are `6,414` and `2,347` (teacher count `41230`). KYRC24 state counts are `6,291` and `2,339` (teacher count `40598`). Those counts are not KPI values, and the rate columns have no commas. The comma strip therefore does not change a published row. It keeps a comma-formatted count alive in the unit test, on object and on StringDtype, before the Int64 cast. `config/mappings.yaml` now names `new_teachers_1_to_3_years` and `new_teachers_less_than_1_year`, matching the columns the ETL writes. The old keys used the source header text and never matched, so that cast did not run. Aligning the keys also does not change a published rate. pandas is unpinned. `bool(pd.NA)` is not used. pandas 2.3.3 and pandas 3.0.6, both with the production `mappings.yaml` config, write the same 143,209 rows apart from `last_updated`.

The 2018-19 workbook has no school code. A school code is the 3-digit district number plus the 3-digit school number. A blank school number is the district total `NNN000`. The state district `999` becomes `999000`. Later historical files keep their published codes, including the 3-digit state code `999`. The cover sheet calls `NEWPCT` the percentage of new teachers. That is the first-year rate. The file has 1,636 rows, school year `20182019`. `NEWPCT` is blank on exactly 448 rows. Each of those rows has `NEWTEACHERS` 0 and `TEACHERS` above 0. On every other row the percent equals new teachers divided by teachers, within a tenth of a point. Those 448 blanks are 0 percent, stored as 0.0 with `suppressed` `N`. 2019 therefore writes 1,636 KPI rows, all `novice_teacher_rate_less_than_1_year`. Dropping them had left 1,188 rows and raised the school median from 5.0 to 7.7. The state rate is `7.10346186` (40,614 teachers, 2,885 new teachers), district `State`, school name `---State Total---`. Fayette County `165000` is 7.2. Bryan Station High `165170` is 7.5. Adair County High `001010` is 1.9. Adair Learning Academy `001012` has 5 teachers and 0 new teachers, and the KPI value is 0.0. A blank percent without a zero new-teacher count still emits nothing. No other registered year has that zero-count blank. `analysis/scripts/base_analysis_dataset.py` sums the two rates with `min_count=2`. 2019 has only the first-year rate, so the combined series is NaN for that year. The first-year metric is unchanged. Where both rates exist, the combined value is the same sum as before. Schools with neither rate had been stored as 0 by `skipna=True` summing two missing cells. That 0 was a fabricated novice rate. Those 3,544 rows are now NaN, after district totals are dropped: 1,478 in 2020, 1,478 in 2021, 260 in 2022, 102 in 2023, 71 in 2024, 82 in 2025, and 73 in 2026. 1,478 + 1,478 + 260 + 102 + 71 + 82 + 73 = 3,544. `impute_missing_values` later mean-imputes a missing `novice_teacher_rate`. The fabricated 0 was not missing, so it was not imputed. Graduation and postsecondary-enrollment analyses keep years 2021-2025, so this changes the 2021 inputs to those two analyses. The 2022-2025 neither-rate schools in that same window are also mean-imputed instead of 0. The 1,478 fabricated 2020 zeros, and the 73 in 2026, are outside that window.

The 2020 institutional file has `TEACHER COUNT` and `TOTAL NEW TEACHERS` and no percent. It has 1,627 rows, school year `20192020`. The state row is 40,742 teachers and 7,444 new teachers, district `State`, school code `999`. That count is not the 2019 new-teacher count (2,885). It is not mapped. The file emits 0 institutional KPI rows, before and after this change. The 2021 file adds `PERCENT INEXPIENCED TEACHERS`. The percent equals new teachers divided by teacher count on every non-blank row (0 mismatches). The state row is 42,526 teachers, 7,745 new teachers, 18.21 percent. 59 percents are blank. That column is one combined inexperienced rate, not either split KPI, so it stays unmapped. The file emits 0 institutional KPI rows. A follow-up should decide whether 2020's count and 2021's combined rate belong on a different metric.

| Year | Base KPI rows | This change | What the year contains |
| --- | --- | --- | --- |
| 2019 | 0 | 1,636 | First-year rate from the workbook, including 448 zeros |
| 2020 | 17,888 | 17,888 | Equity only. Institutional file has no rate |
| 2021 | 17,884 | 17,884 | Equity only. Institutional file is one combined rate |
| 2022 | 20,719 | 20,719 | Institutional 2,784 + equity 17,935 |
| 2023 | 21,067 | 21,067 | Institutional 3,108 + equity 17,959 |
| 2024 | 21,380 | 21,380 | Institutional 3,182 + equity 18,198 |
| 2025 | 21,391 | 21,391 | Institutional 3,166 + equity 18,225 |
| 2026 | — | 21,244 | Institutional 3,174 + equity 18,070 |

The base total is 120,329. This change is 143,209. 120,329 + 1,636 + 21,244 = 143,209. A cell-by-cell compare of 2020-2025, against output from the base code on the registered files, found no value, district, school-name, or suppression change (120,329 shared keys, 0 differences). `openpyxl` is declared in `pyproject.toml`, so a normal install reads the 2019 workbook.

KYRC26 institutional source rows are 1,587. KYRC25 institutional source rows are 1,583. Nineteen school codes are new and fifteen are gone. 1,583 − 15 + 19 = 1,587. Neither year has a blank percent, so the KPI counts are 1,583 × 2 = 3,166 and 1,587 × 2 = 3,174. The added codes are `011030`, `035160`, `147120`, `165220`, `221015`, `301155`, `425015`, `426023`, `551023`, `551025`, `555025`, `571006`, `602000`, `602176`, `777116`, `777124`, `777129`, `777139`, and `777153`. The removed codes are `121015`, `131016`, `134011`, `221024`, `221110`, `275374`, `275463`, `275465`, `331210`, `523022`, `571245`, `571300`, `571305`, `593040`, and `606450`. District numbers go from 174 to 175. The added district is `602`, Kentucky School for the Blind. `602000` is the district total and `602176` is the school, school type `D1`.

KYRC26 equity source rows are 2,019. KYRC25 equity source rows are 2,025. School codes go from 1,669 to 1,663: 10 added, 16 removed. 1,669 − 16 + 10 = 1,663. The removed codes contribute 16 rows and the added codes contribute 10. 2,025 − 16 + 10 = 2,019. The added codes are `011030`, `165137`, `165220`, `221015`, `251025`, `365025`, `425015`, `431130`, `555025`, and `571006`. The removed codes are `121015`, `131016`, `135022`, `146035`, `165904`, `165905`, `221024`, `221110`, `275374`, `331210`, `571245`, `571300`, `571305`, `777156`, `777158`, and `777159`. Nineteen shared codes switch between a single Title I row and a single Not Title I row (11 Not Title I to Title I, 8 Title I to Not Title I) and do not change the row count. Title counts go from 1,299 / 548 / 178 to 1,299 / 542 / 178. The removed rows are 5 Title I and 11 Not Title I. The added rows are 2 Title I and 8 Not Title I. 1,299 − 5 + 2 + 11 − 8 = 1,299. 548 − 11 + 8 − 11 + 8 = 542. Equity Gap stays 178. KPI rows go from 18,225 to 18,070 because of the 101 dropped negative gap cells. 2,025 × 9 = 18,225. 2,019 × 9 − 101 = 18,070. Title I is 1,299 × 9 = 11,691. Not Title I is 542 × 9 = 4,878. Equity Gap is 178 × 9 − 101 = 1,501. 11,691 + 4,878 + 1,501 + 3,174 = 21,244.

`CO-OP` on KYRC26 uses `GLEC` where KYRC25 used `GREC`. The co-op code stays `909`. On the institutional file, KYRC25 has 168 `GREC` rows. Three of those codes are removed (`275374`, `275463`, `275465`). The other 165 become `GLEC`. 168 − 3 = 165, and KYRC26 has 165 `GLEC` rows and no `GREC` rows. On the equity file, KYRC25 has 171 `GREC` rows across 169 codes. Young Elementary `275374` is removed. The other 170 rows become `GLEC`. Nothing in this pipeline joins on the co-op name. Camp Taylor Elementary `275004` is co-op `GREC` in KYRC25 and `GLEC` in KYRC26, code `909` both years. Its KYRC26 NCES id is `210299000623`.

State `999000`, district All Districts, school `All Schools` stored as `---District Total---`, school year `20252026`, KPI year `2026`, suppressed `N`. Institutional rates are 15.30 for 1-3 years (count `6,016`) and 5.60 for less than 1 year (count `2,217`). Teacher count is 39,242. `CO-OP` is blank. Equity All Students is Title I 65.8, Not Title I 34.2, Equity Gap 31.6. Title I White is 64.9, displayed as `White (non-Hispanic)`. Fayette County `165000` is 2,919 teachers, 10.50 (`306`) and 5.10 (`149`), NCES id `2101860`, co-op `CKEC`, co-op code `901`. Bryan Station High `165170` is 133 teachers, 9.80 (`13`) and 3.80 (`5`), state school id `034165170`, NCES id `210186000388`, school type `A1`, county `FAYETTE`, co-op `CKEC` / `901`. Equity Title I All Students is 80.0. Adair County High `001010` is 54 teachers, 9.30 and 9.30, state school id `001001010`, NCES id `210003000001`, co-op `GRREC` / `902`. Equity Title I All Students is 59.1.

Prior-year anchors, unchanged against the base output: KYRC25 state 5.7 and 15.6, equity state 66.5 / 40.5 / 26.0, Bryan Station equity Title I 71.3. KYRC24 state 5.8 and 15.5, Bryan Station rates 8.3 and 8.3, equity state 11.3 / 1.3 / 9.9, Bryan Station equity Title I 0.0. 2023 state school id `999`, district `State`, school name `---State Total---`, rates 7.13 and 15.48. Breathitt County `061` is 13.04 and 20.0, and the source year cell `20022023` still yields KPI year 2023. 2022 state is 4.91 and 17.81. 2020 equity state Title I All Students is 55.65, district `State`, school name `---State Total---`. 2023 equity state Title I is 26.16.

The equity files have no demographic column that matches the shared required race list, so the base check still warns that those labels are absent and that `Non-White` and `Student without Disabilities (IEP)` are unexpected. That warning is the shared required-label list. These files have used those labels since 2020. The 2019 workbook has no demographic column, so `student_group` is `All Students` and the same check warns that `White (non-Hispanic)` is missing. This pull request does not change that list.

Fixtures are verbatim real rows in `tests/fixtures/novice_teachers/`, including the KYRC26 state, Fayette, Bryan Station, Adair, and zero-rate rows, the 2022 blank district totals, the Breathitt `20022023` row, and a CSV export of the 2018-19 state and school rows. `tests/test_fixture_sizes.py` passes. The 2019 fixture is the CSV excerpt, not the xlsx.

Issue #42 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/42): `scripts/generate_report_drafts.py` selects state rows with `district == 'State'`, and `get_state_average` also requires `school_name == '---District Total---'`. These metrics are not in that script's indicator list. The 2019-2023 state rows are district `State` and school name `---State Total---`. KYRC24 through KYRC26 store district `All Districts` and school name `All Schools`, which this parser stores as `---District Total---`. Neither shape matches both conditions. `html/generate_dashboard_data.py` does not read these metrics.

`openpyxl` is declared in `pyproject.toml`. `pd.read_excel` needs it for `NEW_TEACHER_COUNT_18-19.xlsx`. Without the package, `BaseETL.process` logs the error and continues, and 2019 is absent (141,573 rows). A normal install includes it.

Lessons for later agents:

1. HEAD the registered URLs before assuming a temporary file went 404. Every registered URL returned 200. `KYRC22_OVW_Inexperienced_Teachers.csv` is 404, and the guessed OAA name `Inexperienced_Teachers_2026.CSV` is 404. Neither was registered. Do not add a skip for a filename this directory has never used. Both KYRC23 inexperienced-teacher files return 200 and are the 2022-23 extract again. Do not register them. `accepts_data_file` skips both names. The historical KYRC24 URLs still return 200. Leave them. The datasets KYRC24 institutional copy matches the rates and differs on `CO-OP`. The datasets KYRC24 equity copy changes All Students on 1,581 rows. State Title I is 11.3 on the registered file and 66.0 on the datasets copy.
2. The measure is teachers with 0-3 years of experience, split into less than 1 year (a New Teacher) and 1 to 3 years. Store the published percents. Do not add the two rates into one KPI. The 2018-19 `NEWPCT` column is the first-year rate only. The 2020 file is a count with no rate. The 2021 `PERCENT INEXPIENCED TEACHERS` column is one combined rate. Leave those columns unmapped. A follow-up should decide whether they belong on a different metric.
3. KYRC26 has a state row `999000` on each file, district `All Districts`, school name `All Schools` stored as `---District Total---`, year `2026`. Institutional rates are 5.60 and 15.30. Equity All Students is 65.8, 34.2, and 31.6. The same student file stays registered under `students_taught_by_inexperienced_teachers` for download only. This ETL reads the `novice_teachers` copy. Leave that other directory alone.
4. "Header changes: none" means the KYRC26 header matches KYRC25. pandas is unpinned. `dtype=str` is StringDtype on pandas 3. The comma strip treats StringDtype the same as object. It does not change a published row. The rate columns have no commas. The only commas are the state new-teacher counts (`6,016` and `2,217` in KYRC26), and those counts are not KPI values. The unit test is what requires the strip. pandas 2.3.3 and pandas 3.0.6 write the same 143,209 rows.
5. These files have no Suppressed column and no `*` cell. Every KPI row is `suppressed` `N`. A blank row that is not `Y` emits nothing. The 2022 blank district totals are `602`, `603`, one of the two `606` rows, and `777`. Their counts are blank, so they are not zeros. A published 0 is kept. A blank 2018-19 `NEWPCT` with `NEWTEACHERS` 0 and `TEACHERS` above 0 is 0.0. That is 448 rows. 2019 writes 1,636. Adair Learning Academy `001012` is 0.0. A negative equity-gap cell is dropped by `value >= 0`. 2020-2023 and KYRC26 publish a signed gap. KYRC23, KYRC24, and KYRC25 publish the absolute value. Issue #59 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/59) tracks storing the signed gap. Issue #58 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/58) tracks the registered KYRC24 equity copy. This pull request does not change either behavior. KYRC26 drops 101 cells and emits 18,070 equity rows. 2,019 × 9 − 101 = 18,070. Adair County `001000` Equity Gap is negative on every demographic. Bardstown Independent `017000` drops only English Learner `-3.6`.
6. Compare the code sets, then the counts. Institutional: 1,583 − 15 + 19 = 1,587 source rows, and both years emit two rates per row, so 3,166 and 3,174. Equity: 1,669 − 16 + 10 = 1,663 codes, and 2,025 − 16 + 10 = 2,019 source rows. Title I stays 1,299 because the switches and the added and removed rows cancel. Not Title I goes from 548 to 542. The KPI drop from 18,225 to 18,070 is the 101 negative gap cells. The added and removed codes are listed above. They are schools and the new Kentucky School for the Blind district, not co-op totals `901`–`909`.
7. Prior-year identity against the base code is 17,888 / 17,884 / 20,719 / 21,067 / 21,380 / 21,391 for 2020-2025, on pandas 2.3.3 and on pandas 3. No previously published cell changes. 2019 is new: 1,636 first-year rates, including 448 zeros, state `7.10346186`. KYRC26 is new: 21,244 rows, state 5.60 and 15.30. The total is 143,209.
8. State district is `All Districts` for 2024-2026 and `State` for 2019-2023. School name `All Schools` is stored as `---District Total---`. `---State Total---` is left as written. The 2019 state school code is synthesized as `999000`. The 2020-2023 state school code stays `999`. Issue #42: `scripts/generate_report_drafts.py` selects `State` and also requires `---District Total---`. These metrics are not in the indicator list. Neither shape matches both conditions. `analysis/scripts/base_analysis_dataset.py` sums the two rates with `min_count=2`. 2019 has only the first-year metric, so the combined series is NaN. Rows that have both rates keep the same sum. Schools with neither rate had been a fabricated 0. Those 3,544 rows are now NaN (2020 1,478, 2021 1,478, 2022 260, 2023 102, 2024 71, 2025 82, 2026 73) and are mean-imputed. That changes the 2021 inputs to the graduation and postsecondary-enrollment analyses. The old 0 was a fabricated novice rate.
9. `openpyxl` is a project dependency in `pyproject.toml`. A normal install reads the 2019 workbook. Without the package, `BaseETL.process` logs the error and continues, and the total is 141,573.
10. Shared changes are the `openpyxl` dependency and `analysis/scripts/base_analysis_dataset.py` (`sum(min_count=2)`). `BaseETL` was not edited. The comma strip stays in this module and does not change a published row. pandas is unpinned. pandas 2.3.3 and pandas 3.0.6 write the same 143,209 rows. The combined-rate change leaves 3,544 neither-rate schools as NaN instead of a fabricated 0 (2020 1,478, 2021 1,478, 2022 260, 2023 102, 2024 71, 2025 82, 2026 73). Those NaNs are mean-imputed, which changes the 2021 inputs to the graduation and postsecondary-enrollment analyses. The old 0 was a fabricated novice rate. The ETL output is still 143,209 rows.

### `teacher_working_conditions` — S — in review in PR #60

| | |
| --- | --- |
| KYRC26 files | `KYRC26_OVW_Teacher_Working_Conditions.csv` |
| Header changes | None. No `Demographic` column |
| Fixtures / assertions | State `999000` row, year `2026`. District `All Districts`. Percent favorable 69 / 67 / 70 for behavior, climate, and leadership |
| Gotchas | Do not require a demographic value. The value is percent favorable. A literal `N/A` is not a zero. A published 0 is kept. Four survey waves, not seven KPI years. Issues #61 and #62 |

Result, checked 18 October 2026 (UTC) against the public `datasets` container and the historical host. `include_kyrc26 = True` is the parser change, plus registration of `teacher_working_conditions_2020.csv` and a filename skip for the KYRC23 republish. `etl/kyrc.py`, `BaseETL`, and the demographic config were not edited. `openpyxl` was already declared in PR #57. This module does not read a workbook. Re-HEAD the KYRC26 file after 26 October 2026. If the hash diverges from the stamp below, take the newer stamp and record which container it came from.

**The measure is percent favorable on the Impact Kentucky working conditions survey.** KDE's report-card glossary says the Impact Value is the percentage of respondents who selected a favorable answer, most often the top two of five choices. The constructs are connected to teacher retention and student achievement. The older glossary entry "Teaching Working Conditions" uses the same definition for the 2020 survey. Store the published percent. Do not rescale it. There is no demographic column, so `student_group` is `All Students`.

The KPI names are `teacher_working_conditions_managing_student_behavior`, `teacher_working_conditions_school_climate`, `teacher_working_conditions_school_leadership`, and `teacher_working_conditions_teaching_environment`. No other module emits those names. `kpi_master` concatenates without deduping, so a second module must not emit them. 2020-2023 publish Teaching Environment Composite. KYRC24 through KYRC26 publish School Climate. This pull request keeps those as separate metrics and does not bridge them. KDE's own KYRC23 file relabels the 2023 Teaching Environment values, unchanged on all 1,539 schools, as School Climate. The 2020 Impact Kentucky topic list says "School climate". School-level correlation across the label change (0.66) is in line with same-label wave-to-wave correlations (0.67-0.73). Issue #62 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/62) tracks whether to bridge the series.

Every registered URL returned 200 on 18 October 2026 (UTC). KYRC26 on `datasets` is Last-Modified `Thu, 15 Oct 2026 10:00:11 GMT`, 436,507 bytes, ETag `0x8DF2AA319D99D3F`, MD5 `ac5bd3c79bbfeaef42ee5ce015dcb089`. That host does not send Content-MD5. The UAT copy is the same length and the same MD5 (Content-MD5 `rFvTx5u/6u9C7lzgFdywiQ==`), Last-Modified `Sat, 10 Oct 2026 06:02:52 GMT`. KYRC25 on `datasets` is Last-Modified `Sat, 18 Jul 2026 21:29:47 GMT`, 575,006 bytes, MD5 `320bef545246fcae80771648859bfb08`. The UAT copy matches that hash. The historical KYRC24 file is Last-Modified `Thu, 29 May 2025 22:07:04 GMT`, 488,583 bytes, MD5 `98cc38364a596ecae2ae9d9c7ab90666`. `teacher_working_conditions_2020.csv` through `2023.csv` returned 200 on the historical host. 2020 is Last-Modified `Fri, 09 Feb 2024 20:59:46 GMT`, 676,799 bytes, MD5 `a5ae5ec0f7f5b84fc5255497e0f8821d`. The guessed temporary names `Teacher_Working_Conditions_2024.CSV`, `Teacher_Working_Conditions_2025.CSV`, and `Teacher_Working_Conditions_2026.CSV` are 404 on the OAA host. None of those names was registered. There is no leftover temporary filename to skip.

`KYRC23_OVW_Teacher_Working_Conditions.csv` returns 200 and is not registered (625,066 bytes, Last-Modified `Thu, 14 May 2026 19:11:28 GMT`, MD5 `aeaacfd3632b3240e09a5adaebe07be2`). School year is `20222023` on all 4,617 rows. The school-code set matches `teacher_working_conditions_2023.csv`. The historical file's third construct is Teaching Environment Composite. KYRC23 relabels that construct School Climate. The Teaching Environment values are unchanged on all 1,539 schools. On the 1,225 rows where the historical percent is numeric, 1,224 School Climate cells equal the Teaching Environment cell. The one mismatch is Glasgow Independent `197`, 61 versus 91, which is the duplicate district total. Managing Student Behavior and School Leadership each have that same Glasgow mismatch and otherwise match. The historical file stores 945 cells as the text `N/A` (315 schools, all three constructs). KYRC23 turns those 945 cells into 0. Registering both copies would write 2023 twice, with a different metric name and with zeros in place of the omitted `N/A` rows. `accepts_data_file` skips the KYRC23 name even if it is dropped into the raw directory.

`teacher_working_conditions_2020.csv` returns 200 and was not registered. School year is `20192020` on all 4,686 rows. It is not a republish of 2021. The 2021 file is school year `20202021`. Shared numeric percents match, except Glasgow Independent `197`, whose two district-total rows are the same pair of values in a different order. 2020 has 18 school codes the 2021 file does not have. 2021 has one code, `523020`, that 2020 does not have. West Point Elementary `586010` is one of the 2020-only codes: behavior 67, leadership 67, teaching environment 62. This pull request registers the 2020 file. It writes 4,131 KPI rows.

Each survey is published under two KPI years, so there are four waves, not seven. 2020 and 2021 are one survey, 2022 and 2023 are the next, and 2024 and 2025 are the next. KYRC26 is the fourth wave. Issue #62 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/62) tracks that double count and the Glasgow duplicate district totals. Issue #61 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/61) tracks the identifier change from state `999` and three-digit district codes in 2020-2023 to state `999000` and `xxx000` district codes in 2024-2026. The state percents are the same on each pair of report-card years. 2020 and 2021 are behavior 65, leadership 66, teaching environment 62. 2022 and 2023 are behavior 65, leadership 64, teaching environment 60. KYRC24 and KYRC25 are behavior 67, climate 65, leadership 69. KYRC26 is the new survey: behavior 69, climate 67, leadership 70. 2022 and 2023 differ numerically only on the Glasgow duplicate's row order. Five codes are only in 2022. Two of those are `N/A` and emit nothing (`075110` Green River School and `777119` Green County Area Technology Center). The other three have percents and are why 2022 has 1,228 rows per construct and 2023 has 1,225: Watson Lane Elementary `275069`, Robert Frost Sixth-Grade Academy `275085`, and Lynn Camp Schools `301450`. 1,228 − 3 = 1,225.

A `datasets` copy of `KYRC24_OVW_Teacher_Working_Conditions.csv` also returns 200 (572,341 bytes, Last-Modified `Thu, 14 May 2026 19:11:30 GMT`, MD5 `877647e1a9400ac6b6b09b8c823fd119`). It has 1,435 school codes. The registered historical file has 1,228. The datasets copy's percents match KYRC25 on every shared school-measure (4,299 rows, 0 mismatches). Two codes are only in that datasets file (`115017`, `445045`) and one is only in KYRC25 (`777135`, Mason County Career Magnet School). The registered historical file differs from the datasets copy on 9 district-total percents: Carroll County `101000` behavior 67 versus 57; Clark County `121000` 68 / 73 / 75 versus 64 / 63 / 69; Glasgow Independent `197000` behavior 76 versus 69 and climate 87 versus 66; Woodford County `601000` 82 / 76 / 70 versus 70 / 67 / 69. It is also missing 31 districts (209 school codes) that the datasets copy has, and it has Craft Academy `610000` and `610300`, which the datasets copy does not. On 128 shared codes the historical `CO-OP` is blank and the datasets copy has a name. School `121026` stores `CO-OP` as `-84.21926253`. The historical URL still returns 200, so this pull request does not swap it. Swapping would change those 9 published percents and add the 209 codes. The historical KYRC25 file matches the registered datasets percents. Six district-name cells add the word `District` for the Kentucky School for the Deaf (`603000`, `603390`). 2024-25 is not a missing year.

`TELL_EQUITY_17-18.xlsx` and `TELL_EQUITY_18-19.xlsx` return 200. Both data sheets are school year `20172018` (5,157 rows each). The measure column is `EQUITY_MEASURE` (Managing Student Conduct Composite, Community Support & Involvement Composite, School Leadership Composite) and the value is `EQ_VALUE`, which the cover sheet calls an average equity rating. Those are not the Impact Measure and Impact Value columns, so this parser emits nothing. The two workbooks are not identical (249 `EQ_VALUE` cells differ). They are not registered, and they are not skipped, because they already produce no KPI rows. They are not the 2018-19 Impact survey.

There is no Suppressed column. No registered year has a `*` cell. Every KPI row is `suppressed` `N` (26,871 rows). A constructed `Y` row is kept with an empty value, which is the notes/8 shape. A row that is not `Y` and whose percent is blank or the text `N/A` emits nothing. The `N/A` cells are an entire school: all three constructs, never one. 2020 has 555 (185 schools). 2021 has 522 (174 schools). 2022 has 951 (317 schools). 2023 has 945 (315 schools). KYRC24, KYRC25, and KYRC26 have none. There is no response-count column, so `N/A` is not stored as 0. KYRC23's zeros for those cells are the reason that file is not registered. A published 0 is kept. Jefferson County Boys & Girls Haven `275028` is behavior 42, climate 28, and leadership 0 in KYRC24 and in KYRC25. Those two report-card years are the same survey published twice. The school is not in the KYRC26 file. KYRC26 has no published 0. Percents are 0 through 100. 2020 and 2021 print a trailing `.0`. Later files print integers. None of the registered percents contain a comma.

The comma strip does not change a published row. It keeps a comma-formatted percent alive in the unit test, on object and on StringDtype. `config/mappings.yaml` does not cast these columns. pandas is unpinned. `bool(pd.NA)` is not used. pandas 2.3.3 and pandas 3.0.6, both with the production `mappings.yaml` config, write the same 26,871 rows apart from `last_updated`. A cell-by-cell compare of 2021-2025, against output from the base code on the registered files, found no difference (19,458 shared rows).

| Year | Base KPI rows | This change | What the year contains |
| --- | --- | --- | --- |
| 2020 | 0 | 4,131 | Teaching environment, behavior, leadership. 555 `N/A` cells omitted |
| 2021 | 4,113 | 4,113 | Same three constructs. 522 `N/A` cells omitted |
| 2022 | 3,684 | 3,684 | Same three constructs. 951 `N/A` cells omitted |
| 2023 | 3,675 | 3,675 | Same three constructs. 945 `N/A` cells omitted. Breathitt `061` is year `20022023` |
| 2024 | 3,684 | 3,684 | Behavior, climate, leadership. Includes the published 0 |
| 2025 | 4,302 | 4,302 | Same three constructs. Includes the published 0 |
| 2026 | — | 3,282 | Same three constructs. No `N/A` and no 0 |

The base total is 19,458. This change is 26,871. 19,458 + 4,131 + 3,282 = 26,871. 2020 source rows are 4,686. 4,686 − 555 = 4,131. 2021 is 4,635 − 522 = 4,113. 2022 is 4,635 − 951 = 3,684. 2023 is 4,620 − 945 = 3,675. KYRC24 is 1,228 codes × 3 = 3,684. KYRC25 is 1,434 × 3 = 4,302. KYRC26 is 1,094 × 3 = 3,282.

KYRC26 source rows are 3,282. KYRC25 source rows are 4,302. School codes go from 1,434 to 1,094: 400 removed and 60 added. 1,434 − 400 + 60 = 1,094. Every code has exactly three constructs in both years, so the KPI counts are 4,302 − 1,200 + 180 = 3,282. Districts go from 172 to 136: 38 removed and 2 added. 172 − 38 + 2 = 136. The removed codes are 235 in those 38 districts (38 district totals and 197 schools) plus 165 schools in districts that remain. 235 + 165 = 400. The added codes are Lee County `321000`, Williamsburg Independent `592000`, Williamsburg City School `592012`, and 57 schools in districts that remain. 2 + 1 + 57 = 60. Adair County is one of the 38 removed districts, so there is no 2026 Adair row. Fayette County `165000` and Bryan Station High `165170` remain. The two B1 schools leave with Model Laboratory Schools at EKU (`606450` Model Elementary and `606460` Model High). They are not co-op totals `901`–`909`.

`CO-OP` on KYRC26 uses `GLEC` where KYRC25 used `GREC`. The co-op code stays `909`. KYRC25 has 126 `GREC` codes (378 rows). 15 of those codes are removed. The other 111 become `GLEC`. 26 added codes are `GLEC`. 111 + 26 = 137 codes, and 137 × 3 = 411 rows. KYRC25 has 378 rows with co-op code `909`. KYRC26 has 411. Nothing in this pipeline joins on the co-op name.

State `999000`, district All Districts, school `All Schools` stored as `---District Total---`, school year `20252026`, KPI year `2026`, suppressed `N`. Behavior is 69, climate is 67, leadership is 70. `CO-OP` is blank. Fayette County `165000` is 69, 69, and 70, NCES id `2101860`, co-op `CKEC`, co-op code `901`, county `FAYETTE`. Bryan Station High `165170` is 69, 73, and 74, state school id `034165170`, NCES id `210186000388`, school type `A1`, county `FAYETTE`, co-op `CKEC` / `901`.

Prior-year anchors, unchanged against the base output: KYRC25 state 67, 65, and 69. Bryan Station is 58, 61, and 60, and the NCES id is the published `210186000000`. Adair County High `001010` is 67, 45, and 55, NCES id `210003000000`. Boys & Girls Haven `275028` leadership is 0 in 2024 and in 2025 only. The school is not in KYRC26. KYRC24 state is 67, 65, and 69. Bryan Station is 58, 61, and 60, NCES id `210186000388`. 2023 state school id `999`, district `State`, school name `---State Total---`, behavior 65, leadership 64, teaching environment 60. Breathitt County `061` is 77, 76, and 74, and the source year cell `20022023` still yields KPI year 2023. Adair County High is 76, 74, and 59, NCES id `210003000001`. 2022 state is 65, 64, and 60. Bryan Station is 41, 38, and 40. 2021 and 2020 state are 65, 66, and 62, district `State`, school name `---State Total---`. Bryan Station is 45, 44, and 39 in both years.

The files have no demographic column, so the base check still warns that the required race labels are absent. `student_group` is `All Students`. This pull request does not change that list.

Fixtures are verbatim real rows in `tests/fixtures/teacher_working_conditions/`, including the KYRC26 state, Fayette, and Bryan Station rows, the KYRC25 zero at Boys & Girls Haven, the 2023 Ashland Head Start `N/A` rows, and the Breathitt `20022023` rows. The fixture keeps the text `N/A`. `tests/test_fixture_sizes.py` passes.

Issue #42 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/42): `scripts/generate_report_drafts.py` selects state rows with `district == 'State'`, and `get_state_average` also requires `school_name == '---District Total---'`. These metrics are not in that script's indicator list. The 2020-2023 state rows are district `State` and school name `---State Total---`. KYRC24 through KYRC26 store district `All Districts` and school name `All Schools`, which this parser stores as `---District Total---`. Neither shape matches both conditions. `html/generate_dashboard_data.py` does not read these metrics.

`openpyxl` is already declared in `pyproject.toml` from PR #57. This module's registered files are CSV. Without the package, nothing in this directory is skipped.

Lessons for later agents:

1. HEAD the registered URLs before assuming a temporary file went 404. Every registered URL returned 200. The guessed OAA names are 404 and were never registered. Do not add a skip for a filename this directory has never used. `KYRC23_OVW_Teacher_Working_Conditions.csv` returns 200 and is the 2022-23 extract again. It relabels Teaching Environment as School Climate and turns 945 `N/A` cells into 0. Do not register it. `accepts_data_file` skips the name. The historical KYRC24 URL still returns 200. Leave it. The datasets copy matches KYRC25 on shared percents and adds 31 districts. It also changes 9 district totals. `teacher_working_conditions_2020.csv` is a real 2019-20 file, not a republish of 2021. It is registered.
2. The measure is percent favorable. KDE's glossary says so. Store the published number. Do not average the constructs into one KPI. 2020-2023 publish Teaching Environment. KYRC24 through KYRC26 publish School Climate. Leave those as separate metrics. Do not bridge them in this pull request. KDE's KYRC23 file relabels the 2023 Teaching Environment values, unchanged on all 1,539 schools, as School Climate. The 2020 Impact Kentucky topic list says "School climate". School-level correlation across the label change (0.66) lines up with same-label wave-to-wave correlations (0.67-0.73). Issue #62 tracks the bridge.
3. KYRC26 has a state row `999000`, district `All Districts`, school name `All Schools` stored as `---District Total---`, year `2026`. The percents are 69, 67, and 70. There is no demographic column. Do not require one. Adair County is absent from this file.
4. "Header changes: none" means the KYRC26 header matches KYRC25. pandas is unpinned. `dtype=str` is StringDtype on pandas 3. The comma strip treats StringDtype the same as object. It does not change a published row. No registered percent contains a comma. pandas 2.3.3 and pandas 3.0.6 write the same 26,871 rows.
5. These files have no Suppressed column and no `*` cell. Every KPI row is `suppressed` `N`. A blank or `N/A` row that is not `Y` emits nothing. There is no count column, so do not treat that `N/A` as 0. The 2023 `N/A` rows are 315 schools (945 cells), including Ashland Head Start `012012`. A published 0 is kept. Boys & Girls Haven `275028` leadership is 0 in 2024 and 2025 only. Those two years are the same survey published twice, and the school is not in KYRC26. KYRC23 turns those 945 `N/A` cells into 0, and that file is not loaded.
6. Compare the code sets, then the counts. 1,434 − 400 + 60 = 1,094 codes, and both years emit three percents per code, so 4,302 and 3,282. Districts: 172 − 38 + 2 = 136. The 400 removed codes are 38 district totals, 197 schools in those districts, and 165 schools in districts that remain. The 60 added codes are two new district totals, Williamsburg City School, and 57 schools in districts that remain. They are not co-op totals `901`–`909`. `GREC` becomes `GLEC` on the shared codes. The co-op code stays `909`. 126 − 15 + 26 = 137.
7. Prior-year identity against the base code is 4,113 / 3,684 / 3,675 / 3,684 / 4,302 for 2021-2025, on pandas 2.3.3 and on pandas 3. No previously published cell changes. 2020 is new: 4,131 rows, state 65, 66, and 62. KYRC26 is new: 3,282 rows, state 69, 67, and 70. The total is 26,871.
8. State district is `All Districts` for 2024-2026 and `State` for 2020-2023. School name `All Schools` is stored as `---District Total---`. `---State Total---` is left as written. The 2020-2023 state school code stays `999`. KYRC24 through KYRC26 use `999000`. Issue #61 tracks that identifier change. Issue #42: `scripts/generate_report_drafts.py` selects `State` and also requires `---District Total---`. These metrics are not in the indicator list. Neither shape matches both conditions. Each survey is published under two KPI years (2020=2021, 2022=2023, 2024=2025), so there are four waves, not seven. Issue #62 tracks that double count and the Glasgow duplicates.
9. `openpyxl` is already a project dependency from PR #57. This module does not add a workbook. The TELL equity workbooks are 2017-18 and are not this measure. Without a column alias they emit no rows, so they are not skipped by filename.
10. No shared parser or dependency changes. `BaseETL` was not edited. The comma strip stays in this module and does not change a published row. pandas is unpinned. pandas 2.3.3 and pandas 3.0.6 write the same 26,871 rows.

### `students_taught_by_ineffective_teachers` — S — in review in PR #63

| | |
| --- | --- |
| KYRC26 files | `KYRC26_OVW_Students_Taught_by_Ineffective_Teachers.csv` |
| Header changes | None. Nine demographic percent columns, same labels as KYRC25 |
| Fixtures / assertions | State `999000` row, year `2026`. District `All Districts`. Title I All Students 0.5. Not Title I All Students 0.1 |
| Gotchas | No count column, so `N/A`, `#N/A`, and a blank stay missing. A published 0 is kept. The datasets KYRC24 object is 39 rows. Leave the historical file. A negative equity-gap cell is still dropped. Issues #42 and #61 |

Result, checked 18 October 2026 (UTC) against the public `datasets` container and the historical host. `include_kyrc26 = True` is the parser change, plus registration of `students_taught_by_ineffective_teachers_2020.csv` and a filename skip for the KYRC23 republish. `etl/kyrc.py`, `BaseETL`, and the demographic config were not edited. `openpyxl` was already declared in PR #57. This module does not read a workbook. Re-HEAD the KYRC26 file after 26 October 2026. If the hash diverges from the stamp below, take the newer stamp and record which container it came from.

**The measure is the published percent of students taught by an ineffective teacher.** KDE's report-card glossary defines an ineffective teacher as one whose local evaluation system, meeting KRS 156.557, gives a summative rating of Ineffective. Districts self-report the counts in the Kentucky Equity Survey. The report card publishes a percent. In 2020-2023 and in KYRC25 and KYRC26 the group columns are shares of all students: Non-White plus White equals All Students on every nonzero 2020-2023 row and on about 95 percent of nonzero KYRC25 and KYRC26 rows (within 0.15, the rounding on one-decimal percents). The registered historical KYRC24 file is the exception. Its group columns behave like within-group rates. African American plus White equals All Students on 1 of 62 nonzero rows at an exact match, and on 3 of 62 within 0.15. African American exceeds All Students on 31 of those 62. Store the published number. Do not rebuild it and do not rescale it. State Title I All Students is about 0.3 to 0.9, which is already a percent.

The KPI names are `students_taught_by_ineffective_teachers_rate_title_1`, `students_taught_by_ineffective_teachers_rate_not_title_1`, and `students_taught_by_ineffective_teachers_rate_equity_gap`. No other module emits those names. `kpi_master` concatenates without deduping, so a second module must not emit them. `student_group` comes from the demographic column. The equity-gap metric is the demographic cells on a source row whose Title I status is `Equity Gap`. KYRC24, KYRC25, and KYRC26 have no Equity Gap rows, so those years emit no equity-gap KPI. The four historical `INEFFECTIVE GAP %AGE` columns are mapped and are not a second metric. The equity-gap row already stores the gap in the demographic columns.

Every registered URL returned 200 on 18 October 2026 (UTC). KYRC26 on `datasets` is Last-Modified `Thu, 15 Oct 2026 10:00:11 GMT`, 261,805 bytes, ETag `0x8DF2AA319C25EC8`, MD5 `44b49be11d418e83c4ff1e7000965bd7`. That host does not send Content-MD5. The UAT copy is the same length and the same MD5 (Content-MD5 `RLSb4R1BjoPE/x5wAJZb1w==`), Last-Modified `Mon, 12 Oct 2026 19:53:57 GMT`. KYRC25 on `datasets` is Last-Modified `Sat, 18 Jul 2026 21:29:47 GMT`, 261,299 bytes, ETag `0x8DEE513B11B443B`, MD5 `082400fdb181e10b1308615f15d3fb5a`. That host does not send Content-MD5, so the downloader keeps the datasets copy. The UAT copy is the same length and a different MD5 `20d08d7d8d077163e6fa8e21f473a29d` (Content-MD5 `INCNfY0HcWPm+o4h9HOinQ==`), Last-Modified `Mon, 12 Oct 2026 19:55:55 GMT`. All 1,692 school rows match. The only differences are 8 cells on state `999000`. Datasets Title I is All Students 0.8, Non-White 0.1, White 0.4, economically disadvantaged 0.5, non-economically disadvantaged 0.1, students with disabilities 0.1, without disabilities 0.2, English learner 0.1, non-English learner 0.2, and Not Title I All Students 0.0. UAT Title I is 0.9, 0.4, 0.5, 0.6, 0.3, 0.1, 0.7, 0.1, 0.6, and Not Title I All Students 0.1. This pull request uses the datasets file. The historical KYRC24 file is Last-Modified `Thu, 29 May 2025 22:09:22 GMT`, 238,427 bytes, MD5 `f09af13654c91038f7406d018b8d93c7`. `students_taught_by_ineffective_teachers_2020.csv` through `2023.csv` returned 200 on the historical host. 2020 is Last-Modified `Fri, 09 Feb 2024 20:57:10 GMT`, 358,316 bytes, MD5 `4525ed9e45bc6b2e4a91f19909b94935`. The guessed temporary names `Students_Taught_by_Ineffective_Teachers_2025.CSV` and `Students_Taught_by_Ineffective_Teachers_2026.CSV` are 404 on the OAA host. None of those names was registered. There is no leftover temporary filename to skip.

`KYRC23_OVW_Students_Taught_by_Ineffective_Teachers.csv` returns 200 and is not registered (176,336 bytes, Last-Modified `Thu, 14 May 2026 19:11:28 GMT`, MD5 `cb098406ff2e9cccb8beb8a4d1adf1f1`). The UAT copy is the same bytes. School year is `20222023` on all 1,171 rows. Title I is 867 rows and Not Title I is 304. There is no Equity Gap status. The column is `African American`, not `Non-White`. Shared All Students cells against `students_taught_by_ineffective_teachers_2023.csv` correlate at 1.0 (799 of 825 exact, 823 within 0.05). None of the historical `N/A` school codes appear in KYRC23. Ashland Head Start `012012` is absent. KYRC23 drops those schools. It does not turn the `N/A` cells into 0. Registering it would write 2023 twice. `accepts_data_file` skips the KYRC23 name even if it is dropped into the raw directory. A full run that included the file wrote 0 rows from it.

`students_taught_by_ineffective_teachers_2020.csv` returns 200 and was not registered. School year is `20192020` on all 2,008 rows. It is not a republish of 2021. The 2021 file is school year `20202021`, 1,991 rows. Among nonzero All Students cells, 0 match exactly, the correlation is about −0.11, and the mean absolute difference is about 10.3. 2020 has 22 school codes the 2021 file does not have. This pull request registers the 2020 file. It writes 13,288 KPI rows. 188 source rows are skipped: every mapped percent is `N/A` except students with disabilities, which is `0.00` on 186 rows and `0.000%` on 2. Those rows are 46 Title I, 140 Not Title I, and 2 Equity Gap. All Students is `N/A` on each of them, so the 0 is a placeholder. notes/8 says that is not a measured rate. Sunshine School `236021` is one of the 140 Not Title I rows. The fixture row writes nothing.

This series is not the same survey published under two KPI years. State Title I All Students moves each year: 2020 is 0.29, 2021 is 0.43, 2022 is 0.69, 2023 is 0.88, KYRC24 is 0.7, KYRC25 datasets is 0.8, and KYRC26 is 0.5. Issue #62 tracks that pattern for teacher working conditions. It does not apply here.

A `datasets` copy of `KYRC24_OVW_Students_Taught_by_Ineffective_Teachers.csv` also returns 200 (6,110 bytes, Last-Modified `Thu, 14 May 2026 19:11:30 GMT`, MD5 `d0499b27517699100bd3f46fd37f2610`). It has 39 rows. The column is `African American`. State Title I All Students is 4.7 and African American is 35.3. Many cells are above 100. Todd County `551000` Not Title I All Students is 195.2. Wayne County `581000` Title I African American is 200.0. Ballard County `015000` Title I African American is 183.3. Crossroads Elementary `025014` Title I English Learner is 750.0. The registered historical file has 1,562 rows and state Title I All Students 0.7. Swapping would replace that extract with the 39-row file. This pull request leaves the historical registration. That is the opposite of the novice-teachers KYRC24 equity copy in issue #58 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/58). The UAT KYRC24 object is 243,575 bytes, MD5 `d26e9d2547bb2c3105d77a3dd55f7458`, 1,589 rows, and its column is `Non-White`. State Title I All Students is 0.8 and Non-White is 0.3. The downloader probes UAT only for names that start `KYRC25_` or `KYRC26_`. This pull request does not switch the historical KYRC24 URL to that UAT object.

The registered KYRC24 column `African American` is not an alias of `Non-White`. State Title I African American is 1.0. The UAT KYRC24 Non-White cell is 0.3. Across the file the correlation is about 0.79, and 1,506 of 1,561 numeric cells match, which is not the same series. Those cells stay unmapped, which is why 2024 emits no Non-White `student_group` for state `999000`. A follow-up should decide whether 2024 African American is its own student group. KYRC23 uses the same column name and is not loaded.

`Title 1 - Schoolwide School` is not mapped to Title I. KYRC25 has one such row, Adair County `001000`, and every percent on it is 0.0. That district already has a Title 1 row, also 0.0. Mapping the schoolwide label onto Title I would write a second KPI for the same school, year, and group. The row is skipped. A follow-up should decide whether that label is its own metric. KYRC26 has no schoolwide row.

There is no Suppressed column. No registered year has a `*` cell. Every KPI row is `suppressed` `N` (100,640 rows). A constructed `Y` row is kept with an empty value, which is the notes/8 shape. A row that is not `Y` and whose mapped percents are all missing emits nothing. There is no student-count column, so a blank, `N/A`, or `#N/A` is not stored as 0. A published 0 is kept. `read_csv` turns the text `N/A` and `#N/A` into missing before the parser runs. The parser treats those strings the same way when they survive. A sibling cell of 0 does not fill an `N/A` cell.

Cell arithmetic on the nine mapped 2020-2023 percent columns, and on the eight mapped KYRC24 columns (`African American` is the ninth and is unmapped):

| Year | Source rows | Missing or unparsed mapped cells | Negative cells dropped | KPI rows |
| --- | --- | --- | --- | --- |
| 2020 | 2,008 | 4,591 `N/A`, plus 188 placeholder zeros | 5 | 13,288 |
| 2021 | 1,991 | 1,836 `#N/A` and 270 `N/A` | 126 | 15,687 |
| 2022 | 1,949 | 576 `N/A` | 190 | 16,775 |
| 2023 | 1,945 | 5,401 `N/A` | 145 | 11,959 |
| 2024 | 1,562 | 33 empty and 6 parenthetical | 0 | 12,457 |
| 2025 | 1,692 | the one schoolwide row (9 cells) | 0 | 15,219 |
| 2026 | 1,695 | 0 | 0 | 15,255 |

2020 is 2,008 × 9 = 18,072 = 13,288 + 4,591 + 5 + 188. The 188 are the placeholder students-with-disabilities zeros that this pull request skips. 2021 is 1,991 × 9 = 17,919 = 15,687 + 2,106 + 126. 2022 is 1,949 × 9 = 17,541 = 16,775 + 576 + 190. 2023 is 1,945 × 9 = 17,505 = 11,959 + 5,401 + 145. KYRC24 mapped cells are 1,562 × 8 = 12,496 = 12,457 + 33 + 6. The six parenthetical cells are `(0.1)`, `(0.5)`, `(0.2)`, `(0.2)`, `(5.3)`, and `(0.7)`. Both the previous parser and this one leave them unparsed. KYRC25 skips the one `Title 1 - Schoolwide School` row, so 1,691 × 9 = 15,219. KYRC26 is 1,695 × 9 = 15,255. Title I is 1,283 × 9 = 11,547. Not Title I is 412 × 9 = 3,708. 11,547 + 3,708 = 15,255.

Sunshine School `236021` (Harlan Independent), 2020 Not Title I, is `N/A` on every mapped percent except students with disabilities, which is `0.00`. The row writes nothing. APEX Academy `011030` (Anderson County), 2021 Not Title I, is `#N/A` on every percent and emits nothing. Ashland Head Start `012012`, 2023 Not Title I, is the text `N/A` on every percent and emits nothing. KYRC25 and KYRC26 have no empty cells, no `N/A`, no stars, and no negatives.

2020-2023 publish whole rows of `N/A`. KYRC24, KYRC25, and KYRC26 publish none. 471 rows that are entirely `N/A` in 2023 are all 0 in the registered 2024 file, so a 2024 or later 0 can be a no-data case that the older file marked `N/A`. Issue #64 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/64) tracks that.

Negative Title I cells in 2022 (30) and 2023 (10) sit on a complement column KDE computed by subtraction. Rector A. Jones Middle School `035020`, 2022 Title I, is Non-White 5.65 and White −5.40, and 5.65 + (−5.40) = All Students 0.25. The paired positive is inflated by the same subtraction. The parser still drops the negative and keeps the inflated positive. Issue #64 tracks that too.

A negative cell is still dropped. The behavior is unchanged. In 2021-2023 the equity gap is Not Title I minus Title I, so a negative cell is one where Title I is higher, and dropping it removes the cells where Title I is worse. In 2020 the gap is Title I minus Not Title I on 4 of the 5 All Students rows where the two sides differ (districts `035`, `181`, `275`, and `571`). Dropping a negative in 2020 removes the cells where Title I is lower and keeps the cells where Title I is worse. The 2020 state All Students gap is −1.19. Title I is 0.29 and Not Title I is 0.00, so −1.19 matches neither difference. That cell is absent from the output. Non-White on the same row is 0.09, which is Title I minus Not Title I, and it is kept. Jefferson Shelby Academy `275610`, 2023 Title I, keeps All Students 0.91 and Non-White 20.45 and drops White −19.55%. Issue #59 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/59) tracks whether the signed gap is stored. The drop is not suppression `Y`. The test that covers the drop says so.

Nine published cells in the unchanged 2024 and 2025 output are above 100. The maximum is 300.0, Knox Girdler Elementary `301260` Title I English Learner in 2024. This pull request does not cap them.

The comma strip does not change a published row. No registered percent contains a comma. It keeps a comma-formatted percent alive in the unit test, on object and on StringDtype. `config/mappings.yaml` has no dtype cast for this source. pandas is unpinned. `bool(pd.NA)` is not used. pandas 2.3.3 and pandas 3.0.6, both with the production `mappings.yaml` config, write the same 100,640 rows apart from `last_updated`. A cell-by-cell compare of 2021-2025, against output from the base code on the registered files, found no difference (72,097 shared rows). The base run skipped KYRC26 because `include_kyrc26` was false, and 2020 was not registered, so the base total is 72,097. 72,097 + 13,288 + 15,255 = 100,640. The 2020 file, before the placeholder skip, would have been 13,476. 13,476 − 188 = 13,288.

KYRC26 source rows are 1,695. KYRC25 source rows are 1,692. School codes go from 1,585 to 1,587: 26 removed and 28 added. 1,585 − 26 + 28 = 1,587. Districts go from 176 to 174: Anchorage Independent `006` and Burgin Independent `072` leave, and no district is added. 176 − 2 = 174. The removed codes are 4 in those two districts (each district total plus Anchorage Independent Public School and Burgin Independent School) and 22 schools in districts that remain. 4 + 22 = 26. Each removed code is one source row. The added codes are 28 schools in districts that remain. None is a new district total, and none is a co-op total `901`–`909`. On the 1,559 shared codes, 15 Title I status keys are only in KYRC25 and 16 are only in KYRC26. 1,692 − 26 + 28 + 16 − 15 = 1,695. The 15 include Adair County `001000` `Title 1 - Schoolwide School` and status flips such as Ballard Memorial `015010`, Camp Ernst `035010`, and Bryan Station's neighbors Cassidy `165032`, Edythe Jones Hayes `165034`, and Garrett Morgan `165099`. KYRC25 has 1,479 codes with one status, 105 with two, and 1 with three. KYRC26 has 1,479 with one and 108 with two.

`CO-OP` on KYRC26 uses `GLEC` where KYRC25 used `GREC`. The co-op code stays `909`. KYRC25 has 169 `GREC` codes and 170 rows. One code is removed (`275374`). The other 168 become `GLEC`. No added code is `GLEC`. GLEC has 168 codes and 169 rows, because one of those codes has two title rows. KYRC25 has 170 rows with co-op code `909`. KYRC26 has 169. Nothing in this pipeline joins on the co-op name.

State `999000`, district All Districts, school `All Schools` stored as `---District Total---`, district number 999, school year `20252026`, KPI year `2026`, suppressed `N`. `CO-OP` and NCES id are blank. Title I is All Students 0.5, Non-White 0.2, White 0.4, economically disadvantaged 0.3, non-economically disadvantaged 0.2, students with disabilities 0.1, without disabilities 0.4, English learner 0.0, non-English learner 0.4. Not Title I is All Students 0.1, Non-White 0.0, White 0.1, economically disadvantaged 0.2, non-economically disadvantaged 0.0, students with disabilities 0.0, without disabilities 0.0, English learner 0.0, non-English learner 0.1.

Fayette County `165000` Title I All Students is 1.2 and Not Title I All Students is 0.0. NCES id `2101860`, co-op `CKEC`, co-op code `901`, county `FAYETTE`, district number 165. School name `All Schools` is stored as `---District Total---`. Bryan Station High `165170` Title I All Students is 0.0, and the other eight groups are 0.0. State school id `034165170`, NCES id `210186000388`, school type `A1`, county `FAYETTE`, co-op `CKEC` / `901`. Adair County High `001010` Title I All Students is 0.0, NCES id `210003000001`, state school id `001001010`. Adair County `001000` Title I All Students is 0.0, NCES id `2100030`, co-op `GRREC` / `902`. Adair is in the KYRC26 file.

Prior-year anchors, unchanged against the base output: KYRC25 state Title I All Students is 0.8 and Not Title I is 0.0. Fayette Title I All Students is 4.3. Bryan Station Title I All Students is 0.0, and the NCES id is the published `210186000000`. Adair County High NCES id is the published `210003000000`. Adair County `001000` has Title 1, Not Title 1, and `Title 1 - Schoolwide School`, all zeros. The output has one Title I All Students row of 0.0. The schoolwide row is not emitted. KYRC24 state Title I All Students is 0.7 and Not Title I is 0.0. There is no Non-White group for `999000`, because African American 1.0 is unmapped. Bath County `025000` Not Title I All Students is 0.0, and the White cell is empty, so that group is absent. 2023 state school id `999`, district `State`, school name `---State Total---`, Title I 0.88, Not Title I 0.04. The equity-gap row is entirely negative, so school `999` emits no equity-gap metrics. Breathitt `061` includes the source year cell `20022023`, and that row still yields KPI year 2023. Ashland Head Start `012012` is absent. Shelby Academy `275610` keeps All Students 0.91 and Non-White 20.45. 2022 state Title I is 0.69 and Not Title I is 0.3. 2021 state Title I is 0.43 and Not Title I is 0.0. APEX Academy `011030` is absent. 2020 state Title I is 0.29, district `State`, school name `---State Total---`, school id `999`. Equity-gap Non-White 0.09 is kept. All Students −1.19 is absent. Sunshine School `236021` writes nothing. The 2020 Not Title I row is `N/A` except students with disabilities `0.00`, and that placeholder is skipped.

The demographic check still warns that the required race labels are absent and that Non-White is unexpected. 2024 does not emit Non-White because African American is unmapped. This pull request does not change that shared list.

Fixtures are verbatim real rows in `tests/fixtures/students_taught_by_ineffective_teachers/`, including the text `N/A` and `#N/A`, the KYRC25 schoolwide label, the 2023 Ashland `N/A` row, the Breathitt `20022023` rows, and the 2020 Sunshine partial `N/A` row. `tests/test_fixture_sizes.py` passes.

Issue #42 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/42): `scripts/generate_report_drafts.py` selects state rows with `district == 'State'`, and `get_state_average` also requires `school_name == '---District Total---'`. These metrics are not in that script's indicator list. The 2020-2023 state rows are district `State` and school name `---State Total---`. KYRC24 through KYRC26 store district `All Districts` and school name `All Schools`, which this parser stores as `---District Total---`. Neither shape matches both conditions. `html/generate_dashboard_data.py` does not read these metrics. `analysis/config/covariate_catalog.yaml` lists `students_taught_by_ineffective_teachers_rate`. No analysis script reads the three metric names this module emits.

`openpyxl` is already declared in `pyproject.toml` from PR #57. This module's registered files are CSV. Without the package, nothing in this directory is skipped.

Lessons for later agents:

1. HEAD the registered URLs before assuming a temporary file went 404. Every registered URL returned 200. The guessed OAA names are 404 and were never registered. Do not add a skip for a filename this directory has never used. `KYRC23_OVW_Students_Taught_by_Ineffective_Teachers.csv` returns 200 and is the 2022-23 extract again. It drops the historical `N/A` schools and has no Equity Gap rows. Do not register it. `accepts_data_file` skips the name. The historical KYRC24 URL still returns 200. Leave it. The datasets copy is 39 rows with percents above 100, including 750. The UAT KYRC24 object uses `Non-White` and is not what the downloader probes for a KYRC24 name. `students_taught_by_ineffective_teachers_2020.csv` is a real 2019-20 file, not a republish of 2021. It is registered.
2. The measure is the published percent. KDE's glossary says an ineffective teacher has a local KRS 156.557 rating of Ineffective. In 2020-2023 and in KYRC25 and KYRC26 the group columns are shares of all students: Non-White plus White equals All Students. The registered historical KYRC24 file behaves like a within-group rate, and African American plus White does not equal All Students. Store the published number. State Title I All Students is 0.5 in KYRC26. Do not rescale it to 50. A published cell above 100 stays as published. Nine such cells are already in the 2024 and 2025 output, and the maximum is 300.
3. KYRC26 has a state row `999000`, district `All Districts`, school name `All Schools` stored as `---District Total---`, year `2026`. Title I All Students is 0.5. Not Title I All Students is 0.1. Fayette County `165000` Title I All Students is 1.2. Bryan Station High `165170` Title I All Students is 0.0. Adair County is in this file.
4. "Header changes: none" means the KYRC26 header matches KYRC25. pandas is unpinned. `dtype=str` is StringDtype on pandas 3. The comma strip treats StringDtype the same as object. It does not change a published row. No registered percent contains a comma. pandas 2.3.3 and pandas 3.0.6 write the same 100,640 rows.
5. These files have no Suppressed column and no `*` cell. Every KPI row is `suppressed` `N`. A blank, `N/A`, or `#N/A` percent that is not on a `Y` row stays missing. There is no count column, so check that before treating a blank percent as zero. Sunshine School `236021` in 2020 is `N/A` on every group except students with disabilities `0.00`. That row writes nothing. The same rule skips 188 rows (46 Title I, 140 Not Title I, 2 Equity Gap). APEX Academy `011030` in 2021 is `#N/A` and emits nothing. Ashland Head Start `012012` in 2023 is `N/A` and emits nothing. A published 0 is kept. Bryan Station High in KYRC26 is 0.0 on all nine Title I groups.
6. Compare the code sets, then the counts. 1,585 − 26 + 28 = 1,587 codes. Districts: 176 − 2 = 174. The two removed districts are Anchorage Independent `006` and Burgin Independent `072` (4 codes). The other 22 removed codes are schools in districts that remain. The 28 added codes are schools in districts that remain. They are not co-op totals `901`–`909`. On shared codes, 15 status keys leave and 16 arrive, so source rows are 1,692 − 26 + 28 + 16 − 15 = 1,695. KYRC26 emits nine groups on every row, so 1,695 × 9 = 15,255. KYRC25 skips the one schoolwide row, so 1,691 × 9 = 15,219. `GREC` becomes `GLEC` on the shared codes. The co-op code stays `909`. 169 − 1 = 168 codes, and the row count goes from 170 to 169.
7. Prior-year identity against the base code is 15,687 / 16,775 / 11,959 / 12,457 / 15,219 for 2021-2025, on pandas 2.3.3 and on pandas 3. No previously published cell changes. 2020 is new: 13,288 rows after the placeholder skip, state Title I 0.29. KYRC26 is new: 15,255 rows, state Title I 0.5 and Not Title I 0.1. The total is 100,640.
8. State district is `All Districts` for 2024-2026 and `State` for 2020-2023. School name `All Schools` is stored as `---District Total---`. `---State Total---` is left as written. The 2020-2023 state school code stays `999`. KYRC24 through KYRC26 use `999000`. Issue #61 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/61) tracks that identifier change. This pull request does not pad the codes. Issue #42: `scripts/generate_report_drafts.py` selects `State` and also requires `---District Total---`. These metrics are not in the indicator list. Neither shape matches both conditions. Each report-card year is its own survey. State Title I All Students is 0.29, 0.43, 0.69, 0.88, 0.7, 0.8, and 0.5. Issue #62 does not apply to this file.
9. `openpyxl` is already a project dependency from PR #57. This module does not add a workbook.
10. No shared parser or dependency changes. `BaseETL` was not edited. The comma strip stays in this module and does not change a published row. pandas is unpinned. pandas 2.3.3 and pandas 3.0.6 write the same 100,640 rows. A negative equity-gap cell is still dropped. In 2021-2023 that removes the cells where Title I is worse, because the gap is Not Title I minus Title I. In 2020 the gap is Title I minus Not Title I on 4 of 5 differing All Students rows, so the same check keeps the cells where Title I is worse. The state All Students gap −1.19 matches neither difference. Issue #59 tracks the signed gap. `African American` on the registered KYRC24 file is still unmapped. A follow-up should decide whether that column is its own student group. `Title 1 - Schoolwide School` is still skipped. A follow-up should decide whether that label is its own metric. Issue #64 tracks two further source facts: 2022 has 30 negative Title I cells and 2023 has 10, from a complement column KDE computed by subtraction (Rector A. Jones `035020` is Non-White 5.65 and White −5.40), so the paired positive is inflated; and 471 rows that are entirely `N/A` in 2023 are all 0 in 2024, so a later 0 can be a no-data case.

### `students_taught_by_out_of_field_teachers` — S — in review in PR #65

| | |
| --- | --- |
| KYRC26 files | `KYRC26_OVW_Students_Taught_by_Out_of_Field_Teachers.csv` |
| Header changes | None versus KYRC25. The registered historical KYRC24 file still uses `Unnamed: 13` and `Inexperienced Value` through `Inexperienced Value.8` |
| Fixtures / assertions | State `999000` Title I All Students 7.3, Not Title I 6.4, equity-gap English learner −6.0, year `2026`. Fayette `165000` Title I 20.3. Bryan Station High `165170` Title I 37.8 |
| Gotchas | Group columns are within-group rates in every year. A published 0 is kept. A negative equity-gap cell is stored. `students_taught_by_out_of_field_teachers_2020.csv` is a real 2019-20 file and is registered. `KYRC23_OVW_Students_Taught_by_Out_of_Field_Teachers.csv` republishes 2022-23 and is skipped. The datasets KYRC24 object is not swapped for the historical extract |

Result, checked 18 October 2026 against the public `datasets` container and the historical host. `include_kyrc26 = True` and the 2020 registration in `config/kde_sources.yaml` are the source changes. The parser stores the published percent, keeps a published 0, and keeps a negative equity-gap cell. `etl/kyrc.py`, `BaseETL`, and the demographic config were not edited. Re-HEAD the KYRC26 file after 26 October 2026. If the hashes diverge from the stamp below, take the newer stamp and record which container it came from.

`datasets` HEAD for `KYRC26_OVW_Students_Taught_by_Out_of_Field_Teachers.csv` did not return Content-MD5. The UAT Content-MD5 is `FZPJtzkZQxWEbYYJe0rZVA==`, which is MD5 `1593c9b739194315846d86097b4ad954` of the bytes downloaded from `datasets`. `datasets` Last-Modified is newer, so this is the `datasets` object. Both containers are 309,282 bytes.

| File | Host | Bytes | MD5 | Last-Modified |
| --- | --- | --- | --- | --- |
| `KYRC26_OVW_Students_Taught_by_Out_of_Field_Teachers.csv` | datasets and UAT, same bytes | 309,282 | `1593c9b739194315846d86097b4ad954` | datasets Thu, 15 Oct 2026 10:00:11 GMT |
| `KYRC25_OVW_Students_Taught_by_Out_of_Field_Teachers.csv` | datasets, kept | 310,768 | `5ab7eefff403b136adca61b93eee88c8` | Sat, 18 Jul 2026 21:29:47 GMT |
| `KYRC24_OVW_Students_Taught_by_Out_of_Field_Teachers.csv` | historical, registered | 307,799 | `cfeb2f07b26ba040ac3b4e948a1cda37` | Thu, 29 May 2025 22:09:22 GMT |
| `students_taught_by_out_of_field_teachers_2023.csv` | historical | 374,647 | `812d55f7f01b1ff4d05d96d30077b860` | Wed, 07 Feb 2024 16:38:20 GMT |
| `students_taught_by_out_of_field_teachers_2022.csv` | historical | 317,559 | `28763a39b35157caef489a71c298c07f` | Thu, 08 Feb 2024 19:33:26 GMT |
| `students_taught_by_out_of_field_teachers_2021.csv` | historical | 374,528 | `37a3373cf6142ea70de0ae96dd9616be` | Fri, 09 Feb 2024 17:50:43 GMT |
| `students_taught_by_out_of_field_teachers_2020.csv` | historical | 364,651 | `3e2cf361396d6d5787c5116148582275` | Fri, 09 Feb 2024 20:57:10 GMT |

Every registered URL returned 200. The guessed OAA names `Students_Taught_by_Out_of_Field_Teachers_2024.CSV`, `_2025.CSV`, and `_2026.CSV`, and `Out_of_Field_Teachers_2024.CSV` through `_2026.CSV`, are 404. They were never registered. There is no leftover-temp skip.

KRS 161.1221 requires EPSB to define out-of-field teaching and to identify every teacher assigned out of field. 16 KAR 2:120 classifies that work as an emergency, probationary, or temporary provisional certificate, or as a teacher with no certificate, an expired certificate, or a certificate that does not cover the subject or the grade. KDE's School Report Card quality-assurance catalogue calls the measure the comparison of students taught by out-of-field teachers in Title I schools and in schools that are not Title I. The Teacher Equity Report supplies the percentage of the school population or of the subgroup population. The file publishes that percent. It does not publish the counts.

The group columns are within-group rates in every registered year. On a nonzero All Students / Non-White / White triple, Non-White plus White equals All Students within 0.15 on 23 of 206 rows in 2020, 33 of 672 in 2021, 28 of 602 in 2022, 47 of 674 in 2023, 21 of 809 in the registered KYRC24 file, 58 of 838 in KYRC25, and 62 of 726 in KYRC26. State Title I in KYRC26 is All Students 7.3, Non-White 8.0, and White 7.0. 8.0 + 7.0 = 15.0, which is not 7.3. Store the published number. Do not rescale 7.3 to a share, and do not rebuild it from the other columns.

`KYRC23_OVW_Students_Taught_by_Out_of_Field_Teachers.csv` returns 200 on datasets (308,616 bytes, MD5 `c5a7e349f9fb112715e95a8fac41a719`, Last-Modified `Thu, 14 May 2026 19:11:28 GMT`). School year is `20222023` on all 2,015 rows. After a 3-digit district code is padded to six digits, the school codes match the historical 2023 file exactly. State Title I All Students is 6.6 against the historical 6.54. 1,346 of 2,015 keys match exactly, 279 differ only by rounding within 0.05, and 380 differ by more than 0.05. The largest cell difference is 200. It is the 2022-23 extract again, with different cells. Do not register it. `accepts_data_file` skips the name.

A `datasets` copy of `KYRC24_OVW_Students_Taught_by_Out_of_Field_Teachers.csv` also returns 200 (306,509 bytes, MD5 `51ba83816c151d16236f045db2ee4691`, Last-Modified `Thu, 14 May 2026 19:11:30 GMT`). It has the same 2,022 keys as the registered historical file, and the state Title I row matches (All Students 8.7, Non-White 9.7). The headers are the real demographic names. 381 keys differ by more than 0.05, including Adair County High `001010` Title I English Learner 4.1 in the historical file and 0.0 in the datasets file. Swapping would replace the extract this pipeline already publishes. This pull request leaves the historical registration. The historical headers stay mapped: `Unnamed: 13` is Title I status, and `Inexperienced Value` through `Inexperienced Value.8` are the nine groups in the later order.

The UAT KYRC24 object is 306,580 bytes, MD5 `ef75d4815d0b7b513ab7d063ca82fec5`. It matches datasets on 2,006 of 2,022 keys. The other 16 keys flip the sign of All Students, White, Non-Economically Disadvantaged, Student without Disabilities, and Non-English Learner (71 cells). The downloader probes UAT only for names that start `KYRC25_` or `KYRC26_`. This pull request does not switch the historical KYRC24 URL to that UAT object.

The UAT KYRC25 object is 310,861 bytes, MD5 `14a0d088f6b7b945bdb0a69e6b3e99b7`, Last-Modified `Mon, 12 Oct 2026 19:09:42 GMT`. It matches the datasets file on 2,004 of 2,025 keys. The other 21 keys are the same kind of sign flip (93 cells). datasets HEAD returned no Content-MD5, so `prepare_kde_data.py` keeps the datasets object from `Sat, 18 Jul 2026 21:29:47 GMT`. Do not point the registration at UAT.

There is no Suppressed column. No registered year has a `*` cell, a blank measure cell, an `N/A`, or a `#N/A`. Every KPI row is `suppressed` `N` (126,963 rows). A constructed `Y` row is kept with an empty value, which is the notes/8 shape. A row that is not `Y` and whose mapped percents are all missing emits nothing. There is no student-count column, so a blank or `N/A` is not stored as 0. A published 0 is kept. The 2020 ineffective-teacher placeholder, a row whose only number is students with disabilities 0.00, does not occur in this file. A constructed row with that shape keeps the 0.

Every mapped cell is numeric, so the KPI count is the source-row count times nine:

| Year | Source rows | KPI rows |
| --- | --- | --- |
| 2020 | 2,008 | 18,072 |
| 2021 | 2,008 | 18,072 |
| 2022 | 2,010 | 18,090 |
| 2023 | 2,015 | 18,135 |
| 2024 | 2,022 | 18,198 |
| 2025 | 2,025 | 18,225 |
| 2026 | 2,019 | 18,171 |

2,008 × 9 = 18,072. 2,010 × 9 = 18,090. 2,015 × 9 = 18,135. 2,022 × 9 = 18,198. 2,025 × 9 = 18,225. 2,019 × 9 = 18,171. KYRC26 Title I is 1,299 × 9 = 11,691. Not Title I is 542 × 9 = 4,878. Equity Gap is 178 × 9 = 1,602. 11,691 + 4,878 + 1,602 = 18,171. The prior-year total is 18,072 + 18,090 + 18,135 + 18,198 + 18,225 = 90,720. 90,720 + 18,072 + 18,171 = 126,963.

KYRC26 has 12,269 zero cells and 1,293 rows on which all nine percents are 0. Those zeros are published and are kept. Adair County `001000` Title I All Students is 0.0, and Non-White on the same row is 0.3.

The equity gap is stored as published. Issue #59 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/59) tracks the signed gap. This pull request does not recompute it and does not drop a negative.

| Year | Paired cells | Title I minus Not Title I | Other |
| --- | --- | --- | --- |
| 2020 | 1,593 | 1,593 | 0 |
| 2021 | 1,593 | 1,593 | 0 |
| 2022 | 1,593 | 1,593 | 0 |
| 2023 | 1,593 | 1,593 | 0 |
| 2024 historical | 1,602 | 1,323 | 279 |
| 2025 datasets | 1,602 | 1,422 | 180 |
| 2026 | 1,602 | 1,570, plus 32 off by 0.1 | 0 |

The paired cells are the 177 or 178 entities that have all three Title I rows, times nine groups. 177 × 9 = 1,593. 178 × 9 = 1,602. A negative cell in 2020-2023 and in KYRC26 is one where Title I is lower. Negative cells stored: 2020 has 32, 2021 has 109, 2022 has 80, 2023 has 118, KYRC24 has 0, KYRC25 has 0, and KYRC26 has 95. 32 + 109 + 80 + 118 + 95 = 434. All 434 are equity-gap cells, and all are `suppressed` `N`. State KYRC26 English learner is Title I 8.0 and Not Title I 14.0, and the published gap is −6.0. State students with disabilities is Title I 7.4 and Not Title I 4.5. 7.4 − 4.5 = 2.9, and the published gap is 3.0. That 0.1 is one of the 32. The stored value is 3.0. The registered KYRC24 file does not follow one difference: 1,323 cells are Title I minus Not Title I, 93 equal Not Title I, 93 are Not Title I minus Title I, 12 equal Title I, and 81 match none of those. Russell County `495000` Non-White is Title I 0.5, Not Title I 2.9, and the gap is 2.9. Store 2.9.

The comma strip does not change a published row. No registered percent contains a comma. It keeps a comma-formatted percent alive in the unit test, on object and on StringDtype. `config/mappings.yaml` has no dtype cast for this source. pandas is unpinned. `bool(pd.NA)` is not used. pandas 2.3.3 and pandas 3.0.6, both with the production `mappings.yaml` config, write the same 126,963 rows apart from `last_updated`. A cell-by-cell compare of 2021-2025, against output from the base code on the registered files, found no difference (90,720 shared rows). The base run skipped KYRC26 because `include_kyrc26` was false, and 2020 was not registered, so the base total is 90,720.

KYRC26 source rows are 2,019. KYRC25 source rows are 2,025. School codes go from 1,491 to 1,485: 16 removed and 10 added. 1,491 − 16 + 10 = 1,485. All Schools codes stay 178, including state `999000`. No district total is added or removed. 178 + 1,491 = 1,669 codes in KYRC25. 178 + 1,485 = 1,663 in KYRC26. The 16 removed codes are schools in districts that remain, one row each. They include Young Elementary `275374`, Wurtland Middle `221024`, and the three Kentucky Tech System schools `777156`, `777158`, and `777159`. The 10 added codes are schools in districts that remain, one row each. They include Anderson County Learning Academy `011030`, Mary E. Britton Middle `165220`, and Early Learning Academy `571006`. None is a co-op total `901`–`909`. On the 1,653 shared codes, 19 status keys leave and 19 arrive. Each of those 19 codes flips between Title 1 and Not Title 1. 2,025 − 16 + 10 + 19 − 19 = 2,019. The flips include Camp Ernst `035010`, Cassidy `165032`, Edythe Jones Hayes `165034`, and Garrett Morgan `165099`.

`CO-OP` on KYRC26 uses `GLEC` where KYRC25 used `GREC`. The co-op code stays `909`. KYRC25 has 169 `GREC` codes and 171 rows. Young Elementary `275374` is the one removed code. The other 168 become `GLEC`. No added code is `GLEC`. GLEC has 168 codes and 170 rows. 171 − 1 = 170. Nothing in this pipeline joins on the co-op name.

State `999000`, district All Districts, school `All Schools` stored as `---District Total---`, district number 999, school year `20252026`, KPI year `2026`, suppressed `N`. `CO-OP` and NCES id are blank. Title I is All Students 7.3, Non-White 8.0, White 7.0, economically disadvantaged 7.4, non-economically disadvantaged 7.1, students with disabilities 7.4, without disabilities 7.3, English learner 8.0, non-English learner 7.2. Not Title I is All Students 6.4. The equity gap is All Students 0.9 and English learner −6.0.

Fayette County `165000` Title I All Students is 20.3, Not Title I is 29.5, and the equity gap is −9.2. 20.3 − 29.5 = −9.2. NCES id `2101860`, co-op `CKEC`, co-op code `901`, county `FAYETTE`, district number 165. School name `All Schools` is stored as `---District Total---`. Bryan Station High `165170` has only a Title I row. All Students is 37.8. State school id `034165170`, NCES id `210186000388`, school type `A1`. Adair County High `001010` Title I All Students is 0.1, NCES id `210003000001`, state school id `001001010`. Adair County `001000` Title I All Students is 0.0 and Non-White is 0.3, NCES id `2100030`, co-op `GRREC` / `902`.

Prior-year anchors, unchanged against the base output: KYRC25 state Title I All Students is 8.9 and Not Title I is 7.2. Fayette Title I All Students is 22.5. Bryan Station Title I All Students is 44.8, and the NCES id is the published `210186000000`. KYRC24 state Title I All Students is 8.7, Not Title I is 5.6, and Non-White is 9.7. District is `All Districts`. The `Inexperienced Value.1` header is the Non-White column. 2023 state school id `999`, district `State`, school name `---State Total---`, Title I 6.54, Not Title I 4.36, equity gap 2.18. 6.54 − 4.36 = 2.18. Breathitt `061` includes the source year cell `20022023`, and that row still yields KPI year 2023 with All Students 0.00. 2022 state Title I is 7.8, Not Title I is 3.64, and the gap is 4.16. 2021 state Title I is 8.88 and Not Title I is 3.56. 2020 state Title I is 1.41, Not Title I is 0.51, and the equity gap is 0.90. District is `State`, school name is `---State Total---`, and school id is `999`. 1.41 − 0.51 = 0.90.

The demographic check still warns that the required race labels are absent and that Non-White is unexpected. 2020-2023 also name Student without Disabilities as unexpected. 2024-2026 name only Non-White. This pull request does not change that shared list.

Fixtures are verbatim real rows in `tests/fixtures/students_taught_by_out_of_field_teachers/`, including the KYRC24 `Inexperienced Value` headers, the Breathitt `20022023` rows, and the KYRC26 equity-gap cell `−6.0` written as `-6.0`. `tests/test_fixture_sizes.py` passes.

Issue #42 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/42): `scripts/generate_report_drafts.py` selects state rows with `district == 'State'`, and `get_state_average` also requires `school_name == '---District Total---'`. These metrics are not in that script's indicator list. The 2020-2023 state rows are district `State` and school name `---State Total---`. KYRC24 through KYRC26 store district `All Districts` and school name `All Schools`, which this parser stores as `---District Total---`. Neither shape matches both conditions. `html/generate_dashboard_data.py` does not read these metrics. `analysis/config/covariate_catalog.yaml` lists `students_taught_by_out_of_field_teachers_rate`. No analysis script reads the three metric names this module emits.

`openpyxl` is already declared in `pyproject.toml` from PR #57. This module's registered files are CSV. Without the package, nothing in this directory is skipped.

Lessons for later agents:

1. HEAD the registered URLs before assuming a temporary file went 404. Every registered URL returned 200. The guessed OAA names are 404 and were never registered. Do not add a skip for a filename this directory has never used. `KYRC23_OVW_Students_Taught_by_Out_of_Field_Teachers.csv` returns 200 and is the 2022-23 extract again. School cells differ from the historical file, including a maximum difference of 200. Do not register it. `accepts_data_file` skips the name. The historical KYRC24 URL still returns 200. Leave it. The datasets copy repairs the headers and changes 381 keys. The UAT KYRC24 object flips the sign on 16 keys. The UAT KYRC25 object flips the sign on 21 keys. datasets KYRC25 has no Content-MD5, so the downloader keeps datasets. `students_taught_by_out_of_field_teachers_2020.csv` is a real 2019-20 file, not a republish of 2021. State Title I is 1.41 against 8.88. It is registered.
2. The measure is the published percent. KDE's catalogue describes students taught by out-of-field teachers, Title I against not Title I, from the Teacher Equity Report. In every registered year the group columns are within-group rates. Non-White plus White does not equal All Students. Store the published number. State Title I All Students is 7.3 in KYRC26. Do not rescale it, and do not rebuild a gap that the file already publishes. A published cell off by 0.1 from Title I minus Not Title I stays as published. State students with disabilities gap is 3.0.
3. KYRC26 has a state row `999000`, district `All Districts`, school name `All Schools` stored as `---District Total---`, year `2026`. Title I All Students is 7.3. Not Title I All Students is 6.4. The English learner equity gap is −6.0. Fayette County `165000` Title I All Students is 20.3 and the equity gap is −9.2. Bryan Station High `165170` Title I All Students is 37.8 and has no Not Title I row.
4. "Header changes: none" means the KYRC26 header matches KYRC25. The registered KYRC24 header is still the corrupted `Inexperienced Value` row. pandas is unpinned. `dtype=str` is StringDtype on pandas 3. The comma strip treats StringDtype the same as object. It does not change a published row. No registered percent contains a comma. pandas 2.3.3 and pandas 3.0.6 write the same 126,963 rows.
5. These files have no Suppressed column and no `*` cell. Every KPI row is `suppressed` `N`. A blank or `N/A` percent that is not on a `Y` row stays missing. There is no count column, so check that before treating a blank percent as zero. No registered year has a blank measure cell, so there is no grade-not-offered pattern and no disability-only placeholder. A published 0 is kept. Adair County `001000` Title I All Students is 0.0. Bryan Station High in KYRC26 is 37.8, not a suppression.
6. Compare the code sets, then the counts. 1,491 − 16 + 10 = 1,485 school codes. All Schools codes stay 178. The 16 removed codes and the 10 added codes are schools in districts that remain. They are not co-op totals `901`–`909`. On shared codes, 19 status keys flip from one Title I label to the other, so source rows are 2,025 − 16 + 10 + 19 − 19 = 2,019. KYRC26 emits nine groups on every row, so 2,019 × 9 = 18,171. KYRC25 is 2,025 × 9 = 18,225. `GREC` becomes `GLEC` on the shared codes. The co-op code stays `909`. 169 − 1 = 168 codes, and the row count goes from 171 to 170.
7. Prior-year identity against the base code is 18,072 / 18,090 / 18,135 / 18,198 / 18,225 for 2021-2025, on pandas 2.3.3 and on pandas 3. No previously published cell changes. 2020 is new: 18,072 rows, state Title I 1.41. KYRC26 is new: 18,171 rows, state Title I 7.3 and Not Title I 6.4. The total is 126,963.
8. State district is `All Districts` for 2024-2026 and `State` for 2020-2023. School name `All Schools` is stored as `---District Total---`. `---State Total---` is left as written. The 2020-2023 state school code stays `999`. KYRC24 through KYRC26 use `999000`. Issue #61 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/61) tracks that identifier change. This pull request does not pad the codes. Issue #42: `scripts/generate_report_drafts.py` selects `State` and also requires `---District Total---`. These metrics are not in the indicator list. Neither shape matches both conditions. Each report-card year is its own survey. State Title I All Students is 1.41, 8.88, 7.8, 6.54, 8.7, 8.9, and 7.3.
9. `openpyxl` is already a project dependency from PR #57. This module does not add a workbook.
10. No shared parser or dependency changes. `BaseETL` was not edited. The comma strip stays in this module and does not change a published row. pandas is unpinned. pandas 2.3.3 and pandas 3.0.6 write the same 126,963 rows. A negative equity-gap cell is stored. In 2020-2023 that cell is Title I minus Not Title I. In KYRC26 the same difference holds, and 32 cells are off by 0.1. In the registered KYRC24 file and in KYRC25 the published gap does not follow one rule. Issue #59 tracks the signed gap. The datasets KYRC24 file is still not the registered source. A follow-up should decide whether that repaired file replaces the historical extract. The UAT sign flips on KYRC24 and KYRC25 are not loaded.

### `teacher_turnover` — S

| | |
| --- | --- |
| KYRC26 files | `KYRC26_OVW_Teacher_Turnover.csv` |
| Header changes | Added `Instructional Staff Count`, `Instructional Staff Turnover Count`, `Instructional Staff Turnover Percent`. Existing turnover columns are unchanged |
| Fixtures / assertions | State `999000` row on an existing turnover column, year `2026` |
| Gotchas | New columns are optional. Do not require them unless the owner asks for those KPIs |

### `teacher_certification` — S

| | |
| --- | --- |
| KYRC26 files | `KYRC26_OVW_Teacher_Certification_Data.csv` |
| Header changes | Added Rank I and Rank II count and percent. Existing columns are unchanged |
| Fixtures / assertions | State `999000` row on an existing certification column, year `2026` |
| Gotchas | New columns are optional |

### `dual_credit` — S

| | |
| --- | --- |
| KYRC26 files | `KYRC26_EDOP_Dual_Credit_Courses_Offered.csv`, `KYRC26_EDOP_Dual_Credit_Participation_and_Performance.csv` |
| Header changes | Courses offered: `Couse Completers` became `Course Completers`, already mapped. Participation headers are identical. State `999000` rows remain |
| Fixtures / assertions | State `All Enrollments` on courses offered: enrollment 143,848, completers 132,208, qualifying 124,596. School year `20252026` |
| Gotchas | These are real 2025-26 extracts. Do not hold the module for a missing file. `etl/dual_credit.py` also aliases `'Couse Completers '` and `'COUSE COMPLETERS '` with a trailing space. The live KYRC25 header has no trailing space. That is a develop mismatch for the old file, not a KYRC26 requirement. Do not treat the stray alias as required |

### `gifted_talented` — S

| | |
| --- | --- |
| KYRC26 files | `KYRC26_EDOP_Gifted_Participation_by_Grade_Level.csv`, `KYRC26_EDOP_Gifted_Participation_by_Category.csv` |
| Header changes | By-grade dropped `Grade 14`. Category file is identical |
| Fixtures / assertions | State row, year `2026`. Grade 14 is absent |
| Gotchas | Do not fail the test because Grade 14 is missing |

### `advanced_coursework` — S

| | |
| --- | --- |
| KYRC26 files | `KYRC26_OVW_Advanced_Coursework.csv`, `KYRC26_EDOP_Advanced_Courses_Offered.csv`, `KYRC26_EDOP_Advanced_Courses_Participation_and_Performance.csv` |
| Header changes | None on all three. EDOP files still have state `999000` rows |
| Fixtures / assertions | Overview: 4,077 rows, 257 school codes, `Collected School Year` `20212022` on every row, `School Year` `20252026`, no `999000`. EDOP: state `999000` still present |
| Gotchas | KYRC25 overview was 36,612 rows and 1,356 schools, collected year `20202021`, and also had no `999000`. Do not copy the KYRC25 overview total into the fixture. The smaller set is the file, not a truncated download |

### `school_courses` — S

| | |
| --- | --- |
| KYRC26 files | `KYRC26_EDOP_School_Courses_Summary.csv` |
| Header changes | None |
| Fixtures / assertions | State `999000` row, year `2026` |
| Gotchas | None beyond the usual opt-in |

### `cte_pathways` — S

| | |
| --- | --- |
| KYRC26 files | `KYRC26_CTE_Career_Pathways.csv` |
| Header changes | None |
| Fixtures / assertions | State `999000` row, year `2026` |
| Gotchas | District `606` is new versus KYRC25. Not a header change |

### `career_readiness_indicators` — S

| | |
| --- | --- |
| KYRC26 files | `KYRC26_CTE_Career_Readiness_Indicators.csv` |
| Header changes | None |
| Fixtures / assertions | State `999000` row, year `2026` |
| Gotchas | District `880` / school `880000` is in KYRC25 and absent in KYRC26. Not a header change |

### `district_school_list` — S

| | |
| --- | --- |
| KYRC26 files | `KYRC26_OVW_District_School_List.csv` |
| Header changes | `Co-Op` / `Co-Op Code` became `CO-OP` / `CO-OP Code`. `BaseETL` maps both |
| Fixtures / assertions | A school row and a district row resolve `co_op`. Co-op value `GREC` is now `GLEC` |
| Gotchas | 1,669 school codes in KYRC25, 1,663 in KYRC26. Measure columns are unchanged. Duplicate `co_op` columns are allowed when both spellings are present |

### `kindergarten_readiness` — S

| | |
| --- | --- |
| KYRC26 files | `KYRC26_ASMT_Kindergarten_Screen_Composite.csv` |
| Header changes | None versus the KYRC25 composite |
| Fixtures / assertions | State `All Students` / prior setting `All Students`: 55 / 38 / 7 / 45. Year `2026`. Readiness rate 45 |
| Gotchas | Those columns are named like counts and the values are percents, which is how the KYRC24 composite already behaves. Do not divide them again. Do not also register `Kindergarten_Screen_2026`. Leave the temporary 2025 file in place for 2024-25 |

### `kentucky_summative_assessment` — M

| | |
| --- | --- |
| KYRC26 files | `KYRC26_ACCT_Kentucky_Summative_Assessment.csv`, `KYRC26_ASMT_Kentucky_Summative_Assessment.csv` |
| Header changes | Accountability file identical. By-grade file is co-op spelling only. `Proficient / Distinguished` and grades such as `10th` are already mapped |
| Fixtures / assertions | Reading proficient/distinguished 50 / 48 / 49 and combined writing 41 / 47 / 45 by level. One grade row from the by-grade file. Subject slug for combined writing stays `combined_writing` |
| Gotchas | Size M because the pull request has to show level rates and one grade, not because a new header is expected. Do not also load `Assessment_Performance_by_Grade_2026`. After one opt-in pull request has landed |

### `safe_schools_discipline` — S

| | |
| --- | --- |
| KYRC26 files | `KYRC26_SAFE_Discipline_Resolutions.csv`, `KYRC26_SAFE_Legal_Sanctions.csv` |
| Header changes | Resolutions: `(Ssp2)` became `(SSP2)`. Legal-sanction headers already match (`Arrests`, `Charges`, `Civil Proceedings`, `Court Designated Worker Involvement`, `School Resource Officer Involvement`) |
| Fixtures / assertions | One resolution row and one legal-sanction row for year `2026`, including out-of-school suspensions |
| Gotchas | Add aliases next to the KYRC24 titles: `Out-of-School Suspensions (SSP3)`, `Removal by Hearing Officer (IAES2)`, `Unilateral Removal by School Personnel (IAES1)`, `In-School Removal (INSR) or In-District Removal (INDR) >=.5`. `(SSP2)` already matches KYRC26. Keep `(Ssp2)` if KYRC25 is loaded in the same change. `include_kyrc26` does apply. The same two files are also registered under `safe_schools` |

### `safe_schools_events` — M

| | |
| --- | --- |
| KYRC26 files | `KYRC26_SAFE_Behavior_Events_by_Type.csv`, `KYRC26_SAFE_Behavior_Events_by_Grade_Level.csv`, `KYRC26_SAFE_Behavior_Events_by_Location.csv`, `KYRC26_SAFE_Behavior_Events_by_Context.csv` |
| Header changes | Type, location, and context headers match the KYRC24 strings in code. Grade headers are `All Grades`, `PS`, `K`, `1st` through `12th`, and `14th`. There is no `13th` |
| Fixtures / assertions | One event from each file, year `2026`. A `PS` row and a `1st` row land on the preschool and grade-1 metrics |
| Gotchas | `include_kyrc26` does not apply. Add the four names to the hardcoded list in `transform`. Use `normalize_kyrc_grade_token`. The module will ignore the files until that list names them |

### `safe_schools_climate` — M

| | |
| --- | --- |
| KYRC26 files | `KYRC26_ACCT_Index_Scores.csv`, `KYRC26_SAFE_Precautionary_Measures.csv` |
| Header changes | Index headers already map (`Climate Index`, `Safety Index`). Precautionary dropped `All classroom doors lock from the inside`. `All outside doors are locked during school hours` was already on the KYRC25 file |
| Fixtures / assertions | Elementary, middle, and high index rows that share `All Schools` all survive. One precautionary question stores, including outside doors |
| Gotchas | `include_kyrc26` does not apply. `get_files_to_process` will not see `KYRC26_ACCT_Index_Scores.csv` until the filename list names it. Dedup omits level, so those three `All Schools` rows collapse; put level on the dedupe key or the metric name. Precautionary code maps shortened questions (`Visitors required to sign-in`), not `Visitors are required to sign-in to the building`. Outside doors has no mapping; store the question. Leave the compliance-rate item list until the owner decides. Do not register the 297 MB survey unless question-level KPIs are wanted. `test_get_files_to_process` already fails on develop (expects 4 files, finds 6); that failure is pre-existing |

### `spending_per_student` — S, blocked

| | |
| --- | --- |
| KYRC26 files | `KYRC26_FT_Spending_per_Student.csv` |
| Header changes | Unknown until the name returns 200 |
| Fixtures / assertions | Add them after a header check against the KYRC25 file |
| Gotchas | 404 on both containers. Do not register it. Five other KYRC25 spending variants are also 404 and are not this module. Wait does not change because of those variants |

### `financial_summary` — S, blocked

| | |
| --- | --- |
| KYRC26 files | `KYRC26_FT_Financial_Summary.csv` |
| Header changes | Unknown until the name returns 200 |
| Fixtures / assertions | Add them after a header check against the KYRC25 file |
| Gotchas | 404 on both containers. Do not register it |

### `act_scores` — do not implement in this round

| | |
| --- | --- |
| KYRC26 files | None for ACT. `KYRC26_ASMT_CAE.csv` is SAT |
| Header changes | SAT replaces `Reading`, `English`, and `Composite Score` with `Reading and Writing` and `Total Score` |
| Fixtures / assertions | None in `act_scores` |
| Gotchas | The configured download is still `KYRC24_ASMT_The_ACT.csv`. Do not append SAT scale scores to the ACT series. SAT is a separate decision, not the next S script |

### No KYRC26 parser work

| Module | What to do |
| --- | --- |
| `census_acs`, `census_saipe` | Census APIs. No KYRC parser work. Their live-API tests are on the develop failure list |
| `school_tract_geocoding` | No KYRC extract. Rerun after `district_school_list` has 2026 rows if new schools need coordinates |

These configured directories have a KYRC26 file registered and no module in `config/mappings.yaml`: `benchmark_assessment` (`Reading` and `English` became `Reading and Writing`), `student_retention`, `dropout_rate`, `homeless_students`, `migrant_students`, `students_with_disabilities`, `educator_qualifications`, and `students_taught_by_inexperienced_teachers`. No script pull request.

On the blob, not registered, no ETL, do not add unless a module is in scope: `ASMT_CAE`, `OVW_Chronic_Absenteeism_CRDC`, ACCT Profile / Summary / School_Improvement / Assessment_Participation, the eight ACCT crosstabs, Survey_Results, the seven `*_Datasets.zip` packages, and the other overview and assessment files named in journal 105.

## Suggested order

1. `chronic_absenteeism`. Merged. Unchanged headers, state rate 23.5. See the pilot notes above before copying the pattern.
2. `graduation_rates`. In review. State 94.1 and 94.5. The 5-year cohort header was unmapped. See that module's notes before copying the pattern onto the next file.
3. The other opt-in modules above, in the order listed, through `kindergarten_readiness`. `students_taught_by_out_of_field_teachers` is in review in PR #65. It stores the published percent. The group columns are within-group rates. A published 0 is kept. A negative equity-gap cell is stored. `students_taught_by_ineffective_teachers` is in review in PR #63. It stores the published percent, keeps a published 0, and leaves `N/A` and `#N/A` missing because there is no count column. `teacher_working_conditions` is in review in PR #60. It stores percent favorable, keeps a published 0, and does not turn a literal `N/A` into 0. `novice_teachers` is in review in PR #57. It reads both inexperienced-teacher files from the `novice_teachers` directory. The same student file stays registered under `students_taught_by_inexperienced_teachers` for download only. `student_teacher_ratio` is in review in PR #55. It fixtures a school row and does not rebuild district or state totals. `advanced_coursework` expects 257 overview schools and collected year `20212022`.
4. `kentucky_summative_assessment` after one opt-in has landed.
5. `safe_schools_discipline`, then `safe_schools_events`, then `safe_schools_climate`.
6. `spending_per_student` and `financial_summary` when those KYRC26 names return 200.
7. SAT (`ASMT_CAE`) only if a separate series is in scope. No script for the CRDC rename unless a CRDC KPI is in scope.

No S module needs to become M because of a new header.
