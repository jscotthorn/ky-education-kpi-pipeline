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
| `postsecondary_enrollment` | In review in PR #43 (`cursor/postsecondary-enrollment-kyrc26-5640`). Opt-in only. Every registered prior-year URL returned 200, and no temporary OAA name was ever registered, so there is no source swap and no leftover-file skip. Headers match KYRC25. There is no Suppressed column. A `*` cell is already a missing-value token, so a fully starred group emits no KPI row. There are no blank measure cells in 2021-2026, and the base converter already skips an empty non-Y metric, so prior-year KPI counts stay 18,737 / 7,781 / 9,509 / 9,625 / 9,679. KYRC26 writes 10,657. No shared parser or dependency changes. Details are under that module |

`chronic_absenteeism` was re-tested on this branch after the review fixes: `42 failed, 394 passed, 104 skipped`. The 42 failure node ids match the develop baseline above. Run `python3 scripts/compare_pytest_to_journal105.py` and paste the report into the pull request. Do not commit a per-script diff file. The extra passes are the shared-prep tests already on `feature/2025-26-kyrc26` plus the chronic-absenteeism tests added here. Do not treat 375 passed as the number to match once shared prep is in the base.

`graduation_rates` compare run after the 2024 temporary-file skip, pandas 3.0.6: `42 failed, 400 passed, 104 skipped`, errors 0. The 42 failure node ids match the develop baseline. The extra pass beyond the KYRC25 source swap is `test_leftover_temporary_file_does_not_duplicate_2024`. Do not treat 394 passed as the number to match once this module merges.

`english_learner_progress` compare run, pandas 3.0.6: `42 failed, 406 passed, 104 skipped`, errors 0. The 42 failure node ids match the develop baseline. The six passes beyond the graduation-rates total are the KYRC26 acceptance test, the KYRC25 source-swap test, the leftover-temporary-file test, the blank-and-partial-blank test, the sibling-file test, and the real-row fixture test. Do not treat 400 passed as the number to match once this module merges.

`postsecondary_readiness` compare run, pandas 3.0.6: `42 failed, 411 passed, 104 skipped`, errors 0. The 42 failure node ids match the develop baseline. The five passes beyond the english-learner total are the KYRC26 acceptance test, the KYRC25 source-swap test, the leftover-temporary-file test, the blank-row skip test, and the real-row fixture test. Do not treat 406 passed as the number to match once this module merges.

`postsecondary_enrollment` compare run, pandas 3.0.6: `42 failed, 415 passed, 104 skipped`, errors 0. The 42 failure node ids match the develop baseline. The four passes beyond the readiness total are the KYRC26 acceptance test, the registered-source test, the blank-row and sibling-file test, and the real-row fixture test. Do not treat 411 passed as the number to match once this module merges.

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
5. Re-read the module gotcha before requiring a `999000` row. `out_of_school_suspension` has no `999000` row in either year and the KPI year has to come from `School Year`, not the collected year. `student_teacher_ratio` has no state row and no district `All Schools` row in KYRC26; fixture a school row. Do not add a shared helper for a token or header that only one script needs.
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

3,213 + 2,517 + 590 = 6,320. 6,358 + 2,602 + 562 = 9,522. 2,827 + 3,035 + 650 = 6,512. A blank cell does not appear in any of these files, or in the 2021-2023 files. The only non-numeric tokens are `*` (KYRC24, KYRC25, KYRC26), plus `**` in 2022 and 2023 and `<10` in 2021. All three are already in `MISSING_VALUE_INDICATORS`. There is no new suppression token and no Suppressed column, so the output has no `suppressed` `Y` row. A `*` metric is absent. A fully starred group emits nothing. That is the same output as a group the file omits. notes/8's blank-row rule was already what the base converter does: an empty non-Y metric is not written. Zero blank source rows means the prior-year counts do not move. Do not start emitting `*` as `suppressed` `Y`. That would add a row for every starred cell in 2021-2025.

The locked full-suppression example is Bryan Station `165170`, `Asian`: `*` in KYRC24, KYRC25, and KYRC26, and no KPI row in any year. Fayette `165000`, `American Indian or Alaska Native`, is the same in KYRC25 and KYRC26. Adair County High School `001010`, All Students, is fully starred in KYRC24 and KYRC25 (no KPI row) and in KYRC26 publishes only the cohort, 201. The six enrollment cells stay `*`.

The new KYRC26 pattern is a private-college star beside published public and total cells. Seven rows. The locked one is Bowling Green High School `042070`, All Students: cohort 301, public 151, total enrolled 167, public rate 50.2, total rate 55.5. Private count and private rate are `*` and are not emitted. The other five metrics are emitted with `suppressed` `N`.

District `602` is Kentucky School for the Blind. It is absent from KYRC25. KYRC24 already had `602000` (6 all-star rows) and `602176` (5 all-star rows, school type `D1`). KYRC26 has both codes again, 8 demographics each, all `*`, school type `D1` on `602176`. Those 16 rows emit no KPI row. The new code does not change the KPI counts. County is `JEFFERSON`. District NCES id `2100094`. School NCES id `210009401779`.

`CO-OP` `GREC` is 774 rows and 43 school codes in KYRC25, all Jefferson County codes beginning `275`. KYRC26 labels 40 codes `GLEC` (486 rows) and has no `GREC` rows. 39 codes overlap. The four GREC codes that do not return are removed schools `275164`, `275463`, `275620`, and `275768`. The new GLEC code is `275191`, W.E.B. DuBois Academy. Nothing in this pipeline groups or joins on the co-op name.

`KYRC26_ADLF_Current_Year_Graduates.csv` and `KYRC26_ADLF_Graduate_Outcomes.csv` are not registered under this directory. Graduates has `Graduate Count`. Outcomes has `College`, `Military`, `Working`, and percent columns, and no `Demographic`. Neither header is one of the seven mapped measures, so a parse emits no KPI rows. No filename guard.

Follow-up, not fixed here. `scripts/generate_report_drafts.py` selects state rows with `district == 'State'`, then `get_state_average` also requires `school_name == '---District Total---'`. The 2021-2023 files store district `State` and school name `---State Total---` (school code `999`). KYRC24, KYRC25, and KYRC26 store district `All Districts` and school name `All Schools`, which this parser stores as `---District Total---`. The state average for this metric is missing in every year until that lookup is updated. This module emits no empty value, so the NaN guard in issue #42 is not reached by these rows. `analysis/scripts/postsecondary_enrollment_analysis.py` keeps year range `(2021, 2025)` and `school_type == 'A1'`. 2026 school rows will not enter until the year cap moves. School type `A1` is already on the school rows (2,999 source rows, 229 schools). 169 of those schools publish an All Students total rate.

Both years' demographic columns are the same 18 labels, all already in the 2026 demographic config. The full-file run reported 18 found and no unexpected labels. No new KPI names.

Fixtures are real rows in `tests/fixtures/postsecondary_enrollment/`: ten KYRC26 rows, seven KYRC25 rows, and six KYRC24 rows, including the Bryan Station Asian suppression, the Fayette American Indian suppression, the Adair County High School partial row, the Bowling Green private-college star, and both district `602` rows. `tests/test_fixture_sizes.py` passes.

Lessons for later agents:

1. HEAD the registered URLs before assuming a temporary file went 404. This module's KYRC25 entry was already the permanent file, and it returns 200 on `datasets`, on UAT, and on the historical host, with one MD5. The guessed OAA names are 404 and were never registered. Do not add a skip for a filename this directory has never used. The 2021-2023 names stay.
2. "Header changes: none" means the KYRC26 header matches KYRC25. The total-rate header is `Percentage College Enrolled In State Table` on the KYRC files and `PERCENTAGE COLLEGE ENROLLED IN STATE` on the 2021-2023 files. Both were already mapped. A rate without a percent sign still goes through `clean_percentage_values`.
3. This file has no Suppressed column. `*` is a cell, not a row flag, and it is already a missing-value token. A fully starred group emits no KPI row. Do not invent `suppressed` `Y` for those cells. That would change every prior year. A blank cell, which these files do not contain, is the same output: the base converter skips an empty non-Y metric. notes/8 does not require a new skip here.
4. Compare the suppression token set, then the counts. The KYRC token is still `*`. KYRC26 is smaller because it omits all-star demographic rows KYRC25 included, and it writes more KPI rows because more of the remaining cells are numbers. `*` cells are not KPI rows. The source-row drop and the KPI-row increase are not a contradiction.
5. The temporary-to-permanent gap does not apply. There is no temporary file. The 14 schools KYRC26 drops are not co-op totals. Three are `A1` and eleven are `A5`, `A6`, or `A8`. 14 × 18 = 252 source rows, and those rows held 11 published cells.
6. Prior-year identity is a diff against output the base parser already wrote, before `include_kyrc26` was set. 2021-2025 match that file. Published state, Fayette, Bryan Station, and Adair County rates are unchanged.
7. State district is `All Districts` for 2024-2026. `scripts/generate_report_drafts.py` still selects `State`, and it also requires school name `---District Total---`. The 2021-2023 state rows are district `State` and school name `---State Total---`, so that lookup misses every year of this metric. The enrollment analysis script still stops at 2025 and requires `school_type == 'A1'`.
8. District `602` is a code-domain change versus KYRC25, not a header change. KYRC24 already had the Kentucky School for the Blind codes, all starred. KYRC26's 16 rows are starred too and emit nothing. Do not add a shared helper for that code.
9. Before adding a filename guard, parse one sibling row. Current Year Graduates and Graduate Outcomes emit nothing. `GREC` became `GLEC` on the Jefferson codes that remain. Nothing in this pipeline joins on the co-op name.

### `cte_participation` — S

| | |
| --- | --- |
| KYRC26 files | `KYRC26_CTE_Participation.csv` |
| Header changes | None |
| Fixtures / assertions | State participants 144,661 / enrollments 201,255. Counts already go through `clean_numeric_with_commas` |
| Gotchas | None beyond the usual opt-in |

### `student_enrollment` — S

| | |
| --- | --- |
| KYRC26 files | `KYRC26_OVW_Student_Enrollment.csv` |
| Header changes | None |
| Fixtures / assertions | State `999000` row, year `2026` |
| Gotchas | Preschool membership lives on `OVW_Student_Membership`, which is not this file and has no ETL |

### `secondary_enrollment` — S

| | |
| --- | --- |
| KYRC26 files | `KYRC26_OVW_Secondary_Enrollment.csv` |
| Header changes | None |
| Fixtures / assertions | State `999000` row, year `2026` |
| Gotchas | None beyond the usual opt-in |

### `out_of_school_suspension` — S

| | |
| --- | --- |
| KYRC26 files | `KYRC26_OVW_Student_Suspensions.csv` |
| Header changes | None versus KYRC25. Count columns are already in `module_column_mappings` |
| Fixtures / assertions | KPI year `2026` from `School Year`. `collected_school_year` is `20212022` on every row |
| Gotchas | KYRC25 collected year was `20202021`. Do not file the KPI under the collected year. Neither year has a `999000` row. The module currently consumes only the KYRC24 file; the KYRC25 twin exists and was not configured |

### `teacher_experience` — S

| | |
| --- | --- |
| KYRC26 files | `KYRC26_OVW_Average_Years_School_Experience.csv` |
| Header changes | None |
| Fixtures / assertions | State `999000` row, year `2026` |
| Gotchas | None beyond the usual opt-in |

### `student_teacher_ratio` — S

| | |
| --- | --- |
| KYRC26 files | `KYRC26_OVW_Student_to_Teacher_Ratio.csv` |
| Header changes | None |
| Fixtures / assertions | One school row. School code is 6 digits and does not end in `000`. Do not assert `999000` or `School Name` `All Schools` |
| Gotchas | KYRC26 has 1,432 school rows. District number `999` and the KYRC25 district-total codes that end in `000` are gone (KYRC25 had the state row, ratio `15:01`, and 173 `All Schools` district rows). Stays S only if the pull request does not rebuild district or state totals |

### `novice_teachers` — S

| | |
| --- | --- |
| KYRC26 files | `KYRC26_OVW_Inexperienced_Teachers.csv`, `KYRC26_OVW_Students_Taught_by_Inexperienced_Teachers.csv` |
| Header changes | None |
| Fixtures / assertions | State `999000` row on each file, year `2026` |
| Gotchas | The same inexperienced-teachers student file is also registered under `students_taught_by_inexperienced_teachers` for download only. This ETL reads the `novice_teachers` copy |

### `teacher_working_conditions` — S

| | |
| --- | --- |
| KYRC26 files | `KYRC26_OVW_Teacher_Working_Conditions.csv` |
| Header changes | None. No `Demographic` column |
| Fixtures / assertions | State `999000` row, year `2026` |
| Gotchas | Do not require a demographic value |

### `students_taught_by_ineffective_teachers` — S

| | |
| --- | --- |
| KYRC26 files | `KYRC26_OVW_Students_Taught_by_Ineffective_Teachers.csv` |
| Header changes | None |
| Fixtures / assertions | State `999000` row, year `2026` |
| Gotchas | None beyond the usual opt-in |

### `students_taught_by_out_of_field_teachers` — S

| | |
| --- | --- |
| KYRC26 files | `KYRC26_OVW_Students_Taught_by_Out_of_Field_Teachers.csv` |
| Header changes | None |
| Fixtures / assertions | State `999000` row, year `2026` |
| Gotchas | None beyond the usual opt-in |

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
3. The other opt-in modules above, in the order listed, through `kindergarten_readiness`. `student_teacher_ratio` fixtures a school row. `advanced_coursework` expects 257 overview schools and collected year `20212022`.
4. `kentucky_summative_assessment` after one opt-in has landed.
5. `safe_schools_discipline`, then `safe_schools_events`, then `safe_schools_climate`.
6. `spending_per_student` and `financial_summary` when those KYRC26 names return 200.
7. SAT (`ASMT_CAE`) only if a separate series is in scope. No script for the CRDC rename unless a CRDC KPI is in scope.

No S module needs to become M because of a new header.
