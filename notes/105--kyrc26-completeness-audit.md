# 105 - Independent completeness audit of the KYRC26 map

Audited 17 October 2026 against PR #36 (`cursor/kyrc26-shared-prep-61fb`, journal 104). This note does not change parsers or `kde_sources.yaml`. The header diff is `notes/kyrc26-schema-diff-report.md`, produced by `scripts/kyrc_schema_diff.py`.

No request to the blob or to `reportcard.kyschools.us` returned 403, 429, or 503. Those statuses would have been recorded as blocked, not as missing. `https://openhouse.education.ky.gov/Home/SRCData` failed local TLS verification, so that older catalog was not read. Blob listing is not public: GET `datasets?restype=container&comp=list` returns 404 `ResourceNotFound`, and HEAD on the same URL returns 405. That is disabled listing, not a bot block.

## What was inventoried

Primary filename source: the dataset API the download page renders.

`https://reportcard.kyschools.us/api/app/data-set-pages/c340f7d5-efbd-5fb8-cab8-3a128835f84c/with-tree?year=20252026`

The page config sets the directory to `/datasetsuat6051276` and the prefix to `KYRC{{YEAR}}_`. The client builds each link as that prefix, the group code (`OVW_`, `ACCT_`, `ASMT_`, `CTE_`, `EDOP_`, `SAFE_`, `ADLF_`, `FT_`), and the item filename, then replaces spaces and hyphens with underscores. For 2025-26 the tree has 106 active items: 99 CSVs and 7 package zips. Finance is absent. 2024-25 through 2021-22 each have 121 items, which is the same tree plus the `FT_` group.

Also probed: the Open House historical page (99 `KYRC25_` names plus `KYRC24_ASMT_Alternate_Assessment_Participation`), the financial-transparency page (KYRC24 `FT_` only), the 2025-26 temporary catalog, supplemental pages, `KYRC24`/`KYRC25` names in `notes/kde_datasets_*.md`, names in `config/kde_sources.yaml`, and naming variants (`The_ACT`, `SAT`, `CRDC_Chronic_Absenteeism`, English-learner attainment, the extra spending names). Variants that are not in the tree returned 404 on both containers.

## Containers

The download page links `datasetsuat6051276`. PR #36 downloads `datasets`. All 99 KYRC26 CSVs exist on both. Byte MD5 matches on all 99. Length matches on the 7 zips as well (the zips were not hashed separately). Last-Modified does not match: `datasets` is `Thu, 15 Oct 2026 10:00:09` through `10:00:11 GMT` for the CSVs, and `datasetsuat6051276` is earlier (6-15 October). ETags differ because the objects were written twice, not because the bytes differ.

`datasets` is a later copy of the same release the page publishes from the UAT container. Using it for this round is content-correct. The downloader only re-checks `datasets`. An update that lands only on `datasetsuat6051276` would not replace the local file. Before a production download, re-HEAD both containers. If the hashes diverge, take the newer stamp and say which container it came from.

`datasetsdev` still 404s for `KYRC26_OVW_Chronic_Absenteeism.csv`.

## Files the prefix swap missed

These KYRC26 objects are real and are not in the 99-name KYRC25 historical list.

| File | What it is |
| --- | --- |
| `KYRC26_ASMT_CAE.csv` | Rename of `KYRC25_ASMT_The_ACT.csv`. The KYRC26 file is SAT, not ACT. State `All Students`: Mathematics 437.0, Science 59.3, Reading and Writing 470.9, Total Score 908.0. Those are the temporary `College Admissions Exam_2026.CSV` figures. Headers replaced `Reading`, `English`, and `Composite Score` with `Reading and Writing` and `Total Score`. `KYRC26_ASMT_The_ACT.csv` is 404 on both containers. For KYRC25, `KYRC25_ASMT_CAE.csv` and `KYRC25_ASMT_The_ACT.csv` are the same ACT file (UAT Content-MD5 `OkcdQAt2uw1EHF12SxiGQA==`, composite 18.1). |
| `KYRC26_OVW_Chronic_Absenteeism_CRDC.csv` | Rename of `KYRC25_CRDC_Chronic_Absenteeism.csv`. Header matches the KYRC25 file that already uses the `OVW_` name. `School Year` is `20252026`. `Collected School Year` is `20242025` on every row. The KYRC25 file's collected year is `20202021`. `KYRC26_CRDC_Chronic_Absenteeism.csv` is 404 on both containers. |
| `KYRC26_{OVW,ACCT,ASMT,CTE,EDOP,SAFE,ADLF}_Datasets.zip` | Package zips. Member lists are exactly the individually linked CSVs in that group. No extra file. |

`KYRC26_ASMT_SAT.csv` and `KYRC26_ACCT_English_Learners_Attainment.csv` are 404. English learner attainment exists only in the temporary bundle and in the educational-coop zip.

## The four EDOP files

The browser inventory did not list dual credit or advanced-course participation/offerings for 2025-26. The with-tree API still has all four as active items, both containers return 200, and `KYRC26_EDOP_Datasets.zip` contains them:

- `KYRC26_EDOP_Advanced_Courses_Offered.csv`
- `KYRC26_EDOP_Advanced_Courses_Participation_and_Performance.csv`
- `KYRC26_EDOP_Dual_Credit_Courses_Offered.csv`
- `KYRC26_EDOP_Dual_Credit_Participation_and_Performance.csv`

They are 2025-26 data, not a copy of KYRC25. Dual-credit courses offered, state `All Enrollments`: enrollment 143,848 / completers 132,208 / qualifying 124,596, against 136,572 / 124,028 / 117,355 in KYRC25. `School Year` is `20252026`. The only header change on the courses-offered file is `Couse Completers` to `Course Completers`. Participation headers are identical to KYRC25. Advanced-course headers are identical to KYRC25, and the state `999000` rows are still there.

Scheduling `dual_credit` and `advanced_coursework` as S is right. Do not hold them for a missing file.

`Course Completers` is mapped in `etl/dual_credit.py`. The KYRC25 typo alias in that file is `Couse Completers ` with a trailing space. The live KYRC25 header has no trailing space. That is a develop mismatch for the old file, not a KYRC26 blocker.

## ACCT group

The with-tree payload and `KYRC26_ACCT_Datasets.zip` both contain exactly these 19 CSVs, including `Survey_Results`:

`Summary`, `Profile`, `Assessment_Participation`, `Kentucky_Summative_Assessment`, `School_Improvement`, `Crosstab_Reading`, `Crosstab_Mathematics`, `Crosstab_Science`, `Crosstab_SocialStudies`, `Crosstab_EditingMechanics`, `Crosstab_OnDemandWriting`, `Survey_Results`, `Index_Scores`, `English_Learners_Progress_Proficiency_Rate`, `4_Year_High_School_Graduation`, `5_Year_High_School_Graduation`, `Crosstab_Cohort_4_Year`, `Crosstab_Cohort_5_Year`, `Postsecondary_Readiness`.

A DOM count of 22 links is the 19 files plus the package zip and two anchors that are not additional datasets. There is no 20th ACCT CSV.

## Educational coop zip

`_SRC Assessment and Accountability Educational Coop Datasets 2026.zip` has 15 members, all temporary OAA names: the three accountable files, by-grade assessment, college admissions exam, graduation, kindergarten, postsecondary readiness, English language proficiency, English learners attainment, and the five climate-survey files. It does not contain the CSI workbook. Journal 103's description of that zip is right. Nothing in it is a KYRC-shaped file that is missing from `datasets`.

## 2024-25 download page versus the repo

The year selector is `year=20242025` on the same with-tree URL. Every `KYRC25_` file registered in `config/kde_sources.yaml` is on that list. The page also offers many files the repo does not register, including `KYRC25_ASMT_CAE.csv` (same bytes as `KYRC25_ASMT_The_ACT.csv`) and `KYRC25_OVW_Chronic_Absenteeism_CRDC.csv`. `act_scores` in the repo still downloads `KYRC24_ASMT_The_ACT.csv`. Graduation, English learners, kindergarten, by-grade assessment, and readiness still use the temporary 2025 names. That matches journal 103. It is not a KYRC26 hole.

## Header diff

Full result: `notes/kyrc26-schema-diff-report.md`. Of the 99 KYRC26 CSVs, 73 headers match the KYRC25 twin exactly, 12 change only `Co-Op` / `Co-Op Code` to `CO-OP` / `CO-OP Code`, and one changes `Expelled, Not Receiving Services (Ssp2)` to `(SSP2)`. The other 13 change measure columns. Journal 104's "72 of the files that exist match exactly" is the 73 minus the renamed CRDC file, which was not in the 97-name prefix swap. That count is right.

Column changes, all confirmed:

| File | Change |
| --- | --- |
| `ASMT_Benchmark.csv` | `Reading` and `English` removed. `Reading and Writing` added |
| `ASMT_CAE.csv` | `Reading`, `English`, `Composite Score` replaced by `Reading and Writing`, `Total Score` |
| `EDOP_Dual_Credit_Courses_Offered.csv` | `Couse Completers` to `Course Completers` |
| `EDOP_Gifted_Participation_by_Grade_Level.csv` | `Grade 14` removed |
| `OVW_Teacher_Certification_Data.csv` | Rank I and Rank II count and percent added |
| `OVW_Teacher_Turnover.csv` | Instructional-staff count, turnover count, and turnover percent added |
| `OVW_Student_Membership.csv` | `Preschool` removed |
| `SAFE_Precautionary_Measures.csv` | `All classroom doors lock from the inside` removed |
| `EDOP_Career_Studies.csv`, `EDOP_Health_Education_and_Physical_Education.csv`, `EDOP_Visual_and_Performing_Arts.csv`, `EDOP_World_Languages.csv`, `OVW_Access_to_Technology.csv` | Question columns rewritten. World languages immersion columns gained a `WL_` prefix |

`BaseETL` maps both co-op spellings. Demographic labels on the coded `Demographic` column did not gain a new value between KYRC25 and KYRC26. Suppression codes did not gain a new value. `School Year` moves from `20242025` to `20252026` on the files that use that column.

Collected year does not follow that roll on the lagging files:

| File | KYRC25 collected | KYRC26 collected |
| --- | --- | --- |
| `OVW_Student_Suspensions.csv` | `20202021` | `20212022` |
| `OVW_Student_Expulsions.csv`, arrests, harassment, violent offenses, preschool enrollment, preschool suspensions | `20202021` | `20212022` |
| `OVW_Advanced_Coursework.csv` | `20202021` | `20212022` |
| `OVW_Chronic_Absenteeism_CRDC.csv` | `20202021` | `20242025` |

Suspensions match journal 104: keep the KPI year on `School Year`. `extract_year` reads `school_year`, and the module maps `Collected School Year` to `collected_school_year`, not to `school_year`. There is no `999000` row in either year's suspension file.

Co-op value `GREC` is replaced by `GLEC` across the files that carry a co-op name. That does not change a column the parsers map.

Four subject crosstabs (`Reading`, `Mathematics`, `Science`, `SocialStudies`) include a 3-digit school code `901` (about 13,000 rows) next to 6-digit codes. Current ETLs do not read those files. Accountability, enrollment, and overview files used by the S scripts keep 6-digit codes, including `999000` and district `001000`, except student-teacher ratio below.

Behavior-by-grade headers are `All Grades`, `PS`, `K`, `1st` through `12th`, and `14th`. There is no `13th`. Journal 104's grade note is right. `normalize_kyrc_grade_token` is still not called by `safe_schools_events.py`.

## Grain the header diff does not show

`KYRC26_OVW_Student_to_Teacher_Ratio.csv` headers match KYRC25, and it is on the S list. The KYRC26 file has 1,432 school rows and no `School Code` `999000` and no `School Name` `All Schools`. KYRC25 has the state row (`999000`, ratio `15:01`) and 173 district `All Schools` rows. A state-row smoke test will fail. School rows still parse with the current module. Do not synthesize a state rate in the opt-in pull request.

`KYRC26_OVW_Advanced_Coursework.csv` headers match, and there is still no state row (there was not one in KYRC25 either). Rows fall from 36,612 to 4,077 and school codes from 1,356 to 257. `Collected School Year` is `20212022`. The two EDOP advanced-course files still have state rows. The overview file will load, and the totals will cover fewer schools than 2024-25.

Other S-list files that were counted still have `999000` rows: chronic absenteeism (288), both graduation files (24), readiness (27), kindergarten (128), CTE participation (17), transition (18), the teacher-experience family, certification, turnover, ineffective teachers, and both dual-credit files.

## Coverage, ETL to files

"Turn on" is what the script pull request still has to do. Status uses the header diff and the grain check above.

| Module | KYRC26 file | Status |
| --- | --- | --- |
| `chronic_absenteeism` | `OVW_Chronic_Absenteeism` | S. Headers identical. State row present. Suppression unchanged |
| `graduation_rates` | both `ACCT_*_High_School_Graduation` | S. Headers identical. State rows present |
| `english_learner_progress` | `ACCT_English_Learners_Progress_Proficiency_Rate` | S. Headers identical |
| `postsecondary_readiness` | `ACCT_Postsecondary_Readiness` | S. Headers identical. State rows present |
| `postsecondary_enrollment` | `ADLF_Transition_to_In_State_Postsecondary_Education` | S. Headers identical. State rows remain (18). `All Schools` rows fell from 3,060 to 2,248 |
| `cte_participation` | `CTE_Participation` | S. Headers identical. State rows present |
| `student_enrollment` | `OVW_Student_Enrollment` | S. Headers identical |
| `secondary_enrollment` | `OVW_Secondary_Enrollment` | S. Headers identical |
| `out_of_school_suspension` | `OVW_Student_Suspensions` | S. Headers identical. KPI year stays on `School Year`. Collected year is `20212022` |
| `teacher_experience` | `OVW_Average_Years_School_Experience` | S. Headers identical. State row present |
| `novice_teachers` | `OVW_Inexperienced_Teachers` and `OVW_Students_Taught_by_Inexperienced_Teachers` | S. Headers identical. State row present on the inexperienced-teacher file |
| `teacher_working_conditions` | `OVW_Teacher_Working_Conditions` | S. Headers identical. State rows present (3) |
| `students_taught_by_ineffective_teachers` | `OVW_Students_Taught_by_Ineffective_Teachers` | S. Headers identical. State rows present |
| `students_taught_by_out_of_field_teachers` | `OVW_Students_Taught_by_Out_of_Field_Teachers` | S. Headers identical |
| `teacher_turnover` | `OVW_Teacher_Turnover` | S. Three new columns. Existing columns unchanged. State row present |
| `teacher_certification` | `OVW_Teacher_Certification_Data` | S. Rank I and Rank II columns added. Existing columns unchanged. State row present |
| `dual_credit` | both `EDOP_Dual_Credit_*` | S. Real 2025-26 file. Courses-offered typo is fixed to a header the module already maps |
| `gifted_talented` | both `EDOP_Gifted_Participation_by_*` | S. By-grade drops `Grade 14`. Category file identical |
| `advanced_coursework` | three files, including `OVW_Advanced_Coursework` | S for the parser. Overview file covers 257 schools instead of 1,356. Collected year `20212022` |
| `school_courses` | `EDOP_School_Courses_Summary` | S. Headers identical |
| `cte_pathways` | `CTE_Career_Pathways` | S. Headers identical |
| `career_readiness_indicators` | `CTE_Career_Readiness_Indicators` | S. Headers identical |
| `district_school_list` | `OVW_District_School_List` | S. Co-op spelling only. Both spellings map |
| `kindergarten_readiness` | `ASMT_Kindergarten_Screen_Composite` | S. Headers identical. State `All Students` / prior setting `All Students`: 55 / 38 / 7 / 45 |
| `student_teacher_ratio` | `OVW_Student_to_Teacher_Ratio` | S for school rows only. State and district aggregate rows are gone. Do not smoke-test `999000` |
| `kentucky_summative_assessment` | `ACCT_Kentucky_Summative_Assessment` and `ASMT_Kentucky_Summative_Assessment` | M only because the pull request has to show level rates and one grade. Accountability headers identical. By-grade file is co-op spelling only |
| `safe_schools_discipline` | `SAFE_Discipline_Resolutions` and `SAFE_Legal_Sanctions` | S/M as journal 104 says. Legal-sanction headers match. Resolutions need the four KYRC25/26 aliases. `(SSP2)` already matches. Keep `(Ssp2)` if KYRC25 is loaded in the same change |
| `safe_schools_events` | the four `SAFE_Behavior_Events_by_*` files | M. Grade headers confirmed. `include_kyrc26` does not apply. The transform list is still the four KYRC24 names |
| `safe_schools_climate` | `ACCT_Index_Scores` and `SAFE_Precautionary_Measures` | M. `transform()` uses `get_files_to_process`, which does not name the KYRC26 files and does not read `include_kyrc26`. Index headers already map. Classroom-door column is gone. Dedup still omits level |
| `spending_per_student` | `FT_Spending_per_Student` | 404 on both containers. Also 404: the other `FT_` names below |
| `financial_summary` | `FT_Financial_Summary` | 404 on both containers |
| `census_acs`, `census_saipe` | none | No KYRC file |
| `school_tract_geocoding` | none | Rerun after the school list has 2026 rows if new schools need coordinates |

`act_scores` has no KYRC26 ACT file. Do not point it at `KYRC26_ASMT_CAE.csv`.

## Coverage, file to module

Registered files are the ones in `config/kde_sources.yaml`. "Unconsumed" means no ETL reads them.

### Registered and consumed

The module table above. `KYRC26_OVW_Students_Taught_by_Inexperienced_Teachers.csv` is registered twice, under `novice_teachers` and under `students_taught_by_inexperienced_teachers`. Only `novice_teachers` is in `config/mappings.yaml`. The second directory is a duplicate download.

### Registered, no ETL

| Directory | File | Disposition |
| --- | --- | --- |
| `benchmark_assessment` | `ASMT_Benchmark` | Download only. Subject columns changed to `Reading and Writing`. No benchmark ETL |
| `student_retention` | `OVW_Student_Retention_Grades_4_12` | Download only. Headers identical. No module |
| `dropout_rate` | `OVW_Dropout_Rate` | Download only. Headers identical. No module |
| `homeless_students` | `OVW_Homeless` | Download only. Headers identical. No module |
| `migrant_students` | `OVW_Migrant` | Download only. Headers identical. No module |
| `students_with_disabilities` | `OVW_Students_with_Disabilities_IEP` | Download only. Headers identical. No module |
| `educator_qualifications` | `OVW_Educator_Qualifications` | Download only. Headers identical. No module |
| `students_taught_by_inexperienced_teachers` | same file as novice teachers | Duplicate registration. The novice-teacher ETL is the consumer |

### On the blob, not registered

| File | Disposition |
| --- | --- |
| `ASMT_CAE` | New name. SAT. Out of scope for the ACT series. New pipeline only if SAT is wanted |
| `OVW_Chronic_Absenteeism_CRDC` | Renamed CRDC extract. No ETL. The overview chronic-absenteeism file remains the KPI source. Collected year is one year behind, not four |
| `ACCT_Profile`, `ACCT_Summary`, `ACCT_School_Improvement`, `ACCT_Assessment_Participation` | No ETL. Profile and summary are co-op spelling only. Not required to refresh current KPIs |
| Eight `ACCT_Crosstab_*` files | No ETL. Co-op spelling only, except the four subject files also carry school code `901` |
| `ACCT_Survey_Results` | No ETL. 297,011,485 bytes. Headers identical. Load only for question-level climate |
| `ADLF_Current_Year_Graduates`, `ADLF_Graduate_Outcomes` | No ETL. Headers identical |
| `ASMT_Alternate_Assessment_Participation`, `ASMT_English_Language_Proficiency`, `ASMT_Kindergarten_Screen_Developmental_Domains`, three `ASMT_Measurements_of_Interim_Progress_*`, `ASMT_National_Assessment_of_Educational_Progress` | No ETL. Headers identical. Kindergarten domains are not the composite KPI. NAEP is 128 rows in both years |
| `CTE_Perkins_Report`, `CTE_Postsecondary_Opportunities`, `CTE_Student_Objectives` | No ETL. Headers identical |
| `EDOP_Career_Studies`, `EDOP_Health_Education_and_Physical_Education`, `EDOP_Visual_and_Performing_Arts`, `EDOP_World_Languages` | No ETL. Columns rewritten. Do not add a filename until a module is in scope |
| `OVW_Access_to_Technology` | No ETL. Question text rewritten |
| `OVW_Student_Membership` | No ETL. `Preschool` column removed |
| `OVW_Attendance_Rate`, `OVW_Adjusted_Average_Daily_Attendance_AADA`, `OVW_Economically_Disadvantaged`, `OVW_English_Learners`, `OVW_Extra_Year_in_Primary`, `OVW_Board_Members_SBDM`, `OVW_Parental_Involvement`, `OVW_Inexperienced_Staff`, `OVW_Teachers_Full_Time_Equivalent_FTE`, `OVW_Teachers_by_Gender`, `OVW_Teachers_by_Race_Ethnicity` | No ETL. Headers identical. Not the current KPI for the related module (attendance is not chronic absenteeism, English-learner counts are not the progress rate, inexperienced staff is not inexperienced teachers) |
| `OVW_Preschool_Enrollment`, `OVW_Preschool_Suspensions_and_Expulsions`, `OVW_Student_Expulsions`, `OVW_Arrests_and_Referrals_to_Law_Enforcement`, `OVW_Harassment_and_Bullying`, `OVW_Violent_Offenses` | No ETL. Headers identical. Collected year moved from `20202021` to `20212022` while `School Year` is `20252026` |
| `SAFE_Approaches_that_are_actively_implemented_school_wide` | No ETL. Headers identical |
| Seven `*_Datasets.zip` | Packaging only. Do not register beside the CSVs |

### Lost twins

| KYRC25 file the pipeline can already see | KYRC26 |
| --- | --- |
| `FT_Spending_per_Student` (consumed by `spending_per_student`) | 404 |
| `FT_Financial_Summary` (consumed by `financial_summary`) | 404 |
| `FT_Facilities_Debt_Services`, `FT_Grants`, `FT_Learning_Environment`, `FT_Operations`, `FT_State_Funding_SEEK`, `FT_Statewide_Funding_Sources`, `FT_Taxes` | 404. Not consumed |
| `FT_Personnel_Spending_per_Student_Federal_Funds`, `FT_Personnel_Spending_per_Student_State_Local_Funds`, `FT_Total_Spending_per_Student_All_Fund_Sources`, `FT_Total_Spending_per_Student_Federal_Funds`, `FT_Total_Spending_per_Student_State_Local_Funds` | 404. On the 2024-25 download page. Not in journal 104's finance pair |
| `ASMT_The_ACT` (repo still loads the KYRC24 file in `act_scores`) | 404 under that name. Replacement `ASMT_CAE` is SAT |
| `CRDC_Chronic_Absenteeism` | 404 under that name. Replacement is `OVW_Chronic_Absenteeism_CRDC` |

No consumed accountability, enrollment, safety, CTE, or teacher file lost its KYRC26 twin. Safe-schools behavior, discipline, and suspensions are still KYRC24 in the modules even though the KYRC25 and KYRC26 twins exist. Journal 104 already says that.

## Checks against the PR #36 claims

Confirmed:

- The S scripts other than `student_teacher_ratio` have headers the current parser accepts. Opt-in plus a fixture is the code change. Kindergarten state components are 55 / 38 / 7 / 45. KSA accountability headers match. The by-grade file changes co-op spelling only, and `Proficient / Distinguished` was already mapped.
- Suspensions: `Collected School Year` is `20212022` on every KYRC26 row and `School Year` is `20252026`. The module already keeps those fields apart.
- Safe-schools events grade tokens are `PS`, `K`, `1st` through `12th`, `14th`.
- `include_kyrc26` on `BaseETL.accepts_data_file` skips `KYRC26_` until the module sets the flag. `process` and the streaming path both use that check. `safe_schools_events.transform` and `safe_schools_climate.transform` do not.
- Last-Modified re-download is sound for the container it watches. A file with no `.kde-source.yaml` is downloaded again. A changed Last-Modified or a changed ETag, when both sides have an ETag, replaces the local file. HEAD 403 or a missing Last-Modified falls back to a one-byte GET. Azure HEAD returned 200 with Last-Modified in this audit, so the fallback was not required for these blobs.
- `datasetsdev` is not the live host. Both configured base URLs point at `datasets`.

Corrections:

1. `KYRC26_ASMT_The_ACT.csv` and `KYRC26_CRDC_Chronic_Absenteeism.csv` are not unpublished series. They were renamed. Map `ASMT_CAE` as SAT and leave it out of `act_scores`. Map `OVW_Chronic_Absenteeism_CRDC` as unconsumed.
2. The four EDOP dual-credit and advanced-course files are genuine 2025-26 extracts. Keep them on the S list.
3. `student_teacher_ratio` cannot use a `999000` smoke test. School rows are the file.
4. `OVW_Advanced_Coursework` is a much smaller school set than KYRC25, with collected year `20212022`. Say that in the advanced-coursework pull request.
5. The download page's container is `datasetsuat6051276`. Bytes match `datasets` today. Re-HEAD both after 26 October 2026.
6. The ACCT group is 19 CSVs, not 22 files.
7. Finance is larger than the two names in journal 104. Five more KYRC25 spending files exist and also have no KYRC26 twin. None of them change the "wait until the name returns 200" instruction for the two modules that exist.
8. `Couse Completers ` in `etl/dual_credit.py` has a trailing space the KYRC25 file does not. KYRC26 does not need that alias.

## Order and sizes

Keep the journal 104 order, with two notes.

1. `chronic_absenteeism` first. Unchanged.
2. The other opt-in modules, in the journal 104 order. When `student_teacher_ratio` comes up, fixture a school row. Do not require `999000`. `advanced_coursework` stays S. Its overview fixture should expect the smaller school set and collected year `20212022`, not a copy of the KYRC25 total.
3. `kentucky_summative_assessment` after one opt-in has landed. Still M.
4. `safe_schools_discipline`, then `safe_schools_events`, then `safe_schools_climate`. Sizes unchanged.
5. Spending and financial summary when the KYRC26 names return 200. ACT stays out. SAT (`ASMT_CAE`) is a separate decision, not the next S script.
6. Do not add a script for the CRDC rename unless a CRDC KPI is in scope.

No S module needs to become M because of a new header. `student_teacher_ratio` stays S only if the pull request does not try to rebuild district and state totals.

## Develop test baseline

Unchanged `origin/develop` at `b14ec3c`, this environment, pandas 3.0.6, 17 October 2026:

`42 failed, 375 passed, 104 skipped` in 12.26s.

The dtype failures assert `object` and now see pandas `StringDtype`. Census failures call the live API. `test_get_files_to_process` expects 4 climate files and finds 6. These failed before any KYRC26 parser work. A later script pull request should treat a failure on this list as pre-existing unless the assertion changed.

- `tests/test_census_acs.py::TestCensusACSIntegration::test_real_api_fetch_tracts`
- `tests/test_census_acs.py::TestCensusACSIntegration::test_real_api_transform`
- `tests/test_census_saipe.py::TestCensusSAIPEIntegration::test_real_api_fetch`
- `tests/test_census_saipe.py::TestCensusSAIPEIntegration::test_real_api_transform`
- `tests/test_district_school_list.py::TestDistrictSchoolListETL::test_convert_to_kpi_format`
- `tests/test_dtype_enforcement.py::TestDtypeEnforcement::test_value_column_cast_to_float`
- `tests/test_financial_summary.py::TestFinancialSummaryETL::test_clean_numeric_values`
- `tests/test_financial_summary.py::TestFinancialSummaryETL::test_extract_metrics`
- `tests/test_financial_summary.py::TestFinancialSummaryETL::test_get_suppressed_metric_defaults`
- `tests/test_financial_summary.py::TestFinancialSummaryETL::test_transform_kyrc24_format`
- `tests/test_financial_summary.py::TestFinancialSummaryETL::test_transform_2020_format`
- `tests/test_financial_summary.py::TestFinancialSummaryETL::test_mixed_year_formats`
- `tests/test_gifted_talented.py::TestGiftedTalentedHelpers::test_standardize_missing_values`
- `tests/test_graduation_rates.py::TestGraduationRatesETL::test_standardize_missing_values`
- `tests/test_graduation_rates.py::TestGraduationRatesETL::test_dtype_conversion`
- `tests/test_kentucky_summative_assessment.py::TestKentuckySummativeAssessmentETL::test_extract_metrics`
- `tests/test_kentucky_summative_assessment.py::TestKentuckySummativeAssessmentETL::test_convert_to_kpi_format`
- `tests/test_out_of_school_suspension.py::TestOutOfSchoolSuspensionETL::test_convert_to_kpi_format_safe_schools`
- `tests/test_out_of_school_suspension.py::TestOutOfSchoolSuspensionETL::test_negative_values_suppressed`
- `tests/test_out_of_school_suspension_end_to_end.py::TestOutOfSchoolSuspensionE2E::test_kyrc24_and_safe_schools`
- `tests/test_postsecondary_readiness.py::TestPostsecondaryReadinessETL::test_standardize_missing_values`
- `tests/test_postsecondary_readiness.py::TestPostsecondaryReadinessETL::test_dtype_conversion`
- `tests/test_safe_schools_climate.py::TestSafeSchoolsClimateETL::test_get_files_to_process`
- `tests/test_safe_schools_discipline.py::TestSafeSchoolsDisciplineETL::test_extract_metrics_kyrc24_discipline`
- `tests/test_safe_schools_discipline.py::TestSafeSchoolsDisciplineETL::test_extract_metrics_kyrc24_legal`
- `tests/test_safe_schools_discipline.py::TestSafeSchoolsDisciplineETL::test_extract_metrics_historical`
- `tests/test_safe_schools_discipline.py::TestSafeSchoolsDisciplineETL::test_extract_metrics_zero_total`
- `tests/test_safe_schools_discipline.py::TestSafeSchoolsDisciplineETL::test_get_suppressed_metric_defaults`
- `tests/test_safe_schools_discipline.py::TestSafeSchoolsDisciplineETL::test_transform_integration`
- `tests/test_safe_schools_discipline_end_to_end.py::TestSafeSchoolsDisciplineEndToEnd::test_end_to_end_kyrc24_discipline_only`
- `tests/test_safe_schools_discipline_end_to_end.py::TestSafeSchoolsDisciplineEndToEnd::test_end_to_end_kyrc24_legal_only`
- `tests/test_safe_schools_discipline_end_to_end.py::TestSafeSchoolsDisciplineEndToEnd::test_end_to_end_all_files`
- `tests/test_safe_schools_discipline_end_to_end.py::TestSafeSchoolsDisciplineEndToEnd::test_end_to_end_empty_file`
- `tests/test_secondary_enrollment.py::TestSecondaryEnrollmentETL::test_clean_enrollment_data`
- `tests/test_secondary_enrollment.py::TestCleanEnrollmentData::test_comma_removal`
- `tests/test_spending_per_student.py::TestSpendingPerStudentETL::test_clean_spending_values`
- `tests/test_spending_per_student.py::TestSpendingPerStudentETL::test_transform_2024_format`
- `tests/test_spending_per_student.py::TestSpendingPerStudentETL::test_mixed_year_formats`
- `tests/test_student_enrollment.py::TestStudentEnrollmentETL::test_extract_metrics_2024_format`
- `tests/test_student_enrollment.py::TestStudentEnrollmentETL::test_clean_enrollment_data`
- `tests/test_student_enrollment.py::TestStudentEnrollmentETL::test_suppressed_metric_defaults`
- `tests/test_student_enrollment.py::TestCleanEnrollmentData::test_comma_removal`

## Files with no disposition

None. Every confirmed KYRC26 CSV and zip is in the module table or the file table above.
