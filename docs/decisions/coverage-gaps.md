# Coverage gaps

These are series a consumer of this pipeline can be missing. The FCPS equity dashboard repository (`jscotthorn/fcps-equity-dashboard`) returned HTTP 404 to this pass, so the dashboard's own wording was not read. Classifications use this repository, its issues, and the KDE sources in [kde-guidance-sources.md](kde-guidance-sources.md).

Classes: `deliberate`, `dashboard scope, deliberate filter`, `planned or deferred`, `KDE publication gap`, `bug, fix in progress`, `unconfirmed`.

## AP participation for 2023 onward

Classification: **KDE publication gap** for CRDC collections after 2021-22. The claim that only 2021-22 exists is not what this repo stores.

`etl/advanced_coursework.py` and `README.md` store the CRDC overview on the collection year: 2021 is the 2020-21 collection, from `KYRC23_OVW_Advanced_Coursework.csv`; 2022 is the 2021-22 collection, from `KYRC26_OVW_Advanced_Coursework.csv`. That labeling is the rule Scott set on 25 October 2026: the CRDC collection year is the KPI year everywhere. See [0048](0048-crdc-kpi-year-collection.md). Out-of-school suspension still stores those collections on the report-card year (2024 and 2026) until issue [#122](https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/122). No later CRDC collection is registered. Participation-and-performance files are a different measure and are registered for later report-card years (`config/kde_sources.yaml`, `advanced_coursework`). QA and the glossary describe CRDC as a biennial federal collection. No KDE sentence was found that says which year field to use.

## CTE participation and completion rates for 2025-26

Classification: **deliberate**. The same choice also leaves 2024-25 rates unstored.

`notes/104--kyrc26-file-to-etl-mapping.md` (20 October 2026) records Scott's decision not to store participants divided by enrollment, and not to store the calculated grade-12 completion rate, after that rate could not be checked against KDE's published 2024 percent. `notes/106--cte-participation.md` shows zero `cte_participation_rate` and zero `cte_completion_rate_grade_12` rows for 2025 and 2026. KYRC25 and KYRC26 files are count columns only. See [0007](0007-drop-calculated-cte-rates.md). KDE guidance flag: possible conflict, because this pass did not confirm whether the report-card site still displays a rate.

Second source, same choice. Scott, direct, 2026-10-25 11:24 AM ET, in his chat with the ECC manager bot (Moon); transcript checked by Archivist:

"I believe some of those gaps, like CTE participation/completion rates in 25-26, was due to changes in how KDE reported and our inability to generate them reliably and with fidelity to older years with the data at hand."

## High school CTE enrollment for 2024

Classification: **deliberate**.

`KPIS.md` and `notes/106--cte-participation.md`: KYRC24 does not emit `cte_high_school_enrollment_count` because `Total Number of Student` is the participant count (state All Students 143,415), not enrollment. KYRC25 and KYRC26 do publish `All High School Enrollments`.

## Graduation counts after 2021

Classification: **KDE publication gap** for the KYRC cohort files.

`notes/104--kyrc26-file-to-etl-mapping.md` says the KYRC cohort files emit only `graduation_rate_4_year` and `graduation_rate_5_year`. They have no count or total columns. `etl/graduation_rates.py` writes `graduation_count_*` when a grads column is present (`data_source` `2021_detailed`). CHG-2022, re-fetched here, says accountability "Counts are no longer available" and that "Current Year Graduates" is a different count that "does not align to cohort graduate rate" (https://www.education.ky.gov/districts/tech/sis/Documents/SRC_ReportingChanges_2022.pdf).

## Teacher working conditions only in alternate years

Classification: **deliberate**.

Impact Kentucky is stored once per administration, under 2020, 2022, 2024, and 2026. Odd-year files repeat the prior survey and are skipped. `notes/105.md` cites KDE advisory 20-151, impactky.org, Kentucky Teacher, and a KSBA briefing that lists administrations in 2019-20, 2021-22, and 2023-24 with no survey in the years between. The research cites the same cycle from Kentucky Teacher (IKY-2022, IKY-2024, IKY-2026: "administered every two years"). Those three articles were not re-fetched here. See [0014](0014-impact-kentucky-one-wave.md).

Separate gap, **unconfirmed** until the 27 October 2026 recheck: `KYRC26_OVW_Teacher_Working_Conditions.csv` has no districts 001-095. Issue [#111](https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/111) asks whether the source is truncated. The live datasets copy and the UAT copy match each other and both omit those districts. The issue is labeled `kde-recheck`. The file is stored as published. The recheck schedule is on [0049](0049-kyrc26-files-loaded-15-oct-2026.md).

## 2021-22 climate and safety index

Classification: **planned or deferred** ([#121](https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/121)).

The published file `quality_of_school_climate_and_safety_survey_index_scores_2022.csv` is not registered. Losing 2021-22 was accepted on 24 October 2026. On 25 October 2026 Scott said there was no specific reason, and issue #121 tracks a low-priority load of that file into the same per-level metrics. See [0042](0042-do-not-register-2021-22-climate-index.md). The series from 2022-23 on is loaded. This is not a choice to leave the year out for good.

## District staffing and finance figures for 2026

Classification: **KDE publication gap**.

`notes/105--kyrc26-completeness-audit.md`: `KYRC26_FT_Financial_Summary.csv` and `KYRC26_FT_Spending_per_Student.csv` are 404 on both containers. `notes/103--2025-26-kyrc26-data-update-plan.md` says the SRC finance calendar puts the 2024-26 financial-transparency release in May 2027, citing https://www.education.ky.gov/districts/tech/sis/Pages/School_Report_Card_Resources.aspx. CHG-2022, re-fetched here, states the standing rule: "Financial data are reported once audited financials are available in the spring following the initial School Report Card release."

## District per-pupil spending for 2024-25

Classification: **bug, fix in progress** ([#118](https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/118), [#119](https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/119)).

`KYRC24_FT_Spending_per_Student.csv` and `KYRC25_FT_Spending_per_Student.csv` are school rows only, so 2024 and 2025 have no district or state per-pupil rows. District `165` and state `999` rows exist for 2020-2023. KDE publishes the 2024 and 2025 totals in five long files per year (`FT_Total` for all funds, federal, and state/local; `FT_Personnel` for federal and state/local). Pull request #119, open, reads those ten files and adds the rows. It is not merged. [#117](https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/117) fixed comma parsing for the school file only.

The ID width is [0053](0053-spending-2024-25-three-digit-ids.md). Primary source: https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/119#issuecomment-6399994227 ("Decision (Scott, 2026-10-25 12:12 PM ET): #119 keeps 3-digit district/state IDs (165, 999) on the 2024-25 spending rows under the #114 precedent, until #61 converts them to 6-digit."). Moon's note is context: https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/119#issuecomment-6399959209. Issue #118 had asked for 6-digit IDs.

## KYRC25 assessment files rewritten after the February 2026 final copy

Classification: **unconfirmed**.

`config/kde_sources.yaml` registers `KYRC25_ACCT_Kentucky_Summative_Assessment.csv` on `url: kyrc25`, the live datasets container. The 25 October 2026 research says that live object is Last-Modified 14 May 2026 and differs from the Open House copy dated 10 February 2026 (the post-review "final" copy in that research): 342 rows and 868 cells, including state high-school math All Students 57.7 versus 57.6. This pass did not re-download or diff those files. The same research says other 2024-25 files were rewritten and not compared, including postsecondary readiness (9 October 2026) and 4-year graduation (6 August 2026). Both of those names are also registered on `url: kyrc25`. No issue was found. Follow-up (b). A separate docs pull request for that comparison is in progress. See [kde-republishes.md](kde-republishes.md) R19 and R20, and [0052](0052-republish-supersedes-earlier.md).

## No state comparison rows

Classification: **dashboard scope, deliberate filter**.

Per Moon's audit of `fcps-equity-dashboard` `develop` `ce0c5a9`, the dashboard's preprocess step keeps only `district == 'Fayette County'` rows, so state rows are dropped there. Not verified by Archivist because that repo isn't readable from here. This pass got HTTP 404 for `jscotthorn/fcps-equity-dashboard`.

The pipeline is a separate fact. `notes/105--kyrc26-completeness-audit.md` says many KYRC26 files still have a `999000` state row, including both graduation files, chronic absenteeism, readiness, kindergarten, and CTE participation. Documented exceptions with no state row:

- CRDC out-of-school suspension (`notes/104--kyrc26-file-to-etl-mapping.md`)
- KYRC26 student-teacher ratio (`etl/student_teacher_ratio.py`: school rows only)
- spending per student for 2024 and 2025 (issue #118)

Report drafts stay out of scope under [0002](0002-report-scripts-out-of-scope.md). Scott later approved the pull request #115 state-row lookup (`State` or `All Districts`) as a narrow exception. This pass did not run the script.

## KYRC26 files from 15 October 2026

Classification: **unconfirmed** as a decision to treat those bytes as final.

`notes/103--2025-26-kyrc26-data-update-plan.md` says the datasets objects were last modified 15 October 2026 at 10:00 UTC, and that both containers should be re-HEAD'd after the review window that runs through 26 October 2026. A KDE recheck routine is scheduled for 27 October 2026 at 9 AM ET (per Moon). See [0049](0049-kyrc26-files-loaded-15-oct-2026.md).
