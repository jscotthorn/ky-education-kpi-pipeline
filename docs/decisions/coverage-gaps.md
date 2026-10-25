# Coverage gaps

These are series a consumer of this pipeline can be missing. The FCPS equity dashboard repository (`jscotthorn/fcps-equity-dashboard`) returned HTTP 404 to this pass, so the dashboard's own wording was not read. Classifications use this repository, its issues, and the KDE sources in [kde-guidance-sources.md](kde-guidance-sources.md).

Classes: `deliberate`, `KDE publication gap`, `bug`, `unconfirmed`.

## AP participation for 2023 onward

Classification: **KDE publication gap** for CRDC collections after 2021-22. The claim that only 2021-22 exists is not what this repo stores.

`etl/advanced_coursework.py` and `README.md` store the CRDC overview on the collection year: 2021 is the 2020-21 collection, from `KYRC23_OVW_Advanced_Coursework.csv`; 2022 is the 2021-22 collection, from `KYRC26_OVW_Advanced_Coursework.csv`. No later CRDC collection is registered. Participation-and-performance files are a different measure and are registered for later report-card years (`config/kde_sources.yaml`, `advanced_coursework`). QA and the glossary describe CRDC as a biennial federal collection. No KDE sentence was found that says which year field to use. See [0048](0048-crdc-kpi-year-split.md).

## CTE participation and completion rates for 2025-26

Classification: **deliberate**. The same choice also leaves 2024-25 rates unstored.

`notes/104--kyrc26-file-to-etl-mapping.md` (20 October 2026) records Scott's decision not to store participants divided by enrollment, and not to store the calculated grade-12 completion rate, after that rate could not be checked against KDE's published 2024 percent. `notes/106--cte-participation.md` shows zero `cte_participation_rate` and zero `cte_completion_rate_grade_12` rows for 2025 and 2026. KYRC25 and KYRC26 files are count columns only. See [0007](0007-drop-calculated-cte-rates.md). KDE guidance flag: possible conflict, because this pass did not confirm whether the report-card site still displays a rate.

A 25 October 2026 relay said the rates were omitted because they could not be regenerated with fidelity to older years. That sentence is not in the repo. The notes give the reasons quoted in [0007](0007-drop-calculated-cte-rates.md).

## High school CTE enrollment for 2024

Classification: **deliberate**.

`KPIS.md` and `notes/106--cte-participation.md`: KYRC24 does not emit `cte_high_school_enrollment_count` because `Total Number of Student` is the participant count (state All Students 143,415), not enrollment. KYRC25 and KYRC26 do publish `All High School Enrollments`.

## Graduation counts after 2021

Classification: **KDE publication gap** for the KYRC cohort files.

`notes/104--kyrc26-file-to-etl-mapping.md` says the KYRC cohort files emit only `graduation_rate_4_year` and `graduation_rate_5_year`. They have no count or total columns. `etl/graduation_rates.py` writes `graduation_count_*` when a grads column is present (`data_source` `2021_detailed`). CHG-2022, re-fetched here, says accountability "Counts are no longer available" and that "Current Year Graduates" is a different count that "does not align to cohort graduate rate" (https://www.education.ky.gov/districts/tech/sis/Documents/SRC_ReportingChanges_2022.pdf).

## Teacher working conditions only in alternate years

Classification: **deliberate**.

Impact Kentucky is stored once per administration, under 2020, 2022, 2024, and 2026. Odd-year files repeat the prior survey and are skipped. `notes/105.md` cites KDE advisory 20-151, impactky.org, Kentucky Teacher, and a KSBA briefing that lists administrations in 2019-20, 2021-22, and 2023-24 with no survey in the years between. The research cites the same cycle from Kentucky Teacher (IKY-2022, IKY-2024, IKY-2026: "administered every two years"). Those three articles were not re-fetched here. See [0014](0014-impact-kentucky-one-wave.md).

Separate gap, **unconfirmed**: `KYRC26_OVW_Teacher_Working_Conditions.csv` has no districts 001-095. Issue [#111](https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/111) asks whether the source is truncated. The live datasets copy and the UAT copy match each other and both omit those districts. The issue is labeled `kde-recheck`. The file is stored as published.

## District staffing and finance figures for 2026

Classification: **KDE publication gap**.

`notes/105--kyrc26-completeness-audit.md`: `KYRC26_FT_Financial_Summary.csv` and `KYRC26_FT_Spending_per_Student.csv` are 404 on both containers. `notes/103--2025-26-kyrc26-data-update-plan.md` says the SRC finance calendar puts the 2024-26 financial-transparency release in May 2027, citing https://www.education.ky.gov/districts/tech/sis/Pages/School_Report_Card_Resources.aspx. CHG-2022, re-fetched here, states the standing rule: "Financial data are reported once audited financials are available in the spring following the initial School Report Card release."

## District per-pupil spending for 2024-25

Classification: **unconfirmed**.

Issue [#118](https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/118) does not call this a bug. `KYRC24_FT_Spending_per_Student.csv` and `KYRC25_FT_Spending_per_Student.csv` contain school rows only, so 2024 and 2025 have no district `165000` or state `999000` rows. District and state totals are in five other files per year that are not registered. That is not a KDE publication gap: the issue says KDE publishes the totals. It is not documented as a deliberate exclusion. [#117](https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/117) fixed comma parsing for the school file. It did not add the total files.

## No state comparison rows

Classification: **unconfirmed** as a pipeline-wide gap.

`notes/105--kyrc26-completeness-audit.md` says many KYRC26 files still have a `999000` state row, including both graduation files, chronic absenteeism, readiness, kindergarten, and CTE participation. Documented exceptions with no state row:

- CRDC out-of-school suspension (`notes/104--kyrc26-file-to-etl-mapping.md`)
- KYRC26 student-teacher ratio (`etl/student_teacher_ratio.py`: school rows only)
- spending per student for 2024 and 2025 (issue #118)

Report drafts were a separate gap. Issue #42 said they stay out of scope. Pull request #115 later changed `scripts/generate_report_drafts.py` so a state row can be `State` or `All Districts`. See [0002](0002-report-scripts-out-of-scope.md). This pass did not run the script.

## KYRC26 files from 15 October 2026

Classification: **unconfirmed** as a decision to treat those bytes as final.

`notes/103--2025-26-kyrc26-data-update-plan.md` says the datasets objects were last modified 15 October 2026 at 10:00 UTC, and that both containers should be re-HEAD'd after the review window that runs through 26 October 2026. See [0049](0049-kyrc26-files-loaded-15-oct-2026.md).
