# 0006 CTE definitions and computed rates, 18 October 2026

Status: superseded (in part)

Superseded, for the instruction to compute a rate every year, by [0007](0007-drop-calculated-cte-rates.md) (20 October 2026). The column map below was carried into pull request #106. This record stays.

## Question

2022 and 2023 CTE count columns were stored as rates, and values over 100 were dropped. What do KDE's columns mean, and which rates should be stored?

## What KDE changed

The 18 October comment says KDE's completion rate is grade-12 completers divided by grade-12 participants, and that KYRC24 headers reuse older names while the values follow the 2025 layout. KYRC25 and KYRC26 files have count headers only. See [kde-reporting-changes.md](kde-reporting-changes.md).

## Options considered

The comment calls the choice "expanded option 1." The other options are not restated in the comment.

## Decision

18 October 2026. Expanded option 1, in the CTE follow-up:

1. Map KYRC24 columns by position, not header. The "eligible" column is the completer count. State about 143,415 participants and 17,597 completers.
2. Store 2022/23 values as counts. Store "Grade 12 CTE Eligible" as the grade-12 participant count used for 2025-26. Store TOTAL NUMBER OF STUDENT as high school enrollment.
3. Compute participation (participants divided by high school enrollment) and completion (grade-12 completers divided by grade-12 participants) for every year. Keep rates over 100. Check state rows against the published rates in the comment (2025-26 completion 45.4%).
4. Mark the 2022 participant count and every 2023 count column as not comparable with later years. Label them `kde-recheck`.
5. Add the KYRC24 header mismatch to the 27 October recheck.

`N/A` at A2/C2 centers stays a blank denominator.

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/47#issuecomment-6252107444

## Why

The comment cites the report card Participation and Completers page, the KDE glossary, a QA worksheet, the TEDS glossary, and Perkins/ACTE. Those pages were not re-fetched in this pass. The comment's state table is the cited reason for checking computed rates.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/47
- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/106
- [0007](0007-drop-calculated-cte-rates.md)

## Code check

The compute-every-year clause does not match. [0007](0007-drop-calculated-cte-rates.md) removed the calculated 2025 and 2026 rates. Counts are stored as counts. The KYRC24 completer column is `cte_completer_count_grade_12`. Rates over 100 that KDE printed are kept.

## KDE guidance check

Verdict: possible conflict. Flag only. Applies to the compute clause, which [0007](0007-drop-calculated-cte-rates.md) replaced.

CHG-2022 says "CTE Participation/Participation Completers is new" and does not define a stored rate (https://www.education.ky.gov/districts/tech/sis/Documents/SRC_ReportingChanges_2022.pdf). The QA worksheet, not re-fetched, is quoted by the research as a count of students, not a rate. The 18 October comment says KDE publishes state rates, including 45.4% for 2025-26. Whether the dashboard still shows that rate was not confirmed.
