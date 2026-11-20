# Postsecondary Readiness Figure Reconciliation

**Date**: 2026-08-03
**Purpose**: Investigate district-leader feedback that the Equity Scorecard's Postsecondary
Readiness (PSR) figure for FCPS is inaccurate, determine whether our pipeline or KDE's source
data is the cause, and recommend what the published report should say.

## Background

Dr. Soraya Matthews forwarded feedback (2026-07-29) from Amanda Wickersham (Director of CTE) and
Brooke Stinson (Director of Assessment) questioning the Postsecondary Readiness figure in the
2024-25 Equity Scorecard draft:

> "The school report card shows 84.2, and my data from you says 91.3% of 24-25 students were PSR."
> — Amanda Wickersham, 2026-07-28
>
> "Yes 91.3 PSR with High Demand" — Brooke Stinson, 2026-07-28

Our published report shows **82.2%** for FCPS district-wide Postsecondary Readiness, 2024-25.

## The three figures

| Figure | Value | Source |
|---|---|---|
| Our report | 82.2% | `data/raw/postsecondary_readiness/Postsecondary_Readiness_2025.CSV`, downloaded 2025-11-19 |
| Amanda (report card) | 84.2% | FCPS report-card screenshot (not preserved in the email forward) |
| Brooke (FCPS internal, "with High Demand") | 91.3% | FCPS Assessment/CTE internal tracking |

## KDE's two PSR metrics

KDE's postsecondary readiness file publishes two columns per district/school/demographic row:

- **`Postsecondary Rate`** — the base rate: percentage of students who earned a diploma and met
  at least one readiness measure (qualifying exam score, dual credit, AP/IB exam, industry
  certification, or approved work experience).
- **`Postsecondary Rate With Bonus`** — the base rate plus a bonus for readiness measures in
  high-demand career sectors (e.g., certain CTE industry certifications). This is the number
  Brooke's "PSR with High Demand" almost certainly refers to.

Our ETL (`etl/postsecondary_readiness.py`) extracts both metrics into
`postsecondary_readiness_rate` and `postsecondary_readiness_rate_with_bonus`, but the report
generator (`scripts/generate_report_drafts.py`) and the published HTML currently surface only the
base rate. That is the single biggest source of the discrepancy readers see.

For Fayette County, 2024-25 (School Code 165), from the raw file:

| | Base rate | With-bonus rate |
|---|---|---|
| FCPS district | 82.2 | 89.2 |
| Kentucky state | 83.0 | 88.8 |

## Hypotheses tested

**1. Wrong metric selected (base vs. with-bonus).**
Explains most of the gap to Brooke's 91.3, but not all of it: 91.3 − 89.2 = 2.1 pp still
unaccounted for even after switching to the with-bonus metric. **Partially confirmed** — real
effect, but not sufficient on its own.

**2. School composition / alternative-school drag on the district average.**
Tested by reconstructing the district figure from the six FCPS A1 (accountability) high schools:

| School | Cohort | Base rate | With-bonus rate |
|---|---|---|---|
| Henry Clay | 541 | 67.3 | 68.7 |
| Lafayette | 629 | 84.9 | 90.1 |
| Tates Creek | 531 | 87.0 | 97.4 |
| Frederick Douglass | 444 | 88.7 | 97.4 |
| Bryan Station | 532 | 87.9 | 98.5 |
| Paul Laurence Dunbar | 540 | 79.8 | 86.9 |
| **Sum / weighted avg** | **3,217** | **82.45** | **89.57** |

The six-school cohort sum (3,217) matches the district cohort exactly
(`postsecondary_enrollment_total_in_cohort` = 3,217 for FCPS 2025), and the weighted average
(82.45 / 89.57) matches KDE's published district row (82.2 / 89.2) within rounding. **Ruled out**
— the district figure is a straightforward enrollment-weighted average of the six high schools,
not an artifact of alternative-school inclusion or exclusion.

**3. Wrong metric pulled from our own pipeline output.**
Checked every FCPS 2025 district-level metric in `data/kpi/fayette_county_kpi.csv` for a value of
84.2 or 91.3. None matches — 4-year graduation rate is 92.4%, postsecondary enrollment is 47.9%.
**Ruled out.**

**4. Wrong year.**
FCPS PSR by year in our processed data: 2022 = 69.4/72.1, 2023 = 72.5/75.8, 2023-24 (KYRC24) =
77.5/82.6, 2024-25 = 82.2/89.2. No year matches 84.2 or 91.3. **Ruled out.**

## Residual gap

After accounting for the base/with-bonus definition mismatch, both of Amanda's and Brooke's
figures sit **~2.0–2.1 percentage points above** our with-bonus figure (84.2 − 82.2 = 2.0 on base;
91.3 − 89.2 = 2.1 on with-bonus). The same-sized gap on both metrics suggests one underlying cause
— roughly 65 additional students (2 pp of a 3,217-student cohort) credited as ready in FCPS's
internal/report-card figures but not in our downloaded file.

**Leading hypothesis:** KDE revised the file after our 2025-11-19 download. Kentucky's PSR
calculation allows readiness to be earned through the summer following graduation (e.g., a
qualifying exam retake or late-posted industry certification), and KDE reposts accountability
data after an appeals/correction window. A late batch of ~65 students being credited district-wide
would produce exactly this pattern.

**Secondary possibility:** Amanda's screenshot is a different report-card view than the one we
pulled — e.g., a school-level figure being read as district-level, a different demographic filter,
or an accountability *indicator score* (which KDE sometimes displays alongside the raw rate and
which is not the same number).

## Confirmation steps

1. **Re-download completed 2026-08-03**, using the KPI pipeline's own download tool
   (`data/prepare_kde_data.py postsecondary_readiness`) against the same source URL as the
   original file (KDE's OAA Temporary Datasets endpoint —
   `education.ky.gov/Open-House/data/OAA%20Temporary%20Datasets/Postsecondary_Readiness_2025.CSV`).
   An earlier attempt via `reportcard.kyschools.us`'s interactive UI hit the site's
   anti-automation protections and was abandoned in favor of this tool, which is the project's
   documented data-refresh path and hits KDE's file server directly rather than the report-card
   SPA.
2. **Ask Amanda Wickersham** for the exact report-card screen behind 84.2 — superseded; KDE's
   revised file now matches 84.2 exactly (see Resolution below), so this is no longer needed.
3. **Ask Brooke Stinson** to confirm 91.3 is the with-bonus/high-demand rate — superseded for the
   same reason; KDE's revised with-bonus figure is 91.3 exactly.

## Resolution (2026-08-03)

The re-downloaded file is **841,581 bytes**, versus **841,419 bytes** for the 2025-11-19 copy —
confirming KDE revised the dataset in the interim. Diffing the two files on the Fayette County
district row:

| | Base rate | With-bonus rate |
|---|---|---|
| 2025-11-19 download (old) | 82.2 | 89.2 |
| 2026-08-03 download (new) | **84.2** | **91.3** |
| Kentucky state (new) | 83.3 | 89.1 |

The revised figures match Amanda's and Brooke's numbers **exactly** on both the base and
with-bonus rates. The leading hypothesis in this note — that KDE reposts postsecondary readiness
data after the accountability appeals window closes — is confirmed. Every demographic group and
every FCPS high school shifted upward by 2–7 points in the revision (e.g., Henry Clay's base rate
moved from 67.3% to 72.1%), consistent with a district-wide data correction rather than an
isolated fix.

The ETL was re-run (`etl/postsecondary_readiness.py` standalone, then `etl_runner.py --skip-etl`
to recombine `kpi_master.csv`), and the report, draft, and this note have been updated to the
revised figures.

## Recommendation (implemented)

Published figures now reflect the 2026-08-03 KDE download:

- **84.2%** (base rate) as the report's headline number, with **91.3%** (with-bonus) shown
  alongside it.
- The data-vintage note in the report cites both download dates and states plainly that KDE
  revised the dataset between snapshots, so the difference is not a pipeline error.
- The original raw file from 2025-11-19 is preserved as
  `data/raw/postsecondary_readiness/Postsecondary_Readiness_2025_2025-11-19.CSV.bak` for audit
  purposes; the active file (`Postsecondary_Readiness_2025.CSV`) is now the 2026-08-03 version
  used by the ETL.
