# 0037 Store published kindergarten percents for 2024-26

Status: accepted

## Question

KYRC24, KYRC25, and KYRC26 kindergarten columns are named like counts and contain percents. Store the published percent, or rebuild a count?

## What KDE changed

Those three files name the measure columns `Ready With Interventions`, `Ready`, `Ready With Enrichments`, and `Total Ready`. The cells are percents. They have no tested-count column. Older files publish a percent and, in some years, `NUMBER TESTED`.

## Options considered

Renormalize the components so they sum to 100, keep deriving a count for every year, or store the published percent for 2024-26 and keep the older count formula as a documented estimate.

## Decision

19 October 2026.

- For 2024-26, store the published percents: Total Ready, the components, and the prior settings. Do not renormalize.
- Stop emitting `_count` and `_total` for those composite years.
- Existing KYRC24 rates change by up to 0.86 points.
- For 2018-21, keep the count estimate and document it as an estimate.

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/88#issuecomment-6279053253 posted on the `jscotthorn` account through the Cursor GitHub App.

## Why

The comment stores what KDE published in the years the cell is already a percent. The older count is `round(percent / 100 * NUMBER TESTED)` because KDE did not publish that headcount. The module docstring says the estimate is exact when the tested count is below about 1,000, and about plus or minus 24 students at state level, because the percent is rounded to one decimal.

## Links

- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/88
- `etl/kindergarten_readiness.py` lines 8-22 and the count formula near line 410
- `notes/104--kyrc26-file-to-etl-mapping.md`

## Code check

Matches. `etl/kindergarten_readiness.py` stores the published percent for KYRC24-26 and does not emit `_count` or `_total` for those files. The older path still sets count to `round(percent / 100 * tested)` and the docstring calls that an estimate.

## KDE guidance check

Verdict: possible conflict, on the 2018-21 derived count only. Flag only. The decision is unchanged.

Storing the 2024-26 published percents matches the cells KDE published. The derived count can rebuild a small cell from a rounded percent and a tested count. CHG-2022 suppresses low counts and percentages that could identify a student (quote in [0001](0001-suppression-rule.md)). HB 6 website requirements were not re-fetched. Not found: a KDE statement that forbids or allows this estimate. https://www.education.ky.gov/districts/tech/sis/Documents/SRC_ReportingChanges_2022.pdf
