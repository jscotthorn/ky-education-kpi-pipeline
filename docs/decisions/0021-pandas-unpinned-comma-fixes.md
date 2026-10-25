# 0021 Leave pandas unpinned and fix comma parsing per module

Status: accepted

## Question

Pandas 3 reads `dtype=str` as `StringDtype`, so a comma inside a number survives and the cast fails. Pin pandas, or fix the readers?

## What KDE changed

KDE publishes some counts with thousands separators. The change is in how pandas 3 types those strings, not in a KDE layout.

## Options considered

Pin pandas below 3, or keep pandas unpinned and strip commas in each reader that needs it, including a dtype-agnostic helper on `BaseETL`.

## Decision

18 October 2026. Do not pin pandas. Each module's KYRC26 pull request keeps fixing its own object-only checks. One follow-up pull request into `feature/2025-26-kyrc26` will fix `cte_participation.py` (merged in #45 before this was caught), make `BaseETL.standardize_missing_values` dtype-agnostic, and add a test that runs the comma and missing-value handling on both pandas 2.3.3 and pandas 3. Financial summary and spending per student get fixed when the finance work is unblocked.

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/49#issuecomment-6251977891 posted on the `jscotthorn` account through the Cursor GitHub App.

## Why

The comment chooses per-module fixes over a pin so the pipeline can run on pandas 3. Reason not documented beyond that.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/49
- `etl/base_etl.py` `standardize_missing_values`
- Pull requests https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/106 (CTE), https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/110 (financial summary), https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/117 (spending per student)

## Code check

Matches the parts that have merged. `etl/base_etl.py` rewrites `StringDtype` and replaces `*` with NA. CTE comma handling landed in #106. Financial-summary commas landed in #110. Spending-per-student commas landed in #117. This pass did not re-run those modules.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

This is a parser choice. KDE does not publish a pandas requirement.
