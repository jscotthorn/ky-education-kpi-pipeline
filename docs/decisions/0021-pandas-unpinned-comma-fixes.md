# 0021 Leave pandas unpinned and fix comma parsing per module

Status: accepted

## Question

Pandas 3 reads `dtype=str` as `StringDtype`, so a comma inside a number survives and the cast fails. Pin pandas, or fix the readers?

## What KDE changed

KDE publishes some counts with thousands separators. The change is in how pandas 3 types those strings, not in a KDE layout.

## Options considered

Pin pandas below 3, or keep pandas unpinned and strip commas in each reader that needs it, including a dtype-agnostic helper on `BaseETL`.

## Decision

18 October 2026. Do not pin pandas. Fix each module that fails. Make `BaseETL.standardize_missing_values` dtype-agnostic. Fix CTE in the #46 follow-up. Fix finance when that work is unblocked.

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
