# 0028 Keep 2025-26 turnover rows on the existing metric names

Status: accepted

## Question

KYRC26 turnover labels do not line up with the earlier metric names. Rename the 2026 rows, or keep the old names and record a series break?

## What KDE changed

The 2025-26 turnover file uses labels that issue #67 treats as a break in the series. This pass did not re-read the raw headers.

## Options considered

New metric names for 2026, or the existing names with a caveat.

## Decision

18 October 2026. Keep all 2025-26 turnover rows on the existing metric names. Put the series-break caveat in issue #67. Label the series `kde-recheck`.

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/68#issuecomment-6251780197 posted on the `jscotthorn` account through the Cursor GitHub App.

## Why

The comment keeps the names so the rows stay on the series, and points the break at #67. Reason the labels still mean the same measure: not documented. The caveat is the record that they may not.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/68 (still open)
- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/67

## Code check

Not fully re-checked in this pass. The decision is to keep the published rows on the current names. Issue #68 is still open. This record does not claim the output was regenerated.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE note that the 2025-26 turnover labels are or are not comparable to the earlier names.
