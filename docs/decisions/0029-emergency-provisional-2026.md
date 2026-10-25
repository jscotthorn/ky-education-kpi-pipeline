# 0029 Keep 2026 emergency and provisional certification rows

Status: accepted

## Question

The 2025-26 emergency and provisional certification rows do not match the earlier files cleanly. Drop 2026, or keep the rows with a caveat?

## What KDE changed

KYRC26 publishes emergency and provisional certification rows. The covariate catalog had pointed `emergency_provisional_rate` at `kde_staff_reports` rather than the KYRC teacher-certification files.

## Options considered

Drop the 2026 rows, or keep them (option 1) and fix the catalog source.

## Decision

18 October 2026. Option 1: keep the 2026 emergency and provisional rows. The caveat is the one merged in pull request #71. Label them `kde-recheck`. Point the covariate catalog so `emergency_provisional_rate` names the KYRC teacher-certification files, not `kde_staff_reports`.

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/70#issuecomment-6251790427 posted on the `jscotthorn` account through the Cursor GitHub App.

## Why

The comment chooses option 1 and the catalog correction. Reason the 2026 definition still belongs on the old metric: not documented beyond the caveat in #71.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/70 (still open)
- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/71

## Code check

Not fully re-checked. This pass did not confirm whether the covariate catalog now names the KYRC certification files. Issue #70 is still open. Not resolved here.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE statement, re-fetched here, that defines the 2025-26 emergency or provisional rate against the earlier years.
