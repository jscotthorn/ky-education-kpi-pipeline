# 0008 Use the May 2026 KYRC24 CTE republish

Status: accepted

## Question

The datasets KYRC24 CTE file (Last-Modified 14 May 2026) differs from the registered historical copy (29 May 2025). Which copy is the source?

## What KDE changed

Issue #113: the newer copy replaces 3,524 `*` cells with 0.0, blanks, `--`, or `N/A`, fills co-op GREC on 349 rows, and changes no published number. No KDE notice of this republish was found.

## Options considered

Keep the May 2025 historical copy (the issue's holding position) or switch to the May 2026 datasets copy.

## Decision

24 October 2026, 3:24 p.m. ET. Use the May 2026 KYRC24 CTE republish from the live datasets container. A follow-up switches the registration, documents rows that leave suppression, new 0.0 values, and omitted cells, and keeps the existing cell rules.

Implemented in pull request #116, merged at `cfeb6aa` (issue #113 close comment).

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/113#issuecomment-6388522130

Close comment: https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/113#issuecomment-6399041876

## Why

Reason not documented. The comment states the choice. It does not say why the newer copy is preferred. The issue body describes the differences. It does not state a reason either.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/113
- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/116
- `notes/116--cte-kyrc24-republish.md`
- `config/kde_sources.yaml` (CTE participation, May 2026 comment)
- md5 `86677fc9b17a32b62cb533f4352ed63e` (datasets) and `0b0d30c79ff6c5f3c039aafd5ceb8081` (historical), from the pull request body

## Code check

Matches. `config/kde_sources.yaml` registers the May 2026 datasets file and describes the historical copy as superseded. Cell rules in `etl/cte_participation.py` are unchanged: `*` or `<10` is Y, blank / `--` / `N/A` is omitted, a published 0 is kept.

## KDE guidance check

Verdict: consistent. Flag only.

The research cites CWM-2025-10: datasets refresh with the dashboard after release (https://content.govdelivery.com/accounts/KYDE/bulletins/3f857fd, not re-fetched). Using the current live file follows that practice. Not found: a KDE notice of why this file was republished in May 2026, or of what a star that became 0.0 or blank means. That question is the 27 October recheck in [0009](0009-cte-zero-rate-beside-blank-count.md).
