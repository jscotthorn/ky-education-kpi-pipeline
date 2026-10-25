# 0035 Register the 2018-19 safe-schools workbook for events

Status: accepted, code differs (follow-up open)

## Question

Should `SAFE_SCHOOLS_18-19.xlsx` be loaded for safe-schools events?

## What KDE changed

The workbook publishes state totals of 91,033 students and 299,987 events. It has no DEMO or DGROUP sheet. `config/kde_sources.yaml` says the same workbook has no climate table.

## Options considered

Leave the workbook out, or register it for `safe_schools_events` with the same cell rules as pull request #100.

## Decision

19 October 2026. Scope expanded: also register `SAFE_SCHOOLS_18-19.xlsx` for `safe_schools_events`. A star is NA with `suppressed` Y. Blanks are skipped. Zeros are kept. Same cell rules as pull request #100.

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/89#issuecomment-6281041500 posted on the `jscotthorn` account through the Cursor GitHub App.

## Why

The comment names the state totals and the missing DEMO and DGROUP sheets, and still adds the file for events. Reason for adding 2018-19 now: the workbook exists and the events series can use it. Reason beyond that: not documented.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/89 (still open)
- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/100
- `config/kde_sources.yaml` (workbook skipped for discipline)

## Code check

Does not match. The yaml comment says the workbook is skipped for discipline. Issue #89 is still open, so the events registration has not landed. Not resolved here.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE statement that the 2018-19 workbook should or should not be joined to the later events series.
