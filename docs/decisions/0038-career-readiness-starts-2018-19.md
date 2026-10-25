# 0038 Career readiness starts in 2018-19

Status: accepted

## Question

`CTE_TRANSITION_READINESS_17-18.xlsx` exists. Should the career-readiness series include 2017-18?

## What KDE changed

The 2017-18 workbook is a different file from the 2018-19 series the pipeline already loads. This pass did not re-open the workbook.

## Options considered

Register the 2017-18 workbook, or leave it out so the series starts in 2018-19.

## Decision

19 October 2026. Leave `CTE_TRANSITION_READINESS_17-18.xlsx` out. The series starts in 2018-19.

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/92#issuecomment-6279388441 posted on the `jscotthorn` account through the Cursor GitHub App.

## Why

The comment leaves the workbook out. Reason it is not comparable: not documented in this pass beyond that choice. Reason not documented.

## Links

- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/92

## Code check

Matches the exclusion as recorded on the pull request. This pass did not re-read `config/kde_sources.yaml` for this filename.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE statement that 2017-18 transition readiness should be joined to, or kept apart from, the later career-readiness series.
