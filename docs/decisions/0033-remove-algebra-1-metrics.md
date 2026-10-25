# 0033 Remove the two Algebra I grade-8 metrics

Status: accepted

## Question

`algebra_1_grade_8_count` and `has_algebra_1_access` are emitted and documented, and neither has produced data. Remove them or keep the empty metrics?

## What KDE changed

Nothing in this record is a KDE layout change. The metrics were pipeline fields. The comment says neither has produced data.

## Options considered

Leave the empty metrics in the parser, `KPIS.md`, and the covariate catalog, or remove them (option 2).

## Decision

19 October 2026, confirmed 20 October 2026. Option 2: remove `algebra_1_grade_8_count` and `has_algebra_1_access` from the parser, from `KPIS.md`, and remove `has_algebra_1_access_grade_8` from the covariate catalog.

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/83#issuecomment-6279214433

https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/83#issuecomment-6282693160

Both were posted on the `jscotthorn` account through the Cursor GitHub App.

## Why

The comments say the metrics have not produced data. Reason they were added: not documented in those comments.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/83 (still open)
- `etl/school_courses.py`
- `KPIS.md` around the Algebra I lines

## Code check

Does not match. `etl/school_courses.py` still emits both metrics. `KPIS.md` still documents them around lines 715-716. Issue #83 is still open. Not resolved here.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

These names are pipeline metrics. Not found: a KDE column that this pass identified as the source the removal is dropping.
