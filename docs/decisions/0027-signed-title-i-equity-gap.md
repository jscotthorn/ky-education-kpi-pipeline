# 0027 Store the Title I equity gap as a signed value

Status: superseded (in part)

## Question

Novice, ineffective, and out-of-field files publish a Title I gap that can be negative. Should the pipeline drop negatives, store the absolute value, or store the signed difference?

## What KDE changed

2020-2023 and KYRC26 publish a signed gap. KYRC24 and KYRC25 publish the absolute value for novice teachers. The out-of-field module comment says that from the year the gap is Title I minus Not Title I, a negative cell is one where Not Title I is higher.

## Options considered

Keep dropping negatives, store the absolute value in every year, or compute Title I minus Not Title I and store the signed result.

## Decision

18 October 2026. Compute Title I minus Not Title I and store the signed value. Stop dropping negatives in novice, ineffective, and out-of-field. If a component is suppressed, the gap is `suppressed` Y. Document disagreements and label them `kde-recheck`.

Superseded in part by [DNNNN](DNNNN-store-published-gap.md). Computing Title I minus Not Title I does not stand. Record 0054 stores the published cell, and DNNNN applies that rule here. Stopping the drop of a negative cell still stands, and this branch does that. A published negative is stored with `suppressed` N. The rule that a suppressed component makes the gap `suppressed` Y is not applied. Folding that #59 choice into this batch is Data Engineer's plan, not Scott's decision.

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/59#issuecomment-6251941580 posted on the `jscotthorn` account through the Cursor GitHub App.

## Why

The comment says the published gap is a difference, so a negative is a real direction. Dropping it removes the cells where Title I is lower.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/59 (still open)
- `etl/novice_teachers.py`
- `etl/students_taught_by_ineffective_teachers.py`
- `etl/students_taught_by_out_of_field_teachers.py`
- Published-values rule: [0054](0054-store-values-as-kde-publishes.md)

## Code check

The compute-the-gap part is not implemented. Novice equity and ineffective store a published negative cell with `suppressed` N. Out-of-field already did. The institutional novice rates still require `value >= 0`. The gap is not recomputed. Issue #59 is still open. The part that still stands is the stop on dropping negatives, and [DNNNN](DNNNN-store-published-gap.md) records it.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE definition of the equity-gap formula that this pass re-fetched. The pipeline comments describe the formula. They are not a KDE page.
