# 0022 Emit the 20 published teacher-experience averages of 0.00

Status: accepted, code differs (follow-up open)

## Question

`etl/teacher_experience.py` drops a row when average years are not greater than 0. Twenty published averages are 0.00. Keep the drop, or store the zeros?

## What KDE changed

Nothing in this record is a KDE layout change. KDE published the 0.00 averages. Every one of the 20 is a single-educator school with 0 total years.

## Options considered

Keep the `> 0` filter, or emit the 20 published 0.00 averages and label the single-educator schools for a KDE recheck.

## Decision

18 October 2026. Emit the 20 published 0.00 averages. Remove the `> 0` drop. Note the single-educator schools whose total years are 0. Mark them `kde-recheck`.

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/54#issuecomment-6252032940 posted on the `jscotthorn` account through the Cursor GitHub App.

## Why

The comment says the 0.00 figures are published, so the filter should not drop them. The module comment says the omission is not a rule to preserve.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/54 (still open)
- `etl/teacher_experience.py`

## Code check

Does not match. `etl/teacher_experience.py` still requires `avg_years > 0` (around line 146). Issue #54 is still open. Not resolved here. The choice is accepted and the follow-up has not landed, so the status is `accepted, code differs (follow-up open)`.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE statement about a published average of 0.00 years, or about a one-educator school with 0 total years.
