# 0045 One courses-offered row per school and year

Status: accepted

## Question

The courses-offered parser wrote one row per course, with a 1 or NA. Should the output be one row per school and year instead?

## What KDE changed

The source is still one row per course. The change is how those rows are counted into metrics.

## Options considered

Keep a row per course, or collapse to one row per school and year with counts.

## Decision

18 October 2026. One row per school and year, fixed in pull request #81.

- `courses_offered_count` is the number of course rows. Starred rows count, and the metric is stored as `suppressed` N.
- `courses_offered_suppressed_count` is the number of starred rows.
- Per-category counts for CTE, HPE, VPA, and WL, with `_other` only if a course cannot be classified.

A later comment on the same issue: a blank enrollment total is not counted and not stored. The median is 7 courses per school in every year from 2019 to 2026.

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/82#issuecomment-6252285472 posted on the `jscotthorn` account through the Cursor GitHub App.

The implementation note is https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/82#issuecomment-6252575246

## Why

The decision comment sets the grain at one row per school and year so a starred course still counts toward the school total, with a separate suppressed count. `notes/104` says the same collapse of the per-course 1/NA rows.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/82
- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/81 merged as `8381bcd`
- `notes/104--kyrc26-file-to-etl-mapping.md`
- Starred All Students total, which is a different cell: [0034](0034-courses-offered-starred-total.md)

## Code check

Matches. The follow-up comment says the output is 75,299 rows: 10,757 school-years times 7 metrics, with no duplicate keys. This pass did not re-run the module.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

The grain is a pipeline choice. Not found: a KDE statement that courses offered must be stored as one row per school.
