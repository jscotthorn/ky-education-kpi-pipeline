# 0034 Keep a starred courses-offered total as suppressed

Status: accepted

## Question

When KDE stars the All Students total for courses offered, and some grade cells on the same row are published, should the total stay suppressed?

## What KDE changed

The courses-offered file can star the All Students total while publishing one or more grade cells.

## Options considered

Rebuild the total from the published grade cells, or keep `courses_offered_count` as NA with `suppressed` Y (option 1).

## Decision

19 October 2026. Option 1: keep `courses_offered_count` as NA with `suppressed` Y when KDE stars the All Students total, even if some grade cells are published. No code change. Label it `kde-recheck`.

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/84#issuecomment-6279232616 posted on the `jscotthorn` account through the Cursor GitHub App.

## Why

The comment keeps the star. It does not rebuild a total KDE suppressed. See also [0001](0001-suppression-rule.md).

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/84
- Related row shape: [0045](0045-school-courses-one-row.md)

## Code check

Matches the "no code change" clause. The existing reader already stores the star as suppressed. This pass did not re-count the starred totals.

## KDE guidance check

Verdict: consistent. Flag only in the sense that it is a check, not a change.

CHG-2022 says `*` is suppression (quote in [0001](0001-suppression-rule.md)). https://www.education.ky.gov/districts/tech/sis/Documents/SRC_ReportingChanges_2022.pdf Replacing the star with a sum of published grade cells would publish a figure KDE suppressed.
