# 0046 Keep outside doors locked as a published yes/no

Status: accepted

## Question

`outside_doors_locked_during_school_hours` is 1 or 0 in the source. Store it as a new metric, drop it, or turn it into a rate?

## What KDE changed

KDE publishes the answer as 1 for yes and 0 for no, with stars and blanks in the same column.

## Options considered

Drop the column, store it as a rate, or store the published 1 and 0 as their own metric.

## Decision

19 October 2026. Keep `outside_doors_locked_during_school_hours` as a new metric, stored as KDE publishes it. 1 is yes. 0 is no. A star is NA with `suppressed` Y. Blanks are skipped. Zeros are kept. Document it as a yes/no answer, not a rate.

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/103#issuecomment-6282508178 posted on the `jscotthorn` account through the Cursor GitHub App. The comment is "Decision 3 (Scott, 2026-10-19)."

## Why

The comment stores the published answer and says it is not a rate. Reason not documented beyond that.

## Links

- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/103
- `notes/107.md` (the same pull request's climate decisions)

## Code check

Matches the rule in the pull request comment. The re-review on that pull request approved the change. This pass did not re-read the metric emission.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE statement that this column is a rate, or that a 0 should be suppressed.
