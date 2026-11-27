# DNNNN Record flagged keys in a committed list

Status: accepted

## Question

Where should a `kde-recheck` label live when a published cell is stored as printed and still needs a later look?

## What KDE changed

Nothing in this record is a KDE layout change.

## Options considered

Add a column on the KPI row, or keep a committed list of keys plus a caveat in `KPIS.md`.

## Decision

27 November 2026. Scott delegated to Gremlin's recommendation.

Scott, 12:47 PM ET: "go with your recommendations for each, and only bring me ones where you are unsure"

Gremlin's recommendation, the one that sentence approved: "the committed list plus the caveat for now… we can switch to a real column later."

The list for this batch is `notes/flags/G-flagged-keys.csv`. Columns are `module`, `year`, `district`, `school_id`, `student_group`, `metric`, `value`, `reason`, and `label`. `label` is `kde-recheck`. No KPI column was added.

## Who decided and how we know

Scott, chat answer in Gremlin's chat, 27 Nov 2026, 12:47 PM ET, verified by Archivist. Source: "Scott delegated to Gremlin's recommendation".

Repo comment: none

## Why

The verified file records the sentence as approval of Gremlin's stated recommendation, not as Scott's own wording for the list.

## Links

- `notes/flags/G-flagged-keys.csv`
- `KPIS.md`
- [0022](0022-teacher-experience-zero-averages.md)
- [0023](0023-student-teacher-ratio-outliers.md)

## Code check

Matches. The 20 published teacher-experience averages of 0 and the three student-teacher ratio outliers are in `notes/flags/G-flagged-keys.csv`. `KPIS.md` names that file. The KPI schema has no flag column.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE instruction for how a downstream table should mark a published cell for a later recheck.
