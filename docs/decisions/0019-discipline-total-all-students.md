# 0019 Store the discipline Total on All Students only

Status: accepted

## Question

KDE publishes a Total on more than one demographic row. Which row should hold the unduplicated-student metric?

## What KDE changed

The discipline files repeat a Total column on subgroup rows. Those subgroup totals are not the same measure as the All Students total.

## Options considered

Store every published Total, or store the Total only on All Students and skip the subgroup Totals.

## Decision

19 October 2026. The published Total is stored on All Students only, as the unduplicated-student metric. Subgroup Total cells are skipped.

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/87#issuecomment-6278920701 posted on the `jscotthorn` account through the Cursor GitHub App.

## Why

The comment gives the reason: "Subgroup Totals are skipped because they repeat the sum of the event columns." The same sentence is in `notes/104--kyrc26-file-to-etl-mapping.md` line 2367: "On a subgroup row Total repeats the sum of the event columns, so it is not stored." The All Students Total is the unduplicated count.

## Links

- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/87
- `notes/104--kyrc26-file-to-etl-mapping.md`

## Code check

Matches the rule merged in #87. This pass did not re-count the skipped subgroup rows.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE statement that a subgroup Total must be dropped, or that the All Students Total is the only unduplicated count.
