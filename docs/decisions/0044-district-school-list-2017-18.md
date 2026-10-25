# 0044 Add the 2017-18 district and school list

Status: accepted, code differs (follow-up open)

## Question

Kindergarten already loads 2017-18, and those rows have no school-list match. Add `DISTRICT_SCHOOL_LIST_17-18.xlsx`?

## What KDE changed

The file returns HTTP 200. It has 1,644 rows, school year 20172018, and 177 district totals. It has no EMAIL column.

## Options considered

Leave 2017-18 unmatched, add the list and invent the missing email values, or add the list and leave email empty.

## Decision

19 October 2026. Add the 2017-18 district and school list after pull request #97. That sentence is the issue #98 body.

"Do not invent EMAIL values" is a reviewer instruction, not Scott's decision. It is the Data Engineer's #97 re-check: https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/98#issuecomment-6281029233 ("the 2017-18 list is missing the EMAIL field. Handle it the way the module handles other missing fields (don't invent values).").

## Who decided and how we know

The decision is in the body of issue #98: "Decision (Scott, 2026-10-19): add the 2017-18 district/school list after #97." https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/98

The issue was opened on the `jscotthorn` account through the Cursor GitHub App.

## Why

The issue body: kindergarten already loads 2017-18, so those rows have no school-list match. The EMAIL sentence is the Data Engineer's re-check instruction cited above, not a second decision by Scott.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/98 (still open)
- `config/kde_sources.yaml` notes that the file returns 200

## Code check

Does not match. The yaml comment records that the file returns 200 and describes it. The list is not registered as a loaded source. Issue #98 is still open. Not resolved here.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE statement that the 2017-18 list should be joined to later school lists, or that a missing email should be filled.
