# 0030 Keep teacher rates published as 0% or above 100%

Status: accepted

## Question

Some teacher-rate cells are 0% or between 100% and 400% when the teacher count is 0 or blank. Drop them, or store them as published?

## What KDE changed

Nothing in this record is a layout change. KDE published those percents next to a zero or blank teacher count.

## Options considered

Skip a rate when the teacher count is 0 or blank, cap the rate at 100, or keep the published number.

## Decision

18 October 2026, on issues #69 and #72. Keep the rates as published, including 0% and values from 100% through 400% where the teacher count is 0 or blank. No skip rule. Label them `kde-recheck`.

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/69#issuecomment-6251837033

https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/72#issuecomment-6251837203

Both were posted on the `jscotthorn` account through the Cursor GitHub App.

## Why

The comments say the published percent stays, including when the count beside it is 0 or blank. Reason KDE published a rate above 100: not documented.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/69
- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/72

## Code check

Not fully re-checked against output. The decision is to add no skip rule. This pass did not search every teacher module for a cap at 100.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE statement about a 0% rate beside a zero teacher count, or about a rate above 100%. Storing a published 0 is consistent with the keep-zeros rule in [0001](0001-suppression-rule.md) only as a pipeline practice. KDE did not state that practice for these cells.
