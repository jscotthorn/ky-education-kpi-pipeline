# 0016 Glasgow Independent conflicting district totals

Status: accepted

## Question

Glasgow Independent (`197`) has two district-total rows in 2020 and in 2022, and the values conflict. Which value is stored?

## What KDE changed

`notes/105.md` lists both published values. No KDE correction was found.

## Options considered

The 18 October comment said to resolve the duplicates so no duplicate keys remain. The 20 October comment states how.

## Decision

20 October 2026. Conflicting 2020 and 2022 district totals are stored as NA with `suppressed` Y. Both values are recorded in `notes/105.md`. Agreeing pairs stay the published percent. Tracked for the 27 October 2026 recheck in issue #108.

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/62#issuecomment-6300574763

## Why

The comment states the storage rule. It does not say why NA/Y was chosen instead of one of the two numbers. Reason not documented beyond the duplicate key.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/62
- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/108
- `notes/105.md` (Glasgow table)
- Commit cited in the 25 October close comment: `fd0f34d`

## Code check

`notes/105.md` records the stored NA/Y cells and the two source values. This pass did not re-run the module. The close comment says the duplicates were stored as suppressed in `fd0f34d`.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

No KDE page was found that says which Glasgow total is correct, or that a conflicting duplicate should be stored as suppressed.
