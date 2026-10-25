# 0032 Keep the published 2026 IB tested counts

Status: accepted

## Question

The 2026 IB tested count drops to 42 after 234 the year before. Store it, drop it, or replace it with a derived rate?

## What KDE changed

KYRC26 publishes the IB tested count, including the state figure 42. No KDE note explaining the drop was found.

## Options considered

Drop the 2026 count, derive `ib_qualifying_score_rate` for 2026, or keep the published count and leave the derived rate unstored.

## Decision

18 October 2026. Keep the published 2026 IB tested counts. Add a `KPIS.md` note that they look suspect. Leave the derived 2026 `ib_qualifying_score_rate` unstored. Recheck on 27 October 2026.

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/78#issuecomment-6251763949 posted on the `jscotthorn` account through the Cursor GitHub App.

## Why

The comment keeps the published count and withholds the derived rate. Reason the count fell from 234 to 42: not documented.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/78
- `README.md` (states the 42 after 234, and that the derived rate is not stored)
- `KPIS.md`

## Code check

Matches the storage rule described in `README.md`: the published count is kept and the derived 2026 rate is not stored. `etl/advanced_coursework.py` still contains a qualifying-score derivation for other years. This pass did not confirm that the 2026 path is skipped in code. The README states that it is unstored.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE note on the 2026 IB tested count, or a statement that a qualifying-score rate should be derived when the count looks low.
