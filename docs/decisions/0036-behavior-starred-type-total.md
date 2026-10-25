# 0036 Keep the starred behavior type-file Total

Status: accepted

## Question

801 behavior type-file Totals are starred, and the same cells are exactly 3 in the location and context files. Store the 3, or keep the star?

## What KDE changed

Type files never publish 1, 2, or 3. Location, context, and grade files publish 3 and do not publish counts below 3. 801 is the KYRC26 count of type-file Total stars whose location and context Totals are exactly 3. KYRC25 has 769 of that same pattern, KYRC24 has 775, and the KYRC23 republish has 759. Those three counts are other years. They are not parts of the 801. 769 + 775 + 759 = 2,303. The parenthetical that reads as if they were parts of 801 is inherited from https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/101#issuecomment-6281188290. `notes/104--kyrc26-file-to-etl-mapping.md` line 2439 states the four counts as separate years.

## Options considered

Replace the star with the location or context Total (the published 3), or keep the type-file star (option a).

## Decision

19 October 2026. Option a: keep the type-file star. Do not store the location or context Total. No behavior change. Recheck on 27 October 2026. Documented in `notes/104` and `KPIS.md`, and tracked on issue #101.

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/100#issuecomment-6281265859

https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/101#issuecomment-6281266442

Both were posted on the `jscotthorn` account through the Cursor GitHub App. The count is in https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/101#issuecomment-6281188290

## Why

The decision keeps the star on the file that owns the Total. The finding that those cells are 3 elsewhere is the reason for the recheck, not a reason to copy the 3.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/101
- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/100
- `notes/104--kyrc26-file-to-etl-mapping.md`

## Code check

Matches the "no behaviour change" clause. The type-file star stays. This pass did not re-count the 801 rows.

## KDE guidance check

Verdict: consistent. Flag only in the sense that it is a check, not a change.

CHG-2022 says a small count that could identify a student is suppressed (quote in [0001](0001-suppression-rule.md)). Copying a 3 from another file into a starred cell would replace a suppression. Not found: a KDE sentence that these two files use different thresholds. That threshold split is from the issue comment, not from a KDE page re-fetched here.
