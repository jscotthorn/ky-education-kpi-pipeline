# 0017 Store all-star discipline rows as NA with suppressed Y

Status: accepted

## Question

When every cell on a safe-schools discipline row is `*`, should the pipeline skip the row or store it like any other star?

## What KDE changed

A later KYRC23 file republishes 2022-23 discipline. Many rows that were all stars in the earlier file are zeros or positive counts in the republish.

## Options considered

Skip all-star rows, as [0018](0018-discipline-zeros-and-padding.md) had said, or store them as NA with `suppressed` Y.

## Decision

19 October 2026. Store an all-star discipline row the same way as any other star: empty value, `suppressed` Y. This replaces the skip clause in [0018](0018-discipline-zeros-and-padding.md). Published zeros stay 0 with `suppressed` N.

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/87#issuecomment-6278902105 posted on the `jscotthorn` account through the Cursor GitHub App.

## Why

The comment: of 10,321 matched all-star rows in the KYRC23 republish of 2023, 9,057 became all zeros, 434 became positive counts, and 971 stayed starred. Skipping the row would drop cells a later file fills in.

9,057 + 434 + 971 = 10,462, not 10,321. The mismatch is inherited from `notes/104--kyrc26-file-to-etl-mapping.md` line 2363 and from `KPIS.md` line 193. This record does not replace the cited 10,321.

## Links

- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/87 merged at `3b6d597`
- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/52 close comment https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/52#issuecomment-6399043058
- `notes/104--kyrc26-file-to-etl-mapping.md` (discipline all-star section)
- Supersedes the skip clause of [0018](0018-discipline-zeros-and-padding.md)
- Same rule for CTE, decided 25 October 2026: [0050](0050-cte-all-star-rows-stored.md). [0005](0005-cte-skip-all-star-padding.md) is superseded.

## Code check

Matches. Pull request #87 stores the all-star rows. The same change added 410,320 suppressed Y cells. The zero-keeping clause of [0018](0018-discipline-zeros-and-padding.md) still matches.

## KDE guidance check

Verdict: consistent. Flag only in the sense that it is a check, not a change.

CHG-2022 says `*` is suppression. Quote in [0001](0001-suppression-rule.md). https://www.education.ky.gov/districts/tech/sis/Documents/SRC_ReportingChanges_2022.pdf
