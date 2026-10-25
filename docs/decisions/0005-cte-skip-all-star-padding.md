# 0005 Skip all-star CTE padding rows

Status: superseded (in part)

Superseded by Scott on 2026-10-25. The skip-all-star clause is replaced by [0050](0050-cte-all-star-rows-stored.md). This record stays.

## Question

KYRC26 CTE participation adds a full grid, including 1,230 all-`*` demographic rows that KYRC25 omitted. Should those rows be stored as suppressed, or skipped?

## What KDE changed

`notes/104--kyrc26-file-to-etl-mapping.md`: KYRC26 adds 1,230 all-star demographic rows. No KDE statement was found that says those stars are padding.

## Options considered

Store every `*` as Y (notes/8), or skip all-`*` rows. The decision splits them.

## Decision

18 October 2026, with [0004](0004-postsecondary-star-cells.md).

- Store real `*` cells as NA with `suppressed` Y.
- Skip all-`*` padding rows, such as the 1,230 in the KYRC26 full grid.
- At career and technical centers, enrollment `N/A` stays an empty denominator and is not suppressed.
- Document the counts and label ambiguous cases `kde-recheck`.
- Fix in one follow-up with #44 and the #49 CTE pandas-3 fix. #47 is separate.

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/46#issuecomment-6251989539

## Why

The comment names the 1,230 rows as padding. It does not quote KDE. Reason KDE would call them padding: not documented.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/46
- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/106
- `notes/106--cte-participation.md` line 9
- `etl/cte_participation.py` `convert_to_kpi_format`
- Contrast, now the same rule: [0017](0017-discipline-all-star-rows.md) and [0050](0050-cte-all-star-rows-stored.md)

## Code check

The skip clause does not match the code, and that clause is what [0050](0050-cte-all-star-rows-stored.md) replaces. Pull request #106 stored stars, including a fully starred demographic, as Y. The current rule's code check is on 0050.

## KDE guidance check

Verdict: possible conflict. Flag only.

CHG-2022 says `*` is suppression (quote in [0001](0001-suppression-rule.md)). Not found: a KDE statement that an all-star row is padding or means no students. [0017](0017-discipline-all-star-rows.md) found that many all-star discipline rows in another file were zeros or real counts in a republish.
