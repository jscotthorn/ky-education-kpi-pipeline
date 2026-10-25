# 0018 Keep published discipline zeros; skip all-star padding

Status: superseded

The skip-all-star clause is replaced by [0017](0017-discipline-all-star-rows.md). The keep-zeros clause is still in effect.

## Question

How should safe-schools discipline treat a published 0, a real `*`, and a row that is all stars?

## What KDE changed

Nothing in this record is a KDE layout change. The choice is how to read cells KDE already publishes.

## Options considered

Drop zeros, store zeros, store every star, or skip an all-star row.

## Decision

18 October 2026.

- Keep a published 0 as 0 with `suppressed` N.
- Store a real `*` as NA with `suppressed` Y.
- Skip all-`*` padding rows, the same way as issues #44 and #46.

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/52#issuecomment-6252027144 posted on the `jscotthorn` account through the Cursor GitHub App.

## Why

The comment groups the all-star rows with the padding rows named in #44 and #46. It does not quote KDE. Reason KDE would call them padding: not documented.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/52
- Close comment https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/52#issuecomment-6399043058
- Replaced in part by [0017](0017-discipline-all-star-rows.md)
- Same padding idea as [0004](0004-postsecondary-star-cells.md) and [0005](0005-cte-skip-all-star-padding.md)

## Code check

The keep-zeros clause matches pull request #87. The skip clause does not: #87 stores all-star rows as Y. That later choice is [0017](0017-discipline-all-star-rows.md). This record stays because the 18 October choice is what #87 replaced.

## KDE guidance check

Verdict: possible conflict, on the skip clause only. Flag only.

CHG-2022 says `*` is suppression (quote in [0001](0001-suppression-rule.md)). Not found: a KDE statement that an all-star row is padding. [0017](0017-discipline-all-star-rows.md) records that a republish turned most of those rows into zeros or counts. The keep-zeros clause is consistent with storing a published 0.
