# 0053 2024-25 district and state spending IDs stay 3-digit until #61

Status: accepted

## Question

Issue #118 asks for 6-digit district and state IDs on the 2024 and 2025 spending totals. Pull request #119 writes `165` and `999`. Which width is the record?

## What KDE changed

Nothing in this record is a KDE ID change. The 2020-2023 spending rows in this pipeline already use 3-digit `165` and `999`. Issue #118's body asks for 6-digit IDs from 2024, "consistent with #61."

## Options considered

Write `165000` and `999000` now, as #118 asks, or keep `165` and `999` until the shared normalization in #61.

## Decision

25 October 2026, 12:12 PM ET. Yes. Keep 3-digit `165` and `999` for pull request #119 under the #114 precedent until #61. Issue #61 will convert these to 6-digit.

Scott's words: "Yes, keep 3-digit 165/999 for #119 under the #114 precedent until #61."

[0010](0010-cte-2023-mixed-ids.md) is #114 item A. That choice covered mixed 2023 CTE IDs. Extending it to these finance rows is a new application. Scott confirmed that extension in the sentence above.

## Who decided and how we know

Scott, direct, 2026-10-25 12:12 PM ET, his answer on a question card in the ECC manager bot's chat; transcript checked by Archivist.

Moon's earlier note, which this confirms: https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/119#issuecomment-6399959209

That comment is on the `jscotthorn` account. Its text says it was applied by Moon, the project manager, from the #114 precedent, and that it was not itself a new decision. Scott's 12:12 sentence is the decision.

## Why

Scott's sentence is the choice. Moon's comment gives the reason Moon had used: widening the IDs before #61 would put these rows out of step with the existing spending rows and with how the dashboard matches districts. That reason is in the comment. Scott's answer does not restate it.

## Links

- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/119
- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/118
- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/61
- [0010](0010-cte-2023-mixed-ids.md)
- [0003](0003-shared-id-normalization.md)
- [coverage-gaps.md](coverage-gaps.md)

## Code check

Pull request #119 is open. It is not on this branch. This branch still has no 2024 or 2025 district or state spending rows. The ID width is a choice about the pull request that would add them. Not resolved here.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE statement that 2024-25 spending totals must be stored as 3-digit or as 6-digit IDs.
