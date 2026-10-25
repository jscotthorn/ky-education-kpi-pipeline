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

The option he selected: "Yes, keep 3-digit 165/999 for #119 under the #114 precedent until #61."

[0010](0010-cte-2023-mixed-ids.md) is #114 item A. That choice covered mixed 2023 CTE IDs. Extending it to these finance rows is a new application. Scott confirmed that extension in the sentence above.

## Who decided and how we know

Primary source: https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/119#issuecomment-6399994227

That comment was posted on the `jscotthorn` account through the Cursor GitHub App. Its text: "Decision (Scott, 2026-10-25 12:12 PM ET): #119 keeps 3-digit district/state IDs (165, 999) on the 2024-25 spending rows under the #114 precedent, until #61 converts them to 6-digit. This confirms the note above."

Scott's chat answer the same minute, checked verbatim against the transcript by Archivist: "Yes, keep 3-digit 165/999 for #119 under the #114 precedent until #61."

Moon's earlier note, context for the comment above: https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/119#issuecomment-6399959209

That note says it was applied by Moon, the project manager, from the #114 precedent, and that it was not itself a new decision. The Decision comment is the repo record. Moon's note stays as context.

## Why

The Decision comment is the choice. Scott's chat sentence the same minute matches it. Moon's note gives the reason Moon had used: widening the IDs before #61 would put these rows out of step with the existing spending rows and with how the dashboard matches districts. That reason is in Moon's note. Scott's answer does not restate it.

## Links

- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/119
- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/118
- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/61
- [0010](0010-cte-2023-mixed-ids.md)
- [0003](0003-shared-id-normalization.md)
- [coverage-gaps.md](coverage-gaps.md)

## Code check

Matches the 3-digit choice. Pull request #119 merged on 2026-10-25 as `22f588ea0d34ebcc9431b2cdea1f14562fc8d0aa`. Issue #118 is closed. Checked against `feature/2025-26-kyrc26` at that commit. `etl/spending_per_student.py` reads the ten long files and stores the 2024 and 2025 district and state totals with the 2020-2023 labels. Fayette is school code `165`, school `---District Total---`. The state row is school code `999`, district `State`, school `---State Total---`. `select_district_state_spending` says `165000` and `999000` are not written. `config/kde_sources.yaml` registers those ten files on `url: kyrc25`. The wide KYRC24 and KYRC25 spending files are still school rows only.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE statement that 2024-25 spending totals must be stored as 3-digit or as 6-digit IDs.
