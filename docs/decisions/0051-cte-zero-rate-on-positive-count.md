# 0051 Keep a 0.0 CTE participation rate that sits on a positive count

Status: accepted

Source type: Scott's own answer in an agent chat, quoted with date and time (ET). Bot: ECC manager bot. Archivist checked the quote verbatim against the transcript.

Repo comment: pending

## Question

699 CTE participation cells publish a 0.0 rate next to a positive participant count: 208 in KYRC24 and 491 in KYRC23. Keep that 0.0, or treat it as something else?

## What KDE changed

No KDE definition of a 0.0 rate beside a positive count was found.

`notes/116--cte-kyrc24-republish.md` counts 208 KYRC24 participation rates of 0.0 on a positive participant count, across 24 schools. Pike County Virtual Academy `491020` All Students is 108 participants and 0.0. Those cells were already 0.0 before the May 2026 republish, and the note says they stay.

`notes/114--cte-2023-published-rates.md` counts 491 KYRC23 participation rates of 0.0 on a positive participant count, across 54 schools. Adair County High School `001010` All Students is 377 participants and 0.0. The annual enrollment on that row is 865, so the cell is not participants divided by enrollment.

208 + 491 = 699. This pass did not re-count the raw files. These cells are not part of the 5,407 blank-count cells in [0009](0009-cte-zero-rate-beside-blank-count.md).

## Options considered

Option A, named in the pull request #116 review: keep the published 0.0 with a warning, the same rule as the 5,407 cells. Scott chose A. The other options were not restated in his answer.

## Decision

25 October 2026, 12:04 PM ET. Option A. Keep the 699 cells as published (`suppressed` N, value 0.0), with a warning, the same way as the 5,407 cells in [0009](0009-cte-zero-rate-beside-blank-count.md).

Scott's words: "A, but the recheck is likely to only cover the most recent year's data."

The 27 October 2026 recheck is likely to cover only the most recent year, 2025-26, not these 2022-23 and 2023-24 cells. The cells stay as published with a warning until KDE republishes those years (2022-23 / 2023-24).

The phrase "until KDE republishes those years (2022-23 / 2023-24)" is Moon's summary of Scott's remark. It is not his own words. The same end condition is recorded on [0009](0009-cte-zero-rate-beside-blank-count.md). A later public file for these years is read under [0052](0052-republish-supersedes-earlier.md): latest public file after the review window, unless it is visibly broken, with the differences recorded in [kde-republishes.md](kde-republishes.md).

## Who decided and how we know

Scott, direct, 2026-10-25 12:04 PM ET, in the ECC manager bot's chat; transcript checked by Archivist.

Option A is the rule already used for the blank-count cells: https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/116#issuecomment-6388993945 and https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/112#issuecomment-6388875367

## Why

Scott chose A. That is the #116 option A rule: keep the published number, with a warning, and do not convert it to NA. His second clause is why the end condition is not the 27 October recheck. The "until KDE republishes" wording is Moon's summary, marked above.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/112
- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/116
- `notes/114--cte-2023-published-rates.md`
- `notes/116--cte-kyrc24-republish.md`
- [0009](0009-cte-zero-rate-beside-blank-count.md)
- [0052](0052-republish-supersedes-earlier.md)

## Code check

Matches the keep-as-published rule. `etl/cte_participation.py` stores a numeric 0 as `suppressed` N. `notes/116` says the 208 KYRC24 cells stay. `notes/114` says the published 2023 percent is what is stored, including the 491 zeros on a positive count. This pass did not re-run the module.

## KDE guidance check

Verdict: possible conflict. Flag only. The decision is unchanged.

Same flag as [0009](0009-cte-zero-rate-beside-blank-count.md). A published 0 is kept. These 0.0 rates sit on a positive count, and for at least the Adair County High example the rate is not participants divided by enrollment. Storing them as measured zeros may present a cell KDE did not define. Not found: a KDE definition of a 0.0 participation rate beside a positive count. CHG-2022's suppression sentence is in [0001](0001-suppression-rule.md).
