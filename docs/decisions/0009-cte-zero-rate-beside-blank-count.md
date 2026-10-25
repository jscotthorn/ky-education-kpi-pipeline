# 0009 Keep CTE 0.0 rates that sit next to a blank count

Status: accepted

## Question

KDE prints a 0.0 CTE rate next to a blank count. Should that cell be stored as a real zero (`suppressed` N) or treated as suppression?

## What KDE changed

No KDE definition of this pair was found.

The 24 October comment on issue #112 states the count as 5,407 cells:

- 2024: 1,230 completion and 146 participation
- 2023: 2,542 completion and 1,489 participation

1,230 + 146 + 2,542 + 1,489 = 5,407. The four parts are in the notes. This pass did not re-count the raw files.

- 2024 parts: `notes/116--cte-kyrc24-republish.md` and `KPIS.md` (1,376 = 1,230 + 146). Each was `*` in the May 2025 copy. The paired count is blank.
- 2023 completion: `notes/114--cte-2023-published-rates.md`, 2,542 rates of 0.0, none with a numeric completer count. The annual completer cell is `*`.
- 2023 participation: the same note, 1,489 of 1,980 participation zeros sit on a blank participant count.

Pull request #116's review calls this option A. The decision comment does not use that label.

A different set, 208 KYRC24 and 491 KYRC23 participation rates of 0.0 on a positive participant count, is [0051](0051-cte-zero-rate-on-positive-count.md).

`notes/116` also says 918 of the 1,230 new completion zeros have a positive, unchanged participant count and a positive, unchanged participation rate. Further rows in that table also have a positive participant count (72 `--`, 46 `N/A`, 42 already 0.0). 918 is not the count of every 0.0 completion cell on a positive participant count.

## Options considered

The review name "option A" is keep as published. Other options are not written in the decision comment.

## Decision

24 October 2026. Keep KDE's published 0.0 CTE rates that sit next to a blank paired count as published (`suppressed` N, value 0.0), with a warning in `KPIS.md` and the notes. Pull request #116 calls this option A.

25 October 2026, 12:04 PM ET. The end condition is no longer "until the 27 October 2026 KDE recheck." Scott said the recheck is likely to cover only the most recent year's data. That year is 2025-26, not these 2022-23 and 2023-24 cells. The cells stay as published with a warning until KDE republishes those years (2022-23 / 2023-24).

The phrase "until KDE republishes those years (2022-23 / 2023-24)" is Moon's summary of that remark. It is not Scott's own words. His words are quoted on [0051](0051-cte-zero-rate-on-positive-count.md). A later public file for these years is read under [0052](0052-republish-supersedes-earlier.md): latest public file after the review window, unless it is visibly broken, with the differences recorded in [kde-republishes.md](kde-republishes.md).

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/112#issuecomment-6388875367

The pull request #116 approval says this matches option A and does not convert the cells to NA: https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/116#issuecomment-6388993945

## Why

The 24 October comment gives evidence the cells may be suppressed, and at that time said keep them as published until a recheck. The 25 October end condition, above, replaces that date:

- All 1,376 such 2024 cells were `*` in the May 2025 copy.
- All 2,542 such 2023 completion cells are `*` in the 2023 annual file.
- No KDE CTE file from 2023 to 2026 publishes a numeric 0 count.
- Lynn Camp `301025` shows 0.0 completion in 2024 and 38 completers in 2025.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/112
- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/116
- `notes/114--cte-2023-published-rates.md`
- `notes/116--cte-kyrc24-republish.md`
- `KPIS.md` (`cte_participation_rate`, `cte_completion_rate_grade_12`)
- [0008](0008-kyrc24-cte-may-2026-republish.md)
- [0051](0051-cte-zero-rate-on-positive-count.md)
- [0052](0052-republish-supersedes-earlier.md)

## Code check

Matches the keep-as-published rule. `etl/cte_participation.py` stores a numeric 0 as `suppressed` N. `notes/116` says the 1,376 new 0.0 rates are kept and are not a new kind of suppression. `notes/116` describes the behavior and cites #112. It does not use the words "option A" or link the 24 October decision comment. The #116 review listed that wording as a non-blocking follow-up. Not added here, because this log does not edit `notes/`.

## KDE guidance check

Verdict: possible conflict. Flag only.

The base rule "a published 0 is kept" is what the decision applies. The flag: the 2024 cells were `*` a year earlier, and CWM-2024-12 says KYRC reloads change "the way small cell counts are suppressed" (https://content.govdelivery.com/accounts/KYDE/bulletins/3c68ec8, not re-fetched). Storing them as measured zeros may present suppressed cells as zeros. CHG-2022's suppression sentence is the privacy rule that sentence is about (quote in [0001](0001-suppression-rule.md)). Not found: a KDE definition of a 0.0 rate beside a blank count. The 27 October 2026 recheck is not the end condition for these cells. See the decision.
