# 0031 Dual credit for 2021 and 2022, and the KYRC23 EDOP files

Status: accepted, code differs (follow-up open)

## Question

Where do 2020-21 and 2021-22 dual-credit counts come from, and does the KYRC23 EDOP file stay the 2022-23 source when another slice also matches?

## What KDE changed

`advanced_courses` files have a DC slice for 2021 and 2022. KYRC23 publishes separate EDOP dual-credit files. The DC slice matches the EDOP file on 472 of 472 compared cells. KYRC23 stars 58 more cells and omits school `145035`.

## Options considered

Leave 2021 and 2022 dual credit unloaded, or load the DC slice after pull request #79. For 2022-23, replace the EDOP files with the DC slice, or keep the EDOP files and document the alternate.

## Decision

18 October 2026.

- (b) Option 1: after #79, load 2021 and 2022 dual credit from the DC slice of the advanced-courses files. https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/74#issuecomment-6251799530
- (c) Option 1: the KYRC23 EDOP dual-credit files stay the source for 2022-23. Document the alternate DC slice (472 of 472 match), the 58 extra KYRC23 stars, and the omitted school `145035`. https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/74#issuecomment-6251808825

## Who decided and how we know

Both comments were posted on the `jscotthorn` account through the Cursor GitHub App.

## Why

The comments choose the DC slice for the two missing years and keep the EDOP files where they already exist. Reason for keeping EDOP when the slice matches: the comment records the extra stars and the missing school, and still keeps EDOP.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/74 (still open)
- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/79
- `etl/advanced_coursework.py`

## Code check

Part (b) is loaded on this branch. `etl/dual_credit.py` reads the `DC` slice of `advanced_courses_participation_and_performance_2021.csv`, `advanced_courses_participation_and_performance_2022.csv`, `advanced_courses_offered_2021.csv`, and `advanced_courses_offered_2022.csv`. Those four names are registered under `dual_credit` in `config/kde_sources.yaml`. `prepare_kde_data.py` downloads that copy into `data/raw/dual_credit/`. State All Students enrollment is 36,641 in 2021 and 38,371 in 2022. School codes stay as published (`999`, `165`). Part (c) still matches: KYRC23 EDOP files are the 2022-23 source. The 2023 advanced-course DC slice is not loaded. Issue #74 is still open. The status stays `accepted, code differs (follow-up open)` until that status is updated after merge. This code check is not that status change.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE statement that names the DC slice or the EDOP file as the dual-credit source for these years.
