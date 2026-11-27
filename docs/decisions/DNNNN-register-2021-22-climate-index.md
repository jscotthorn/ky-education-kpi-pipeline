# DNNNN Register the 2021-22 climate and safety index file

Status: accepted

The number is a placeholder. A number is assigned at merge.

## Question

KDE published `quality_of_school_climate_and_safety_survey_index_scores_2022.csv`. [0042](0042-do-not-register-2021-22-climate-index.md) left it unregistered. Should the per-level index series include 2021-22, and may that year show on the dashboard?

## What KDE changed

The file is the published 2021-22 per-level index. It has 47,574 rows and the same 18 columns as KYRC23. Level codes are `ES`, `MS`, and `HS`. Nothing in this record recomputes a climate or safety score. [0054](0054-store-values-as-kde-publishes.md) still stands: store the published cells.

## Options considered

Leave [0042](0042-do-not-register-2021-22-climate-index.md)'s "Do not register" choice in place, or register the file and load it on the same per-level metric names.

## Decision

27 November 2026. Register `quality_of_school_climate_and_safety_survey_index_scores_2022.csv` and load it into `climate_index_score_{level}` and `safety_index_score_{level}`. Values stay as published. `ES`, `MS`, and `HS` use the same suffixes as the later files. State and district school codes stay as published (`999` and `165`).

This replaces the registration choice in [0042](0042-do-not-register-2021-22-climate-index.md). The metric-name parts of 0042 still stand. The 2022 accountability number stays `climate_safety_indicator_rate_{level}`. The 2023 combined rate stays `climate_safety_combined_indicator_rate_{level}`. There is no unsuffixed alias.

Scott's words, quoted as written: "Approve the J2 mirror additions and the new 2022 climate year on the dashboard".

The condition is Gremlin's question, which Scott picked, not Scott's own wording: the year shows "once Data Engineer confirms it loaded exactly as published". Data Engineer confirmed that condition on review of this load: md5 `aaec467c52a2310176900619881d2ad6`, sha256 `93f59d93fad6e0893e01a462fc1caed5f5b51662c8b1342acb2a5942ca4ba0fb`, 6,553,428 bytes; 58,954 values identical, 36,194 `*` became NA with `suppressed` Y. The file is in the regeneration mirror `mirror-2026-11-27-j2`. This record does not edit the mirror.

The 25 October 2026 sentence in [0042](0042-do-not-register-2021-22-climate-index.md) is the earlier load-later plan, not these 27 November words: "No specific reason; reopen the 2021-22 climate index as a low-priority load-later item." Issue #121 is that plan.

## Who decided and how we know

Scott, chat answer in Gremlin's chat, 27 Nov 2026, 12:29 PM ET, verified by Archivist.

Repo comment: none.

## Why

Scott approved the new 2022 climate year on the dashboard, on the condition quoted above. Registering the published file is what puts that year in the series. Reversing 0042's "Do not register" follows from this approval.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/121
- [0042](0042-do-not-register-2021-22-climate-index.md)
- [0054](0054-store-values-as-kde-publishes.md)
- `notes/120--dual-credit-dc-slice-and-climate-index-2022.md`
- `config/kde_sources.yaml`
- `etl/safe_schools_climate.py`

## Code check

Matches this branch. The file is registered as a historical string under `safe_schools_climate`. The loader selects that exact name. A registered run writes 95,148 KPI rows from the 47,574 source rows. State All Students climate / safety is elementary 79.4 / 73.8, middle 69.2 / 62.6, and high 64.1 / 58.6. No score is recomputed.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

KDE did publish the 2021-22 file. Not found: a KDE statement that the file should be left out of a longitudinal series. The same verdict is on [0042](0042-do-not-register-2021-22-climate-index.md). REL-2026, re-fetched for that record, says the indicator's last accountability year is 2025-26. It does not say to omit 2021-22. https://content.govdelivery.com/accounts/KYDE/bulletins/42e02ae
