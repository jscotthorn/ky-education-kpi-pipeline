# 0042 Do not register the 2021-22 climate and safety index file

Status: accepted

## Question

KDE published `quality_of_school_climate_and_safety_survey_index_scores_2022.csv` (47,574 rows, the same 18 columns as KYRC23). Should the per-level index series include 2021-22?

## What KDE changed

The file exists and is the published 2021-22 per-level index. CHG-2021, not re-fetched, is cited by the research as saying the index was new for 2021. CHG-2022, re-fetched, moves quality of school climate and safety into accountability. The series in this pipeline starts in 2022-23.

## Options considered

Register the 2022 file so the per-level series starts in 2021-22, or leave it unregistered.

## Decision

24 October 2026. Do not register `quality_of_school_climate_and_safety_survey_index_scores_2022.csv`. The per-level `climate_index_score_*` and `safety_index_score_*` series starts in 2022-23. The 2022 accountability number stays `climate_safety_indicator_rate_{level}`. The 2023 combined rate stays `climate_safety_combined_indicator_rate_{level}`. Those are not this series.

## Who decided and how we know

`notes/107.md`: "Decision recorded 2026-10-24 (Scott)."

## Why

Reason not documented. The note says "Scott accepts losing 2021-22 for this series." That states the consequence. It does not say why the published file stays unregistered.

## Links

- `notes/107.md`
- `notes/kde_datasets_2021_2022.md` (the file is listed there)
- [0041](0041-drop-calculated-climate-indexes.md)

## Code check

Matches. The note says a registered run does not download the file. The old selector would have written 95,148 KPI rows from it when the file was on disk. Those rows are not in the registered output.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

KDE did publish the 2021-22 file. Not found: a KDE statement that the file should be left out of a longitudinal series. The gap is the pipeline's. REL-2026, re-fetched, says the indicator's last accountability year is 2025-26. It does not say to omit 2021-22. https://content.govdelivery.com/accounts/KYDE/bulletins/42e02ae
