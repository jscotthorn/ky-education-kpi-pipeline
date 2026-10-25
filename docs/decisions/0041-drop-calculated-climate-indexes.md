# 0041 Drop the calculated climate and safety indexes

Status: accepted

## Question

`climate_index_score_calculated` and `safety_index_score_calculated` averaged survey items and then kept one row per school. The dedupe key omitted school level, so the elementary file won. Keep those averages?

## What KDE changed

KDE publishes per-level climate and safety indexes. The release bulletin, re-fetched here, says 2025-26 is the last year the climate and safety indicator is in accountability. https://content.govdelivery.com/accounts/KYDE/bulletins/42e02ae

## Options considered

Keep the level-blind average, or remove it and use the published per-level indexes from 2022-23 on.

## Decision

20 October 2026. Remove `climate_index_score_calculated` and `safety_index_score_calculated`. The level-blind dedupe had kept the elementary value. The published indexes replace them: `climate_index_score_{elementary|middle|high}` and the same three safety names, from 2022-23 on. There is no unsuffixed alias.

## Who decided and how we know

`notes/107.md`: "Scott Horn decided on 2026-10-20 to remove `climate_index_score_calculated` and `safety_index_score_calculated`."

## Why

The note: the survey average ignored school level and kept the elementary file, because the old key omitted level and the elementary filename sorted first. The published per-level indexes replace that average.

## Links

- `notes/107.md`
- `notes/104--kyrc26-file-to-etl-mapping.md` (climate section)
- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/103
- 2021-22 registration is a separate choice: [0042](0042-do-not-register-2021-22-climate-index.md)

## Code check

Matches the removal described in `notes/107.md`. `etl/safe_schools_climate.py` no longer writes the two calculated metrics. This pass did not re-run the module.

## KDE guidance check

Verdict: consistent. Flag only in the sense that it is a check, not a change.

The dropped values were a pipeline average. KDE publishes the per-level indexes. REL-2026, re-fetched: 2025-26 is the last year the climate and safety indicator is in the accountability system. https://content.govdelivery.com/accounts/KYDE/bulletins/42e02ae
