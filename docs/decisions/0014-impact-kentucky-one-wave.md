# 0014 Store each Impact Kentucky survey once

Status: accepted

## Question

Each Impact Kentucky survey is published under two KPI years. Should both copies be stored?

## What KDE changed

The survey is every other year. `notes/105.md` lists administration windows and the duplicate files: 2021 repeats 2020, 2023 and KYRC23 repeat 2022, and KYRC25 repeats the 2023-24 survey. The research cites Kentucky Teacher: "administered every two years" (IKY-2024 and IKY-2026, not re-fetched).

## Options considered

The issue offered: document the double count, add a survey-wave column, or both. The decision is to store one wave per administration year.

## Decision

18 October 2026, item 1. Store each Impact Kentucky survey wave once, under the year the survey was administered, and guard the republished copy by exact filename.

`notes/105.md` uses the ending year of the administration school year: 2020, 2022, 2024, 2026.

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/62#issuecomment-6251855652

## Why

The issue body: 2020 and 2021 are the same survey (4,627 of 4,629 shared cells identical), and the same pattern holds for 2022/2023 and 2024/2025, so year-over-year trends double-count.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/62
- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/105
- `notes/105.md`
- `config/kde_sources.yaml` (`teacher_working_conditions`)
- `etl/teacher_working_conditions.py`

## Code check

Matches. The yaml comment says each administration is stored once and names the skipped republish files. Odd years have no row of their own. [0013](0013-climate-bridge-declined.md) is the separate naming choice.

## KDE guidance check

Verdict: consistent. Flag only.

The KPI years 2020, 2022, 2024, and 2026 are the administration school years in `notes/105.md`. Gaps in the other years match a biennial survey. Kentucky Teacher pages were not re-fetched in this pass.
