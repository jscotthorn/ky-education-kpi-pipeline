# 0003 Normalize state and district IDs to the 2024 convention

Status: accepted

## Question

State and district IDs change between 2023 and 2024 (`999` / `xxx` / `State` versus `999000` / `xxx000` / `All Districts`). Should the pipeline normalize them?

## What KDE changed

The files use both forms. The glossary defines a six-digit school code and a three-digit district code (https://reportcard.kyschools.us/glossary, not re-fetched). No KDE notice of the format change was found. HIST says datasets include consistent identifiers (https://education.ky.gov/Open-House/data/Pages/Historical-SRC-Datasets.aspx, not re-fetched). The files do not.

## Options considered

The issue proposes one shared step. It does not list a rejected alternative in the decision comment.

## Decision

18 October 2026. Add one shared step that normalizes every module's state and district IDs to the 2024+ convention: state `999000`, districts `xxx000`, and one state name. School IDs are unchanged. It is its own pull request after the per-script KYRC26 loop. The comment says this should also resolve the #42 symptom. [0002](0002-report-scripts-out-of-scope.md) says #61 must not fix the report scripts.

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/61#issuecomment-6251845769

## Why

The issue body: school_id joins break between 2023 and 2024, so year-over-year district and state series do not line up.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/61
- Later case: https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/61#issuecomment-6387831105
- [0010](0010-cte-2023-mixed-ids.md)

## Code check

Not implemented. Modules still store the IDs printed in each file. 2023 CTE rates and counts use different ID widths ([0010](0010-cte-2023-mixed-ids.md)). Issue #61 is open.

## KDE guidance check

Verdict: consistent. Flag only.

The target `999000` / `xxx000` is the form KYRC files publish. Not found: a KDE crosswalk from `999` / `xxx` to those IDs. Keeping each file's published ID until that step is the temporary rule in [0010](0010-cte-2023-mixed-ids.md).
