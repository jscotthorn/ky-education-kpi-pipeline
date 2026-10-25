# 0023 Keep student-teacher ratio outliers and resolve the 2023 duplicate keys

Status: accepted

## Question

A few student-teacher ratios are far above the rest of the series, 2023 has duplicate district keys, and some cells are `*`. What should the pipeline store?

## What KDE changed

KYRC26 `KYRC26_OVW_Student_to_Teacher_Ratio.csv` is school rows only, so the district and state ratio series stop at 2025. The named outliers are published: Corbin district 483 in 2025, Cloverport district 86 in 2024, Cloverport school 115 in 2026.

## Options considered

Drop the outliers, average the 2023 duplicate district keys, or keep the published cells and pick the 2023 row that matches adjacent years.

## Decision

18 October 2026.

- Keep the outliers. Label them `kde-recheck`.
- For the 2023 duplicate district keys (Corbin and Floyd), keep the row that is consistent with the adjacent years.
- A `*` dropped by the old reader is fixed with the #49 `BaseETL` change, so a star is stored as NA with `suppressed` Y.
- The district and state ratio series end at 2025 because the KYRC26 file is school rows only.

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/56#issuecomment-6252037128 posted on the `jscotthorn` account through the Cursor GitHub App.

## Why

The comment keeps the published outliers and names the adjacent-year rule for the duplicate keys. Reason KDE published those outliers: not documented.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/56 (still open)
- `etl/student_teacher_ratio.py`
- [0021](0021-pandas-unpinned-comma-fixes.md)

## Code check

The KYRC26 school-only limit matches `etl/student_teacher_ratio.py` and `config/kde_sources.yaml`. The star path is not confirmed by a run. `should_skip_row` skips an empty ratio. `BaseETL.standardize_missing_values` now turns `*` into NA before that check. This pass did not confirm that a star becomes a stored Y row. The outlier-keeping and 2023 key choice were not re-checked against output. Issue #56 is still open. Not resolved here.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE note on these outliers, on the 2023 duplicate district keys, or on why the KYRC26 ratio file has no district or state row.
