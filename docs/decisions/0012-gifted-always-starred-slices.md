# 0012 Omit always-starred 2020-2023 gifted slices

Status: accepted

## Question

For `gifted_and_talented_2020.csv` through `gifted_and_talented_2023.csv`, preschool, grade 14, and Consolidated Student Group are starred on every row. Should those cells be written?

## What KDE changed

Those four files publish no 0 and no blank. The smallest number is 3. The issue says a star covers a count of 0, 1, or 2 and a not-applicable cell. The KYRC23 republish of 2022-23 has no preschool column and no Consolidated Student Group rows, and grade 14 is blank on all 13,567 rows (decision comment). No KDE sentence was found that says a gifted star means not applicable.

## Options considered

The issue asks whether to keep them (notes/8) or drop them. Option 2 is drop them.

## Decision

18 October 2026. Option 2. For those four files only, starred preschool, grade 14, and Consolidated Student Group cells are not written. Implemented in pull request #80.

Scope in the comment: a missing group row does not mean the group is empty. Consolidated Student Group is a real group (2019 state count 14,874). 2019 and 2024+ are unchanged. Every other 2020-23 star stays NA/Y. A published value in these slices would still be written. The comment says this removes 409,944 rows. The published count (N) stays 965,385.

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/77#issuecomment-6252407538

Resolved by #80 at `c84a6c9`: https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/77#issuecomment-6399042448

## Why

Quoted from the comment: KDE did not report these slices in these four files; they are starred on every row, including the state. In 2020-23 a star covers 0-2 as well as not applicable or not reported, so it is not evidence the slice exists. The KYRC23 republication has no preschool column, no Consolidated Student Group rows, and grade 14 blank on all 13,567 rows.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/77
- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/80
- `notes/104--kyrc26-file-to-etl-mapping.md` (gifted section)
- `notes/8--suppression-handling-enhancement.md`

## Code check

Matches the decision as recorded in `notes/104`: the skip is those filenames and those slices, and only when the cell is a star. This pass did not re-run the ETL. The row count 409,944 is the decision comment's figure, also in `notes/104`.

## KDE guidance check

Verdict: possible conflict. Flag only.

CHG-2022's only published meaning of `*` is suppression (quote in [0001](0001-suppression-rule.md)). Not found: a KDE statement that a gifted star means not applicable. The decision's evidence is from the files (starred on every row, including the state, and the KYRC23 republish). That departs from storing every star as Y, which is [0017](0017-discipline-all-star-rows.md) for a different file.
