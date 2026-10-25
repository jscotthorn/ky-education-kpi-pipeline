# 0001 Store a suppressed cell as NA with suppressed Y

Status: accepted

## Question

When KDE marks a cell suppressed, should the pipeline drop the row or keep it with an empty value?

## What KDE changed

CHG-2022 widened suppression: a NAPD performance level with fewer than three students suppresses all performance levels, and low counts or percentages that could identify a student are also suppressed. https://www.education.ky.gov/districts/tech/sis/Documents/SRC_ReportingChanges_2022.pdf

## Options considered

`notes/8--suppression-handling-enhancement.md` describes the prior behavior (drop the row) and the replacement (keep the row).

## Decision

19 July 2025, in that journal: keep the cell. `value` is empty. `suppressed` is Y. A missing row is not read as "this group does not exist."

## Who decided and how we know

The journal does not name Scott. It presents the rule as the implemented design. Later decisions cite it. https://github.com/jscotthorn/ky-education-kpi-pipeline/blob/1c48f448df371191bd428e17767e27003ccc552c/notes/8--suppression-handling-enhancement.md

## Why

The journal's problem statement: dropped rows look like a group that is not in the school, and trend charts break. Quoted from `notes/8--suppression-handling-enhancement.md`.

## Links

- `notes/8--suppression-handling-enhancement.md`
- `KPIS.md` (CTE cell rules, around the participation metrics)
- Later records [0004](0004-postsecondary-star-cells.md), [0005](0005-cte-skip-all-star-padding.md), [0050](0050-cte-all-star-rows-stored.md), [0017](0017-discipline-all-star-rows.md)

## Code check

Modules do not all follow it. CTE participation stores a `*` as Y ([0050](0050-cte-all-star-rows-stored.md)). Postsecondary enrollment still drops `*` ([0004](0004-postsecondary-star-cells.md)). This record does not pick one of those later choices.

## KDE guidance check

Verdict: consistent, with one gap. Flag only.

KDE, re-fetched: "Suppression rules have been expanded following U.S. Department of Education (USED) guidelines. If a performance level – Novice, Apprentice, Proficient and Distinguished (NAPD) – has less than three students, all the performance levels will be suppressed. Data also will be suppressed in situations where low cell counts or percentages could lead to identification of individual student performance." https://www.education.ky.gov/districts/tech/sis/Documents/SRC_ReportingChanges_2022.pdf

Storing `*` as suppressed matches that meaning. Not found: a KDE definition of blank, `--`, or `N/A`. `notes/8` says suppression is for small counts under 10. KDE's sentence above uses three students for NAPD levels. The accountability minimum n in KDE's minimum-n guide was not re-fetched. The research cites it as 30, and cites a non-KDE ECS page for "N=10 for reporting."
