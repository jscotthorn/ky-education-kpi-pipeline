# 0057 Postsecondary stars use the in-file cohort count

Status: accepted

Repo comment: none

## Question

Which cohort count tells a real postsecondary `*` from padding, and what is stored when that cohort count is itself `*`?

## What KDE changed

Nothing in this record is a new KDE layout. `*` is already KDE's suppression mark. [0004](0004-postsecondary-star-cells.md) records the 18 October 2026 rule. This record specifies which cohort count that rule uses, and how a starred cohort is labeled.

## Options considered

Use KDE's ADLF Current_Year_Graduates file to decide that a cohort exists. Use the cohort count already in the transition file. Leave a starred cohort count unclassified.

The graduates-file option would need a new raw file. That option was the other side of the question Scott answered. It is not what he chose.

## Decision

27 November 2026. Use the in-file cohort count for #44.

Scott's words (exact): "Use the in-file cohort count for #44"

"No graduates file and no mirror addition" is Gremlin's addition. It is not Scott's sentence. Gremlin's question set the in-file count against a separate graduates file that would need a mirror addition. The scope that follows from Scott's choice is that this change does not register a graduates file and does not add a mirror file. Those words stay labeled as Gremlin's addition.

27 November 2026. Leave rows with a starred cohort count unclassified and label them kde-recheck.

Scott's words (exact): "Leave rows with a starred cohort count unclassified and label them kde-recheck"

A `*` measure cell is stored as NA with `suppressed` Y when the same row's `total_in_group` is a published number greater than 0. The star is not stored when that cohort count is 0 or blank. A published 0 is kept as 0 with `suppressed` N. A blank measure cell is omitted. `**` and `<10` are not `*` and stay omitted.

A row whose cohort count is `*` keeps the prior behavior for that row: no new suppressed row. Each such row is listed in [notes/flags/A-flagged-keys.csv](../../notes/flags/A-flagged-keys.csv) with reason `starred cohort count` and label `kde-recheck`.

The 7 KYRC26 rows where private = total − public leaks a suppressed value are in that same list, reason `private = total - public leaks suppressed value`, label `kde-recheck`. They are not recomputed and the published public and total cells are not suppressed. [0054](0054-store-values-as-kde-publishes.md): "Store values as KDE publishes them. Do not add a new metric KDE does not publish."

[Issue #44](https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/44) states that 92 KYRC24/26 groups absent from the transition file have 3 to 42 graduates in the graduates file. That figure is `kde-recheck`. The graduates file is not fetched. That omission is the scope that follows from the in-file choice, and the "no graduates file" wording is Gremlin's addition.

Flag storage, for this list, follows Scott's delegation. Scott's words (exact): "go with your recommendations for each, and only bring me ones where you are unsure". The recommendation approved, Gremlin's words: "the committed list plus the caveat for now… we can switch to a real column later." The list is the committed CSV named above. The caveat is in `KPIS.md`. No KPI column is added.

## Who decided and how we know

Scott, chat answer in Gremlin's chat, 27 Nov 2026, 12:23 PM ET, verified by Archivist. Repo comment: none.

Scott's words (exact): "Use the in-file cohort count for #44"

Scott, chat answer in Gremlin's chat, 27 Nov 2026, 12:47 PM ET, verified by Archivist. Repo comment: none.

Scott's words (exact): "Leave rows with a starred cohort count unclassified and label them kde-recheck"

Flag storage source: "Scott delegated to Gremlin's recommendation". Scott's words (exact): "go with your recommendations for each, and only bring me ones where you are unsure". Gremlin's recommendation, the thing approved: "the committed list plus the caveat for now… we can switch to a real column later."

The 18 October 2026 rule this record applies is [0004](0004-postsecondary-star-cells.md), from https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/44#issuecomment-6251989485

## Why

Scott chose the in-file cohort count, and chose to leave a starred cohort unclassified. [0004](0004-postsecondary-star-cells.md) already required a cohort test and the `kde-recheck` label for ambiguous cases. [0054](0054-store-values-as-kde-publishes.md) keeps the published public and total cells and does not add the subtracted private count as a metric.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/44
- [0004](0004-postsecondary-star-cells.md)
- [0001](0001-suppression-rule.md)
- [0054](0054-store-values-as-kde-publishes.md)
- `etl/postsecondary_enrollment.py`
- `notes/flags/A-flagged-keys.csv`
- `notes/120--postsecondary-enrollment-star-cells.md`
- `KPIS.md` (Postsecondary Enrollment)

## Code check

The module matches this choice. A `*` measure is NA with `suppressed` Y only when `total_in_group` is a published number greater than 0. A starred cohort emits no new row and is listed in `notes/flags/A-flagged-keys.csv`. The 7 KYRC26 leak rows are in that list and are not recomputed. No graduates file is registered. `config/kde_sources.yaml` is unchanged. [0004](0004-postsecondary-star-cells.md) stays `accepted, code differs (follow-up open)` until that status is updated after merge.

## KDE guidance check

Verdict: possible conflict. Flag only.

CHG-2022 defines `*` as suppression, not as padding. Quote in [0001](0001-suppression-rule.md): "Data also will be suppressed in situations where low cell counts or percentages could lead to identification of individual student performance." https://www.education.ky.gov/districts/tech/sis/Documents/SRC_ReportingChanges_2022.pdf

Not found: a KDE statement that a starred postsecondary cohort means no students. Calling a 0 or blank cohort padding, and leaving a starred cohort unclassified, is the pipeline's reading.
