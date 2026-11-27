# DNNNN Store the published Title I gap and flag the absolute-value years

Status: accepted

This record is DNNNN. DNNNN is a placeholder. The number is assigned at merge. Repo comment: none.

## Question

Should the pipeline compute Title I minus Not Title I, or store the gap KDE published, and what happens to a negative cell?

## Decision

Scott, chat answer in Gremlin's chat, 27 Nov 2026, 12:26 PM ET, verified by Archivist. Repo comment: none.

Scott's words: "Flag both the negative and paired Title I cells".

"Keep" is the label of the option he picked ("Keep and flag both cells"). What "flag" means is settled by the 12:47 PM delegation below.

Scott, chat answer in Gremlin's chat, 27 Nov 2026, 12:27 PM ET, verified by Archivist. Repo comment: none.

Scott's words: "Store the published gap, stop dropping negatives, and flag KYRC24/25 as absolute values (0054 wins)".

Folding the #59 choice into this batch is Data Engineer's plan, not Scott's decision.

Scott delegated to Gremlin's recommendation, 27 Nov 2026, 12:47 PM ET, verified by Archivist. Repo comment: none.

Scott's words: "go with your recommendations for each, and only bring me ones where you are unsure".

Gremlin's recommendation, the thing approved: "the committed list plus the caveat for now… we can switch to a real column later."

## What the code does

No new metric is computed. Each published gap cell is stored. A blank is omitted. A published 0 stays 0 with `suppressed` N. A published negative stays negative with `suppressed` N.

`StudentsTaughtByIneffectiveTeachersETL.extract_metrics` no longer skips `value < 0`. The novice equity path no longer requires `value >= 0`. The institutional novice rates at `percent_less_than_1` and `percent_1_to_3` still require `value >= 0`. Out-of-field already stored a negative cell. That path is unchanged.

[0027](0027-signed-title-i-equity-gap.md) is superseded in part. Its "compute Title I minus Not Title I" part does not stand. Its "stop dropping negatives" part does. [0054](0054-store-values-as-kde-publishes.md) is unchanged.

## Flags

The list is [notes/flags/B-flagged-keys.csv](../../notes/flags/B-flagged-keys.csv). The label on every row is `kde-recheck`. The reasons are `absolute-value gap year`, `negative complement cell`, and `paired complement cell`.

KYRC24 and KYRC25 novice equity and out-of-field are the absolute-value years. 6,408 gap cells are listed. On the rows where Title I and Not Title I differ, most equal `|Title I - Not Title I|`. The rest are off by at most 0.1: novice 124 in 2024 and 139 in 2025, out-of-field 40 in 2024 and 52 in 2025. They stay in the year. No ineffective year matches on every differing row, so none is listed under that reason. KYRC24, KYRC25, and KYRC26 ineffective files have no Equity Gap row.

The negative Title I complement cells are 30 in 2022 and 10 in 2023, all in ineffective. Each has a positive partner. Rector A. Jones Middle School `035020` in 2022 is one of them: Non-White 5.65 and White -5.40 (stored -5.4). The complement subtraction holds within 0.01 on 39 of those 40 cells. Harlan County district `235` in 2023, Non-Economically Disadvantaged, is off by 0.29.

The 2021-2023 ineffective equity-gap negatives are not on the flag list. KDE publishes those years the opposite way round, and the KPIS note covers that.

`UAT-negated gap` has no rows. The two files loaded from `kyrc_uat` are the KYRC24 and KYRC25 ineffective copies. Neither they nor the datasets KYRC24 and KYRC25 ineffective objects have an Equity Gap row. Out-of-field KYRC24 loads the datasets copy. UAT negates some of its gap cells, and that object is not loaded.

## Restored negatives

Issue #59 counted 804 novice equity negatives: 184 in 2020, 188 in 2021, 155 in 2022, 176 in 2023, and 101 in 2026. This run restores those 804. The KYRC24 copy switch does not change the count. KYRC24 publishes the absolute value and has no negative gap cell. The 2026 count is the pre-refresh UAT object. The other 4 Nov KYRC26 files are not adopted here.

Ineffective restores 466 cells. The equity-gap negatives are 5 in 2020, 126 in 2021, 160 in 2022, and 135 in 2023. The issue #64 complement cells are the other 40 (30 in 2022 and 10 in 2023). Out-of-field already stored its negatives: 109 in 2021, 80 in 2022, 118 in 2023, and 95 in 2026. None are new. The 2019-20 out-of-field file stays unregistered, so the 32 negatives #59's comment counted for that file are not in this output.

A cell that was already stored is unchanged. The added rows are the negatives above, all `suppressed` N.

Issue #59 counted 157 wrong-sign novice gap cells in 2024-25 (36 on the replaced historical KYRC24 copy plus 121 in KYRC25). On the registered copies the count is 212 (91 in KYRC24 datasets, 121 in KYRC25); out-of-field has 122 and 154. They are stored as published and are inside the flagged absolute-value years; none is flipped.

## Code check

`etl/novice_teachers.py` stores an equity value that is present, including a negative. The two institutional checks still use `value >= 0`. `etl/students_taught_by_ineffective_teachers.py` stores a parsed percent, including a negative. `etl/students_taught_by_out_of_field_teachers.py` is unchanged and already stored a negative. No KPI column was added.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE notice that says to replace the published gap with Title I minus Not Title I, or that says a negative complement cell should be dropped.
