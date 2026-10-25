# 0039 Drop the calculated CTE pathway completion rate

Status: accepted

## Question

The pipeline had stored `cte_pathway_completion_rate` as completers divided by concentrators. KDE does not publish that ratio. Keep it or drop it?

## What KDE changed

KDE publishes completer counts and concentrator counts. Completers are four or more credits. Concentrators are at least two and fewer than four credits. Those definitions are from the pipeline note. The TEDS glossary was not re-fetched. KDE does not publish completers divided by concentrators.

## Options considered

Keep the calculated rate under an existing-practice exception, or drop it and add no alias.

## Decision

20 October 2026. Drop `cte_pathway_completion_rate`. The 19 October 2026 existing-practice exception is withdrawn. Do not restore the metric and do not add an alias. Completer counts, concentrator counts, enrollment, and `num_cte_pathways` stay.

## Who decided and how we know

`notes/104--kyrc26-file-to-etl-mapping.md`, "Owner decision, Scott Horn, 2026-10-20. Drop `cte_pathway_completion_rate`."

## Why

Quoted from that note: the pipeline calculated completers divided by concentrators, times 100, rounded to one decimal. KDE never published that figure. Completers and concentrators are separate groups, so the ratio is not a share of concentrators and can exceed 100%.

The run before the removal wrote 17,892 rows, of which 3,300 were the rate (188 a published number, 3,112 suppressed Y). The run after writes 14,592 rows.

## Links

- `notes/104--kyrc26-file-to-etl-mapping.md` (pathway section, including the line cited above)
- The 19 October rule, and this withdrawal of its existing-calculation exception: [0054](0054-store-values-as-kde-publishes.md)
- Related, not this decision: issue #86, state pathway cells are not the sum of districts, labeled `kde-recheck`. That issue does not change storage.

## Code check

Matches the drop recorded in the note. This pass did not re-run the pathway module.

## KDE guidance check

Verdict: consistent, after the drop. Flag only in the sense that it is a check, not a change.

The rate was a pipeline calculation. Dropping it leaves the published counts. The TEDS glossary definitions of concentrator and completer were not re-fetched. https://www.education.ky.gov/CTE/teds/Documents/TEDS_Glossary.pdf
