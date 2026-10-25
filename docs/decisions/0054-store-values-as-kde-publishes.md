# 0054 Store values as KDE publishes them; no new pipeline calculations (Scott, 2026-10-19)

Status: accepted

## Question

When a metric is new, or when an old calculation no longer matches a published figure, does the pipeline compute a value KDE did not publish?

## What KDE changed

Nothing in this record is a KDE layout change. The rule is about what this pipeline stores.

## Options considered

Add a metric the files do not contain, keep calculating a figure KDE stopped publishing, or store the published value and add no new calculation.

## Decision

19 October 2026. Store values as KDE publishes them. Do not add a new metric KDE does not publish. Existing calculations stay unless the method is flawed. A figure KDE used to publish and later stopped, such as an average, may be calculated only when the same method run on the earlier years reproduces KDE's published figures. A methodological problem is reported rather than changed in place.

`notes/104--kyrc26-file-to-etl-mapping.md` line 230: "Existing calculations stay unless the method is flawed. The rule that values stay as KDE publishes them applies to new metrics: do not add a metric KDE does not publish. A figure KDE used to publish and later stopped, such as an average, may be calculated only when the same method run on the earlier years reproduces KDE's published figures."

The same note repeats the rule as "Scott's rule (2026-10-19)":

- Line 2361, safe-schools discipline: "it guides new metrics and new design choices. Existing calculations stay unless there is a real methodological problem, and a problem is reported rather than changed."
- Line 2417, the events-agent lesson: "Scott's published-value rule guides new metrics and new design choices. Keep an existing calculation unless you find a methodological problem, and then report it instead of changing it."
- Line 2433, safe-schools events: the same 2026-10-19 sentence. `safe_incident_rate_*` is an existing calculation (events divided by students affected). KDE does not publish it. It is kept.

20 October 2026. Scott Horn withdrew the existing-practice exception for `cte_pathway_completion_rate`. The pipeline had calculated completers divided by concentrators. KDE never published that ratio. [0039](0039-drop-pathway-completion-rate.md). The same day's drop of `has_dual_credit_program` is [0040](0040-drop-has-dual-credit-program.md). Line 230 records both drops in the paragraph that states the rule.

## Who decided and how we know

`notes/104--kyrc26-file-to-etl-mapping.md`. The 19 October lines name it "Scott's rule (2026-10-19)". The 20 October withdrawal is in the same note: "Scott Horn dropped `cte_pathway_completion_rate` on 2026-10-20."

## Why

The note states the rule. For the pathway rate, the same paragraph says why the exception was withdrawn: completers and concentrators are separate groups, so the ratio is not a share and can exceed 100%. Reason for the 19 October rule beyond those sentences: not documented.

## Links

- `notes/104--kyrc26-file-to-etl-mapping.md` lines 230, 2361, 2417, and 2433
- [0007](0007-drop-calculated-cte-rates.md)
- [0027](0027-signed-title-i-equity-gap.md)
- [0037](0037-kindergarten-published-percents.md)
- [0039](0039-drop-pathway-completion-rate.md)
- [0040](0040-drop-has-dual-credit-program.md)
- [0041](0041-drop-calculated-climate-indexes.md)

## Code check

This record states the rule. It does not audit every metric. The linked records say where the current code matches. No pipeline code is changed here.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE statement that this pipeline must not calculate a figure KDE does not publish.
