# 0007 Do not store calculated CTE rates for 2024-25 or 2025-26

Status: accepted

Replaces the compute clause of [0006](0006-cte-compute-rates-2026-10-18.md).

## Question

After the 18 October instruction to compute participation and completion rates, should 2025 and 2026 still get a calculated rate?

## What KDE changed

KYRC25 and KYRC26 publish counts only: `All High School CTE Participants`, `All High School Enrollments`, `12th Grade Completers`, `12th Grade Participants`. `notes/106--cte-participation.md` state row for 2026: participants 144,661, enrollment 201,255, completers 19,990, grade-12 participants 44,038. KYRC24 publishes the percents. No KDE note was found that announces the header change or says whether the website still shows a rate.

## Options considered

Compute the rates ([0006](0006-cte-compute-rates-2026-10-18.md)), or store only percents KDE printed.

## Decision

20 October 2026, in `notes/104--kyrc26-file-to-etl-mapping.md`.

Participation: do not store participants divided by `All High School Enrollments`. `cte_participation_rate` holds only the published KYRC23 and KYRC24 percent.

Completion: the gate failed. Do not store completers divided by grade-12 participants for 2025 or 2026. `cte_completion_rate_grade_12` holds only the published KYRC23 and KYRC24 percent. Count metrics stay.

`notes/106` row counts after the drop: 2025 and 2026 participation-rate rows 0, completion-rate rows 0.

## Who decided and how we know

`notes/104--kyrc26-file-to-etl-mapping.md`, sections "Follow-up, 20 October 2026 — drop the calculated 2025 and 2026 participation rate" and the completion-rate section under it. Both say "Scott's decision."

Second source, same choice. Scott, direct, 2026-10-25 11:24 AM ET, in his chat with the ECC manager bot (Moon); transcript checked by Archivist:

"I believe some of those gaps, like CTE participation/completion rates in 25-26, was due to changes in how KDE reported and our inability to generate them reliably and with fidelity to older years with the data at hand."

## Why

Participation, quoted from notes/104: "Participants are counted where the course is taught. Enrollment is counted where the student is enrolled. The quotient is not a participation rate, 90 of the stored values are above 100 (40 in 2025, 50 in 2026), and the 2024 base is KDE's published percent."

Completion: the gate treated `Grade 12 CTE Eligible Completer` as eligible completers. The cells are the completer count, so KYRC24 has no eligible-completer denominator. The notes list comparisons that do not match the published 42.3 percent. KYRC25 and KYRC26 have no eligible-completer column. The calculated completers / grade-12 participants rate was removed.

Scott's 25 October sentence, quoted above, is a second statement of why the 2025-26 rates are absent. The word "believe" is his. The 20 October notes give the file-level reasons. Both stand. Status stays accepted.

## Links

- `notes/104--kyrc26-file-to-etl-mapping.md` (those two follow-ups)
- `notes/106--cte-participation.md`
- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/106
- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/47
- [0006](0006-cte-compute-rates-2026-10-18.md)

## Code check

Matches. `etl/cte_participation.py` says KYRC25 and KYRC26 keep the counts and do not divide them. `notes/106` shows no 2025 or 2026 rate rows.

## KDE guidance check

Verdict: possible conflict. Flag only.

Not calculating a rate matches the count-only files and the rule against unpublished rates ([0039](0039-drop-pathway-completion-rate.md)). The flag: pull request #106's notes say 19,096 / 42,834 = 44.6% for 2025 and 19,990 / 44,038 = 45.4% for 2026, and the 18 October comment calls those "published state rates." If the report-card site still shows a completion rate, the rate is missing from the CSV, not from KDE. This pass did not open the script-rendered dashboard. Not found: a KDE text that says the rate is or is not still displayed.
