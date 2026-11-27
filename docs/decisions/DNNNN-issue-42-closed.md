# DNNNN Issue #42 is closed; the NaN guard and the post-2025 analysis stay out of scope

Status: accepted

Decision source: Scott, chat answer in Gremlin's chat, 27 Nov 2026, 12:34 PM ET, verified by Archivist. Two answers, items 1 and 12 in DECISIONS-VERIFIED.md. Repo comment: none.

The closure of issue #42 rests on item 1, not on item 12.

## Question

Record 0002 says "Issue #42 stays open." Does that sentence still stand, and do the NaN guard and the post-2025 analysis stay out of scope?

## What changed

Nothing in a KDE file. Issue #42 is the report-script scope recorded in [0002](0002-report-scripts-out-of-scope.md).

On 27 Nov 2026 the issue's GitHub state, read for this record, is closed. `closed_at` is `2026-11-27T17:36:10Z` (12:36 PM ET). Comment 7224584217, created `2026-11-27T17:36:00Z`, says: "The state-row fix landed in #115. The NaN guard and post-2025 analysis are out of scope under record 0002, confirmed by Scott on November 27, 2026. Closing."

That comment is the closure note Archivist lists. It is not a "Decision (Scott…)" comment, and it is not Scott's words. The decision source stays the chat answers. Repo comment: none.

## Options considered

Leave 0002's sentence "Issue #42 stays open" as the current statement. Record that #42 is closed and that the NaN guard and the post-2025 analysis stay out of scope, and mark that one sentence superseded in part.

## Decision

27 November 2026, 12:34 PM ET.

Item 1, Scott's words: "Close #47, #46 and #42 with notes, and comment on #49, #108 and #111."

The closing of #42 rests on that answer. This record does not close #47 or #46, and it does not comment on #49, #108, or #111. Those were already posted. Archivist: comments 7224583801 (#47), 7224583937 (#46), 7224584217 (#42), 7224584462 (#49), 7224584646 (#108), and 7224584930 (#111), all at 12:36 PM ET. This record cites 7224584217. It does not post another comment.

Item 12, Scott's words: "Confirm #42's NaN guard and post-2025 analysis stay out of scope under 0002."

The NaN guard and extending the analysis past 2025 stay out of scope under [0002](0002-report-scripts-out-of-scope.md). Item 12 does not close the issue. DECISIONS-VERIFIED.md says the option's label said "close #42" and its value did not.

[0002](0002-report-scripts-out-of-scope.md) is `superseded (in part)`, limited to the sentence "Issue #42 stays open." The out-of-scope rule, the 25 October narrow exception for pull request #115, and the limit on the #61 ID normalization still stand.

## Who decided and how we know

Scott, chat answer in Gremlin's chat, 27 Nov 2026, 12:34 PM ET, verified by Archivist. The quotes are the values of the card options he picked, as DECISIONS-VERIFIED.md items 1 and 12 give them. Repo comment: none.

The closure note on the issue is https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/42#issuecomment-7224584217. Archivist checked that it was posted at 12:36 PM ET. This pass read the same comment from the GitHub API.

## Why

Scott's two sentences are the choice. Reason the report scripts stay out of scope: not restated here. [0002](0002-report-scripts-out-of-scope.md) says reason not documented.

## Links

- [0002](0002-report-scripts-out-of-scope.md)
- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/42
- Comment https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/42#issuecomment-7224584217
- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/115
- `notes/120--kyrc26-twc-refresh-and-caveat-check.md`

## Code check

No script is edited. `scripts/generate_report_drafts.py` stays as pull request #115 left it. The NaN guard is not added. The analysis year cap is not moved. That matches item 12 and the part of [0002](0002-report-scripts-out-of-scope.md) that still stands.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

No KDE page was fetched for this record. The question is the scope of this repository's report scripts, not a published measure.
