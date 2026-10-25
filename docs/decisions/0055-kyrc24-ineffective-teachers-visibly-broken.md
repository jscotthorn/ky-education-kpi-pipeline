# 0055 KYRC24 ineffective-teachers live copy is visibly broken; #64 item 1 falls under 0052's exception (Scott, 2026-10-25 1:07 PM ET)

Status: accepted

Primary source: https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/64#issuecomment-6400597552

## Question

Does the live public KYRC24 ineffective-teachers file still count as visibly broken, so that #64 item 1 stays on the test-site copy under [0052](0052-republish-supersedes-earlier.md)?

## What KDE changed

Nothing in this record is a new KDE notice. The recheck compared two copies that were already on KDE.

## Options considered

Take the latest public datasets copy, or keep the 18 October test-site copy because the live copy is visibly broken.

## Decision

25 October 2026, 1:07 PM ET. The KYRC24 copy chosen under issue #64 item 1 stands under the republish rule. The live public copy is visibly broken, so the switch to the test-site copy stays.

Primary source: https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/64#issuecomment-6400597552 "Decision (Scott, 2026-10-25 1:07 PM ET): the KYRC24 copy chosen under item 1 stands under the republish rule (0052)."

The comment states the rule it applies: use the latest public KDE file after the review window closes, unless that copy is visibly broken. Scott confirmed that the live public KYRC24 copy counts as visibly broken.

Background, source type chat answer. Scott's typed answer in Moon's chat at 1:07 PM ET, verified verbatim by Archivist: "recheck that that is still the case on the kde site now. If so, yep it and note in our decision log why". The recheck was done by Moon and is recorded in the comment above.

The comment's recheck, 1:07 PM ET on 25 October 2026:

| Copy | Last-Modified | Rows (excluding header) | md5 | Notes |
| --- | --- | --- | --- | --- |
| `datasets/KYRC24_OVW_Students_Taught_by_Ineffective_Teachers.csv` (live public) | Thu, 14 May 2026 19:11:30 GMT | 39 | `d0499b27517699100bd3f46fd37f2610` | State and district summary rows only. 119 cells are above 100 (max 1016.0), which is impossible for a percentage. |
| `datasetsuat6051276/KYRC24_OVW_Students_Taught_by_Ineffective_Teachers.csv` (UAT) | Mon, 12 Oct 2026 19:55:55 GMT | 1,589 | `d26e9d2547bb2c3105d77a3dd55f7458` (matches the published Content-MD5) | Same share layout as 2023 and 2025. |

If KDE publishes a corrected live copy, compare it under 0052 and record the differences. This record does not cover #64 item 2 (the KYRC25 test-site copy).

## Who decided and how we know

Primary source: https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/64#issuecomment-6400597552 posted on the `jscotthorn` account through the Cursor GitHub App. The comment is quoted in the decision.

Background, source type chat answer: the typed sentence above, from Moon's chat at 1:07 PM ET. Archivist checked that quote verbatim against the transcript. The recheck in the comment is Moon's.

## Why

The comment gives the reason: the live file is state and district summary rows only, and 119 cells are above 100 (max 1016.0), which is impossible for a percentage. Scott's typed sentence is the instruction to recheck and, if that was still true, to record why.

## Links

- [0052](0052-republish-supersedes-earlier.md)
- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/64
- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/123
- The 18 October copy choice: [0025](0025-ineffective-teachers-copy-and-zeros.md)
- [kde-republishes.md](kde-republishes.md) row R6

## Code check

`config/kde_sources.yaml` does not conform yet. `students_taught_by_ineffective_teachers` still registers the bare historical name `KYRC24_OVW_Students_Taught_by_Ineffective_Teachers.csv`. The yaml comment says the datasets object is 39 rows with percents above 100 and "It is not swapped." The 1,589-row test-site copy is not the registered file. Issue #123 tracks that change. Not fixed here. [0025](0025-ineffective-teachers-copy-and-zeros.md) stays `conflicts-with-code` for that registration.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE notice that names the live KYRC24 ineffective-teachers file as broken, or that names the test-site copy as the replacement.
