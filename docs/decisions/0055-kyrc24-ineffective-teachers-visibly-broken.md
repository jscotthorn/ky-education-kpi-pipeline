# 0055 KYRC24 ineffective-teachers live copy is visibly broken; #64 item 1 falls under 0052's exception (Scott, 2026-10-25 1:07 PM ET)

Status: accepted

Decision source: Scott's typed instruction in Moon's chat at 1:07 PM ET, quoted below. Archivist checked that quote verbatim against the transcript.

Moon's recheck record, not the decision source: https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/64#issuecomment-6400597552

## Question

Does the live public KYRC24 ineffective-teachers file still count as visibly broken, so that #64 item 1 stays on the test-site copy under [0052](0052-republish-supersedes-earlier.md)?

## What KDE changed

Nothing in this record is a new KDE notice. The recheck compared two copies that were already on KDE.

## Options considered

Take the latest public datasets copy, or keep the 18 October test-site copy because the live copy is visibly broken.

## Decision

25 October 2026, 1:07 PM ET. The KYRC24 copy chosen under issue #64 item 1 stands under the republish rule. The live public copy is visibly broken, so the switch to the test-site copy stays.

Decision source: Scott's typed instruction in Moon's chat at 1:07 PM ET, verified verbatim by Archivist: "recheck that that is still the case on the kde site now. If so, yep it and note in our decision log why". The instruction is conditional. The recheck condition was met under the corrected figures too, so the status stays accepted.

Moon's recheck record: https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/64#issuecomment-6400597552. The text and the recheck in that comment are Moon's. Moon posted it under Scott's account at 1:08 PM ET, acting on the 1:07 instruction. Scott did not review the comment's text. A corrected #64 comment may follow.

The comment's figures were wrong, and the Data Engineer's recount on pull request #126 corrected them. The comment said the live file is state and district summary rows only, and that 119 cells are above 100 (max 1016.0). Those 119 cells count ID columns. District Number, School Code, CO-OP Code, County, and School Number make up 87 of the 119, and the count skips 2,250.0. In the nine percentage columns, 34 of 351 cells are above 100, max 2,250.0. The 39 rows are 2 state, 14 district, and 23 school rows. The chosen UAT copy has 4 cells above 100 (max 112.0). The live copy is still visibly broken.

The corrected recheck:

| Copy | Last-Modified | Rows (excluding header) | md5 | Notes |
| --- | --- | --- | --- | --- |
| `datasets/KYRC24_OVW_Students_Taught_by_Ineffective_Teachers.csv` (live public) | Thu, 14 May 2026 19:11:30 GMT | 39 | `d0499b27517699100bd3f46fd37f2610` | 2 state, 14 district, and 23 school rows. In the nine percentage columns, 34 of 351 cells are above 100 (max 2,250.0). Comment 6400597552's figures (119 cells, max 1016.0, "state and district summary rows only") were wrong. The Data Engineer's recount corrected them. |
| `datasetsuat6051276/KYRC24_OVW_Students_Taught_by_Ineffective_Teachers.csv` (UAT) | Mon, 12 Oct 2026 19:55:55 GMT | 1,589 | `d26e9d2547bb2c3105d77a3dd55f7458` (matches the published Content-MD5) | The chosen copy. 4 cells are above 100 (max 112.0). The comment says the share layout matches 2023 and 2025. |

If KDE publishes a corrected live copy, compare it under 0052 and record the differences. This record does not cover #64 item 2 (the KYRC25 test-site copy).

## Who decided and how we know

Decision source: the 1:07 PM ET sentence in Moon's chat, quoted in the decision. Archivist checked that quote verbatim against the transcript. That instruction is the decision. The status stays accepted because the recheck condition was met under the corrected figures too.

Comment 6400597552 is Moon's recheck record, not the decision source. The text and the recheck are Moon's. Moon posted them under Scott's account at 1:08 PM ET, acting on the 1:07 instruction. Scott did not review the comment's text.

## Why

Scott's instruction was to recheck the live file and, if it was still visibly broken, record why. The Data Engineer's recount is why the condition is met: in the nine percentage columns, 34 of 351 cells are above 100 (max 2,250.0), which is impossible for a percentage. The 39 rows are 2 state, 14 district, and 23 school rows. The chosen UAT copy has 4 cells above 100 (max 112.0). Comment 6400597552's figures (119 cells, max 1016.0) were wrong, and the Data Engineer's recount corrected them. Those 119 cells count ID columns. District Number, School Code, CO-OP Code, County, and School Number make up 87 of the 119, and the count skips 2,250.0. The comment's "state and district summary rows only" is also wrong: the file has 23 school rows.

## Links

- [0052](0052-republish-supersedes-earlier.md)
- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/64
- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/123
- The 18 October copy choice: [0025](0025-ineffective-teachers-copy-and-zeros.md)
- [kde-republishes.md](kde-republishes.md) row R6
- Data Engineer's recount: pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/126#issuecomment-6400743689

## Code check

`config/kde_sources.yaml` does not conform yet. `students_taught_by_ineffective_teachers` still registers the bare historical name `KYRC24_OVW_Students_Taught_by_Ineffective_Teachers.csv`. The yaml comment says the datasets object is 39 rows with percents above 100 and "It is not swapped." The 1,589-row test-site copy is not the registered file. Issue #123 tracks that change. Not fixed here. [0025](0025-ineffective-teachers-copy-and-zeros.md) stays `conflicts-with-code` for that registration.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE notice that names the live KYRC24 ineffective-teachers file as broken, or that names the test-site copy as the replacement.
