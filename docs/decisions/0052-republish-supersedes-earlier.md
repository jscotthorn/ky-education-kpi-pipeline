# 0052 Use the latest public KDE file after the review window, unless it is broken

Status: accepted

Primary source: https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/120#issuecomment-6400284741

Background, source type chat answer: the 12:11 PM ET option he selected, and his 12:02 PM ET typed words, both quoted below. Bot: ECC manager bot. Archivist checked both verbatim against the transcript.

## Question

When KDE publishes the same school year more than once, which file should the pipeline keep?

## What KDE changed

KDE republishes files without a public change log. The cases this repo has hit, and the differences recorded so far, are in [kde-republishes.md](kde-republishes.md). Recording those differences is now required for each republish. That table is the place.

No KDE page found in the 25 October 2026 research says a later file always replaces an earlier one.

## Options considered

- Provisional, 12:02 PM the same day: a republish should always supersede an earlier publish. Superseded by the decision below. Not the rule.
- The rule Scott then set: latest public file after the review window closes, unless that copy is visibly broken, and record the differences.
- Keep choosing every file with no shared rule.

## Decision

25 October 2026, 12:11 PM ET. Use the latest public KDE file after the review window closes, unless it is visibly broken. Record the differences on each republish.

Primary source: https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/120#issuecomment-6400284741 "Decision (Scott, 2026-10-25 12:11 PM ET): Record 0052. Use the latest public KDE file after the review window closes, unless it's visibly broken; record differences on each republish."

Background, source type chat answer. The option he selected: "Use the latest public KDE file after the review window closes, unless it's visibly broken; record differences on each republish."

History, same day, superseded by that option. At 12:02 PM ET his typed words were: "Several issues arose during the work showing that a republish had better information, so it seems a republish should always supercede an earlier publish. But, I want that from the KDE if available." The spelling "supercede" is his. That provisional "always" rule is not the rule. The 12:11 option is.

The 12:11 option, and Decision comment 6400284741, do not name pull request #63 or issue #64. On 25 October 2026 at 1:07 PM ET Scott confirmed that the live KYRC24 ineffective-teachers copy is visibly broken, so #64 item 1 stays on the test-site copy under this exception. [0055](0055-kyrc24-ineffective-teachers-visibly-broken.md). The 18 October choice is [0025](0025-ineffective-teachers-copy-and-zeros.md). #64 item 2, the KYRC25 test-site copy, is not named in that confirmation. Applying the exception to item 2 remains a reading of this rule that Scott hasn't confirmed.

Pull request #113 and pull request #116 adopted the May 2026 KYRC24 CTE file. Checked against this rule: `config/kde_sources.yaml` records that datasets copy as Last-Modified 14 May 2026, after the May 2025 historical copy. This pass did not re-fetch a KDE date for the close of the 2023-24 review window. 14 May 2026 is after the fall 2024 release and after the May 2025 historical file. The yaml comment and `notes/116` say no published number changed. Stars became 0.0, blank, `--`, or `N/A`. That is not the 39-row pattern. If those two points hold, the May 2026 adoption complies. The separate 0.0 warnings stay in [0009](0009-cte-zero-rate-beside-blank-count.md) and [0051](0051-cte-zero-rate-on-positive-count.md). A later public republish of 2022-23 or 2023-24 would be read under this rule, and the differences would be recorded in [kde-republishes.md](kde-republishes.md).

## Who decided and how we know

Primary source: https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/120#issuecomment-6400284741 posted on the `jscotthorn` account through the Cursor GitHub App. The comment is quoted in the decision.

Background, source type chat answer. The 12:11 PM ET sentence is the option he selected on a question card in the ECC manager bot's chat. It is not text he typed. The 12:02 sentence is his typed words, nine minutes earlier, and is the history above. Archivist checked both against the transcript.

## Why

The 12:11 option is the rule. The 12:02 typed words give the reason he had reached for an "always" rule: republishes met during the work had better information. He also said he wanted that from KDE if available. The adopted rule is the narrower one. KDE has not stated it. See the guidance check.

## Links

- Primary source https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/120#issuecomment-6400284741
- [kde-republishes.md](kde-republishes.md)
- [republish-diffs/kyrc25-2026-rewrites.md](republish-diffs/kyrc25-2026-rewrites.md)
- [0055](0055-kyrc24-ineffective-teachers-visibly-broken.md)
- [0008](0008-kyrc24-cte-may-2026-republish.md)
- [0009](0009-cte-zero-rate-beside-blank-count.md)
- [0051](0051-cte-zero-rate-on-positive-count.md)
- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/113
- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/116
- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/63 and issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/64
- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/123
- [0024](0024-novice-kyrc24-equity-copy.md), [0025](0025-ineffective-teachers-copy-and-zeros.md), [0026](0026-out-of-field-file-and-copy.md)

## Code check

No shared "take the latest public file" step was added. Registrations stay explicit. The May 2026 CTE registration matches the rule on the facts above (kde-republishes R1), if the review-window point holds.

R15 is not yet conformed to the rule. It is not treated as exempt. R19 and R20 comply. The differences are in [republish-diffs/kyrc25-2026-rewrites.md](republish-diffs/kyrc25-2026-rewrites.md) (pull request #124). R6 applies the visibly-broken exception, confirmed by [0055](0055-kyrc24-ineffective-teachers-visibly-broken.md). The config change for R6 is not done. R7 and R9 are not marked as conflicts.

- R15. Many KYRC24 files still use the older Open House copy while later same-name datasets copies exist. Config comments say "not swapped." [Row R15](kde-republishes.md#r15). Issue #123 covers the novice, ineffective, and out-of-field files in that row (#58, #64, and #66). It does not cover the other files in the row.
- R6. The exception applies, confirmed by [0055](0055-kyrc24-ineffective-teachers-visibly-broken.md). The latest public copy is the live datasets file (39 rows, 119 cells above 100). 0055 records that copy as visibly broken, so this rule picks #64 item 1's test-site copy (1,589 rows). Config still registers the older Open House file. Issue #123 tracks that change. Not yet conformed.
- R7. 0052 picks the latest public copy, the live datasets file, which is what config registers. #64 item 2 chose the test-site copy. Applying the visibly-broken exception so that copy wins is a reading Scott hasn't confirmed. Not marked as a conflict.
- R9. Not covered by #63 or #64. 0052 picks the latest public copy, the live datasets file, which is what config registers. The test-site copy is not the public file. Not marked as a conflict.
- R19. Complies. `KYRC25_ACCT_Kentucky_Summative_Assessment.csv` is registered as `url: kyrc25`. The 14 May datasets copy is the latest public copy and is not visibly broken. Differences: [republish-diffs/kyrc25-2026-rewrites.md](republish-diffs/kyrc25-2026-rewrites.md).
- R20. Complies for postsecondary readiness, 4-year graduation, and 5-year graduation. All three are registered on `url: kyrc25`. Postsecondary readiness and 4-year graduation are compared in that note. The 5-year copies are byte-identical, md5 `ddcd0df0de09e52291c98a78353b03ac`, the Data Engineer's finding recorded in issue #125.

The comparison is pull request #124, in [republish-diffs/kyrc25-2026-rewrites.md](republish-diffs/kyrc25-2026-rewrites.md). This record does not run it. Issue #123 tracks the Open House registrations that still do not apply the 18 October copy choices on #58, #64, and #66: https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/123. Those three conflicts stay on [0024](0024-novice-kyrc24-equity-copy.md), [0025](0025-ineffective-teachers-copy-and-zeros.md), and [0026](0026-out-of-field-file-and-copy.md). This record does not swap a file.

## KDE guidance check

Verdict: no KDE statement that a later file supersedes an earlier one. Possible conflict with wording that calls the first release final. Flag only. The rule is unchanged. These pages were not re-fetched in this pass. Citations are from the 25 October 2026 research.

- HB 500 FAQ, and the HB 6 FAQ with the same sentence: "Due to quality control checks and the regulatory review period, there could be multiple releases of data." The "most current, accurate data" sentence is scoped to reading and math percentages on district websites. https://www.education.ky.gov/AA/Acct/Documents/HB500_ReadingMathPerformance_WebsiteRequirements.pdf and https://www.education.ky.gov/AA/Acct/Documents/HB6_ReadingMathPerformance_WebsiteRequirements.pdf
- 703 KAR 5:240 §7: a data-review request is due within ten days after KDE "officially releases the final accountability classifications." https://apps.legislature.ky.gov/law/kar/titles/703/005/240/
- DAC email, 2 February 2026: "Final Data Files After 10-Day Regulatory Data Review," and "updated datasets" on 3 February. https://content.govdelivery.com/accounts/KYDE/bulletins/4075ddd
- Appeals, twice-weekly refreshes, and phased finance releases: KAR §7(2)–(4); commissioner's message 23 October 2025 (https://content.govdelivery.com/accounts/KYDE/bulletins/3f857fd); finance advisory 10 July 2026 (https://content.govdelivery.com/accounts/KYDE/bulletins/41e69fb).
- Wording that cuts the other way: the regulation's "final accountability classifications" at the first release, and the 10 December 2024 commissioner's message that assessment and accountability data is "already final," with reload differences "due to rounding and the way small cell counts are suppressed." https://content.govdelivery.com/accounts/KYDE/bulletins/3c68ec8
- Not found: version numbers or change logs on public SRC files, a statement of which host is official, or a KDE process for correcting a prior school year.
