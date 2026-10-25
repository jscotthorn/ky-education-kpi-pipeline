# 0052 Use the latest public KDE file after the review window, unless it is broken

Status: accepted

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

Scott's words: "Use the latest public KDE file after the review window closes, unless it's visibly broken; record differences on each republish."

History, same day, superseded by that sentence. At 12:02 PM ET Scott had said: "Several issues arose during the work showing that a republish had better information, so it seems a republish should always supercede an earlier publish. But, I want that from the KDE if available." The spelling "supercede" is his. That provisional "always" rule is not the rule. The 12:11 sentence is.

A visibly broken copy, the example already in the repo: the live-site KYRC24 ineffective-teachers file is 39 rows with percents above 100. Scott chose the test-site copy (1,589 rows) on 18 October 2026. Issues #63 and #64. [0025](0025-ineffective-teachers-copy-and-zeros.md). The broken live copy is not taken just because it is later.

Pull request #113 and pull request #116 adopted the May 2026 KYRC24 CTE file. Checked against this rule: `config/kde_sources.yaml` records that datasets copy as Last-Modified 14 May 2026, after the May 2025 historical copy. This pass did not re-fetch a KDE date for the close of the 2023-24 review window. 14 May 2026 is after the fall 2024 release and after the May 2025 historical file. The yaml comment and `notes/116` say no published number changed. Stars became 0.0, blank, `--`, or `N/A`. That is not the 39-row pattern. If those two points hold, the May 2026 adoption complies. The separate 0.0 warnings stay in [0009](0009-cte-zero-rate-beside-blank-count.md) and [0051](0051-cte-zero-rate-on-positive-count.md). A later public republish of 2022-23 or 2023-24 would be read under this rule, and the differences would be recorded in [kde-republishes.md](kde-republishes.md).

## Who decided and how we know

Scott, direct, 2026-10-25 12:11 PM ET, his answer on a question card in the ECC manager bot's chat; transcript checked by Archivist.

The 12:02 sentence is the same kind of source, nine minutes earlier, and is the history above.

## Why

Scott's 12:11 sentence is the rule. The 12:02 sentence gives the reason he had reached for an "always" rule: republishes met during the work had better information. He also said he wanted that from KDE if available. The adopted rule is the narrower one. KDE has not stated it. See the guidance check.

## Links

- [kde-republishes.md](kde-republishes.md)
- [0008](0008-kyrc24-cte-may-2026-republish.md)
- [0009](0009-cte-zero-rate-beside-blank-count.md)
- [0051](0051-cte-zero-rate-on-positive-count.md)
- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/113
- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/116
- Issues https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/63 and https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/64
- [0024](0024-novice-kyrc24-equity-copy.md), [0025](0025-ineffective-teachers-copy-and-zeros.md), [0026](0026-out-of-field-file-and-copy.md)

## Code check

No shared "take the latest public file" step was added. Registrations stay explicit. The May 2026 CTE registration matches the rule on the facts above. The 18 October copy choices in #58, #64, and #66 still do not match `config/kde_sources.yaml`. Those conflicts are on those records. This record does not swap a file and does not resolve those issues.

## KDE guidance check

Verdict: no KDE statement that a later file supersedes an earlier one. Possible conflict with wording that calls the first release final. Flag only. The rule is unchanged. These pages were not re-fetched in this pass. Citations are from the 25 October 2026 research.

- HB 500 FAQ, and the HB 6 FAQ with the same sentence: "Due to quality control checks and the regulatory review period, there could be multiple releases of data." The "most current, accurate data" sentence is scoped to reading and math percentages on district websites. https://www.education.ky.gov/AA/Acct/Documents/HB500_ReadingMathPerformance_WebsiteRequirements.pdf and https://www.education.ky.gov/AA/Acct/Documents/HB6_ReadingMathPerformance_WebsiteRequirements.pdf
- 703 KAR 5:240 §7: a data-review request is due within ten days after KDE "officially releases the final accountability classifications." https://apps.legislature.ky.gov/law/kar/titles/703/005/240/
- DAC email, 2 February 2026: "Final Data Files After 10-Day Regulatory Data Review," and "updated datasets" on 3 February. https://content.govdelivery.com/accounts/KYDE/bulletins/4075ddd
- Appeals, twice-weekly refreshes, and phased finance releases: KAR §7(2)–(4); commissioner's message 23 October 2025 (https://content.govdelivery.com/accounts/KYDE/bulletins/3f857fd); finance advisory 10 July 2026 (https://content.govdelivery.com/accounts/KYDE/bulletins/41e69fb).
- Wording that cuts the other way: the regulation's "final accountability classifications" at the first release, and the 10 December 2024 commissioner's message that assessment and accountability data is "already final," with reload differences "due to rounding and the way small cell counts are suppressed." https://content.govdelivery.com/accounts/KYDE/bulletins/3c68ec8
- Not found: version numbers or change logs on public SRC files, a statement of which host is official, or a KDE process for correcting a prior school year.
