# Republishes and alternate copies

This is the place [0052](0052-republish-supersedes-earlier.md) requires for differences on each republish. The rule is: use the latest public KDE file after the review window closes, unless that copy is visibly broken, and record the differences here.

The 26 rows are from the 25 October 2026 research attachment. Cell counts, hashes, and Last-Modified times in the "What differs" column were not re-counted in this pass, except where the "Checked here" column says the config was re-read. KDE quotations in that research were not re-fetched. Sources for the KDE side are listed under the table.

Hosts: Hist is Open House HistoricalDatasets (`base_url`). DS is the live blob `datasets` container (`base_url_kyrc25` / `base_url_kyrc26`). UAT is `datasetsuat6051276`.

| # | File / year | Copies | What the pipeline uses | What differs | Checked here |
| --- | --- | --- | --- | --- | --- |
| R1 | KYRC24 CTE participation, 2023-24 | Hist 29 May 2025, md5 `0b0d30c7…`. DS 14 May 2026, md5 `86677fc9…`, same bytes as UAT | DS republish. Scott, #113, pull request #116. [0008](0008-kyrc24-cte-may-2026-republish.md) | Research: 3,544 stars changed to 0.0, blank, `--`, or `N/A`. No published number changed. 349 GREC fills. Output 160,029 to 157,861 | Yaml comment matches those md5s and the 14 May 2026 Last-Modified. Counts not re-counted. Complies with [0052](0052-republish-supersedes-earlier.md) if the review-window point holds. See that record. |
| R2 | Same KYRC24 CTE file, 0.0 rates | DS republish | Kept as published, with a warning. [0009](0009-cte-zero-rate-beside-blank-count.md), [0051](0051-cte-zero-rate-on-positive-count.md) | 5,407 cells are 0.0 beside a blank count. 699 are a 0.0 participation rate beside a positive count (208 KYRC24, 491 KYRC23) | Arithmetic of the notes was checked earlier. Raw files not re-counted here. |
| R3 | KYRC24 teacher working conditions | Hist missing some districts; DS; KYRC25 is the same survey | DS, plus a few Hist-only and KYRC25-only cells, stored as 2024. [0015](0015-twc-kyrc24-datasets-copy.md) | Research names district-total disagreements, including Glasgow | Not re-diffed here. |
| R4 | Impact Kentucky waves | Odd-year files repeat the prior survey | One row per administration. [0014](0014-impact-kentucky-one-wave.md) | Research: 2022 equals 2023; KYRC24 DS equals KYRC25 | Not re-diffed here. |
| R5 | KYRC24 novice equity | Hist state Title I 11.3; DS 66.0 | Decision: DS (#58). Config still registers Hist. [0024](0024-novice-kyrc24-equity-copy.md) | Every published cell differs in level, per the research | Yaml re-read: "66.0 versus 11.3 here" and "is not swapped." |
| R6 | KYRC24 ineffective teachers | Hist 1,562 rows; DS 39 rows, percents above 100; UAT 1,589 rows | Decision: UAT (#64 item 1). Config still registers Hist. [0025](0025-ineffective-teachers-copy-and-zeros.md) | The 39-row DS file is the visibly broken example in [0052](0052-republish-supersedes-earlier.md) | Yaml re-read: "39 rows with percents above 100" and "It is not swapped." |
| R7 | KYRC25 ineffective teachers | DS vs UAT | Decision: UAT, and compare when MD5 is missing (#64 item 2). Config registers DS | Research: only 8 state cells differ | Yaml re-read: `url: kyrc25`. Header: downloader keeps datasets when Content-MD5 is missing. |
| R8 | KYRC24 out-of-field | Hist vs DS (DS = UAT) | Decision: DS (#66). Config still registers Hist | Research: 381 keys differ, in some equity columns only | Yaml re-read: "Do not swap it here." |
| R9 | KYRC25 out-of-field | DS vs UAT | DS, because Content-MD5 is missing | Yaml: UAT negates some equity-gap cells | Yaml comment re-read. Cells not re-diffed. |
| R10 | KYRC23 discipline vs annual 2023 | Annual file vs KYRC23 on DS | Annual file for discipline. KYRC23 legal sanctions file is the only 2022-23 legal source. [0020](0020-kyrc23-legal-sanctions-source.md), [0017](0017-discipline-all-star-rows.md) | Of 10,321 all-star 2023 rows, the republish had 9,057 all zeros, 434 positive, 971 still starred | Counts are from the #87 comment cited on [0017](0017-discipline-all-star-rows.md). Not re-counted here. |
| R11 | KYRC23 gifted | Annual vs KYRC23 | Annual. KYRC23 skipped | Research: no preschool column, no consolidated-student-group rows; shared All Students cells match | Not re-diffed here. |
| R12 | KYRC23 CTE | Annual counts vs KYRC23 rates | Rates from KYRC23. Counts from the annual file. [0010](0010-cte-2023-mixed-ids.md) | Research: 7,888 keys, 0 numeric conflicts | Not re-counted here. |
| R13 | KYRC23 groups that exist only in the republish | Annual vs KYRC23 | Decision: load keys that exist only in the republish (#95). Not implemented. [0043](0043-kyrc23-republish-only-groups.md) | Kindergarten: 3 groups, 15,126 rows, per the issue body | Issue body was the source of [0043](0043-kyrc23-republish-only-groups.md). Not re-counted here. |
| R14 | Other KYRC23 republishes of 2022-23 | KYRC23 on DS/UAT. Hist 404 | Annual 2023 files. Config comments say the KYRC23 names are skipped | Research: turnover N/A to 0, certification rounding, TWC relabel, and other repeats | Skipped-filename comments were seen in yaml for several of these. Not all were re-read. |
| R15 | KYRC24 same-name DS copies left on Hist | Hist vs DS, many dated 14 May 2026 in the research | Hist. Comments say "not swapped" | Research: later DS copies turn zeros into stars, stub a file, rename a column, or fill blanks with 0 | "Not swapped" was re-read for turnover, certification, novice, ineffective, and out-of-field. Other files in this row were not re-read one by one. |
| R16 | KYRC25 overview suspensions | Same as KYRC24 except school year | Not registered | Research: cell-for-cell the same CRDC collection | Not re-checked here. Year rule is [0048](0048-crdc-kpi-year-split.md). |
| R17 | Advanced coursework overviews | Same CRDC collection on more than one KYRC file | KYRC23 stored as KPI year 2021. Later overviews skipped | Research: KYRC24 zero-fills some rows KYRC25 leaves blank | Registration not re-opened here. |
| R18 | KYRC25 school courses summary | Hist 19 Feb 2026 vs DS 9 Oct 2026 | DS | Research: Hist has 1 extra course; 4 totals differ by 1 student | Not re-diffed here. |
| R19 | KYRC25 Kentucky Summative Assessment, 2024-25 | Hist 10 Feb 2026; DS 14 May 2026; UAT same size as DS | Config registers `url: kyrc25` (DS). No issue found | Research: 342 rows, 868 cells. State high-school math All Students 57.7 vs 57.6. Johnson Central HS math 62 to 53 | Config registration re-read. The diff was not re-run. Unconfirmed. [coverage-gaps.md](coverage-gaps.md). |
| R20 | Other KYRC25 accountability files | DS Last-Modified after February 2026, per the research | DS (`url: kyrc25` for postsecondary readiness and both graduation files) | Research: postsecondary readiness 9 Oct 2026; 4-year graduation 6 Aug 2026. Not compared to Hist | Names are registered on `url: kyrc25`. Dates and diffs not re-checked. |
| R21 | KYRC26 files | DS 15 Oct 2026; UAT same bytes in the notes; OAA temporary names | DS | Inside the review window through 26 Oct 2026. [0049](0049-kyrc26-files-loaded-15-oct-2026.md) | Load date is in `notes/103`. Files not re-HEAD'd here. |
| R22 | KYRC25 college-admissions names | Two names, same bytes, per notes/103 | One alias | Alias | Not re-checked here. |
| R23 | KYRC25 financial summary | DS = UAT, July 2026, per the research | DS | An old dashboard snapshot had held a stale 2023-24 figure in the 2025 slot. Not a KDE republish | Not re-checked here. |
| R24 | December 2024 reload of 2021-22 and 2022-23 | Annual files vs KYRC23 | Annual files, plus the KYRC23 keys in R10, R12, and R13 | KDE: rounding and suppression variance. Commissioner's message 10 Dec 2024. Not re-fetched | https://content.govdelivery.com/accounts/KYDE/bulletins/3c68ec8 |
| R25 | 2020 4-year graduation rate | Bulletin 90.9 vs a later trend 91.1 | The pipeline stores the registered file. It does not reconcile the bulletins | No KDE explanation found in the earlier research | Not re-fetched here. |
| R26 | Glasgow 2020 and 2022 TWC district totals | Two rows in one file | NA, suppressed Y. [0016](0016-glasgow-conflicting-totals.md) | Within-file conflict, not a republish | Record [0016](0016-glasgow-conflicting-totals.md). Not re-counted here. |

## KDE sources for the triggers

Not re-fetched in this pass. Short names match the 25 October 2026 research.

| Short name | URL |
| --- | --- |
| DAC 19 Nov 2025, 10-day review | https://content.govdelivery.com/accounts/KYDE/bulletins/3fbfe1b |
| DAC 2 Feb 2026, final data files | https://content.govdelivery.com/accounts/KYDE/bulletins/4075ddd |
| DAC 15 Oct 2026, 2025-26 review | https://content.govdelivery.com/accounts/KYDE/bulletins/42f2b1a |
| 703 KAR 5:240 | https://apps.legislature.ky.gov/law/kar/titles/703/005/240/ |
| HB 500 FAQ | https://www.education.ky.gov/AA/Acct/Documents/HB500_ReadingMathPerformance_WebsiteRequirements.pdf |
| HB 6 FAQ | https://www.education.ky.gov/AA/Acct/Documents/HB6_ReadingMathPerformance_WebsiteRequirements.pdf |
| Commissioner, 10 Dec 2024 | https://content.govdelivery.com/accounts/KYDE/bulletins/3c68ec8 |
| Commissioner, 23 Oct 2025 | https://content.govdelivery.com/accounts/KYDE/bulletins/3f857fd |
| Finance advisory, 10 Jul 2026 | https://content.govdelivery.com/accounts/KYDE/bulletins/41e69fb |

Not found in that research: a KDE statement that a later file always supersedes an earlier one; a public change log; a ranking of the datasets, UAT, and Open House hosts; a process for correcting a prior school year.
