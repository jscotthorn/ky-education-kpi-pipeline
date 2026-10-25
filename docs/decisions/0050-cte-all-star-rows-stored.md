# 0050 Store all-star CTE rows as suppressed

Status: accepted

Supersedes the skip-all-star clause of [0005](0005-cte-skip-all-star-padding.md).

## Question

KYRC26 CTE participation adds a full grid, including 1,230 all-`*` demographic rows that KYRC25 omitted. Should those rows be skipped, or stored like any other star?

## What KDE changed

`notes/104--kyrc26-file-to-etl-mapping.md`: KYRC26 adds 1,230 all-star demographic rows. No KDE statement was found that says those stars are padding. CHG-2022 says `*` is suppression. Quote in [0001](0001-suppression-rule.md).

## Options considered

Skip all-`*` rows, which is what [0005](0005-cte-skip-all-star-padding.md) said on 18 October 2026, or store them as NA with `suppressed` Y, which is what pull request #106 already does and what #87 did for discipline ([0017](0017-discipline-all-star-rows.md)).

## Decision

25 October 2026, 11:59 AM ET. Keep storing all-star CTE rows as suppressed, the same way as any other star: value NA, `suppressed` Y. This matches #87. The #46 decision is updated by this record. [0005](0005-cte-skip-all-star-padding.md) is superseded.

Scott's words: "Keep storing all-star CTE rows as suppressed, matching #87; update the #46 decision."

## Who decided and how we know

Scott, direct, 2026-10-25 11:59 AM ET, his answer on a question card in the ECC manager bot's chat.

Source type: Scott's own answer in an agent chat, quoted with date and time (ET). Bot: ECC manager bot. Archivist checked the quote verbatim against the transcript.

Repo comment: pending

The behavior he confirmed was already merged in pull request #106 and was the discipline rule in pull request #87.

## Why

Scott's sentence is the reason recorded here. It points at the #87 rule: an all-star row is stored, not skipped. [0017](0017-discipline-all-star-rows.md) records why that rule was used for discipline. A separate reason for the CTE rows is not in his answer.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/46
- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/87
- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/106
- [0005](0005-cte-skip-all-star-padding.md)
- [0017](0017-discipline-all-star-rows.md)
- `notes/106--cte-participation.md`
- `etl/cte_participation.py` `convert_to_kpi_format`

## Code check

Matches. Pull request #106 already stores a star, including a fully starred demographic, as value NA with `suppressed` Y. The pull request body says: "Source stars with a demographic still land on a Y row with an empty value: … KYRC26 completers 4,949 and enrollment 1,869." `notes/106--cte-participation.md` says the same rule, "including a fully starred demographic." `etl/cte_participation.py` writes each star cell as `suppressed` Y and does not skip an all-star row.

## KDE guidance check

Verdict: consistent. Flag only in the sense that it is a check, not a change.

CHG-2022 says `*` is suppression (quote in [0001](0001-suppression-rule.md)). https://www.education.ky.gov/districts/tech/sis/Documents/SRC_ReportingChanges_2022.pdf Storing the all-star row matches that meaning. The possible conflict on [0005](0005-cte-skip-all-star-padding.md) was the skip. Not found: a KDE statement that an all-star row is padding.
