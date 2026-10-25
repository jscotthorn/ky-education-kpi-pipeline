# 0040 Drop the constructed dual-credit program flag

Status: accepted

## Question

`has_dual_credit_program` was 1 when unduplicated enrollment was positive and suppressed when enrollment was starred. It was never 0. Keep it?

## What KDE changed

KDE publishes dual-credit enrollment. It does not publish this flag. A school without a program has no participation row.

## Options considered

Keep the flag, or drop it and add no alias.

## Decision

20 October 2026. Drop `has_dual_credit_program`. Do not restore it and do not add an alias. Enrollment, completer counts, qualifying-score counts, the two derived student rates, and the course-level counts and rates stay.

## Who decided and how we know

`notes/104--kyrc26-file-to-etl-mapping.md`, "Owner decision, Scott Horn, 2026-10-20." The same paragraph is repeated in the dual-credit section of that note.

## Why

Quoted from that note: the pipeline set the flag to 1 when unduplicated enrollment was positive and stored it as value NA with suppressed Y when enrollment was starred. No registered enrollment is 0. A school without a program has no participation row, so the flag cannot record the absence of a program. It was never 0: 16,347 ones and 18,249 suppressed rows, 34,596 in all, one per participation source row.

## Links

- `notes/104--kyrc26-file-to-etl-mapping.md` (dual-credit removal entry)
- Published-values rule: [0054](0054-store-values-as-kde-publishes.md)
- Pull request history for the removal is the KYRC26 dual-credit work recorded in that note

## Code check

Matches the drop recorded in the note. The note's before-and-after row counts are the check this pass used. The module was not re-run.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE field that means "this school has a dual-credit program," including a published 0 for a school that does not.
