# 0002 Report scripts are out of scope for KYRC26

Status: accepted

## Question

Should the KYRC26 update fix `scripts/generate_report_drafts.py`, which misses state rows labeled `All Districts`?

## What KDE changed

KYRC files label the state row `All Districts` / `999000`. Older files use `State` / `999`. No KDE notice of that label change was found. See [0003](0003-shared-id-normalization.md).

## Options considered

The issue asks for a fix. The decision leaves the issue open.

## Decision

18 October 2026. Report scripts are out of scope for the 2025-26 update. Issue #42 stays open. The #61 ID normalization should not try to fix the scripts.

25 October 2026, 12:00 PM ET. That scope stands. Scott approved one narrow exception: the state-row lookup already merged in pull request #115. `scripts/generate_report_drafts.py` may treat a state district as `State` or `All Districts`. The same change accepts school code `999` or `999000`, and school name `---District Total---` or `---State Total---`. That exception does not open the rest of the report-script work.

Scott's words: "Keep report scripts out of scope for KYRC26; record #115's state-row fix as a narrow exception."

Per Moon, 2026-10-25; not in Scott's answer: the NaN guard and extending the analysis past 2025 stay out of scope. Those two items are an open confirmation. Scott's answer does not mention them.

## Who decided and how we know

18 October scope: https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/42#issuecomment-6251982959 (Cursor GitHub App, account `jscotthorn`).

25 October exception: Scott, direct, 2026-10-25 12:00 PM ET, his answer on a question card in the ECC manager bot's chat.

Source type for the 25 October exception: chat answer. Scott's own answer in an agent chat, quoted with date and time (ET). Bot: ECC manager bot. Archivist checked the quote verbatim against the transcript.

Repo comment: pending

## Why

Reason not documented. The comment states the scope and the consequences. It does not say why the scripts are out of scope.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/42
- Comment above
- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/115
- `notes/115--cleanup-batch1.md`
- `scripts/generate_report_drafts.py`

## Code check

Matches the exception. Pull request #115, merged 24 October 2026, changed `state_row_mask` and `get_state_average` in `scripts/generate_report_drafts.py` (around lines 391-413 and 786-815) to the labels in the decision. `notes/115--cleanup-batch1.md` describes that change. The 18 October out-of-scope rule still stands for the rest of the script. The earlier conflict is closed by the 25 October exception. This log does not edit the script.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

The label change is in the files. No KDE page was found that says whether report drafts must show a state average.
