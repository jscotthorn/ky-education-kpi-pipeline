# 0002 Report scripts are out of scope for KYRC26

Status: conflicts-with-code

## Question

Should the KYRC26 update fix `scripts/generate_report_drafts.py`, which misses state rows labeled `All Districts`?

## What KDE changed

KYRC files label the state row `All Districts` / `999000`. Older files use `State` / `999`. No KDE notice of that label change was found. See [0003](0003-shared-id-normalization.md).

## Options considered

The issue asks for a fix. The decision leaves the issue open.

## Decision

18 October 2026. Report scripts are out of scope for the 2025-26 update. Issue #42 stays open. Until it is fixed, report drafts have no state average for 2024-2026, non-finite values can leak into drafts, and the analysis scripts stop at 2025. The #61 ID normalization should not try to fix this.

## Who decided and how we know

Recorded as Scott's decision in https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/42#issuecomment-6251982959 (Cursor GitHub App, account `jscotthorn`).

## Why

Reason not documented. The comment states the scope and the consequences. It does not say why the scripts are out of scope.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/42
- Comment above
- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/115
- `notes/115--cleanup-batch1.md`
- `scripts/generate_report_drafts.py`

## Code check

Does not match. Pull request #115, merged 24 October 2026, changed `scripts/generate_report_drafts.py`. `state_row_mask` selects district `State` or `All Districts`, and school code `999` or `999000`. `get_state_average` accepts school name `---District Total---` or `---State Total---` (`scripts/generate_report_drafts.py` around lines 391-413 and 786-815). `notes/115--cleanup-batch1.md` describes that change. Issue #42 is still open. This log does not revert #115 or edit the script.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

The label change is in the files. No KDE page was found that says whether report drafts must show a state average. The conflict with #115 is internal. It is not a KDE conflict.
