# 0020 Load KYRC23 legal sanctions as the only 2022-23 legal source

Status: accepted

## Question

2022-23 legal sanctions appear in `KYRC23_SAFE_Legal_Sanctions.csv` and again inside a discipline file. Which file is the source?

## What KDE changed

The historical host returns 404 for the legal-sanctions files for 2019-20 through 2021-22. KYRC23 publishes a legal file. Its IDs are `999000` and 6-digit school codes. The discipline file for the same year still uses `999` and 3-digit district codes. Issue #61 notes that split. See [0003](0003-shared-id-normalization.md).

## Options considered

Load both and dedupe, load only the legal file, or load only the discipline twin.

## Decision

19 October 2026. Load `KYRC23_SAFE_Legal_Sanctions.csv` as the only 2022-23 legal source. Skip the discipline twin for that measure.

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/87#issuecomment-6279807406 posted on the `jscotthorn` account through the Cursor GitHub App.

## Why

The comment names the KYRC23 legal file as the source and the discipline copy as the one to skip. Reason for preferring the legal file: not documented beyond that choice.

## Links

- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/87
- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/61
- [0003](0003-shared-id-normalization.md)

## Code check

Matches the source choice merged in #87. Shared ID normalization is still open under [0003](0003-shared-id-normalization.md).

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE statement that names which of the two 2022-23 files is canonical, or a notice that the earlier legal files were withdrawn.
