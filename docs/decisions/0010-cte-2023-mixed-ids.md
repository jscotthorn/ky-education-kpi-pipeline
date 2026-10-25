# 0010 Accept mixed 2023 CTE IDs until the shared normalization

Status: accepted

## Question

2023 CTE rates from KYRC23 use 6-digit district and state IDs. 2023 counts from the annual file use 3-digit IDs. Should this pull request normalize them?

## What KDE changed

Both forms are in KDE files. KYRC23 state row is `999000` / `All Districts`. The annual file uses `999` / `State`. School codes are 6 digits in both. See [0003](0003-shared-id-normalization.md).

## Options considered

Normalize in pull request #114, or leave the mixed IDs until issue #61.

## Decision

24 October 2026. (A) 2023 district and state rates stay on `xxx000`, `999000`, and district `All Districts`. 2023 counts stay on `xxx`, `999`, and district `State`, until #61. School rows are already 6 digits.

The completion-rate name is [0011](0011-cte-2023-completion-rate-name.md).

## Who decided and how we know

Pull request #114 body, "Review follow-up": "Scott, 24 October 2026." https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/114

Repeated on issue #61: https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/61#issuecomment-6387831105

## Why

The #61 comment says this is the same approach as the #87 legal-sanctions file, which also keeps the IDs KDE published until the shared step. A separate reason for waiting is not given beyond #61's schedule.

## Links

- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/114
- `notes/114--cte-2023-published-rates.md`
- `notes/104--kyrc26-file-to-etl-mapping.md` (CTE follow-up, 24 October 2026)
- [0003](0003-shared-id-normalization.md)

## Code check

Matches the temporary choice. The notes and the #114 description say the two 2023 sources keep different ID widths. The shared step in [0003](0003-shared-id-normalization.md) is not built.

## KDE guidance check

Verdict: consistent. Flag only.

Each row keeps the ID KDE printed. The join problem is the pipeline's. Not found: a KDE crosswalk.
