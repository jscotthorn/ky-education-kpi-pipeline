# 0025 Ineffective-teacher copies, KYRC25 comparison, and published zeros

Status: accepted

## Question

Which KYRC24 and KYRC25 copies of students-taught-by-ineffective-teachers should the pipeline load, and should a published 0 be kept?

## What KDE changed

The datasets KYRC24 object is 39 rows with some percents above 100. The historical extract is 1,562 rows. KYRC25 has a UAT copy. datasets has no Content-MD5 for the related out-of-field file; the ineffective comparison is the same kind of datasets-versus-UAT check.

## Options considered

Keep the historical 1,562-row extract, switch 2024 to the UAT copy, and either drop or keep KYRC24-26 zeros.

## Decision

18 October 2026, four comments on issue #64.

1. Switch 2024 to the UAT copy (1,589 rows). https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/64#issuecomment-6251862442
2. Load the KYRC25 UAT copy and compare datasets with UAT when Content-MD5 is missing. https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/64#issuecomment-6251894590
3. Keep KYRC24-26 zeros as published. https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/64#issuecomment-6251903063
4. Repoint the covariate-catalog orphan. https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/64#issuecomment-6251914299

## Who decided and how we know

Those four comments were posted on the `jscotthorn` account through the Cursor GitHub App.

## Why

The comments choose the UAT copy and say a published 0 stays. Reason the 39-row datasets object is wrong, beyond the row count and the percents above 100: not documented.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/64 (still open)
- `config/kde_sources.yaml` `students_taught_by_ineffective_teachers`
- Signed-gap follow-up: [0027](0027-signed-title-i-equity-gap.md)

## Code check

Does not match the copy switch. `config/kde_sources.yaml` says the datasets KYRC24 object is 39 rows and is not swapped, and that the historical 1,562-row extract stays registered. Issue #64 is still open. Not resolved here.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE notice that names which KYRC24 ineffective-teacher file replaces the other, or that a published 0 percent should be dropped.
