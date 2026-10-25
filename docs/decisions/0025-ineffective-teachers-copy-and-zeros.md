# 0025 Ineffective-teacher copies, KYRC25 comparison, and published zeros

Status: conflicts-with-code

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
- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/123 (open). The yaml still registers the Open House copies, so the 18 October copy choices on #58, #64, and #66 are not applied.
- `config/kde_sources.yaml` `students_taught_by_ineffective_teachers`
- Signed-gap follow-up: [0027](0027-signed-title-i-equity-gap.md)

## Code check

Does not match items 1 and 2. Re-read on this branch.

- Item 1: `students_taught_by_ineffective_teachers` still registers the bare historical name `KYRC24_OVW_Students_Taught_by_Ineffective_Teachers.csv`. The comment says the datasets object is 39 rows with percents above 100 and "It is not swapped." The 1,589-row UAT copy is not the registered file.
- Item 2: KYRC25 is registered as `url: kyrc25`, the datasets container. The yaml header says the downloader keeps datasets when Content-MD5 is missing. That is not "load the UAT copy."

A title search of pull requests found no follow-up that performs either switch. Issue #64 is still open. Issue #123 tracks that the yaml still has the Open House copies. Items 3 and 4 (keep published zeros; repoint the catalog) were not re-checked as implemented. Not fixed here. The 39-row datasets file is the visibly broken example in [0052](0052-republish-supersedes-earlier.md). This conflict stays open as follow-up (a).

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE notice that names which KYRC24 ineffective-teacher file replaces the other, or that a published 0 percent should be dropped.
