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

The comments choose the UAT copy and say a published 0 stays. The later confirmation that the live 39-row KYRC24 file is visibly broken, and the recheck counts, are [0055](0055-kyrc24-ineffective-teachers-visibly-broken.md).

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/64 (still open)
- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/123 (open). The yaml still registers the Open House copies, so the 18 October copy choices on #58, #64, and #66 are not applied.
- [0055](0055-kyrc24-ineffective-teachers-visibly-broken.md). #64 item 1 falls under [0052](0052-republish-supersedes-earlier.md). The config change stays on #123.
- `config/kde_sources.yaml` `students_taught_by_ineffective_teachers`
- Signed-gap follow-up: [0027](0027-signed-title-i-equity-gap.md)

## Code check

Items 1 and 2 now match the registration. Re-read on this branch.

- Item 1: `students_taught_by_ineffective_teachers` registers `url: kyrc_uat` for `KYRC24_OVW_Students_Taught_by_Ineffective_Teachers.csv`. The comment records the 1,589-row UAT copy (Non-White) and says the live datasets object is 39 rows with percents above 100 and is not loaded. That live file is the visibly broken copy under [0052](0052-republish-supersedes-earlier.md), confirmed by [0055](0055-kyrc24-ineffective-teachers-visibly-broken.md).
- Item 2: KYRC25 is `url: kyrc_uat`. The downloader compares bodies and reports a mismatch when either Content-MD5 is missing, and still keeps the primary URL for an unpinned key. This file is pinned, so it does not auto-select. The 27 Nov 2026 choice to load this UAT copy is [DNNNN](DNNNN-kyrc25-uat-and-kyrc26-ineffective-refresh.md).

Item 3 (keep published zeros) is unchanged in the ineffective and novice extractors. Item 4 (the covariate catalog) is not this edit. Status stays `conflicts-with-code` until it is updated after merge. That status change is not this edit.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE notice that names which KYRC24 ineffective-teacher file replaces the other, or that a published 0 percent should be dropped.
