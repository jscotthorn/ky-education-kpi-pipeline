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

The comments choose the UAT copy and say a published 0 stays. The later confirmation that the live 39-row KYRC24 file is visibly broken, and the recheck counts, are [0055](0055-kyrc24-ineffective-teachers-visibly-broken.md).

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/64 (still open)
- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/123 (open). Pull request #140 registers the 18 October copy choices on #58, #64, and #66. The issue was not closed there.
- [0055](0055-kyrc24-ineffective-teachers-visibly-broken.md). #64 item 1 falls under [0052](0052-republish-supersedes-earlier.md). The config change is in pull request #140 (`url: kyrc_uat`).
- `config/kde_sources.yaml` `students_taught_by_ineffective_teachers`
- Signed-gap follow-up: [0027](0027-signed-title-i-equity-gap.md)

## Code check

Re-read after merging `feature/2025-26-kyrc26` at `9c0755f`. Items 1 and 2 match the registration.

- Item 1: `students_taught_by_ineffective_teachers` registers `url: kyrc_uat` for `KYRC24_OVW_Students_Taught_by_Ineffective_Teachers.csv`. The yaml comment records the 1,589-row UAT copy (Non-White) and says the live datasets object is 39 rows with percents above 100 and is not loaded. That live file is the visibly broken copy under [0052](0052-republish-supersedes-earlier.md), confirmed by [0055](0055-kyrc24-ineffective-teachers-visibly-broken.md).
- Item 2: KYRC25 is `url: kyrc_uat`. `data/prepare_kde_data.py` compares bodies and reports a mismatch when either Content-MD5 is missing, and still keeps the primary URL for an unpinned key. This file is pinned (`kyrc_uat` is in `_PINNED_URL_TYPES`), so it does not auto-select. The 27 Nov 2026 choice to load this UAT copy is [0058](0058-kyrc25-uat-and-kyrc26-ineffective-refresh.md).
- Item 3: a published 0 is kept. `StudentsTaughtByIneffectiveTeachersETL.extract_metrics` keeps a percent of 0 and omits a blank. A negative cell is stored with `suppressed` N. The novice equity path stores a negative too. The institutional novice rates still require `value >= 0`. Record [DNNNN](DNNNN-store-published-gap.md) makes that change. The copy switch did not.
- Item 4: done by the J1 catalog cleanup now on this branch. `analysis/config/covariate_catalog.yaml` names `students_taught_by_ineffective_teachers_rate_title_1`, the Title I metric the module emits. The copy-switch edit did not touch the catalog.

The earlier code check said no pull request performed the switch and that the yaml still registered the Open House copy. This branch registers the UAT copies, and items 1-4 match the code. Status is `accepted` (was `conflicts-with-code`). Issues #64 and #123 were not closed here.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE notice that names which KYRC24 ineffective-teacher file replaces the other, or that a published 0 percent should be dropped.
