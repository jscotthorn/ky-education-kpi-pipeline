# 0004 Postsecondary enrollment stars

Status: accepted, code differs (follow-up open)

## Question

`postsecondary_enrollment` drops `*` instead of storing `suppressed` Y. Which stars are real suppression, and which are padding?

## What KDE changed

`notes/104--kyrc26-file-to-etl-mapping.md` says KYRC25 pads cohorts that do not exist with stars, while KYRC24 and KYRC26 omit fully starred groups. No KDE sentence was found that calls a star padding.

## Options considered

The decision comment states the rule. It does not number options.

## Decision

18 October 2026, with [0005](0005-cte-skip-all-star-padding.md). Store `*` as NA with `suppressed` Y only when the star hides a real value. A starred postsecondary group counts as real when KDE's ADLF Current_Year_Graduates file, or the cohort count, shows the cohort exists. Skip padding stars for cohorts with no graduates. Document counts for KYRC24/26 groups that are missing but have graduates. Label ambiguous cases `kde-recheck`. Note the 7 KYRC26 rows where private = total - public leaks a suppressed value. Fix in one follow-up with #46 and the #49 CTE comma fix.

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/44#issuecomment-6251989485

## Why

The comment states the rule. It does not add a separate reason beyond the cohort test. Reason for the cohort test: not a longer rationale than that sentence.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/44
- `etl/postsecondary_enrollment.py` (module docstring, lines 9-11)
- `notes/104--kyrc26-file-to-etl-mapping.md` (postsecondary section)

## Code check

The module stores a `*` measure as NA with `suppressed` Y when that row's `total_in_group` is a published number greater than 0. A cohort of 0 or blank still emits no row for those stars. A cohort that is itself `*` still emits no new row. Those rows, and the 7 KYRC26 rows where private = total − public, are listed in `notes/flags/A-flagged-keys.csv`. The in-file cohort test and the starred-cohort label are recorded in [0057](0057-postsecondary-in-file-cohort-count.md). Status stays `accepted, code differs (follow-up open)`. The status can be updated after the pull request that makes this change merges. That status change is not made here.

## KDE guidance check

Verdict: possible conflict. Flag only.

CHG-2022 defines `*` as suppression, not as padding (quote in [0001](0001-suppression-rule.md)). Not found: a KDE statement that a starred postsecondary cohort means no students. Calling those rows padding is the pipeline's reading.
