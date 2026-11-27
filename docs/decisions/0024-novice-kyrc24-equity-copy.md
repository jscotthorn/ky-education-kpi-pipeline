# 0024 Point 2024 novice-teacher equity at the datasets copy

Status: conflicts-with-code

## Question

The historical KYRC24 inexperienced-teacher file and the datasets copy disagree on equity cells. Which copy should 2024 use?

## What KDE changed

Two public copies of the same KYRC24 object do not match. `config/kde_sources.yaml` says the datasets copy's state Title I All Students rate is 66.0 and the registered historical file's rate is 11.3 ("11.3 here").

## Options considered

Keep the historical registration, or switch the 2024 equity registration to the datasets copy.

## Decision

18 October 2026. Switch the 2024 novice equity registration to the datasets copy.

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/58#issuecomment-6251862223 posted on the `jscotthorn` account through the Cursor GitHub App.

## Why

The comment chooses the datasets copy. Reason that copy is the right Title I figure: not documented in the comment beyond the disagreement.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/58 (still open)
- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/123 (open). The yaml still registers the Open House copies, so the 18 October copy choices on #58, #64, and #66 are not applied.
- `config/kde_sources.yaml` `novice_teachers`
- The same historical filename is also registered at `config/kde_sources.yaml` lines 326-327 under `students_taught_by_inexperienced_teachers`. The `novice_teachers` comment (lines 478-480) says that key is for download only and that this ETL reads the copy in the `novice_teachers` directory. That sentence names the KYRC26 file. The KYRC24 historical name is on both lists.

## Code check

The `novice_teachers` registration now matches. Re-read on this branch: KYRC24 equity is `url: kyrc_datasets` for `KYRC24_OVW_Students_Taught_by_Inexperienced_Teachers.csv`. The comment records the replaced Open House file (Last-Modified Thu, 29 May 2025 22:09:21 GMT, md5 `9168e6d6572692925a269885dabdc7dd`, state Title I All Students 11.3) and this copy (state Title I All Students 66.0, md5 `e0cf730225ef7ab3a010ec6142b7b080`, same bytes on UAT). The datasets copy is the later public file and is not visibly broken, so the registration follows [0052](0052-republish-supersedes-earlier.md). The same historical name is still registered under `students_taught_by_inexperienced_teachers`. The `novice_teachers` comment says that list is for download only and that this ETL reads the copy in the `novice_teachers` directory. Status stays `conflicts-with-code` until it is updated after merge. That status change is not this edit.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE notice that says which KYRC24 inexperienced-teacher file replaces the other.
