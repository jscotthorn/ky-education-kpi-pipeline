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

Does not match. Re-read on this branch: `novice_teachers` still registers the bare historical name `KYRC24_OVW_Students_Taught_by_Inexperienced_Teachers.csv` and says the datasets copy "is not swapped." The same historical name is also registered at lines 326-327 under `students_taught_by_inexperienced_teachers`. A title search of pull requests found no follow-up that performs the switch. Issue #58 is still open. Issue #123 tracks that the yaml still has the Open House copies. Not fixed here. The standing file rule is [0052](0052-republish-supersedes-earlier.md). This conflict stays open as follow-up (a).

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE notice that says which KYRC24 inexperienced-teacher file replaces the other.
