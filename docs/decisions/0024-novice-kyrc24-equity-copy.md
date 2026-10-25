# 0024 Point 2024 novice-teacher equity at the datasets copy

Status: accepted

## Question

The historical KYRC24 inexperienced-teacher file and the datasets copy disagree on equity cells. Which copy should 2024 use?

## What KDE changed

Two public copies of the same KYRC24 object do not match. The historical file's state Title I rate is 66.0. The datasets copy's state Title I rate is 11.3.

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
- `config/kde_sources.yaml` `novice_teachers`

## Code check

Does not match. `config/kde_sources.yaml` still says the historical KYRC24 equity file is not swapped, and it cites state Title I 66.0 versus 11.3. Issue #58 is still open. Not resolved here. Status stays `accepted` because no merged change did the opposite on purpose. The registered file is still the one the decision said to leave.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE notice that says which KYRC24 inexperienced-teacher file replaces the other.
