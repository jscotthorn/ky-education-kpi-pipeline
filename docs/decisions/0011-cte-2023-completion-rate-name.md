# 0011 Keep the 2023 CTE completion rate on cte_completion_rate_grade_12

Status: accepted

## Question

KYRC23's grade-12 completion percent uses a different base than neighboring years. Should it keep the metric name `cte_completion_rate_grade_12`?

## What KDE changed

`notes/114--cte-2023-published-rates.md`: the KYRC23 rate equals completers divided by eligible completers on 2,420 of 2,420 rows. State eligible completers are 25,097 in 2023 and 43,229 in 2022. No KDE note was found that says the 2023 denominator changed.

## Options considered

The pull request body states the choice. It does not list a rejected metric name.

## Decision

24 October 2026. (B) The 2023 completion rate stays `cte_completion_rate_grade_12`. It is not comparable year over year. It is on the 27 October 2026 KDE recheck. The stored value is the printed cell. Rounded half-up to 0.1, that rate equals published completers divided by published eligible completers on 2,420 of 2,420 rows.

## Who decided and how we know

https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/114 (review follow-up in the body)

https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/112#issuecomment-6387833464

## Why

The #112 comment: the 2023 eligible base is about half of the neighboring years, so the rate is not comparable. State eligible is 43,229 in 2022 and 25,097 in 2023, with about 41,600 implied for 2024.

## Links

- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/114
- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/112
- `notes/114--cte-2023-published-rates.md`
- `KPIS.md`

## Code check

Matches. The metric name is `cte_completion_rate_grade_12` for the KYRC23 percent. `KPIS.md` says the 2023 rate is not the same comparison as later years' published cells in the sense recorded on #112. This pass did not reword `KPIS.md`.

## KDE guidance check

Verdict: possible conflict. Flag only.

The same metric name holds a rate on a base KDE has not defined as the same measure across years. The "not comparable" note is the pipeline's. Not found: a KDE definition that makes the 2023 rate the same measure as 2022 or 2024. The recheck is issue #112.
