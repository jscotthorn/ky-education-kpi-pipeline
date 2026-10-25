# 0048 CRDC KPI year is the collection year

Status: accepted, code differs (follow-up open)

## Question

A CRDC file has both a report-card school year and a collected school year. Which one is the KPI year?

## What KDE changed

CRDC is a biennial federal collection. The files carry both year fields. No KDE sentence was found that says which field a longitudinal file should use. The quality-assurance worksheet and the glossary were not re-fetched.

## Options considered

Keep the split the two modules already had, or use the collection year in every module. Suspensions can move in this change or in a later pull request.

## Decision

25 October 2026. Use the CRDC collection year as the KPI year everywhere. Move suspensions over in a later pull request. Scott confirmed this choice on 2026-10-25 at 12:38 PM ET.

Primary source: https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/122#issuecomment-6400284053 "Decision (Scott, 2026-10-25 12:05 PM ET): Record 0048. Use the CRDC collection year as the KPI year everywhere; move suspensions over in a later PR (this issue)."

Background, source type chat answer: "Use the CRDC collection year as the KPI year everywhere; move suspensions over in a later PR."

- Advanced coursework, pull request #79, already uses the collection year. KPI year 2021 is the 2020-21 collection, from the KYRC23 overview. KPI year 2022 is the 2021-22 collection, from the KYRC26 overview. `etl/advanced_coursework.py` and `README.md`.
- Out-of-school suspension, pull request #51, still uses the report-card school year. KPI year 2026 is the 2021-22 collection. KPI year 2024 is the 2020-21 collection. There is no state row. `etl/out_of_school_suspension.py` and `README.md`. That move is tracked on issue #122. It is not done on this branch.

The #51 rule "KPI year = report-card year (2026 = CRDC 2021-22)" is superseded by this record once #122 lands. Until then the suspension code does not match. This log does not change any code.

## Who decided and how we know

Primary source: https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/122#issuecomment-6400284053 posted on the `jscotthorn` account through the Cursor GitHub App. Scott confirmed the choice on 2026-10-25 at 12:38 PM ET. The comment is quoted in the decision.

Background, source type chat answer: the sentence quoted above, from the ECC manager bot's chat. The comment dates that choice 12:05 PM ET.

Issue #122 tracks the suspension follow-up. Its body states the same rule: "Decision (Scott, 2026-10-25): every module uses the CRDC collection year as the KPI year." https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/122

## Why

Scott's sentence is the choice. It does not say why the collection year is the KPI year. Reason for preferring that field: not documented beyond the sentence. The later pull request is so suspensions move without changing them in this decision.

## Links

- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/51
- Pull request https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/79
- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/122
- Confirming comment https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/122#issuecomment-6400284053
- `README.md` (CRDC year sentences)
- `etl/out_of_school_suspension.py`
- `etl/advanced_coursework.py`
- `config/kde_sources.yaml` (`advanced_coursework` overview files KYRC23 and KYRC26)
- [coverage-gaps.md](coverage-gaps.md) (AP participation)

## Code check

Partial match. Advanced coursework already uses the collection year. Suspension still uses the report-card year. That mismatch stays until issue #122 lands. This record does not change either module.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

QA and the glossary, not re-fetched, say CRDC is biennial and that the files have both a school year and a collected school year. Not found: a KDE statement that names which field to store. https://www.education.ky.gov/districts/tech/sis/Documents/SRC_QA_Worksheet.pdf and https://reportcard.kyschools.us/glossary
