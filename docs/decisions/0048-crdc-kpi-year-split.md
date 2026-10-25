# 0048 CRDC modules use different year fields

Status: unconfirmed

## Question

A CRDC file has both a report-card school year and a collected school year. Which one is the KPI year?

## What KDE changed

CRDC is a biennial federal collection. The files carry both year fields. No KDE sentence was found that says which field a longitudinal file should use. The quality-assurance worksheet and the glossary were not re-fetched.

## Options considered

No Scott choice between the two fields was found. The two modules already differ. This record flags that split. It does not choose a year.

## Decision

No decision found. The repo documents the split as implemented behavior.

- Out-of-school suspension uses the report-card school year. KPI year 2026 is the 2021-22 collection. KPI year 2024 is the 2020-21 collection. There is no state row. `etl/out_of_school_suspension.py` and `README.md`.
- Advanced coursework CRDC uses the collection year. KPI year 2021 is the 2020-21 collection, from the KYRC23 overview. KPI year 2022 is the 2021-22 collection, from the KYRC26 overview. `etl/advanced_coursework.py` and `README.md`.

## Who decided and how we know

Nobody's choice was found. `README.md` states both rules. The module docstrings state them. There is no "Decision (Scott, …)" comment that says the two modules should use different fields.

## Why

Reason not documented. The cause of the split is not in an issue, a pull request, a note, or a commit that this pass found.

## Links

- `README.md` (CRDC year sentences)
- `etl/out_of_school_suspension.py`
- `etl/advanced_coursework.py`
- `config/kde_sources.yaml` (`advanced_coursework` overview files KYRC23 and KYRC26)
- [coverage-gaps.md](coverage-gaps.md) (AP participation)

## Code check

The code matches the split the README describes. That is the inconsistency. This record does not change either module.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

QA and the glossary, not re-fetched, say CRDC is biennial and that the files have both a school year and a collected school year. Not found: a KDE statement that names which field to store. https://www.education.ky.gov/districts/tech/sis/Documents/SRC_QA_Worksheet.pdf and https://reportcard.kyschools.us/glossary
