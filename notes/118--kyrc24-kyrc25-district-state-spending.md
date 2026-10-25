# 118 - KYRC24 and KYRC25 district and state spending totals

Refs #118. Refs #117. The base is `feature/2025-26-kyrc26` at `1c48f448`. `develop` and `master` are not touched.

## Sources

The wide files `KYRC24_FT_Spending_per_Student.csv` and `KYRC25_FT_Spending_per_Student.csv` are schools only. District and state totals are in five long files per year. Each row has a Category or Metric, then School, District, and State (Statewide on the personnel files).

The historical host returns 404 for all ten names. They are registered on the datasets host, the `kyrc25` URL key, the same way other KYRC24 and KYRC25 datasets files are registered.

- `KYRC2{4,5}_FT_Total_Spending_per_Student_All_Fund_Sources.csv`
- `KYRC2{4,5}_FT_Total_Spending_per_Student_Federal_Funds.csv`
- `KYRC2{4,5}_FT_Total_Spending_per_Student_State_Local_Funds.csv`
- `KYRC2{4,5}_FT_Personnel_Spending_per_Student_Federal_Funds.csv`
- `KYRC2{4,5}_FT_Personnel_Spending_per_Student_State_Local_Funds.csv`

KYRC24 Last-Modified is Thu, 14 May 2026 19:11:29 GMT. KYRC25 Last-Modified is Sat, 18 Jul 2026 21:29:46 GMT. UAT Content-MD5 matches the datasets bytes for all ten files.

## Decisions

A row is a district or the state when School Name is `All Schools` and School Code ends in `000`. The district amount is the District column. District number `999` uses the State or Statewide column. The School column on those rows is `0`, and the state row's District column is `0`. Neither `0` is the published total, so neither is stored. The State column on a district row repeats the statewide figure and is not copied onto the district.

School rows in the long files are not stored. The wide file already has them. A blank level cell is omitted (91 cells). These ten files do not publish a `*` or a real `0` on the district or state amount. A `*` would be value NA with `suppressed` Y. A published 0 would be 0 with `suppressed` N.

Identifiers match the 2020-2023 spending rows. Fayette is school id `165`, district `Fayette County`, school `---District Total---`. The state is school id `999`, district `State`, school `---State Total---`. `165000` and `999000` are not written. Issue #61 is not attempted. Other district names stay as KDE published them for that year.

The personnel files publish the non-personnel category, so all seven metrics are published cells. Nothing is summed. For Fayette in 2024, federal 1,655 plus state/local 17,788 is 19,443. The published all-funds cell is 22,322, and that is what is stored.

Commas are removed on object and on StringDtype. pandas 2.3.3 and pandas 3.1.0 write the same 64,015 rows apart from `last_updated`.

## Row counts

Level is `---District Total---` for a district, `---State Total---` for the state, and a school otherwise. 2020 already has district and state spending rows. 2024 and 2025 did not.

| Year | District before | District after | School before | School after | State before | State after |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2019 | 0 | 0 | 8,092 | 8,092 | 0 | 0 |
| 2020 | 1,197 | 1,197 | 8,099 | 8,099 | 7 | 7 |
| 2021 | 1,197 | 1,197 | 8,092 | 8,092 | 7 | 7 |
| 2022 | 1,197 | 1,197 | 8,064 | 8,064 | 7 | 7 |
| 2023 | 1,197 | 1,197 | 8,057 | 8,057 | 7 | 7 |
| 2024 | 0 | 1,197 | 8,337 | 8,337 | 0 | 7 |
| 2025 | 0 | 1,197 | 8,050 | 8,050 | 0 | 7 |

2019-2023, and the 2024 and 2025 school rows, match the before file on every cell except `last_updated`. Total: 61,607 before, 64,015 after, added 2,408, removed 0. Each new year is 171 districts times 7 metrics (1,197) plus the state times 7 metrics. No duplicate `year` / `school_id` / `metric` / `student_group` key. Every new cell is `suppressed` N.

## Fayette (`165`) and the state (`999`)

| Year | School id | Federal personnel | Federal non-personnel | Federal total | State/local personnel | State/local non-personnel | State/local total | All funds |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2024 | 165 | 666 | 989 | 1,655 | 14,881 | 2,907 | 17,788 | 22,322 |
| 2024 | 999 | 1,153 | 795 | 1,948 | 10,930 | 2,612 | 13,542 | 19,020 |
| 2025 | 165 | 623 | 290 | 913 | 15,550 | 2,652 | 18,202 | 22,974 |
| 2025 | 999 | 935 | 480 | 1,415 | 11,787 | 2,843 | 14,630 | 19,973 |

Those 28 cells match the source files. Henry Clay `165039` all-funds stays 20,164 in 2024 and 26,072 in 2025. Adair County's district id stays `001`.
