# Spending per student thousands separators

PR #117. Refs #110. Same class of bug as the financial summary comma fix. `develop` and `master` are not touched. The base is `feature/2025-26-kyrc26` at `cfeb6aa964738bd5b67697389242db4a90675af6`.

## What was wrong

`clean_spending_values` removed thousands separators only when `dtype == object`. `BaseETL._read_data_file` reads with `dtype=str`. On pandas 3.0.6 that dtype is StringDtype, so a comma in `20,164` stayed in the cell and `to_numeric` turned it into missing. pandas 2.3.3 still returns object, so the same files kept those amounts.

The cells are in `KYRC24_FT_Spending_per_Student.csv` (KPI year 2024, school year `20232024`) and `KYRC25_FT_Spending_per_Student.csv` (KPI year 2025, school year `20242025`). KYRC24 has 1,488 source rows and 6,918 comma cells in the seven spending columns, plus 2,079 blanks (297 schools with every spending cell blank) and 22 published zeros. KYRC25 has 1,150 source rows and 6,061 comma cells, and no blanks, zeros, or stars. `spending_per_student_2020.csv` through `spending_per_student_2023.csv` and `SPENDING_PER_STUDENT_18-19.xlsx` do not put thousands separators in the measure columns. Their row counts did not change.

No registered spending file publishes a `*`.

## Parser

Commas are removed on object and on StringDtype. `20,164` becomes 20164. A `*` is recorded before missing-value replacement and stored as value NA with `suppressed` Y. A blank is not stored. A published 0 stays 0. No spending amount is calculated from the other columns.

`BaseETL.standardize_missing_values` had the same `dtype == object` check. It now rewrites object and StringDtype. A column whose name starts with `_` is left alone. Modules copy the source text into `_raw_` / `__raw_` before this method runs, and those copies have to keep a published `*`, `N/A`, or `<10`.

## Row counts

Before is pandas 3.0.6 at `cfeb6aa`, which dropped the comma values. After is the same files on pandas 3.0.6 and pandas 2.3.3 (61,607 rows, no value or suppressed differences). pandas 2.3.3 before this change already had the comma values: 61,607 rows, and every shared key matches the after file.

Columns, in order: non-personnel federal, non-personnel state/local, personnel federal, personnel state/local, all funds, total federal, total state/local.

| Year | Before (pandas 3.0.6) | After, every metric |
| --- | --- | --- |
| 2019 | 1,156 for every metric | 1,156 |
| 2020 | 1,329 for every metric | 1,329 |
| 2021 | 1,328 for every metric | 1,328 |
| 2022 | 1,324 for every metric | 1,324 |
| 2023 | 1,323 for every metric | 1,323 |
| 2024 | 795 / 23 / 458 / 3 / 3 / 134 / 3 | 1,191 |
| 2025 | 996 / 10 / 619 / 0 / 0 / 363 / 1 | 1,150 |

2024 added 6,918 rows and 2025 added 6,061. That is one KPI row per comma cell. 2019-2023 match the pandas 3 before file on every value. Shared keys between the pandas 3 before file (48,628 rows) and the after file: 48,628, removed 0, value diffs 0, suppressed diffs 0. Total: 48,628 before, 61,607 after, added 12,979, removed 0.

KYRC24's 297 all-blank schools stay omitted, so 2024 is 1,191 schools rather than 1,488 source rows. KYRC25 stores every source row. No duplicate `year` / `school_id` / `metric` / `student_group` key. The transform does not read the output back. The real file has 0 suppressed Y and 0 missing values. The 22 published zeros stay 0 with `suppressed` N.

Peak resident memory for this transform: pandas 3.0.6 `99056` KB, pandas 2.3.3 `103816` KB. The full 31-module run peaked at `1552444` KB on pandas 3.0.6 and `1896352` KB on pandas 2.3.3, both in `chronic_absenteeism`. All of those are under 4,194,304 KB.

## Fayette (`165000`) and the state (`999000`)

Neither id is a row in any year, before or after. KYRC24 and KYRC25 publish schools only. 2020-2023 district totals use school code `165` for Fayette County and `999` for the state. 2019 uses the 9-digit state school id (Henry Clay is `034165039`) and has no district total. Those `165` and `999` amounts are unchanged.

Fayette all-funds school rows: 54 in 2019, 56 in 2020-2023 (55 schools plus the `165` district total), 55 in 2024, and 55 in 2025. Before the fix, pandas 3.0.6 had 0 Fayette all-funds rows in 2024 and 0 in 2025.

| Year | School id | Federal personnel | Federal non-personnel | Federal total | State/local personnel | State/local non-personnel | State/local total | All funds |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2020 | 165 | 516 | 144 | 659 | 11,484 | 1,900 | 13,384 | 16,200 |
| 2020 | 999 | 712 | 247 | 959 | 9,666 | 1,736 | 11,402 | 14,496 |
| 2021 | 165 | 721 | 450 | 1,171 | 11,756 | 1,468 | 13,223 | 17,307 |
| 2021 | 999 | 933 | 600 | 1,533 | 9,683 | 1,531 | 11,213 | 15,067 |
| 2022 | 165 | 1,274 | 466 | 1,740 | 12,324 | 2,254 | 14,578 | 18,360 |
| 2022 | 999 | 1,517 | 816 | 2,333 | 9,995 | 1,950 | 11,945 | 16,422 |
| 2023 | 165 | 981 | 754 | 1,735 | 14,436 | 2,794 | 17,231 | 22,007 |
| 2023 | 999 | 1,330 | 751 | 2,081 | 11,102 | 2,441 | 13,544 | 18,522 |

Henry Clay High School, school id `165039`, all funds, is the published comma value that pandas 3 dropped. 2024 is 20,164 (`"20,164"` in `KYRC24_FT_Spending_per_Student.csv`). 2025 is 26,072 (`"26,072"` in `KYRC25_FT_Spending_per_Student.csv`). Both are stored with `suppressed` N.

## Other modules

`BaseETL.standardize_missing_values` now also rewrites StringDtype. On pandas 3.0.6 the other 30 KPI csvs match the base run with `last_updated` ignored: same row counts, same values. The demographic reports match byte for byte. `_summary.json` differs only in `peak_kb`. The spending row count there is 48,628 before and 61,607 after.

Two unit tests that build a DataFrame containing `*` and a blank now pass on pandas 3, because those columns are StringDtype and the old check skipped them:

- `tests/test_graduation_rates.py::TestGraduationRatesETL::test_standardize_missing_values`
- `tests/test_postsecondary_readiness.py::TestPostsecondaryReadinessETL::test_standardize_missing_values`

`graduation_rates.csv` and `postsecondary_readiness.csv` did not change.

Pandas 3.0.6 head versus pandas 2.3.3 head, compared as a multiset of rows with `last_updated` ignored: `spending_per_student.csv` is identical (61,607 rows). The other 24 KPI files that already matched across versions still match. Six files already differed on the base, and the head still differs in the same rows: `county_number` is blank on pandas 3.0.6 and the text `nan` on pandas 2.3.3, on state rows only. Counts of those rows: graduation 1,454, student enrollment 2,727, secondary enrollment 1,307, novice teachers 200, teacher experience 8, student-teacher ratio 7. Head pandas 2.3.3 matches base pandas 2.3.3 on all six (0 row diffs). Head pandas 3.0.6 matches base pandas 3.0.6 on all six. This pull request does not change those cells.

## Tests

Full `tests/` on this environment with `data/raw` absent, so the end-to-end cases that need a raw file stay skipped. That is the same skip set as the base run.

| | Failed | Passed | Skipped |
| --- | ---: | ---: | ---: |
| pandas 3.0.6 base `cfeb6aa` | 15 | 687 | 109 |
| pandas 3.0.6 head | 10 | 694 | 109 |
| pandas 2.3.3 base `cfeb6aa` | 7 | 695 | 109 |
| pandas 2.3.3 head | 7 | 697 | 109 |

The five pandas 3 failures that now pass are the three spending tests (`test_clean_spending_values`, `test_transform_2024_format`, `test_mixed_year_formats`) plus `test_graduation_rates.py::TestGraduationRatesETL::test_standardize_missing_values` and `test_postsecondary_readiness.py::TestPostsecondaryReadinessETL::test_standardize_missing_values`. The two extra passes on both versions are the new comma tests. No failure was added. The ten pandas 3 failures and the seven pandas 2 failures are on the list in `notes/105--kyrc26-completeness-audit.md`.

## Fixture

`tests/fixtures/spending_per_student/comma_formatted_kyrc.csv` copies four KYRC24 rows byte for byte: Henry Clay (`"20,164"`), Raceland-Worthington Middle (published federal non-personnel 0), Hopkins County Career and Technology Center (0 in every spending column), and UK Early Childhood Lab (every spending cell blank, not stored). The `*` row is labelled synthetic, school code `SYNSTAR`. It is not a KDE school. A `*` is value NA with `suppressed` Y. The other cells on that row stay as written.

On pandas 3.0.6 the base module fails both new tests. `clean_spending_values` turns StringDtype `"20,164"` into missing, and the assertion raises `TypeError: boolean value of NA is ambiguous`. The fixture transform stores no Henry Clay all-funds row.

`KPIS.md` now lists years 2019-2025, names `SPENDING_PER_STUDENT_18-19.xlsx`, and says a thousands separator is a published dollar amount. `README.md` says KYRC24 and KYRC25 are schools only, and that the 2020-2023 totals are `165` and `999`.
