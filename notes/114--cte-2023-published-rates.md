# Published 2023 CTE participation and completion rates

PR #114. Refs #47 and #112. Follow-up to PR #106.

`KYRC23_CTE_Participation.csv` is the KYRC republish of 2022-23. The datasets copy used here is md5 `9ac6152de241e6ec5e68f65dc871b13f` (7,888 data rows, school year `20222023`, KPI year 2023). It is registered on the datasets host (`url: kyrc25`). The file uses the same title-case headers as KYRC24. Two of those headers do not match the cells, the same way they do not match in KYRC24.

This change stores only the two published rates. It does not calculate a rate. It does not store the KYRC23 count columns. The 2023 counts remain the rows from `cte_by_student_group_2023.csv`.

| KYRC23 header | What the cells are | Stored as |
| --- | --- | --- |
| `Total Number of Student` | Participant count. State All Students `123,372` | Not stored. The annual file already has `cte_participant_count` |
| `CTE Participants in All Grades` | Participation percent. State All Students 59.1 | `cte_participation_rate` |
| `Grade 12 CTE Eligible Completer` | Completer count. State All Students `11,471` | Not stored. The annual file already has `cte_completer_count_grade_12` |
| `Grade 12 CTE Completers` | Completion percent. State All Students 45.7 | `cte_completion_rate_grade_12` |

## Counts stay on the annual file

The annual file uses three-digit codes for a district and the state (`165`, `999`). KYRC23 uses those digits plus `000` (`165000`, `999000`). School codes are already six digits on both files. Compared on that key plus demographic:

| Count | Both numeric | Equal | Numeric conflicts |
| --- | --- | --- | --- |
| Participants (`Total Number of Student` vs `CTE PARTICIPANTS IN ALL GRADES`) | 5,150 | 5,150 | 0 |
| Completers (`Grade 12 CTE Eligible Completer` vs `GRADE 12 CTE COMPLETERS`) | 2,420 | 2,420 | 0 |

The review's 2,420 of 2,420 completer matches is the second row. A numeric conflict would be logged and the annual number kept. There are none on this copy. The pipeline logs `0 numeric conflicts` when both files are present.

The other differences are suppression, not numbers. They do not change the annual row.

Completer column, KYRC23 cell against the annual cell:

| KYRC23 | Annual | Rows |
| --- | --- | --- |
| blank | `*` | 3,281 |
| number | number | 2,420 |
| `*` | `*` | 1,936 |
| `*` | number | 251 |

Participant column:

| KYRC23 | Annual | Rows |
| --- | --- | --- |
| number | number | 5,150 |
| blank | `*` | 1,774 |
| `*` | `*` | 947 |
| `*` | number | 17 |

3,281 + 2,420 + 1,936 + 251 = 7,888. The 251 annual numbers that are `*` in KYRC23, and the 3,281 annual stars that are blank in KYRC23, stay as the annual file stored them.

## Published rates

Nothing in this module divides a count by another count. The stored percent is the cell KDE printed.

Participation (`CTE Participants in All Grades`): 5,806 numbers, 964 `*`, 776 `--`, 342 `N/A`. Numbers run from 0.0 through 400.0. 1,980 are 0.0. 21 are above 100 (102.4 through 400.0). North Hardin High School `231075`, Migrant, is 400.0 on a participant count of 4.

Completion (`Grade 12 CTE Completers`): 4,962 numbers and 2,926 `*`. No blanks, no `--`, no `N/A`. Numbers run from 0.0 through 100.0. 2,542 are 0.0. None of those 0.0 cells has a numeric completer count. Fayette County Learning Center `165898` All Students is a blank completer count and a completion rate of 0.0.

Stored rows from this file: 5,806 + 964 = 6,770 participation rates, and 4,962 + 2,926 = 7,888 completion rates. 6,770 + 7,888 = 14,658. All of them are year 2023. The run before this change was 145,371 rows. 145,371 + 14,658 = 160,029.

A `*` or `<10` is value NA with `suppressed` Y. This file has no `<10>` cell. A blank, `--`, or `N/A` is not stored. That is the KYRC24 rule. Storing `--` would also add the 554 KYRC24 participation cells that are `--`, and this change does not add 2024 rows. The 776 KYRC23 participation cells of `--` are omitted, and the 342 `N/A` cells are omitted. A published 0 is kept. No output key repeats. A later star on the same key does not replace a published number.

## 0.0 participation rates

Refs #112. These cells are kept as published.

Of the 1,980 participation rates of 0.0:

- 491 sit on a positive participant count, across 54 schools. Adair County High School `001010` All Students is 377 participants and 0.0. The annual enrollment on that row is 865, so the cell is not participants divided by enrollment.
- 1,489 sit on a blank participant count.
- None sit on a starred participant count.

Where the annual participant count and the annual enrollment are both numbers (2,474 school rows; district and state enrollment in the annual file is `*`), participants divided by enrollment, rounded half-up to 0.1, matches the published participation rate on 1,854 rows and does not match on 620. 481 of those 620 are 0.0. 2,474 = 1,854 + 620. The 481 are a subset of the 491 zeros on a positive participant count. The other 10 have a starred enrollment, so that division is not defined. The published percent is what is stored, including the 139 non-zero rates in the 620 that do not match.

The same check on the completion percent, completers divided by eligible completers where both annual counts are numbers (2,420 rows), matches on 2,406 rows. The other 14 differ by 0.1. Those 14 published percents are stored as printed. The calculated percent is not stored.

## State and Fayette

District number `165`. KYRC school code `165000` for the district and `999000` for the state. The annual count rows stay on school id `165` and `999`.

| Row | Participation | Completion | Annual participants | Annual completers |
| --- | --- | --- | --- | --- |
| State All Students | 59.1 | 45.7 | 123,372 | 11,471 |
| Fayette County All Students | 55.7 | 39.4 | 7,569 | 599 |
| Lafayette High School `165060` All Students | 50.2 | 38.2 | 1,300 | 94 |

Lafayette's annual enrollment is 2,592 and its annual eligible count is 246. 1,300 / 2,592 rounded half-up to 0.1 is 50.2. 94 / 246 rounded half-up to 0.1 is 38.2. Those happen to equal the published percents. The pipeline stores the published cells.
