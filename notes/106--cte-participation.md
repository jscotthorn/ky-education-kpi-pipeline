# CTE participation counts, and published 2024 rates only

PR #106. Refs #47, #46, and #112.

The 2022 and 2023 files publish counts in every measure column. Those counts had been stored on `cte_participation_rate` and `cte_completion_rate_grade_12` when they were 100 or less, and dropped when they were larger. This change stores each published cell on a count metric. It does not calculate a rate from counts.

`cte_participation_rate` and `cte_completion_rate_grade_12` hold only the percents KYRC24 published. KYRC25 and KYRC26 store the published counts and do not emit either rate. KYRC23's published 2023 rates are loaded in `notes/114--cte-2023-published-rates.md`. The registered KYRC24 file stays the historical copy.

A `*` or `<10` is value NA with `suppressed` Y, including a fully starred demographic. A blank, `--`, or `N/A` is omitted. A published 0 is kept. No output key repeats on year, school, student group, and metric. A star on a duplicate key does not replace a published number.

No shared parser, `config/kde_sources.yaml`, or dependency change. `analysis/config/covariate_catalog.yaml` lists `cte_participation_rate` and has no completion-rate entry. The participation-rate description says it is the KDE-published 2024 rate, can exceed 100, and includes the published 0.0 cells.

## What each year stores

2022 (`cte_by_student_group_2022.csv`) and 2023 (`cte_by_student_group_2023.csv`) use uppercase headers. No measure cell has a decimal, a zero, or a negative. The only non-numeric token is `*`. Where both grade-12 cells are numbers, completers are at or below eligible (2,767 rows in 2022, 2,671 in 2023).

| Header | Stored as |
| --- | --- |
| `TOTAL NUMBER OF STUDENT` | `cte_high_school_enrollment_count` |
| `CTE PARTICIPANTS IN ALL GRADES` | `cte_participant_count` |
| `GRADE 12 CTE ELIGIBLE COMPLETER` | `cte_eligible_completer_count_grade_12` |
| `GRADE 12 CTE COMPLETERS` | `cte_completer_count_grade_12` |

State All Students completion is 13,624 / 43,229 = 31.5% in 2022 and 11,471 / 25,097 = 45.7% in 2023. Those match KDE's published state rates. The rate is not in the file, so it is not stored. Blank demographic rows stay skipped (1,185 in 2022, 1,197 in 2023).

Every district and state enrollment cell in those two files is `*`. That is 2,833 cells in 2022 and 2,817 in 2023, and it is every demographic on those rows, not only All Students. The starring is structural. School enrollment cells are a mix of numbers and stars.

KYRC24 uses the same words in title case, and two headers do not match the cells:

| Header | Kind | Stored as |
| --- | --- | --- |
| `Total Number of Student` | Count. State All Students 143,415 | `cte_participant_count` |
| `CTE Participants in All Grades` | Percent. 4,459 numbers, 0.0 through 400.0. 208 are 0.0. 42 are above 100. 1,156 are `*`, 554 are `--`, 317 are `N/A` | `cte_participation_rate` |
| `Grade 12 CTE Eligible Completer` | Completer count. No decimal. State All Students 17,597. 2,761 numbers and 3,725 stars | `cte_completer_count_grade_12` |
| `Grade 12 CTE Completers` | Percent. 2,757 numbers, 2.8 through 100.0. None above 100 | `cte_completion_rate_grade_12` |

`Total Number of Student` is the participant count, not enrollment. In 2022 and 2023, `TOTAL NUMBER OF STUDENT` is enrollment and `CTE PARTICIPANTS IN ALL GRADES` is the participant count. The KYRC24 state cell is 143,415, which matches the ACTE participant count of 143,414. The next column is the published participation percent (state 68.7). KYRC24 has no separate enrollment column, so `cte_high_school_enrollment_count` is absent for 2024. The 42 participation rates above 100 are kept, from 100.4 to 400.0. Harlan County High School `235025`, American Indian or Alaska Native, is 400.0 on a participant count of 4.

`Grade 12 CTE Eligible Completer` is the same kind of header-versus-content problem. It is the completer count, so `cte_eligible_completer_count_grade_12` has one definition: the 2022 and 2023 eligible count. Evidence from the datasets copy of `KYRC23_CTE_Participation.csv` (md5 `9ac6152de241e6ec5e68f65dc871b13f`), which is the KYRC republish of 2022-23 and is not a registered input:

- The column equals `GRADE 12 CTE COMPLETERS` in `cte_by_student_group_2023.csv` in 2,420 of 2,420 cells where both are numbers. 0 numeric conflicts. State All Students is 11,471, Fayette is 599, and Lafayette `165060` is 94.
- 1,936 further cells are `*` on both sides. 2,420 + 1,936 = 4,356.
- The remaining differences are suppression. 3,281 annual stars are blank in KYRC23. 251 annual numbers are `*` in KYRC23.
- KYRC23 `Grade 12 CTE Completers` is the rate. State All Students is 45.7, which is 11,471 / 25,097.
- KYRC24's column divided by KYRC25 `12th Grade Completers` has median 0.935 on 2,581 rows. Divided by KYRC25 `12th Grade Participants` the median is 0.333 on 2,733 rows. State All Students is 17,597 against 19,096 KYRC25 completers.

The 208 KYRC24 participation rates of 0.0 each sit on a positive participant count, across 24 schools. Participant counts on those cells run from 3 to 504. Pike County Virtual Academy `491020` All Students is 108 participants and 0.0. The cells are kept as published. Refs #112, for the 27 October KDE re-check.

KYRC25 and KYRC26 headers are `All High School CTE Participants`, `All High School Enrollments`, `12th Grade Completers`, and `12th Grade Participants`. Those are counts. Career-center enrollment `N/A` means the students are counted at the home school, so that row has no enrollment count (1,057 cells in KYRC25, 1,309 in KYRC26).

## Completion-rate gate

The gate was to store completers / eligible completers × 100 wherever both counts are published, only if that formula reproduces KDE's published 2024 percent on every row. It does not.

KYRC24 publishes the completer count in `Grade 12 CTE Eligible Completer` and the completion percent in `Grade 12 CTE Completers`. It does not publish an eligible-completer count, so the formula has no denominator in that file. State All Students is 42.3. The published percents are already one decimal (2,757 of 2,757), so rounding is not the gap. The comparisons below used 17,597 as if it were eligible completers. It is the completer count.

| Count source | Rows | Round-to-0.1 matches | State against 42.3 |
| --- | --- | --- | --- |
| Career Pathways `Pathway Completers` summed to the school, divided by the completer-count column | 382 | 0 | 32,223 / 17,597 = 183.1% (max absolute difference 502.8) |
| Student Objectives `12th Grade Completing` (itself a percent) | 382 | 4 | 43 |
| Implied count from that percent times 12th-grade enrollment, divided by the completer-count column | 381 | 0 | 17,144 / 17,597 = 97.4% |
| Completer-count column divided by student-objectives enrollment | 381 | 2 | 17,597 / 39,869 = 44.1% |
| Perkins 5S1 credential attainment | 1 | 0 | 16,257 / 34,636 = 46.9% |

KYRC25 and KYRC26 have no eligible-completer column, so completers / eligible completers cannot be stored for those years either. The calculated completers / grade-12 participants rate is not stored.

## Rows

The file before this change was 57,476 rows, all `suppressed` N. Storing the counts, the stars, and the calculated 2025 and 2026 rates raised it to 172,547 (96,947 N and 75,600 Y). The two rate drops then remove only calculated rates.

`cte_participation_rate` removed: 2025 is 5,469 (4,535 numbers and 934 stars) and 2026 is 6,936 (4,536 numbers and 2,400 stars). 5,469 + 6,936 = 12,405. 172,547 − 12,405 = 160,142. Ninety of those numbers were above 100. Kentucky Virtual Academy `132035` All Students in 2026 was 30,566.7.

`cte_completion_rate_grade_12` removed: 2025 is 6,526 (3,180 numbers and 3,346 stars) and 2026 is 8,245 (3,296 numbers and 4,949 stars). 6,526 + 8,245 = 14,771. 160,142 − 14,771 = 145,371.

The 24 October remap does not add or remove rows. It moves the 6,486 KYRC24 `Grade 12 CTE Eligible Completer` rows from `cte_eligible_completer_count_grade_12` to `cte_completer_count_grade_12`.

| Year | Rows | participation rate | completion rate | participant count | enrollment | eligible | completers | grade-12 participants |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2022 | 32,028 | 0 | 0 | 8,007 | 8,007 | 8,007 | 8,007 | 0 |
| 2023 | 31,552 | 0 | 0 | 7,888 | 7,888 | 7,888 | 7,888 | 0 |
| 2024 | 25,073 | 5,615 | 6,486 | 6,486 | 0 | 0 | 6,486 | 0 |
| 2025 | 25,047 | 0 | 0 | 6,526 | 5,469 | 0 | 6,526 | 6,526 |
| 2026 | 31,671 | 0 | 0 | 8,245 | 6,936 | 0 | 8,245 | 8,245 |

32,028 + 31,552 + 25,073 + 25,047 + 31,671 = 145,371. 5,615 + 6,486 × 3 = 25,073. The 2024 participation rate stays 5,615 rows (4,459 numbers, including the 42 rates above 100 and 208 zeros, and 1,156 stars). The 2024 completion rate stays 6,486 rows (2,757 numbers and 3,729 stars). The 2024 completer count is 6,486 rows (2,761 numbers and 3,725 stars).

## State and Fayette, All Students

District code `165` in 2022 and 2023, `165000` in 2024-2026. State code `999` in 2022 and 2023, `999000` in 2024-2026. A `*` enrollment is suppressed Y. District and state enrollment is `*` for every demographic.

| Year | State | Fayette |
| --- | --- | --- |
| 2022 | participants 167,510; enrollment suppressed; eligible 43,229; completers 13,624 | participants 11,123; enrollment suppressed; eligible 2,763; completers 782 |
| 2023 | participants 123,372; enrollment suppressed; eligible 25,097; completers 11,471 | participants 7,569; enrollment suppressed; eligible 1,519; completers 599 |
| 2024 | participants 143,415; participation rate 68.7; completers 17,597; completion rate 42.3 | participants 8,889; participation rate 64.3; completers 1,053; completion rate 38.5 |
| 2025 | participants 145,369; enrollment 206,733; completers 19,096; grade-12 participants 42,834 | participants 9,658; enrollment 13,652; completers 1,281; grade-12 participants 2,937 |
| 2026 | participants 144,661; enrollment 201,255; completers 19,990; grade-12 participants 44,038 | participants 9,972; enrollment 13,244; completers 1,404; grade-12 participants 2,946 |

The 2022 state participant count of 167,510 is about 1.27 times the Perkins figure of 141,867. The 2023 eligible cell is 25,097, against 43,229 in 2022. Those published cells stay, for the KDE re-check. They are not dropped. The 208 published 0.0 participation rates stay as well. Refs #112.
