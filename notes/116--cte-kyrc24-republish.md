# KYRC24 CTE participation republish

PR #116. Refs #113, #112, and #47. Follow-up to PR #106.

Scott decided on 24 October 2026, 3:24 PM ET, to load the May 2026 KYRC24 CTE republish from the live `datasets` container. The registered file is `KYRC24_CTE_Participation.csv` on that host (`url: kyrc25`). Last-Modified Thu, 14 May 2026 19:11:29 GMT. md5 `86677fc9b17a32b62cb533f4352ed63e`. UAT is the same bytes (Content-MD5 `hmd/ybF6MrYstTP0NS7WPg==`). The historical copy is not loaded. That copy is 845,823 bytes, Last-Modified Thu, 29 May 2025 23:02:15 GMT, md5 `0b0d30c79ff6c5f3c039aafd5ceb8081`.

The cell rules are the ones KYRC24 and KYRC23 already use. A `*` or `<10` is value NA with `suppressed` Y. A blank, `--`, or `N/A` is omitted. A published 0.0 is kept. No rate is calculated. The headers are unchanged, and no published number changes, so the column meanings are unchanged. `Total Number of Student` is still the participant count. `CTE Participants in All Grades` is still the participation percent. `Grade 12 CTE Eligible Completer` is still the completer count. `Grade 12 CTE Completers` is still the completion percent.

## Star cells

Both copies have 6,486 data rows and the same school-code and demographic keys. 0 duplicate keys. 0 numeric cells differ.

#113 said the republish replaced 3,524 `*` cells with 0.0, blanks, `--`, or `N/A`. The verified count is 3,544. The difference is 20, and those 20 are `*` to `--` on the participation rate. 3,544 − 20 = 3,524. Historical stars are 9,766. Republish stars are 6,222. 9,766 − 6,222 = 3,544.

| Column | May 2025 | May 2026 | Cells |
| --- | --- | --- | --- |
| `Grade 12 CTE Eligible Completer` | `*` | blank | 1,944 |
| `Grade 12 CTE Completers` | `*` | 0.0 | 1,230 |
| `Total Number of Student` | `*` | blank | 185 |
| `CTE Participants in All Grades` | `*` | 0.0 | 146 |
| `CTE Participants in All Grades` | `*` | `--` | 20 |
| `CTE Participants in All Grades` | `*` | `N/A` | 19 |

1,944 + 1,230 + 185 + 146 + 20 + 19 = 3,544.

Every one of the 1,230 completion 0.0 cells has a completer count that goes from `*` to blank on the same row. The other 714 completer counts that go from `*` to blank keep a `*` completion rate. 1,230 + 714 = 1,944.

Every one of the 146 participation 0.0 cells has a participant count that goes from `*` to blank. The other participant-count blanks are the 20 `--` rows and the 19 `N/A` rows. 146 + 20 + 19 = 185.

Of the 1,230 completion 0.0 cells:

| Participant count | Participation rate | Rows |
| --- | --- | --- |
| Positive, unchanged | Positive, unchanged | 918 |
| `*` to blank | `*` to 0.0 | 116 |
| Positive, unchanged | `--`, unchanged | 72 |
| Positive, unchanged | `N/A`, unchanged | 46 |
| Positive, unchanged | 0.0, unchanged | 42 |
| `*` to blank | `*` to `N/A` | 19 |
| `*` to blank | `*` to `--` | 17 |

918 + 116 + 72 + 46 + 42 + 19 + 17 = 1,230. The 42 are part of the 208 participation rates of 0.0 that already sat on a positive participant count. Augusta Independent `013011` All Students is 63 participants, participation 0.0, a blank completer count, and completion 0.0. Adair district `001000` Asian is 3 participants, participation 60.0, a blank completer count, and completion 0.0.

The 208 participation rates of 0.0 on a positive participant count are unchanged, across the same 24 schools. Pike County Virtual Academy `491020` All Students is 108 participants and 0.0. The 42 participation rates above 100 are unchanged, from 100.4 to 400.0. Harlan County High School `235025`, American Indian or Alaska Native, is 400.0 on 4 participants. Those cells stay. Refs #112.

The 146 new participation 0.0 cells do not sit on a positive count. Allen County `005000`, Native Hawaiian or Pacific Islander, is a blank participant count, participation 0.0, a blank completer count, and completion 0.0. The May 2025 row was `*` in every measure cell.

Fayette County Learning Center `165898` All Students stays 26 participants and participation 0.0. The completer count goes from `*` to blank, and the completion rate goes from `*` to 0.0. Bryan Station `165170`, Native Hawaiian or Pacific Islander, keeps `*` on the participant count, the participation rate, and the completion rate. The completer count goes from `*` to blank and is omitted.

## GREC

#113 said the republish fills co-op GREC on 349 rows. That count is exact. CO-OP goes from blank to `GREC`. No other identity column changes. CO-OP code stays 909. 276 of the 349 rows have no measure change. 73 also have a measure change. 276 + 73 = 349.

| District | Codes | Rows |
| --- | --- | --- |
| Jefferson County `275` | 23, including district `275000` | 333 |
| Kentucky School for the Blind `602` | `602000`, `602176` | 16 |

333 + 16 = 349. Butler Traditional High `275045`, African American, keeps 454 participants, participation 55.8, 35 completers, and completion 19.4. CO-OP becomes `GREC`.

KYRC26 labels Jefferson `GLEC`. #110 records a GLEC versus GREC difference on the datasets financial summary. This file prints `GREC`. The stored co-op is that token. No crosswalk is applied.

## Rows

The run before this change was 160,029 rows. 2024 drops by the 2,168 omitted cells. 160,029 − 2,168 = 157,861. The 1,376 cells that go from a star to 0.0 stay in the file and change from `suppressed` Y to N.

| Year | Rows | participation rate | completion rate | participant count | enrollment | eligible | completers | grade-12 participants |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2022 | 32,028 | 0 | 0 | 8,007 | 8,007 | 8,007 | 8,007 | 0 |
| 2023 | 46,210 | 6,770 | 7,888 | 7,888 | 7,888 | 7,888 | 7,888 | 0 |
| 2024 | 22,905 | 5,576 | 6,486 | 6,301 | 0 | 0 | 4,542 | 0 |
| 2025 | 25,047 | 0 | 0 | 6,526 | 5,469 | 0 | 6,526 | 6,526 |
| 2026 | 31,671 | 0 | 0 | 8,245 | 6,936 | 0 | 8,245 | 8,245 |

32,028 + 46,210 + 22,905 + 25,047 + 31,671 = 157,861. 2022, 2023, 2025, and 2026 are the rows from PR #114. Only 2024 changes.

2024 participation rate: 5,576 rows (4,605 numbers, including the 42 rates above 100 and 354 zeros, and 971 stars). 4,605 + 971 = 5,576. 354 = 208 + 146. 2024 completion rate: 6,486 rows (3,987 numbers, including 1,230 zeros, and 2,499 stars). 3,987 + 2,499 = 6,486. 2024 participant count: 6,301 rows (5,330 numbers and 971 stars). 5,330 + 971 = 6,301. 2024 completer count: 4,542 rows (2,761 numbers and 1,781 stars). 2,761 + 1,781 = 4,542. 5,576 + 6,486 + 6,301 + 4,542 = 22,905.

## State and Fayette, All Students, 2024

These four cells are the same in both copies, and the stored rows match.

| | State `999000` | Fayette `165000` |
| --- | --- | --- |
| Participants | 143,415 | 8,889 |
| Participation rate | 68.7 | 64.3 |
| Completers | 17,597 | 1,053 |
| Completion rate | 42.3 | 38.5 |
| Co-op | blank | `CKEC` |

## Output diff against the historical file

Keys are `school_id`, `year`, `student_group`, and `metric`. Base output is 160,029 rows. Head output is 157,861. Duplicate keys: 0 on both. Added keys: 0. Shared keys: 157,861.

Removed keys, all year 2024, all `suppressed` Y:

| Metric | Rows |
| --- | --- |
| `cte_completer_count_grade_12` | 1,944 |
| `cte_participant_count` | 185 |
| `cte_participation_rate` | 39 |

1,944 + 185 + 39 = 2,168.

Changed keys, all year 2024. Each one goes from `suppressed` Y with an empty value to `suppressed` N with 0.0:

| Metric | Rows |
| --- | --- |
| `cte_completion_rate_grade_12` | 1,230 |
| `cte_participation_rate` | 146 |

1,230 + 146 = 1,376.

`co_op` changes from blank to `GREC` on 1,305 KPI rows, all 2024. 1,241 of those have the same value and the same suppressed flag. 64 also move from suppressed Y to 0.0. 1,241 + 64 = 1,305. No other column changes. Years other than 2024 have 0 added, 0 removed, and 0 changed rows.

Suppressed-Y rows go from 67,861 to 64,317. 2,168 removed plus 1,376 flipped to N is 3,544. 67,861 − 3,544 = 64,317. Every suppressed-Y value is empty. Every suppressed-N value is present. KYRC23 still logs `0 numeric conflicts` with `cte_by_student_group_2023.csv`.

The head transform's max RSS was 138,188 KB on pandas 3.0.6 and 144,440 KB on pandas 2.3.3. The two outputs match apart from `last_updated`.

The 1,376 new 0.0 rates are kept as published. Refs #112. They were stars in the May 2025 file, and the paired count is blank. The pipeline does not treat them as a new kind of suppression.
