# 120 Postsecondary enrollment star cells

Issue #44. The transition files have no Suppressed column. Before this change a `*` was dropped, so 2021-2026 had no `suppressed` Y row.

The rule is the in-file cohort count. A `*` measure cell is value NA with `suppressed` Y when `Total In Group` on that row is a published number greater than 0. A cohort of 0 or a blank cohort leaves the star unstored. A row whose cohort is itself `*` stays unclassified: no new row, and the row is listed in `notes/flags/A-flagged-keys.csv` with reason `starred cohort count` and label `kde-recheck`. A published 0 is kept. A blank cell is omitted. `**` and `<10` are not `*` and stay omitted. No new metric. The graduates file is not read. Record DNNNN. [0004](../docs/decisions/0004-postsecondary-star-cells.md) keeps its status.

## What the files actually contain

Checked against the six registered files. MD5s match `notes/104`:

| File | Bytes | MD5 |
| --- | --- | --- |
| `KYRC26_ADLF_Transition_to_In_State_Postsecondary_Education.csv` | 887,543 | `ba19e9d0c848fbc06715a005727d3318` |
| `KYRC25_ADLF_Transition_to_In_State_Postsecondary_Education.csv` | 1,311,339 | `a0646ff1b21da0d6bb3b8a9f1ec97c0e` |
| `KYRC24_ADLF_Transition_to_In_State_Postsecondary_Education.csv` | 847,759 | `55af3896a4342a0521fcca43fc9a1d3b` |
| `transition_in_state_postsecondary_education_2023.csv` | 863,283 | `41ae848e2f82a9922bf4adf1b3582ae9` |
| `transition_in_state_postsecondary_education_2022.csv` | 834,976 | `6f99eda044316536d5eb12d8abc4603a` |
| `transition_in_state_postsecondary_education_2021.csv` | 831,315 | `61b505fcff3a7d9f8dfe16ebf297345e` |

No registered year has a cohort of 0 or a blank cohort cell. No measure cell is blank. The padding branch (cohort 0 or blank) therefore adds no row in 2021-2026.

Collins Lane `181025` (Franklin County, KYRC25) is the group issue #44 calls a padding star. All 18 demographic rows have `*` in the cohort cell and in every measure cell. Under the in-file rule those rows are unclassified, not confirmed padding. They emit no KPI row. All Students is in the flagged list.

KYRC25 is the year that still includes fully starred groups. KYRC24 and KYRC26 omit many of those groups. A starred cohort in every year is the same unclassified case.

| Year | Starred cohort rows | Real `*` cells (cohort > 0) |
| --- | ---: | ---: |
| 2021 | 1,260 | 7,140 |
| 2022 | 3,125 | 5,434 |
| 2023 | 3,064 | 6,029 |
| 2024 | 3,240 | 11,972 |
| 2025 | 6,375 | 12,412 |
| 2026 | 2,827 | 15,138 |

2024 has 27 starred-cohort rows that also publish a number, and 2025 has 17. The published number stays. The stars on those rows are not stored. The cohort `*` is the flagged key.

On a cohort greater than 0, the non-numeric tokens are:

- 2021 counts: `*`. 2021 rates: `<10` (1,900 on each rate column). `<10` stays omitted.
- 2022 and 2023 counts: `*`. Rates: `**`. `**` stays omitted.
- 2024-2026 counts and rates: `*`, stored when the cohort is greater than 0.

2021 publishes 576 rates of 0 (472 private, 58 public, 46 total). All 576 stay 0 with `suppressed` N.

## Output change

Compared with a run of the base commit on the same six files, ignoring `last_updated`. Keys are `(district, school_id, school_name, year, student_group, metric)`.

| Year | Rows before | Rows after | Added | Removed | Value changed |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2021 | 18,737 | 25,877 | 7,140 | 0 | 0 |
| 2022 | 7,781 | 13,215 | 5,434 | 0 | 0 |
| 2023 | 9,509 | 15,538 | 6,029 | 0 | 0 |
| 2024 | 9,625 | 21,597 | 11,972 | 0 | 0 |
| 2025 | 9,679 | 22,091 | 12,412 | 0 | 0 |
| 2026 | 10,657 | 25,795 | 15,138 | 0 | 0 |
| Total | 65,988 | 124,113 | 58,125 | 0 | 0 |

Every added row is `suppressed` Y with an empty value. Every previous key is unchanged, including `suppressed` N and the other columns. Duplicate keys in this file: 0 before, 0 after.

2021-2023 are not byte-identical. Each of those years has real stars. The added rows are the count stars. The `<10` and `**` rate cells stay absent, so 2021-2023 gain no rate rows.

## Fayette dashboard slice

`district == 'Fayette County'`, year ≥ 2021, the three dashboard metrics `postsecondary_enrollment_total_in_cohort`, `postsecondary_enrollment_total_ky_college_count`, and `postsecondary_enrollment_total_ky_college_rate`.

Before: 1,375 rows, all `suppressed` N. After: those 1,375 rows are unchanged, plus 114 new Y rows. The cohort metric gains no Y row, because a starred cohort is not stored.

| Year | New Y, total count | New Y, total rate |
| --- | ---: | ---: |
| 2021 | 16 | 0 |
| 2022 | 11 | 0 |
| 2023 | 15 | 0 |
| 2024 | 12 | 12 |
| 2025 | 10 | 10 |
| 2026 | 14 | 14 |

Fayette rows of every metric: 2,367 before, all N. After: 3,349, of which 982 are Y (2021 68, 2022 111, 2023 115, 2024 212, 2025 244, 2026 232).

Published Fayette All Students anchors, unchanged:

| Year | School id | Cohort | Total count | Total rate |
| --- | --- | ---: | ---: | ---: |
| 2021 | 165 | 2,783 | 1,399 | 50.3 |
| 2022 | 165 | 2,896 | 1,336 | 46.1 |
| 2023 | 165 | 2,979 | 1,449 | 48.64 |
| 2024 | 165000 | 3,053 | 1,529 | 50.1 |
| 2025 | 165000 | 3,217 | 1,540 | 47.9 |
| 2026 | 165000 | 3,340 | 1,594 | 47.7 |

Bryan Station High School `165170`, All Students, total rate: 42.2, 36.9, 39.19, 44.6, 40.6, 43.5 for 2021-2026. Unchanged.

## Spot checks

State All Students, cohort / public / private / total / rates. Source cell, then the stored value. All `suppressed` N.

| Year | School id | District | Source | Stored |
| --- | --- | --- | --- | --- |
| 2021 | 999 | State | 45688, 17629, 3642, 21271, 38.6%, 8.0%, 46.6% | 45688.0, 17629.0, 3642.0, 21271.0, 38.6, 8.0, 46.6 |
| 2026 | 999000 | All Districts | 48,798, 18,769, 3,563, 22,332, 38.5, 7.3, 45.8 | 48798.0, 18769.0, 3563.0, 22332.0, 38.5, 7.3, 45.8 |

Adair County district All Students, 2026, school `001000`: source 215, 36, 47, 83, 16.7, 21.9, 38.6. Stored the same numbers, `suppressed` N.

Fayette school, Henry Clay High School `165039`, African American, KYRC26. Source: cohort 142, public `*`, private `*`, total 56, public rate `*`, private rate `*`, total rate 39.4. Stored: cohort 142.0 N, total count 56.0 N, total rate 39.4 N, and the four stars as Y.

Adair County High School `001010`, All Students, KYRC26. Source: cohort 201 and six stars. Stored: cohort 201.0 N and six Y rows.

2021 Fayette district Migrant, school `165`. Source: cohort 13, public 5, private `*`, total 5, rates 38.5%, 0.0%, 38.5%. Stored: the numbers, including private rate 0.0 N, and private count Y.

2022 Adair County district, Students with Disabilities (IEP), school `001`. Source: cohort 15, three count stars, three rates `**`. Stored: cohort 15.0 N, three count Y rows, no rate rows.

Collins Lane `181025`, All Students, KYRC25. Source: seven stars. Stored: no row.

Bryan Station `165170` Asian, and Fayette `165000` American Indian or Alaska Native, and both district `602` All Students rows in KYRC26: cohort `*`, no row.

## The seven KYRC26 leaks

Private count and private rate are `*`. Public count and total count are numbers, so total − public is the private count KDE did not print. The difference is not stored. The public and total cells stay as published. The private cells are Y because the cohort is greater than 0. Each row is in the flagged list.

| School id | Group | Cohort | Public | Total | Total − public |
| --- | --- | ---: | ---: | ---: | ---: |
| 042070 Bowling Green High School | All Students | 301 | 151 | 167 | 16 |
| 121030 George Rogers Clark High School | Male | 184 | 64 | 75 | 11 |
| 133040 Corbin High School | Male | 86 | 26 | 45 | 19 |
| 145010 Daviess County High School | Female | 172 | 102 | 114 | 12 |
| 581200 Wayne County High School | Economically Disadvantaged | 175 | 48 | 58 | 10 |
| 601084 Woodford County High School | Male | 157 | 56 | 74 | 18 |
| 601084 Woodford County High School | Gifted and Talented | 73 | 45 | 55 | 10 |

2022-2025 have no row of this shape. 2021 has 789 rows where the private count is `*` and the public and total counts are numbers (486 differences above 0, 303 differences of 0). On all 789 the private rate is a published number, which is not the KYRC26 pattern. Those 2021 private counts are stored as Y. The difference is not stored. They are not in the flagged list. The decision names the 7 KYRC26 rows.

## Absent groups with graduates

Issue #44 says 92 keys that KYRC24/26 omit from the transition file have 3 to 42 graduates in the graduates file. That figure is cited here as `kde-recheck`. The graduates file was not opened, and it is not registered.

## Flags

`notes/flags/A-flagged-keys.csv` has 19,898 rows: the starred-cohort counts in the table above, plus the 7 leaks. Columns are `module,year,district,school_id,student_group,metric,value,reason,label`. Every label is `kde-recheck`. Every value is `*`. No duplicate key on year, district, school id, student group, metric, and reason.

## Mirror

None. This change reads only the six files already registered for `postsecondary_enrollment`.
