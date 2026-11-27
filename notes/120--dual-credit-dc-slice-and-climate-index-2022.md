# 120 - 2021 and 2022 dual credit, and the 2021-22 climate index

The base is `feature/2025-26-kyrc26` at `1aef27b4a43dffb9757b4651a4dc687945479b52`. The branch had not moved. `develop` and `master` are not touched.

Two loads:

- 2020-21 and 2021-22 dual credit, the `DC` slice of the advanced-course participation and courses-offered files. Issue #74 (b) and (c). Record [0031](../docs/decisions/0031-dual-credit-dc-slice.md).
- The published 2021-22 climate and safety index. Issue #121. Record [0061](../docs/decisions/0061-register-2021-22-climate-index.md) replaces the registration choice in [0042](../docs/decisions/0042-do-not-register-2021-22-climate-index.md). The metric names in 0042 still stand.

No score is recomputed. School codes are not padded. `NUMBER TESTED` is not stored. `has_dual_credit_program` is not restored.

## Source files

Fetched 27 Nov 2026, 12:59 PM ET. A later range request on the same day returned the same `Last-Modified` and the same byte length. The climate file is in the regeneration mirror `mirror-2026-11-27-j2`. The four advanced-course files are already mirrored for `advanced_coursework`. This note does not edit the mirror. `prepare_kde_data.py` downloads the dual-credit copy into `data/raw/dual_credit/` because the names are also registered under `dual_credit`. The climate row count below uses the registered `KYRC26_SAFE_Precautionary_Measures.csv` (md5 `220575cf689e134424fb28e3797b1687`, 198,110 bytes), not the 4 November 2026 refresh.

| File | Bytes | md5 | sha256 | Last-Modified |
| --- | --- | --- | --- | --- |
| `advanced_courses_participation_and_performance_2021.csv` | 2,974,724 | `530dbba911a07d5ed905e86f51f895d8` | `bc6669765ff4491b50d5f9398ec2f56cb28d541949647369c1f16d91b00de2ba` | Fri, 09 Feb 2024 16:32:46 GMT |
| `advanced_courses_participation_and_performance_2022.csv` | 3,142,217 | `3a23679da12d4c36026d50be2ba4d91c` | `e36d512e303be9f023d5ba7e93fa046088c898f2baad5ace0fcf66b7976f3d3f` | Thu, 08 Feb 2024 17:21:59 GMT |
| `advanced_courses_offered_2021.csv` | 1,364,326 | `d11780c6f46f45a1e5bcc4c62a5d5a2f` | `178f6175569c49456c05f283c23a3f87fbbdc35b1ee76da7e0abc7eeba236f98` | Fri, 09 Feb 2024 16:32:45 GMT |
| `advanced_courses_offered_2022.csv` | 1,402,343 | `f7c2b24e7aeba6ada42b1e10e2ba3f19` | `caeb4f85eb5a185aa178560132881350ef7d2b4e89f48548b11ead11c1c8c5b4` | Thu, 08 Feb 2024 17:21:59 GMT |
| `quality_of_school_climate_and_safety_survey_index_scores_2022.csv` | 6,553,428 | `aaec467c52a2310176900619881d2ad6` | `93f59d93fad6e0893e01a462fc1caed5f5b51662c8b1342acb2a5942ca4ba0fb` | Thu, 08 Feb 2024 20:04:28 GMT |

URLs, historical host `https://www.education.ky.gov/Open-House/data/HistoricalDatasets/`:

- `advanced_courses_participation_and_performance_2021.csv`
- `advanced_courses_participation_and_performance_2022.csv`
- `advanced_courses_offered_2021.csv`
- `advanced_courses_offered_2022.csv`
- `quality_of_school_climate_and_safety_survey_index_scores_2022.csv`

The climate file is in the regeneration mirror `mirror-2026-11-27-j2`. This repo does not edit that mirror.

## Dual credit

The course type is filtered before repeated keys are collapsed. The 2021 and 2022 files repeat a school and demographic once per course type. Collapsing first would keep the larger `ALL` enrollment and then drop it, which deletes the `DC` row. State All Students participation enrollment is 36,641, not the `ALL` enrollment 63,321 and not the `AP` enrollment 37,916.

`advanced_courses_*_2020.csv` and `advanced_courses_*_2023.csv` also have a `DC` slice, so a filename guard is what keeps them out. 2020 is outside this load. 2023 school codes match the KYRC23 EDOP school rows.

State and district codes stay as published. 2021 school ids are 3 digits (state `999`, Fayette `165`, Adair `001`) or 6 digits (Bryan Station `165170`). `---State Total---` and `---District Total---` stay as published.

Spot checks, participation, All Students:

| Year | School | Enrollment | Completers | Qualifying | Completion rate | Qualifying rate |
| --- | --- | --- | --- | --- | --- | --- |
| 2021 | State `999` | 36,641 | 32,454 | 29,482 | 88.6 | 90.8 |
| 2021 | Fayette `165` | 1,262 | 1,205 | 1,138 | 95.5 | 94.4 |
| 2021 | Bryan Station `165170` | 115 | 106 | 99 | 92.2 | 93.4 |
| 2022 | State `999` | 38,371 | 34,597 | 32,664 | 90.2 | 94.4 |
| 2022 | Fayette `165` | 1,234 | 1,188 | 1,154 | 96.3 | 97.1 |
| 2022 | Bryan Station `165170` | 133 | 122 | 120 | 91.7 | 98.4 |

2022 Bryan Station World Languages course enrollment is 36, completers are `*` (`suppressed` Y), and qualifying is 30. Both rates are omitted. 2022 Bardstown Independent (`017`) Asian enrollment is 7, and completers and qualifying are `*`.

2023 state All Students enrollment stays 45,873 from `KYRC23_EDOP_Dual_Credit_Participation_and_Performance.csv`.

### Alternate 2023 DC slice, not loaded

Compared 27 Nov 2026. School codes were padded with `000` only for the join. Nothing is stored that way.

Participation, course type `DC` versus KYRC23 `Dual Credit`: 8,514 historical rows and 8,496 EDOP rows. The only extra school is `145035` (18 demographic rows) in the historical file. All Students: 472 shared codes, 0 mismatches on enrollment, completers, and qualifying, including stars. One All Students code is only in the historical file (`145035`).

On the 8,496 shared rows, every cell both sides publish as a number matches. That is 3,900 enrollment cells, 3,761 completer cells, and 3,692 qualifying cells, with 0 numeric differences. KYRC23 stars 58 enrollment cells, 50 completer cells, and 45 qualifying cells that the DC slice publishes as a number. That is 153 cells on 58 rows. The "58 more cells" in #74 (c) matches the enrollment-cell count and the row count. All Students has 0 extra stars.

Courses offered, `DC` only: 5,881 rows on both sides, including school `145035` (4 rows on both). Numeric cells match. KYRC23 has 1 extra enrollment star. The historical file has 1,626 extra completer stars. Qualifying stars match.

## Climate and safety index

The 2022 file has 47,574 rows and the same 18 columns as `KYRC23_ACCT_Index_Scores.csv`. Level counts match: elementary 23,598, middle 13,257, high 10,719. 2022 uses `ES` / `MS` / `HS`. KYRC23 uses `Elementary School` / `Middle School` / `High School`. Both map to the same metric suffixes.

2022 climate numbers run 44.4 to 98.6 (29,477 numeric cells). Safety runs 42.2 to 100.0. KYRC23 climate runs 37.2 to 98.4 and safety 35.2 to 99.5 (29,723 numeric cells each). There is no blank, no 0, and no row that stars one index and publishes the other. A `*` is value NA with `suppressed` Y. The published `SUPPRESSED` column is Y on those 18,097 rows and N on the 29,477 numbered rows.

The 2022 accountability indicator rates stay `climate_safety_indicator_rate_{level}`. Fayette elementary All Students is still 76.9 from `accountability_profile_2022.csv`. The new index rows use different metric names, so they do not collide.

State All Students, school `999`, district `State`, school `---State Total---`:

| Level | Climate | Safety |
| --- | --- | --- |
| Elementary | 79.4 | 73.8 |
| Middle | 69.2 | 62.6 |
| High | 64.1 | 58.6 |

Fayette County All Students, school `165`, district total:

| Level | Climate | Safety |
| --- | --- | --- |
| Elementary | 79.5 | 74.2 |
| Middle | 67.6 | 61.9 |
| High | 64.3 | 60.9 |

Bryan Station High `165170` is 63.9 / 59.9. Arlington Elementary `165005` is 76.7 / 69.9. Adair County `001` high is 67.1 / 60.5. Fayette American Indian or Alaska Native high is `*` / `*`.

Fayette index rows (`district == Fayette County`, year >= 2021) before this load: 2023, 2024, and 2025 have 3,186 rows each, and 2026 has 3,240. 2022 had none. After this load, 2022 also has 3,186. The 2022 district total is school code `165`. 2023 and later use `165000`.

`fcps-equity-dashboard` `develop` at `7e820e1` selects the six per-level climate and safety names and keeps a Fayette row when the district name is `Fayette County`, so the 2022 rows are in range after a regen. `main` still selects `climate_index_score_calculated` and `safety_index_score_calculated`. Those names are not written. No `dual_credit_*` metric is in `TARGET_INDICATORS` on that develop file. Data Engineer confirmed the 2022 climate file loaded exactly as published. `analysis/scripts/school_climate_analysis.py` still filters to 2023-2026.

## Before and after

Compared to output from the base-branch code on the previously registered files. `last_updated` is ignored. No shared key changed a value. No key was removed. Each module output has 0 duplicate keys before and after. `kpi_master.csv` is gitignored and was not rebuilt, so the base count of 1,813 duplicate keys was not recounted.

`dual_credit.csv`: 250,306 rows before, 356,781 after.

| Year | Before | After | Added | Removed | Value changes on shared keys |
| --- | --- | --- | --- | --- | --- |
| 2021 | 0 | 51,612 | 51,612 | 0 | 0 |
| 2022 | 0 | 54,863 | 54,863 | 0 | 0 |
| 2023 | 59,815 | 59,815 | 0 | 0 | 0 |
| 2024 | 62,331 | 62,331 | 0 | 0 | 0 |
| 2025 | 64,168 | 64,168 | 0 | 0 | 0 |
| 2026 | 63,992 | 63,992 | 0 | 0 | 0 |

`safe_schools_climate.csv`: 400,581 rows before, 495,729 after. The count uses the registered `KYRC26_SAFE_Precautionary_Measures.csv` (md5 `220575cf689e134424fb28e3797b1687`). The index adds 95,148 rows.

| Year | Before | After | Added | Removed | Value changes on shared keys |
| --- | --- | --- | --- | --- | --- |
| 2020 | 2,589 | 2,589 | 0 | 0 | 0 |
| 2021 | 2,609 | 2,609 | 0 | 0 | 0 |
| 2022 | 4,390 | 99,538 | 95,148 | 0 | 0 |
| 2023 | 99,550 | 99,550 | 0 | 0 | 0 |
| 2024 | 96,300 | 96,300 | 0 | 0 | 0 |
| 2025 | 97,621 | 97,621 | 0 | 0 | 0 |
| 2026 | 97,522 | 97,522 | 0 | 0 | 0 |

No flagged cell. No `notes/flags/` file.
