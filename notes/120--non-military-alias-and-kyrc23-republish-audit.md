# 120 - Non-Military alias and KYRC23 republish audit

Part F-1 of brief F. The shared demographic map sends KDE's `Non-Military` label to `Non-Military Dependent`. F-2, loading republish-only groups, is not implemented. The base is `feature/2025-26-kyrc26` at `1aef27b4a43dffb9757b4651a4dc687945479b52`. That branch had not moved, so this work was not rebased. `develop` and `master` are not touched.

Record DNNNN is the alias. It supersedes record 0047 for the Non-Military case only. 0047 stands for every other 2018-19 demographic choice. The 2018-19 code mappings (`MIL`, `MilitaryConnected`, and the 2019 year-specific block) are unchanged. The canonical stored label `Non-Military Dependent` is an implementation detail of brief F-1, not Scott's words.

Scott's words (exact), from DECISIONS-VERIFIED.md item 9: "Approve the global Non-Military alias". "For every module" is Gremlin's framing of the question, not Scott's words.

## Alias

`config/demographic_mappings.yaml` no longer lists `Non-Military` as its own canonical group. The general map is `Non-Military` to `Non-Military Dependent`, and the lowercase map is `non-military` to `Non-Military Dependent`. `Non-Military` had to leave the canonical list. The mapper returns a canonical label unchanged before it reads the general map, so leaving it there would have blocked the alias.

`etl/advanced_coursework.py` keeps a module-local `"Military Connected": "Military Dependent"`. That is the positive group. It does not mention `Non-Military`, so it was not changed.

The alias changes the stored `student_group` and nothing else. It does not add a row and it does not change `value` or `suppressed`.

## Stop condition

No registered source file that this audit opened publishes both `Non-Military` and `Non-Military Dependent`. Relabelling does not make two rows in one file share a key. The census is the exact published string, not a case-folded search. Files opened: every configured CSV whose name starts with `KYRC2` or ends in `_2022.csv` or `_2023.csv` and whose header has a demographic column, plus the KYRC24 assessment and index files the first header probe missed (historical host returned 504; the datasets copies were read), plus 2020 and 2021 graduation and assessment CSVs and the 2018-19 graduation, assessment, and transition-readiness workbooks. Those older files contain neither string.

`act_scores` and `benchmark_assessment` are download sources only. There is no ETL module and no `config/mappings.yaml` entry for either. KYRC24 ACT has 531 `Non-Military` rows, KYRC25 ACT has 543, KYRC24 benchmark has 531, KYRC25 benchmark has 543, and KYRC26 benchmark has 551. They produce no KPI rows today. The shared map will apply if a module is added.

## What the alias changes

The modules below were run twice, once on the base yaml and once on the alias, through each module's `transform`. `last_updated` is a clock and was ignored. After the before side's `Non-Military` label is renamed to `Non-Military Dependent`, a sorted row merge of all four outputs has 0 keys added, 0 keys removed, and 0 value or suppressed changes. Duplicate keys on `(district, school_id, school_name, year, student_group, metric)` are 0 on both sides of every module.

| Module | Year | Rows relabelled | Added | Removed | Value changed |
| --- | --- | --- | --- | --- | --- |
| graduation_rates | 2022 | 0 | 0 | 0 | 0 |
| graduation_rates | 2023 | 0 | 0 | 0 | 0 |
| graduation_rates | 2024 | 794 | 0 | 0 | 0 |
| graduation_rates | 2025 | 794 | 0 | 0 | 0 |
| graduation_rates | 2026 | 794 | 0 | 0 | 0 |
| postsecondary_readiness | 2022 | 0 | 0 | 0 | 0 |
| postsecondary_readiness | 2023 | 0 | 0 | 0 | 0 |
| postsecondary_readiness | 2024 | 794 | 0 | 0 | 0 |
| postsecondary_readiness | 2025 | 794 | 0 | 0 | 0 |
| postsecondary_readiness | 2026 | 794 | 0 | 0 | 0 |
| kentucky_summative_assessment | 2022 | 0 | 0 | 0 | 0 |
| kentucky_summative_assessment | 2023 | 0 | 0 | 0 | 0 |
| kentucky_summative_assessment | 2024 | 143764 | 0 | 0 | 0 |
| kentucky_summative_assessment | 2025 | 144154 | 0 | 0 | 0 |
| kentucky_summative_assessment | 2026 | 144163 | 0 | 0 | 0 |
| safe_schools_climate | 2023 | 3524 | 0 | 0 | 0 |
| safe_schools_climate | 2024 | 3518 | 0 | 0 | 0 |
| safe_schools_climate | 2025 | 3516 | 0 | 0 | 0 |
| safe_schools_climate | 2026 | 3512 | 0 | 0 | 0 |

Graduation's 794 rows are 397 `graduation_rate_4_year` and 397 `graduation_rate_5_year`. Postsecondary's 794 rows are 397 `postsecondary_readiness_rate` and 397 `postsecondary_readiness_rate_with_bonus`. KSA 2022 is 51,488 `Non-Military Dependent` rows on both sides (the 2022 accountability file already uses that label). KSA 2023 is 51,550 on both sides. Climate's counts are the per-level climate and safety indexes. `climate_index_score_calculated` and `safety_index_score_calculated` are not emitted.

Other configured CSV sources with a demographic column had 0 rows labelled `Non-Military`. The yaml diff does not change any other label, so those module-years do not change. `kpi_master.csv` was not regenerated. The processed directory is gitignored and was empty here, and a portal copy would not be the output of `1aef27b`. The base duplicate-key count of 1,813 (1,810 in 2019, 1 in 2022, 2 in 2023) is the figure in brief F. It was not recomputed. On the four regenerated outputs the duplicate-key count is 0 before and 0 after. No scanned file contains both labels, so the rename cannot create a new key inside a file.

## Spot checks

Source cell, then output. `All Schools` on a district total is stored as `---District Total---` by the existing school-name rule. School names of real schools are not rewritten. District stays the published district.

KYRC26 four-year graduation, datasets container. State `999000`, source row 25, `Non-Military` 94.1. Output year 2026, district `All Districts`, school `---District Total---`, group `Non-Military Dependent`, `graduation_rate_4_year` 94.1, suppressed N. Adair County `001000`, source row 49, 97.6. Output district `Adair County`, school `---District Total---`, 97.6. Fayette County `165000`, source row 2833, 93.9. Output district `Fayette County`, school `---District Total---`, 93.9. Lafayette High School `165060`, source row 2881, 98.1. Output school name `Lafayette High School`, 98.1. Fayette All Students `165000`, source row 2810, 93.9, stays All Students 93.9.

2023 graduation, historical host. Fayette All Students four-year rate 90.2 stays 90.2, district `Fayette County`, school id `165`. State All Students 91.4 stays 91.4, district `State`, school id `999`. That file has no military label.

KYRC26 postsecondary readiness, datasets. State `999000` `Non-Military` postsecondary rate 85.3. Output `postsecondary_readiness_rate` 85.3, group `Non-Military Dependent`, district `All Districts`. Adair `001000` 97.5. Fayette district `165000` is `*` (output value blank, suppressed Y). Lafayette `165060` in 2026 is `*`. Lafayette in 2024 is 81.5 on both sides of the rename. Fayette All Students 2026 is 86.4 and stays All Students.

KYRC24 assessment by grade, datasets. Grade 3 reading proficient/distinguished, `Non-Military`: state `999000` 47 (output 47.0), Fayette district `165000` 51 (output 51.0), Tates Creek Elementary `165095` 28 (output 28.0, school name unchanged). Grade 8 mathematics: state 37, Fayette district 39, Beaumont Middle School `165015` 51. Same numbers after the rename. Fayette All Students grade 3 reading stays 51.0.

2023 accountability, historical host, group already `Non-Military Dependent`. State elementary reading proficient/distinguished 47 and content index 65.9. Fayette district `165` 52 and 70.2. Tates Creek `165095` 41. Those values are unchanged. The fixture rows are in `tests/fixtures/kentucky_summative_assessment/accountable_2023_non_military_dependent.csv`.

KYRC26 climate index, datasets. State elementary climate 80.0 and safety 74.8. Fayette district elementary climate 79.3 and safety 73.8. Tates Creek Elementary `165095` climate 76.2 and safety 71.0. Fayette high climate 67.1 and safety 63.5. Lafayette `165060` climate 67.1 and safety 62.8. Output metrics are `climate_index_score_elementary`, `safety_index_score_elementary`, `climate_index_score_high`, and `safety_index_score_high`, group `Non-Military Dependent`.

## Fayette groups that change

Filter: district `Fayette County`, year 2021 or later. The only `student_group` that changes is `Non-Military` to `Non-Military Dependent`. No other group is renamed.

On the dashboard target metrics this pipeline emits:

| Metric | Years | Fayette rows relabelled |
| --- | --- | --- |
| `graduation_rate_4_year` | 2024, 2025, 2026 | 7 each year |
| `postsecondary_readiness_rate` | 2024, 2025, 2026 | 7 each year |
| `kentucky_summative_assessment_reading_proficient_distinguished_rate_grade_3` | 2024, 2025, 2026 | 40, 40, 40 |
| `kentucky_summative_assessment_math_proficient_distinguished_rate_grade_8` | 2024, 2025, 2026 | 20, 19, 20 |

`graduation_count_4_year`, `graduation_count_5_year`, `climate_index_score_calculated`, and `safety_index_score_calculated` are in the extract's target list and are not emitted for this group. The five-year graduation rate and the postsecondary rate with bonus are emitted and do change label. They are not in the target list. The per-level climate and safety indexes change label in 2023, 2024, 2025, and 2026, including Fayette district `165000`, Lafayette `165060`, and Tates Creek Elementary `165095`. The extract does not request those metric names.

2023 assessment rows were already `Non-Military Dependent`. They do not change.

## KYRC23 republish audit

Read-only. Nothing in this table was loaded. F-2 is not approved. Kindergarten stays with batch I.

Comparison: school codes of six digits ending in `000` are folded to the first three digits so `165000` and `165` are one school. Demographic labels go through the shared mapper. KSA level and subject codes are expanded (`ES` to elementary, `RD` to reading). A measure cell is nonblank when it is not empty. It is numeric when it parses as a float after commas and a trailing percent sign are removed. `*` is nonblank and not numeric.

KSA accountability, before the alias: the republish's only extra group is `Non-Military` (8,748 rows, 51,550 nonblank measure cells, 46,860 numeric). The registered file's matching group is `Non-Military Dependent`. After the alias those keys match: 279,936 keys on both sides, 0 keys only in the republish, 0 numeric differences on matched keys. The files are not cell-identical. 280,995 measure cells are blank on the republish where the registered file has `*`. That is a suppression difference, not a new group, and it is not loaded.

KSA by grade, before and after the alias: `Non-Foster Care` (raw `Non-Foster`) and `Non-Military` (after the alias, `Non-Military Dependent`). 18,677 rows each, 37,354 rows, 183,240 nonblank, 148,635 numeric. The registered grade file has neither group. These are the F-2 groups. They are not loaded.

Both live containers return HTTP 200 for every republish name below. `datasets` and `datasetsuat6051276` have the same Content-Length except the out-of-field file. The regen mirror `MANIFEST.csv` was not on this machine, so mirror membership was not verified against that manifest. The brief's statement that the KSA files sit in the mirror only as `datasetsuat6051276/2023/...` was not re-checked against a manifest. No mirror file was added.

KSA accountability, downloaded from `datasets`: 47,926,483 bytes, md5 `c90a58f70c5e1c251ba81c7d4ff9de09`, Last-Modified Thu, 14 May 2026 19:11:26 GMT. UAT Content-MD5 decodes to the same md5. UAT Last-Modified Tue, 06 Oct 2026 20:27:28 GMT.

KSA by grade, downloaded from `datasets`: 93,205,215 bytes, md5 `5ed039814372926f96dea829bd301339`, same May Last-Modified. UAT length matches. UAT sent no Content-MD5.

| Module | Republish file | Groups or keys only in the republish | Nonblank cells | Numeric cells | Live containers |
| --- | --- | --- | --- | --- | --- |
| kentucky_summative_assessment | KYRC23_ACCT_Kentucky_Summative_Assessment.csv | After the alias, none. Before the alias, group `Non-Military` only (8,748 rows). | 51,550 before the alias; 0 after | 46,860 before the alias; 0 after | both 200, lengths and md5 match |
| kentucky_summative_assessment | KYRC23_ASMT_Kentucky_Summative_Assessment.csv | `Non-Foster Care` and `Non-Military` (after the alias, `Non-Military Dependent`). 18,677 rows each. Not loaded. | 183,240 | 148,635 | both 200, lengths match, UAT md5 header empty |
| kindergarten_readiness | KYRC23_ASMT_Kindergarten_Screen_Composite.csv | `Non-Economically Disadvantaged`, `Non-English Learner`, `Students without IEP`. 5,042 rows each. Audit only. | 56,172 | 31,884 | both 200, lengths match |
| novice_teachers | KYRC23_OVW_Inexperienced_Teachers.csv | none (1,554 school keys on both sides; no demographic) | 0 | 0 | both 200, lengths match |
| novice_teachers | KYRC23_OVW_Students_Taught_by_Inexperienced_Teachers.csv | none. Nine group columns match the registered percent columns under different headers. Gap columns are registered-only. 2,015 school and Title I keys on both sides. | 0 | 0 | both 200, lengths match |
| students_taught_by_ineffective_teachers | KYRC23_OVW_Students_Taught_by_Ineffective_Teachers.csv | none. 0 school keys only in the republish. 774 keys are only in the registered file. `African American` (1,170 nonblank) replaces the registered Non-White column. `White (non-Hispanic)` (1,170 nonblank) replaces White. | 0 extra keys | 0 extra keys | both 200, lengths match |
| students_taught_by_out_of_field_teachers | KYRC23_OVW_Students_Taught_by_Out_of_Field_Teachers.csv | none. Same shape as the inexperienced student file. 2,015 keys on both sides. | 0 | 0 | both 200. datasets 308,616 bytes, md5 `c5a7e349f9fb112715e95a8fac41a719`. UAT 308,667 bytes, Content-MD5 `16ee00365af5729866a67d644b4d4a4e` |
| advanced_coursework | KYRC23_EDOP_Advanced_Courses_Offered.csv | none. 0 schools only in the republish. State course code `N/A` on the republish is blank on the registered file. After that spelling, 0 course keys only in the republish and 131 only in the registered file. | 0 | 0 | both 200, lengths match |
| advanced_coursework | KYRC23_EDOP_Advanced_Courses_Participation_and_Performance.csv | none. The republish is the AP, IB, and Cambridge slice. The registered file also has `ALL` and `DC`. Course type text differs (`AP` versus `Advanced Placement`). 0 schools only in the republish. | 0 | 0 | both 200, lengths match |
| career_readiness_indicators | KYRC23_CTE_Career_Readiness_Indicators.csv | none (397 schools on both sides). `work based learning` renames exceptional work experience and is blank on all 397 rows. Total graduates is registered-only. | 0 | 0 | both 200, lengths match |
| cte_participation | KYRC23_CTE_Participation.csv | Not a full skip. The two published rates are loaded. Count columns are not stored. 0 demographics only in the republish. 1,197 keys are only in the annual file. | 0 | 0 | both 200, lengths match |
| cte_pathways | KYRC23_CTE_Career_Pathways.csv | none (6,034 pathway keys on both sides). 40 completer cells are a number here and `*` on the registered file. Other completer cells that are numeric on both sides do not differ. | 0 extra keys | 40 star-versus-number completer cells, not loaded | both 200, lengths match |
| gifted_talented | KYRC23_EDOP_Gifted_Participation_by_Grade_Level.csv | none. The registered file has `Consolidated Student Group`, which the republish does not. | 0 | 0 | both 200, lengths match |
| gifted_talented | KYRC23_EDOP_Gifted_Participation_by_Category.csv | none. No demographic column. Categories match the wide registered columns. 1,658 schools on both sides after the school-code fold. | 0 | 0 | both 200, lengths match |
| school_courses | KYRC23_EDOP_School_Courses_Summary.csv | none (30,688 course keys on both sides). Grade columns are renamed (`All Students` versus `STUDENT COUNT`). | 0 | 0 | both 200, lengths match |
| student_teacher_ratio | KYRC23_OVW_Student_to_Teacher_Ratio.csv | none (1,602 school keys on both sides). No demographic. The registered header glues the ratio column name to itself. | 0 | 0 | both 200, lengths match |
| teacher_experience | KYRC23_OVW_Average_Years_School_Experience.csv | none (1,587 school keys). `total years experience` is registered-only. | 0 | 0 | both 200, lengths match |
| teacher_working_conditions | KYRC23_OVW_Teacher_Working_Conditions.csv | none. 0 schools only in the republish (1,539 shared, 5 only in the 2021-22 file). The republish year cell is `20222023`. The registered file is `teacher_working_conditions_2022.csv`, year `20212022`. Impact measures drop the word Composite, and Teaching Environment is School Climate. The module already skips this file as that republish. | 0 | 0 | both 200, lengths match |
| district_school_list | KYRC23_OVW_District_School_List.csv | none (1,661 schools). No demographic. This skip is outside the brief's filename list and is included because record 0043 asks for every module that skips a KYRC23 republish. | 0 | 0 | both 200, lengths match |
| safe_schools_climate | KYRC23_SAFE_Precautionary_Measures.csv | none. 0 schools only in the republish, 172 only in the registered file. The ten republish questions correspond to ten registered columns. The registered file has additional program columns. Index scores are loaded and are not this skip. | 0 | 0 | both 200, lengths match |
| safe_schools_discipline | KYRC23_SAFE_Discipline_Resolutions.csv | none (30,096 school and demographic keys on both sides). Extra measure column `harassment (includes bullying)`: 30,096 nonblank, 30,090 numeric. Corporal punishment is registered-only. Legal sanctions are loaded and are not this skip. | 30,096 in that column | 30,090 in that column | both 200, lengths match |
| safe_schools_events | KYRC23_SAFE_Behavior_Events_by_Type.csv | none (30,096 keys). Column punctuation differs. Location, context, and legal sanctions are separate republish files and are already columns on `safe_schools_event_details_2023.csv`. | 0 | 0 | both 200, lengths match |
| safe_schools_events | KYRC23_SAFE_Behavior_Events_by_Grade_Level.csv | none (30,077 keys). Grade headers differ (`1st` versus `grade1 count`). | 0 | 0 | both 200, lengths match |
| safe_schools_events | KYRC23_SAFE_Behavior_Events_by_Location.csv | none versus event details (30,096 keys). Location measures exist there under different headers. | 0 | 0 | both 200, lengths match |
| safe_schools_events | KYRC23_SAFE_Behavior_Events_by_Context.csv | none versus event details (30,096 keys). | 0 | 0 | both 200, lengths match |
| teacher_certification | KYRC23_OVW_Teacher_Certification_Data.csv | none (1,661 schools). Header punctuation differs. | 0 | 0 | both 200, lengths match |
| teacher_turnover | KYRC23_OVW_Teacher_Turnover.csv | none (1,661 schools). | 0 | 0 | both 200, lengths match |
| dual_credit | KYRC23_EDOP_Dual_Credit_Courses_Offered.csv | Loaded, not a skip. No historical counterpart on the historical host. | n/a | n/a | both 200, lengths match |
| dual_credit | KYRC23_EDOP_Dual_Credit_Participation_and_Performance.csv | Loaded, not a skip. Demographics do not include `Non-Military` or `Non-Military Dependent`. | n/a | n/a | both 200, lengths match |

`KYRC23_OVW_Advanced_Coursework.csv` and `KYRC23_ACCT_Index_Scores.csv` are loaded. They are not skips. The index file is one of the files the alias relabels.
