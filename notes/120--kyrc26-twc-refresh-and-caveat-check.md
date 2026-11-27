# 120 — KYRC26 teacher working conditions refresh, and the batch K caveat check

Docs only. No ETL module, no registered filename, and no processed output is changed. The file comparison below was run in this workspace on 27 Nov 2026. Decision records: `docs/decisions/0060-twc-kyrc26-refresh.md` and `docs/decisions/0059-issue-42-closed.md`.

## Source files

Fetched 27 Nov 2026. The HTTP `Date` on the datasets response is `Fri, 27 Nov 2026 18:00:31 GMT` (1:00:32 PM ET).

| Copy | URL | Last-Modified | Bytes | md5 | sha256 |
| --- | --- | --- | --- | --- | --- |
| Datasets, current | `https://kdeschoolreportcard.blob.core.windows.net/datasets/KYRC26_OVW_Teacher_Working_Conditions.csv` | Wed, 04 Nov 2026 13:21:19 GMT | 512,257 | `2d813911b205483f422311f8a8e0f5e5` | `ffefc54e99835089e999b40aadd854ba493806c55f389242d62737e498295478` |
| UAT, still the 15 October hash | `https://kdeschoolreportcard.blob.core.windows.net/datasetsuat6051276/KYRC26_OVW_Teacher_Working_Conditions.csv` | Sat, 10 Oct 2026 06:02:52 GMT | 436,507 | `ac5bd3c79bbfeaef42ee5ce015dcb089` | `7883834c2870375939475d93c75faa0d7f7783f3433146ca4dcc9dc77500024d` |
| District-school list, comparison only | `https://kdeschoolreportcard.blob.core.windows.net/datasets/KYRC26_OVW_District_School_List.csv` | Wed, 04 Nov 2026 13:20:38 GMT | 426,339 | `c10799c0c3d21e7f90676654f40cedb1` | `a92b91ac82dc5b79910cd36134e9447028a408a59426300153432a0f9300034e` |

The datasets Content-MD5 header was `LYE5EbIFSD9CIxH4qOD15Q==`, which is the base64 form of md5 `2d813911b205483f422311f8a8e0f5e5`. The UAT md5 is the hash `notes/104--kyrc26-file-to-etl-mapping.md` recorded for the 15 October datasets object. This note does not claim a second download of that October datasets object.

The district-school list is not adopted. Its Last-Modified is 4 November 2026. Scott's 12:30 PM ET words keep other refreshed files in the ongoing review. It is used here only to count districts.

`mirror-2026-11-07-twc` is not in this repository, and a GitHub code search for that name returned no file. This note cannot confirm a mirror md5. No mirror row is added.

## What the two teacher working conditions files do

Both have the same 15 columns. Every data row is school year `20252026`. Keys are (School Code, Impact Measure). Neither file has a duplicate key.

| | UAT | Datasets |
| --- | ---: | ---: |
| Rows | 3,282 | 3,852 |
| District numbers | 136 | 167 |

Removed keys: 0. Added keys: 570. School codes ending in `000`: 93 added rows (31 districts, 3 constructs). Other school codes: 477 added rows (159 schools, 3 constructs).

Shared keys: 3,282. Impact Value differs on 0 of them. The only other differing field is District Name, on 6 rows, district 603, school codes `603000` and `603390`. UAT: `Kentucky School for the Deaf`. Datasets: `Kentucky School for the Deaf District`. Managing Student Behavior on `603000` is 63 on both.

The 31 added district numbers, and the name on the district-school list: Adair County 001, Allen County 005, Anchorage Independent 006, Anderson County 011, Ashland Independent 012, Augusta Independent 013, Ballard County 015, Barbourville Independent 016, Bardstown Independent 017, Barren County 021, Bath County 025, Beechwood Independent 026, Bell County 031, Bellevue Independent 032, Berea Independent 034, Boone County 035, Bourbon County 041, Bowling Green Independent 042, Boyd County 045, Boyle County 051, Bracken County 055, Breathitt County 061, Breckinridge County 065, Bullitt County 071, Burgin Independent 072, Butler County 075, Caldwell County 081, Calloway County 085, Campbell County 091, Campbellsville Independent 092, Carlisle County 095.

The list has 177 districts plus the `---State---` row. UAT is missing 42 of those 178 (178 − 136). Datasets is missing 11 (178 − 167). State 999 is in both teacher working conditions files.

Still absent from the datasets file:

| Code | Name on the list |
| --- | --- |
| 131 | Clinton County |
| 186 | Fulton Independent |
| 261 | Hickman County |
| 493 | Pineville Independent |
| 537 | Southgate Independent |
| 593 | Williamstown Independent |
| 595 | Wolfe County |
| 602 | Kentucky School for the Blind District |
| 606 | Model Laboratory Schools at EKU |
| 610 | Craft Academy |
| 880 | Kentucky National Guard Youth ChalleNGe Academies |

Fayette codes beginning `165`: 201 rows on each copy, every field the same. State `999000`: 3 rows, every field the same (behavior 69, climate 67, leadership 70).

Header, then one datasets row for each check. The cells are the file's text, including empty fields.

```
School Year,County Number,County Name,District Number,District Name,School Number,School Name,School Code,State School Id,NCES ID,CO-OP,CO-OP Code,School Type,Impact Measure,Impact Value
20252026,,,999,All Districts,,All Schools,999000,,,,,,Managing Student Behavior,69
20252026,034,FAYETTE,165,Fayette County,,All Schools,165000,,2101860,CKEC,901,,Managing Student Behavior,69
20252026,034,FAYETTE,165,Fayette County,170,Bryan Station High School,165170,034165170,210186000388,CKEC,901,A1,Managing Student Behavior,69
20252026,001,ADAIR,001,Adair County,,All Schools,001000,,2100030,GRREC,902,,Managing Student Behavior,64
20252026,011,BOYLE,603,Kentucky School for the Deaf District,,All Schools,603000,,2100095,CKEC,901,,Managing Student Behavior,63
```

The UAT twin of the last row is the same cells with District Name `Kentucky School for the Deaf` and the same Impact Value 63. Adair `001000` is not in the UAT file.

Processed output is not regenerated. These rows are the source file, not a new `teacher_working_conditions.csv` output.

## Caveat check

No value is changed. The notes below say where the current caveat sits and what this batch did to the wording.

| Issue | Decided caveat | This batch |
| --- | --- | --- |
| #68 | Keep the 2026 turnover rows. District and state rows are a different measure. Fayette's district row is 407 against a school sum of 582. Record 0028. | Already in `KPIS.md`. Added that `teacher_turnover_rate` is the covariate the analysis dataset loads. |
| #70 | Keep the 2026 emergency and provisional rows. State percent 14.3 is not the next point after 1.9. Record 0029. | Already in `KPIS.md`. Added that `base_analysis_dataset.py` loads `emergency_provisional_teacher_rate`. When this note was written the catalog `source` was `kde_staff_reports`. The J1 catalog cleanup since set it to `kde_teacher_certification`. This batch did not change it. |
| #112 | The 5,407 cells and the 699 cells stay as published, with a warning, until KDE republishes those years. Records 0009 and 0051. The 27 October 2026 recheck is not the end condition. | `KPIS.md` and `notes/116` said the 0.0 rates "are on the 27 October KDE recheck". That end condition is replaced with the 0009 / 0051 wording. The 2023 comparability sentence in record 0011 is not amended. |
| #78 | Keep the published 2026 IB tested counts. Leave the derived 2026 rate unstored. Record 0032. | `KPIS.md` said the issue "holds whether to keep" the count. It now cites 0032. |
| #101 | Keep the type-file star. Do not store the location or context Total. Record 0036. | `KPIS.md` already says that, with the kde-recheck label. No wording change. No KDE answer is recorded here. |
| #94 | Keep each level's published career-readiness cells. The issue asks KDE whether the state row is a different population, and whether the 2019 more-than-one indicator was fully reported. | `README.md` already says the state cell does not equal the sum of the districts where every district cell is a number. No wording change. |
| #86 | Keep each level's published pathway value. Do not replace the state cell with the district sum. The issue body is the 19 October decision. | `KPIS.md` and `README.md` already say that, flagged kde-recheck. No wording change. |
| #84 | Keep `courses_offered_count` as NA with suppressed Y when the total is starred. Record 0034. | `KPIS.md` already says a starred total counts as suppressed and grade cells are not summed into the count. No wording change. |
| #91 | The 91 Breathitt rows whose school year is `20022023` are year 2023. No code change. The issue flags them for KDE. | `KPIS.md` already says that. No wording change. |
| #72 | Keep the 0% rates on a 0 or blank teacher count. Record 0030, quoting the #72 comment. | `KPIS.md` said the issue "proposes skipping" those rates. It now cites 0030. `notes/104` still uses "proposes" as the question the mapping wrote down. The decision is 0030. |
| #69 | Keep the 0% and 100-400% rows where the teacher count is 0. Record 0030, quoting the #69 comment. | `KPIS.md` already says the placeholder is stored and cites #69. `notes/104` still says the issue proposes skipping. The decision is 0030. |
| #108 | Glasgow conflicting totals stay NA with suppressed Y. Record 0016. | Comment 7224584646 (27 Nov 2026, 12:36 PM ET): "The code is done (Glasgow conflicting totals are stored as NA/Y). This issue remains open only for the KDE recheck." Noted in `notes/105.md` and on record 0016. Storage is not changed. |

### #68

`KPIS.md` already says Fayette County's 2026 district row is 407 against a school sum of 582. That figure is issue #68's body, checked against the registered files on 18 October 2026. This batch did not re-count the turnover file.

`analysis/scripts/base_analysis_dataset.py` reads metric `teacher_turnover_rate`, keeps All Students, drops suppressed Y, and drops `school_name == ---District Total---`. School rows are what it keeps. `analysis/config/covariate_catalog.yaml` lists `teacher_turnover_rate` (source `kde_staff_reports` in that file). A search of `html/` and `scripts/` in this repository found no use of the metric name. The FCPS equity dashboard repository was not read.

### #70

`KPIS.md` already says not to read the 2026 state percent 14.3 as the next point after 1.9. Those figures are issue #70's body, checked 18 October 2026. This batch did not re-count the certification file.

The same analysis script reads `emergency_provisional_teacher_rate` and renames the column to `emergency_provisional_rate`. The catalog entry `emergency_provisional_rate` had `source: kde_staff_reports` when this note was written. The J1 catalog cleanup since set it to `kde_teacher_certification`, as record 0029 says. This batch does not edit the catalog. Record 0029's status stays `accepted`.

### #112

Records 0009 and 0051: both cell sets stay as published, suppressed N, with a warning, until KDE republishes those years. The 25 October 2026 comment is https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/112#issuecomment-6400283769. Record 0009 says the 27 October 2026 recheck is not the end condition.

This batch did not re-count the 5,407 or the 699. It does not say whether KDE has republished those years since 25 October 2026. The warning stays.

The separate 2023 completion-rate comparability sentence (record 0011, `notes/114`, and the `notes/104` sentence that the 2023 rate is on the 27 October recheck) is not this amendment.

### #42

Record 0059. Issue #42 was closed on GitHub at 12:36 PM ET on 27 Nov 2026. This note does not close it. The NaN guard and the post-2025 analysis stay out of scope under record 0002.
