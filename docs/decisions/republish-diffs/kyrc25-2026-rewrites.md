# KYRC25 files rewritten after the February 2026 post-review copy

This note pairs with record 0052 in pull request [#120](https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/120) (branch `cursor/decision-log-bbca`). Record 0052 is Scott's rule, 25 October 2026, 12:11 p.m. Eastern: use the latest public KDE file after the review window closes, unless it is visibly broken, and record the differences on each republish.

The comparison was run on 25 October 2026. It reads public file headers and the downloaded bytes. It does not change pipeline code, config, or data. The cause of a rewrite is stated only when the file itself shows it. Where the file does not say why it changed, this note says so.

The script is `docs/decisions/republish-diffs/scripts/diff_kyrc25_2026_rewrites.py`. The counts below are that script's output.

## The February copy

The 2 February 2026 DAC email, "Final Data Files After 10-Day Regulatory Data Review," said the updated datasets would be available on 3 February 2026. That email is the one cited in record 0052: https://content.govdelivery.com/accounts/KYDE/bulletins/4075ddd

No separate object dated 3 February 2026 was found. These names return 404 on the Open House historical host, the OAA temporary host, `datasets`, and `datasetsuat6051276`:

- `Accountable_Assessment_Performance_2025.CSV`
- `Assessment_Performance_by_Grade_2025.CSV`
- `Postsecondary_Readiness_2025.CSV`
- `Graduation_Rate_2025.CSV`

The earliest copy found for each of the three files is the Open House HistoricalDatasets object. Each one has Last-Modified on Tuesday, 10 February 2026. A public HEAD did not return an earlier blob version. Whether those bytes are the same as the file that appeared on 3 February cannot be told from the headers. This note uses the 10 February historical object as the February copy and names it that way.

`School Year` is `20242025` on every row of all three files, in every copy.

Fayette rows are district number `165`. That column is 3 digits. The district total is school code `165000`. Other Fayette schools are school codes `165xxx`. State rows are district number `999`, district name `All Districts`, school code `999000`, school name `All Schools`.

## Summary

| File | February copy | Later copy compared | Added rows | Removed rows | Changed rows | Changed cells | Fayette cells | State cells | Classification |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `KYRC25_ACCT_Kentucky_Summative_Assessment.csv` | Historical, 10 Feb 2026 | `datasets`, 14 May 2026. UAT is the same bytes. | 0 | 0 | 342 | 868 | 0 | 22 | Likely correction |
| `KYRC25_ACCT_Postsecondary_Readiness.csv` | Historical, 10 Feb 2026 | `datasets`, 9 Oct 2026. UAT is the same bytes. | 0 | 0 | 3,109 | 6,218 | 90, each ` *` to `*` | 0 | Leading space removed from `*`. Rates unchanged. |
| `KYRC25_ACCT_4_Year_High_School_Graduation.csv` | Historical, 10 Feb 2026 | `datasets`, 6 Aug 2026. UAT is the same bytes. Historical is the same bytes too. | 0 | 0 | 0 | 0 | 0 | 0 | No cell change |

UAT is `datasetsuat6051276`. For all three files its MD5 matches `datasets`. Last-Modified and ETag still differ.

## Kentucky Summative Assessment

File: `KYRC25_ACCT_Kentucky_Summative_Assessment.csv`. This is the accountability file (level and subject), not the by-grade `KYRC25_ASMT_` file. The by-grade file is under "Not compared."

Natural key: `School Year`, `School Code`, `Level`, `Subject`, `Demographic`. Both copies have 278,464 keys and no duplicate keys. The 23 column names match, in the same order.

### Copies

| Copy | URL | Last-Modified | Bytes | MD5 | Data rows |
| --- | --- | --- | --- | --- | --- |
| Historical | https://www.education.ky.gov/Open-House/data/HistoricalDatasets/KYRC25_ACCT_Kentucky_Summative_Assessment.csv | Tue, 10 Feb 2026 20:34:59 GMT (3:34 p.m. Eastern) | 47,436,743 | `c0e98f580274b04a381ec51ac83d20b1` | 278,464 |
| `datasets` | https://kdeschoolreportcard.blob.core.windows.net/datasets/KYRC25_ACCT_Kentucky_Summative_Assessment.csv | Thu, 14 May 2026 19:11:31 GMT (3:11 p.m. Eastern) | 47,436,758 | `a8498b4d02c2b9c04a1022ab3ee3b844` | 278,464 |
| UAT | https://kdeschoolreportcard.blob.core.windows.net/datasetsuat6051276/KYRC25_ACCT_Kentucky_Summative_Assessment.csv | Tue, 06 Oct 2026 20:27:22 GMT (4:27 p.m. Eastern) | 47,436,758 | `a8498b4d02c2b9c04a1022ab3ee3b844` | 278,464 |

None of the three HEADs returned `Content-MD5`. Historical ETag `"{447B802D-DC63-4D0C-A2E1-EAF6C712AC0A},5"`. Datasets ETag `0x8DEB1EC9BA61DC6`. UAT ETag `0x8DF23E83A498A7F`. The MD5 above is of the downloaded bytes. `datasets` and UAT are the same bytes. The historical file is not.

### What changed

Added rows: 0. Removed rows: 0. Changed rows: 342. Changed cells: 868.

The changed columns are only the measure columns and, on one row, `Suppressed`. County, district, school, NCES, co-op, and school-type cells do not change.

| Column | Changed cells | Number to a different number | Star to a number | Other |
| --- | --- | --- | --- | --- |
| Content Index | 336 | 335 | 0 | 1 (blank to `117.1`) |
| Novice | 180 | 179 | 1 | 0 |
| Proficient / Distinguished | 124 | 123 | 1 | 0 |
| Proficient | 90 | 89 | 1 | 0 |
| Distinguished | 69 | 68 | 1 | 0 |
| Apprentice | 68 | 67 | 1 | 0 |
| Suppressed | 1 | 0 | 0 | 1 (`Y` to `N`) |
| Total | 868 | 861 | 5 | 2 |

The five star-to-number cells and the blank content index and the `Y` to `N` flag are one row: Henderson County district, middle-school mathematics, Gifted and Talented. It is described below.

Of the 861 number-to-number cells, the absolute change is:

| Absolute change | Cells |
| --- | ---: |
| 0.1 | 87 |
| more than 0.1 and less than 1 | 148 |
| 1 | 311 |
| more than 1 and at most 5 | 186 |
| more than 5 | 129 |

774 of the 861 numeric cells change by more than 0.15. The median absolute change is 1. The largest absolute change is 63.7 (a content index), on Johnson Central High School `285115`, high school, mathematics, Students with Disabilities (IEP): content index 107.6 to 43.9, and proficient/distinguished 91 to 36. The signed mean across the 861 cells is about −1.39 (May minus February).

Direction of the numeric cells:

| Column | Cells | Direction |
| --- | ---: | --- |
| Novice | 179 | all increase (median +1, mean +3.67) |
| Apprentice | 67 | 61 decrease, 6 increase (median −1) |
| Proficient | 89 | all decrease (median −2) |
| Distinguished | 68 | all decrease (median −2) |
| Proficient / Distinguished | 123 | all decrease (median −1, mean −4.48) |
| Content Index | 335 | all decrease (median −0.3, mean −2.02) |

On the 342 changed rows, `Suppressed` is `N` to `N` on 337 rows, `Y` to `Y` on 4 rows, and `Y` to `N` on the Henderson row.

By the largest absolute numeric change on the row: 61 rows move by at most 0.1, 195 rows move by more than 0.1 and at most 1, 49 rows move by more than 1 and at most 5, 36 rows move by more than 5, and 1 row has no number-to-number cell (Henderson). All 61 of the 0.1-point rows change only the content index. 110 rows change only the content index; 61 of those are the 0.1-point rows, and 49 move the content index by more than 0.1.

### Where the rows are

Subjects present in the file, and changed rows:

| Subject | Rows in the file | Changed rows |
| --- | ---: | ---: |
| Reading | 56,256 | 80 |
| Mathematics | 56,256 | 242 |
| Science | 55,808 | 0 |
| Social Studies | 55,360 | 0 |
| Combined Writing | 54,784 | 20 |

Level of the changed rows: middle school 139, high school 124, elementary school 79.

The 342 rows are in 12 districts plus the state, and in 15 schools plus 12 district totals plus the state. 125 of the 129 cells that move by more than 5 points are Johnson County. 34 of the 36 rows whose largest change is more than 5 points are Johnson County.

| Changed rows | District | School | Code |
| ---: | --- | --- | --- |
| 31 | Johnson County | All Schools | 285000 |
| 30 | Johnson County | Johnson Central High School | 285115 |
| 17 | All Districts | All Schools | 999000 |
| 12 | Edmonson County | All Schools | 151000 |
| 12 | Edmonson County | Edmonson County High School | 151060 |
| 12 | Jefferson County | Fern Creek Elementary | 275011 |
| 12 | Jefferson County | Farnsley Middle | 275049 |
| 12 | Jefferson County | J. Graham Brown School | 275165 |
| 12 | Kenton County | Turkey Foot Middle School | 291103 |
| 12 | Lawrence County | All Schools | 315000 |
| 12 | Lawrence County | Louisa Middle School | 315200 |
| 12 | Madison County | Waco Elementary School | 365120 |
| 12 | Shelby County | All Schools | 531000 |
| 12 | Shelby County | Marnel C. Moorman School | 531054 |
| 12 | Todd County | All Schools | 551000 |
| 12 | Todd County | South Todd Elementary School | 551015 |
| 12 | Walton-Verona Independent | All Schools | 567000 |
| 12 | Walton-Verona Independent | Walton-Verona High School | 567020 |
| 11 | Henderson County | All Schools | 251000 |
| 11 | Henderson County | Henderson County South Middle School | 251150 |
| 11 | McCracken County | Lone Oak Middle School | 395085 |
| 11 | Shelby County | Shelby County East Middle School | 531110 |
| 8 | Kenton County | All Schools | 291000 |
| 8 | Madison County | All Schools | 365000 |
| 7 | Christian County | All Schools | 115000 |
| 7 | Jefferson County | All Schools | 275000 |
| 6 | Jefferson County | Jefferson County Traditional Middle | 275396 |
| 4 | McCracken County | All Schools | 395000 |

Christian County's changed rows are the district total only. The other 11 districts have at least one school in the table along with the district total. Jefferson County has four schools. Shelby County has two.

26 demographic labels appear on a changed row. The largest row counts are Non-Foster 27, All Students 25, and Non-Gifted and Talented 25. The changes are not confined to one demographic.

### Examples

Johnson Central High School `285115`, high school mathematics, All Students. `Suppressed` stays `N`.

| Column | February | May |
| --- | --- | --- |
| Novice | 14 | 22 |
| Apprentice | 24 | 24 |
| Proficient | 38 | 35 |
| Distinguished | 24 | 18 |
| Proficient / Distinguished | 62 | 53 |
| Content Index | 79.9 | 69.9 |

February novice + apprentice + proficient + distinguished = 100. May = 99. Proficient + distinguished equals the published proficient/distinguished cell in both copies (62, then 53).

Same school, high school reading, All Students. `Suppressed` stays `N`.

| Column | February | May |
| --- | --- | --- |
| Novice | 9 | 11 |
| Apprentice | 23 | 22 |
| Proficient | 37 | 36 |
| Distinguished | 31 | 31 |
| Proficient / Distinguished | 68 | 67 |
| Content Index | 87.7 | 85.4 |

Both copies sum to 100 on the four levels. Proficient + distinguished equals the published combined cell in both copies (68, then 67).

State `999000`, high school mathematics, All Students. Novice 31, apprentice 29, proficient 28, distinguished 12, and proficient/distinguished 40 are the same in both copies. The content index changes from 57.7 to 57.6. The four levels sum to 100. Proficient + distinguished equals 40.

The one row whose four-level sum moves by 2 points is Johnson Central, high school reading, Non-Gifted and Talented: the four levels sum to 101 in February and 99 in May. Proficient/distinguished goes from 67 to 65.

Henderson County district `251000`, middle school mathematics, Gifted and Talented:

| Column | February | May |
| --- | --- | --- |
| Suppressed | Y | N |
| Novice | * | 1 |
| Apprentice | * | 3 |
| Proficient | * | 16 |
| Distinguished | * | 80 |
| Proficient / Distinguished | * | 96 |
| Content Index | blank | 117.1 |

May's four levels sum to 100. Proficient + distinguished equals 96.

Four other changed rows keep every performance cell as `*` and keep `Suppressed` as `Y`. Only the content index changes: Turkey Foot Middle, combined writing, African American, 52.2 to 50.0; McCracken County district, middle school mathematics, African American, 49.3 to 48.7; Lone Oak Middle, the same subject and demographic, 50.0 to 48.7; Shelby County district, middle school mathematics, English Learner including Monitored, 27.0 to 26.6.

### Fayette and state

Fayette: 9,440 rows with district number `165`. Changed cells: 0.

State: 480 rows. Changed cells: 22, all on school code `999000`. Every state cell that changed is in this table.

| Level | Subject | Demographic | Column | February | May |
| --- | --- | --- | --- | --- | --- |
| Elementary School | Mathematics | Migrant | Content Index | 37.4 | 37.3 |
| High School | Mathematics | All Students | Content Index | 57.7 | 57.6 |
| High School | Mathematics | Non Economically Disadvantaged | Novice | 20 | 21 |
| High School | Mathematics | Non English Learner | Content Index | 59.9 | 59.8 |
| High School | Mathematics | Non-English Learner or monitored | Content Index | 59.9 | 59.8 |
| High School | Mathematics | Non-Foster | Content Index | 58.1 | 58.0 |
| High School | Mathematics | Non-Gifted and Talented | Content Index | 55.0 | 54.9 |
| High School | Mathematics | Non-Homeless | Content Index | 58.3 | 58.2 |
| High School | Mathematics | Students with Disabilities (IEP) | Novice | 55 | 56 |
| High School | Mathematics | Students with Disabilities (IEP) | Content Index | 29.5 | 29.1 |
| High School | Mathematics | Students with Disabilities/IEP Regular Assessment | Content Index | 26.6 | 26.2 |
| High School | Mathematics | Students with Disabilities/IEP with Accommodations | Proficient | 11 | 10 |
| High School | Mathematics | Students with Disabilities/IEP with Accommodations | Proficient / Distinguished | 13 | 12 |
| High School | Mathematics | Students with Disabilities/IEP with Accommodations | Content Index | 28.0 | 27.4 |
| High School | Mathematics | White (non-Hispanic) | Novice | 26 | 27 |
| High School | Mathematics | White (non-Hispanic) | Content Index | 62.9 | 62.8 |
| High School | Reading | Non-Migrant | Content Index | 63.6 | 63.5 |
| High School | Reading | Students with Disabilities (IEP) | Distinguished | 3 | 2 |
| High School | Reading | Students with Disabilities (IEP) | Content Index | 30.3 | 30.2 |
| High School | Reading | Students with Disabilities/IEP Regular Assessment | Content Index | 28.2 | 28.1 |
| High School | Reading | Students with Disabilities/IEP with Accommodations | Content Index | 29.5 | 29.3 |
| Middle School | Mathematics | English Learner | Content Index | 24.9 | 24.8 |

### Do the levels still add up

On 337 changed rows, all four of novice, apprentice, proficient, and distinguished are numbers in both copies. Their sum is 99, 100, or 101 in both copies:

| February sum | May sum | Rows |
| ---: | ---: | ---: |
| 100 | 100 | 146 |
| 100 | 101 | 52 |
| 101 | 101 | 32 |
| 101 | 100 | 32 |
| 99 | 100 | 32 |
| 99 | 99 | 27 |
| 100 | 99 | 15 |
| 101 | 99 | 1 |

99 and 101 already occur as February sums on these rows (59 rows sum to 99 in February, 65 sum to 101). The May file does not introduce a new sum on these rows.

On those same 337 rows, the absolute gap between proficient + distinguished and the published proficient/distinguished cell has a maximum of 1.0 in the February copy and 1.0 in the May copy.

No novice, apprentice, proficient, distinguished, or proficient/distinguished cell on a changed row is outside 0–100 in either copy.

The content index is a different scale. The February file has 135,822 numeric content-index cells, 1,146 of them above 100, maximum 122.7. The May file has 135,823 numeric cells, 1,146 above 100, maximum 122.7. The extra numeric cell is the Henderson blank becoming 117.1. A published performance cell equal to 1 occurs 4,597 times in February and 4,598 times in May.

### Classification

Likely correction.

The pattern that supports that label:

- 342 of 278,464 rows change. The key set and the columns stay the same. Science and social studies do not change.
- The changed rows cluster in 15 schools and their district totals, plus the state. Johnson County holds 61 of the 342 rows and 125 of the 129 cells that move by more than 5 points.
- The numeric moves have a direction. Novice only increases. Proficient, distinguished, proficient/distinguished, and content index only decrease. A 0.1 rounding pass would move indexes up and down. These content-index cells only move down, and 129 cells move by more than 5 points.
- The four performance levels still sum to 99, 100, or 101. Proficient + distinguished stays within 1 point of the published combined cell. The state changes are 0.1 to 0.6 on a content index (12 cells at 0.1; 0.2, 0.4, 0.4, 0.6 on HS students-with-disabilities rows) or 1 point on a percent (6 cells). Fayette does not change.
- One row leaves suppression (Henderson, middle school mathematics, Gifted and Talented). Its new percents sum to 100. A published 1 is already common in the February file. Four other rows stay suppressed and only move the content index.

The pattern that argues against the other labels:

- A file-wide method change, such as a new rounding rule or a new suppression rule, would show up across subjects or across schools. Science and social studies are unchanged, and the suppression flag changes on one row.
- Breakage of the kind named in the request (truncation, impossible values, missing columns) is not what these bytes show. The row count, the columns, and the 0–100 percents are intact. This is not the 39-row file with percents above 100 that record 0052 uses as the broken-file example.

The file does not say what was corrected. There is no change-log column and no version note in the header or the cells. This comparison did not contact KDE. The cause is not established.

### What the pipeline loads, and what 0052 points to

`config/kde_sources.yaml` registers this file with `url: kyrc25`, which `data/prepare_kde_data.py` resolves to `base_url_kyrc25`, the `datasets` host. The downloader also HEADs UAT for a `KYRC25_` name. When `Content-MD5` is missing or the hashes match, it keeps the `datasets` URL. Both HEADs here omitted `Content-MD5`, so the downloader keeps `datasets`. UAT is the same bytes, so the later UAT timestamp does not change a value.

Record 0052 points to the latest public file after the review window, unless that copy is visibly broken. The review in the 2 February email had already closed, and the May file is after 3 February. On the checks in this note the May file is not the broken-file pattern in 0052. The rule points to the May `datasets` file. That is the file the config already registers. This note records the differences. The config was not changed.

Current pipeline outputs do not change, because the registered copy is already the May `datasets` file.

Whether any shipped output used the February summative copy cannot be determined from this repository. `*.csv` and `data/raw/` are gitignored. `config/kde_sources.yaml` registers the `datasets` URL for this file and stores no MD5. No other committed file records the February MD5 `c0e98f580274b04a381ec51ac83d20b1`.

Those registrations are separate from the copy conflicts in issue [#123](https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/123), which tracks the 18 October choices for novice equity (#58), ineffective teachers (#64), and out-of-field teachers (#66).

## Postsecondary readiness

File: `KYRC25_ACCT_Postsecondary_Readiness.csv`.

Natural key: `School Year`, `School Code`, `Demographic`. Both copies have 10,719 keys and no duplicate keys. The 17 column names match.

### Copies

| Copy | URL | Last-Modified | Bytes | MD5 | Data rows |
| --- | --- | --- | --- | --- | --- |
| Historical | https://www.education.ky.gov/Open-House/data/HistoricalDatasets/KYRC25_ACCT_Postsecondary_Readiness.csv | Tue, 10 Feb 2026 20:37:59 GMT (3:37 p.m. Eastern) | 1,371,527 | `54c8b0bd22b81beebfeefdeec25af9b1` | 10,719 |
| `datasets` | https://kdeschoolreportcard.blob.core.windows.net/datasets/KYRC25_ACCT_Postsecondary_Readiness.csv | Fri, 09 Oct 2026 19:39:31 GMT (3:39 p.m. Eastern) | 1,365,309 | `1ee5f118a10ecd6f9408c85a1c939ded` | 10,719 |
| UAT | https://kdeschoolreportcard.blob.core.windows.net/datasetsuat6051276/KYRC25_ACCT_Postsecondary_Readiness.csv | Fri, 09 Oct 2026 19:41:42 GMT (3:41 p.m. Eastern) | 1,365,309 | `1ee5f118a10ecd6f9408c85a1c939ded` | 10,719 |

Historical HEAD did not return `Content-MD5`. Datasets and UAT both returned `Content-MD5: HuXxGKEOzW+UCMhaHJOd7Q==`, which matches the MD5 of the downloaded bytes. Historical ETag `"{A3A084CF-A638-4B65-97ED-8D6F72434641},5"`. Datasets ETag `0x8DF263D0A466518`. UAT ETag `0x8DF263D586CABC5`. `datasets` and UAT are the same bytes. UAT's Last-Modified is two minutes later.

### What changed

Added rows: 0. Removed rows: 0. Changed rows: 3,109. Changed cells: 6,218.

| Column | Changed cells | Kind |
| --- | ---: | --- |
| Postsecondary Rate | 3,109 | leading space on `*` |
| Postsecondary Rate With Bonus | 3,109 | leading space on `*` |

Every changed cell is the same pair: the February cell is ` *` (a space, then an asterisk) and the October cell is `*`. There is one such pair, repeated 6,218 times. The October file is 6,218 bytes smaller than the February file, one byte for each of those cells.

No numeric rate changes. The `Suppressed` column does not change. On all 3,109 changed rows the flag is `Y` in both copies. Both rate columns change together on each of those rows.

The state file has 27 rows and no star cells in either copy. State All Students is 83.3 and 89.1 in both copies, `Suppressed` `N`. Fayette district All Students, school `165000`, is 84.2 and 91.3 in both copies, `Suppressed` `N`. The same is true of these All Students rates: Henry Clay `165039` 72.1 and 73.5, Lafayette `165060` 88.6 and 93.8, Tates Creek `165105` 87.0 and 97.4, Frederick Douglass `165125` 88.7 and 97.4, Bryan Station `165170` 87.9 and 98.5, Paul Laurence Dunbar `165200` 83.6 and 90.6.

### Fayette and state

State changed cells: 0.

Fayette has 189 rows. 90 cells change, on 45 rows, across the district total and six high schools. Each cell is ` *` in February and `*` in October. `Suppressed` stays `Y`.

| School | Code | Demographic | Column | February | October |
| --- | --- | --- | --- | --- | --- |
| All Schools | 165000 | American Indian or Alaska Native | Postsecondary Rate | ` *` | `*` |
| All Schools | 165000 | American Indian or Alaska Native | Postsecondary Rate With Bonus | ` *` | `*` |
| All Schools | 165000 | Military Dependent | Postsecondary Rate | ` *` | `*` |
| All Schools | 165000 | Military Dependent | Postsecondary Rate With Bonus | ` *` | `*` |
| All Schools | 165000 | Non-Military | Postsecondary Rate | ` *` | `*` |
| All Schools | 165000 | Non-Military | Postsecondary Rate With Bonus | ` *` | `*` |
| Henry Clay High School | 165039 | Foster Care | Postsecondary Rate | ` *` | `*` |
| Henry Clay High School | 165039 | Foster Care | Postsecondary Rate With Bonus | ` *` | `*` |
| Henry Clay High School | 165039 | Homeless | Postsecondary Rate | ` *` | `*` |
| Henry Clay High School | 165039 | Homeless | Postsecondary Rate With Bonus | ` *` | `*` |
| Henry Clay High School | 165039 | Military Dependent | Postsecondary Rate | ` *` | `*` |
| Henry Clay High School | 165039 | Military Dependent | Postsecondary Rate With Bonus | ` *` | `*` |
| Henry Clay High School | 165039 | Non-Foster | Postsecondary Rate | ` *` | `*` |
| Henry Clay High School | 165039 | Non-Foster | Postsecondary Rate With Bonus | ` *` | `*` |
| Henry Clay High School | 165039 | Non-Homeless | Postsecondary Rate | ` *` | `*` |
| Henry Clay High School | 165039 | Non-Homeless | Postsecondary Rate With Bonus | ` *` | `*` |
| Henry Clay High School | 165039 | Non-Migrant | Postsecondary Rate | ` *` | `*` |
| Henry Clay High School | 165039 | Non-Migrant | Postsecondary Rate With Bonus | ` *` | `*` |
| Henry Clay High School | 165039 | Non-Military | Postsecondary Rate | ` *` | `*` |
| Henry Clay High School | 165039 | Non-Military | Postsecondary Rate With Bonus | ` *` | `*` |
| Lafayette High School | 165060 | American Indian or Alaska Native | Postsecondary Rate | ` *` | `*` |
| Lafayette High School | 165060 | American Indian or Alaska Native | Postsecondary Rate With Bonus | ` *` | `*` |
| Lafayette High School | 165060 | Homeless | Postsecondary Rate | ` *` | `*` |
| Lafayette High School | 165060 | Homeless | Postsecondary Rate With Bonus | ` *` | `*` |
| Lafayette High School | 165060 | Migrant | Postsecondary Rate | ` *` | `*` |
| Lafayette High School | 165060 | Migrant | Postsecondary Rate With Bonus | ` *` | `*` |
| Lafayette High School | 165060 | Military Dependent | Postsecondary Rate | ` *` | `*` |
| Lafayette High School | 165060 | Military Dependent | Postsecondary Rate With Bonus | ` *` | `*` |
| Lafayette High School | 165060 | Non-Homeless | Postsecondary Rate | ` *` | `*` |
| Lafayette High School | 165060 | Non-Homeless | Postsecondary Rate With Bonus | ` *` | `*` |
| Lafayette High School | 165060 | Non-Migrant | Postsecondary Rate | ` *` | `*` |
| Lafayette High School | 165060 | Non-Migrant | Postsecondary Rate With Bonus | ` *` | `*` |
| Lafayette High School | 165060 | Non-Military | Postsecondary Rate | ` *` | `*` |
| Lafayette High School | 165060 | Non-Military | Postsecondary Rate With Bonus | ` *` | `*` |
| Tates Creek High School | 165105 | Foster Care | Postsecondary Rate | ` *` | `*` |
| Tates Creek High School | 165105 | Foster Care | Postsecondary Rate With Bonus | ` *` | `*` |
| Tates Creek High School | 165105 | Homeless | Postsecondary Rate | ` *` | `*` |
| Tates Creek High School | 165105 | Homeless | Postsecondary Rate With Bonus | ` *` | `*` |
| Tates Creek High School | 165105 | Migrant | Postsecondary Rate | ` *` | `*` |
| Tates Creek High School | 165105 | Migrant | Postsecondary Rate With Bonus | ` *` | `*` |
| Tates Creek High School | 165105 | Military Dependent | Postsecondary Rate | ` *` | `*` |
| Tates Creek High School | 165105 | Military Dependent | Postsecondary Rate With Bonus | ` *` | `*` |
| Tates Creek High School | 165105 | Non-Foster | Postsecondary Rate | ` *` | `*` |
| Tates Creek High School | 165105 | Non-Foster | Postsecondary Rate With Bonus | ` *` | `*` |
| Tates Creek High School | 165105 | Non-Homeless | Postsecondary Rate | ` *` | `*` |
| Tates Creek High School | 165105 | Non-Homeless | Postsecondary Rate With Bonus | ` *` | `*` |
| Tates Creek High School | 165105 | Non-Migrant | Postsecondary Rate | ` *` | `*` |
| Tates Creek High School | 165105 | Non-Migrant | Postsecondary Rate With Bonus | ` *` | `*` |
| Tates Creek High School | 165105 | Non-Military | Postsecondary Rate | ` *` | `*` |
| Tates Creek High School | 165105 | Non-Military | Postsecondary Rate With Bonus | ` *` | `*` |
| Frederick Douglass High School | 165125 | Foster Care | Postsecondary Rate | ` *` | `*` |
| Frederick Douglass High School | 165125 | Foster Care | Postsecondary Rate With Bonus | ` *` | `*` |
| Frederick Douglass High School | 165125 | Homeless | Postsecondary Rate | ` *` | `*` |
| Frederick Douglass High School | 165125 | Homeless | Postsecondary Rate With Bonus | ` *` | `*` |
| Frederick Douglass High School | 165125 | Migrant | Postsecondary Rate | ` *` | `*` |
| Frederick Douglass High School | 165125 | Migrant | Postsecondary Rate With Bonus | ` *` | `*` |
| Frederick Douglass High School | 165125 | Military Dependent | Postsecondary Rate | ` *` | `*` |
| Frederick Douglass High School | 165125 | Military Dependent | Postsecondary Rate With Bonus | ` *` | `*` |
| Frederick Douglass High School | 165125 | Non-Foster | Postsecondary Rate | ` *` | `*` |
| Frederick Douglass High School | 165125 | Non-Foster | Postsecondary Rate With Bonus | ` *` | `*` |
| Frederick Douglass High School | 165125 | Non-Homeless | Postsecondary Rate | ` *` | `*` |
| Frederick Douglass High School | 165125 | Non-Homeless | Postsecondary Rate With Bonus | ` *` | `*` |
| Frederick Douglass High School | 165125 | Non-Migrant | Postsecondary Rate | ` *` | `*` |
| Frederick Douglass High School | 165125 | Non-Migrant | Postsecondary Rate With Bonus | ` *` | `*` |
| Frederick Douglass High School | 165125 | Non-Military | Postsecondary Rate | ` *` | `*` |
| Frederick Douglass High School | 165125 | Non-Military | Postsecondary Rate With Bonus | ` *` | `*` |
| Bryan Station High School | 165170 | Asian | Postsecondary Rate | ` *` | `*` |
| Bryan Station High School | 165170 | Asian | Postsecondary Rate With Bonus | ` *` | `*` |
| Bryan Station High School | 165170 | Foster Care | Postsecondary Rate | ` *` | `*` |
| Bryan Station High School | 165170 | Foster Care | Postsecondary Rate With Bonus | ` *` | `*` |
| Bryan Station High School | 165170 | Migrant | Postsecondary Rate | ` *` | `*` |
| Bryan Station High School | 165170 | Migrant | Postsecondary Rate With Bonus | ` *` | `*` |
| Bryan Station High School | 165170 | Non-Foster | Postsecondary Rate | ` *` | `*` |
| Bryan Station High School | 165170 | Non-Foster | Postsecondary Rate With Bonus | ` *` | `*` |
| Bryan Station High School | 165170 | Non-Migrant | Postsecondary Rate | ` *` | `*` |
| Bryan Station High School | 165170 | Non-Migrant | Postsecondary Rate With Bonus | ` *` | `*` |
| Paul Laurence Dunbar High School | 165200 | American Indian or Alaska Native | Postsecondary Rate | ` *` | `*` |
| Paul Laurence Dunbar High School | 165200 | American Indian or Alaska Native | Postsecondary Rate With Bonus | ` *` | `*` |
| Paul Laurence Dunbar High School | 165200 | Homeless | Postsecondary Rate | ` *` | `*` |
| Paul Laurence Dunbar High School | 165200 | Homeless | Postsecondary Rate With Bonus | ` *` | `*` |
| Paul Laurence Dunbar High School | 165200 | Military Dependent | Postsecondary Rate | ` *` | `*` |
| Paul Laurence Dunbar High School | 165200 | Military Dependent | Postsecondary Rate With Bonus | ` *` | `*` |
| Paul Laurence Dunbar High School | 165200 | Non-Foster | Postsecondary Rate | ` *` | `*` |
| Paul Laurence Dunbar High School | 165200 | Non-Foster | Postsecondary Rate With Bonus | ` *` | `*` |
| Paul Laurence Dunbar High School | 165200 | Non-Homeless | Postsecondary Rate | ` *` | `*` |
| Paul Laurence Dunbar High School | 165200 | Non-Homeless | Postsecondary Rate With Bonus | ` *` | `*` |
| Paul Laurence Dunbar High School | 165200 | Non-Migrant | Postsecondary Rate | ` *` | `*` |
| Paul Laurence Dunbar High School | 165200 | Non-Migrant | Postsecondary Rate With Bonus | ` *` | `*` |
| Paul Laurence Dunbar High School | 165200 | Non-Military | Postsecondary Rate | ` *` | `*` |
| Paul Laurence Dunbar High School | 165200 | Non-Military | Postsecondary Rate With Bonus | ` *` | `*` |

### Classification

The cells do not show a correction of a rate, a change of which rows are suppressed, or a broken file.

What the bytes show is one character: a leading space removed from `*`, on both rate columns of the same 3,109 rows, with `Suppressed` staying `Y`. The row count and the columns are unchanged. Published rates, including the state rates 83.3 and 89.1 and the Fayette rates 84.2 and 91.3, match.

That is not a new suppression rule. The same rows stay suppressed. It is not truncation or a missing column. Calling it a rounding change does not fit, because no rate numeral changed.

`BaseETL.MISSING_VALUE_INDICATORS` lists `*` and does not list ` *`. The copy the pipeline loads is the October `datasets` file, whose token is `*`. The Data Engineer ran `etl/postsecondary_readiness.py` on each copy alone. The output is identical (16,130 rows; the 6,218 star cells are `suppressed` Y in both; pandas 3.0.6 and 2.3.3), because `to_numeric(errors='coerce')` plus the published flag make the space irrelevant.

The file does not say why it was rewritten on 9 October. The cause of the new Last-Modified is not established.

### What the pipeline loads, and what 0052 points to

The config registers `url: kyrc25` for `KYRC25_ACCT_Postsecondary_Readiness.csv`, the `datasets` host. Datasets and UAT have the same `Content-MD5`, so the downloader keeps `datasets`.

The October file is after the review window. It is not the broken-file pattern in record 0052. The rule points to that October `datasets` file, which is the file already registered. The numeric rates match the February file. The recorded difference is the leading space on `*`. The config was not changed. Issue [#123](https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/123) is the tracker for the other copy conflicts, not this file.

Current pipeline outputs do not change, because the registered copy is already the October `datasets` file.

## 4-year graduation

File: `KYRC25_ACCT_4_Year_High_School_Graduation.csv`. This is the file whose `datasets` Last-Modified is 6 August 2026. The 5-year file is under "Not compared."

Natural key: `School Year`, `School Code`, `Demographic`. All three copies have 9,528 keys and no duplicate keys. The 16 column names match.

### Copies

| Copy | URL | Last-Modified | Bytes | MD5 | Data rows |
| --- | --- | --- | --- | --- | --- |
| Historical | https://www.education.ky.gov/Open-House/data/HistoricalDatasets/KYRC25_ACCT_4_Year_High_School_Graduation.csv | Tue, 10 Feb 2026 20:37:52 GMT (3:37 p.m. Eastern) | 1,175,161 | `7e4fb3432624c4a8681a46862418e618` | 9,528 |
| `datasets` | https://kdeschoolreportcard.blob.core.windows.net/datasets/KYRC25_ACCT_4_Year_High_School_Graduation.csv | Thu, 06 Aug 2026 17:29:55 GMT (1:29 p.m. Eastern) | 1,175,161 | `7e4fb3432624c4a8681a46862418e618` | 9,528 |
| UAT | https://kdeschoolreportcard.blob.core.windows.net/datasetsuat6051276/KYRC25_ACCT_4_Year_High_School_Graduation.csv | Tue, 06 Oct 2026 20:27:27 GMT (4:27 p.m. Eastern) | 1,175,161 | `7e4fb3432624c4a8681a46862418e618` | 9,528 |

Historical HEAD did not return `Content-MD5`. Datasets and UAT both returned `Content-MD5: fk+zQyYkxKhoGkaGJBjmGA==`, which matches the MD5 of the downloaded bytes. Historical ETag `"{7ADCED5F-FFF7-4350-BFC1-258273281255},4"`. Datasets ETag `0x8DEF3E054DB5085`. UAT ETag `0x8DF23E83D6DB03B`.

The three MD5s match. Last-Modified and ETag do not. The August and October objects are the same bytes as the 10 February historical file.

### What changed

Added rows: 0. Removed rows: 0. Changed rows: 0. Changed cells: 0.

Fayette has 168 rows. State has 24 rows. Changed Fayette cells: 0. Changed state cells: 0.

State All Students, school `999000`, is 93.6 in every copy, `Suppressed` `N`. Fayette district All Students, school `165000`, is 92.6 in every copy, `Suppressed` `N`.

### Classification

There is no cell change to classify as a correction, a method change, or breakage.

The `datasets` Last-Modified is 6 August 2026 and the UAT Last-Modified is 6 October 2026, and both objects are the February bytes. The file does not say why the timestamp changed. The cause is not established.

### What the pipeline loads, and what 0052 points to

The config registers `url: kyrc25` for this file, the `datasets` host. The `Content-MD5` values match, so the downloader keeps `datasets`. That object is the same bytes as the February historical file and as UAT.

Record 0052 points to the latest public file after the review window. For this file the later public object does not change a value. The config was not changed. Issue [#123](https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/123) does not cover this file.

Current pipeline outputs do not change, because the registered copy is already the August `datasets` file.

## kde-republishes

`kde-republishes.md` is not updated in this pull request. Pull request [#120](https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/120) is frozen and under re-review.

R19 (`KYRC25_ACCT_Kentucky_Summative_Assessment.csv`) complies with 0052. The 14 May `datasets` copy is the latest public copy, and it is not the broken-file pattern in 0052.

R20 splits three ways. Postsecondary readiness complies. 4-year graduation complies. 5-year graduation was not compared.

The table update in `kde-republishes.md` and the link to 0052 by path will come after #120 merges.

## Not compared

Two related objects were HEADed on 25 October 2026 and were not downloaded or cell-diffed. The same length on two hosts is not evidence that the bytes match. The 4-year graduation file is the same length and the same bytes. The accountability assessment file is 15 bytes apart and 868 cells apart.

`KYRC25_ACCT_5_Year_High_School_Graduation.csv`:

| Copy | Last-Modified | Content-Length |
| --- | --- | ---: |
| Historical | Tue, 10 Feb 2026 20:38:29 GMT | 1,174,850 |
| `datasets` | Thu, 14 May 2026 19:11:31 GMT | 1,174,850 |
| UAT | Tue, 06 Oct 2026 20:27:28 GMT | 1,174,850 |

The datasets HEAD did not return `Content-MD5`. The UAT HEAD returned `Content-MD5: 3c0N8N4J5SKRyYp4NTsDrA==`. OAA `Graduation_Rate_2025.CSV` is 404, as above.

`KYRC25_ASMT_Kentucky_Summative_Assessment.csv` (by grade):

| Copy | Last-Modified | Content-Length |
| --- | --- | ---: |
| Historical | Thu, 19 Feb 2026 18:40:58 GMT | 91,151,540 |
| `datasets` | Thu, 14 May 2026 19:11:31 GMT | 91,151,548 |
| UAT | Tue, 06 Oct 2026 20:27:29 GMT | 91,151,548 |

The May `datasets` object is 8 bytes larger than the 19 February historical object. UAT matches the May length. The cell difference was not counted.

## Open questions

These are open. This note does not answer them.

1. For the accountability assessment file, record 0052 points to the May `datasets` copy on the checks above, and the config already registers it. Fayette cells do not change. Twenty-two state cells do, including high school mathematics All Students content index 57.7 to 57.6. The proficient/distinguished percent on that row stays 40. Is this record enough, or is a dashboard reload wanted?
2. The cause of the May assessment number changes is not in the file. No KDE notice was read as part of this comparison, and KDE was not contacted.
3. Postsecondary rates are unchanged. The difference is a leading space on `*`. Is any action wanted beyond this record?
4. The 4-year graduation timestamps differ and the bytes do not. Is any action wanted beyond this record?
5. The February baseline used here is the Open House file dated 10 February 2026. No 3 February object was found. Is that the baseline to keep using?
6. Should the 5-year graduation file and the by-grade assessment file be cell-diffed the same way? Their headers are above. Their cells were not compared.
7. Config changes for the other copy conflicts stay on issue [#123](https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/123). This note does not change `config/kde_sources.yaml`.

## Re-run

```bash
python3 docs/decisions/republish-diffs/scripts/diff_kyrc25_2026_rewrites.py
```

The script uses the Python standard library. It downloads the three files from the three hosts into `/tmp/kyrc25-republish-diffs` (override with `--cache`) and prints the counts. It does not write into the repository.
