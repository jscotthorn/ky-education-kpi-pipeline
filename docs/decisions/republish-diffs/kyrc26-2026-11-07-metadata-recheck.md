# Observation: KYRC26 public datasets metadata, 7 November 2026

This page is an observation. It is not a numbered decision. Scott did not choose a source file here. The next free decision number stays `0056`.

The pipeline registration, config, cached source files, and outputs stay as they were. This page did not HEAD KDE, download a source CSV, or regenerate anything.

## What was reported

Gremlin reported a metadata recheck on 7 November 2026. The report supplied for this page:

- 23 KYRC26 files on the public `datasets` container differ in md5 from the cached blobs. Their live Last-Modified values fall on 4 November 2026.
- The other 539 cached blob files in the same recheck are unchanged on the count defined below.
- Open House was not checked.
- The 23 Last-Modified timestamps run from 4 November 2026 13:17:24 GMT through 13:21:52 GMT. That is 8:17:24 a.m. through 8:21:52 a.m. Eastern Standard Time. An earlier relay that started the changed-file span at 8:11 a.m. does not match the saved `CHANGED.tsv`.
- `KYRC26_OVW_Teacher_Working_Conditions.csv` grew from 436,507 bytes to 512,257 bytes (75,750 bytes, about 17.35%). Whether that growth repairs the missing districts 001–095 in issue [#111](https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/111) is unverified.
- Header metadata does not show which cells or rows changed.
- Source CSV contents were not downloaded or compared during the recheck.
- No adoption decision and no regeneration decision was made.

## Who approved this page

Scott approved a docs-only pull request recording the recheck, in the words supplied with this task: "Open a docs-only PR recording this metadata recheck, without adopting files or regenerating outputs." That sentence authorizes this page. It leaves the registered files in place. Scott is the only decision-maker. The pull request is https://github.com/jscotthorn/ky-education-kpi-pipeline/pull/129.

## Saved recheck metadata

The two files in [kyrc26-2026-11-07-metadata-recheck/](kyrc26-2026-11-07-metadata-recheck/) are saved recheck metadata. They are the files attached for this note. They are not content fetched again while writing this page.

| File | What it is | Bytes | SHA-256 |
| --- | --- | ---: | --- |
| [CHANGED.tsv](kyrc26-2026-11-07-metadata-recheck/CHANGED.tsv) | The 23-row extract (header plus 23 data rows) | 3,956 | `e650119d63301c3c0897b6acf1b7fb4995c552b6daeb9e336f1b2a552bd7998f` |
| [results.json](kyrc26-2026-11-07-metadata-recheck/results.json) | 562 blob header records from the same recheck | 189,617 | `704bbfba1965813ef337736603ae71847e8133fff82726f44eb16374bddb78ff` |

`results.json` has no field for when the recheck ran. The 7 November 2026 date is Gremlin's report. Each record has `path`, `status`, `size`, `md5`, `lm`, `cached` (`size`, `md5`, `lm`), and `diffs`.

`.gitignore` ignores `*.json`. The exception for this one file is the same kind of exception already used for `notes/kyrc26-schema-diff-report.json`, so the saved metadata can stay in git.

## How the 23 and the 539 are counted

`results.json` has 562 records. Every `status` is 200. The 562 paths are unique. Hosts: `datasets` 98, `datasetsuat6051276` 464. No path is an Open House path.

A record is one of the 23 when both the cached md5 and the live md5 are present and they differ. That set is exactly `CHANGED.tsv`. For each of those 23 paths, cached size, live size, size delta, cached md5, live md5, and live Last-Modified in the TSV match the same path in `results.json`. All 23 live md5 values differ from the cached md5. Nine of the 23 also differ in size. The size deltas sum to 81,359.

The other 539 records have the same size as the cache. Where both md5 values are present (466 records), they match. On 73 records the live md5 is null and the size matches. Those 73 stay in the 539. The raw `diffs` array is nonempty on 96 records: the 23 md5 differences plus the 73 null live md5 values. A count of nonempty `diffs` is not the count of 23.

Cached Last-Modified is null on all 98 `datasets` rows, including the 23. It is present and equal to the live Last-Modified on all 464 `datasetsuat6051276` rows. Among the 539, 75 records have a live Last-Modified and no cached Last-Modified. Those 75 are the `datasets` rows outside the 23 (98 − 23 = 75).

## Clock

Every `CHANGED.tsv` Last-Modified value is `Wed, 04 Nov 2026`, from 13:17:24 GMT through 13:21:52 GMT.

4 November 2026 is Eastern Standard Time (UTC−5). Daylight time ended on 1 November 2026. 13:17:24 GMT is 8:17:24 a.m. EST. 13:21:52 GMT is 8:21:52 a.m. EST. The Eastern column below is that conversion. It is not a separate field in the TSV.

`results.json` also has live Last-Modified `Wed, 04 Nov 2026 13:11:13 GMT` (8:11:13 a.m. EST) on two public KYRC26 files that are in the 539. Their size and md5 match the cache:

- `datasets/2026/KYRC26_ACCT_4_Year_High_School_Graduation.csv`
- `datasets/2026/KYRC26_ACCT_5_Year_High_School_Graduation.csv`

That 13:11:13 GMT stamp is not the start of `CHANGED.tsv`.

All 48 `datasets/2026` records in `results.json` have a live Last-Modified on 4 November 2026, from 13:11:13 GMT through 13:21:52 GMT. Twenty-three of them are the md5 changes in the table. Twenty-four of the other twenty-five have a matching md5 and a matching size. The remaining file is `datasets/2026/KYRC26_ASMT_Kentucky_Summative_Assessment.csv`: the live md5 is null, both sizes are 91,031,722, and the record is in the 539. Its bytes were not compared.

The saved cache has no Last-Modified for any `datasets` object, so this recheck metadata does not show the previous Last-Modified of those objects. `notes/103--2025-26-kyrc26-data-update-plan.md` still records the 15 October 2026 10:00 GMT stamps from that earlier pass. This page leaves that note in place.

## The 23 files

Every path is `datasets/2026/`. Last-Modified is the live header. Cached Last-Modified is absent for every one of these rows.

| Path | Cached bytes | Live bytes | Delta | Cached md5 | Live md5 | Last-Modified | Eastern (EST) |
| --- | ---: | ---: | ---: | --- | --- | --- | --- |
| `datasets/2026/KYRC26_ASMT_Benchmark.csv` | 1,690,361 | 1,690,361 | +0 | `52421dbb42b5f372193269df8ce2d834` | `f2b3d8c0869d7d41053fa5aade208131` | Wed, 04 Nov 2026 13:17:24 GMT | 08:17:24 |
| `datasets/2026/KYRC26_EDOP_Gifted_Participation_by_Category.csv` | 3,135,968 | 3,136,202 | +234 | `c1702aed29d110ba2655a1eab2fff42b` | `d459170136fdcfbe19ce13a5baf82122` | Wed, 04 Nov 2026 13:19:17 GMT | 08:19:17 |
| `datasets/2026/KYRC26_EDOP_Gifted_Participation_by_Grade_Level.csv` | 2,017,415 | 2,017,559 | +144 | `0359bd5a4f0eee8104bf2d7b2832818b` | `1c22c66c995c2fcdcff0ededd2c8faf6` | Wed, 04 Nov 2026 13:19:18 GMT | 08:19:18 |
| `datasets/2026/KYRC26_EDOP_School_Courses_Summary.csv` | 5,908,076 | 5,908,076 | +0 | `3561b930ccc20c3042c5fb8832328a86` | `97caefd06c0d1ec87e0f8af48fe2456c` | Wed, 04 Nov 2026 13:19:27 GMT | 08:19:27 |
| `datasets/2026/KYRC26_OVW_Average_Years_School_Experience.csv` | 190,191 | 190,191 | +0 | `5f37a7504b48aa3d32343f1a70bbe031` | `6f4db2df56077d85a99eeb17f5a88fa9` | Wed, 04 Nov 2026 13:20:11 GMT | 08:20:11 |
| `datasets/2026/KYRC26_OVW_Chronic_Absenteeism.csv` | 60,964,965 | 60,969,573 | +4,608 | `07d218e589daaac167e0faacabecfe8e` | `2c317303f3f136bc102deee832c3cc95` | Wed, 04 Nov 2026 13:20:37 GMT | 08:20:37 |
| `datasets/2026/KYRC26_OVW_District_School_List.csv` | 426,362 | 426,339 | -23 | `705d5b0c4b266149f83c5f7355d46dda` | `c10799c0c3d21e7f90676654f40cedb1` | Wed, 04 Nov 2026 13:20:38 GMT | 08:20:38 |
| `datasets/2026/KYRC26_OVW_Educator_Qualifications.csv` | 1,025,305 | 1,025,305 | +0 | `57e02126c4a4604c5f163b49213463ca` | `3a84aa25e9d9b70df7167ebac42f7419` | Wed, 04 Nov 2026 13:20:42 GMT | 08:20:42 |
| `datasets/2026/KYRC26_OVW_Homeless.csv` | 3,874,972 | 3,874,972 | +0 | `c3a398bc3a96cc91b3d82857317f2b0c` | `292123c5740f12257b323c33515396ae` | Wed, 04 Nov 2026 13:20:47 GMT | 08:20:47 |
| `datasets/2026/KYRC26_OVW_Inexperienced_Teachers.csv` | 205,136 | 205,136 | +0 | `92528e45b4aa5f29269bd5794be0d328` | `c9cd546dd4e465fd020ad7864a1654e1` | Wed, 04 Nov 2026 13:20:49 GMT | 08:20:49 |
| `datasets/2026/KYRC26_OVW_Migrant.csv` | 3,851,260 | 3,851,260 | +0 | `fa4b801d4ec33a6b7c44a84df438e103` | `1cef1f3b68b410a6e91727ab43ad281a` | Wed, 04 Nov 2026 13:20:51 GMT | 08:20:51 |
| `datasets/2026/KYRC26_OVW_Student_Enrollment.csv` | 3,586,080 | 3,586,080 | +0 | `34fb611ac6c7356089d79681a9748fdd` | `69f88506236c1eecab9fcfc86f496238` | Wed, 04 Nov 2026 13:20:57 GMT | 08:20:57 |
| `datasets/2026/KYRC26_OVW_Students_Taught_by_Ineffective_Teachers.csv` | 261,805 | 261,965 | +160 | `44b49be11d418e83c4ff1e7000965bd7` | `643c0e828f7ca820a99e06b1a404c798` | Wed, 04 Nov 2026 13:21:04 GMT | 08:21:04 |
| `datasets/2026/KYRC26_OVW_Students_Taught_by_Inexperienced_Teachers.csv` | 321,011 | 321,047 | +36 | `b3d64712c06852ed7460b9f2b72306f1` | `2f6144bcbaffa54459bb0c0fa91c4b80` | Wed, 04 Nov 2026 13:21:04 GMT | 08:21:04 |
| `datasets/2026/KYRC26_OVW_Students_Taught_by_Out_of_Field_Teachers.csv` | 309,282 | 309,282 | +0 | `1593c9b739194315846d86097b4ad954` | `58274ef0bef4e0185bd97bf6bfdada36` | Wed, 04 Nov 2026 13:21:05 GMT | 08:21:05 |
| `datasets/2026/KYRC26_OVW_Students_with_Disabilities_IEP.csv` | 5,000,668 | 5,000,974 | +306 | `1b1dc1df9271aabfb2bf6c602b00ea68` | `75a2e2661b73e659db9f3bce538117e2` | Wed, 04 Nov 2026 13:21:16 GMT | 08:21:16 |
| `datasets/2026/KYRC26_OVW_Teacher_Certification_Data.csv` | 227,395 | 227,395 | +0 | `9d6565d7f00da937eeb1a2c8725ffb17` | `897148d374d01b194e3bac6fa3f802d8` | Wed, 04 Nov 2026 13:21:17 GMT | 08:21:17 |
| `datasets/2026/KYRC26_OVW_Teacher_Turnover.csv` | 218,806 | 218,806 | +0 | `4b670fdf37796f68e379046aa98ea63d` | `089b48d9627afaa6d50fd9810f9eade2` | Wed, 04 Nov 2026 13:21:18 GMT | 08:21:18 |
| `datasets/2026/KYRC26_OVW_Teacher_Working_Conditions.csv` | 436,507 | 512,257 | +75,750 | `ac5bd3c79bbfeaef42ee5ce015dcb089` | `2d813911b205483f422311f8a8e0f5e5` | Wed, 04 Nov 2026 13:21:19 GMT | 08:21:19 |
| `datasets/2026/KYRC26_SAFE_Behavior_Events_by_Context.csv` | 4,425,549 | 4,425,549 | +0 | `87d0acc49ae8c3c06990c01f6194021d` | `d23c7521a1b283da1bede41681c64622` | Wed, 04 Nov 2026 13:21:26 GMT | 08:21:26 |
| `datasets/2026/KYRC26_SAFE_Behavior_Events_by_Grade_Level.csv` | 4,880,364 | 4,880,364 | +0 | `61e1ba61b2cb6527033703c3ec8b02ac` | `bc4faa364cf7592e2663cf91cf95e895` | Wed, 04 Nov 2026 13:21:31 GMT | 08:21:31 |
| `datasets/2026/KYRC26_SAFE_Legal_Sanctions.csv` | 4,456,730 | 4,456,730 | +0 | `c7970ecb5e4b42ec66f113bd1d174c2c` | `fd79b9f432b8b0940ce4a815f1b5c21e` | Wed, 04 Nov 2026 13:21:52 GMT | 08:21:52 |
| `datasets/2026/KYRC26_SAFE_Precautionary_Measures.csv` | 198,110 | 198,254 | +144 | `220575cf689e134424fb28e3797b1687` | `3358d6e56f6e107fc87caa27e0b77e27` | Wed, 04 Nov 2026 13:21:52 GMT | 08:21:52 |

The table is transcribed from `CHANGED.tsv`. The TSV is the saved extract. Fourteen rows have size delta 0 and a different md5, so the byte count can match while the bytes differ. Nine rows change size.

## Teacher working conditions and issue 111

Issue [#111](https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/111) is open. It says `KYRC26_OVW_Teacher_Working_Conditions.csv` has no districts 001–095, and that the live `datasets` copy and the UAT copy matched each other in that respect when they were checked on 24 October 2026. The issue asks for a recheck after the 2025-26 review window.

In the saved 7 November metadata the public file is:

| | Bytes | md5 | Last-Modified |
| --- | ---: | --- | --- |
| Cached | 436,507 | `ac5bd3c79bbfeaef42ee5ce015dcb089` | absent in this saved metadata |
| Live | 512,257 | `2d813911b205483f422311f8a8e0f5e5` | Wed, 04 Nov 2026 13:21:19 GMT (8:21:19 a.m. EST) |

75,750 / 436,507 = 0.17353673595, which is 17.35% when rounded to two decimal places.

The missing-district repair is unverified. The saved headers do not list district numbers, rows, or cells. The source CSV was not downloaded or compared during this recheck. Issue #111 stays open. The file was not adopted.

## What stays as it was

Record [0052](../0052-republish-supersedes-earlier.md) is Scott's rule for which public file to keep after a review window closes, unless that copy is visibly broken, with the differences recorded in [kde-republishes.md](../kde-republishes.md). This page stores the header differences from the saved recheck. It does not apply 0052, and it does not adopt the 4 November objects.

[kde-republishes.md](../kde-republishes.md) row R21 remains the 25 October statement about the 15 October load. Row R27 points here and adopts no file.

[0049](../0049-kyrc26-files-loaded-15-oct-2026.md) stays `unconfirmed`. No decision there treats the 15 October bytes as final.

[coverage-gaps.md](../coverage-gaps.md) still classifies the missing districts 001–095 as unconfirmed. This page does not change that class.

## Checks made on the saved files

These checks read only the two saved files. They did not contact KDE.

- `CHANGED.tsv` has 23 data rows, all under `datasets/2026/`, and every live Last-Modified is `Wed, 04 Nov 2026` from 13:17:24 GMT through 13:21:52 GMT.
- Those GMT times convert to 08:17:24 through 08:21:52 EST at UTC−5.
- Each TSV row matches `results.json` on path, cached size, live size, size delta, cached md5, live md5, and live Last-Modified.
- The 23 paths are exactly the `results.json` records whose cached md5 and live md5 are both present and different.
- The other record count is 562 − 23 = 539. Those 539 match on size. They match on md5 wherever both md5s are present.
- Teacher working conditions is 436,507 to 512,257, delta 75,750, and 75,750 / 436,507 rounds to 17.35%.
- SHA-256 of the saved files matches the table in "Saved recheck metadata."
