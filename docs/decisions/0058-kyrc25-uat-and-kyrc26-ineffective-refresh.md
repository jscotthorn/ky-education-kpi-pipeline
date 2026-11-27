# 0058 Load the KYRC25 ineffective UAT copy, adopt the 4 Nov KYRC26 ineffective refresh, and approve the batch B mirror additions

Status: accepted

This record is 0058. Repo comment: none.

## Question

Which KYRC25 students-taught-by-ineffective-teachers copy should the pipeline load, should the 4 November 2026 KYRC26 ineffective refresh be adopted in this batch, and which mirror additions does that batch include?

## What KDE changed

Nothing in this record is a new KDE notice. The copies compared here were already on KDE's containers.

The KYRC24 ineffective file is [0055](0055-kyrc24-ineffective-teachers-visibly-broken.md). This record's KYRC25 file is `KYRC25_OVW_Students_Taught_by_Ineffective_Teachers.csv`.

Fetched 27 Nov 2026, about 1:03 PM ET. Neither copy has an Equity Gap row. The two state rows (school code 999000, All Schools) are Title 1 and Not Title 1. Columns after Title I Status are All Students, Non-White, White (non-Hispanic), Economically Disadvantaged, Non Economically Disadvantaged, Students with Disabilities (IEP), Student without Disabilities (IEP), English Learner, Non English Learner.

Datasets, 261,299 bytes, md5 `082400fdb181e10b1308615f15d3fb5a`, sha256 `87cb2270a5b2af9dcf67404f0ce4d8836335a02ba748ef4715d9f1280e50a5be`, Last-Modified Sat, 18 Jul 2026 21:29:47 GMT, no Content-MD5:

```
20242025,,,999,All Districts,,All Schools,999000,,,,,,Not Title 1,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
20242025,,,999,All Districts,,All Schools,999000,,,,,,Title 1,0.8,0.1,0.4,0.5,0.1,0.1,0.2,0.1,0.2
```

On the Title 1 row, Non-White 0.1 + White 0.4 = 0.5, and All Students is 0.8. Economically Disadvantaged 0.5 + Non Economically Disadvantaged 0.1 = 0.6. Students with Disabilities (IEP) 0.1 + Student without Disabilities (IEP) 0.2 = 0.3. English Learner 0.1 + Non English Learner 0.2 = 0.3.

UAT, 261,299 bytes, md5 `20d08d7d8d077163e6fa8e21f473a29d`, sha256 `63aff38d3b87810e5f3823860c3bc13943fcbcb599ea0762a2f445a6a8e1ffc4`, Last-Modified Mon, 12 Oct 2026 19:55:55 GMT, Content-MD5 `INCNfY0HcWPm+o4h9HOinQ==`:

```
20242025,,,999,All Districts,,All Schools,999000,,,,,,Not Title 1,0.1,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
20242025,,,999,All Districts,,All Schools,999000,,,,,,Title 1,0.9,0.4,0.5,0.6,0.3,0.1,0.7,0.1,0.6
```

On the Title 1 row, Non-White 0.4 + White 0.5 = 0.9, which is All Students. Economically Disadvantaged 0.6 + Non Economically Disadvantaged 0.3 = 0.9. Students with Disabilities (IEP) 0.1 + Student without Disabilities (IEP) 0.7 = 0.8, against All Students 0.9. English Learner 0.1 + Non English Learner 0.6 = 0.7, against All Students 0.9. The Not Title 1 row is All Students 0.1 and every subgroup 0.0.

Those two extra partitions are in the UAT file. The race split Gremlin showed Scott is the one that adds up. Eight cells differ between the two copies, all on school code 999000.

KYRC26 `KYRC26_OVW_Students_Taught_by_Ineffective_Teachers.csv` on datasets, fetched the same afternoon: 261,965 bytes, 1,696 data rows, md5 `643c0e828f7ca820a99e06b1a404c798`, sha256 `8cbb4a2963575174dc48af6c82fb1cf8cf568feac43979c430d928544930ddf9`, Last-Modified Wed, 04 Nov 2026 13:21:04 GMT, Content-MD5 `ZDwOgo98qCCpngaxpATHmA==`. The previous object, still on UAT, is md5 `44b49be11d418e83c4ff1e7000965bd7`, 261,805 bytes, 1,695 data rows, Last-Modified Mon, 12 Oct 2026 19:53:57 GMT. The refresh adds Bullitt East High School, school code 071016, Not Title 1. Fayette rows are identical.

## Options considered

Keep the live KYRC25 datasets row, or load the UAT copy. Keep the October KYRC26 ineffective object, or adopt the 4 November datasets object. Approve the named mirror additions subject to a Data Engineer rehash, or leave the mirror as it is.

## Decision

27 November 2026.

12:24 PM ET. Scott's words: "Load the KYRC25 UAT copy; treat the live statewide row as visibly broken".

12:24 PM ET. Scott's words: "Adopt the Nov 4 KYRC26 ineffective-teachers refresh now in batch B".

12:26 PM ET. Scott's words: "Approve the batch B mirror additions with DE rehash verification".

The question those mirror words answer, which is Gremlin's description and not extra words from Scott, was: "the KYRC24 UAT and datasets copies, the KYRC25 UAT copy, and the Nov 4 KYRC26 ineffective refresh."

"Under 0052" is Gremlin's framing of the 12:24 PM KYRC25 answer. Scott did not cite 0052 in that answer. The reading that the live row shows 0.1 + 0.4 as 0.8 while the UAT row is 0.4 + 0.5 = 0.9 is Gremlin's. The rows and the arithmetic are in "What KDE changed" above, from the files.

The md5 `643c0e828f7ca820a99e06b1a404c798` is not in Scott's words. It matches the 7 November recheck metadata in pull request #129. The fetch in this record got that md5 on the live datasets object.

This record does not change how a negative equity-gap cell is stored. That choice is not this record.

## Who decided and how we know

Scott, chat answer in Gremlin's chat, 27 Nov 2026, 12:24 PM ET, verified by Archivist. Repo comment: none.

Scott, chat answer in Gremlin's chat, 27 Nov 2026, 12:24 PM ET, verified by Archivist. Repo comment: none.

Scott, chat answer in Gremlin's chat, 27 Nov 2026, 12:26 PM ET, verified by Archivist. Repo comment: none.

## Why

Scott's words are the decision. The file check in this record is why the 12:24 PM KYRC25 sentence applies to the live datasets row: Non-White 0.1 + White 0.4 = 0.5, shown as All Students 0.8, and the UAT Title 1 row is 0.4 + 0.5 = 0.9. The UAT row still has disability and English-learner partitions that do not add up to All Students, and its Not Title 1 row is 0.1 beside subgroups of 0.0. Those are recorded above. They are not a second decision.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/64 item 2 (KYRC25 UAT and the missing-Content-MD5 comparison)
- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/123
- [0025](0025-ineffective-teachers-copy-and-zeros.md)
- [0052](0052-republish-supersedes-earlier.md)
- [0054](0054-store-values-as-kde-publishes.md)
- [kde-republishes.md](kde-republishes.md) rows R7 and R28
- Pull request #129, for the 7 November recheck metadata that names md5 `643c0e828f7ca820a99e06b1a404c798`
- `config/kde_sources.yaml` `students_taught_by_ineffective_teachers`

## Code check

Matches. `students_taught_by_ineffective_teachers` registers KYRC25 as `url: kyrc_uat`, which resolves to `base_url_kyrc_uat` and does not auto-select a container. KYRC26 stays `url: kyrc26`. Both containers send Content-MD5 for that file and the datasets Last-Modified (Wed, 04 Nov 2026 13:21:04 GMT) is newer than UAT (Mon, 12 Oct 2026 19:53:57 GMT), so the existing downloader keeps the datasets refresh. Values stay as published ([0054](0054-store-values-as-kde-publishes.md)).

The mirror additions are approved here and are not applied by this repository. `dashregen/bin/stage_raw_strict.py` has to learn `kyrc_uat` (`datasetsuat6051276`) and `kyrc_datasets` (`datasets`) before a pinned key stages. That change is on the dashboard box, not in this repo.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found: a KDE notice that names the KYRC25 ineffective statewide row as broken, or that names the 4 November KYRC26 ineffective object as the replacement.
