# DNNNN Accept the refreshed KYRC26 teacher working conditions file as published

Status: accepted

Decision source: Scott, chat answer in Gremlin's chat, 27 Nov 2026, 12:30 PM ET, verified by Archivist. Repo comment: none.

The config comment in `config/kde_sources.yaml` is Gremlin's plan, not Scott's decision. Gremlin's question recommended "a matching decision record and config comment". The option Scott picked leaves the comment out. The comment is kept because it changes no values. "File-by-file" is Gremlin's wording in the relay, not Scott's. Scott's words are "the ongoing review".

This record does not cite a 7 November 2026 approval as Scott's. DECISIONS-VERIFIED.md does not cover one.

## Question

KDE replaced `KYRC26_OVW_Teacher_Working_Conditions.csv` on the public datasets container after the 15 October 2026 copy. Which copy is the accepted source, and which missing-district statement do the docs keep?

## What KDE changed

Recounted in this workspace on 27 Nov 2026 from the public files. Fetch time: HTTP `Date` `Fri, 27 Nov 2026 18:00:31 GMT` (1:00:32 PM ET).

The current datasets object:

- URL: `https://kdeschoolreportcard.blob.core.windows.net/datasets/KYRC26_OVW_Teacher_Working_Conditions.csv`
- Last-Modified: `Wed, 04 Nov 2026 13:21:19 GMT`
- Content-MD5: `LYE5EbIFSD9CIxH4qOD15Q==`
- Size: 512,257 bytes
- md5: `2d813911b205483f422311f8a8e0f5e5`
- sha256: `ffefc54e99835089e999b40aadd854ba493806c55f389242d62737e498295478`

The comparison copy is the UAT object at `https://kdeschoolreportcard.blob.core.windows.net/datasetsuat6051276/KYRC26_OVW_Teacher_Working_Conditions.csv`. Last-Modified `Sat, 10 Oct 2026 06:02:52 GMT`. Size 436,507 bytes. md5 `ac5bd3c79bbfeaef42ee5ce015dcb089`. sha256 `7883834c2870375939475d93c75faa0d7f7783f3433146ca4dcc9dc77500024d`. That md5 is the 15 October datasets hash recorded in `notes/104--kyrc26-file-to-etl-mapping.md`. This pass did not download a separate 15 October datasets object. The UAT bytes are that hash.

Both files have the same 15 columns, in the same order: School Year, County Number, County Name, District Number, District Name, School Number, School Name, School Code, State School Id, NCES ID, CO-OP, CO-OP Code, School Type, Impact Measure, Impact Value. Every data row has School Year `20252026`.

| | UAT (15 October hash) | Datasets (4 November) |
| --- | ---: | ---: |
| Data rows | 3,282 | 3,852 |
| Duplicate keys on (School Code, Impact Measure) | 0 | 0 |

Removed keys: 0. Added keys: 570. Of those, 93 have a school code ending in `000` (31 districts times 3 constructs) and 477 do not (159 schools times 3 constructs).

On the 3,282 shared keys, Impact Value is the same on every row. The only other field that differs is District Name, on 6 rows, all district 603: school codes `603000` and `603390`, three constructs each. The UAT name is `Kentucky School for the Deaf`. The datasets name is `Kentucky School for the Deaf District`. The Impact Value on `603000` Managing Student Behavior is 63 on both copies.

The 31 added district numbers are the districts the district-school list publishes from Adair County `001` through Carlisle County `095`. Those codes are 001, 005, 006, 011, 012, 013, 015, 016, 017, 021, 025, 026, 031, 032, 034, 035, 041, 042, 045, 051, 055, 061, 065, 071, 072, 075, 081, 085, 091, 092, and 095. Not every integer from 001 to 095 is a district. Every district the list publishes in that range is in the datasets file.

The comparison list is the datasets `KYRC26_OVW_District_School_List.csv` fetched at the same time. URL: `https://kdeschoolreportcard.blob.core.windows.net/datasets/KYRC26_OVW_District_School_List.csv`. Last-Modified `Wed, 04 Nov 2026 13:20:38 GMT`. Size 426,339 bytes. md5 `c10799c0c3d21e7f90676654f40cedb1`. sha256 `a92b91ac82dc5b79910cd36134e9447028a408a59426300153432a0f9300034e`. This record does not adopt that file. Scott's words keep other refreshed files in the ongoing review.

That list has 177 districts plus the `---State---` row. The UAT teacher working conditions file contains 136 of those district numbers (178 − 136 = 42 absent). The datasets file contains 167 (178 − 167 = 11 absent). State `999` is in both teacher working conditions files.

The 11 still absent, with the district name on the list:

| Code | District name on the list |
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

Fayette County school codes beginning `165`: 201 rows on each copy, and every field matches. The three state rows (`999000`, Managing Student Behavior 69, School Climate 67, School Leadership 70) match on every field.

No KDE notice of this refresh was fetched for this record.

The raw file is not in git. `data/raw/` is ignored. Replacing the raw file and regenerating processed output are not part of this record.

## Options considered

Keep the 15 October copy documented as missing districts 001-095. Adopt the 4 November datasets copy as the source to load as published, and correct the docs. Adopt the other files KDE refreshed on 4 November in the same batch.

## Decision

27 November 2026, 12:30 PM ET. Scott's words: "Add a TWC adoption decision record and fix the missing-districts docs in batch K; keep other refreshed files in the ongoing review".

The accepted source is the refreshed datasets file, as published. The filename `KYRC26_OVW_Teacher_Working_Conditions.csv` was already registered. This record does not put the refreshed bytes in `data/raw/` and does not regenerate processed output.

The docs that describe the current KYRC26 source no longer say districts 001-095 are missing. They say the 11 districts in the table above are still absent. The 18 October measurements in `notes/104` stay as the account of the file that was current then, with a dated follow-up.

Other files refreshed on 4 November stay in the ongoing review. That includes the district-school list used only as the comparison above. Gremlin's relay said "file-by-file". Those two words are not Scott's.

The config comment is Gremlin's plan. It names this record and the hash. It does not change the registration.

## Who decided and how we know

Scott, chat answer in Gremlin's chat, 27 Nov 2026, 12:30 PM ET, verified by Archivist. The quote is the value of the card option he picked, as DECISIONS-VERIFIED.md item 11 gives it. Repo comment: none.

Archivist's check of the relay: "plus a config comment" is not in Scott's answer, and "File-by-file" is Gremlin's wording.

Issue #111 remains open. Comment 7224584930 (27 Nov 2026, 12:36 PM ET) says the docs and the decision record will be updated in batch K and that the issue is left open. This record does not close it.

## Why

Scott's sentence is the choice. [0052](0052-republish-supersedes-earlier.md) is the standing rule for a later public file after the review window, unless it is visibly broken. The datasets Last-Modified is 4 November 2026, after the review window recorded on [0049](0049-kyrc26-files-loaded-15-oct-2026.md). The shared Impact Values match, the schema matches, and the added rows are districts the earlier copy omitted. The district 603 name adds the word District. That is not a reason, in this record, to call the file visibly broken. [0054](0054-store-values-as-kde-publishes.md) keeps the published name and the published percent.

## Links

- `notes/120--kyrc26-twc-refresh-and-caveat-check.md`
- `notes/105.md` (the follow-up that names the 11 districts)
- `notes/104--kyrc26-file-to-etl-mapping.md` (the 15 October md5, and the 27 November follow-up)
- `config/kde_sources.yaml` (`teacher_working_conditions`, the KYRC26 entry)
- `KPIS.md`, Teacher Working Conditions
- `README.md`, Teacher Working Conditions
- [kde-republishes.md](kde-republishes.md) row R28
- [coverage-gaps.md](coverage-gaps.md)
- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/111
- Comment https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/111#issuecomment-7224584930
- [0052](0052-republish-supersedes-earlier.md)
- [0054](0054-store-values-as-kde-publishes.md)
- [0056](0056-kyrc26-chronic-absenteeism-district-602-name.md) (the pattern for a source choice that does not regenerate output)

## Code check

The registration is unchanged: `url: kyrc26`, file `KYRC26_OVW_Teacher_Working_Conditions.csv`. The comment above that entry is the only edit in that block. `etl/teacher_working_conditions.py` is not edited. Its docstring does not say districts 001-095 are missing. The Glasgow handling in `convert_to_kpi_format` is not edited.

This branch does not replace the raw file and does not regenerate processed output. A later load of the registered URL gets the datasets object that is current at download time. This record does not run that download.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

Not found in this pass: a KDE notice that the 4 November teacher working conditions file should replace the 15 October file, or that the 11 absent districts are withheld on purpose. The guidance pages listed in [kde-guidance-sources.md](kde-guidance-sources.md) were not re-fetched.
