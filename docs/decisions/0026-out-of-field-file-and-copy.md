# 0026 Out-of-field file for 2019-20, and the KYRC24 copy

Status: conflicts-with-code

## Question

Should `students_taught_by_out_of_field_teachers_2020.csv` be registered, and should 2024 use the datasets copy?

## What KDE changed

The 2019-20 file returns HTTP 200. It is school year 20192020. 1,680 of 1,831 Title I and Not Title I rows are all zero, and 122 of 177 districts are entirely zero. A statewide 1.41 in that file sits next to 8.88 in 2020-21. Advisory 20-318 said a new equity tab would appear on the 2019-20 report card. That bulletin was not re-fetched here. https://content.govdelivery.com/accounts/KYDE/bulletins/2a5782d

## Options considered

Register the 2019-20 file and start the series then, or leave it unregistered so the series starts in 2020-21. Separately, keep the historical KYRC24 file or switch to the datasets copy.

## Decision

18 October 2026.

- Leave `students_taught_by_out_of_field_teachers_2020.csv` unregistered. The series starts in 2020-21. https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/66#issuecomment-6251880088
- Switch the 2024 registration to the datasets copy. https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/66#issuecomment-6251862885

## Who decided and how we know

Both comments were posted on the `jscotthorn` account through the Cursor GitHub App.

## Why

The 2019-20 comment's reason is "first year of KDE's Equity tab, incomplete coverage." The 1.41 and 8.88 figures are in the "What KDE changed" section above. They come from `config/kde_sources.yaml`, not from that comment. The copy comment chooses the datasets file. Reason that copy is preferred: not documented beyond the cell disagreement described in the yaml.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/66 (still open)
- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/123 (open). The yaml still registers the Open House copies, so the 18 October copy choices on #58, #64, and #66 are not applied.
- `config/kde_sources.yaml` `students_taught_by_out_of_field_teachers`
- [0027](0027-signed-title-i-equity-gap.md)

## Code check

The unregistered 2019-20 file still matches: it is not registered, and the parser skips that filename. The comment records the reason from issue #66 item 1: first year of KDE's Equity tab, incomplete coverage. The 2024 registration now matches. Re-read on this branch: KYRC24 is `url: kyrc_datasets` for `KYRC24_OVW_Students_Taught_by_Out_of_Field_Teachers.csv`. The comment records the replaced historical extract and says UAT negates some equity-gap cells and is not loaded. The datasets copy is the later public file (Last-Modified Thu, 14 May 2026 19:11:30 GMT) and is not visibly broken, so the registration follows [0052](0052-republish-supersedes-earlier.md). Status stays `conflicts-with-code` until it is updated after merge. That status change is not this edit.

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

REL-2020, not re-fetched, is cited by the research as announcing the equity tab. It was not found to say whether the 2019-20 out-of-field file should be joined to the later series, or which KYRC24 copy is current.
