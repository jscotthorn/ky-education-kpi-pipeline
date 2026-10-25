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

The 2019-20 comment cites the zero-filled equity rows and the jump from 1.41 to 8.88. The copy comment chooses the datasets file. Reason that copy is preferred: not documented beyond the cell disagreement described in `config/kde_sources.yaml`.

## Links

- Issue https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/66 (still open)
- `config/kde_sources.yaml` `students_taught_by_out_of_field_teachers`
- [0027](0027-signed-title-i-equity-gap.md)

## Code check

The unregistered 2019-20 file matches the yaml comment: the file is not registered and the parser skips that filename. The 2024 copy switch does not match. Re-read on this branch: the bare historical name `KYRC24_OVW_Students_Taught_by_Out_of_Field_Teachers.csv` is still registered, and the comment says "Do not swap it here." A title search of pull requests found no follow-up that performs the switch. Issue #66 is still open. Not fixed here. The standing file rule is [0052](0052-republish-supersedes-earlier.md). This conflict stays open as follow-up (a).

## KDE guidance check

Verdict: no KDE guidance found. Flag only.

REL-2020, not re-fetched, is cited by the research as announcing the equity tab. It was not found to say whether the 2019-20 out-of-field file should be joined to the later series, or which KYRC24 copy is current.
