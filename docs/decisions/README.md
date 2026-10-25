# Decision log

This folder records choices about Kentucky Department of Education (KDE) sources and how this pipeline treats them. It does not change pipeline code. A later edit to a choice is a new record. The old record stays, keeps its number, and links to the new one.

Numbers are `NNNN-short-slug.md`, starting at `0001`. The next free number is `0056`. A number is never reused.

## When a record is needed

Write a record for any choice about:

- which KDE source file or republish to use
- how to treat suppression, stars, or blank cells
- column or metric mapping
- ID normalization
- dropping or excluding a metric or year
- scope (what this update will not do)

## Status

| Status | Meaning |
| --- | --- |
| `accepted` | A cited issue comment, pull request, note, or commit records the choice as Scott's. `accepted` also covers Scott's own answer in an agent chat, quoted with date and time (ET), with the bot named and the quote checked verbatim against the transcript by Archivist. That source type is labeled `chat answer`. Records that use it: 0002 (the 25 October exception), 0007 (the 11:24 AM ET sentence), the 0009 amendment, 0042 (the 12:03 PM ET answer), 0050, 0051, 0052, 0053, and 0055 (Scott's 1:07 PM ET typed answer in Moon's chat). Where a repo Decision comment exists, that comment is the primary source and the chat quote stays as background: 0002, the 0009 amendment, 0050, 0051, 0052, 0053, and 0055. 0007's record is `notes/104`, and the chat sentence is a second source. 0042's answer is also on issue #121. 0048 is not on this list: the 12:05 wording is Moon's relay, and Scott's 12:38 PM ET confirmation of the #122 comment is the source. A later record whose only source is a chat answer keeps a `Repo comment: pending` line until a repo Decision comment is added. |
| `accepted, code differs (follow-up open)` | The choice is accepted. Current code or config on `feature/2025-26-kyrc26` does not match it, and a follow-up is still open. The difference is described and not fixed here. |
| `relayed-unconfirmed` | The only source is another agent's relay. Scott has not stated it in the repo. |
| `superseded` | A later record replaces this one. This file stays and links forward. A record replaced only in part is `superseded (in part)`. The file names the part that still stands. |
| `conflicts-with-code` | Current code on `feature/2025-26-kyrc26` does the opposite of the choice. The conflict is described and not fixed here. |
| `unconfirmed` | The repo does not show Scott making the choice, or the cause is not documented and the record exists only to flag that gap. |

Comments that begin "Decision (Scott, …)" were posted on the `jscotthorn` account through the Cursor GitHub App. This log treats that comment as the repo's record of the choice. It is not a separate comment typed in the GitHub UI outside that app. No such outside comment was found in this pass.

If the cause is not in the repo, a pull request, an issue, or a commit, the Why field says `Reason not documented`.

## KDE guidance check

Each record has a **KDE guidance check**. The verdict is one of `consistent`, `possible conflict`, `conflict`, or `no KDE guidance found`. The check quotes KDE or says the statement was not found, and it cites the URL. Sources are listed in [kde-guidance-sources.md](kde-guidance-sources.md).

These verdicts are flags for Scott. They do not change the decision, the status, or the code.

Two KDE pages were re-fetched while writing this log: CHG-2022 and the 15 October 2026 release bulletin. Other KDE quotations come from the 25 October 2026 research attachment and were not fetched again. Claims about this repository were checked against the tree. See [kde-guidance-sources.md](kde-guidance-sources.md).

## GitHub label

Decisions are labeled `decision` on GitHub. The label exists. Its description is "A data or reporting decision Scott made; recorded in docs/decisions/". This log does not add or remove labels.

## Record template

- Question
- What KDE changed (if applicable)
- Options considered
- Decision (what Scott chose) and date
- Who decided and how we know
- Why (quoted or cited, or `Reason not documented`)
- Links (issue, pull request, `notes/` file, source file, commit SHA)
- Code check (does current code on this feature branch match? If not, describe the conflict. Do not resolve it.)
- KDE guidance check (verdict, short quote, URL)

## Notes file numbers

Two note numbers are used twice. They were not renamed.

- `notes/105.md` and `notes/105--kyrc26-completeness-audit.md`
- `notes/88--advanced-coursework-dual-credit-pipeline-updates.md` and `notes/88--school-programs-offerings-pipelines-implementation.md`

## Other pages

- [kde-reporting-changes.md](kde-reporting-changes.md) — what KDE changed, by series and year, and how the pipeline handled it
- [coverage-gaps.md](coverage-gaps.md) — series the equity dashboard is missing, classified from this repo
- [kde-guidance-sources.md](kde-guidance-sources.md) — KDE URLs and statements that were not found
- [kde-republishes.md](kde-republishes.md) — republish and alternate-copy cases, and where each difference is recorded

## Records

55 records. 39 accepted. 9 accepted, code differs (follow-up open). 3 conflicts-with-code. 3 superseded (in part). 1 unconfirmed. 0 relayed-unconfirmed.

| Record | Status | KDE guidance check |
| --- | --- | --- |
| [0001](0001-suppression-rule.md) | accepted | consistent, with one gap |
| [0002](0002-report-scripts-out-of-scope.md) | accepted | no KDE guidance found |
| [0003](0003-shared-id-normalization.md) | accepted | consistent |
| [0004](0004-postsecondary-star-cells.md) | accepted, code differs (follow-up open) | possible conflict |
| [0005](0005-cte-skip-all-star-padding.md) | superseded (in part) | possible conflict |
| [0006](0006-cte-compute-rates-2026-10-18.md) | superseded (in part) | possible conflict |
| [0007](0007-drop-calculated-cte-rates.md) | accepted | possible conflict |
| [0008](0008-kyrc24-cte-may-2026-republish.md) | accepted | consistent |
| [0009](0009-cte-zero-rate-beside-blank-count.md) | accepted | possible conflict |
| [0010](0010-cte-2023-mixed-ids.md) | accepted | consistent |
| [0011](0011-cte-2023-completion-rate-name.md) | accepted | possible conflict |
| [0012](0012-gifted-always-starred-slices.md) | accepted | possible conflict |
| [0013](0013-climate-bridge-declined.md) | accepted | possible conflict |
| [0014](0014-impact-kentucky-one-wave.md) | accepted | consistent |
| [0015](0015-twc-kyrc24-datasets-copy.md) | accepted | no KDE guidance found |
| [0016](0016-glasgow-conflicting-totals.md) | accepted | no KDE guidance found |
| [0017](0017-discipline-all-star-rows.md) | accepted | consistent |
| [0018](0018-discipline-zeros-and-padding.md) | superseded (in part) | possible conflict |
| [0019](0019-discipline-total-all-students.md) | accepted | no KDE guidance found |
| [0020](0020-kyrc23-legal-sanctions-source.md) | accepted | no KDE guidance found |
| [0021](0021-pandas-unpinned-comma-fixes.md) | accepted | no KDE guidance found |
| [0022](0022-teacher-experience-zero-averages.md) | accepted, code differs (follow-up open) | no KDE guidance found |
| [0023](0023-student-teacher-ratio-outliers.md) | accepted | no KDE guidance found |
| [0024](0024-novice-kyrc24-equity-copy.md) | conflicts-with-code | no KDE guidance found |
| [0025](0025-ineffective-teachers-copy-and-zeros.md) | conflicts-with-code | no KDE guidance found |
| [0026](0026-out-of-field-file-and-copy.md) | conflicts-with-code | no KDE guidance found |
| [0027](0027-signed-title-i-equity-gap.md) | accepted | no KDE guidance found |
| [0028](0028-turnover-2026-metric-names.md) | accepted | no KDE guidance found |
| [0029](0029-emergency-provisional-2026.md) | accepted | no KDE guidance found |
| [0030](0030-keep-zero-teacher-rates.md) | accepted | no KDE guidance found |
| [0031](0031-dual-credit-dc-slice.md) | accepted, code differs (follow-up open) | no KDE guidance found |
| [0032](0032-ib-tested-counts-2026.md) | accepted | no KDE guidance found |
| [0033](0033-remove-algebra-1-metrics.md) | accepted, code differs (follow-up open) | no KDE guidance found |
| [0034](0034-courses-offered-starred-total.md) | accepted | consistent |
| [0035](0035-safe-schools-2018-19-workbook.md) | accepted, code differs (follow-up open) | no KDE guidance found |
| [0036](0036-behavior-starred-type-total.md) | accepted | consistent |
| [0037](0037-kindergarten-published-percents.md) | accepted | possible conflict |
| [0038](0038-career-readiness-starts-2018-19.md) | accepted | no KDE guidance found |
| [0039](0039-drop-pathway-completion-rate.md) | accepted | consistent |
| [0040](0040-drop-has-dual-credit-program.md) | accepted | no KDE guidance found |
| [0041](0041-drop-calculated-climate-indexes.md) | accepted | consistent |
| [0042](0042-do-not-register-2021-22-climate-index.md) | accepted | no KDE guidance found |
| [0043](0043-kyrc23-republish-only-groups.md) | accepted, code differs (follow-up open) | no KDE guidance found |
| [0044](0044-district-school-list-2017-18.md) | accepted, code differs (follow-up open) | no KDE guidance found |
| [0045](0045-school-courses-one-row.md) | accepted | no KDE guidance found |
| [0046](0046-outside-doors-locked.md) | accepted | no KDE guidance found |
| [0047](0047-workbook-demographic-labels-2018-19.md) | accepted, code differs (follow-up open) | no KDE guidance found |
| [0048](0048-crdc-kpi-year-collection.md) | accepted, code differs (follow-up open) | no KDE guidance found |
| [0049](0049-kyrc26-files-loaded-15-oct-2026.md) | unconfirmed | possible conflict |
| [0050](0050-cte-all-star-rows-stored.md) | accepted | consistent |
| [0051](0051-cte-zero-rate-on-positive-count.md) | accepted | possible conflict |
| [0052](0052-republish-supersedes-earlier.md) | accepted | no KDE statement on supersession |
| [0053](0053-spending-2024-25-three-digit-ids.md) | accepted | no KDE guidance found |
| [0054](0054-store-values-as-kde-publishes.md) | accepted | no KDE guidance found |
| [0055](0055-kyrc24-ineffective-teachers-visibly-broken.md) | accepted | no KDE guidance found |
