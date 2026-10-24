# 115 — Cleanup batch 1: KYRC26 docs/tests follow-ups and the report-drafts state row

PR #115 into `feature/2025-26-kyrc26` (base `cbd46c569ff8d1f673b6d4fc72ee99afd492466e`). Refs #105 #107 #110 #111 #114. No ETL logic, no registered file, and no KPI output changes.

The edits in commits `603b697` and `7c7ccb9` were authored by Claude Code (Sonnet) on Scott's Mac. The review fixes in the follow-up commit and the gate below were run on the shared box.

## Changes

- `notes/107.md`: the numbering sentence now says `notes/106--cte-participation.md` uses 106 (it said "PR #110 also uses 106"). Records Scott's 24 October 2026 decision that KDE's 2021-22 per-level climate/safety index file (`quality_of_school_climate_and_safety_survey_index_scores_2022.csv`) is not registered, so `climate_index_score_*` and `safety_index_score_*` start at 2022-23.
- `etl/teacher_working_conditions.py`: docstring only. The Glasgow `197` published pair is in `notes/105.md`, not notes/104 (#105 follow-up).
- `KPIS.md` and `README.md`, teacher working conditions (#105, #111): the survey is administered in even years only (2020, 2022, 2024, 2026) and odd years have no row; Teaching Environment (2020, 2022) and School Climate (2024, 2026) are separate series with no bridge (Scott, 20 October 2026, #62 item 2); KYRC26 is missing districts 001-095 at the source (#111, `kde-recheck`).
- `KPIS.md`, financial summary (#110): documents `students_per_certified_staff`, `students_per_classified_staff`, and `students_per_non_teacher_certified_staff`. They are recomputed from KDE-published membership and staff FTE, rounded to 2 decimals, and omitted when an input is missing, starred, or 0.
- `analysis/config/covariate_catalog.yaml`: `twc_managing_student_behavior`, `twc_school_climate`, and `twc_school_leadership` are labelled percent favorable (`unit: percentage`) instead of `index_score`. Only `description` and `unit` changed. Keys and every other field, including the required/excluded flags, are identical to base (parsed and compared). No code reads those label strings: `analysis/config/__init__.py` returns the metadata dict, and `generate_report_drafts.py`'s `unit` comes from its own `INDICATORS` table.
- `tests/test_financial_summary.py` (#110): the four tests that expected 8 metrics now compare against the full 11-metric set. The asserted ratios come from the test's own fixture row (membership 2546, certified 219, teachers 187, classified 168): round(2546 / 219, 2) = 11.63, round(2546 / 168, 2) = 15.15, round(2546 / (219 − 187), 2) = round(2546 / 32, 2) = 79.56. They were recomputed on the box and the tests pass on both pandas versions.
- `scripts/generate_report_drafts.py`: state averages read `district == 'State'` and `school_name == '---District Total---'`, which matched neither real state-row shape, so every state average was None. Real outputs have two shapes: 2018-2023 `State` / `999` / `---State Total---`, and KYRC `All Districts` / `999000` / `---District Total---` (2024 onward and the 2023 CTE rates from #114). `state_row_mask` selects a row when `school_code` is `999` or `999000` or the district is `State` or `All Districts`, and `load_data` reads `school_code` when the file has it. `get_state_average` accepts either `---District Total---` or `---State Total---`. `get_district_data` still requires `---District Total---`, so district averages are unchanged. New test file: `tests/test_generate_report_drafts_state_row.py`, 8 tests, synthetic values on the real row shapes.
- `notes/104--kyrc26-file-to-etl-mapping.md`: the 24 October KYRC24 completer follow-up said KYRC23's published 2023 rates "are not loaded". It now says they were not loaded when that follow-up was written and that #114 loads them.
- DE review nits on `ebc048e`: KPIS school_climate is "2024 and 2026 administrations"; TWC `State` rows are 2020 and 2022 in KPIS and README; the KYRC26 gap also lists districts 131, 261, 537, 593, 595, 602, and 606 (notes/105.md); KPIS says the `students_per_*` ratios are not in the Financial Summary file (KDE publishes a separate student-to-teacher ratio file); notes/107 points the "separate decision" sentence at Scott's 24 October decision; notes/105's catalog follow-up notes that this PR relabels `twc_*`.

## Review fixes to the Claude Code edit

- Mask (`d4037e9`): Claude's first `state_row_mask` used the school code only if some row in the frame matched it, and otherwise fell back to the label. `load_data` applies the mask per `read_csv` chunk, so the result depended on what else was in the chunk. This was latent: in the real ETL outputs every state row carries both a state label and a state code, so no real row was dropped. The mask is now a row-wise OR of code and label, which does not depend on the rest of the frame. `test_state_mask_keeps_label_rows_alongside_code_rows` fails on Claude's version.
- Legacy state averages (`eae8e49`, Data Engineer review R1): Claude's legacy test used a `State` / `999` / `---District Total---` row, which does not occur in real data, so it passed while every 2018-2023 state average was still None. The fixture now uses `State` / `999` / `---State Total---`, and `get_state_average` accepts that name. On the real teacher working conditions output (`/workspace/cl115/oh3`, built by DE from the KDE cache), school leadership state averages are 2020 66.0, 2022 64.0, 2024 69.0, 2026 70.0 (before: None, None, 69.0, 70.0), matching the published state percents. Behavior is 65.0 / 65.0 / 67.0 / 69.0, teaching environment 62.0 / 60.0, and school climate 65.0 / 67.0, all identical on pandas 3.0.6 and 2.3.3. Fayette County's 2022 leadership district average is 61.0 before and after.

## Gate

`python -m pytest tests/ -q --tb=no` with junit output, fresh venvs on Python 3.13 (project dependencies plus pytest).

| pandas | base `cbd46c5` | this head |
|---|---|---|
| 3.0.6 | 19 failed, 674 passed, 109 skipped | 15 failed, 686 passed, 109 skipped |
| 2.3.3 | 11 failed, 682 passed, 109 skipped | 7 failed, 694 passed, 109 skipped |

Newly failing ids versus base: 0 on both versions. Fixed on both versions: `tests/test_financial_summary.py::TestFinancialSummaryETL::test_extract_metrics`, `test_get_suppressed_metric_defaults`, `test_mixed_year_formats`, and `test_transform_2020_format`. The other failures are unchanged pre-existing ids (Census API integration tests, `test_safe_schools_discipline_end_to_end::test_end_to_end_empty_file`, two `test_student_enrollment` tests, and on 3.0.6 only the dtype tests in `test_dtype_enforcement`, `test_graduation_rates`, `test_postsecondary_readiness`, and `test_spending_per_student`).

The state-row tests can fail:

| Break | Result |
|---|---|
| `scripts/generate_report_drafts.py` at base | module import error |
| script at `ebc048e` (`---District Total---` only) | `test_state_average_found_for_legacy_state_total_row` fails |
| `---State Total---` removed from the accepted names | same test fails |
| Claude's any-match mask | `test_state_mask_keeps_label_rows_alongside_code_rows` fails |
| `school_code` read removed from `load_data` | `test_load_data_reads_school_code_for_code_only_state_rows[whole_file]` and `[chunked]` fail |

`etl/` changes are one docstring line, so no ETL output changed and no output or duplicate-key comparison was run. `develop` and `master` are untouched.
