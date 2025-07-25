# CTE Participation Pipeline Review

## Date: 2025-07-21

## Overview
Brief review of how the `cte_participation` pipeline maps KPI metrics from the Kentucky Department of Education source files and potential opportunities for additional indicators.

## KPI Mapping
- **cte_participation_rate** – mapped from the column `CTE Participants in All Grades` or `CTE PARTICIPANTS IN ALL GRADES`.
- **cte_eligible_completer_count_grade_12** – mapped from `Grade 12 CTE Eligible Completer` or `GRADE 12 CTE ELIGIBLE COMPLETER`.
- **cte_completion_rate_grade_12** – mapped from `Grade 12 CTE Completers` or `GRADE 12 CTE COMPLETERS`.

These mappings are defined in `etl/cte_participation.py` lines 68‑81.

During processing the module cleans and validates numeric fields, ensuring rates fall between 0–100 and counts are non‑negative (lines 30‑62).

## Source Data Observations
- The raw files also contain a `Total Number of Student` column giving overall enrollment counts by demographic. This field is not currently output as a KPI.
- Test data suggests the 2023 file may provide participant counts rather than rates; values over 100 are treated as invalid and set to `NA`.
- Additional columns such as `CO-OP` and `School Type` are available but not exposed in the KPI output.

## Potential Additional KPIs
1. **cte_total_student_count** – exposing the `Total Number of Student` values would allow context for participation rates.
2. **cte_participation_count** – if participant counts are provided in earlier years, convert those to a count metric in addition to the rate.
3. **cte_completion_count_grade_12** – similarly, if counts of grade‑12 completers exist, provide them directly rather than only the rate.

These additions could enhance longitudinal analysis, particularly if some years report counts instead of percentages.
