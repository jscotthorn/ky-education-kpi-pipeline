# Kindergarten Readiness KPI Enhancements

## Date
2025-07-23

## Summary
Implemented additional metrics in the kindergarten readiness pipeline. The ETL now exposes counts and rates for each readiness component and prior setting. Existing metrics were renamed with an `_all_students` suffix for clarity.

To avoid duplicated KPI rows, all-students metrics are now emitted only when the prior setting column equals "All Students". Prior-setting metrics are generated exclusively for the "All Students" demographic rows.

### Final KPI Names
- `kindergarten_ready_with_interventions_count_all_students` – number of students needing interventions
- `kindergarten_ready_with_interventions_rate_all_students` – percent of students needing interventions
- `kindergarten_ready_count_all_students` – number of students fully ready
- `kindergarten_ready_rate_all_students` – percent of students fully ready
- `kindergarten_ready_with_enrichments_count_all_students` – number of students exceeding readiness expectations
- `kindergarten_ready_with_enrichments_rate_all_students` – percent exceeding readiness expectations
- `kindergarten_readiness_count_all_students` – total students ready
- `kindergarten_readiness_rate_all_students` – readiness rate for all students
- `kindergarten_readiness_total_all_students` – students screened
- `kindergarten_<prior_setting>_count_all_students` – total ready students from each prior setting
- `kindergarten_<prior_setting>_rate_all_students` – readiness rate for each prior setting
