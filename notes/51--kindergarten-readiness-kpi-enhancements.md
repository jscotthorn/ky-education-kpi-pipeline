# Kindergarten Readiness KPI Enhancements

## Date
2025-07-23

## Summary
Implemented additional metrics in the kindergarten readiness pipeline. The ETL now exposes counts and rates for each readiness component and prior setting.

To avoid duplicated KPI rows, all-students metrics are now emitted only when the prior setting column equals "All Students". Prior-setting metrics are generated exclusively for the "All Students" demographic rows.

### Final KPI Names
- `kindergarten_ready_with_interventions_count` – number of students needing interventions
- `kindergarten_ready_with_interventions_rate` – percent of students needing interventions
- `kindergarten_ready_count` – number of students fully ready
- `kindergarten_ready_rate` – percent of students fully ready
- `kindergarten_ready_with_enrichments_count` – number of students exceeding readiness expectations
- `kindergarten_ready_with_enrichments_rate` – percent exceeding readiness expectations
- `kindergarten_readiness_count` – total students ready
- `kindergarten_readiness_rate` – readiness rate for all students
- `kindergarten_readiness_total` – students screened
- `kindergarten_<prior_setting>_count` – total ready students from each prior setting
- `kindergarten_<prior_setting>_rate` – readiness rate for each prior setting
