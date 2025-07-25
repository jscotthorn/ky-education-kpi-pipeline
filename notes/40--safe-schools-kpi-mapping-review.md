# 40 -- Safe Schools KPI Mapping Review

## Date: 2025-07-22

## Objective
Review the three Safe Schools ETL pipelines (events, discipline, climate) to understand how each KPI is derived from source files and identify opportunities for additional metrics.

## Current KPI Mapping

### 1. `safe_schools_events`
- **Sources**: `KYRC24_SAFE_Behavior_Events_*` (Type, Grade, Location, Context) and historical files (`safe_schools_event_details_*.csv`, `safe_schools_events_by_grade_*.csv`).
- **Metrics Generated**:
  - **Tier 1 – Students Affected**: `safe_students_affected_*` metrics derived from "All Students" rows.
  - **Tier 2 – Demographic Breakdown**: `safe_event_count_*_by_demo` metrics from demographic rows.
  - **Tier 3 – Total Events**: `safe_event_count_*` metrics from "Total Events" rows.
  - **Tier 4 – Derived Rates**: `safe_incident_rate_*` calculated as total events ÷ students affected.
- **Mapping Logic**: `_process_rows_helper` translates column names (e.g., `Alcohol` → `safe_event_count_alcohol`) using the `base_map` dictionary and appends suffixes for demographics or student scope.

### 2. `safe_schools_discipline`
- **Sources**: `KYRC24_SAFE_Discipline_Resolutions.csv`, `KYRC24_SAFE_Legal_Sanctions.csv`, and historical discipline files.
- **Metrics Generated**: Rate-based indicators only. Example mappings:
  - `restraint_count` → `restraint_rate`
  - `out_of_school_suspension_count` → `out_of_school_suspension_rate`
  - `arrest_count` → `arrest_rate`
- **Mapping Logic**: `extract_metrics()` computes each rate as `count / total * 100` and drops zero-value metrics. No counts are currently output.

### 3. `safe_schools_climate`
- **Sources**: `KYRC24_SAFE_Precautionary_Measures.csv`, `KYRC24_ACCT_Index_Scores.csv`, survey result files, and historical accountability profiles.
- **Metrics Generated**:
  - `safety_policy_compliance_rate` – percent of "Yes" responses across eight policy questions.
  - `climate_index_score` and `safety_index_score` – from index score files, survey question indexes, or combined historical rates.
- **Mapping Logic**: `extract_metrics()` chooses which metric(s) to produce based on available columns for each row.

## Opportunities for Additional KPIs
1. **Discipline Counts** – The discipline pipeline only outputs rates. Counts (e.g., `restraint_count`, `arrest_count`) could be exposed to allow direct comparison of incident volume between schools without recalculating from rates and totals.
2. **Policy Question Metrics** – Precautionary measures include individual Yes/No fields (e.g., `visitors_sign_in`, `resource_officer`). We currently collapse these into a single compliance rate. Exposing each question as a separate binary metric would enable more granular tracking of safety practices.
3. **Survey Response Percentages** – The climate survey files contain `agree_strongly_agree_pct` for specific questions. These percentages are not used. Reporting them by question could provide insight into which aspects of school climate need improvement.
4. **Discipline Totals** – Historical files include `total_discipline_resolutions`. A KPI for the total number of resolutions could help contextualize the rate metrics.

## Conclusion
The Safe Schools pipelines successfully standardize events, discipline, and climate data into KPI format. However, several columns in the raw files are not currently surfaced. Adding metrics for discipline counts, individual policy questions, survey response percentages, and total discipline resolutions would expand analytical capabilities without significant pipeline changes.
