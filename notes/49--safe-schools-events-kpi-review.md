# Journal Entry 40: Safe Schools Events KPI Review

**Date:** 2025-07-21

## Overview
This entry reviews `etl/safe_schools_events.py` to document how KPIs are derived from the raw Safe Schools data files and to identify additional fields that could be surfaced as new KPIs.

## KPI Generation Flow
1. **Column Normalization** – `normalize_column_names()` standardizes field names from both KYRC24 and historical datasets. Mappings cover school identifiers, demographic labels and all metric columns.
2. **Source Detection** – `add_derived_fields()` infers the type of file (events by type, grade, location, context or historical details) based on which metric columns exist. The detected value is stored in `data_source`.
3. **Cleaning** – `clean_event_data()` removes suppression markers and coerces numeric columns to floats, ensuring negative values are set to `NaN`.
4. **Tier Separation** – `convert_to_kpi_format()` splits rows into three logical tiers based on the `demographic` column:
   - *Students Affected* (rows labelled "All Students")
   - *Total Events* (rows labelled "Total Events")
   - *Demographic Breakdowns* (all other rows)
5. **Metric Mapping** – `_process_rows_helper()` converts wide data into KPI rows. The metric list depends on the `data_source`:
   - Events by type or detail → `total_events`, `alcohol_events`, `assault_1st_degree`, `drug_events`, `harassment_events`, `other_assault_events`, `other_state_events`, `tobacco_events`, `weapon_events`.
   - Events by grade → grade-level columns `all_grades`, `preschool`, `kindergarten`, `grade_1` … `grade_14`.
   - Events by location → `classroom`, `bus`, `hallway_stairwell`, `cafeteria`, `restroom`, `gymnasium`, `playground`, `other_location`, `campus_grounds`.
   - Events by context → `school_sponsored_during`, `school_sponsored_not_during`, `non_school_sponsored_during`, `non_school_sponsored_not_during`.
   - Mapping abbreviations (e.g. `assault_1st_degree` → `assault_1st`) are handled via `base_map`.
6. **Three‑Tier Output** –
   - Students affected metrics prefixed `safe_students_affected_…`.
   - Demographic metrics suffixed `_by_demo`.
   - Total event metrics prefixed `safe_event_count_…`.
7. **Derived Rates** – `_calculate_derived_rates()` computes `safe_incident_rate_<metric>` by dividing total events by students affected where both values exist.
8. **All results are concatenated and written to `safe_schools_events.csv` along with a demographic audit report.**

## Additional KPI Opportunities
The raw files include several identification fields that are currently dropped from the KPI output such as `co_op`, `co_op_code`, `district_number` and `school_type`. These could be used to create aggregated metrics by educational cooperative or by school type (A1, A5, etc.).

Historical event detail files also contain location and context columns which are currently converted only into event counts. If unique student counts by location or by context are available in the raw data, separate "students affected" metrics could be generated for those categories. This would enable incident rate calculations for specific locations or contexts (e.g. incidents per affected student in the classroom).

Finally, the pipeline only calculates incident rates where both "students affected" and "total events" metrics exist. If enrollment data were joined, additional rates such as events per 100 enrolled students could be derived.
