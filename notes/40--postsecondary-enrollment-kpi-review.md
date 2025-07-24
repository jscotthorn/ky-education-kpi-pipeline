# Postsecondary Enrollment KPI Review

## Date: 2025-07-25

## Objective
Summarize how `etl/postsecondary_enrollment.py` maps KPI fields and document potential additional metrics from available source data.

## KPI Mapping
- Uses `module_column_mappings` to normalize raw columns. Key mappings:
  - `Total In Group` → `total_in_group`
  - `Public College Enrolled In State` → `public_college_enrolled`
  - `Private College Enrolled In State` → `private_college_enrolled`
  - `College Enrolled In State` → `college_enrolled_total`
  - Percentage columns mapped similarly for rates.
- `extract_metrics()` converts each row to seven KPI metrics:
  1. `postsecondary_enrollment_total_cohort` ← `total_in_group`
  2. `postsecondary_enrollment_public_count` ← `public_college_enrolled`
  3. `postsecondary_enrollment_private_count` ← `private_college_enrolled`
  4. `postsecondary_enrollment_total_count` ← `college_enrolled_total`
  5. `postsecondary_enrollment_public_rate` ← `public_college_rate`
  6. `postsecondary_enrollment_private_rate` ← `private_college_rate`
  7. `postsecondary_enrollment_total_rate` ← `college_enrollment_rate`
- Missing or suppressed rows default all metrics to `NA`.

## Potential Additional KPIs
- **Not Enrolled Count/Rate**: Calculate students not enrolling in Kentucky institutions using `total_in_group - college_enrolled_total` and `100 - college_enrollment_rate`.
- **Public vs Private Ratio**: Compute the ratio of public to private enrollment to highlight institutional preferences.
- **Leverage ADLF Graduate Files**: `KYRC24_ADLF_Current_Year_Graduates.csv` and `KYRC24_ADLF_Graduate_Outcomes.csv` include graduate counts and post-graduation pathways (college, work, military). Integrating these sources could yield KPIs such as workforce entry rate or military enlistment rate.

These additions would extend the pipeline's coverage beyond in-state college-going rates and provide a fuller picture of post-graduation outcomes.
