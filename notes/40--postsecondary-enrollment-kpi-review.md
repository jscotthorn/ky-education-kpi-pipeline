# Postsecondary Enrollment KPI Review

## Overview
Investigated how `etl/postsecondary_enrollment.py` generates current KPIs from the raw Kentucky Department of Education files. The pipeline uses `BaseETL` to standardize columns and metrics.

## KPI Mapping
- **total_in_cohort** → derived from `Total In Group`
- **public_ky_college_count** → from `Public College Enrolled In State`
- **private_ky_college_count** → from `Private College Enrolled In State`
- **total_ky_college_count** → from `College Enrolled In State`
- **public_ky_college_rate** → from `Percentage Public College Enrolled In State`
- **private_ky_college_rate** → from `Percentage Private College Enrolled In State`
- **total_ky_college_rate** → from `Percentage College Enrolled In State`

Mapping occurs in `module_column_mappings` and `extract_metrics`:
```
metrics["postsecondary_enrollment_total_in_cohort"] = row.get("total_in_group")
metrics["postsecondary_enrollment_public_ky_college_count"] = row.get("public_college_enrolled")
metrics["postsecondary_enrollment_private_ky_college_count"] = row.get("private_college_enrolled")
metrics["postsecondary_enrollment_total_ky_college_count"] = row.get("college_enrolled_total")
metrics["postsecondary_enrollment_public_ky_college_rate"] = row.get("public_college_rate")
metrics["postsecondary_enrollment_private_ky_college_rate"] = row.get("private_college_rate")
metrics["postsecondary_enrollment_total_ky_college_rate"] = row.get("college_enrollment_rate")
```

## Potential Additional Metrics
The source files appear to only include in-state public/private counts and rates. However we could derive new KPIs:
1. **Non-enrollment count** = `total_in_cohort - total_ky_college_count`
2. **Non-enrollment rate** = `100 - total_ky_college_rate`

If future files include out-of-state enrollment or 2-year/4-year splits, those could be added similarly.

## Next Steps
- Confirm whether raw files contain additional columns (e.g., out-of-state or institution type) not currently mapped.
- If available, extend the pipeline to generate complementary KPIs like non-enrollment metrics.
