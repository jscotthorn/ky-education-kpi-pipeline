# Student Enrollment Duplicate Records Fix & Secondary Enrollment KPI

**Date**: 2025-11-25

## Issue

The `student_enrollment.csv` processed KPI file contained duplicate records for the same school/year/metric/student_group combination with different values. This caused downstream analysis scripts to produce incorrect results.

### Example

Pembroke Elementary School (school_id: 115130) for 2025 had two `student_enrollment_total` / `All Students` records:

| Row    | Year | School                     | Metric                   | Student Group | Value |
|--------|------|----------------------------|--------------------------|---------------|-------|
| 381715 | 2025 | Pembroke Elementary School | student_enrollment_total | All Students  | 1     |
| 952195 | 2025 | Pembroke Elementary School | student_enrollment_total | All Students  | 681   |

### Impact

When computing demographic percentages (e.g., % economically disadvantaged), if the erroneous value=1 was selected, calculations became wildly incorrect:
- Economically Disadvantaged count: 502
- Total enrollment (wrong): 1
- Calculated percentage: 50,200% instead of ~74%

## Root Cause

The ETL was processing both **primary enrollment** and **secondary enrollment** files into the same KPI, which created duplicate records with conflicting values.

The **primary enrollment files contain ALL grades** (PreK-12), while secondary enrollment files contain only secondary grades (6-12). When both were processed into `student_enrollment`:

1. Primary file for Pembroke Elementary: `All Grades = 681` (correct total)
2. Secondary file for Pembroke Elementary: `All Grades = 1` (only secondary students at an elementary school)

Both created a `student_enrollment_total` metric, resulting in duplicates.

## Solution

Separated secondary enrollment into its own distinct KPI (`secondary_enrollment`) rather than deleting the data.

### Changes Made

1. **Updated `config/kde_sources.yaml`** - Split into two separate source definitions:

```yaml
student_enrollment:
  # Note: Primary enrollment files contain ALL grades (PreK-12).
  # Secondary enrollment is processed separately as secondary_enrollment KPI.
  - "KYRC24_OVW_Student_Enrollment.csv"
  - url: "kyrc25"
    file: "KYRC25_OVW_Student_Enrollment.csv"
  - "primary_enrollment_2023.csv"
  - "primary_enrollment_2022.csv"
  - "primary_enrollment_2021.csv"
  - "primary_enrollment_2020.csv"

secondary_enrollment:
  # Secondary enrollment (grades 6-12) - separate from primary enrollment
  - url: "kyrc25"
    file: "KYRC25_OVW_Secondary_Enrollment.csv"
  - "secondary_enrollment_2023.csv"
  - "secondary_enrollment_2022.csv"
  - "secondary_enrollment_2021.csv"
  - "secondary_enrollment_2020.csv"
```

2. **Created new ETL module** `etl/secondary_enrollment.py`:
   - Processes secondary enrollment files (grades 6-12 only)
   - Outputs metrics with `secondary_enrollment_*` prefix to avoid collision
   - Includes aggregations for middle school (6-8) and high school (9-12)

3. **Added to `config/mappings.yaml`** - Added `secondary_enrollment` source configuration

4. **Moved secondary files** to `data/raw/secondary_enrollment/` directory

### New Secondary Enrollment Metrics

| Metric | Description |
|--------|-------------|
| `secondary_enrollment_total` | Total secondary students (grades 6-12) |
| `secondary_enrollment_grade_6` through `_grade_12` | Individual grade counts |
| `secondary_enrollment_middle` | Middle school aggregate (grades 6-8) |
| `secondary_enrollment_high_school` | High school aggregate (grades 9-12) |

## Verification

After the fix:

```python
# student_enrollment - Pembroke Elementary 2025 has correct value
pembroke = df[(df['school_id'] == 115130) & (df['year'] == 2025) &
              (df['metric'] == 'student_enrollment_total') &
              (df['student_group'] == 'All Students')]
# Result: 1 record with value = 681.0 (correct)

# Zero duplicates in student_enrollment
dupes = df[
    (df['metric'] == 'student_enrollment_total') &
    (df['student_group'] == 'All Students')
].groupby(['year', 'school_id']).size()
# Result: 0 duplicates

# secondary_enrollment - now a separate KPI
sec_df = pd.read_csv('secondary_enrollment.csv')
sec_df['metric'].value_counts()
# secondary_enrollment_total          4908
# secondary_enrollment_high_school    4853
# secondary_enrollment_grade_11       4140
# ...
```

## Files Modified

- `config/kde_sources.yaml` - Split enrollment sources
- `config/mappings.yaml` - Added secondary_enrollment configuration
- `etl/secondary_enrollment.py` - New ETL module (created)
- `data/raw/secondary_enrollment/` - New directory with secondary files
