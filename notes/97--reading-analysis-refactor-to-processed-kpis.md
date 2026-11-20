# Reading Analysis Script Refactor to Use Processed KPIs

**Date:** 2025-11-25

## Summary

Refactored `analysis/scripts/prepare_reading_analysis.py` to load Grade 3 reading assessment data from processed KPI files instead of raw KDE per-year files. This leverages the ETL pipeline's normalization and data quality handling.

## Changes Made

### 1. Updated Data Source Path

Changed `PROCESSED_DIR` to point to the portal's processed data directory:

```python
PORTAL_DIR = BASE_DIR.parent / "ky-education-portal"
PROCESSED_DIR = PORTAL_DIR / "src" / "data" / "processed"
```

### 2. Chunked Loading for Large Files

The `kentucky_summative_assessment.csv` file is ~30M rows. Added chunked loading with progress reporting:

```python
CHUNK_SIZE = 100_000
TARGET_METRIC = 'kentucky_summative_assessment_reading_proficient_distinguished_rate_grade_3'

for chunk_num, chunk in enumerate(pd.read_csv(assessment_file, chunksize=CHUNK_SIZE)):
    # Filter each chunk immediately to save memory
    chunk_filtered = chunk[
        (chunk['metric'] == TARGET_METRIC) &
        (chunk['student_group'] == 'All Students') &
        ...
    ]
```

Progress updates every 500K rows prevent the script from appearing stuck.

### 3. Fixed Duplicate Column Issue

The processed file has both `school_id` and `state_school_id` columns. Removed erroneous rename that was creating duplicate columns.

### 4. Demographics Calculation Fixes

Added numeric conversion for enrollment values:
```python
totals['value'] = pd.to_numeric(totals['value'], errors='coerce')
demo_counts['value'] = pd.to_numeric(demo_counts['value'], errors='coerce')
```

Added defensive `max` aggregation to handle any duplicate school/year entries in enrollment data:
```python
totals = totals.groupby([...], as_index=False).agg({'value': 'max'})
```

## Bug Discovered: Duplicate Enrollment Records

During testing, discovered that `student_enrollment.csv` contained duplicate records for the same school/year/metric/student_group with different values. Example:

- Pembroke Elementary 2025: value=1 (erroneous) and value=681 (correct)

This caused demographic percentage calculations to be wildly incorrect (e.g., 50,200% economically disadvantaged). A separate fix was applied to the student enrollment ETL pipeline.

## Results

After fixes:

| Metric | Records |
|--------|---------|
| Grade 3 Reading observations | 3,145 |
| Years covered | 2021-2025 |
| Unique schools | 702 |
| Mean proficiency rate | 44.3% |

Demographics now correct:
- pct_economically_disadvantaged: mean=65.8%, std=16.7%
- pct_african_american: mean=9.3%, std=14.7%

## Files Modified

- `analysis/scripts/prepare_reading_analysis.py`

## Related

- Note 96: Student enrollment duplicate records fix (ETL pipeline fix)
