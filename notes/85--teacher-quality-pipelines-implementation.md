# Teacher Quality Pipeline Implementation (Continued)

**Date**: 2024-11-24
**Purpose**: Document implementation of additional teacher quality ETL pipelines

## Overview

This note documents the implementation of two additional teacher quality ETL pipelines following the investigation in note 84:
1. `students_taught_by_out_of_field_teachers.py`
2. `teacher_working_conditions.py`

These complement the previously implemented pipelines:
- `students_taught_by_ineffective_teachers.py`
- `teacher_turnover.py`
- `teacher_certification.py`

## Implemented Pipelines

### 1. Students Taught by Out-of-Field Teachers (`students_taught_by_out_of_field_teachers.py`)

**Description**: Equity metrics showing the percentage of students taught by out-of-field teachers (teaching outside their certification area), broken down by Title I status and demographics.

**Data Sources**:
- KYRC25_OVW_Students_Taught_by_Out_of_Field_Teachers.csv
- KYRC24_OVW_Students_Taught_by_Out_of_Field_Teachers.csv (corrupted headers)
- students_taught_by_out_of_field_teachers_2023.csv
- students_taught_by_out_of_field_teachers_2022.csv
- students_taught_by_out_of_field_teachers_2021.csv

**Years**: 2021-2025

**KPIs Generated**:
- `students_taught_by_out_of_field_teachers_rate_title_1`
- `students_taught_by_out_of_field_teachers_rate_not_title_1`
- `students_taught_by_out_of_field_teachers_rate_equity_gap`

**Demographics**: Full demographic breakdowns via `student_group` column:
- All Students
- White (non-Hispanic) / Non-White
- Economically Disadvantaged / Non-Economically Disadvantaged
- Students with Disabilities (IEP) / Student without Disabilities (IEP)
- English Learner / Non-English Learner

**Special Handling**:
- KYRC24 file has corrupted column headers (Unnamed: 13, Inexperienced Value, etc.)
- Pipeline maps corrupted headers to correct semantic names
- Historical files (2021-2023) use uppercase column names and `%` prefix
- Equity gap values can be negative (when Title 1 schools have lower rates)

**Output**: 90,720 KPI rows

### 2. Teacher Working Conditions (`teacher_working_conditions.py`)

**Description**: Survey-based index scores measuring teacher perceptions of working conditions across different impact measures.

**Data Sources**:
- KYRC25_OVW_Teacher_Working_Conditions.csv
- KYRC24_OVW_Teacher_Working_Conditions.csv
- teacher_working_conditions_2023.csv
- teacher_working_conditions_2022.csv
- teacher_working_conditions_2021.csv

**Years**: 2021-2025

**KPIs Generated**:
- `teacher_working_conditions_managing_student_behavior`
- `teacher_working_conditions_school_climate`
- `teacher_working_conditions_school_leadership`
- `teacher_working_conditions_teaching_environment` (historical only)

**Data Structure**:
- Pivoted data with one row per school-measure combination
- KYRC24/25: 3 measures per school
- Historical (2021-2023): 4 measures per school (includes Teaching Environment)

**Special Handling**:
- Historical files use "Composite" suffix on measure names
- School-level data only (no demographic breakdowns)
- All records have `student_group='All Students'`

**Data Quality Note**:
- Source data from KDE contains some duplicate rows (e.g., school 197 in 2021-2023)
- These duplicates are preserved in output as they exist in source

**Output**: 19,458 KPI rows

## Implementation Details

### Pipeline Architecture

Both pipelines extend `BaseETL` and follow the established patterns:

1. **Column Mapping**: Handle multiple naming conventions across years
2. **Extract Metrics**: Module-specific metric extraction logic
3. **KPI Conversion**: Convert to standard KPI format with proper student_group handling
4. **Suppressed Records**: Handle suppressed data with NA values

### Key Design Decisions

1. **Out-of-Field Teachers**:
   - Follows same structure as `students_taught_by_ineffective_teachers.py`
   - Uses double-underscore separator for equity metrics with demographics
   - Allows negative values for equity gap metrics

2. **Teacher Working Conditions**:
   - Overrides `create_kpi_template` to force `student_group='All Students'`
   - Maps impact measure names to metric suffixes
   - Handles both current and historical measure naming

## Testing

### Unit Tests
- `tests/test_students_taught_by_out_of_field_teachers.py` (13 tests)
- `tests/test_teacher_working_conditions.py` (15 tests)

### End-to-End Tests
- `tests/test_students_taught_by_out_of_field_teachers_end_to_end.py` (8 tests)
- `tests/test_teacher_working_conditions_end_to_end.py` (10 tests)

**All 46 tests pass.**

## Files Created/Modified

### New Files
- `etl/students_taught_by_out_of_field_teachers.py`
- `etl/teacher_working_conditions.py`
- `tests/test_students_taught_by_out_of_field_teachers.py`
- `tests/test_teacher_working_conditions.py`
- `tests/test_students_taught_by_out_of_field_teachers_end_to_end.py`
- `tests/test_teacher_working_conditions_end_to_end.py`

### Modified Files
- `KPIS.md` - Added documentation for new KPIs

## Data Quality Observations

### KYRC24 Out-of-Field File Corruption
The KYRC24 out-of-field teachers file has corrupted column headers:
- `Unnamed: 13` instead of `Title_I_Status`
- `Inexperienced Value` through `Inexperienced Value.8` instead of demographic columns

The pipeline handles this by mapping the corrupted names to correct semantic values.

### Source Data Duplicates
Some schools have duplicate rows in teacher working conditions source data:
- School 197 (Glasgow Independent) has duplicate entries in 2021, 2022, 2023
- These appear to be data entry errors in the source
- Pipeline preserves these duplicates as they exist in source

## Output Summary

| Pipeline | KPI Rows | Years | Metrics |
|----------|----------|-------|---------|
| students_taught_by_out_of_field_teachers | 90,720 | 2021-2025 | 3 |
| teacher_working_conditions | 19,458 | 2021-2025 | 4 |

## Next Steps

1. Consider implementing data quality checks for source data duplicates
2. Monitor KYRC25 data for similar header corruption issues
3. Review educator_qualifications.py implementation (separate investigation)
