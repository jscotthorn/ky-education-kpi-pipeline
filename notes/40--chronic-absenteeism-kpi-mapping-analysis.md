# Chronic Absenteeism KPI Mapping Analysis

**Date:** 2025-07-22

## Overview
A review of `etl/chronic_absenteeism.py` to document how each KPI is generated from the raw source data and to identify potential additional metrics that could be derived from the files.

## Current KPI Generation
- **Column Mappings** – The module maps raw columns to three standardized fields:
  - `Chronically Absent Students` → `chronically_absent_count`
  - `Students Enrolled 10 or More Days` → `enrollment_count`
  - `Chronic Absenteeism Rate` → `chronic_absenteeism_rate`
  These mappings also support the older 2023 uppercase column names.
- **Metric Extraction** – `extract_metrics()` reads each row and produces three metrics per grade level using the grade suffix from the `grade` column:
  1. `chronic_absenteeism_rate_{grade}` – percentage of students chronically absent.
  2. `chronic_absenteeism_count_{grade}` – number of chronically absent students.
  3. `chronic_absenteeism_enrollment_{grade}` – enrollment count used as the denominator.
- **Suppression Handling** – If a record is marked suppressed, `get_suppressed_metric_defaults()` still creates the three metric names with `pd.NA` values so suppressed data is retained.
- **Data Cleaning** – `clean_numeric_values()` removes commas and enforces valid ranges (0‑100% for rates, non‑negative counts). `standardize_suppression_field()` normalizes various suppression indicators.
- **Grade Normalization** – The BaseETL class converts grade strings like "Grade 1" or "Kindergarten" to consistent suffixes (`grade_1`, `kindergarten`, etc.) before the metric names are assembled.

## Potential Additional KPIs
While the source files only provide counts, enrollment, and rates, there are a few derivative metrics we could compute:
1. **Attendance Rate** – `attendance_rate_{grade}` = `100 - chronic_absenteeism_rate_{grade}`. This would show the proportion of students NOT chronically absent.
2. **Chronically Present Count** – `chronically_present_count_{grade}` = `enrollment_count - chronically_absent_count`. Useful when the underlying counts are available.
3. **Chronically Present Rate** – Derived from the two values above. Provides the same information as attendance rate but framed as a percentage of enrollment.

These metrics would leverage existing columns without needing new data sources and could provide a clearer picture of attendance strength alongside chronic absenteeism.
