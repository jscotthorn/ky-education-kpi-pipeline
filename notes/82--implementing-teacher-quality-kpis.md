# Implementing Teacher Quality KPI Pipelines

**Date:** 2025-11-24
**Status:** Implemented (Pending Validation)

## Objective

Implement ETL pipelines for four requested teacher quality Key Performance Indicators (KPIs) using Kentucky Department of Education (KDE) data:
1. Per-pupil expenditure
2. Teacher experience (average years)
3. Student-teacher ratio
4. % Novice teachers (<3 years experience)

## Findings

Upon analyzing the available KYRC24 and KYRC25 data files, we determined:

*   **Available:**
    *   **Teacher Experience:** `KYRC25_OVW_Average_Years_School_Experience.csv`
    *   **Student-Teacher Ratio:** `KYRC25_OVW_Student_to_Teacher_Ratio.csv`
    *   **Novice Teachers:** `KYRC25_OVW_Inexperienced_Teachers.csv` and `KYRC25_OVW_Students_Taught_by_Inexperienced_Teachers.csv`
*   **Not Available:**
    *   **Per-pupil expenditure:** This financial metric is not included in the standard Kentucky Report Card (KYRC) data exports. It likely requires accessing KDE's separate finance/budget data portal.

## Implementation Details

Three new ETL modules were created to process the available indicators.

### 1. Teacher Experience (`etl/teacher_experience.py`)

*   **Source:** `KYRC*_OVW_Average_Years_School_Experience.csv`
*   **KPI:** `teacher_average_years_experience`
*   **Logic:** Extracts educator count and average years. Handles institutional-level aggregation (no demographic breakdowns).

### 2. Student-Teacher Ratio (`etl/student_teacher_ratio.py`)

*   **Source:** `KYRC*_OVW_Student_to_Teacher_Ratio.csv`
*   **KPI:** `student_teacher_ratio`
*   **Logic:**
    *   Parses ratio strings in the format "15:01" to numeric values (15.0).
    *   Uses regex `^([\d.]+):(\d+)$` to extract the numerator.
    *   Handles potential variations or plain numeric values.

### 3. Novice Teachers (`etl/novice_teachers.py`)

*   **Sources:**
    *   `KYRC*_OVW_Inexperienced_Teachers.csv` (Institutional percentages)
    *   `KYRC*_OVW_Students_Taught_by_Inexperienced_Teachers.csv` (Equity metrics)
*   **KPIs:**
    *   `novice_teacher_rate_less_than_1_year`
    *   `novice_teacher_rate_1_to_3_years`
    *   `students_taught_by_inexperienced_teachers_rate` (broken down by Title I status and demographics)
*   **Equity Focus:** The second file is critical as it reveals which students (by race, economic status, etc.) are being taught by inexperienced teachers, highlighting systematic quality gaps.

## Configuration Updates

### `config/kde_sources.yaml`
Added `raw_directories` entries for `teacher_experience`, `student_teacher_ratio`, and `novice_teachers`, mapping both KYRC24 and KYRC25 files.

### `config/mappings.yaml`
Added source configurations with:
*   **Dtype enforcement:** Ensuring correct types for counts (Int64) and rates (float64).
*   **Processing metadata:** `processing_date` and `data_quality_flag`.
*   **Column Mappings:** Aligned with actual CSV headers (e.g., "Student Teacher Ratio" instead of "Ratio", "Total New Teachers With 1 3 Years Experience" instead of hyphenated).

## Issues Resolved

1.  **Logging Error:** Fixed a typo `logging.getName(__name__)` -> `logging.getLogger(__name__)` in `student_teacher_ratio.py`.
2.  **Missing Configs:** Added missing entries to `mappings.yaml` which were causing "No valid KPI rows created" warnings.
3.  **Column Mismatches:** Corrected ETL module mappings to match the exact spacing and format of CSV headers (e.g., "1 3 Years" vs "1-3 Years").

## Next Steps

*   **Validation:** Run the pipelines end-to-end (requires Python environment setup).
*   **Master File Check:** Verify new KPIs appear in `data/kpi/kpi_master.csv`.
*   **Per-Pupil Expenditure:** Decide whether to pursue alternative data sources for the missing financial metric.
