# Journal Entry 83: Spending Per Student Pipeline Implementation

**Date:** 2025-11-24
**Status:** Completed

## Objective

Implement a new ETL pipeline for processing Kentucky Department of Education per-pupil spending data, with coverage from 2020-2025 and comprehensive testing.

## Background

Per-pupil spending is a critical equity indicator that shows resource allocation across schools and districts. Unlike most other KDE datasets, spending data:
- Has no demographic breakdowns (school-level aggregates only)
- Tracks spending across multiple funding sources (federal, state/local, combined)
- Contains both personnel and non-personnel expenditures
- Includes district-level totals as well as individual school data

## Implementation Summary

### 1. Data Source Configuration

Added `spending_per_student` configuration to `config/kde_sources.yaml`:
- **2024-2025:** `KYRC25_FT_Spending_per_Student.csv` (Azure blob storage)
- **2023-2024:** `KYRC24_FT_Spending_per_Student.csv`
- **2022-2023:** `spending_per_student_2023.csv`
- **2021-2022:** `spending_per_student_2022.csv`
- **2020-2021:** `spending_per_student_2021.csv`
- **2019-2020:** `spending_per_student_2020.csv`

Files downloaded successfully using `data/prepare_kde_data.py spending_per_student`.

### 2. Data Analysis

**Key findings from raw data inspection:**

**Column Name Variations:**
- 2020: Uppercase columns (`SCHOOL YEAR`, `PERSONNEL SPENDING PER STUDENT - FEDERAL FUNDS`)
- 2024+: Title case columns (`School Year`, `Personnel Expenditures State Local Per Student`)
- Column terminology changed: "Spending" → "Expenditures" for some metrics

**Data Format Changes:**
- 2024: Values contain commas (e.g., "9,827")
- 2020: Plain numeric values (e.g., 9827.0)
- 2025: File exists but contains no spending values (all NaN)

**Schema Differences:**
- 2020: Includes `MEMBERSHIP` column (not in later years)
- 2020: Explicit `Total Spending per Student - Federal Funds` column
- 2024+: Implicit federal total as `Total Expenditures Federal Per Student`

**No Demographic Dimension:**
- All rows represent school-level or district-level aggregates
- No breakdown by race, gender, disability status, etc.
- Requires synthetic "All Students" demographic for KPI format consistency

### 3. ETL Pipeline Design

Created `etl/spending_per_student.py` extending `BaseETL`:

**Module-Specific Column Mappings:**
```python
{
    # Federal funding
    'Personnel Spending per Student - Federal Funds': 'personnel_spending_federal',
    'Non-Personnel Spending per Student - Federal Funds': 'non_personnel_spending_federal',
    'Total Spending per Student - Federal Funds': 'total_spending_federal',
    'Total Expenditures Federal Per Student': 'total_spending_federal',

    # State/Local funding
    'Personnel Spending per Student - State/Local Funds': 'personnel_spending_state_local',
    'Personnel Expenditures State Local Per Student': 'personnel_spending_state_local',
    'Non-Personnel Spending per Student - State/Local Funds': 'non_personnel_spending_state_local',
    'Non-Personnel Expenditures State Local Per Student': 'non_personnel_spending_state_local',
    'Total Spending per Student - State/Local Funds': 'total_spending_state_local',

    # All funds
    'Total Spending per Student - All Fund Sources': 'total_spending_all_funds',
    'Total Expenditures Per Student': 'total_spending_all_funds',
}
```

**Metrics Extracted (7 total):**
1. `personnel_spending_per_student_federal` - Federal personnel costs
2. `non_personnel_spending_per_student_federal` - Federal non-personnel costs
3. `total_spending_per_student_federal` - Total federal spending
4. `personnel_spending_per_student_state_local` - State/local personnel costs
5. `non_personnel_spending_per_student_state_local` - State/local non-personnel costs
6. `total_spending_per_student_state_local` - Total state/local spending
7. `total_spending_per_student_all_funds` - Combined total spending

**Key Implementation Details:**

1. **Data Cleaning:** `clean_spending_values()` function handles:
   - Comma removal from 2024 format ("9,827" → 9827.0)
   - Numeric conversion with error handling
   - Negative value detection and replacement with NA

2. **Row Skipping Override:** Modified `should_skip_row()` to:
   - NOT skip rows without demographics (this data has none)
   - Skip only rows with missing school codes
   - Preserve district totals (---District Total---)

3. **Demographic Assignment:** Overrode `create_kpi_template()` to:
   - Always assign "All Students" as student_group
   - Maintain consistency with KPI format expectations

4. **Schema Handling:**
   - Normalized column names across years
   - Handled both uppercase and title case conventions
   - Mapped terminology variations (spending vs expenditures)

### 4. Testing Implementation

**Unit Tests** (`tests/test_spending_per_student.py` - 11 tests):
- Column name normalization (2020 and 2024 formats)
- Spending value cleaning (comma removal, validation)
- Row skipping logic
- KPI template creation with synthetic demographics
- Metric extraction (full and partial data)
- Suppressed metric defaults
- Full transformation for both formats
- Mixed year format handling

**End-to-End Tests** (`tests/test_spending_per_student_end_to_end.py` - 5 tests):
- Source-to-KPI transformation validation (10 random rows per file)
- All expected metrics present
- Student group consistency ("All Students" only)
- Value range validation ($0-$200k per student)
- Required KPI columns present

All 16 tests pass successfully.

### 5. Data Processing Results

**Files Processed:**
- `spending_per_student_2023.csv`: 1,661 rows → 9,261 KPI rows
- `spending_per_student_2022.csv`: 1,656 rows → 9,268 KPI rows
- `spending_per_student_2020.csv`: 1,654 rows → 9,303 KPI rows
- `spending_per_student_2021.csv`: 1,654 rows → 9,296 KPI rows
- `KYRC25_FT_Spending_per_Student.csv`: Skipped (no data)
- `KYRC24_FT_Spending_per_Student.csv`: 1,488 rows → 8,337 KPI rows

**Total Output:** 45,465 KPI rows across 5 years (2020-2024)

**Value Range:** $45.00 - $151,133.00 per student
**Median Spending:** $2,109.50 per student

Note: High outliers (>$100k) are legitimate and represent small schools or specialized programs with high per-pupil costs.

## Data Quality Observations

### Expected Warnings
- **Missing Demographics:** All years show missing required demographics (race, ethnicity, etc.)
  - This is expected and correct for this dataset
  - All records properly assigned "All Students" demographic
  - Demographic mapper correctly validates presence of at least one group

### 2025 Data Gap
- KYRC25 file exists but contains no spending values (all columns are empty)
- This appears to be a timing issue - data may not yet be published
- Pipeline correctly handles this by skipping the file

### Value Outliers
- Some schools show very high per-pupil spending ($100k+)
- Investigation shows these are typically:
  - Very small schools with few students
  - Specialized programs (alternative education, special needs)
  - Schools with unique facility or operational costs
- 95th percentile: $19,115 (more typical maximum)
- 99th percentile: $25,794

## Integration

### ETL Runner Configuration
The pipeline is automatically included when running `etl_runner.py` as it follows the standard module pattern.

### KPI Format Compliance
Output file (`data/processed/spending_per_student.csv`) includes all required KPI columns:
- `year`, `metric`, `district`, `school_name`, `student_group`
- `value`, `suppressed`, `county_number`, `county_name`, `district_number`
- `school_id`, `school_code`, `state_school_id`, `nces_id`
- `co_op`, `co_op_code`, `school_type`, `source_file`, `last_updated`

## Documentation Updates Required

1. **README.md:** Add spending_per_student to data sources list
2. **KPIS.md:** Document all 7 spending metrics with descriptions
3. **CLAUDE.md:** Already includes guidance on metric naming conventions

## Lessons Learned

1. **Synthetic Demographics:** This is the first pipeline requiring synthetic demographic assignment ("All Students"). The pattern established here can be reused for other school-level aggregate datasets.

2. **Value Range Validation:** Initial test threshold of $100k was too conservative. Real-world data requires understanding of legitimate outliers before setting validation bounds.

3. **Column Terminology:** KDE's inconsistent terminology (spending vs expenditures) requires careful mapping to ensure all years are captured correctly.

4. **Comma Handling:** 2024+ data uses comma separators requiring explicit cleaning - this pattern may appear in other recent datasets.

## Next Steps

1. Update README.md and KPIS.md with spending metrics documentation
2. Consider adding spending metrics to equity analysis dashboards
3. Monitor 2025 data for availability and rerun when published

## Files Created/Modified

**Created:**
- `etl/spending_per_student.py` - ETL pipeline module
- `tests/test_spending_per_student.py` - Unit tests
- `tests/test_spending_per_student_end_to_end.py` - End-to-end tests
- `notes/83--spending-per-student-pipeline-implementation.md` - This journal entry

**Modified:**
- `config/kde_sources.yaml` - Added spending_per_student configuration

**Generated:**
- `data/processed/spending_per_student.csv` - 45,465 KPI records
- `data/processed/spending_per_student_demographic_report.md` - Validation report

## References

- KDE Dataset URLs: `notes/kde_datasets_2024_2025.md` (and historical versions)
- Base ETL Pattern: `etl/base_etl.py`
- Similar Pipeline: `etl/postsecondary_readiness.py`
- Testing Pattern: `tests/test_postsecondary_readiness*.py`
