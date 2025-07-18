# Graduation Rates Data Analysis

**Date**: 2025-07-18  
**Analyst**: Claude Code  
**Purpose**: Initial analysis of graduation rates data for ETL pipeline development

## Data Source Overview

Found 4 files in `data/raw/graduation-rates/`:
- `KYRC24_ACCT_4_Year_High_School_Graduation.csv` (2024 data)
- `graduation_rate_2021.csv` (2021 data)
- `graduation_rate_2022.csv` (2022 data)
- `graduation_rate_2023.csv` (2023 data)

## Schema Evolution Analysis

### 2024 Format (KYRC24_ACCT_4_Year_High_School_Graduation.csv)
- **Columns**: 16 fields including School Year, identifiers, demographics, suppression flag, graduation rate
- **Key Feature**: Simplified format with only 4-year graduation rate
- **Naming**: Mixed case column names
- **Sample**: School Year `20232024`, Rate `92.3` for All Students

### 2021 Format (graduation_rate_2021.csv)
- **Columns**: 22 fields - most comprehensive format
- **Key Features**: 
  - Both 4-year and 5-year graduation rates
  - Actual cohort counts (`NUMBER OF GRADS IN 4-YEAR COHORT`, `NUMBER OF STUDENTS IN 4-YEAR COHORT`)
  - Separate suppression flags for 4-year and 5-year data
- **Naming**: ALL CAPS column names
- **Sample**: 96.1% 4-year rate, 99.5% 5-year rate for Adair County

### 2022-2023 Format
- **Columns**: 18 fields - intermediate format
- **Key Features**: 
  - Both 4-year and 5-year graduation rates
  - No cohort counts (removed from 2021 format)
  - Separate suppression flags maintained
- **Naming**: ALL CAPS column names
- **Sample**: 94.7% 4-year rate, 97.4% 5-year rate for Adair County (2022)

## Data Quality Issues Identified

1. **Inconsistent Column Naming**
   - 2024: Mixed case ("School Year", "County Name")
   - 2021-2023: ALL CAPS ("SCHOOL YEAR", "COUNTY NAME")

2. **Missing Value Representations**
   - Empty strings for suppressed data
   - Mix of "Y"/"N" and empty cells for suppression flags

3. **Data Suppression Patterns**
   - Small cell sizes suppressed with "*" or empty values
   - Suppression flags inconsistently applied

4. **Demographic Categories**
   - Consistent across years: All Students, Female, Male
   - Race/Ethnicity: African American, American Indian or Alaska Native, Asian, Hispanic or Latino, Native Hawaiian or Pacific Islander, Two or More Races, White (not visible in samples but likely present)

## Recommended ETL Approach

1. **Normalize Column Names**: Convert all to lowercase with underscores
2. **Standardize Missing Values**: Convert empty strings to None/NaN
3. **Handle Suppression**: Create boolean suppression flags
4. **Merge Strategy**: Union all years with year identifier
5. **Primary Key**: Combination of school_year + district_number + school_number + demographic

## Next Steps

1. Create `etl/graduation_rates.py` module
2. Design configuration for column mapping and data types
3. Implement logic to handle schema variations by year
4. Create comprehensive test suite covering all formats
5. Update `config/mappings.yaml` with graduation rates configuration

## Technical Considerations

- **Memory**: Files appear reasonably sized for pandas processing
- **Performance**: May need chunking for very large districts
- **Validation**: Should validate graduation rates are between 0-100%
- **Audit Trail**: Preserve original suppression flags for compliance

## Implementation Notes

### Python Version Compatibility
- **Issue Discovered**: System defaults to Python 2.7.16 with `python` command
- **Solution**: Use `python3` (version 3.12.2 available) for all operations
- **Type Annotations**: Used `typing.Dict` instead of `dict[]` for broader compatibility
- **Testing**: Always use `python3` for syntax checking and module execution

### Syntax Error Resolution
- **Original Error**: `SyntaxError: invalid syntax` on line 17 with type annotations
- **Root Cause**: Python 2.7 doesn't support type annotations
- **Fix Applied**: Updated all commands to use `python3` instead of `python`
- **Status**: ✅ Syntax validation passes with `python3 -m py_compile`