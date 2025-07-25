# 36 -- Safe Schools Climate Pipeline Implementation

## Overview
Implemented Pipeline 3 from the longitudinal data analysis: Safe Schools Climate & Safety ETL pipeline to process Kentucky education safety and climate data.

## Implementation Summary

### Data Sources Processed
1. **KYRC24_SAFE_Precautionary_Measures.csv** (2024)
   - School-level safety policy compliance data
   - 8 Yes/No policy questions per school
   - Generated `safety_policy_compliance_rate` metric

2. **Quality of School Climate and Safety Survey Index Scores** (2021-2023)
   - Aggregated climate and safety perception scores by demographic
   - Generated `climate_index_score` and `safety_index_score` metrics
   - Scores range from 0-100

### Key Features Implemented

#### ETL Pipeline (`etl/safe_schools_climate.py`)
- **Selective File Processing**: Only processes relevant files (precautionary measures + index scores), skips individual survey response files
- **Custom Transform Logic**: Overrides base ETL to handle different file types appropriately
- **Policy Compliance Calculation**: Calculates percentage of "Yes" responses across 8 safety policy questions
- **Index Score Validation**: Validates scores are within 0-100 range
- **Demographic Mapping**: Uses standardized demographic categories for consistent reporting

#### Metrics Generated
1. **`safety_policy_compliance_rate`**: Percentage of implemented safety policies (0-100%)
2. **`climate_index_score`**: School climate perception index (0-100)
3. **`safety_index_score`**: School safety perception index (0-100)

#### Data Quality Features
- **Missing Value Handling**: Proper handling of NA/empty values in policy responses and index scores
- **Suppression Support**: Maintains suppression flags from source data
- **Data Validation**: Range validation for scores, case-insensitive policy responses
- **Duplicate Prevention**: Removes duplicates while keeping most recent data

### Technical Implementation Details

#### File Structure
```
etl/safe_schools_climate.py          # Main ETL module
tests/test_safe_schools_climate.py   # Unit tests (14 tests)
tests/test_safe_schools_climate_end_to_end.py  # E2E tests (8 tests)
```

#### Key Functions
- `calculate_policy_compliance_rate()`: Calculates safety policy compliance percentage
- `clean_index_scores()`: Validates and cleans climate/safety index scores
- `SafeSchoolsClimateETL.extract_metrics()`: Extracts appropriate metrics based on data type
- `SafeSchoolsClimateETL.process_precautionary_measures()`: Special handling for policy data

#### Testing Coverage
- **Unit Tests**: 14 tests covering calculation logic, data cleaning, and ETL methods
- **End-to-End Tests**: 8 tests covering full pipeline execution, output validation, and data quality
- **Test Coverage**: Policy compliance calculation, index score validation, demographic mapping, suppression handling

### Output Statistics
- **Records Generated**: 1,314 KPI records from 3 processed files
- **Metrics Coverage**: 3 distinct metrics across multiple years and demographics
- **Years Covered**: 2021-2024
- **File Size**: ~748MB raw data processed

### Challenges Resolved
1. **File Selection**: Base ETL processed all CSV files; implemented custom file filtering
2. **Demographic Mapping**: Added proper demographic standardization using existing mapper
3. **Missing Dependencies**: Fixed method name inconsistencies between base and derived classes
4. **Year Extraction**: Handled different year formats in filenames vs. data columns
5. **Test Data Types**: Resolved integer vs. string year comparisons in tests

### Data Pipeline Integration
- Follows established KPI format with standard columns
- Uses consistent metric naming convention (`*_rate`, `*_score`)
- Integrates with existing demographic mapping configuration
- Compatible with master KPI aggregation process

### Performance Notes
- **Processing Time**: ~30 seconds for full dataset (including demographic warnings)
- **Memory Usage**: Handles large survey files efficiently by processing only index scores
- **Error Handling**: Comprehensive try/catch with file-specific error reporting

## Next Steps
1. Add `Non-Military Dependent` demographic mapping to reduce warnings
2. Consider optimizing demographic mapping for large datasets
3. Monitor data quality as new survey years are added

## Files Modified/Created
- `etl/safe_schools_climate.py` (new)
- `tests/test_safe_schools_climate.py` (new)
- `tests/test_safe_schools_climate_end_to_end.py` (new)
- `data/raw/safe_schools_climate/` (new directory with 14 files)
- `data/processed/safe_schools_climate_kpi.csv` (generated output)

## Validation Results
✅ All unit tests pass (14/14)  
✅ All end-to-end tests pass (8/8)  
✅ Pipeline executes successfully  
✅ Output format matches KPI standard  
✅ Metrics follow naming conventions  
✅ Data quality validations working  

**Pipeline 3 implementation complete and ready for production use.**