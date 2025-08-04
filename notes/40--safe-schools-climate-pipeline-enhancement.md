# Safe Schools Climate Pipeline Enhancement

## Date: 2025-07-21

## Overview
Enhanced the Safe Schools Climate ETL pipeline to support additional 2024 data files and historical accountability profile data for comprehensive longitudinal analysis.

## Problem Statement
The safe_schools_climate pipeline was missing several important data sources:
1. KYRC24_ACCT_Index_Scores.csv - Direct climate and safety index scores for 2024
2. KYRC24_ACCT_Survey_Results.csv - Detailed survey question responses for 2024  
3. Historical accountability_profile files (2020-2023) containing climate/safety indicators
4. Missing 2021 survey files from downloads

## Key Findings

### File Structure Analysis
- **2024 Files**: 3 distinct types with different structures
  - KYRC24_ACCT_Index_Scores.csv: Direct Climate Index and Safety Index columns
  - KYRC24_ACCT_Survey_Results.csv: Question-level responses with Question Type and Question Index
  - KYRC24_SAFE_Precautionary_Measures.csv: Policy compliance Yes/No questions
  
- **Historical Files**: accountability_profile_*.csv files contain:
  - QUALITY OF SCHOOL CLIMATE AND SAFETY COMBINED INDICATOR RATE
  - Can serve as proxy for climate/safety indexes for 2020-2023

### Data Mapping
The pipeline now handles 4 distinct file types:
1. **Index Scores**: Direct climate_index and safety_index values
2. **Survey Results**: Question-level data aggregated by question type  
3. **Precautionary Measures**: Policy compliance rate calculations
4. **Accountability Profiles**: Combined indicator rates used as both climate and safety scores

## Implementation Changes

### 1. Updated Config (kde_sources.yaml)
Added new files to safe_schools_climate mapping:
```yaml
safe_schools_climate:
  - "KYRC24_ACCT_Index_Scores.csv"
  - "KYRC24_ACCT_Survey_Results.csv"
  - "KYRC24_SAFE_Precautionary_Measures.csv"
  - "accountability_profile_2023.csv"
  - "accountability_profile_2022.csv"
  - "accountability_profile_2021.csv"
  - "accountability_profile_2020.csv"
  # ... existing files ...
```

### 2. Pipeline Updates (safe_schools_climate.py)
- **Enhanced column mappings** to handle all file variations
- **Added file type identification** logic based on filename patterns
- **Unified file processing** through single process_file() method
- **Improved extract_metrics()** to handle different data types:
  - Direct index scores from KYRC24_ACCT_Index_Scores
  - Question-based scores from survey results 
  - Combined rates from accountability profiles
  - Policy compliance calculations from precautionary measures
- **Fixed duplicate record issue** by ensuring metrics are only generated when relevant data exists
- **Updated get_suppressed_metric_defaults()** to only return appropriate metrics based on file type

### 3. Test Updates
- Fixed failing unit tests for updated get_suppressed_metric_defaults behavior
- Updated e2e tests to handle multiple file types in test data
- All 14 unit tests and 8 e2e tests now passing

## Data Quality Improvements
1. **Comprehensive Coverage**: Now processing 2020-2024 data (vs. 2021-2024 previously)
2. **Multiple Data Sources**: Combining survey results, index scores, and policy compliance
3. **Reduced Duplicates**: Fixed issue where suppressed records generated spurious NA metrics
4. **Better File Handling**: Glob patterns now capture all relevant quality survey files

## Metrics Generated
The pipeline now generates three key metrics:
- `climate_index_score`: School climate perception (0-100)
- `safety_index_score`: School safety perception (0-100) 
- `safety_policy_compliance_rate`: Percentage of implemented safety policies

## Files Downloaded
Successfully downloaded and integrated:
- KYRC24_ACCT_Index_Scores.csv (7.0 MB)
- KYRC24_ACCT_Survey_Results.csv (299.5 MB)
- accountability_profile_2020-2023.csv files (1.6 MB total)
- quality_of_school_climate_and_safety_survey_high_school_2021.csv (106.6 MB)

## Testing Summary
- Unit Tests: 14/14 passing
- E2E Tests: 8/8 passing  
- Pipeline executed successfully generating all expected KPIs
- Output verified with proper metric separation by file type

## Next Steps
1. Consider adding survey response aggregation logic for more granular analysis
2. Implement trend analysis across the 5-year span (2020-2024)
3. Add data quality checks for survey response rates
4. Document the different data sources in user-facing documentation