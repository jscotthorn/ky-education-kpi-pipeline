# Safe Schools Climate Pipeline Review

## Current State Analysis

### Raw Data Files Overview
The safe_schools_climate pipeline currently processes files from multiple data sources:

1. **2024 Data (KYRC24 files)**:
   - `KYRC24_ACCT_Index_Scores.csv` - Direct climate and safety index scores
   - `KYRC24_ACCT_Survey_Results.csv` - Detailed survey responses (1.2M+ lines)
   - `KYRC24_SAFE_Precautionary_Measures.csv` - Safety policy compliance data

2. **Historical Data (2020-2023)**:
   - `accountability_profile_*.csv` - Contains combined climate/safety indicator rates
   - `precautionary_measures_*.csv` - Historical safety policy data
   - `quality_of_school_climate_and_safety_survey_*.csv` - Survey data by school level

### KPI Coverage by Year

| Metric | 2020 | 2021 | 2022 | 2023 | 2024 |
|--------|------|------|------|------|------|
| climate_index_score | ✓ (combined) | ✓ | ✓ | ✓ | ✓ |
| safety_index_score | ✓ (combined) | ✓ | ✓ | ✓ | ✓ |
| safety_policy_compliance_rate | ✓ | ✓ | ✓ | ✓ | ✓ |

**Note**: 2020 data uses a combined climate/safety indicator rate from accountability profiles rather than separate indices.

## Issues Identified

### 1. Excessive Data Processing
- `KYRC24_ACCT_Survey_Results.csv` contains 1.2M+ rows of individual survey responses
- This file provides question-level detail but the pipeline only extracts index scores
- Processing this large file is inefficient when index scores are available directly

### 2. Redundant Data Sources
- For 2024, we have both:
  - Direct index scores in `KYRC24_ACCT_Index_Scores.csv`
  - Raw survey results that need aggregation in `KYRC24_ACCT_Survey_Results.csv`
- The pipeline attempts to process both, leading to potential duplicates

### 3. Mixed Data Types
- The pipeline handles 4 different file types with varying structures:
  - Direct index scores
  - Survey responses requiring aggregation
  - Precautionary measures (policy compliance)
  - Accountability profiles (historical combined metrics)

## Recommendations

### 1. Streamline File Selection
**Exclude from current pipeline**:
- `KYRC24_ACCT_Survey_Results.csv` - Use direct index scores instead
- Individual survey response files (`quality_of_school_climate_and_safety_survey_*.csv`) when index scores are available

**Keep in current pipeline**:
- `KYRC24_ACCT_Index_Scores.csv` - Primary source for 2024 climate/safety indices
- `KYRC24_SAFE_Precautionary_Measures.csv` - Safety policy compliance
- `accountability_profile_*.csv` - Historical data source for indices
- `precautionary_measures_*.csv` - Historical policy compliance

### 2. Create Separate Survey Response Pipeline
Consider creating a `safe_schools_survey_details` pipeline for:
- Detailed survey response analysis
- Question-level metrics
- Response distribution analysis

This would handle:
- `KYRC24_ACCT_Survey_Results.csv`
- `quality_of_school_climate_and_safety_survey_*.csv` files

### 3. Optimize Processing Logic
- Use `KYRC24_ACCT_Index_Scores.csv` as the primary source for 2024 indices
- Only fall back to survey aggregation if direct scores are unavailable
- Add validation to ensure we're not double-counting metrics from multiple sources

### 4. Updated File Patterns
Modify `get_files_to_process()` to use:
```python
patterns = [
    'KYRC24_ACCT_Index_Scores.csv',  # Primary 2024 index source
    'KYRC24_SAFE_Precautionary_Measures.csv',  # 2024 policy compliance
    'accountability_profile_*.csv',  # Historical indices (2020-2023)
    'precautionary_measures_*.csv'  # Historical policy compliance
]
```

### 5. Data Quality Improvements
- Add logging to track which data source provided each metric
- Implement priority ordering when multiple sources exist
- Add validation for index score ranges (0-100)

## Implementation Priority
1. **High**: Remove survey results file from processing (immediate performance gain)
2. **Medium**: Refactor to prioritize direct index scores over calculated values
3. **Low**: Create separate survey details pipeline for deeper analysis

## Expected Benefits
- Reduced processing time by ~90% (avoiding 1.2M row file)
- Clearer data lineage and source tracking
- Elimination of potential duplicate metrics
- More maintainable and understandable pipeline structure