# 19 - English Learner Metric Correction

**Date**: 2025-07-19  
**Issue**: Invented progress rate calculations not reflecting source data  
**Status**: Resolved

## Problem Analysis

The English learner progress ETL module was creating artificial "progress rate" and "proficiency rate" calculations that do not appear in the Kentucky Department of Education source data. The user correctly identified that these were invented interpretations rather than direct data representations.

### Source Data Structure
The raw data contains 4 proficiency score bands:
- `PERCENTAGE OF VALUE TABLE SCORE OF 0` (Beginning level)
- `PERCENTAGE OF VALUE TABLE SCORE OF 60 AND 80` (Intermediate level) 
- `PERCENTAGE OF VALUE TABLE SCORE OF 100` (Advanced level)
- `PERCENTAGE OF VALUE TABLE SCORE OF 140` (Mastery level)

### Problematic Calculations
The code was creating these artificial metrics:
- `english_learner_progress_rate` = score_60_80 + score_100 + score_140 (arbitrary "scores 60+" definition)
- `english_learner_proficiency_rate` = score_100 + score_140 (arbitrary "proficient" definition)
- `english_learner_non_progress_rate` = score_0 (redundant with beginning_rate)

## Solution Implemented

### 1. Updated ETL Module Logic
**File**: `etl/english_learner_progress.py`

- Replaced `calculate_progress_metrics()` function to extract direct scores instead of calculating artificial rates
- Updated metric names to reflect actual data structure:
  - `english_learner_score_0` (was `english_learner_beginning_rate`)
  - `english_learner_score_60_80` (was `english_learner_intermediate_rate`)
  - `english_learner_score_100` (was `english_learner_advanced_rate`)
  - `english_learner_score_140` (was `english_learner_mastery_rate`)

- Removed artificial aggregation metrics:
  - Removed `english_learner_progress_rate`
  - Removed `english_learner_proficiency_rate`
  - Removed `english_learner_non_progress_rate`

### 2. Updated Test Suite
**Files**: 
- `tests/test_english_learner_progress.py`
- `tests/test_english_learner_progress_end_to_end.py`

- Updated test expectations to match new direct score metrics
- Modified assertions to check for `score_X` patterns instead of `rate` patterns
- Maintained all validation logic for data quality and formatting

### 3. Updated Dashboard Configuration
**File**: `html/data/dashboard_config.json`

- Updated `rate_metrics` array to use new metric naming convention
- Removed old JSON data files with outdated metric names
- Simplified from 21 metrics to 12 metrics (4 scores × 3 education levels)

## Data Quality Validation

### Before (Artificial Metrics)
```
english_learner_proficiency_rate = 36  # 23 + 13 (arbitrary sum)
english_learner_progress_rate = 71     # 35 + 23 + 13 (arbitrary sum)
english_learner_non_progress_rate = 29 # Same as beginning_rate (redundant)
```

### After (Direct Data)
```
english_learner_score_0 = 29      # Direct from source
english_learner_score_60_80 = 35  # Direct from source  
english_learner_score_100 = 23    # Direct from source
english_learner_score_140 = 13    # Direct from source
```

## Impact Assessment

### Positive Changes
1. **Data Accuracy**: Metrics now directly reflect Kentucky Department of Education data
2. **Transparency**: No artificial interpretation or aggregation of source data
3. **Maintainability**: Simpler logic reduces chance of calculation errors
4. **Auditability**: Clear traceability from source data to output metrics

### Considerations
1. **Dashboard Updates**: Existing visualizations may need adjustment for new metric names
2. **Documentation**: Any external documentation referencing old metrics needs updating
3. **User Training**: Users may need explanation of the new direct score approach

## Testing Results

All tests pass with updated metric expectations:
- ✅ Unit tests for direct score extraction
- ✅ Integration tests for KPI format compliance  
- ✅ End-to-end tests for multi-file processing
- ✅ Data validation tests for edge cases

## Files Modified

1. `etl/english_learner_progress.py` - Core ETL logic
2. `tests/test_english_learner_progress.py` - Unit tests
3. `tests/test_english_learner_progress_end_to_end.py` - Integration tests
4. `html/data/dashboard_config.json` - Dashboard configuration
5. `notes/19--english-learner-metric-correction.md` - This documentation

## Recommendation

This change aligns the pipeline with the principle of accurately representing source data without artificial interpretation. Future ETL modules should follow this pattern of direct data extraction unless official Kentucky Department of Education documentation explicitly defines composite metrics.