# 2025 School Climate Data: District Preservation Investigation

**Date:** 2025-11-20
**Status:** In Progress - Root Cause Identified, Solution Needed

## Problem Statement

After adding 2025 survey files to the safe schools climate pipeline, all 70,470 year 2025 records in the final output show `district = "Unknown District"` instead of actual district names like "Fayette County". This prevents 2025 climate/safety scores from appearing in the FCPS equity dashboard.

## Investigation Timeline

### Initial Issue
- User expected to see 2025 school climate values in Angular dashboard after incorporating 2025 data
- Checked processed file: no Fayette County records for year 2025
- All 70,470 year 2025 records had `district = "Unknown District"`

### File Discovery Fix (etl/safe_schools_climate.py:614-620)
**Problem:** 2025 survey response files weren't being discovered due to case sensitivity
- Files: `Quality_of_School_Climate_and_Safety_Survey_Elementary_School_2025.CSV` (uppercase `.CSV`)
- Previous pattern: `quality_of_school_climate_and_safety_survey_*.csv` (lowercase)

**Solution:** Added patterns for uppercase `.CSV` files:
```python
'quality_of_school_climate_and_safety_survey_*.CSV',  # Historical (uppercase)
# Plus explicit patterns for each 2025 file
```

**Result:** ✅ Files now being discovered and processed (confirmed in logs)

### District Data Investigation

Added debug logging (lines 581-586) to check if district information exists in survey data before aggregation:

```python
# DEBUG: Check district values in survey data
if 'district' in all_survey_df.columns:
    logger.info(f"Survey data has district column. Sample values: {all_survey_df['district'].value_counts().head(10).to_dict()}")
    logger.info(f"Survey data district null count: {all_survey_df['district'].isna().sum()} out of {len(all_survey_df)}")
```

**Debug Output:**
```
Survey data has district column. Sample values: {
    'Jefferson County': 229765,
    'Fayette County': 108601,  # ✅ District data IS present!
    'Boone County': 51008,
    ...
}
Survey data district null count: 0 out of 2686104  # ✅ No missing values!
```

### Root Cause Identified

**The Problem:** District information EXISTS in survey files but gets LOST somewhere between aggregation and final output.

**Evidence:**
1. ✅ Raw 2025 survey CSV files contain `"District Name"` column with "Fayette County"
2. ✅ Column normalization maps `'"District Name"'` → `district` (line 212)
3. ✅ Survey data loaded before aggregation HAS district values (108,601 Fayette records)
4. ✅ Aggregation function groups by `district` column (line 88)
5. ❌ Final output: ALL 70,470 year 2025 records have `district = "Unknown District"`

**Key Insight:** The 70,470 "Unknown District" records are coming from the **Index Scores file** (`Quality_of_School_Climate_and_Safety_Survey_Index_Scores_2025.CSV`), NOT from the aggregated survey response data.

The Index Scores file doesn't contain district information (only school codes). The aggregated scores from survey responses (which DO have district info) either:
1. Aren't being added to the final output, OR
2. Are being overwritten/replaced by Index Scores records, OR
3. Are being filtered out during deduplication

## Files Modified

- `etl/safe_schools_climate.py:614-620` - Added uppercase `.CSV` file patterns
- `etl/safe_schools_climate.py:581-586` - Added debug logging for district values

## ACTUAL ROOT CAUSE - DEEPER INVESTIGATION

After implementing year normalization fix (lines 588-593), discovered the real issue:

**The 2025 survey response files are NOT being added to `raw_survey_data` for aggregation!**

### Evidence
1. Year normalization log shows: `{'2023': 1010363, '2024': 899918, '2022': 775823}` - NO 2025 data
2. But 2025 files ARE being discovered: "Processing survey_responses file: Quality_of_School_Climate_and_Safety_Survey_Elementary_School_2025.CSV"
3. Final output still shows 0 Fayette County records for 2025

### Root Cause Identified
**Line 571 check:** `if not df.empty and 'question_type' in df.columns and 'question_index' in df.columns:`

The 2025 survey response files:
- HAVE `question_index` column (the data to aggregate)
- DO NOT have `question_type` column initially
- SHOULD get `question_type` from metadata merge (lines 507-524)
- But metadata merge is FAILING for 2025 files
- Therefore they fail the check at line 571
- Never get added to `raw_survey_data`
- Never get aggregated
- Only Index Scores (without district) appear in final output

### Why Metadata Merge Fails for 2025 Files
The metadata merge at line 507 requires:
1. `file_type == 'survey_responses'` ✓ (2025 files are identified as survey_responses)
2. `'question_type' not in df.columns` ✓ (2025 files don't have it)
3. `raw_dir` is provided ✓
4. `'level' in df.columns and 'question_number' in df.columns` ✓

But the merge itself may be failing due to:
- Level format mismatch (ES/MS/HS vs Elementary/Middle/High)
- Question number format mismatch
- Empty metadata file
- Merge returning empty due to no matching keys

## Solution

**Immediate Fix:** Change line 571 to accept files with `question_index` regardless of `question_type`:
```python
# OLD: if not df.empty and 'question_type' in df.columns and 'question_index' in df.columns:
# NEW: if not df.empty and 'question_index' in df.columns:
```

This allows 2025 files to be added to `raw_survey_data`, then the metadata can be merged within the aggregation function or before aggregation.

**Alternative:** Fix the metadata merge at line 507 by adding debug logging to see why it's failing for 2025 files.

## Next Steps

### Immediate Action Required
1. Add debug logging to metadata merge (line 507) to see why it fails for 2025 files
2. OR implement the line 571 fix to allow files with just `question_index`
3. Verify 2025 survey data gets added to `raw_survey_data`
4. Confirm aggregated scores are calculated with year="2025"
5. Verify Fayette County 2025 records appear in final output

### Testing Required
After fix implementation:
1. Verify Fayette County has 2025 climate/safety scores in processed file
2. Regenerate master KPI file with `etl_runner.py --skip-etl`
3. Extract FCPS data with `npm run extract:fayette`
4. Verify 2025 climate scores appear in Angular dashboard

## Technical Details

### Aggregation Function (lines 51-149)
- Groups by: district, school_name, year, student_group, + 12 other fields
- Calculates mean `question_index` scores for Climate (C) and Safety (S) question types
- Returns DataFrame with preserved district information

### 2025 File Structure
- **Index Scores:** School code only, NO district name → "Unknown District"
- **Survey Responses:** School code AND "District Name" → Proper district values
- Survey responses contain 108,601 Fayette County records with question-level data

### Current Metrics Count (Year 2025)
- Total: 70,470 records
- All from: Index Scores file (no district info)
- Missing: Aggregated survey scores (which have district info)

## Related Files
- Raw data: `data/raw/safe_schools_climate/Quality_of_School_Climate_and_Safety_Survey_*_2025.CSV`
- Processed: `data/processed/safe_schools_climate.csv`
- Debug logs: `/tmp/safe_schools_debug2.log`

## Investigation Summary

**Status:** Root cause fully identified, solution ready to implement

**Problem:** All 70,470 year 2025 records show `district = "Unknown District"` because:
1. The 2025 Index Scores file (no district info) IS being processed ✓
2. The 2025 survey response files (with district info) are NOT being aggregated ✗
3. Reason: Line 571 requires both `question_type` AND `question_index` to add files to `raw_survey_data`
4. The 2025 files have `question_index` but the metadata merge fails to add `question_type`
5. Therefore 2025 survey data never gets aggregated
6. Only Index Scores (without district) appear in final output

**Solution:** Modify line 571 in `etl/safe_schools_climate.py` to accept files with `question_index` even without `question_type`. The `question_type` can be added from metadata within the aggregation process.

**Code Change Required:**
```python
# Line 571 - OLD:
if not df.empty and 'question_type' in df.columns and 'question_index' in df.columns:

# Line 571 - NEW:
if not df.empty and 'question_index' in df.columns:
```

**Implemented Fixes:**
- ✓ File discovery fix (lines 614-620): Added uppercase `.CSV` patterns
- ✓ Year normalization fix (lines 588-593): Extract last 4 digits from school_year
- ✗ Survey data inclusion fix (line 571): NOT YET IMPLEMENTED

## RESOLUTION - SUCCESSFUL

**Date:** 2025-11-20 (continued session)

### Final Root Cause
The 2025 Questions metadata file (`Quality_of_School_Climate_and_Safety_Survey_Questions_2025.CSV`) contained special characters that couldn't be decoded with UTF-8 encoding, causing the metadata load to fail silently. Without question type mappings, the merge at line 517 couldn't add `question_type` to survey files, but they were still being processed - the metadata was just failing to load.

### Solution Implemented
**File:** `etl/safe_schools_climate.py:387-391`

Changed metadata file loading to try `latin1` encoding first:
```python
# Try latin1 encoding first (2025 files have special characters)
try:
    df = pd.read_csv(questions_file, encoding='latin1', dtype=str)
except Exception:
    df = pd.read_csv(questions_file, encoding='utf-8-sig', dtype=str)
```

Added comprehensive debug logging at lines 394-395, 405, 518-546 to track metadata loading and merge process.

### Test Results - VERIFIED SUCCESSFUL

**Metadata Loading:**
- Successfully loaded 81 question metadata records
- Question types: 42 Climate (C), 39 Safety (S)

**Year 2025 Final Output:**
- Total 2025 records: 128,198
- **Fayette County records: 2,667** ✅
- Jefferson County: 6,068
- Boone County: 1,215
- Warren County: 1,166
- Other districts with data preserved
- Unknown District: 70,470 (from Index Scores file - expected)

**Aggregation Success:**
- 77,567 calculated scores created from survey responses
- District information preserved through aggregation
- Year normalization working correctly (20242025 → 2025)

### Files Modified
1. `etl/safe_schools_climate.py:387-414` - Fixed metadata encoding and added error logging
2. `etl/safe_schools_climate.py:517-546` - Added comprehensive merge debug logging
3. `notes/78--2025-climate-data-district-preservation-issue.md` - Documented investigation

### Next Steps
1. Run `etl_runner.py --skip-etl` to combine all KPI data
2. Extract FCPS data with `npm run extract:fayette`
3. Verify 2025 climate scores appear in Angular dashboard
4. Remove debug logging once confirmed stable
