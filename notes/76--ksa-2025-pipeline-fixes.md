# Journal 76: Kentucky Summative Assessment 2025 Data Pipeline Fixes

## Date
2025-11-19

## Purpose
Document testing results and fixes for kentucky_summative_assessment.py pipeline to handle 2024-2025 school year data with Assessment_Performance_by_Grade_2025.CSV file.

## Schema Analysis

### KYRC24 ASMT Schema (22 columns)
```
School Year, County Number, County Name, District Number, District Name,
School Number, School Name, School Code, State School Id, NCES ID,
Co-Op, Co-Op Code, School Type, Grade, Subject, Demographic, Suppressed,
Novice, Apprentice, Proficient, Distinguished, Proficient / Distinguished
```

### 2025 Schema (13 columns)
```
School Code, District Name, School Name, School Classification, Grade,
Subject, Demographic, Suppressed, Novice, Apprentice, Proficient,
Distinguished, Proficient/Distinguished
```

**Key Differences**:
1. **REMOVED**: School Year, County/District numbers, State IDs, NCES ID, CO-OP info, School Type
2. **ADDED**: School Classification field
3. **CHANGED**: "Proficient / Distinguished" → "Proficient/Distinguished" (NO SPACES)
4. **KEPT**: Grade, Subject columns (critical for grade-level metrics)

## Issue Found

### Column Mapping Mismatch
**Problem**: 2025 file uses "Proficient/Distinguished" without spaces, but existing mappings only handled "Proficient / Distinguished" (with spaces).

**Evidence**:
```csv
# KYRC24 format:
"Proficient / Distinguished"

# 2025 format:
"Proficient/Distinguished"
```

**Impact**: Without the mapping, proficient_distinguished metric would not be extracted from 2025 data.

## Fix Applied

### Code Changes

#### kentucky_summative_assessment.py - Column Mappings (line 79)
```python
@property
def module_column_mappings(self) -> Dict[str, str]:
    return {
        'Level': 'level',
        'LEVEL': 'level',
        'Subject': 'subject',
        'SUBJECT': 'subject',
        'Novice': 'novice',
        'NOVICE': 'novice',
        'Apprentice': 'apprentice',
        'APPRENTICE': 'apprentice',
        'Proficient': 'proficient',
        'PROFICIENT': 'proficient',
        'Distinguished': 'distinguished',
        'DISTINGUISHED': 'distinguished',
        'Proficient / Distinguished': 'proficient_distinguished',  # KYRC24 format
        'Proficient/Distinguished': 'proficient_distinguished',    # 2025 format (no spaces)
        'PROFICIENT/DISTINGUISHED': 'proficient_distinguished',
        'Content Index': 'content_index',
        'CONTENT INDEX': 'content_index',
    }
```

## File Details

### 2025 File Characteristics
- **Filename**: Assessment_Performance_by_Grade_2025.CSV
- **Size**: 592,628 rows (very large dataset)
- **Structure**: Grade-level data with Subject breakdown
- **Grades**: 03-08, 10, 11 (elementary, middle, and high school)
- **Subjects**: Reading, Math, Science, Social Studies, Writing
- **School Classification**: Uses codes like "A1", "A2", etc.

### Sample Data
```csv
School Code,District Name,School Name,School Classification,Grade,Subject,Demographic,Suppressed,Novice,Apprentice,Proficient,Distinguished,Proficient/Distinguished
001,Adair County,,A1,03,Reading,All Students,N,22,28,35,15,50
001,Adair County,,A1,03,Reading,Female,N,25,25,32,17,49
```

## Pipeline Design Notes

### Grade-Level Metrics
The KSA pipeline generates metrics at multiple granularities:

1. **Grade-specific** (primary):
   - `kentucky_summative_assessment_reading_proficient_distinguished_rate_grade_3`
   - `kentucky_summative_assessment_math_novice_rate_grade_8`

2. **Level-based** (derived from grade):
   - `kentucky_summative_assessment_reading_proficient_distinguished_rate_elementary`
   - `kentucky_summative_assessment_math_proficient_distinguished_rate_middle`

### Chunked Processing
Due to large file sizes, the pipeline uses chunked processing:
- Processes 50,000 rows at a time
- Logs progress for each chunk
- Prevents memory issues with large datasets

### Expected Metrics
For each grade + subject + demographic combination:
- `novice_rate`: Percentage scoring at Novice level
- `apprentice_rate`: Percentage scoring at Apprentice level
- `proficient_rate`: Percentage scoring at Proficient level
- `distinguished_rate`: Percentage scoring at Distinguished level
- `proficient_distinguished_rate`: Combined Proficient + Distinguished (KEY METRIC for equity scorecard)
- `content_index_score`: Optional scale score (if available)

## Test Results

### Command
```bash
/Users/scott/venvs/equity-etl/bin/python3 etl/kentucky_summative_assessment.py
```

### Status
**In Progress** - Pipeline is processing in background due to large file size (592K rows).

Expected processing time: 5-10 minutes due to:
- Large source file (592,628 rows)
- Chunked processing (50K rows per chunk = ~12 chunks)
- Multiple metrics per row (6 metrics × 2 granularities = 12 KPI rows per source row)
- Expected output: ~7+ million KPI rows

### BaseETL Fixes Apply Automatically
Previous fixes made to base_etl.py automatically benefit this pipeline:
- ✅ **Case-insensitive file discovery**: Handles .CSV extension
- ✅ **School name standardization**: Treats blank School Name as "---District Total---"
- ✅ **Year parsing**: Will extract "2025" from filename

## Expected Success Criteria

Once processing completes:
- [ ] 2025 file discovered and processed
- [ ] All performance level rates extracted (novice, apprentice, proficient, distinguished)
- [ ] Proficient/Distinguished combined rate extracted (KEY METRIC)
- [ ] Grade-level metrics generated correctly
- [ ] District Name preserved
- [ ] School Name properly shows "---District Total---"
- [ ] All KPI rows have valid year ("2025")
- [ ] Suppression flags properly set (N/Y)
- [ ] No "Unknown School" entries
- [ ] Subject normalization working (Reading → reading, etc.)
- [ ] Grade normalization working (03 → grade_3, etc.)

## Known Considerations

### File Selection Decision
Per journal 72, we chose **Assessment_Performance_by_Grade_2025.CSV** over Accountable_Assessment_Performance_2025.CSV because:
- Grade-level data provides more granular metrics for equity analysis
- Matches historical data structure (by grade)
- Allows tracking specific grade-level performance gaps

### Large Dataset Handling
The KSA pipeline is the largest in the equity scorecard system:
- Processes 500K+ source rows
- Generates millions of KPI rows (multiple metrics per row)
- Uses memory-efficient chunked processing
- Takes significantly longer than other pipelines

### Backward Compatibility
Pipeline handles multiple historical formats:
- 2021: asmt_performance_by_grade_2021.csv
- 2022-2023: accountable_assessment_performance_YYYY.csv
- 2024: KYRC24_ASMT_Kentucky_Summative_Assessment.csv
- 2025: Assessment_Performance_by_Grade_2025.CSV

All formats supported with varying column names (uppercase, mixed case, different spacing).

## Next Steps

1. ✅ Identify column mapping issue
2. ✅ Apply fix to kentucky_summative_assessment.py
3. ⏳ **In Progress**: Test updated pipeline with all files
4. [ ] Verify 2025 data quality in output
5. [ ] Update this journal with final test results
6. [ ] Proceed to english_learner_progress.py

## Conclusion

**PIPELINE FIX APPLIED - TESTING IN PROGRESS**

The kentucky_summative_assessment.py pipeline required a minor column mapping fix to handle 2025 data:
- Added mapping for "Proficient/Distinguished" (no spaces)
- All other metrics already supported through existing mappings
- BaseETL fixes from previous work apply automatically
- Large file size requires extended processing time (5-10 minutes)

Once testing completes, this pipeline will support full 2024-2025 data extraction for all grades, subjects, and demographics with proper grade-level granularity for equity scorecard analysis.
