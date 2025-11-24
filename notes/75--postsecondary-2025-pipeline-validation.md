# Journal 75: Postsecondary Readiness 2025 Data Pipeline Validation

## Date
2025-11-19

## Purpose
Document testing results for postsecondary_readiness.py pipeline to handle 2024-2025 school year data. This pipeline required NO code changes - validating that the existing implementation handles 2025 data correctly.

## Test Results

### Command
```bash
/Users/scott/venvs/equity-etl/bin/python3 etl/postsecondary_readiness.py
```

### Results
```
Found 4 files to process for postsecondary_readiness
✓ postsecondary_readiness_2023.csv: 9,131 → 18,262 KPI rows
✓ postsecondary_readiness_2022.csv: 9,131 → 18,262 KPI rows
✓ KYRC24_ACCT_Postsecondary_Readiness.csv: 10,719 → 21,438 KPI rows
✓ Postsecondary_Readiness_2025.CSV: 10,962 → 21,924 KPI rows ✅

Total KPI rows: 79,886
```

### Success Metrics
✅ **File Discovery**: 2025 file found and processed (base_etl.py .CSV fix applies)
✅ **Data Extraction**: 21,924 KPI rows generated from 10,962 source rows (2x expansion - expected)
✅ **Year Parsing**: Correctly identified as year "2025"
✅ **Metric Values**: Successfully extracted both base and bonus rates
✅ **Suppression Flags**: Properly handled (Y/N values)
✅ **School Names**: Correctly shows "---District Total---" (base_etl.py fix applies)
✅ **No Code Changes Required**: Existing column mappings work perfectly

## Schema Comparison

### KYRC24 Schema (17 columns)
```
School Year, County Number, County Name, District Number, District Name,
School Number, School Name, School Code, State School Id, NCES ID,
CO-OP, CO-OP Code, School Type, Demographic, Suppressed,
Postsecondary Rate, Postsecondary Rate With Bonus
```

### 2025 Schema (9 columns)
```
School Year, School Code, District Name, School Name, type,
Demographic, Suppressed, Postsecondary Rate, Postsecondary Rate With Bonus
```

**Key Differences**:
1. **REMOVED**: County/District numbers, State IDs, NCES ID, CO-OP info, School Type
2. **ADDED**: "type" field (appears to be school classification, e.g., "A1")
3. **UNCHANGED**: Metric column names are IDENTICAL to KYRC24
   - "Postsecondary Rate"
   - "Postsecondary Rate With Bonus"
4. **UNCHANGED**: Suppressed column format

## Why No Code Changes Were Needed

### 1. Column Mappings Already Compatible
The existing `module_column_mappings` in postsecondary_readiness.py already handles both formats:

```python
@property
def module_column_mappings(self) -> Dict[str, str]:
    return {
        'Postsecondary Rate': 'postsecondary_rate',
        'POSTSECONDARY RATE': 'postsecondary_rate',
        'Postsecondary Rate With Bonus': 'postsecondary_rate_with_bonus',
        'POSTSECONDARY RATE WITH BONUS': 'postsecondary_rate_with_bonus',
    }
```

The 2025 file uses "Postsecondary Rate" (mixed case), which is already mapped.

### 2. BaseETL Fixes Apply Automatically
Previous fixes made to base_etl.py automatically benefit this pipeline:
- **Case-insensitive file discovery**: Handles .CSV extension
- **School name standardization**: Treats blank School Name as "---District Total---"
- **Year parsing**: Extracts "2025" from "20242025" format

### 3. Metric Extraction Logic Is Generic
The `extract_metrics()` method simply extracts both metrics regardless of file format:

```python
def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
    metrics = {}
    metrics['postsecondary_readiness_rate'] = row.get('postsecondary_rate', pd.NA)
    metrics['postsecondary_readiness_rate_with_bonus'] = row.get('postsecondary_rate_with_bonus', pd.NA)
    return metrics
```

This works identically for 2022, 2023, 2024, and 2025 files.

### 4. Custom convert_to_kpi_format() Ensures Consistency
The pipeline overrides `convert_to_kpi_format()` to ALWAYS create both metrics (base and bonus) for every demographic, even when values are missing or suppressed. This ensures data consistency across all years.

## Final Results

### Test Run Output
```
Total KPI rows: 79,886
- 2022 data: 18,262 rows
- 2023 data: 18,262 rows
- 2024 data: 21,438 rows
- 2025 data: 21,924 rows ✅
```

### Metrics Per Year
**2025 File Metrics**:
- postsecondary_readiness_rate: 10,962 rows
- postsecondary_readiness_rate_with_bonus: 10,962 rows
- **Total**: 21,924 rows (exactly 2x source rows, as expected)

### Sample Output
```csv
year,metric,district_name,school_name,demographic,value,suppressed
2025,postsecondary_readiness_rate,Adair County,---District Total---,All Students,90.2,N
2025,postsecondary_readiness_rate_with_bonus,Adair County,---District Total---,All Students,98.0,N
2025,postsecondary_readiness_rate,Adair County,---District Total---,Female,93.7,N
2025,postsecondary_readiness_rate_with_bonus,Adair County,---District Total---,Female,102.6,N
2025,postsecondary_readiness_rate,Adair County,---District Total---,Male,87.3,N
2025,postsecondary_readiness_rate_with_bonus,Adair County,---District Total---,Male,94.1,N
```

**Notable**: Bonus rates can exceed 100% (e.g., 102.6 for females), which is expected and handled by the validation logic (allows 0-150% for bonus rates).

## Success Criteria - FINAL

- [x] 2025 file discovered and processed
- [x] Both base and bonus readiness rates extracted
- [x] Correct 2x expansion (10,962 source rows → 21,924 KPI rows)
- [x] District Name preserved (Adair County, etc.)
- [x] School Name properly shows "---District Total---"
- [x] All KPI rows have valid year ("2025")
- [x] Suppression flags properly set (N/Y)
- [x] No "Unknown School" entries (0 found)
- [x] Bonus rates >100% handled correctly (102.6, etc.)
- [x] Demographic mapping working properly
- [x] Backward compatibility maintained (2022-2024 files still process)

## Code Quality Notes

### Why This Pipeline Worked Without Changes

This pipeline demonstrates excellent design principles that made 2025 compatibility automatic:

1. **Generic Column Mappings**: Included both mixed-case and uppercase variants from the start
2. **Flexible Metric Extraction**: Doesn't hardcode assumptions about file structure
3. **BaseETL Inheritance**: Leverages centralized fixes for common issues
4. **Always-Pair Metrics**: Custom logic ensures both metrics always generated together
5. **Validation Tolerance**: Bonus rate validation allows values >100%, anticipating data patterns

### Lessons for Other Pipelines

The postsecondary_readiness pipeline serves as a model for robust ETL design:
- Map multiple column name variants upfront
- Don't assume specific location identifiers will exist
- Use generic extraction logic that works across file formats
- Leverage BaseETL for common transformations
- Test with diverse data patterns (rates >100%, missing values, etc.)

## Comparison with Other Pipelines

| Pipeline | 2025 Changes Required | Why |
|----------|----------------------|-----|
| kindergarten_readiness | ✅ Column mappings | 2025 uses "Percent" prefix, "PRIOR_SETTING" |
| graduation_rates | ✅ Column mappings | 2025 uses "4-Year" (hyphens), separate suppression columns |
| postsecondary_readiness | ❌ None | 2025 kept identical metric column names |

## Next Steps

1. ✅ Document findings in this journal
2. Apply similar testing to remaining pipelines:
   - kentucky_summative_assessment.py (filter by grade)
   - english_learner_progress.py (proficiency metrics)
3. Run full ETL pipeline test
4. Update Phase 4 documentation in journal 72

## Conclusion

✅ **PIPELINE VALIDATED - NO CHANGES NEEDED**

The postsecondary_readiness.py pipeline successfully processes 2024-2025 data with:
- Zero code changes required
- Proper column mapping for 2025 schema
- Full backward compatibility with 2022-2024 files
- 21,924 KPI rows from 2025 file (10,962 source rows → 2x expansion)
- Both base and bonus readiness rates extracted per demographic
- Proper handling of rates >100% for bonus metrics

Total postsecondary readiness dataset: 79,886 KPI rows across years 2022-2025

This pipeline exemplifies robust ETL design that anticipates schema variations and handles them gracefully through flexible column mappings and generic metric extraction logic.
