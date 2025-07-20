### 1. KPI Output Format
**ALL ETL modules MUST produce long format KPI data with exactly these 10 columns:**

```
district,school_id,school_name,year,student_group,metric,value,suppressed,source_file,last_updated
```

| Column | AI Validation Rules |
|--------|-------------------|
| `district` | String, Kentucky district name (e.g., "Fayette County") |
| `school_id` | String, unique identifier (NCES/state ID) |
| `school_name` | String, human-readable school name |
| `year` | Integer, 4-digit academic year (e.g., 2024) |
| `student_group` | String, standardized via DemographicMapper |
| `metric` | String, follows naming convention: `{indicator}_{type}_{period}` |
| `value` | Float or NaN (when suppressed=Y) |
| `suppressed` | String, exactly "Y" or "N" |
| `source_file` | String, original filename for audit trail |
| `last_updated` | String, ISO timestamp |

### 2. Metric Naming Convention - CRITICAL
**AI must follow exact naming patterns:**
- **Rates**: `{indicator}_rate_{period}` (e.g., `graduation_rate_4_year`)
- **Counts**: `{indicator}_count_{period}` (e.g., `graduation_count_4_year`) 
- **Totals**: `{indicator}_total_{period}` (e.g., `graduation_total_4_year`)

**Validation**: Rate = (Count ÷ Total) × 100

### 3. Suppression Handling - CRITICAL
**Suppressed records MUST be included, not filtered out:**
- When `suppressed = "Y"`: Set `value = pd.NA` (not filtered out)
- When `suppressed = "N"`: Include actual numeric value
- **Never filter suppressed records** - transparency over data reduction

### 4. Demographic Mapping - REQUIRED
**Every ETL module MUST use DemographicMapper:**

```python
from etl.demographic_mapper import DemographicMapper

# Required in convert_to_kpi_format():
demographic_mapper = DemographicMapper()
student_group = demographic_mapper.map_demographic(
    original_demographic, year, source_file
)
```

**AI Validation**:
- Check demograpihc audit logs are generated
- Verify standardized demographics in output
- Validate year-specific mapping rules applied


### Testing Protocol
**AI MUST test during development, not after:**

1. **Syntax Test**: `python3 etl/module_name.py` after each code change
2. **Type Compatibility**: Use `typing.Dict` not `dict[]` (Python 3.8+ compatibility)
3. **Unit Tests**: `python3 -m pytest tests/test_module_name.py -v` after test creation
4. **Integration Test**: Run full ETL pipeline to validate end-to-end
5. **Data Validation**: Check KPI format, column count, metric naming

### Error Handling - REQUIRED
**When errors occur:**
1. Fix syntax/import errors immediately (don't defer)
2. Update type annotations for compatibility
3. Re-test until clean execution
4. Document compatibility requirements in notes

### Code Quality Standards
- Use `logging` for system messages, `print()` for user feedback only
- Include comprehensive try/except blocks with specific error handling
- Validate data ranges (rates: 0-100%, counts: non-negative)
- Add data source tracking for audit trails
- Use `python3` command explicitly (system `python` may be Python 2.7)

### Documentation Standards
**AI must maintain:**
- **Journal entries**: Numbered sequence for investigations
- **Code comments**: Explain complex transformation logic
- **Test documentation**: Clear test case descriptions
- **README updates**: Keep user documentation current

### Memory Management
- Process data in chunks for large files
- Clean up intermediate dataframes
- Use appropriate data types (int8 vs int64)
- Monitor memory usage during development

## 📋 AI Task Patterns

### 1. New Data Source Analysis
**When analyzing new CSV files:**
1. Place file in `data/raw/source_name/`
2. Run data profiling: columns, data types, value ranges, missing data
3. Identify suppression markers and demographic variations
4. Create transformation logic following existing patterns
5. Generate corresponding test files

### 2. ETL Module Creation
**Template-based development:**
- Start with `etl/template.py` structure (if exists) or existing module
- Implement required functions: `normalize_column_names()`, `convert_to_kpi_format()`
- Add data source detection logic in `add_derived_fields()`
- Include demographic mapping integration
- Create comprehensive test coverage

### 3. Data Quality Investigation
**When investigating data issues:**
- Create numbered journal entries: `notes/#--descriptive-title.md`
- Document problem, analysis, and resolution
- Include before/after validation results
- Update relevant code and tests
- Regenerate affected outputs
r bottlenecks on large datasets
