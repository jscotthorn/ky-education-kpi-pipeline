You can use the etl_runner.py file in the project root to run all pipelines and python3 -m pytest tests/ to run all tests

### 1. KPI Output Format
**ALL ETL modules MUST produce long format KPI data with exactly these 10 columns:**

```
district,school_id,school_name,year,student_group,metric,value,suppressed,source_file,last_updated
```

### 2. Metric Naming Convention - CRITICAL
**AI must follow exact naming patterns:**
- **Rates**: `{indicator}_rate_{period}` (e.g., `graduation_rate_4_year`)
- **Counts**: `{indicator}_count_{period}` (e.g., `graduation_count_4_year`) 
- **Totals**: `{indicator}_total_{period}` (e.g., `graduation_total_4_year`)

**Validation**: Rate = (Count ÷ Total) × 100



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
- Fully define typing for all functions and variables

### Documentation Standards
**AI must maintain:**
- **Journal entries**: Numbered sequence for investigations
- **Code comments**: Explain complex transformation logic
- **Test documentation**: Clear test case descriptions
- **README updates**: Keep user documentation current

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
- Base new ETL pipelines off of BaseETL following patterns seen in other exising pipelines.
- Create comprehensive unit and e2e test coverage, following patterns from existing pipelines

### 3. Data Quality Investigation
**When investigating data issues:**
- Create numbered journal entries: `notes/#--descriptive-title.md`
- Document problem, analysis, and resolution
- Update relevant code and tests
- Regenerate affected outputs
