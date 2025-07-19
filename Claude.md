# Claude.md - AI Assistant Usage Guide

## Purpose
This file documents AI usage patterns, instructions, and technical requirements specifically for AI assistants working on the Equity Scorecard ETL project. **This is not user documentation** - see README.md for user instructions.

## Claude Code Integration
This project is designed to work with Claude Code for:
- Generating new ETL modules when new data sources are discovered
- Analyzing raw data dumps to create appropriate transformations
- Debugging and refining ETL logic
- Creating tests for new modules
- Maintaining data quality and documentation

## 🎯 Key Technical Requirements for AI

### 1. KPI Output Format - CRITICAL
**ALL ETL modules MUST produce long format KPI data with exactly these 10 columns:**

```
district,school_id,school_name,year,student_group,metric,value,suppressed,source_file,last_updated
```

| Column | AI Validation Rules |
|--------|-------------------|
| `district` | String, typically "Fayette County" |
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
- Check audit logs are generated
- Verify standardized demographics in output
- Validate year-specific mapping rules applied

## 🔧 AI Development Workflow

### Testing Protocol - MANDATORY
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

## 🚨 Critical AI Instructions

### Data Format Validation
**AI must validate these requirements:**
- **Long format only**: One KPI per row, not wide format
- **Exact column count**: 10 columns, no more, no less
- **Naming consistency**: Follow metric naming convention exactly
- **Suppression inclusion**: Suppressed records included with NaN values
- **Demographic standardization**: All student groups mapped via DemographicMapper

### Code Compatibility
**Python environment constraints:**
- **Minimum Python 3.8+**: Use compatible type annotations
- **Import handling**: Use try/except for relative imports
- **Dependency management**: Validate all required packages available
- **Cross-platform**: Code works on macOS, Linux, Windows

### Testing Requirements
**AI must ensure:**
- **Syntax validation**: Code runs without errors
- **Type checking**: Compatible with Python 3.8+
- **Unit tests**: All functions tested with sample data
- **Integration tests**: Full ETL pipeline validation
- **Data quality tests**: KPI format compliance

### Documentation Standards
**AI must maintain:**
- **Journal entries**: Numbered sequence for investigations
- **Code comments**: Explain complex transformation logic
- **Test documentation**: Clear test case descriptions
- **README updates**: Keep user documentation current

## 🔄 AI Workflow Examples

### Example 1: Adding New Data Source
```python
# 1. Analyze raw data structure
df = pd.read_csv('data/raw/new_source/file.csv')
# Profile: columns, types, ranges, patterns

# 2. Create transformation module
def transform(raw_dir, proc_dir, config):
    # Follow existing pattern
    # Include demographic mapping
    # Generate KPI format output

# 3. Create tests
def test_transform_new_source():
    # Test with sample data
    # Validate KPI format
    # Check demographic mapping

# 4. Validate end-to-end
python3 etl_runner.py  # Test full pipeline
```

### Example 2: Data Quality Investigation
```markdown
# notes/12--new-data-issue.md

## Problem
Data source showing unexpected values

## Analysis
- Value range analysis
- Suppression pattern review
- Demographic coverage check

## Resolution
- Code changes made
- Validation results
- Testing confirmation
```

## 🎛️ AI Configuration Management

### Environment Variables
**AI should respect:**
- Virtual environment activation required
- Use `python3` not `python`
- Dependencies in `requirements.txt` or `pyproject.toml`

### File Organization
**AI must maintain:**
- Raw data in `data/raw/source_name/`
- Processed output in `data/processed/`
- Tests in `tests/` with matching module names
- Documentation in `notes/` with numbered sequence

### Version Control
**AI practices:**
- Commit logical units of work
- Include descriptive commit messages
- Test before committing
- Update documentation with code changes

## 🚀 Performance Optimization

### Efficiency Guidelines
- Use pandas vectorized operations over loops
- Implement data type optimization
- Cache expensive operations when appropriate
- Profile code for bottlenecks on large datasets

### Memory Management
- Process data in chunks for large files
- Clean up intermediate dataframes
- Use appropriate data types (int8 vs int64)
- Monitor memory usage during development

**This file serves as the definitive technical reference for AI assistants working on this project. All AI-generated code must comply with these requirements.**