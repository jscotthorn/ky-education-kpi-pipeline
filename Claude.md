# Claude.md - AI Assistant Usage Guide

## Purpose
This file documents AI usage patterns and policies for the Equity Scorecard ETL project.

## Claude Code Integration
This project is designed to work with Claude Code for:
- Generating new ETL modules when new data sources are discovered
- Analyzing raw data dumps to create appropriate transformations
- Debugging and refining ETL logic
- Creating tests for new modules

## 1  Final Artifacts the Pipeline Must Produce

| Path | File | Shape & Key Columns | Purpose |
|------|------|--------------------|---------|
| `data/processed/<source>.csv` | One CSV per data source. Examples: `fcps_test_scores.csv`, `enrollment.csv`, `discipline.csv`. | Row‑per‑analytic‑unit (usually **school × student_group × year**). Must contain the *standard column set* listed in §2. | A clean, normalized table ready to be joined with other processed files. |
| `kpi/kpi_master.csv` | Single consolidated CSV | **Long layout**: one KPI per row. Columns: `district, school_id, school_name, year, student_group, metric, value, source_file, last_updated`. | **Authoritative feed** for dashboards, scorecards, and ad‑hoc analysis. |

## 2  Standard Column Definitions
The resulting KPI files should have a standard format.

| Column | Meaning |
|--------|---------|
| `district` | Always “Fayette County” for now, but schema allows expansion. |
| `school_id` / `school_name` | NCES or FCPS identifier plus friendly name. |
| `year` | Four‑digit academic year in which the KPI was measured (e.g., `2024`). |
| `student_group` | Sub‑population label (*All, Hispanic, Special Ed, Low Income,…*). |
| `metric` | KPI slug (e.g., `grade_3_reading_pct_proficient`). |
| `value` | Numeric value **already scaled** (store `82.5`, not `0.825`). |
| `source_file` | Name of the processed source file for provenance. |
| `last_updated` | ISO timestamp when the ETL generated the row. |

## 2.1 KPI Format Requirements - CRITICAL

**The processed files must be in LONG format, not wide format.**

### ✅ CORRECT Long Format (One KPI per row with expanded metrics):
```csv
district,school_id,school_name,year,student_group,metric,value,source_file,last_updated
Fayette County,210090000123,Bryan Station High School,2024,All Students,graduation_rate_4_year,85.2,graduation_rates.csv,2025-07-18T10:30:00
Fayette County,210090000123,Bryan Station High School,2024,All Students,graduation_count_4_year,201,graduation_rates.csv,2025-07-18T10:30:00
Fayette County,210090000123,Bryan Station High School,2024,All Students,graduation_total_4_year,236,graduation_rates.csv,2025-07-18T10:30:00
Fayette County,210090000123,Bryan Station High School,2024,Hispanic,graduation_rate_4_year,78.9,graduation_rates.csv,2025-07-18T10:30:00
Fayette County,210090000123,Bryan Station High School,2024,Hispanic,graduation_count_4_year,41,graduation_rates.csv,2025-07-18T10:30:00
Fayette County,210090000123,Bryan Station High School,2024,Hispanic,graduation_total_4_year,52,graduation_rates.csv,2025-07-18T10:30:00
```

### ❌ INCORRECT Wide Format (Multiple KPIs per row):
```csv
school_id,school_name,year,student_group,graduation_rate_4_year,graduation_count_4_year,graduation_total_4_year
210090000123,Bryan Station High School,2024,All Students,85.2,201,236
```

**ETL modules must transform from wide source data to long KPI format.**

### KPI Metric Naming Convention:
- **Rates**: `{indicator}_rate_{period}` (e.g., `graduation_rate_4_year`) - Percentage values
- **Counts**: `{indicator}_count_{period}` (e.g., `graduation_count_4_year`) - Number passing/meeting criteria  
- **Totals**: `{indicator}_total_{period}` (e.g., `graduation_total_4_year`) - Total number eligible/assessed

**Key Points:**
- **Three KPI rows per indicator**: rate (%), count (passing), total (eligible)
- **Rate calculation**: count ÷ total × 100 = rate
- **Each metric value gets its own row** for maximum flexibility
- **Consistent naming** allows dashboards to automatically group related metrics

## Raw Data Folder Structure

Raw data should be organized as follows:
```
data/raw/
├── graduation_rates/           # Source-specific folder
│   ├── graduation_rate_2021.csv
│   ├── graduation_rate_2022.csv
│   ├── graduation_rate_2023.csv
│   └── KYRC24_ACCT_4_Year_High_School_Graduation.csv
├── fcps_test_scores/          # Another source
│   └── 20240315/              # Date-stamped folder (optional)
│       └── test_scores.csv
└── enrollment/                # Another source
    └── enrollment_2024.csv
```

**Key principles:**
- Each data source gets its own folder under `data/raw/`
- Files can be placed directly in source folder OR in date-stamped subfolders
- ETL modules automatically find the newest data in their source folder
- Original filenames are preserved for audit trails

## AI Workflows

### 1. New Data Source Onboarding
When a new CSV export is received:
1. Place raw file in `data/raw/source_name/` (or `data/raw/source_name/YYYYMMDD/`)
2. Run `python etl_runner.py --draft` to generate initial ETL logic
3. Ask Claude to analyze the raw data and create appropriate transformation logic
4. Refine the generated module based on domain knowledge

### 2. ETL Module Development
- Use `etl/template.py` as starting point for new modules
- Ask Claude to generate config mappings in `config/mappings.yaml`
- Generate corresponding test files in `tests/`

### 3. Data Analysis
- Use `notes/` directory to store AI-generated analysis of new data sources
- Include data quality assessments and transformation recommendations
- Document any anomalies or special handling requirements

## Self-Instructions for Claude Code

### CRITICAL: Always Test During Development
When creating new ETL modules, Claude MUST:
1. **Test syntax immediately** - Run `python3 etl/module_name.py` after creation
2. **Use compatible Python syntax** - Use `typing.Dict` instead of `dict[]` for Python 3.8+ compatibility
3. **Run pytest** - Execute `python3 -m pytest tests/test_module_name.py -v` after creating tests
4. **Check imports** - Ensure all required packages are available
5. **Validate data processing** - Test with sample data before marking complete
6. **Use Python 3** - Always use `python3` command, not `python` (may default to Python 2.7)

### Error Handling Protocol
If errors occur during testing:
1. Fix syntax/import errors immediately
2. Update type annotations for Python compatibility
3. Re-test until clean execution
4. Document any compatibility requirements in notes

### Code Quality Standards
- Use `logging` for informational messages, `print()` for user feedback
- Include comprehensive error handling with try/except blocks
- Validate data quality (ranges, required fields, data types)
- Add data source tracking fields for audit trails

## Best Practices
- Always validate AI-generated ETL logic with domain experts
- Test transformations on sample data before production runs
- Keep this file updated as new AI workflows are established
- Use version control for all AI-generated code
- **NEVER deploy untested code** - Always run tests during development

## Commands to Remember
```bash
# Setup (use Python 3.8+ - system may default to Python 2.7)
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -e .

# Run draft mode for new data source analysis
python3 etl_runner.py --draft

# Run full ETL pipeline
python3 etl_runner.py

# Run tests
python3 -m pytest tests/

# Test individual ETL modules
python3 etl/graduation_rates.py

# Check Python version (should be 3.8+)
python3 --version
```

## Python Version Requirements
- **Minimum**: Python 3.8+ (for type annotations and modern pandas)
- **Note**: System `python` command may default to Python 2.7 - always use `python3`
- **Dependencies**: pandas, pydantic, ruamel-yaml, pyarrow, pytest