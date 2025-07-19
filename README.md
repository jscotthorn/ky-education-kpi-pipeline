# Equity Scorecard ETL

Transform heterogeneous CSV exports from Fayette County Public Schools (FCPS) and other sources into a standardized KPI format for equity dashboards tracking achievement, discipline, enrollment, staffing, and other educational metrics.

## 🚀 Quick Start

```bash
# Setup environment
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -e .

# Run ETL pipeline
python3 etl_runner.py

# View results
open data/processed/  # Individual source files
```

## 📋 Developer Workflow

### 1. Adding New Data Sources
1. **Drop files** → Place new CSV exports in `data/raw/source_name/`
   - Files can be in source folder directly or in date-stamped subfolders
   - Original filenames are preserved for audit trails
2. **Draft run** → `python3 etl_runner.py --draft` to generate initial ETL logic
3. **Refine** → Edit generated ETL modules in `etl/` directory
4. **Test** → `python3 -m pytest tests/` to validate processing logic
5. **Production run** → `python3 etl_runner.py` to process all sources
6. **Review** → Check outputs in `data/processed/`

### 2. Dashboard Monitoring
```bash
# Generate dashboard data
python3 html/generate_dashboard_data.py

# Serve dashboard locally
python3 html/serve_dashboard.py
# Opens http://localhost:8000/equity_dashboard.html
```

## 📁 Project Structure

```
equity-etl/
├── data/
│   ├── raw/                    # Source CSV files (never edited)
│   │   ├── graduation_rates/   # One folder per data source
│   │   ├── kindergarten_readiness/
│   │   └── fcps_test_scores/
│   └── processed/              # Standardized KPI outputs
├── etl/                        # ETL transformation modules
│   ├── graduation_rates.py
│   ├── kindergarten_readiness.py
│   └── demographic_mapper.py   # Standardizes student groups
├── html/                       # Dashboard and visualization
│   ├── equity_dashboard.html
│   ├── generate_dashboard_data.py
│   └── data/                   # Generated JSON for dashboard
├── config/                     # Configuration files
│   └── demographic_mappings.yaml
├── tests/                      # Test suite
├── notes/                      # Documentation and analysis
└── kpi/                        # Future: consolidated output
```

## 🎯 Data Output Format

All ETL modules produce standardized **long format** KPI data:

| Column | Description | Example |
|--------|-------------|---------|
| `district` | District name | `"Fayette County"` |
| `school_id` | Unique school identifier | `"210090000123"` |
| `school_name` | School display name | `"Bryan Station High School"` |
| `year` | Academic year (4-digit) | `2024` |
| `student_group` | Standardized demographic | `"Hispanic or Latino"` |
| `metric` | KPI identifier | `"graduation_rate_4_year"` |
| `value` | Numeric value (NaN if suppressed) | `85.2` |
| `suppressed` | Privacy suppression flag | `"Y"` or `"N"` |
| `source_file` | Data provenance | `"graduation_rates.csv"` |
| `last_updated` | Processing timestamp | `"2025-07-19T10:30:00"` |

### KPI Metric Naming Convention
- **Rates**: `{indicator}_rate_{period}` → Percentage values (0-100)
- **Counts**: `{indicator}_count_{period}` → Number meeting criteria  
- **Totals**: `{indicator}_total_{period}` → Total number eligible

**Example**: One graduation indicator creates three KPI rows:
- `graduation_rate_4_year` = 85.2 (rate)
- `graduation_count_4_year` = 201 (students graduating)
- `graduation_total_4_year` = 236 (students in cohort)

## 🔧 Common Commands

```bash
# Environment setup
python3 --version                    # Should be 3.8+
source .venv/bin/activate           # Activate virtual environment

# ETL operations
python3 etl_runner.py               # Full pipeline
python3 etl_runner.py --draft       # Analysis mode only
python3 etl/graduation_rates.py     # Test single module

# Testing and validation
python3 -m pytest tests/            # Full test suite
python3 -m pytest tests/test_graduation_rates.py -v  # Specific module
python3 -m pytest --tb=short        # Concise error output

# Dashboard generation
python3 html/generate_dashboard_data.py              # Default: Fayette County only
python3 html/generate_dashboard_data.py --all-districts  # Include all districts
python3 html/serve_dashboard.py                      # Start web server
```

## 📊 Available Data Sources

### Working Pipelines
- **Graduation Rates** (`graduation_rates.csv`)
  - 4-year and 5-year graduation rates, counts, and totals
  - Years: 2021-2024
  - Demographic breakdowns with suppression handling

- **Kindergarten Readiness** (`kindergarten_readiness.csv`)
  - Readiness rates (2024: rate only, 2021: includes counts/totals)
  - Years: 2021-2024
  - Prior setting and demographic analysis

### Dashboard Features
- **Interactive heatmaps** showing schools vs demographics
- **Performance-based ranking** (highest performing schools at top)
- **Suppression transparency** (gaps for privacy-protected data)
- **Real-time statistics** and data coverage metrics

## 🛡️ Data Quality & Privacy

### Suppression Handling
- Records with `SUPPRESSED='Y'` are included with `value=NaN`
- Maintains data structure while protecting privacy (typically <10 students)
- Dashboard visualizations show gaps for suppressed data

### Demographic Standardization
All ETL pipelines use `DemographicMapper` to ensure consistent student group names:
- Handles year-to-year variations in naming
- Maps inconsistent labels to standard format
- Generates audit trails for transparency

### Validation Requirements
- Data type validation for all numeric fields
- Range validation (rates: 0-100%, counts: non-negative integers)
- Consistency checks (rate = count/total * 100)
- Required demographic coverage validation

## 🤝 Contributing

### Code Standards
- Use `python3` (not `python` - may default to Python 2.7)
- Follow existing patterns for ETL module structure
- Include comprehensive error handling and logging
- Add tests for new modules in `tests/` directory

### Documentation
- Document new data sources in numbered journal entries: `notes/#--description.md`
- Update this README for new features or data sources
- Maintain AI usage patterns in `CLAUDE.md`

### AI Assistance
See [CLAUDE.md](./CLAUDE.md) for AI assistant usage patterns and workflows.

## 📈 Recent Improvements

- **Suppression Transparency**: Includes suppressed records with explicit indicators
- **Enhanced Dashboard**: Interactive heatmaps with performance ranking
- **Data Quality Fixes**: Corrected 2024 kindergarten readiness interpretation
- **Comprehensive Testing**: Full test coverage with validation checks
- **Documentation**: Detailed journal entries tracking all changes

## 🎯 Future Roadmap

- **Multi-year trend analysis** in dashboard
- **Additional data sources** (discipline, enrollment, test scores)
- **Automated data validation** and alerting
- **Advanced equity gap analysis** and reporting
- **Integration with existing FCPS systems**

For detailed technical implementation and AI usage guidelines, see [CLAUDE.md](./CLAUDE.md).