# Kentucky Education Data Pipeline

Transform heterogeneous CSV exports from the Kentucky Department of Education (KDE) into standardized KPI format for multi-year educational performance reporting. Handles achievement, discipline, enrollment, readiness, and other educational metrics across districts and schools. This project has been developed with the use of [Claude Code](https://www.anthropic.com/claude-code).

**📂 Repository**: https://github.com/jscotthorn/ky-education-kpi-pipeline

## 🚀 Quick Start

```bash
# Setup environment
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -e .

# Optional: Install parquet support
# Option 1: Try direct install (may require system libraries)
pip install -e .[parquet]

# Option 2: Install Apache Arrow first (macOS)
brew install apache-arrow
pip install -e .[parquet]

# Option 3: Use conda for better pre-built support
conda install -c conda-forge pyarrow

# Run ETL pipeline
python3 etl_runner.py

# View results
open data/processed/  # Individual source files
open data/kpi/        # Combined master dataset (CSV & Parquet)
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
ky-education-data-pipeline/
├── data/
│   ├── raw/                    # Source CSV files (never edited)
│   │   ├── graduation_rates/   # One folder per data source
│   │   ├── kindergarten_readiness/
│   │   └── chronic_absenteeism/
│   ├── processed/              # Standardized KPI outputs
│   └── kpi/                    # Combined master KPI file
├── etl/                        # ETL transformation modules
│   ├── graduation_rates.py
│   ├── kindergarten_readiness.py
│   ├── chronic_absenteeism.py
│   └── demographic_mapper.py   # Standardizes student groups
├── html/                       # Dashboard and visualization
│   ├── equity_dashboard.html
│   ├── generate_dashboard_data.py
│   └── data/                   # Generated JSON for dashboard
├── config/                     # Configuration files
│   └── demographic_mappings.yaml   # Single source of demographic mappings used by DemographicMapper
├── tests/                      # Test suite
├── notes/                      # Documentation and analysis
└── etl_runner.py               # Main orchestration script
```

## 🎯 Data Output Format

All ETL modules produce standardized **long format** KPI data:

| Column | Description | Example |
|--------|-------------|---------|
| `district` | District name | `"Jefferson County"` |
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

## 🌐 Data Access

### Online Data Browser
Browse and download data files at: **https://education.kyopengov.org/data/**

- The data directory contains:
- **📊 KPI Master Dataset** (`/kpi/`) - Combined dataset with all metrics in standardized format (CSV and Parquet, 175MB)
- **⚙️ Processed Files** (`/processed/`) - Individual metric files ready for analysis with audit logs
- **📁 Raw Data** (`/raw/`) - Original unmodified files from Kentucky Department of Education

Each directory includes detailed descriptions, file metadata, and direct download links. All data follows the standardized 10-column KPI format with demographic breakdowns and suppression handling.

## 📊 Available Data Sources

### Working Pipelines
- **Graduation Rates** (`graduation_rates.csv`)
  - 4-year and 5-year graduation rates, counts, and totals
  - Years: 2021-2024
  - All districts with demographic breakdowns

- **Safe Schools Events** (`safe_schools_events.csv`) ⭐ **NEW**
  - Behavioral incident counts by type, grade, location, and context
  - Years: 2020-2024 (5-year longitudinal coverage)
  - Includes both demographic breakdowns and aggregate totals
  - Supports school-to-prison pipeline analysis

- **Kindergarten Readiness** (`kindergarten_readiness.csv`)
  - Kindergarten readiness screening rates and counts
  - Years: 2021-2024  
  - All districts with demographic breakdowns

- **Chronic Absenteeism** (`chronic_absenteeism.csv`)
  - Chronic absenteeism rates, counts, and enrollment
  - Years: 2023-2024
  - All districts with demographic breakdowns

- **English Learner Progress** (`english_learner_progress.csv`)
  - Proficiency rates across elementary, middle, and high school
  - Years: 2022-2024
  - All districts with demographic breakdowns

- **Postsecondary Readiness** (`postsecondary_readiness.csv`)
  - College and career readiness rates
  - Years: 2022-2024
  - All districts with demographic breakdowns

- **Postsecondary Enrollment** (`postsecondary_enrollment.csv`)
  - Post-graduation enrollment in Kentucky institutions
  - Years: 2020-2024
  - All districts with demographic breakdowns

- **Out-of-School Suspension** (`out_of_school_suspension.csv`)
  - Discipline action counts by type
  - Years: 2020-2023
  - All districts with demographic breakdowns

### Dashboard Features
- **Interactive heatmaps** showing schools vs demographics  
- **Performance-based ranking** (highest performing schools at top)
- **Suppression transparency** (gaps for privacy-protected data)
- **Multi-district filtering** and data coverage metrics
- **Time series visualization** across available years

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
- **S3-based data hosting** for public access
- **Type Enforcement**: Uses `config/mappings.yaml` to enforce column dtypes

## 🎯 Future Roadmap

### Data Expansion Strategy
Based on comprehensive analysis of 94 additional Kentucky Department of Education data sources, the project roadmap includes **40+ high-value KPI opportunities** across school discipline, teacher quality, advanced coursework, vulnerable populations, and intersectional equity analysis.

**Key Implementation Priorities**:
- **School Discipline Equity** - Complete school-to-prison pipeline tracking through SAFE behavior event files
- **Teacher Quality Disparities** - Resource equity analysis (Title I schools have 66.0% vs Non-Title I 35.4% students with inexperienced teachers)
- **Vulnerable Population Analysis** - Extreme intersections (14.4% of migrant students experiencing homelessness)
- **Advanced Academic Opportunities** - College prep and career readiness equity gaps
- **Intersectional Analysis** - Multi-dimensional demographic combinations across all performance areas

### Planning Documentation
- **Journal 20** (`notes/20--kyrc24-new-data-sources-investigation.md`) - Comprehensive analysis of 94 files across 8 categories with detailed equity insights and discovery documentation
- **Journal 21** (`notes/21--kyrc24-implementation-roadmap.md`) - Strategic implementation roadmap with 12 sprints across 4 phases, prioritized by equity impact
- **Journal 22** (`notes/22--longitudinal-data-availability-analysis.md`) - Historical data availability analysis across all planned sprints, documenting 4 complete school years (2020-24) plus current year with specific legacy file naming patterns

For detailed technical implementation and AI usage guidelines, see [CLAUDE.md](./CLAUDE.md).

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

Copyright 2025 Kentucky Open Government Coalition

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.