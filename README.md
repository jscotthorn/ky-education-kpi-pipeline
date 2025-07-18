# Equity Scorecard ETL

Transform heterogeneous CSV exports from Fayette County Public Schools (FCPS) and other sources into a canonical `kpi_master.csv` file for equity KPI dashboards tracking achievement, discipline, enrollment, staffing, and other metrics.

## Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -e .
```

## Developer Loop

1. **Drop files** → Place new CSV exports in `data/raw/source_name/YYYYMMDD/`
2. **Draft run** → `python3 etl_runner.py --draft` to generate initial ETL logic
3. **Refine** → Edit generated ETL modules in `etl/` directory
4. **Production run** → `python3 etl_runner.py` to process all sources
5. **Review** → Check `kpi/kpi_master.csv` output
6. **Pull Request** → Submit refined ETL logic for review

## Project Structure

- `data/raw/` - Timestamped source dumps (never edited)
- `etl/` - Python modules with `transform()` functions per source
- `data/processed/` - Normalized outputs from ETL modules
- `kpi/` - Final combined output and logic
- `config/` - YAML configuration for field mappings
- `notes/` - Auto-generated analysis from AI investigations

## AI Usage

See [Claude.md](./Claude.md) for AI assistance policies and workflows.