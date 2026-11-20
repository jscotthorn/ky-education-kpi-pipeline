### ETL Pipeline Execution
Use the etl_runner.py file in the project root to run all pipelines:
- **Standard run**: `python3 etl_runner.py` (warnings/errors only)
- **Verbose logging**: `python3 etl_runner.py --verbose` (detailed progress)
- **Skip ETL**: `python3 etl_runner.py --skip-etl` (only combine files)
- **Testing**: `python3 -m pytest tests/` (run all tests)

### Metric Naming Convention - CRITICAL
- **Rates**: `{indicator}_rate_{period}` (e.g., `graduation_rate_4_year`)
- **Counts**: `{indicator}_count_{period}` (e.g., `graduation_count_4_year`) 
- **Totals**: `{indicator}_total_{period}` (e.g., `graduation_total_4_year`)

### Data Preparation
Use the KDE data preparation tool to populate raw directories:
- **Prepare all**: `python3 data/prepare_kde_data.py`
- **Specific dataset**: `python3 data/prepare_kde_data.py chronic_absenteeism`
- **List available**: `python3 data/prepare_kde_data.py --list`
- Configuration in `config/kde_sources.yaml`

### New ETL Pipeline process
- Use data preparation tool to populate `data/raw/source_name` directories automatically, adding config for your source if needed.
- Review `etl/postsecondary_readiness.py` and `etl/base_etl.py` to understand the base class and implementation.
- Sample all of the data files to be processed by the pipeline.
- Create a plan for how to implement the pipeline.
- Implement the pipeline
- Run the pipeline and fix any errors encountered
- Create and run unit tests, fixing errors. See `tests/test_postsecondary_readiness.py`
- Create and run e2e tests, fixing errors. See `tests/test_postsecondary_readiness_end_to_end.py`
- Create a new numbered journal entry to document the pipeline.

### Testing Protocol
**AI MUST test during development, not after:**
1. **Syntax Test**: `python3 etl/module_name.py` after each code change
2. **Unit Tests**: `python3 -m pytest tests/test_module_name.py -v` after test creation
3. **Integration Test**: Run full ETL pipeline to validate end-to-end
4. **Data Validation**: Check KPI format, column count, metric naming

### Error Handling - REQUIRED
**When errors occur:**
1. Fix syntax/import errors immediately (don't defer)
2. Update type annotations for compatibility
3. Re-test until clean execution

### Code Quality Standards
- Use `logging` for system messages, `print()` for user feedback only
- Include comprehensive try/except blocks with specific error handling
- Validate data ranges (rates: 0-100%, counts: non-negative)
- Add data source tracking for audit trails
- Fully define typing for all functions and variables

### Bayesian Analysis Pipeline (Full Workflow)

The complete analysis pipeline has 5 stages:

**Stage 1: Generate Analysis Datasets**
```bash
# Create datasets with outcome + covariates for each indicator/student group
python analysis/scripts/graduation_analysis.py --all-groups
python analysis/scripts/chronic_absenteeism_analysis.py --all-groups
# ... or run all at once:
python analysis/run_all_groups.py --datasets-only
```
Scripts: `analysis/scripts/*_analysis.py` → Output: `analysis/datasets/{indicator}_analysis_{group}.csv`

**Stage 2: Generate Empirical Priors**
```bash
# Analyze historical variance to set model priors
python analysis/prior_analysis/indicators/graduation_rate/historical_review.py --all-groups
# ... repeat for each indicator
python analysis/prior_analysis/aggregate_priors.py  # Combine all priors
```
Scripts: `analysis/prior_analysis/indicators/*/historical_review.py` → Output: `analysis/outputs/prior_analysis/`

**Stage 3: Run Bayesian Models (with MLflow tracking)**
```bash
python analysis/bayesian_models/run_experiment.py graduation -g all_students -n baseline
# Sensitivity analysis:
python run_experiment.py graduation -g all_students -n tighter -s 0.75
python run_experiment.py graduation -g all_students -n looser -s 1.5
# Compare and publish:
python run_experiment.py graduation --compare
python run_experiment.py graduation --publish <RUN_ID> -g all_students
python run_experiment.py --list-published
python run_experiment.py --ui  # MLflow web UI at http://localhost:5000
```
Scripts: `analysis/bayesian_models/*_model.py`, `run_experiment.py` → Output: `analysis/outputs/models/`

**Stage 4: Combine Results for Portal**
```bash
# From published MLflow runs (recommended):
python analysis/bayesian_models/combine_results.py --from-mlflow
# Or from filesystem (legacy):
python analysis/bayesian_models/combine_results.py
```
Output: `data/bayesian/bayesian_results.json`

**Stage 5: Extract Fayette Data (in fcps-equity-dashboard repo)**
```bash
cd ../fcps-equity-dashboard
npm run extract:fayette  # Extracts FCPS-specific KPI data
npm run preprocess-data  # Prepares data for dashboard
```
Script: `scripts/extract-fayette-kpi.js` → Output: `data/fayette-*.json`

**Quick Full Run** (datasets + models + combine):
```bash
python analysis/run_all_groups.py --indicators graduation chronic_absenteeism
```

Key docs: `analysis/bayesian_models/EXPERIMENT_TRACKING.md`, `analysis/PRIOR_SPECIFICATION_GUIDE.md`

### Documentation Standards
**AI must maintain:**
- **Journal entries**: Numbered sequence for investigations, ex "notes/23--safe-schools-events-pipeline-implementation.md"
- **Code comments**: Explain complex transformation logic
- **Test documentation**: Clear test case descriptions
- **README updates**: Keep user documentation current
- **KPI documentation**: Update KPIS.md when new KPIs are added or changed, and update README.md data source list when pipelines are added/modified

