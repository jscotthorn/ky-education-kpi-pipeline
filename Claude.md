You can use the etl_runner.py file in the project root to run all pipelines and python3 -m pytest tests/ to run all tests

### Metric Naming Convention - CRITICAL
- **Rates**: `{indicator}_rate_{period}` (e.g., `graduation_rate_4_year`)
- **Counts**: `{indicator}_count_{period}` (e.g., `graduation_count_4_year`) 
- **Totals**: `{indicator}_total_{period}` (e.g., `graduation_total_4_year`)

### Data Download
Use the KDE data downloader to populate raw directories:
- **Download all**: `python3 data/download_kde_data.py`
- **Specific dataset**: `python3 data/download_kde_data.py chronic_absenteeism`
- **List available**: `python3 data/download_kde_data.py --list`
- Configuration in `config/kde_sources.yaml`

### New ETL Pipeline process
- Use data downloader to populate `data/raw/source_name` directories automatically, adding config for your source if needed.
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

### Documentation Standards
**AI must maintain:**
- **Journal entries**: Numbered sequence for investigations, ex "notes/23--safe-schools-events-pipeline-implementation.md"
- **Code comments**: Explain complex transformation logic
- **Test documentation**: Clear test case descriptions
- **README updates**: Keep user documentation current

