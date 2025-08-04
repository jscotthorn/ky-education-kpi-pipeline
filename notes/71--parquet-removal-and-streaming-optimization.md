# 71 - Parquet Removal and Streaming Optimization

**Date**: 2025-07-26  
**Status**: ✅ Complete

## Changes Made

### 1. Removed Parquet Support
- **etl_runner.py**: Removed all parquet-related code and parameters
- **pyproject.toml**: Removed `[project.optional-dependencies] parquet = ["pyarrow"]`
- **README.md**: Removed parquet installation instructions and references
- **tests/test_kpi_combination.py**: Removed parquet-dependent tests and pyarrow imports
- **ky-education-portal/astro.config.mjs**: Removed `.parquet` from file extension whitelist

### 2. Implemented Streaming File Combination
**Previous**: `combine_kpi_files()` loaded all CSV files into pandas DataFrames, concatenated them in memory, then wrote output
**New**: `combine_kpi_files()` uses file streaming:
- Writes standard KPI header once to output file
- Opens each source CSV file and streams data rows directly to output
- Skips header line in each source file
- Processes one row at a time without loading entire datasets into memory

### 3. Added --skip-etl Flag
- New command line argument: `--skip-etl`
- When used, skips all ETL pipeline processing and only runs the combine function
- Useful for quickly regenerating master file from existing processed data

## Technical Benefits

### Memory Efficiency
- **Before**: Could require several GB of RAM for large datasets
- **After**: Memory usage constant regardless of dataset size
- Eliminates potential out-of-memory errors on large datasets

### Simplified Dependencies
- Removed pyarrow dependency (complex binary package)
- Reduced optional dependency management complexity
- Cleaner installation process

### Faster Combination Process
- No DataFrame operations (sorting, casting, validation)
- Direct file-to-file streaming
- Reduced CPU overhead

## Breaking Changes

### API Changes
- `combine_kpi_files()` signature simplified - removed `output_parquet_path` parameter
- No longer generates `.parquet` files
- Test suite updated to reflect CSV-only output

### Output Changes
- Master dataset now only available in CSV format
- Removed automatic parquet file generation
- File size impact: CSV-only reduces storage complexity but may be slightly larger than parquet

## Impact Assessment

### Positive
- ✅ Eliminates memory constraints for large datasets
- ✅ Simpler installation (no pyarrow compilation issues)
- ✅ Faster file combination process
- ✅ Reduced dependency complexity

### Neutral
- 📊 Parquet format rarely used in this workflow
- 📊 CSV format sufficient for downstream consumers
- 📊 Web portal and API generation already CSV-based

## Files Modified
- `ky-education-kpi-pipeline/etl_runner.py`
- `ky-education-kpi-pipeline/pyproject.toml` 
- `ky-education-kpi-pipeline/README.md`
- `ky-education-kpi-pipeline/tests/test_kpi_combination.py`
- `ky-education-portal/astro.config.mjs`

## Usage
```bash
# Normal ETL + combine
python3 etl_runner.py

# Skip ETL, only combine existing processed files
python3 etl_runner.py --skip-etl
```