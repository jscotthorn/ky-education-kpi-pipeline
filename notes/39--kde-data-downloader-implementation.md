# 38 - KDE Data Downloader Implementation

**Date**: 2025-07-21  
**Status**: Complete  
**Type**: Infrastructure Development  

## Overview

Implemented an automated data downloader to fetch raw source files from the Kentucky Department of Education (KDE) Historical Datasets website. This eliminates the need for developers to manually download and organize data files.

## Problem Statement

Developers needed an easy way to populate `data/raw/` directories with the latest files from KDE without manually navigating to the website and downloading individual CSV files. The existing manual process was error-prone and time-consuming.

## Solution Implementation

### 1. Configuration File (`config/kde_sources.json`)
Created a centralized configuration mapping raw data directories to their corresponding KDE files:
- **Base URL**: `https://www.education.ky.gov/Open-House/data/HistoricalDatasets/`
- **Headers**: Proper browser headers to avoid blocking
- **Directory Mappings**: Maps each `data/raw/` subdirectory to list of CSV files

Example configuration for chronic absenteeism:
```json
"chronic_absenteeism": [
  "KYRC24_CRDC_Chronic_Absenteeism.csv",
  "chronic_absenteeism_2023.csv",
  "chronic_absenteeism_2022.csv",
  "chronic_absenteeism_2021.csv",
  "chronic_absenteeism_2020.csv"
]
```

### 2. Download Script (`data/download_kde_data.py`)
Features implemented:
- **Command-line interface** with argument parsing
- **Rate limiting** (0.5s between files, 1.0s between directories)
- **Error handling** with retry logic and detailed logging
- **File existence checks** to avoid re-downloading
- **Progress reporting** with file sizes
- **Flexible targeting** (all datasets or specific ones)

### 3. Usage Patterns
```bash
# Download all configured datasets
python3 data/download_kde_data.py

# Download specific datasets
python3 data/download_kde_data.py chronic_absenteeism graduation_rates

# List available datasets
python3 data/download_kde_data.py --list
```

## Technical Implementation

### Core Features
1. **Class-based architecture** (`KDEDownloader`) for maintainability
2. **JSON configuration** for easy updates without code changes
3. **Proper HTTP headers** to mimic browser requests and avoid blocking
4. **Streaming downloads** for large files with chunk-based writing
5. **Comprehensive error handling** with specific exception types
6. **Logging integration** with timestamps and severity levels

### Rate Limiting Strategy
- **0.5 seconds** between individual file downloads
- **1.0 seconds** between directory processing
- Prevents server overload while maintaining reasonable speed

### File Organization
- Files downloaded directly to appropriate `data/raw/subdirectory/`
- Preserves original KDE filenames for audit trails
- Creates directories automatically if they don't exist

## Documentation Updates

### README.md
- Added "Data acquisition" section to Common Commands
- Updated Developer Workflow with download instructions
- Included example usage patterns

### CLAUDE.md  
- Added "Data Download" section with key commands
- Updated New ETL Pipeline process to reference downloader
- Provided configuration file location

## Directories Covered

Currently configured for 11 raw data directories:
1. `chronic_absenteeism` (5 files)
2. `cte_participation` (5 files)  
3. `english_learner_progress` (5 files)
4. `graduation_rates` (5 files)
5. `kindergarten_readiness` (5 files)
6. `out_of_school_suspension` (5 files)
7. `postsecondary_enrollment` (5 files)
8. `postsecondary_readiness` (5 files)
9. `safe_schools` (19 files)
10. `safe_schools_climate` (15 files)

**Total**: 84 files across 11 directories

## Benefits

### For Developers
- **One-command setup** for raw data acquisition
- **Consistent file organization** across development environments
- **No manual website navigation** required
- **Selective downloading** for specific datasets

### For Project Maintenance
- **Centralized configuration** makes updates easy
- **Version control** tracks which files are expected
- **Audit trail** through filename preservation
- **Extensible design** for future data sources

## Future Considerations

### Potential Enhancements
1. **Checksum validation** for file integrity verification
2. **Incremental updates** to only download changed files
3. **Parallel downloads** for improved performance
4. **Configuration validation** to catch mapping errors
5. **Integration with ETL runner** for automated workflows

### Maintenance Requirements
- **URL monitoring** - KDE may change file locations
- **Configuration updates** when new years of data become available
- **Error monitoring** to detect broken downloads

## Usage Recommendation

Developers should use this tool as the standard method for populating raw data directories. Manual downloading should only be used for testing or one-off analysis needs.

The tool is designed to be idempotent - running it multiple times will skip existing files and only download missing ones.

## Source Reference

Original inspiration from Jupyter notebook at `/Users/scott/Projects/jupylter playground/equity/scrape kde.ipynb` which demonstrated the URL patterns and HTTP header requirements for successful KDE data access.