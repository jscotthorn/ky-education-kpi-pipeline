# Excel (xlsx) Support for Historical KDE Data

**Date**: 2024-12-01
**Status**: Implemented

## Background

Kentucky Department of Education provides historical datasets (pre-2020) in Excel format (.xlsx) rather than CSV. These files contain valuable longitudinal data, particularly for Kentucky Summative Assessment results by grade and demographic.

Example file analyzed:
- `ASSESSMENT_PROFICIENCY_GRADE_18-19.xlsx`
- 501,859 rows in the DATA sheet
- Contains 3rd grade reading by demographic - critical for Read to Achieve analysis

## Current Limitations

The ETL pipeline currently only processes CSV files:
- `base_etl.py` uses `glob("*.csv")` for file discovery
- `pd.read_csv()` for reading data
- No support for multi-sheet workbooks

## Proposed Changes

### 1. kde_sources.yaml

Add historical dataset base URL:
```yaml
base_url_historical: "https://www.education.ky.gov/Open-House/data/HistoricalDatasets/"
```

Add xlsx files to existing sources:
```yaml
kentucky_summative_assessment:
  # ... existing CSV sources ...
  - url: "historical"
    file: "ASSESSMENT_PROFICIENCY_GRADE_18-19.xlsx"
  - url: "historical"
    file: "ASSESSMENT_PROFICIENCY_GRADE_17-18.xlsx"
```

### 2. prepare_kde_data.py

Add handling for `url: "historical"` type:
```python
elif url_type == 'historical':
    base_url = self.config.get('base_url_historical', self.config['base_url'])
```

The binary file download already works for xlsx files.

### 3. base_etl.py

Add new property for xlsx sheet configuration:
```python
@property
def xlsx_sheet_map(self) -> Dict[str, str]:
    """Map xlsx filename patterns to sheet names containing data.

    Override in subclass to specify which sheet to read for each xlsx file.
    Default: 'DATA' sheet for all xlsx files.

    Returns:
        Dict mapping filename patterns (glob-style) to sheet names.
        Use '*' as a catch-all default.

    Example:
        return {
            'ASSESSMENT_PROFICIENCY_*.xlsx': 'DATA',
            'GAP_*.xlsx': 'DATA',
            '*': 'Sheet1'  # fallback
        }
    """
    return {'*': 'DATA'}
```

Update file discovery:
```python
def _find_data_files(self, source_dir: Path) -> List[Tuple[Path, Optional[str]]]:
    """Find all data files with optional sheet info for xlsx."""
    files = []

    # CSV files (no sheet needed)
    for pattern in ["*.csv", "*.CSV"]:
        for f in source_dir.glob(pattern):
            files.append((f, None))

    # XLSX files (with sheet from config)
    for xlsx_file in source_dir.glob("*.xlsx"):
        sheet = self._get_xlsx_sheet(xlsx_file)
        files.append((xlsx_file, sheet))

    return files

def _get_xlsx_sheet(self, xlsx_file: Path) -> str:
    """Determine which sheet to read from xlsx file."""
    import fnmatch
    for pattern, sheet in self.xlsx_sheet_map.items():
        if fnmatch.fnmatch(xlsx_file.name, pattern):
            return sheet
    return 'DATA'  # Default fallback
```

Add file reading abstraction:
```python
def _read_data_file(self, file_path: Path, sheet: Optional[str] = None) -> pd.DataFrame:
    """Read CSV or XLSX file into DataFrame."""
    if file_path.suffix.lower() == '.xlsx':
        return pd.read_excel(
            file_path,
            sheet_name=sheet or 'DATA',
            dtype=str
        )
    else:
        return pd.read_csv(
            file_path,
            encoding='utf-8-sig',
            dtype=str,
            low_memory=False
        )
```

### 4. ETL Module Updates (e.g., kentucky_summative_assessment.py)

Add xlsx sheet configuration:
```python
@property
def xlsx_sheet_map(self) -> Dict[str, str]:
    return {
        'ASSESSMENT_PROFICIENCY_*.xlsx': 'DATA',
        'ASSESSMENT_PROFICIENCY_LEVEL_*.xlsx': 'DATA',
        'GAP_*.xlsx': 'DATA',
        '*': 'DATA'
    }
```

Add historical demographic code mapping:
```python
HISTORICAL_DEMOGRAPHIC_MAP = {
    'ACO': 'All Students',
    'ETW': 'White',
    'ETB': 'African American',
    'ETH': 'Hispanic or Latino',
    'ETA': 'Asian',
    'ETI': 'American Indian or Alaska Native',
    'ETO': 'Two or More Races',
    'ETP': 'Native Hawaiian or Pacific Islander',
    'LUP': 'Economically Disadvantaged',
    'LUN': 'Non-Economically Disadvantaged',
    'CSG': 'Students with Disabilities (IEP)',
    'CSN': 'Students without Disabilities (IEP)',
    'LEP': 'English Learner',
    'ELN': 'Non-English Learner',
    'SXF': 'Female',
    'SXM': 'Male',
    'FOS': 'Foster Care',
    'HOM': 'Homeless',
    'HON': 'Non-Homeless',
    'MIG': 'Migrant',
    'MIL': 'Military Connected',
    'GTR': 'Gifted and Talented',
    'GTN': 'Non-Gifted',
    'TST': 'Tested',  # All tested students
}
```

## Historical Data Format Differences

| Field | Historical (xlsx) | Modern (CSV) |
|-------|-------------------|--------------|
| Demographic | 3-letter codes (ETA, ETB, etc.) | Full text labels |
| Grade | Integer (3, 4, 5...) | "Grade 3", "Grade 4" |
| Subject | 2-letter codes (RD, MA, WR) | "Reading", "Math" |
| School ID | SCH_CD column | school_code column |
| Proficiency | PROFICIENT_DISTINGUISHED combined | Separate columns |

## Implementation Order

1. Update `base_etl.py` with xlsx reading capability
2. Update `prepare_kde_data.py` for historical URL type
3. Add entries to `kde_sources.yaml`
4. Update `kentucky_summative_assessment.py` with historical format handling
5. Test with 2018-2019 data
6. Extend to additional historical years

## Files to Modify

- `etl/base_etl.py` - Core xlsx support
- `data/prepare_kde_data.py` - Download historical files
- `config/kde_sources.yaml` - Source configuration
- `etl/kentucky_summative_assessment.py` - Historical format handling
- `etl/demographic_mapper.py` - Historical code mappings

## Future Considerations

- Multi-sheet processing: Some xlsx files might have multiple data sheets (e.g., by year or by metric). The current design supports this by extending `xlsx_sheet_map` to return a list and processing each sheet separately.
- Memory efficiency: Large xlsx files should use chunked reading if memory becomes an issue.
- Validation: Add sheet existence check before reading to provide clear error messages.

## Testing

1. Unit tests for xlsx reading functions
2. Integration test with actual historical file
3. Verify demographic mappings produce consistent student_group values
4. Compare metrics between historical and modern data for overlapping years
