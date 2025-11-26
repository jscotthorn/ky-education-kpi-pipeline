# District School List Coordinates Pipeline Implementation

**Date**: 2025-11-24
**Author**: Claude (AI Assistant)

## Overview

Implemented a new ETL pipeline to extract school geographic coordinates (latitude and longitude) from the Kentucky Department of Education District School List files. These coordinates enable location-based analysis and mapping of school-level KPIs.

## Data Source

The District School List files (`KYRC24_OVW_District_School_List.csv`, `district_school_list_{year}.csv`) were already being downloaded but not processed by any ETL pipeline. These files contain comprehensive school directory information including:

- School identification (name, code, NCES ID)
- District and county information
- Contact details (address, phone, fax)
- **Geographic coordinates (latitude, longitude)**
- Title I status
- Grade range

## Implementation Details

### New Files Created

1. **`etl/district_school_list.py`** - New ETL module
2. **`tests/test_district_school_list.py`** - Unit tests (11 tests)
3. **`tests/test_district_school_list_end_to_end.py`** - End-to-end tests (8 tests)

### Configuration Updates

- **`config/kde_sources.yaml`** - Added KYRC25 file URL
- **`config/mappings.yaml`** - Added `district_school_list` source entry

### KPI Metrics

Two new metrics exposed in standard KPI format:

| Metric | Description | Unit |
|--------|-------------|------|
| `school_latitude` | School latitude in decimal degrees | ~36-39° for Kentucky |
| `school_longitude` | School longitude in decimal degrees | ~-89 to -82° for Kentucky |

### Data Validation

The pipeline includes geographic validation to ensure coordinates fall within Kentucky's boundaries:

- **Latitude**: 35.0° to 40.0° (Kentucky spans ~36.5° to ~39.1°)
- **Longitude**: -90.0° to -80.0° (Kentucky spans ~-89.6° to ~-81.9°)

**Important**: Both coordinates must be valid for either to be included. This ensures data integrity - a school with only one valid coordinate would be unusable for mapping.

### Filtering Logic

The pipeline skips:
- District aggregate rows (`---District Total---`, `All Schools`, empty school names)
- Rows with missing or invalid coordinates
- Coordinates outside Kentucky's geographic bounds

## Output Statistics

| Year | Schools |
|------|---------|
| 2020 | 1,464 |
| 2021 | 1,478 |
| 2022 | 1,480 |
| 2023 | 1,485 |
| 2024 | 1,485 |
| 2025 | 1,485 |

**Total KPI rows**: 17,754 (2 metrics × ~1,480 schools × 6 years)

## Usage Examples

### Geographic Analysis

```python
import pandas as pd

# Load KPI data
kpi_df = pd.read_csv('data/processed/district_school_list.csv')

# Get coordinates for 2025
coords_2025 = kpi_df[kpi_df['year'] == 2025].pivot(
    index=['school_id', 'school_name', 'district'],
    columns='metric',
    values='value'
).reset_index()

# Now has school_latitude and school_longitude columns
print(coords_2025.head())
```

### Joining with Other KPIs

```python
# Load graduation rates
grad_df = pd.read_csv('data/processed/graduation_rates.csv')

# Load coordinates
coord_df = kpi_df[kpi_df['metric'].isin(['school_latitude', 'school_longitude'])]
coord_pivot = coord_df.pivot(
    index=['school_id', 'year'],
    columns='metric',
    values='value'
).reset_index()

# Join
grad_with_coords = grad_df.merge(
    coord_pivot,
    on=['school_id', 'year'],
    how='left'
)
```

## Notes

1. **Institutional Data**: This pipeline produces school-level data without demographic breakdowns. All records have `student_group='All Students'`.

2. **Historical Consistency**: Column names vary between file formats:
   - KYRC24/KYRC25: Title case (`Latitude`, `Longitude`)
   - Historical: Uppercase (`LATITUDE`, `LONGITUDE`)

   The ETL handles both formats.

3. **Coordinate Precision**: Original data has variable precision (some coordinates have 6 decimal places, others have 9). Values are preserved as-is.

## Testing

All 19 tests pass:

```
tests/test_district_school_list.py: 11 passed
tests/test_district_school_list_end_to_end.py: 8 passed
```

Key test coverage:
- Column mapping normalization
- Metric extraction with valid/invalid coordinates
- District total row filtering
- Geographic boundary validation
- KPI format conversion
- Multi-year data processing
- Latitude/longitude pair integrity
