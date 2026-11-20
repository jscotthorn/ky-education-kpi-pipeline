# Census External Data Pipelines Implementation

**Date:** November 24, 2025
**Purpose:** Implement Census Bureau data pipelines for Hierarchical Bayesian bright spots modeling

---

## Overview

Implemented two new ETL pipelines for external (non-KDE) Census data to support the Hierarchical Bayesian bright spots methodology. These pipelines fetch economic and demographic indicators that KDE does not publish but are critical covariates for the model.

## Background

The bright spots methodology requires controlling for community economic context when identifying schools performing above expectations. With Fayette County's small sample sizes (n=6 high schools, n<20 middle schools), standard regression is invalid. Hierarchical Bayesian modeling borrows strength from statewide data while accounting for local patterns.

Key requirements from `BRIGHT_SPOTS_DATA_PIPELINE_PRIORITIES.md`:
1. County-level income and poverty data (Census SAIPE)
2. Tract-level demographics for within-county variation (Census ACS 5-Year)

---

## Pipeline 1: Census SAIPE (Small Area Income and Poverty Estimates)

### File: `etl/census_saipe.py`

**Purpose:** Fetch county-level economic indicators for all 120 Kentucky counties.

**Data Source:**
- API: `https://api.census.gov/data/timeseries/poverty/saipe`
- Update frequency: Annual (December release for prior year)
- Typical lag: 12 months

### Metrics Extracted

| Metric | Description | Model Use |
|--------|-------------|-----------|
| `median_household_income` | Median household income estimate | Community economic context |
| `poverty_rate_all_ages` | Poverty rate for all ages | Economic stress indicator |
| `poverty_rate_0_17` | Child poverty rate (ages 0-17) | Student population context |
| `poverty_rate_5_17` | School-age poverty (ages 5-17) | Most relevant for schools |

### Usage

```bash
# Default: fetch 2021-2023
python etl/census_saipe.py

# Specific years
python etl/census_saipe.py --years 2020 2021 2022 2023

# With API key (recommended)
export CENSUS_API_KEY=your_key
python etl/census_saipe.py
```

### Output

```
data/external/census_saipe/
├── census_saipe_2022.csv
├── census_saipe_2023.csv
└── census_saipe_combined.csv
```

### Sample Data (Fayette County 2023)

| Metric | Value |
|--------|-------|
| Median Household Income | $67,320 |
| Poverty Rate (all ages) | 15.7% |
| Child Poverty (0-17) | 18.2% |
| School-age Poverty (5-17) | 17.2% |

---

## Pipeline 2: Census ACS 5-Year Estimates

### File: `etl/census_acs.py`

**Purpose:** Fetch tract-level demographic data for capturing within-county variation.

**Data Source:**
- API: `https://api.census.gov/data/YYYY/acs/acs5`
- Update frequency: Annual (December release)
- Typical lag: 24 months (5-year rolling average)

### Why Tract-Level Data Matters

County-level SAIPE data treats all of Fayette County as homogeneous, but schools serve very different communities. The tract-level ACS data reveals massive within-county variation:

| Metric | Min | Max | Range |
|--------|-----|-----|-------|
| Median Income | $19,229 | $216,607 | 11x difference |
| Poverty Rate | 0.8% | 87.0% | Enormous spread |
| Bachelor's+ | 8.5% | 80.7% | 10x difference |

This variation is critical for properly controlling for community context in the model.

### Metrics Extracted

**Raw Variables (20+):**
- Income: B19013_001E (median household income)
- Poverty: B17001_001E, B17001_002E
- Education: B15003 series (educational attainment)
- Employment: B23025 series (labor force, unemployment)
- Housing: B25003, B25070 series
- Internet: B28002 series (broadband access)
- Family structure: B11003 series

**Derived Metrics:**

| Metric | Calculation | Model Use |
|--------|-------------|-----------|
| `poverty_rate` | Below poverty / Total | Neighborhood economic stress |
| `unemployment_rate` | Unemployed / Labor force | Economic distress |
| `pct_bachelors_plus` | Bach+ / Pop 25+ | Community education level |
| `pct_single_parent` | Single parent / Families | Family structure risk |
| `pct_owner_occupied` | Owner / Total housing | Neighborhood stability |
| `pct_broadband` | Broadband / Households | Digital divide |
| `pct_housing_cost_burden_30_plus` | 30%+ on housing | Housing stress |

### Usage

```bash
# Fayette County tracts (default)
python etl/census_acs.py --year 2022 --county 067

# All major Kentucky counties
python etl/census_acs.py --year 2022 --all-major

# County-level for all 120 counties
python etl/census_acs.py --year 2022 --counties-only

# Long format output
python etl/census_acs.py --year 2022 --long-format
```

### Output

```
data/external/census_acs/
├── census_acs_tracts_fayette_2022.csv    # 82 tracts, wide format
├── census_acs_counties_2022.csv          # All KY counties
└── census_acs_tracts_combined_2022.csv   # Multiple counties
```

### Special Value Handling

Census uses negative values (e.g., -666666666) to indicate missing/unavailable data. The ETL converts these to NaN to prevent calculation errors.

---

## Directory Structure Created

```
data/external/
├── census_saipe/
│   ├── census_saipe_2022.csv
│   ├── census_saipe_2023.csv
│   └── census_saipe_combined.csv
├── census_acs/
│   └── census_acs_tracts_fayette_2022.csv
├── metadata/
│   └── data_dictionary.csv
└── README.md
```

---

## Tests Created

### `tests/test_census_saipe.py`
- 23 unit tests covering extraction, transformation, loading
- 2 integration tests (real API calls)
- Edge case handling (missing values, API errors)

### `tests/test_census_acs.py`
- 23 unit tests
- Derived metric calculation verification
- Census missing value handling tests
- Zero denominator edge cases

All tests passing:
```
tests/test_census_saipe.py: 25 passed
tests/test_census_acs.py: 25 passed
```

---

## Temporal Alignment Notes

Per `BRIGHT_SPOTS_DATA_PIPELINE_PRIORITIES.md`, economic indicators change slowly enough that lag is acceptable:

| Data Source | Lag | Acceptable? | Rationale |
|-------------|-----|-------------|-----------|
| SAIPE | 12 months | Yes | Income/poverty change slowly |
| ACS 5-Year | 24 months | Yes | 5-year average is inherently smoothed |

For 2024-25 school year analysis:
- School outcomes: 2025 (current)
- SAIPE: 2023 vintage (acceptable)
- ACS: 2022 vintage (acceptable)

---

## Integration with Model

These pipelines support Phase 1 of the Bayesian model implementation:

```python
# Load school outcomes
outcomes = pd.read_csv('data/processed/graduation_rates.csv')

# Load county-level economic context
saipe = pd.read_csv('data/external/census_saipe/census_saipe_2023.csv')

# Join by county
model_data = outcomes.merge(
    saipe.pivot(index='county_fips', columns='metric', values='value'),
    left_on='county_fips',
    right_index=True
)

# Now ready for PyMC hierarchical model
```

For within-Fayette variation, schools can be geocoded to census tracts for more granular neighborhood characteristics.

---

## API Key Setup

Get a free Census API key at: https://api.census.gov/data/key_signup.html

```bash
# Add to .env file
CENSUS_API_KEY=your_key_here

# Or export directly
export CENSUS_API_KEY=your_key_here
```

The APIs work without a key for reasonable usage, but a key prevents rate limiting during development.

---

## Next Steps

Per `BRIGHT_SPOTS_DATA_PIPELINE_PRIORITIES.md`:

1. **BLS Unemployment via FRED** (Tier 2) - Monthly county unemployment
2. **School-to-tract geocoding** - Match schools to census tracts
3. **Model data preparation script** - `scripts/prepare_model_data.py`
4. **PyMC hierarchical model MVP** - Start with graduation rates

---

## Files Modified/Created

### New Files
- `etl/census_saipe.py` - SAIPE ETL module
- `etl/census_acs.py` - ACS 5-Year ETL module
- `tests/test_census_saipe.py` - SAIPE unit tests
- `tests/test_census_acs.py` - ACS unit tests
- `data/external/README.md` - External data documentation
- `data/external/metadata/data_dictionary.csv` - Variable definitions

### Data Files Generated
- `data/external/census_saipe/census_saipe_2022.csv`
- `data/external/census_saipe/census_saipe_2023.csv`
- `data/external/census_saipe/census_saipe_combined.csv`
- `data/external/census_acs/census_acs_tracts_fayette_2022.csv`
