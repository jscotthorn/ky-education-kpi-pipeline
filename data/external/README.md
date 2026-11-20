# External Data Sources

This directory contains data from external (non-KDE) sources used in the Hierarchical Bayesian bright spots model. These data sources provide economic and demographic context that KDE does not publish.

## Directory Structure

```
external/
├── census_saipe/           # Census county-level income and poverty
│   ├── census_saipe_2021.csv
│   ├── census_saipe_2022.csv
│   ├── census_saipe_2023.csv
│   └── census_saipe_combined.csv
├── census_acs/             # Census tract-level detailed demographics
│   ├── census_acs_tracts_fayette_2022.csv
│   └── census_acs_counties_2022.csv
├── metadata/
│   └── data_dictionary.csv # Variable definitions and update schedules
└── README.md               # This file
```

## Census SAIPE (Small Area Income and Poverty Estimates)

**Source:** U.S. Census Bureau
**API Documentation:** https://www.census.gov/programs-surveys/saipe.html
**Update Frequency:** Annual (released December for prior calendar year)
**Typical Lag:** 12 months
**Geographic Level:** County

### Variables

| Variable | Description | Use in Model |
|----------|-------------|--------------|
| `median_household_income` | Median household income estimate | Community economic context |
| `poverty_rate_all_ages` | Poverty rate for all ages | Community economic stress |
| `poverty_rate_0_17` | Child poverty rate (ages 0-17) | Student population economic context |
| `poverty_rate_5_17` | School-age poverty rate (ages 5-17) | Most relevant for school analysis |

### Running the ETL

```bash
# Fetch 2021-2023 data (default)
python etl/census_saipe.py

# Fetch specific years
python etl/census_saipe.py --years 2020 2021 2022 2023

# With API key (optional but recommended for rate limits)
python etl/census_saipe.py --api-key YOUR_API_KEY

# Or set environment variable
export CENSUS_API_KEY=your_key_here
python etl/census_saipe.py
```

### Getting a Census API Key

Get a free API key at: https://api.census.gov/data/key_signup.html

While the API works without a key for reasonable usage, having a key ensures
you won't hit rate limits during development.

## Temporal Alignment

Census SAIPE data has a 12-month lag. When analyzing 2024-25 school year outcomes:

| Data Type | Vintage to Use | Notes |
|-----------|----------------|-------|
| School outcomes | 2025 | Current year |
| Census income | 2023 | Most recent (acceptable - changes slowly) |
| Census poverty | 2023 | Most recent (acceptable - changes slowly) |

Economic indicators change slowly enough that 12-month lag is acceptable for
modeling purposes. See `BRIGHT_SPOTS_DATA_PIPELINE_PRIORITIES.md` for detailed
discussion of temporal alignment.

## Joining with School Data

SAIPE data is at the **county level**. To join with school-level data:

```python
import pandas as pd

# Load school data (has county_name or county_number)
schools = pd.read_csv('data/processed/graduation_rates.csv')

# Load SAIPE data
saipe = pd.read_csv('data/external/census_saipe/census_saipe_2023.csv')

# Pivot to wide format (one row per county)
saipe_wide = saipe.pivot(
    index=['county_fips', 'county_name'],
    columns='metric',
    values='value'
).reset_index()

# Join by county name
schools_with_income = schools.merge(
    saipe_wide,
    on='county_name',
    how='left'
)
```

## Data Quality Notes

1. **All 120 Kentucky counties** are included in SAIPE
2. **No suppression** - county-level estimates are always available
3. **Estimates only** - these are modeled estimates, not actual counts
4. **Margin of error** - Census provides margins of error (not currently extracted)

---

## Census ACS 5-Year Estimates (Tract-Level Demographics)

**Source:** U.S. Census Bureau American Community Survey
**API Documentation:** https://www.census.gov/data/developers/data-sets/acs-5year.html
**Update Frequency:** Annual (released December)
**Typical Lag:** 24 months (5-year rolling average ending 2 years prior)
**Geographic Level:** Census Tract (within-county granularity)

### Purpose

ACS 5-year estimates provide **tract-level** data for capturing within-county
variation. This is critical for Fayette County analysis where schools in
different neighborhoods serve very different communities:

- Median income ranges from ~$19K to ~$217K across tracts
- Poverty rates range from 0.8% to 87%
- Bachelor's degree attainment ranges from 8% to 81%

### Variables

| Variable | Description | Use in Model |
|----------|-------------|--------------|
| `median_household_income` | Median income by tract | Within-county economic variation |
| `poverty_rate` | Poverty rate by tract | Neighborhood economic stress |
| `unemployment_rate` | Labor force unemployment | Economic distress indicator |
| `pct_bachelors_plus` | % with bachelor's degree+ | Community education level |
| `pct_single_parent` | % single-parent families | Family structure risk factor |
| `pct_owner_occupied` | % owner-occupied housing | Neighborhood stability proxy |
| `pct_broadband` | % with broadband internet | Digital divide indicator |
| `pct_housing_cost_burden_30_plus` | % paying 30%+ on housing | Housing stress |

### Running the ETL

```bash
# Fetch Fayette County tracts (default)
python etl/census_acs.py --year 2022 --county 067

# Fetch all major Kentucky counties
python etl/census_acs.py --year 2022 --all-major

# County-level only (all 120 counties)
python etl/census_acs.py --year 2022 --counties-only

# Long format (one row per metric)
python etl/census_acs.py --year 2022 --county 067 --long-format
```

### County FIPS Codes

| County | FIPS | Notes |
|--------|------|-------|
| Fayette | 067 | Primary focus |
| Jefferson | 111 | Louisville |
| Boone | 015 | Northern KY |
| Kenton | 117 | Northern KY |
| Warren | 227 | Bowling Green |

### Joining with School Data

Tract-level data requires geocoding schools to census tracts. For now, we can
use tract-level aggregates to characterize the variation within a county:

```python
import pandas as pd

# Load tract data for Fayette County
tracts = pd.read_csv('data/external/census_acs/census_acs_tracts_fayette_2022.csv')

# Summarize within-county variation
print("Fayette County within-county variation:")
print(tracts[['median_household_income', 'poverty_rate', 'pct_bachelors_plus']].describe())

# Can be used to:
# 1. Understand range of community contexts schools serve
# 2. Geocode school addresses to tracts for direct matching
# 3. Calculate distance-weighted neighborhood characteristics
```

### Temporal Alignment

ACS 5-year has 24-month lag. For 2024-25 school year analysis:

| Data Type | Vintage | Period Covered |
|-----------|---------|----------------|
| ACS 5-year 2022 | Dec 2023 release | 2018-2022 average |
| ACS 5-year 2023 | Dec 2024 release | 2019-2023 average |

The 5-year rolling average means the data is inherently smoothed and less
sensitive to short-term economic fluctuations, making the 24-month lag
less problematic than it might seem.
