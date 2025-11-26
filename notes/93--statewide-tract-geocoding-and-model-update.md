# Statewide Census Tract Geocoding and Graduation Rate Model Update

**Date:** November 25, 2025
**Context:** Expanding tract-level Census data from Fayette County to statewide coverage

---

## Summary

Extended the graduation rate Bayesian model from 20 to 24 covariates by adding statewide tract-level Census data. Previously, tract-level data was only available for Fayette County (72 schools). Now all 1,478 Kentucky schools are geocoded to Census tracts with ACS demographic data.

---

## Work Completed

### 1. Statewide School Geocoding

**Script:** `etl/school_tract_geocoding.py`

Geocoded all Kentucky schools to Census tracts using the Census Geocoding API:

| Metric | Value |
|--------|-------|
| Schools geocoded | 1,478 |
| Unique tracts | 773 |
| Unique counties | 120 |
| Success rate | 100% |
| Processing time | ~18 minutes |

**Output:** `data/external/school_tracts/school_tracts_statewide_2021.csv`

The geocoding uses school latitude/longitude from `ky-education-portal/src/data/processed/district_school_list.csv` and the Census Geocoding API to return tract FIPS codes.

### 2. Statewide ACS Tract Data

**Script:** `etl/census_acs.py` (added `--statewide` option)

Downloaded Census ACS 5-Year Estimates for all Kentucky tracts:

| Metric | Value |
|--------|-------|
| Total tracts | 1,306 |
| Counties covered | 120 |
| Variables | 8 tract-level metrics |
| Year | 2022 (2018-2022 ACS 5-Year) |

**Output:** `data/external/census_acs/census_acs_tracts_statewide_2022.csv`

Income variation statewide: $10,455 - $216,607 median household income.

### 3. Analysis Pipeline Updates

**Script:** `analysis/create_analysis_dataset.py`

Updated `load_tract_level_data()` to:
- Prefer statewide geocoding file over Fayette-only
- Automatically join with statewide ACS tract data
- Match 98.6% of high school observations (1,609/1,631)

### 4. Model Updates

**Script:** `analysis/bayesian_models/graduation_rate_model.py`

Added 4 new tract-level covariates (total now 8):

| New Covariate | Description |
|---------------|-------------|
| `tract_pct_single_parent` | Family structure indicator |
| `tract_pct_owner_occupied` | Housing stability indicator |
| `tract_pct_broadband` | Digital access indicator |
| `tract_pct_housing_cost_burden_30_plus` | Housing stress indicator |

**Total predictors:** 24 (was 20)

---

## Model Results Comparison

### Convergence Diagnostics

| Metric | Before (Fayette-only tract) | After (Statewide tract) |
|--------|----------------------------|-------------------------|
| Divergences | 0/8000 | 0/8000 |
| R-hat (max) | 1.0175 | 1.0069 |
| ESS bulk (min) | 280 | 336 |
| Posterior predictive | Passed | Passed |

Convergence improved slightly with statewide data (more variation in tract covariates).

### Significant Covariate Effects

| Covariate | Effect | 95% CI | Significant? |
|-----------|--------|--------|--------------|
| tract_poverty_rate | **+1.03** | (+0.22, +1.85) | Yes |
| per_pupil_spending | +0.44 | (+0.24, +0.64) | Yes |
| teacher_avg_experience | +0.36 | (+0.04, +0.68) | Yes |

**Note on tract_poverty_rate:** The positive coefficient is counterintuitive. This may indicate:
1. Schools in higher-poverty tracts may receive additional resources/support
2. Resilience/community factors not captured by other variables
3. Possible multicollinearity with county_poverty_rate (which has negative but non-significant effect)

### Fayette County School Effects

| School | Before | After | Change |
|--------|--------|-------|--------|
| Lafayette HS | +0.26 | **+1.97** | Much higher |
| Frederick Douglass HS | -0.30 | **+1.28** | Now positive |
| Henry Clay HS | -3.56 | -1.90 | Less negative |
| Bryan Station HS | -0.70 | -2.73 | More negative |
| Paul Laurence Dunbar HS | -1.88 | -4.03 | More negative |
| Tates Creek HS | -1.18 | -4.69 | Most negative |

With statewide tract data, Lafayette and Frederick Douglass are now identified as **above expected** (positive effects), while Tates Creek is now identified as most below expected.

---

## Files Created/Modified

### Created
- `data/external/school_tracts/school_tracts_statewide_2021.csv` - 1,478 schools geocoded
- `data/external/census_acs/census_acs_tracts_statewide_2022.csv` - 1,306 tracts

### Modified
- `etl/census_acs.py` - Added `run_all_tracts_statewide()` and `--statewide` CLI option
- `analysis/create_analysis_dataset.py` - Updated `load_tract_level_data()` for statewide data
- `analysis/bayesian_models/graduation_rate_model.py` - Added 4 tract covariates (8 total)
- `analysis/BRIGHT_SPOTS_METHODOLOGY.md` - Updated to version 1.1 with statewide coverage

---

## Recommendations for Next Steps

### High Priority

1. **Investigate tract_poverty_rate positive effect**
   - Run sensitivity analysis excluding this variable
   - Check correlation matrix between county and tract poverty
   - Consider interaction terms

2. **Address ESS_bulk for sigma_district (336 < 400)**
   - Run longer chains (4000 draws instead of 2000)
   - Or tighten prior on sigma_district

3. **Implement LOO-CV**
   - Current implementation missing log_likelihood in trace
   - Add `pm.Deterministic('log_lik', ...)` to model

### Medium Priority

4. **Qualitative validation of Fayette school rankings**
   - Lafayette and Frederick Douglass now show positive effects
   - Tates Creek now shows largest negative effect
   - Compare with stakeholder perceptions and qualitative data

5. **Prior sensitivity analysis**
   - Run `run_sensitivity=True` to test prior robustness
   - Especially for Half-Cauchy scale parameters

### Lower Priority

6. **Extend to other school levels**
   - All 1,478 schools are geocoded (elementary, middle, high)
   - Can now run similar models for other outcomes

7. **Multi-year longitudinal structure**
   - Current model pools all years
   - Consider adding year random effects or trend terms

---

## Commands to Reproduce

```bash
# Geocode all Kentucky schools
python etl/school_tract_geocoding.py --year 2021

# Download statewide ACS tract data
python etl/census_acs.py --statewide --year 2022

# Regenerate analysis dataset
python analysis/create_analysis_dataset.py

# Run graduation rate model
cd analysis/bayesian_models && python graduation_rate_model.py
```

---

## Data Quality Notes

1. **Tract matching rate:** 98.6% (1,609/1,631 high school observations)
   - 22 schools had missing tract income (imputed with statewide mean)

2. **ACS data limitations:**
   - 5-year estimates (2018-2022) - reflects average, not point-in-time
   - Some tracts have missing values for certain variables
   - 23 tracts have missing median_household_income (suppressed for privacy)

3. **Geocoding accuracy:**
   - Uses school centroid coordinates
   - Some schools may span multiple tracts
   - Virtual/online schools may have administrative addresses, not service areas
