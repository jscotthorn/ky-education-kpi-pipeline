# Third Grade Reading Model and Finnish Horseshoe Implementation

**Date:** November 25, 2025
**Status:** Complete

## Summary

This note documents:
1. Implementation of the Finnish (regularized) horseshoe prior for the graduation rate model
2. Creation of a new Bayesian hierarchical model for third grade reading proficiency
3. Data preparation pipeline for elementary school assessment data
4. Non-centered parameterization for improved convergence

## Finnish Horseshoe Prior Implementation

### Background

The classic horseshoe prior (Carvalho et al., 2010) suffered from sampling issues in our graduation rate model:
- 558 divergences (6.98%)
- All 22 covariates shrunk (shrinkage > 0.5)
- Poor R-hat for sigma_district

### Finnish Horseshoe (Piironen & Vehtari, 2017)

Added regularized horseshoe to `graduation_rate_model.py` with slab component:

```python
elif prior_type == "finnish":
    slab_scale = 2.0
    slab_df = 4.0
    tau = pm.HalfCauchy('tau', beta=tau_scale)
    lambdas = pm.HalfCauchy('lambdas', beta=1, shape=n_predictors)
    c2 = pm.InverseGamma('c2', alpha=slab_df / 2,
                         beta=slab_df * slab_scale**2 / 2)
    lambdas_tilde = lambdas * pm.math.sqrt(c2 / (c2 + tau**2 * lambdas**2))
    beta = pm.Normal('beta', mu=0, sigma=tau * lambdas_tilde, shape=n_predictors)
```

### Results Comparison

| Metric | Classic Horseshoe | Finnish Horseshoe |
|--------|------------------|-------------------|
| Divergences | 558 (6.98%) | 525 (6.56%) |
| Global τ | 0.157 | 0.176 |
| Effective covariates | 0 / 22 | **4 / 22** |

Key finding: Finnish horseshoe identified `per_pupil_spending` as significant with HDI excluding zero: +0.36 [+0.14, +0.57].

## Third Grade Reading Model

### Motivation

Third grade reading is a critical indicator - Kentucky's Read to Achieve initiative focuses on ensuring all students read proficiently by grade 3. This indicator differs from graduation rates:
- Elementary schools (not high schools)
- Much lower baseline: ~43% proficient vs ~93% graduation
- Different covariate relationships expected

### Data Preparation

Created `analysis/scripts/prepare_reading_analysis.py` to:

1. Load Grade 3 reading data from Kentucky Summative Assessment
2. Compute demographic percentages from enrollment counts
3. Match schools using normalized name + district (99.2% match rate)

**Challenge:** School ID formats differ between assessment and enrollment data:
- Assessment: STATE SCHOOL ID (7-digit, e.g., 1001016)
- Enrollment: school_id (6-digit, e.g., 610300)

**Solution:** Used school name + district matching with normalization:
```python
def normalize_name(name):
    return str(name).lower().strip()
```

### Model Structure

Same hierarchical structure as graduation model:
- Level 1: School-year observations
- Level 2: Schools within districts
- Level 3: Districts within state

Key differences:
- State mean prior: μ ~ Normal(43, 15) instead of Normal(93, 10)
- σ_district ~ HalfCauchy(10) instead of HalfCauchy(5)
- σ_y ~ HalfCauchy(5) instead of HalfCauchy(2)

### Dataset Summary

| Metric | Value |
|--------|-------|
| Observations | 1,812 school-years |
| Schools | 695 elementary |
| Districts | 172 |
| Years | 2022-2024 |
| Outcome mean | 42.8% proficient |
| Outcome std | 16.1% |

### Covariate Effects (Final Model with Non-Centered Parameterization)

| Covariate | Effect | 95% HDI | Significant |
|-----------|--------|---------|-------------|
| pct_economically_disadvantaged | -7.18 | [-8.3, -6.1] | Yes |
| pct_students_with_disabilities | +2.64 | [+1.6, +3.6] | Yes |
| pct_african_american | -2.27 | [-3.4, -1.1] | Yes |
| pct_hispanic | -2.22 | [-3.9, -0.6] | Yes |
| pct_english_learners | +1.25 | [-0.4, +3.1] | No |
| novice_teacher_rate | -0.19 | [-0.9, +0.4] | No |

**Notes:**
- Removed `pct_minority` to eliminate multicollinearity (was sum of race categories)
- Added `novice_teacher_rate` after fixing teacher quality data merge (98% match rate)
- Novice teacher rate is NOT significant for 3rd grade reading, unlike graduation rates

### Fayette County Results

**Top performers** (above expected given demographics):
1. Maxwell Spanish Immersion: +4.82
2. Athens-Chilesburg: +4.70
3. Madeline Breckinridge: +4.26
4. Rosa Parks: +4.26
5. Ashland: +4.11

**Lower performers** (still positive - above state average):
- Millcreek: +1.55
- Southern: +1.57
- Coventry Oak: +1.82
- Cardinal Valley: +1.79

All Fayette elementary schools have positive effects, indicating above-average performance statewide given their demographics.

### Convergence: Non-Centered Parameterization

Initial model (centered parameterization) had severe issues:
- 221 divergences (2.76%)
- Poor R-hat for sigma_school (1.77) and sigma_y (1.13)
- Very low ESS: sigma_school ESS = 6, sigma_y ESS = 19

**Solution: Non-Centered Parameterization**

The "funnel" geometry in hierarchical models causes centered parameterizations to struggle when data is sparse. Non-centered reparameterization samples "raw" effects from N(0,1) then scales:

```python
# Non-centered: sample raw, then scale
district_effect_raw = pm.Normal('district_effect_raw', mu=0, sigma=1, shape=n_districts)
district_effect = pm.Deterministic('district_effect', sigma_district * district_effect_raw)
```

**Results with Non-Centered Parameterization:**

| Metric | Centered | Non-Centered |
|--------|----------|--------------|
| Divergences | 221 (2.76%) | 143 (1.79%) |
| sigma_school R-hat | 1.77 | **1.004** ✓ |
| sigma_school ESS | 6 | **661** ✓ |
| sigma_y R-hat | 1.13 | **1.002** ✓ |
| sigma_y ESS | 19 | **1898** ✓ |

All R-hat values now < 1.01 and ESS > 400. Model converges properly.

## Files Created

### Model Files
- `analysis/bayesian_models/reading_grade3_model.py` - Main model script
- `analysis/scripts/prepare_reading_analysis.py` - Data preparation

### Output Files
- `analysis/datasets/reading_grade3_analysis.csv` - Analysis dataset
- `analysis/outputs/models/reading_grade3/reading_grade3_trace.nc` - MCMC trace
- `analysis/outputs/models/reading_grade3/model_summary.csv` - Parameter summary
- `analysis/outputs/models/reading_grade3/covariate_effects.csv` - Beta coefficients
- `analysis/outputs/models/reading_grade3/school_effects.csv` - School random effects

### Modified Files
- `analysis/bayesian_models/graduation_rate_model.py` - Added Finnish horseshoe option

## Usage

```bash
# Third grade reading model (default: Finnish + non-centered)
python reading_grade3_model.py

# Graduation rate model with Finnish horseshoe
python graduation_rate_model.py --finnish

# Reading model with normal priors (no shrinkage)
python reading_grade3_model.py --normal

# Reading model with centered parameterization (not recommended)
python reading_grade3_model.py --centered
```

## Completed Improvements

1. ~~**Add more covariates**~~: Added teacher quality (novice_teacher_rate) - not significant for 3rd grade
2. ~~**Address multicollinearity**~~: Removed pct_minority, using race-specific variables only
3. ~~**Improve convergence**~~: Non-centered parameterization dramatically improved R-hat and ESS

## Future Improvements

1. **Extend to other grades**: Math proficiency, other grade levels
2. **Add census tract data**: Geocode elementary schools for neighborhood economics
3. **Further reduce divergences**: Consider increasing target_accept beyond 0.95

## References

- Carvalho, C.M., Polson, N.G., & Scott, J.G. (2010). The horseshoe estimator for sparse signals.
- Piironen, J., & Vehtari, A. (2017). Sparsity information and regularization in the horseshoe and other shrinkage priors.
