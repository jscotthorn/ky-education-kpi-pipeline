# 98 - Bayesian Model Base Class and Tract Data Fix

**Date:** November 25, 2025

## Summary

Created a base class architecture for Bayesian hierarchical models and fixed a critical school_id type mismatch that was preventing tract-level data from being merged into analysis datasets.

## Changes Made

### 1. Bayesian Model Base Class (`base_hierarchical_model.py`)

Created `analysis/bayesian_models/base_hierarchical_model.py` (~700 lines) containing all shared logic:

- Path setup and configuration
- Data loading and preparation
- Predictor standardization
- PyMC model building (normal, horseshoe, Finnish horseshoe priors)
- Non-centered parameterization support
- MCMC sampling
- Convergence diagnostics (R-hat, ESS, divergences)
- Prior/posterior predictive checks
- LOO cross-validation
- Results saving (traces, summaries, school effects)

Abstract methods subclasses must implement:
```python
@property
def OUTCOME_NAME(self) -> str: ...
@property
def MODEL_NAME(self) -> str: ...
def get_state_mean_prior(self) -> Tuple[float, float]: ...
def get_variance_priors(self) -> Dict[str, float]: ...
def get_tau_scale(self) -> float: ...
def get_slab_parameters(self) -> Tuple[float, float]: ...
def get_tract_columns(self) -> List[str]: ...
def get_model_description(self) -> str: ...
```

### 2. Refactored Model Subclasses

**`graduation_rate_model.py`** (refactored from ~1250 to 115 lines):
- State mean prior: (93.0, 10.0) - KY average ~93%
- Tighter variance priors (high rates, low variance)
- Tau scale: 0.23 (~5 expected effective predictors)
- 6 tract columns (removed broadband, housing cost burden)

**`reading_grade3_model.py`** (refactored from ~600 to 124 lines):
- State mean prior: (43.0, 15.0) - KY average ~43%
- Wider variance priors (more variation across schools)
- Tau scale: 0.5 (more lenient shrinkage)
- All 8 tract columns included

### 3. Tract Data Merge Fix

**Problem:** school_id type mismatch prevented tract data from merging.

| Source | Format | After astype(str) |
|--------|--------|-------------------|
| KPI master | `1010.0` (float64) | `"1010.0"` |
| Tract file | `1010` (int64) | `"1010"` |

Result: No match → 0% tract coverage for graduation dataset.

**Fix:** Added normalization in `base_analysis_dataset.py:841-850`:
```python
def normalize_school_id(x):
    if pd.isna(x):
        return None
    return str(int(float(x)))

merged['school_id'] = merged['school_id'].apply(normalize_school_id)
tract_df['school_id'] = tract_df['school_id'].apply(normalize_school_id)
```

### 4. Regenerated Analysis Datasets

All datasets now have 100% tract coverage:

| Dataset | Rows | Tract Coverage |
|---------|------|----------------|
| Graduation | 1,631 | 100% |
| Reading Grade 3 | 3,262 | 100% |
| Kindergarten | 4,062 | 100% |

## Model Results (Post-Fix)

### Graduation Rate Model
- 21 predictors (including 6 tract-level)
- 4 effective covariates after Finnish horseshoe shrinkage
- Top effects:
  - `pct_african_american`: -0.47 (strongest)
  - `per_pupil_spending`: +0.37
  - `county_poverty_rate`: -0.28
  - `teacher_avg_experience`: +0.28

### Reading Grade 3 Model
- 23 predictors (including 8 tract-level)
- Global shrinkage tau=10.1 (not shrinking effectively - needs tuning)
- Top effects:
  - `pct_economically_disadvantaged`: -6.22 (strongest)
  - `pct_students_with_disabilities`: +2.58
  - `per_pupil_spending`: +2.46
  - `emergency_provisional_rate`: -2.25
  - `tract_pct_bachelors_plus`: +2.18

## Files Modified

- `analysis/bayesian_models/base_hierarchical_model.py` (NEW)
- `analysis/bayesian_models/graduation_rate_model.py` (refactored)
- `analysis/bayesian_models/reading_grade3_model.py` (refactored)
- `analysis/scripts/base_analysis_dataset.py` (school_id normalization fix)

## Deleted Files

- `analysis/create_analysis_dataset.py` (replaced by base class)
- `analysis/scripts/prepare_reading_analysis.py` (replaced by subclass)

## Next Steps

1. Tune Finnish horseshoe tau parameter for reading model (currently not shrinking)
2. Address convergence warnings (divergences ~6-7%)
3. Create kindergarten readiness Bayesian model using new base class
4. Run full model comparison across outcomes
