# Bayesian Model Expansion - November 2024

## Summary

Expanded the Bayesian hierarchical model suite from 5 to 11 models covering additional Kentucky education KPIs. This note documents the implementation, findings, and recommendations for model usage.

## Models Implemented

### Previously Existing (5 models)
1. `graduation` - 4-year graduation rate (high schools)
2. `kindergarten_readiness` - School readiness scores (elementary)
3. `math_grade8` - 8th grade math proficiency
4. `postsecondary_enrollment` - College enrollment rates (high schools)
5. `reading_grade3` - 3rd grade reading proficiency

### New Models (6 models)
6. `postsecondary_readiness` - CCR readiness rate (high schools)
7. `chronic_absenteeism` - Chronic absenteeism rate (all schools)
8. `el_progress_elementary` - English Learner progress (elementary)
9. `el_progress_middle` - English Learner progress (middle schools)
10. `el_progress_high` - English Learner progress (high schools)
11. `school_climate` - School climate index (all schools)

## Combined Output

**Location:** `data/bayesian/bayesian_results.json`

**Contents:**
- 11 models
- 5,426 school effects
- 244 covariate effects

## Model Quality Assessment

### High Confidence Models

| Model | Schools | Effective Covariates | Top Predictors |
|-------|---------|---------------------|----------------|
| chronic_absenteeism | 1,353 | 16/23 | county_poverty_rate (+4.9), tract_pct_bachelors_plus (-1.6) |
| school_climate | 1,152 | 11/16 | tract_pct_owner_occupied (+1.4), pct_students_with_disabilities (+1.3) |
| reading_grade3 | 702 | 23/23 | - |
| kindergarten_readiness | 693 | 23/23 | - |
| math_grade8 | 336 | 23/23 | - |
| graduation | 228 | varies | - |
| postsecondary_readiness | 228 | varies | emergency_provisional_rate, teacher_avg_experience |
| postsecondary_enrollment | 215 | varies | - |
| el_progress_elementary | 339 | **6/23** | per_pupil_spending (+0.9), student_teacher_ratio (-0.5) |

### Low Confidence Models (Interpret with Caution)

| Model | Schools | Effective Covariates | Issue |
|-------|---------|---------------------|-------|
| el_progress_middle | 96 | **0/23** | Extreme shrinkage - no useful covariate effects |
| el_progress_high | 84 | **1/23** | Extreme shrinkage - essentially uninformative |

## Key Findings

### 1. EL Progress: Fundamentally Different Across Levels

The EL progress metric shows dramatically different values by school level:

| Level | Schools | Mean Score | Std |
|-------|---------|------------|-----|
| Elementary | 508 | **12.7** | 6.4 |
| Middle | 169 | **4.7** | 3.5 |
| High | 159 | **4.9** | 4.4 |

**Interpretation:** Elementary scores are ~3x higher than middle/high. This reflects the different stages of language acquisition - elementary students show more measurable progress toward the 140-point proficiency threshold.

**Recommendation:** Keep models separate (don't combine). Use elementary model for covariate insights. Flag middle/high as "low confidence" or remove from combined results.

### 2. Chronic Absenteeism: Combined Model Works Well

Unlike EL progress, chronic absenteeism has consistent meaning across school levels. The combined model (89% A1 regular schools) produced strong results:

- 1,353 schools across all levels
- 16 effective covariates
- Clear predictor patterns (poverty, education levels)

**Recommendation:** Keep combined. Splitting by level would reduce sample size without clear benefit.

### 3. School Climate Data Issue

The school climate model showed "1 district" in the output, suggesting a data issue with district mapping. The model still produced useful school effects (1,152 schools) but the district-level hierarchy may not be functioning correctly.

**Action needed:** Investigate school_climate_analysis.py to verify district_number is being properly extracted and mapped.

## Technical Notes

### Model Architecture
- All models use Finnish horseshoe priors with non-centered parameterization
- 23 predictors (demographics, teacher quality, financial, census tract/county)
- NUTS sampler: 4000 draws, 2000 tune, target_accept=0.95

### Runtime
- Small models (EL middle/high): 2-3 minutes
- Medium models (EL elementary): ~5 minutes
- Large models (chronic absenteeism, school climate): 38-49 minutes

### Convergence
Most models showed some divergences but passed R-hat and ESS checks. Posterior predictive checks passed for all models.

## Files Created

### Analysis Scripts
- `analysis/scripts/postsecondary_readiness_analysis.py`
- `analysis/scripts/chronic_absenteeism_analysis.py`
- `analysis/scripts/el_progress_elementary_analysis.py`
- `analysis/scripts/el_progress_middle_analysis.py`
- `analysis/scripts/el_progress_high_analysis.py`
- `analysis/scripts/school_climate_analysis.py`

### Bayesian Models
- `analysis/bayesian_models/postsecondary_readiness_model.py`
- `analysis/bayesian_models/chronic_absenteeism_model.py`
- `analysis/bayesian_models/el_progress_elementary_model.py`
- `analysis/bayesian_models/el_progress_middle_model.py`
- `analysis/bayesian_models/el_progress_high_model.py`
- `analysis/bayesian_models/school_climate_model.py`

### Output Files (per model)
- `analysis/outputs/models/{model_name}/school_effects.csv`
- `analysis/outputs/models/{model_name}/covariate_effects.csv`
- `analysis/outputs/models/{model_name}/{model_name}_trace.nc`
- `analysis/outputs/models/{model_name}/model_summary.csv`
- `analysis/outputs/diagnostics/{model_name}/posterior_predictive_check.png`

## Recommendations for Future Work

1. **Consider removing EL middle/high** from combined JSON or adding confidence flags
2. **Investigate school climate district mapping** issue
3. **Consider level-specific chronic absenteeism** if interpretability by level becomes important (data supports it)
4. **Add model metadata** to combined JSON including:
   - Effective covariate count
   - Divergence rate
   - Sample size (schools/observations)
   - Confidence rating

## Date
November 26, 2024
