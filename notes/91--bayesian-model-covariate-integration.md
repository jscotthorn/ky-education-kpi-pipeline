# Bayesian Model Covariate Integration

**Date:** November 24, 2025
**Status:** Implemented
**Topic:** Integration of Teacher Quality, Financial, and Census Data into Hierarchical Bayesian Model

## Overview

Updated the Hierarchical Bayesian bright spots model to incorporate newly implemented KPI pipelines from notes 82-89. The model now includes 16 predictors across 4 categories, up from 6 demographic predictors in the initial implementation.

## Integrated Covariates

### Category 1: Student Demographics (6 predictors - existing)
| Predictor | Source | Description |
|-----------|--------|-------------|
| `pct_economically_disadvantaged` | KPI Master | % students qualifying for free/reduced lunch |
| `pct_english_learners` | KPI Master | % English language learners |
| `pct_students_with_disabilities` | KPI Master | % students with IEPs |
| `pct_african_american` | KPI Master | % African American students |
| `pct_hispanic` | KPI Master | % Hispanic/Latino students |
| `pct_minority` | KPI Master | % non-white students |

### Category 2: Teacher Quality (5 predictors - NEW)
| Predictor | Source | Description |
|-----------|--------|-------------|
| `novice_teacher_rate` | novice_teachers.csv | % teachers with <3 years experience |
| `teacher_avg_experience` | teacher_experience.csv | Average years of teaching experience |
| `student_teacher_ratio` | student_teacher_ratio.csv | Students per FTE teacher |
| `teacher_turnover_rate` | teacher_turnover.csv | Annual teacher turnover % |
| `emergency_provisional_rate` | teacher_certification.csv | % on emergency/provisional certification |

### Category 3: Financial Resources (2 predictors - NEW)
| Predictor | Source | Description |
|-----------|--------|-------------|
| `per_pupil_spending` | spending_per_student.csv | Total spending per student (district-level) |
| `students_per_certified_staff` | financial_summary.csv | Students per certified staff FTE |

### Category 4: Census Economic Context (3 predictors - NEW)
| Predictor | Source | Description |
|-----------|--------|-------------|
| `county_median_income` | census_saipe_combined.csv | County median household income |
| `county_poverty_rate` | census_saipe_combined.csv | County poverty rate (all ages) |
| `county_child_poverty_rate` | census_saipe_combined.csv | County child poverty rate (5-17) |

## Files Modified

### `analysis/create_analysis_dataset.py`
- Added `load_teacher_quality_metrics()` function to extract school-level teacher metrics
- Added `load_financial_metrics()` function to extract district-level financial data
- Added `load_census_data()` function to load county-level Census SAIPE data
- Updated `create_analysis_dataset()` to merge all data sources
- Updated `validate_dataset()` to report on all covariate categories

### `analysis/bayesian_models/graduation_rate_model.py`
- Updated `load_and_prepare_data()` to include all 16 predictors across 4 categories
- Added `predictor_categories` dict to track predictor groupings
- Updated `save_results()` to output `covariate_effects.csv` with category labels
- Added effect significance indicators (*** for 95% CI excluding zero)

## Data Pipeline Summary

```
Processed KPI Files → create_analysis_dataset.py → graduation_analysis.csv → graduation_rate_model.py
         ↑                       ↑
   KDE Pipelines          Census SAIPE
   (Notes 82-88)          (Note 89)
```

## Analysis Dataset Output

**File:** `analysis/datasets/graduation_analysis.csv`
- **Rows:** 1,631 school-year observations
- **Columns:** 38 (including 16 model predictors)
- **Schools:** 228 Type A1 high schools
- **Years:** 2021-2024

### Covariate Coverage
| Category | Available/Expected |
|----------|-------------------|
| Demographics | 6/6 (100%) |
| Teacher Quality | 5/5 (100%) |
| Financial | 2/2 (100%) |
| Census | 3/3 (100%) |
| **Total** | **16/16 (100%)** |

## Expected Model Improvements

Based on literature (see BRIGHT_SPOTS_ADDITIONAL_COVARIATES.md):

1. **Teacher Quality Predictors:**
   - Novice teacher rate: Expected -1.0 to -1.5 pp per 10% increase
   - Teacher experience: Expected +0.3 to +0.5 pp per year
   - Turnover rate: Indicator of school stability

2. **Financial Predictors:**
   - Per-pupil spending: Expected +0.5 to +1.5 pp per $1,000 increase
   - Addresses resource inequity not captured by demographics alone

3. **Census Economic Predictors:**
   - Captures community context beyond free/reduced lunch eligibility
   - County income variation more stable than individual FRL status
   - Expected improvement in R² from 0.3-0.4 to 0.6-0.8

## Next Steps

1. **Run Updated Model:** Execute `graduation_rate_model.py` with new predictors
2. **Convergence Check:** Verify R-hat < 1.01 and ESS > 400 for all parameters
3. **Covariate Analysis:** Interpret new predictor effects
4. **Bright Spots Re-identification:** Update school rankings with improved model
5. **Expand to Other Indicators:** Apply same framework to:
   - Postsecondary readiness
   - Math/Reading proficiency
   - Chronic absenteeism

## Related Notes

- Note 82: Implementing Teacher Quality KPIs
- Note 83: Spending Per Student Pipeline Implementation
- Note 84: Teacher Quality Data Investigation
- Note 85: Teacher Quality Pipelines Implementation
- Note 86: Financial Summary Pipeline Implementation
- Note 89: Census External Data Pipelines Implementation
- Note 90: Bayesian Model Implementation Log (initial implementation)

## Alignment with Methodology Documents

This implementation addresses priorities from:

**BRIGHT_SPOTS_DATA_PIPELINE_PRIORITIES.md:**
- ✅ Tier 1: Per-pupil spending, Novice teacher rate, Student-teacher ratio, Teacher experience
- ✅ Tier 1: Census SAIPE (median income, poverty)
- ✅ Tier 2: Teacher turnover, Emergency certification

**BRIGHT_SPOTS_ADDITIONAL_COVARIATES.md:**
- ✅ Economic indicators beyond FRL (county median income, poverty)
- ✅ School resource measures (per-pupil spending, staffing)
- ✅ Teacher quality (experience, certification, turnover)
