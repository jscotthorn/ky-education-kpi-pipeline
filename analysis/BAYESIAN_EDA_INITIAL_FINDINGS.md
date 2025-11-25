# Bayesian Bright Spots EDA - Initial Findings

**Date:** November 24, 2025  
**Status:** Planning Complete, Data Quality Issue Identified

## Summary

Completed initial planning and research for Bayesian bright spots exploratory data analysis. Created implementation plan and EDA script based on research findings. Encountered data quality issue in KPI master file that needs resolution before proceeding with full analysis.

## Research Findings

### Bayesian Hierarchical Models for Education Equity

Based on web research, Bayesian hierarchical models are ideal for this use case:

**Key Advantages:**
- **Partial Pooling**: Small-sample groups "borrow strength" from larger groups and overall population
- **Shrinkage**: Estimates for small samples are pulled toward population mean, preventing overfitting
- **Handles Nested Data**: Students → Schools → Districts → Regions → State
- **Informative Priors**: Can use statewide Kentucky data (~1,200 schools) as priors for Fayette County estimates
- **Honest Uncertainty**: Provides credible intervals reflecting small sample limitations

**Critical for Our Use Case:**
- With only **6 high schools** in Fayette County, standard regression is statistically invalid
- Hierarchical Bayesian is **the only valid approach** for n=6 samples
- Requires informative priors (we have ~1,200 KY schools statewide)
- Highest-level sample size is most critical (we meet this requirement)

### Recommended Outcome KPIs

Based on BRIGHT_SPOTS_BAYESIAN_APPROACH.md and KPIS.md:

**High Schools (n=6 in Fayette):**
- `graduation_rate_4_year`
- `postsecondary_readiness_rate`
- `postsecondary_enrollment_total_ky_college_rate`

**Middle Schools (n<20 in Fayette):**
- `kentucky_summative_assessment_math_proficient_distinguished_rate_grade_8`

**Elementary Schools (n~35-40 in Fayette):**
- `kentucky_summative_assessment_reading_proficient_distinguished_rate_grade_3`

**All Levels:**
- `chronic_absenteeism_rate_all_grades` (inverse outcome)

### Recommended Predictor Variables

**Demographics (from enrollment by student_group):**
- % Economically Disadvantaged
- % English Learners
- % Students with Disabilities
- % African American
- % Hispanic

**School Context:**
- School size (`student_enrollment_total`)
- Title I status
- Urban/Rural classification
- School type (elementary/middle/high)
- Region (Bluegrass, Appalachia, etc.)

**Teacher Quality (optional covariates):**
- `teacher_average_years_experience`
- `student_teacher_ratio`
- `teacher_turnover_rate`

**Resources (optional covariates):**
- `total_spending_per_student_all_funds`
- `climate_index_score`
- `safety_index_score`

## Data Quality Issue

### CSV Parsing Error

**Error:** `pandas.errors.ParserError: Error tokenizing data. C error: Expected 19 fields in line 4736384, saw 20`

**Location:** KPI master file at row ~4.7M (out of 33.4M total rows)

**Impact:** Cannot load full dataset for analysis until resolved

**Possible Causes:**
1. Unescaped delimiter in a text field (e.g., comma in school name)
2. Inconsistent quoting in source data
3. Data corruption during ETL processing
4. Newline character in a text field

**Recommended Resolution:**
1. Identify the problematic row: `sed -n '4736384p' data/kpi/kpi_master.csv`
2. Determine which ETL pipeline produced it (check `source_file` column)
3. Fix the source ETL pipeline to properly escape/quote fields
4. Regenerate KPI master file

## Completed Work

### 1. Implementation Plan ✅

Created implementation_plan.md with:
- Research findings on Bayesian hierarchical models
- KPI selection criteria
- Data quality assessment plan
- Hierarchical structure analysis (ICC calculations)
- Correlation analysis approach
- Visualization requirements

### 2. EDA Script ✅

Created bayesian_bright_spots_eda.py with:
- Sample size analysis by school level
- ICC calculation for hierarchical structure assessment
- Fayette County vs. statewide comparison framework
- Visualization generation (sample sizes, ICC values)
- Error handling for empty dataframes

### 3. Environment Setup ✅

- Created virtual environment (`.venv`)
- Installed dependencies (pandas, matplotlib, seaborn, scipy)
- Regenerated KPI master file (33.4M rows from 26 sources)

## Next Steps

### Immediate (Before EDA)

1. **Fix CSV parsing error**:
   - Identify problematic row in KPI master file
   - Trace back to source ETL pipeline
   - Fix quoting/escaping in source pipeline
   - Regenerate KPI master file

2. **Test EDA script on smaller sample**:
   - Load first 1M rows (before error point)
   - Verify script logic works correctly
   - Check if target KPIs are present in early rows

### After Data Quality Fix

3. **Run full EDA analysis**:
   - Load 5-10M row sample (or full file if memory allows)
   - Generate sample size tables by school level
   - Calculate ICC values for hierarchical structure
   - Create visualizations

4. **Derive demographic percentages**:
   - Use `student_enrollment` KPIs with `student_group` column
   - Calculate % for each demographic at school level
   - Merge with outcome KPIs for analysis dataset

5. **Create analysis dataset**:
   - Combine outcome KPIs + predictors + school metadata
   - One row per school-year combination
   - Ready for Bayesian modeling

## Files Created

- `implementation_plan.md` (artifact)
- `task.md` (artifact)
- `analysis/bayesian_bright_spots_eda.py`
- `analysis/bayesian_eda_outputs/` (directory)
- `analysis/BAYESIAN_EDA_INITIAL_FINDINGS.md` (this file)
