# Statewide Bright Spots Analysis Implementation

**Date:** 2025-11-23
**Purpose:** Move Bright Spots regression analysis from FCPS-only dashboard to statewide KPI pipeline to achieve statistically valid sample sizes

## Background

### Critical Sample Size Issue

The current fcps-equity-dashboard implementation (scripts/bright-spots.ts) has a **fundamental statistical error**:

```typescript
// CURRENT (INCORRECT):
const totalStudentsInDemographic = validSchools.reduce((sum, school) => {
    const count = school.indicators['enrollment'][demographic] as number;
    return sum + (count || 0);
}, 0);

if (totalStudentsInDemographic < MIN_STUDENTS_FOR_REGRESSION) return [];
```

**Problem:** This counts **students** (e.g., 58 students across 3 schools) when regression requires **observations** (e.g., 58 schools as data points).

Green (1991) requires **N ≥ 58 schools** as observations, not 58 students total.

### Current Limitations

**Fayette County District-Level Analysis:**
- Elementary schools: ~35-40
- Middle schools: ~12-15
- High schools: ~8-10
- Total: ~50-65 schools

**Sample Size Reality:**
- Only 2-3 indicators meet N ≥ 40 threshold within Fayette County (Climate Index, Chronic Absenteeism, possibly Kindergarten Readiness)
- Grade-specific indicators (8th grade math) and high school indicators fail to meet threshold
- District-level analysis not feasible for most indicators

**Solution:** Run regression analysis on **statewide Kentucky data stratified by school type**, then filter/highlight Fayette County schools in the dashboard.

**Stratification Strategy:**
- Elementary indicators → KY elementary schools (N ~800)
- Middle indicators → KY middle schools (N ~300)
- High indicators → KY high schools (N ~200)
- District-wide indicators → All KY schools (N ~1,200)

**Trade-Off:**
This pools schools across districts, which may conflate district-level factors (funding, policies) with school-level performance. However, stratifying by school type maintains important contextual similarities (grade levels, curriculum, student developmental stage) and provides statistically valid sample sizes.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│ ky-education-kpi-pipeline                                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  data/kpi/kpi_master.csv (10GB, all KY schools, years 2021-2025)│
│           ↓                                                       │
│  analysis/bright_spots.py ← NEW PYTHON SCRIPT                    │
│           ↓                                                       │
│  - Loads statewide KPI data stratified by school type:           │
│    • Elementary schools (N ~800)                                 │
│    • Middle schools (N ~300)                                     │
│    • High schools (N ~200)                                       │
│    • All schools for district-wide indicators (N ~1,200)         │
│  - Runs correlation validation within each stratum (2021-2025)   │
│  - Performs regression analysis by indicator/subgroup/stratum    │
│  - Identifies bright spots (Z > 1.0)                              │
│  - Outputs: analysis/bright_spots_statewide.json                 │
│           ↓                                                       │
│  - Generates: fcps-equity-dashboard/src/assets/bright-spots.json │
│    (filtered to Fayette County schools only)                     │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────────┐
│ fcps-equity-dashboard                                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  src/assets/bright-spots.json (Fayette County filtered results)  │
│           ↓                                                       │
│  - Frontend consumes pre-computed bright spots                   │
│  - No client-side regression computation                          │
│  - Visualizes Fayette County schools with statewide context      │
│  - Shows school type stratification in results                   │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Indicator Mapping

### Dashboard Indicators → KPI Metrics

| Dashboard Indicator | KPI Metric(s) | Notes |
|---------------------|---------------|-------|
| `kindergarten_readiness` | `kindergarten_readiness_rate` | Direct match |
| `reading_3rd_grade` | TBD | Need to identify in KSA data |
| `math_8th_grade` | TBD | Need to identify in KSA data |
| `9th_grade_on_track` | N/A | FCPS-specific, not in state data |
| `graduation_rate` | `graduation_rate_4_year` | Direct match |
| `postsecondary_readiness` | `postsecondary_readiness_rate_with_bonus` | Use bonus version |
| `postsecondary_enrollment` | `postsecondary_enrollment_total_ky_college_rate` | Total KY enrollment |
| `climate_index_score_calculated` | `climate_index_score_calculated` | Direct match |
| `chronic_absenteeism` | `chronic_absenteeism_rate_all_grades` | All grades aggregated |

### Student Groups (Standardized)

KPI `student_group` column already uses standardized names:
- `"All Students"`
- `"Economically Disadvantaged"`
- `"African American"`
- `"Hispanic or Latino"`
- `"English Learner"`
- `"Students with Disabilities (IEP)"`
- `"White (non-Hispanic)"`
- `"Two or More Races"`

---

## Implementation Plan

### Phase 1: Create Python Bright Spots Analysis Script

**File:** `ky-education-kpi-pipeline/analysis/bright_spots.py`

**Key Components:**

1. **Load Statewide KPI Data with School Type Stratification**
   ```python
   import pandas as pd

   # Load full statewide data
   kpi_df = pd.read_csv('data/kpi/kpi_master.csv', low_memory=False)

   # Get school-level enrollment for demographic %
   enrollment_df = kpi_df[kpi_df['metric'].str.contains('enrollment')].copy()

   # Determine school type from school_type column or indicator
   def get_school_type(row):
       """Classify school as Elementary/Middle/High based on school_type field."""
       school_type = row.get('school_type', '')
       if 'A1' in school_type or 'Elementary' in school_type:
           return 'Elementary'
       elif 'A2' in school_type or 'Middle' in school_type:
           return 'Middle'
       elif 'A5' in school_type or 'High' in school_type:
           return 'High'
       return 'Other'
   ```

2. **Correlation Validation (Step 1) - Stratified by School Type**
   - For each indicator/subgroup combination
   - Filter to appropriate school type stratum
   - Calculate Pearson r for years 2021-2025 within stratum
   - Validate |r| ≥ 0.5 sustained over 5 years
   - **Use school count (N observations), not student count**
   - **Require N ≥ 58 schools in stratum for validation**

3. **Regression Analysis (Step 2) - Stratified by School Type**
   - For validated combinations only
   - Filter to appropriate school type stratum (Elementary/Middle/High)
   - Independent variable (X): % of students in subgroup within school
   - Dependent variable (Y): Subgroup performance score
   - Calculate residuals, z-scores within stratum
   - Identify bright spots (Z > 1.0)

4. **Multi-Year Trend Analysis**
   - Track schools appearing as bright spots 2021-2025
   - Categorize: Sustained Excellence (3+ years), Rising Star, Current
   - Maintain school type context in trend data

5. **Output Generation**
   - `analysis/bright_spots_statewide.json` - Full statewide results with school type stratification
   - `../fcps-equity-dashboard/src/assets/bright-spots.json` - Fayette County filtered
   - `analysis/correlation_validation.json` - 5-year correlation data by school type
   - `analysis/BRIGHT_SPOTS_RESULTS.md` - Markdown report with stratification details

### Phase 2: Modify Dashboard to Consume Pre-Computed Data

**Changes to fcps-equity-dashboard:**

1. **Remove TypeScript Analysis Scripts**
   - Archive `scripts/bright-spots.ts`
   - Archive `scripts/correlation-analysis.ts`
   - Archive `scripts/equity-audit.ts`

2. **Update Data Extraction**
   - Remove client-side regression computation
   - Consume `src/assets/bright-spots.json` (pre-computed)
   - Add context: "Based on statewide Kentucky analysis (N=~1,200 schools)"

3. **Visualization Enhancements**
   - Show Fayette County schools highlighted on statewide regression plots
   - Display sample size: "Analysis based on N=X Kentucky schools"
   - Add disclaimer about statewide context

### Phase 3: Update Methodology Documentation

Update `fcps-equity-dashboard/BRIGHT_SPOTS_METHODOLOGY.md`:

1. **Correct Sample Size Definition (CRITICAL FIX)**
   ```markdown
   ### Minimum Sample Size Requirement

   Regression analysis requires adequate sample sizes to produce reliable,
   statistically valid results. Green (1991) recommends a minimum of
   **N ≥ 50 + 8m** for testing the overall regression model, where m is
   the number of predictors. With one predictor (demographic percentage),
   this suggests a minimum of **58 schools** as observations.

   **Implementation:**
   We require at least **58 schools** with valid data for the target
   demographic. This is a count of schools (observations), not students.

   **Statewide Analysis:**
   To meet this requirement, we perform regression analysis on statewide
   Kentucky data (~1,200 schools), then filter results to identify
   Fayette County bright spots within this broader context.
   ```

2. **Add Statewide Context Section**
   ```markdown
   ## Statewide Analysis Approach

   While this dashboard focuses on Fayette County Public Schools, the
   regression analysis is performed on **statewide Kentucky data** to
   ensure statistically valid sample sizes (N ≥ 58 schools).

   **Benefits:**
   - Valid regression models with sufficient observations
   - Robust trend lines representing Kentucky-wide patterns
   - Identification of schools beating statewide expectations
   - Ability to compare Fayette County schools to state context

   **Interpretation:**
   Fayette County schools identified as "Bright Spots" are performing
   significantly better than predicted based on statewide Kentucky
   patterns, not just within Fayette County.
   ```

---

## Data Schema

### Input: kpi_master.csv

```csv
year,metric,district,school_name,student_group,value,suppressed,county_number,county_name,district_number,school_id,...
2025,kindergarten_readiness_rate,Fayette County,Ashland Elementary,All Students,65.2,N,161,Fayette,175,210090000123,...
2025,kindergarten_readiness_rate,Fayette County,Ashland Elementary,Economically Disadvantaged,42.1,N,161,Fayette,175,210090000123,...
2025,enrollment_count_grade_k,Fayette County,Ashland Elementary,All Students,125,N,161,Fayette,175,210090000123,...
2025,enrollment_count_grade_k,Fayette County,Ashland Elementary,Economically Disadvantaged,78,N,161,Fayette,175,210090000123,...
```

### Output: bright_spots.json (Fayette County Filtered)

```json
{
  "kindergarten_readiness": {
    "Economically Disadvantaged": {
      "regression": {
        "slope": -0.45,
        "intercept": 75.2,
        "r": -0.529,
        "n_schools": 856
      },
      "brightSpots": [
        {
          "schoolName": "Deep Springs Elementary School",
          "schoolId": "210090000456",
          "category": "current_bright_spot",
          "actualScore": 56.0,
          "predictedScore": 27.3,
          "residual": 28.7,
          "zScore": 2.15,
          "demographicPct": 85.3,
          "trendData": {
            "yearsAsBrightSpot": ["2025"],
            "consistencyScore": 0.2,
            "residualSlope": 0.0
          }
        }
      ],
      "points": [
        {
          "schoolName": "Ashland Elementary",
          "schoolId": "210090000123",
          "x": 62.4,
          "y": 42.1,
          "isFayette": true
        }
      ]
    }
  }
}
```

### Output: correlation_validation.json

```json
{
  "kindergarten_readiness": {
    "Economically Disadvantaged": {
      "correlations": {
        "2021": -0.678,
        "2022": -0.692,
        "2023": -0.474,
        "2024": -0.432,
        "2025": -0.529
      },
      "avg_r": -0.561,
      "strength": "Moderate",
      "validated": true,
      "n_schools_by_year": {
        "2021": 823,
        "2022": 845,
        "2023": 867,
        "2024": 891,
        "2025": 856
      }
    }
  }
}
```

---

## Kentucky Summative Assessment (KSA) Mapping

**Need to identify specific metrics for:**
- Reading 3rd Grade
- Math 8th Grade

**Action Items:**
1. Examine KSA data structure in `data/kpi/kpi_master.csv`
2. Identify relevant metrics (likely grade-specific proficiency rates)
3. Map to dashboard indicators

**Query to identify KSA metrics:**
```python
ksa_metrics = kpi_df[kpi_df['metric'].str.contains('assessment|reading|math', case=False, na=False)]['metric'].unique()
print(sorted(ksa_metrics))
```

---

## Implementation Checklist

### ky-education-kpi-pipeline

- [ ] Create `analysis/` directory
- [ ] Implement `analysis/bright_spots.py` with:
  - [ ] KPI data loading
  - [ ] Correlation validation (5-year)
  - [ ] Regression analysis (statewide)
  - [ ] Multi-year trend tracking
  - [ ] JSON output generation
  - [ ] Fayette County filtering
- [ ] Identify KSA metrics for reading/math grades
- [ ] Test with full statewide data
- [ ] Verify N ≥ 58 schools for all validated combinations
- [ ] Generate correlation validation report
- [ ] Create journal entry documenting implementation

### fcps-equity-dashboard

- [ ] Archive existing TypeScript analysis scripts
- [ ] Update `package.json` scripts to call Python analysis
- [ ] Modify dashboard to consume pre-computed bright_spots.json
- [ ] Add statewide context to visualizations
- [ ] Update scatter plots to show Fayette County vs. statewide
- [ ] Display N (number of schools) in analysis
- [ ] Update BRIGHT_SPOTS_METHODOLOGY.md with corrections
- [ ] Add METHODOLOGY_REVIEW.md recommendations
- [ ] Test dashboard with new data structure

### Documentation

- [ ] Update BRIGHT_SPOTS_METHODOLOGY.md (sample size fix)
- [ ] Add statewide analysis section
- [ ] Update APPENDIX_B_ALL_SCHOOLS.md generation
- [ ] Create data dictionary for bright_spots.json
- [ ] Document KPI metric → Dashboard indicator mappings
- [ ] Address METHODOLOGY_REVIEW.md critical issues

---

## Statistical Improvements to Address from Review

From `fcps-equity-dashboard/METHODOLOGY_REVIEW.md`:

### Must Fix (Addressed by Statewide Analysis)

1. **✅ Sample Size Definition** - Fixed by using school count, not student count
2. **✅ Insufficient Observations** - Statewide data provides N ~1,200 schools
3. **⚠️ Inconsistent Correlation Thresholds** - Need consistent |r| ≥ 0.5 application
4. **⚠️ Postsecondary Readiness Anomaly** - Exclude one-year spikes

### Should Fix

5. **⚠️ Z-Score Threshold** - Consider Z > 1.5 instead of Z > 1.0
6. **⚠️ Regression Diagnostics** - Add linearity, normality, homoscedasticity checks
7. **⚠️ Positive Correlation Paradox** - Investigate postsecondary enrollment patterns

### Consider

8. **Future:** Multivariate regression (control for multiple demographics)
9. **Future:** Hierarchical linear modeling (true longitudinal analysis)
10. **Future:** Qualitative follow-up (case studies of bright spots)

---

## Expected Benefits

1. **Statistical Validity**
   - N ~1,200 schools meets Green (1991) requirements
   - Robust regression models
   - Valid z-score interpretations

2. **Statewide Context**
   - Fayette County schools compared to state patterns
   - "Beating Kentucky expectations" narrative
   - More meaningful bright spots identification

3. **Reproducibility**
   - Python-based analysis (not TypeScript)
   - Can be run for any Kentucky district
   - Potential for statewide equity dashboard

4. **Methodological Rigor**
   - Addresses critical sample size error
   - Implements proper correlation validation
   - Follows research-based best practices

---

## Next Steps

1. **Immediate:** Examine KSA data to map reading/math metrics
2. **Week 1:** Implement `analysis/bright_spots.py` with statewide data
3. **Week 2:** Test Python script, validate outputs
4. **Week 3:** Update dashboard to consume new JSON format
5. **Week 4:** Update methodology documentation with fixes

---

## References

- Green, S. B. (1991). How Many Subjects Does It Take To Do A Regression Analysis? *Multivariate Behavioral Research*, 26(3), 499-510.
- `fcps-equity-dashboard/METHODOLOGY_REVIEW.md` - Statistical critique document
- `fcps-equity-dashboard/BRIGHT_SPOTS_METHODOLOGY.md` - Current methodology
- `ky-education-kpi-pipeline/KPIS.md` - Available KPI metrics
