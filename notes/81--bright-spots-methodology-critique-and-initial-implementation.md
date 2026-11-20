# Bright Spots Methodology Critique and Initial Implementation

**Date:** 2025-11-23
**Status:** Phase 1 Complete - Foundation Built
**Related Notes:** 80--statewide-bright-spots-analysis-implementation.md

## Summary

This session focused on critiquing the existing Bright Spots methodology from fcps-equity-dashboard and beginning implementation of a statistically rigorous statewide analysis using the ky-education-kpi-pipeline data infrastructure.

## Deliverables

### 1. Methodology Critique Document
**File:** `fcps-equity-dashboard/METHODOLOGY_REVIEW.md` (400+ lines)

**Key Findings:**
- Identified **critical sample size error**: Current implementation counts students instead of schools for regression requirements
- Documented 10 major issues requiring fixes
- Provided concrete recommendations with code examples
- Categorized issues as "Must Fix" (4), "Should Fix" (6), "Consider" (3)

**Critical Issues Identified:**
1. **Sample Size Confusion (Major)**: Conflates student counts with observation counts for N ≥ 58 requirement
2. **Inconsistent Correlation Thresholds**: Results include weak correlations that don't meet |r| ≥ 0.5 threshold
3. **Postsecondary Readiness Anomaly**: One-year correlation spike (r=0.572 in 2025) used despite 5-year average near zero
4. **Missing Regression Diagnostics**: No checks for linearity, normality, homoscedasticity, outliers

### 2. Implementation Plan Document
**File:** `ky-education-kpi-pipeline/notes/80--statewide-bright-spots-analysis-implementation.md`

**Key Decisions:**
- **Stratified statewide analysis** by school type (Elementary/Middle/High) instead of district-level
- Acknowledges Fayette County sample size limitations (Elementary: 35-40, Middle: 12-15, High: 8-10 schools)
- Only 2-3 indicators meet N ≥ 40 threshold within Fayette County alone
- Solution: Analyze statewide within school type strata, then filter to Fayette County schools

**Rationale for Statewide Stratification:**
- Elementary indicators → ~800 KY elementary schools (meets N ≥ 58)
- Middle indicators → ~300 KY middle schools (meets N ≥ 58)
- High indicators → ~200 KY high schools (meets N ≥ 58)
- Controls for school-level factors (grade levels, curriculum, student development)
- Transparent about trade-off: district-level confounding vs. statistical validity

### 3. Python Bright Spots Analysis Script (Phase 1)
**File:** `ky-education-kpi-pipeline/analysis/bright_spots.py` (500+ lines)

**Implemented Components:**

#### Infrastructure
- KPI data loading with chunked reading and progress logging
- School type classification (Elementary/Middle/High based on KDE school codes)
- Test mode flag (`--test`) for rapid iteration with subset of data
- Year filtering during CSV load to reduce memory usage

#### Statistical Functions
- Pearson correlation coefficient calculation
- Linear regression (slope, intercept, r-value)
- Z-score calculation for residuals
- Proper handling of missing/NaN values

#### Step 1: Correlation Validation (COMPLETE)
```python
def validate_correlation(kpi_df, indicator, subgroup, school_type):
    """
    Validate that demographic density predicts performance over 5 years.

    - Filters to school type stratum (Elementary/Middle/High/All)
    - Calculates Pearson r for each year 2021-2025
    - Validates |r| ≥ 0.5 sustained correlation
    - Requires N ≥ 58 schools in stratum
    - Uses SCHOOL COUNT not student count (fixes critical error)
    """
```

**Test Results:**
- Loads 232K rows in ~2 seconds (chunked reading)
- Processes all 8 indicators × 5 subgroups = 40 combinations in 28 seconds
- Generates `analysis/correlation_validation.json` output

#### Remaining Implementation (TODO)
- **Step 2:** Regression analysis and bright spots identification
- **Step 3:** Multi-year trend analysis (Sustained Excellence, Rising Star, Current)
- **Step 4:** Output generation (statewide JSON, Fayette County filtered JSON, markdown reports)

## Key Technical Improvements

### Sample Size Correction
**Before (INCORRECT):**
```typescript
const totalStudentsInDemographic = validSchools.reduce((sum, school) => {
    return sum + (school.indicators['enrollment'][demographic] || 0);
}, 0);
if (totalStudentsInDemographic < 58) return [];  // WRONG!
```

**After (CORRECT):**
```python
n_schools = len(validSchools)  # Count schools, not students
if n_schools < MIN_SCHOOLS_FOR_REGRESSION:  # N ≥ 58 schools
    return None
```

### Performance Optimization
**Original Approach:**
- Attempted to load entire 10GB CSV into memory
- No progress indication
- Ran for 16+ minutes without completing

**Optimized Approach:**
```python
def load_kpi_data(kpi_path, years_filter=None, test_mode=False):
    chunk_size = 500000
    for i, chunk in enumerate(pd.read_csv(kpi_path, chunksize=chunk_size)):
        if years_filter:
            chunk = chunk[chunk['year'].isin(years_filter)]
        chunks.append(chunk)
        logger.info(f"Loaded chunk {i+1}: {len(chunk):,} rows")
```

**Results:**
- Loads filtered data (2024-2025) in ~2 seconds
- Progress logging every 500K rows
- Test mode flag for rapid iteration
- Completed correlation validation in 28 seconds

## Strategic Decisions

### District-Level vs. Statewide Analysis

**Initial Approach (Rejected):**
- Run regression within Fayette County only
- **Problem:** Insufficient schools (N=35-40 elementary, N=12-15 middle, N=8-10 high)
- Fails to meet N ≥ 58 requirement for most indicators

**Final Approach (Implemented):**
- Run regression on statewide data stratified by school type
- Filter results to Fayette County schools for dashboard
- **Trade-off acknowledged:** District-level factors (funding, policies) may confound results
- **Mitigation:** School type stratification maintains contextual similarity

**Interpretation:**
> Fayette County schools identified as "Bright Spots" are performing significantly better than predicted based on **statewide patterns for similar school types**, not just within Fayette County.

### Ecological Fallacy Concerns

**Question:** Should correlation validation be at state or district level?

**Analysis:**
- State-level correlation may conflate between-district variation with within-district patterns
- Example: High-poverty District A might have universally lower scores vs. low-poverty District B
- Statewide correlation driven by district differences, not school-level effects (Simpson's Paradox)

**Solution:**
- Stratify by school type to maintain contextual similarity
- Accept district-level confounding as necessary trade-off for statistical validity
- Document limitation transparently in methodology

## Indicator Configuration

### Mapped Indicators
```python
INDICATOR_CONFIG = {
    'kindergarten_readiness_rate': {
        'school_type': 'Elementary',
        'display_name': 'Kindergarten Readiness'
    },
    'graduation_rate_4_year': {
        'school_type': 'High',
        'display_name': 'Graduation Rate'
    },
    'climate_index_score_calculated': {
        'school_type': 'All',
        'display_name': 'Climate Index'
    },
    'chronic_absenteeism_rate_all_grades': {
        'school_type': 'All',
        'display_name': 'Chronic Absenteeism'
    },
    # ... etc
}
```

### TBD: Kentucky Summative Assessment Metrics
**Need to identify:**
- 3rd Grade Reading proficiency metric name
- 8th Grade Math proficiency metric name

**Action:** Query KPI master file for assessment metrics:
```python
ksa_metrics = df[df['metric'].str.contains('assessment|reading|math',
                                             case=False)]['metric'].unique()
```

## Files Created/Modified

### New Files
1. `fcps-equity-dashboard/METHODOLOGY_REVIEW.md` - Statistical critique
2. `ky-education-kpi-pipeline/notes/80--statewide-bright-spots-analysis-implementation.md` - Implementation plan
3. `ky-education-kpi-pipeline/analysis/bright_spots.py` - Analysis script
4. `ky-education-kpi-pipeline/analysis/correlation_validation.json` - Test output

### Modified Files
None (new implementation, no existing files modified)

## Lessons Learned

### Statistical Rigor
1. **Always validate sample sizes** - The student vs. school count error was fundamental
2. **Check regression assumptions** - Linearity, normality, homoscedasticity, outliers
3. **Use statistical significance tests** - Not just arbitrary correlation thresholds
4. **Multi-year validation prevents one-year anomalies** from driving decisions

### Performance Optimization
1. **Chunked reading is essential** for large CSV files (10GB+)
2. **Progress logging prevents user uncertainty** during long operations
3. **Test mode enables rapid iteration** - 28s vs. 16+ minutes
4. **Year filtering during load** reduces memory pressure significantly

### Methodology Design
1. **Sample size constraints drive architecture** - District-level ideal but not feasible
2. **Stratification balances validity and context** - School type maintains similarity
3. **Transparency about trade-offs** builds trust in methodology
4. **Explicit acknowledgment of limitations** strengthens rigor

## Next Steps

### Immediate (Next Session)
1. Identify KSA metric names for reading/math grades
2. Run full correlation validation with all years (2021-2025)
3. Review validation results to determine which indicators meet |r| ≥ 0.5 threshold

### Short-Term
4. Implement Step 2: Regression analysis and bright spots identification
5. Implement Step 3: Multi-year trend analysis
6. Implement Step 4: Output generation (JSON, markdown)
7. Test full pipeline with statewide data

### Medium-Term
8. Update fcps-equity-dashboard to consume pre-computed bright spots JSON
9. Archive TypeScript analysis scripts (bright-spots.ts, correlation-analysis.ts, equity-audit.ts)
10. Update BRIGHT_SPOTS_METHODOLOGY.md with corrections from review

### Long-Term
11. Add regression diagnostics (diagnostic plots in appendices)
12. Consider multivariate regression (control for multiple demographics simultaneously)
13. Develop qualitative follow-up protocol (case studies, interviews, observations)
14. Create practice briefs for district-wide learning from bright spots

## References

### Documents Created
- Methodology Review: `fcps-equity-dashboard/METHODOLOGY_REVIEW.md`
- Implementation Plan: `notes/80--statewide-bright-spots-analysis-implementation.md`

### Academic Citations (from Methodology Review)
- Green, S. B. (1991). How Many Subjects Does It Take To Do A Regression Analysis?
- García et al. (2018). QuantCrit: Rectifying quantitative methods through critical race theory
- Carnegie Foundation (2013). Quality Improvement Approaches: Positive Deviance
- Georgia GOSA. Beating the Odds Analysis

### Related Files
- Analysis Script: `analysis/bright_spots.py`
- Test Output: `analysis/correlation_validation.json`
- Original Dashboard Scripts: `fcps-equity-dashboard/scripts/bright-spots.ts`

---

**Session Duration:** ~3 hours
**Lines of Code Written:** ~900 (Python) + ~400 (Markdown documentation)
**Key Achievement:** Corrected fundamental sample size error and established statistically rigorous foundation
