# Bright Spots Methodology: Hierarchical Bayesian Framework for Identifying Positive Deviant Schools

**Date:** December 1, 2025
**Version:** 1.5
**Context:** Kentucky Statewide Analysis with Fayette County Focus
**Model Location:** `analysis/bayesian_models/base_hierarchical_model.py` (base class)
**Indicator Models:** `analysis/bayesian_models/*_model.py` (11 indicators)
**Prior Analysis:** `analysis/prior_analysis/` (empirically-informed priors)

---

## Executive Summary

This document describes a rigorous, research-based approach to identifying "Bright Spots"—schools achieving exceptional outcomes for their most vulnerable students given their specific context. Rather than simply ranking schools by overall performance, we employ a **Hierarchical Bayesian framework** grounded in **Positive Deviance methodology**[^1] and **QuantCrit (Quantitative Critical Race Theory)**[^2].

### Why Hierarchical Bayesian?

Our initial approach using standard OLS regression within Fayette County alone failed due to fundamental sample size constraints:

| School Level | Fayette County Schools | Minimum for OLS[^3] | Status |
|--------------|----------------------|---------------------|--------|
| High Schools | 6 | 58 (N ≥ 50 + 8m) | **Invalid** |
| Middle Schools | <20 | 58 | **Invalid** |
| Elementary Schools | ~35 | 58 | **Marginal** |

Standard regression with n=6-20 schools produces unstable coefficient estimates, wide confidence intervals, and high false positive rates[^4]. A single outlier can dramatically alter the regression line, making genuine outliers impossible to distinguish from noise[^5].

**The Solution:** Hierarchical Bayesian modeling that:
1. Borrows strength from ~1,200 Kentucky schools statewide via partial pooling[^6]
2. Accounts for regional differences without forcing statewide patterns on local schools[^7]
3. Handles multiple demographic predictors simultaneously (intersectionality)
4. Provides principled uncertainty quantification through posterior distributions[^8]
5. Works validly with samples as small as n=6-10 schools[^9]

---

## 1. Theoretical Framework

### 1.1 Positive Deviance: Learning from Outliers

The **Positive Deviance** framework[^1] recognizes that in every community, certain individuals or organizations succeed against the odds. Rather than imposing external solutions, this approach seeks to identify and learn from these "positive deviants"—those who have found uncommon, beneficial practices despite facing the same constraints as their peers.

In educational contexts, this means identifying schools that achieve exceptional outcomes for historically underserved populations—not by having more resources or serving easier populations, but by employing effective practices that can be studied and potentially replicated.

### 1.2 QuantCrit: Numbers Are Not Neutral

**Quantitative Critical Race Theory (QuantCrit)**[^2] provides the ethical framework for our analysis. Key principles:

1. **Context matters**: Comparing schools without accounting for structural advantages reinforces privilege rather than identifying excellence
2. **Numbers are not neutral**: Aggregated data often masks systemic inequities
3. **Focus on the system**: We must identify schools disrupting predictive patterns of inequity, not simply those with high raw scores

### 1.3 Moving Beyond Deficit Thinking

Traditional educational data analysis often falls into **Deficit Thinking**—attributing performance gaps solely to perceived deficiencies of students, families, or communities[^10]. Our approach instead employs **Asset-Based Analysis**, asking: "What are these schools doing differently that enables success despite structural barriers?"

### 1.4 Student Group Analysis: QuantCrit in Practice

Following QuantCrit methodology, we run **separate models for each student group** rather than aggregating all students. This ensures we identify schools achieving exceptional outcomes for specific populations.

**Analyzed Student Groups:**

| Group | KDE Identifier | Description | Target Group |
|-------|----------------|-------------|--------------|
| All Students | `All Students` | School-wide average | No (baseline) |
| Economically Disadvantaged | `Economically Disadvantaged` | FRL-eligible students | Yes |
| African American | `African American` | Black/African American students | Yes |
| Students with Disabilities | `Students with Disabilities (IEP)` | Students with IEPs | Yes |
| Hispanic/Latino | `Hispanic or Latino` | Hispanic/Latino students | Yes |
| Homeless | `Homeless` | Students experiencing homelessness | Yes |
| English Learners | `English Learner` | English Language Learners | Yes |

**Why separate models?** Aggregating outcomes across groups masks critical variation. A school with strong overall performance may simultaneously underserve specific populations. Separate models enable us to identify schools excelling for *each* historically marginalized group, consistent with QuantCrit's emphasis on disaggregation[^2].

**Implementation:** Each indicator model runs 7 times (all students + 6 target groups), producing group-specific:
- School effect estimates
- Predictor effect estimates
- Bright spot classifications

See `analysis/config/student_groups.py` for the student group configuration.

---

## 2. The Sample Size Problem: Why Standard Regression Failed

### 2.1 Statistical Requirements for OLS Regression

Research establishes minimum sample sizes for reliable regression analysis:

- **Green (1991)**[^3]: N ≥ 50 + 8m for testing overall model fit (m = predictors)
- **Rule of thumb**[^4]: n = 20 + 5k where k = number of predictors
- **Conservative threshold**[^11]: N ≥ 58 students for single-predictor regression

With only **6 high schools** in Fayette County:
- One school = 17% of the sample (extreme leverage)
- Cannot assess normality of residuals
- Cannot detect genuine outliers vs. random variation
- Perfect or near-perfect R² doesn't indicate good fit—it indicates overfitting

### 2.2 Simpson's Paradox and Ecological Fallacy

When correlations exist locally but not statewide (or vice versa), we observe **Simpson's Paradox**[^12] and the **ecological fallacy**[^13]—situations where relationships at one aggregation level don't hold at another.

Robinson (1950) demonstrated this dramatically: states with more foreign-born residents had higher literacy rates (+0.53) but foreign-born *individuals* were less literate (-0.11)[^14]. The reversal occurred because immigrants concentrated in high-literacy states.

**In our context:** If poverty-achievement correlations exist within Fayette County but not statewide, this reflects:
1. Within Fayette schools, students from disadvantaged backgrounds score lower (within-group effect)
2. Across Kentucky, districts with more disadvantaged students don't necessarily have lower achievement because other factors (funding, teacher quality) vary systematically at the state level

**Implication:** Pooling data with different underlying correlation structures violates regression's homogeneity assumption[^15] and produces estimates that accurately represent *neither* local nor state relationships.

---

## 3. Hierarchical Bayesian Modeling: The Solution

### 3.1 How Partial Pooling Works

Hierarchical models explicitly account for nested data structures (schools within districts within regions) and use **partial pooling**[^6] to stabilize small-sample estimates by borrowing strength from larger samples.

The key mechanism: school-specific estimates become weighted averages of the school's own data and the overall mean:

$$\hat{\alpha}_j = \lambda_j \bar{y}_j + (1-\lambda_j)\bar{y}_{all}$$

Where weights depend on sample size[^9]. Schools with few observations get heavy shrinkage toward the mean; schools with many observations rely mostly on their own data.

**Mathematical guarantee (Stein's Paradox)**[^16]: With 3+ groups, a shrinkage estimator *always* has lower mean squared error than no-pooling estimators. Partial pooling reduces MSE by 36-47% compared to no-pooling approaches[^17].

### 3.2 Three-Level Model Structure with County-Varying Slopes

Our model implements a three-level hierarchy matching Kentucky's educational structure, with an additional county-varying slopes component:

```
Level 3: State          μ_state ~ Normal(93, 10)
                              |
Level 2: Districts      district_effect ~ Normal(0, σ_district)
           (n=170)            |
Level 1: Schools        school_effect ~ Normal(district_effect, σ_school)
           (n=700+)
                              +
County-Varying Slopes   β_county[c] = β + deviation[c]
           (n=120)
```

**Mathematical Specification:**

```
y_ijk ~ Normal(μ_ijk, σ_y)

μ_ijk = μ_state + district_effect[j] + school_effect[i] + X_ijk · β_county[c]

Where:
  y_ijk = outcome (e.g., graduation rate) for school i in district j in county c
  μ_state = state-level intercept
  district_effect[j] = district-level random effect
  school_effect[i] = school-level random effect (nested within district)
  X_ijk = matrix of standardized covariates
  β = global regression coefficients
  β_county[c] = β + β_deviation[c] (county-specific slopes)
  β_deviation[c] ~ Normal(0, σ_county_slope) for each predictor
```

### 3.3 County-Varying Slopes

The model includes county-varying slopes to capture regional differences in how predictors relate to outcomes. This allows us to identify where predictor effects differ significantly from statewide patterns (e.g., "In Fayette County, teacher experience has a stronger relationship with outcomes than statewide").

**Implementation (non-centered parameterization):**

```python
# Variance for county-level slope deviations (one per predictor)
sigma_county_slope = pm.HalfCauchy('sigma_county_slope', beta=1.0, shape=n_predictors)

# County-specific deviations from global slopes
beta_county_raw = pm.Normal('beta_county_raw', mu=0, sigma=1, shape=(n_counties, n_predictors))
beta_county_deviation = beta_county_raw * sigma_county_slope

# Total county-specific slopes = global + county deviation
beta_county = beta + beta_county_deviation
```

A county's predictor effect is flagged as "different from statewide" if the 95% credible interval for the county-specific effect excludes the global effect.

### 3.4 Prior Specifications

Following recommendations from Gelman (2006)[^18] for variance parameters in hierarchical models, our system now uses **empirically-informed priors** computed from historical data. This follows guidance from Van de Schoot & Miočević (2017)[^38] showing that properly constructed informative priors enhance parameter estimates, especially with small sample sizes.

#### 3.4.1 Empirically-Informed Prior Pipeline

Rather than using fixed default priors, the model loads **group-specific priors** from historical analysis:

```
analysis/outputs/prior_analysis/{indicator}/{student_group}/recommended_priors.csv
```

The prior analysis pipeline (`analysis/prior_analysis/indicators/{indicator}/historical_review.py`) computes priors using:

1. **State mean prior**: Historical mean ± 0.7 × historical SD (allows shrinkage while remaining data-informed)
2. **Variance priors**: 1.5 × observed between-school/district SD (weakly informative but scaled to data)

This ensures that priors for African American students differ appropriately from All Students priors, consistent with QuantCrit methodology.

#### 3.4.2 Prior Parameter Table

| Parameter | Prior Distribution | Computation | Rationale |
|-----------|-------------------|-------------|-----------|
| `μ_state` | Normal(μ, σ) | Historical mean, max(5, 0.7×SD) | Empirically-informed intercept |
| `σ_district` | HalfCauchy(β) | 1.5 × observed district SD | Allows variability while stabilizing |
| `σ_school` | HalfCauchy(β) | 1.5 × observed school SD | Slightly tighter than district |
| `β` | Finnish Horseshoe[^40] | Data-dependent τ | Sparse shrinkage with slab regularization |
| `σ_county_slope` | HalfCauchy(1) | Scale=1 | County-level slope deviations |
| `σ_y` | HalfCauchy(β) | 1.5 × observed residual SD | Observation-level noise |

#### 3.4.3 Data-Dependent Horseshoe Prior

The global shrinkage parameter τ is computed using the formula from Piironen & Vehtari (2017)[^39]:

$$\tau_0 = \frac{m_{eff}}{p - m_{eff}} \cdot \frac{\sigma_y}{\sqrt{n}}$$

Where:
- $m_{eff}$ = expected number of effective (non-zero) predictors (default: 6)
- $p$ = total number of predictors (~20-27)
- $\sigma_y$ = prior for observation noise (from variance priors)
- $n$ = sample size (number of school-year observations)

**Implementation:**
```python
# In _build_coefficient_priors()
m_eff = self.get_m_eff()  # Default 6, overridable per indicator
tau_scale = (m_eff / (n_predictors - m_eff)) * (sigma_y_prior / np.sqrt(n_obs))
tau = pm.HalfCauchy('tau', beta=tau_scale)
```

**Slab regularization** prevents extreme large effects:
```python
slab_scale, slab_df = self.get_slab_parameters()  # Default (2.0, 4.0)
c2 = pm.InverseGamma('c2', alpha=slab_df/2, beta=slab_df * slab_scale**2 / 2)
# Results in c2 ~ InverseGamma(2, 8), mean ~8
```

#### 3.4.4 Fallback Default Prior Settings

When prior analysis is unavailable, indicators use these defaults:

| Indicator | State Mean Prior | σ_district | σ_school | σ_y |
|-----------|------------------|------------|----------|-----|
| Graduation Rate | Normal(93.7, 2.5) | 5.0 | 2.9 | 3.6 |
| Reading Grade 3 | Normal(43, 15) | 10.0 | 5.0 | 5.0 |
| Math Grade 8 | Normal(38, 15) | 10.0 | 5.0 | 5.0 |
| Kindergarten Readiness | Normal(47, 15) | 10.0 | 5.0 | 5.0 |
| Postsecondary Enrollment | Normal(45, 15) | 8.0 | 5.0 | 5.0 |
| Postsecondary Readiness | Normal(83, 10) | 6.0 | 4.0 | 4.0 |
| Chronic Absenteeism | Normal(24.4, 5.0) | 15.1 | 9.9 | 4.5 |
| EL Progress (all levels) | Normal(15, 15) | 10.0 | 8.0 | 8.0 |
| School Climate | Normal(85, 10) | 5.0 | 4.0 | 4.0 |

**Why Half-Cauchy priors?** Gelman (2006)[^18] and Polson & Scott (2012)[^40] recommend against inverse-gamma for variance parameters, which can behave poorly near zero. Half-Cauchy priors:
- Are unbounded at zero (allow for no variance if warranted)
- Have heavy tails (robust to outliers)
- Provide better behavior than inverse-gamma in small samples

### 3.5 Likelihood Options

The model supports three likelihood types for different outcome characteristics:

| Likelihood | Use Case | Implementation |
|------------|----------|----------------|
| `normal` | Unbounded outcomes | Standard Normal(μ, σ) |
| `beta` | Bounded [0,100] rates | Beta(μ·ν, (1-μ)·ν) with logit link |
| `logit` | Bounded rates with better sampling | Normal on logit scale, sigmoid back-transform |

**Logit-transformed Normal** (used for graduation rates) provides:
- Bounded predictions via sigmoid back-transformation
- Better MCMC sampling than Beta regression for hierarchical models
- Easier interpretation of school effects on logit scale

### 3.6 Year Fixed Effects

For multi-year data, the model includes **sum-to-zero constrained year fixed effects**:

```python
# Free parameters for years 0 to n_years-2
year_effect_free = pm.Normal('year_effect_free', mu=0, sigma=5, shape=n_years - 1)
# Last year constrained: sum(all years) = 0
year_effect_last = -pm.math.sum(year_effect_free)
year_effect = pm.math.concatenate([year_effect_free, [year_effect_last]])
```

This addresses temporal trends while maintaining identifiability (the year effects sum to zero by construction)

---

## 4. Covariates: Beyond Free/Reduced Lunch

### 4.1 Why FRL Alone is Insufficient

Free/Reduced Lunch (FRL) eligibility is a crude proxy for economic disadvantage[^19]:
- Binary (qualify or not)—doesn't capture degree of disadvantage
- Subject to underreporting (stigma, incomplete paperwork)
- Policy changes affect eligibility independent of actual poverty
- Within Fayette County, neighborhoods with identical FRL% can have very different median incomes

### 4.2 Covariate Categories

Our model incorporates **27 covariates** across six categories:

**Category 1: Student Demographics (6 variables)**
- `pct_economically_disadvantaged` - % students qualifying for FRL
- `pct_english_learners` - % English Language Learners
- `pct_students_with_disabilities` - % students with IEPs
- `pct_african_american` - % African American students
- `pct_hispanic` - % Hispanic/Latino students

**Category 2: Teacher Quality (5 variables)**
- `novice_teacher_rate` - Combined % teachers with <1yr + 1-3yr experience
- `teacher_avg_experience` - Average years of teaching experience
- `student_teacher_ratio` - Students per teacher
- `teacher_turnover_rate` - Annual teacher turnover rate
- `emergency_provisional_rate` - % teachers on emergency certification

**Category 3: Financial Resources (2 variables)**
- `per_pupil_spending` - District-level spending per student (all funds)
- `students_per_certified_staff` - Students per certified staff member

**Category 4: Community Economic Context - County Level (3 variables)**
- `county_median_income` - Census SAIPE median household income[^20]
- `county_poverty_rate` - Census SAIPE poverty rate (all ages)
- `county_child_poverty_rate` - Census SAIPE poverty rate (ages 5-17)

**Category 5: Neighborhood Context - Tract Level (6 variables)**

Census tract-level data provides more granular neighborhood context than county-level data. This is important for capturing within-county variation in economic conditions that affect school performance.

**Coverage:** 1,478 Kentucky schools geocoded to 773 unique Census tracts across 120 counties. Tract-level data matched for 98.6% of high school observations (1,609/1,631)[^36].

**Income variation statewide:** Tract median household income ranges from $10,455 to $216,607[^34].

- `tract_median_household_income` - Census ACS 5-Year tract-level income
- `tract_poverty_rate` - Census ACS 5-Year tract-level poverty
- `tract_pct_bachelors_plus` - % adults with bachelor's degree or higher
- `tract_unemployment_rate` - Local unemployment rate
- `tract_pct_single_parent` - % single-parent households (family structure)
- `tract_pct_owner_occupied` - % owner-occupied housing (housing stability)

**Indicator-Specific Tract Variables:**

Some tract-level variables are included only for specific indicators where theory supports their relevance:

- `tract_pct_broadband` - **Included for chronic absenteeism only**: Research shows heterogeneous effects on achievement[^37], but broadband access may have clearer effects on attendance (enabling virtual learning, parent communication). Excluded from other indicators due to high correlation with income (r=0.63) and education (r=0.60).

- `tract_pct_housing_cost_burden_30_plus` - **Included for chronic absenteeism only**: Housing instability directly impacts attendance through moves, stress, and transportation issues. Excluded from other indicators where correlation with `tract_pct_owner_occupied` (r=-0.64) makes it redundant with existing poverty measures.

Schools are geocoded to census tracts using their latitude/longitude coordinates via the Census Geocoding API[^35]. This enables joining tract-level ACS data to individual schools for more precise neighborhood characterization.

**Category 6: Institutional Characteristics (5 variables, dummy-encoded)**

Categorical variables representing school classification and federal funding status. These are encoded as binary (0/1) dummy variables to enable their use in regression models while preserving interpretability.

**Title I Status** (Reference: "Not a Title 1 School"):
- `title_i_schoolwide` - School has Title I Schoolwide program (1=yes, 0=no)
- `title_i_targeted` - School has Title I Targeted Assistance program (1=yes, 0=no)
- `title_i_eligible_no_program` - School is Title I eligible but no program implemented (1=yes, 0=no)

**School Type** (Reference: A1 - Standard public school):
- `school_type_a5` - Alternative program for remediation (1=yes, 0=no)
- `school_type_a6` - Alternative program for state agency children (1=yes, 0=no)

**Encoding Rationale:** Categorical variables are dummy-encoded with a reference category omitted to avoid multicollinearity. Binary predictors are NOT standardized—they retain their 0/1 values so coefficients represent the effect of "having the characteristic vs. not having it" rather than "per standard deviation change."

**Why include institutional characteristics?** Title I status and school type reflect structural differences in school populations and resources that affect outcomes independently of demographics. For example, alternative schools (A5, A6) serve at-risk populations; their lower outcomes reflect population characteristics, not instructional quality. Including these controls ensures fair comparison of schools serving similar populations.

### 4.3 Covariate Catalog and Selection Framework

All available covariates are documented in a **covariate catalog** (`analysis/config/covariate_catalog.yaml`) that specifies:
- Data source and update frequency
- Expected direction of effect
- Literature support and citations
- Theoretical relevance per indicator
- Exclusion reasons for removed variables

For exploratory analysis, the model supports **horseshoe priors** (Carvalho et al., 2010) for automatic coefficient shrinkage. This allows the data to determine which covariates contribute predictive power, rather than requiring manual selection. See `analysis/bayesian_models/base_model.py` for implementation details.

### 4.4 Philosophical Note: Adjusting for What?

We deliberately include both demographics AND school resources as covariates. This follows guidance from the National Academies[^21] that "models with and without covariates can yield substantially different results."

**Our rationale:**
- **Community context matters** beyond what prior achievement captures (unemployment, housing instability)
- **Resource equity:** Including school resources ensures we're not just finding wealthy schools
- **Honest attribution:** A school with 90% poverty achieving 70% proficiency deserves different recognition than one with 20% poverty achieving 70%

**Critical principle:** We use covariates for *fairness*, not excuses. The logic:

> "Given this school's demographics, resources, and context, we predict 60% proficiency. They achieved 75%. They are beating the odds by 15 percentage points. What practices can we learn from them?"

---

## 5. Identifying Bright Spots

### 5.1 Credible Interval Criterion

A school is identified as a **Bright Spot** when its entire 95% credible interval for the school effect is above zero:

$$\text{CI}_{2.5\%}(\theta_j) > 0$$

Where:
- θ_j = school effect (performance relative to expectation)
- CI_{2.5%} = lower bound of the 95% highest density interval

**Interpretation:** "We are at least 95% confident this school's true effect is positive (above expectation), after controlling for demographics and regional patterns."

For **reverse indicators** (e.g., chronic absenteeism where lower is better), the criterion is inverted:

$$\text{CI}_{97.5\%}(\theta_j) < 0$$

This ensures a school is only labeled a bright spot when we are 95% confident it is performing better than expected (lower absenteeism in this case).

### 5.2 Threshold Selection Rationale

The **95% credible interval criterion** is conservative:
- Equivalent to requiring p < 0.05 in frequentist terms
- Ensures we only identify schools with strong statistical evidence
- Reduces false positive rate at the cost of potentially missing some true bright spots

This approach:
- Avoids arbitrary effect size thresholds
- Lets the uncertainty in each school's estimate drive the classification
- Is robust to sample size differences (schools with more data have tighter CIs)

### 5.3 Credible Intervals

We report **95% Highest Density Intervals (HDI)**[^22] for all school effects:
- HDI contains 95% of posterior mass
- Preferred over equal-tailed intervals for skewed posteriors
- School is a reliable bright spot if 95% HDI lower bound exceeds threshold

---

## 6. Model Diagnostics

### 6.1 Convergence Diagnostics

Following modern standards from Vehtari et al. (2021)[^23]:

| Diagnostic | Threshold | Purpose |
|------------|-----------|---------|
| R-hat | < 1.01 | Chain mixing/convergence |
| Bulk ESS | > 400 | Effective samples for central tendency |
| Tail ESS | > 400 | Effective samples for credible intervals |
| Divergences | 0 | Sampling problems in hierarchical models |

### 6.2 Posterior Predictive Checks

We validate model fit through posterior predictive checks[^24]:
1. **Distribution comparison:** Replicated data distribution vs. observed
2. **Test statistics:** Mean, SD, min, max of graduation rates
3. **Bayesian p-values:** Should fall between 0.05-0.95 for well-calibrated models

### 6.3 Leave-One-Out Cross-Validation

PSIS-LOO (Pareto-smoothed importance sampling)[^25] provides:
- `elpd_loo`: Expected log predictive density (higher = better)
- `p_loo`: Effective number of parameters
- Pareto k diagnostics for individual observation influence

**Reliability criterion:** < 5% of observations with Pareto k > 0.7

### 6.4 Collinearity Diagnostics

Multicollinearity among predictors can inflate posterior uncertainty and produce unstable coefficient estimates. We compute three complementary diagnostics:

**Variance Inflation Factor (VIF)**

VIF measures how much the variance of a coefficient estimate is inflated due to correlation with other predictors:

| VIF Range | Interpretation |
|-----------|----------------|
| < 5 | Acceptable |
| 5-10 | Moderate concern |
| > 10 | High multicollinearity (consider removing) |

VIF is computed from standardized predictors using:
```
VIF_j = 1 / (1 - R²_j)
```
where R²_j is the R-squared from regressing predictor j on all other predictors.

**Pairwise Correlation Matrix**

We examine all predictor-predictor correlations, flagging pairs with |r| > 0.7 for potential concern. Highly correlated predictors may represent redundant information or measurement of similar constructs.

### 6.5 Suppression Effect Detection

Statistical suppression occurs when controlling for correlated variables changes the relationship between a predictor and outcome—sometimes dramatically. We detect suppression by comparing bivariate correlations with model coefficients:

| Pattern | Interpretation | Reporting Guidance |
|---------|----------------|-------------------|
| **direct_effect** | Same sign, similar or stronger magnitude | Safe to report without caveats |
| **suppressed_sign_flip** | Model coefficient has opposite sign from bivariate r | **Requires caveat** - effect depends on other variables |
| **suppressed_weakened** | Same sign but model effect < 50% of bivariate | **Requires caveat** - shared variance with other predictors |
| **conditional_only** | Near-zero bivariate r (|r| < 0.05) but significant model effect | Effect emerges only when controlling for confounders |
| **not_significant** | 95% HDI includes zero | Do not interpret as meaningful |

**Detection logic:**
- **Sign flip:** `(bivariate_corr × model_coef) < 0` AND 95% CI excludes zero
- **Magnitude change:** `|model_coef| > 2 × |bivariate_corr|` AND bivariate |r| > 0.05

**Interpretation guidance:**
Suppression effects are not necessarily problematic—they can reveal true conditional relationships. However, they require careful interpretation:
- Report suppressed effects with explicit acknowledgment that the relationship holds "controlling for community factors"
- Consider whether the suppressor variable is causally prior to the predictor of interest
- For policy communication, emphasize direct effects unless suppression reveals actionable insights

---

## 7. MCMC Configuration

### 7.1 Sampling Parameters

| Setting | Value | Rationale |
|---------|-------|-----------|
| Chains | 4 | Minimum for robust R-hat computation[^23] |
| Draws per chain | 4000 | Sufficient for stable posterior estimates with county-varying slopes |
| Tuning/warmup | 2000 | Extended for complex hierarchical structure |
| Target accept | 0.95 | Higher for hierarchical models[^26] |
| Random seed | 42 | Reproducibility |

**Note:** The increased draws (4000 vs 2000) and tuning (2000 vs 1000) are necessary for the county-varying slopes component, which adds n_counties × n_predictors additional parameters to estimate.

### 7.2 Software Stack

- **PyMC 5.x**: Probabilistic programming framework[^27]
- **ArviZ**: Bayesian diagnostics and visualization[^28]
- **NumPy/Pandas**: Data manipulation
- **Matplotlib**: Visualization

---

## 8. Data Sources

### 8.1 Primary Sources

| Data | Source | Geographic Level | Update Frequency |
|------|--------|------------------|------------------|
| Student outcomes | KDE KPI Pipeline | School | Annual (August) |
| Demographics | KDE Enrollment | School | Annual |
| Teacher Quality | KDE Staff Reports | School/District | Annual (Oct) |
| Financial | KDE Financial Transparency | District | Annual (Spring) |
| Economic (County) | Census SAIPE[^20] | County | Annual (December) |
| Economic (Tract) | Census ACS 5-Year | Census Tract | Annual (December) |
| School Locations | KDE District School List | School | Annual |

### 8.2 Temporal Alignment

Economic data lags by 12 months (Census SAIPE releases December for prior year). This is acceptable because:
- Median income changes ~2-3% annually (slow-moving)
- Poverty rates are stable except during recessions
- Documentation explicitly tracks data dates

**Acceptable Lag Times by Variable Type:**

| Variable Type | Acceptable Lag | Rationale |
|--------------|----------------|-----------|
| Student outcomes | Current year | Primary analysis target |
| School characteristics | Current year | Changes quickly (enrollment, staff) |
| Teacher data | Current year | Turnover matters year-to-year |
| Financial data | 6-12 months | Fiscal year alignment (July-June) |
| Economic (income, poverty) | 12-24 months | Changes slowly (~2-3% annually) |
| Unemployment | < 3 months | Can spike during recessions |

**Principle:** Use most recent available data for each variable. Fast-moving variables (teachers, enrollment) require current-year data; slow-moving economic indicators are acceptable with 12-month lag.

---

## 9. Limitations and Assumptions

### 9.1 Key Assumptions

1. **Schools are exchangeable within groups**: Reasonable for traditional public schools; may not hold for specialized schools (magnet, alternative)
2. **Outliers in Y not X**: Robust regression assumes extreme values in outcomes, not predictors[^29]
3. **Prior specification matters**: We conduct sensitivity analysis across prior configurations[^30]
4. **Cross-sectional causality**: Identified bright spots may be effective OR may serve students with favorable unmeasured characteristics

### 9.2 Known Limitations

1. **Statistical power**: With n=20-50 schools, we can reliably identify only strong outliers (effect sizes > 0.5-0.8 SD)[^31]
2. **Generalizability**: Results may not generalize beyond Kentucky; local contextual factors matter
3. **Measurement error**: Proficiency test measurement error attenuates correlations
4. **Single-year analysis**: Less reliable than multi-year; future work will incorporate longitudinal structure

---

## 10. Ethical Considerations

### 10.1 Avoiding Harm

This analysis is designed to:
- **Celebrate success** in challenging contexts, not penalize schools for demographics
- **Identify practices** for potential replication, not create high-stakes rankings
- **Acknowledge uncertainty** honestly through credible intervals

### 10.2 Intended Use

Results should inform:
- Qualitative case studies of identified bright spots
- Professional learning community discussions
- Resource allocation for practice-sharing

Results should NOT be used for:
- Teacher or administrator evaluation
- Punitive accountability measures
- Real estate decisions

---

## 11. References

[^1]: Pascale, R., Sternin, J., & Sternin, M. (2010). *The Power of Positive Deviance: How Unlikely Innovators Solve the World's Toughest Problems*. Harvard Business Press. See also: Carnegie Foundation for the Advancement of Teaching. (2013). "Quality Improvement Approaches: Positive Deviance." https://www.carnegiefoundation.org/blog/quality-improvement-approaches-positive-deviance/

[^2]: García, N. M., López, N., & Vélez, V. N. (2018). "QuantCrit: Rectifying quantitative methods through critical race theory." *Race Ethnicity and Education*, 21(2), 149-157. https://www.tandfonline.com/doi/full/10.1080/13613324.2017.1377675

[^3]: Green, S. B. (1991). "How Many Subjects Does It Take To Do A Regression Analysis?" *Multivariate Behavioral Research*, 26(3), 499-510.

[^4]: Austin, P. C., & Steyerberg, E. W. (2015). "The number of subjects per variable required in linear regression analyses." *Journal of Clinical Epidemiology*, 68(6), 627-636.

[^5]: UCLA Statistical Consulting. "Robust Regression | Stata Data Analysis Examples." https://stats.oarc.ucla.edu/stata/dae/robust-regression/

[^6]: Gelman, A., & Hill, J. (2007). *Data Analysis Using Regression and Multilevel/Hierarchical Models*. Cambridge University Press. Chapter 12: "Multilevel Linear Models."

[^7]: Raudenbush, S. W., & Bryk, A. S. (2002). *Hierarchical Linear Models: Applications and Data Analysis Methods* (2nd ed.). Sage Publications.

[^8]: Kruschke, J. K. (2015). *Doing Bayesian Data Analysis: A Tutorial with R, JAGS, and Stan* (2nd ed.). Academic Press.

[^9]: McElreath, R. (2020). *Statistical Rethinking: A Bayesian Course with Examples in R and Stan* (2nd ed.). CRC Press. Chapter 13: "Models with Memory."

[^10]: Valencia, R. R. (2010). *Dismantling Contemporary Deficit Thinking: Educational Thought and Practice*. Routledge.

[^11]: VanVoorhis, C. R. W., & Morgan, B. L. (2007). "Understanding power and rules of thumb for determining sample sizes." *Tutorials in Quantitative Methods for Psychology*, 3(2), 43-50.

[^12]: Simpson, E. H. (1951). "The interpretation of interaction in contingency tables." *Journal of the Royal Statistical Society: Series B*, 13(2), 238-241.

[^13]: Robinson, W. S. (1950). "Ecological correlations and the behavior of individuals." *American Sociological Review*, 15(3), 351-357.

[^14]: NCES Kids' Zone. "Ask a Question: What is an Ecological Fallacy?" https://nces.ed.gov/nceskids/help/user_guide/graph/variables.asp

[^15]: Snijders, T. A. B., & Bosker, R. J. (2012). *Multilevel Analysis: An Introduction to Basic and Advanced Multilevel Modeling* (2nd ed.). Sage Publications.

[^16]: Efron, B., & Morris, C. (1977). "Stein's paradox in statistics." *Scientific American*, 236(5), 119-127.

[^17]: Chung, Y., Rabe-Hesketh, S., Dorie, V., Gelman, A., & Liu, J. (2013). "A nondegenerate penalized likelihood estimator for variance parameters in multilevel models." *Psychometrika*, 78(4), 685-709.

[^18]: Gelman, A. (2006). "Prior Distributions for Variance Parameters in Hierarchical Models." *Bayesian Analysis*, 1(3), 515-533. https://sites.stat.columbia.edu/gelman/research/published/taumain.pdf

[^19]: Harwell, M., & LeBeau, B. (2010). "Student eligibility for a free lunch as an SES measure in education research." *Educational Researcher*, 39(2), 120-131.

[^20]: U.S. Census Bureau. "Small Area Income and Poverty Estimates (SAIPE)." https://www.census.gov/programs-surveys/saipe.html

[^21]: National Research Council. (2010). *Getting Value Out of Value-Added: Report of a Workshop*. National Academies Press. https://nap.nationalacademies.org/read/12820/chapter/3

[^22]: Kruschke, J. K. (2015). "Highest Density Interval." In *Doing Bayesian Data Analysis* (2nd ed.). Academic Press.

[^23]: Vehtari, A., Gelman, A., Simpson, D., Carpenter, B., & Bürkner, P. C. (2021). "Rank-normalization, folding, and localization: An improved R-hat for assessing convergence of MCMC." *Bayesian Analysis*, 16(2), 667-718. https://arxiv.org/abs/1903.08008

[^24]: Gelman, A., Meng, X. L., & Stern, H. (1996). "Posterior predictive assessment of model fitness via realized discrepancies." *Statistica Sinica*, 6(4), 733-760.

[^25]: Vehtari, A., Gelman, A., & Gabry, J. (2017). "Practical Bayesian model evaluation using leave-one-out cross-validation and WAIC." *Statistics and Computing*, 27(5), 1413-1432.

[^26]: Betancourt, M. (2017). "A Conceptual Introduction to Hamiltonian Monte Carlo." https://arxiv.org/abs/1701.02434

[^27]: Salvatier, J., Wiecki, T. V., & Fonnesbeck, C. (2016). "Probabilistic programming in Python using PyMC3." *PeerJ Computer Science*, 2, e55. https://www.pymc.io/

[^28]: Kumar, R., Carroll, C., Hartikainen, A., & Martin, O. (2019). "ArviZ a unified library for exploratory analysis of Bayesian models in Python." *Journal of Open Source Software*, 4(33), 1143. https://python.arviz.org/

[^29]: Penn State STAT 501. "Detecting Outliers." https://online.stat.psu.edu/stat501/lesson/10/10.2

[^30]: Depaoli, S., & van de Schoot, R. (2017). "Improving transparency and replication in Bayesian statistics: The WAMBS-Checklist." *Psychological Methods*, 22(2), 240-261.

[^31]: Cohen, J. (1988). *Statistical Power Analysis for the Behavioral Sciences* (2nd ed.). Lawrence Erlbaum Associates.

---

## Appendix A: Alternative Frameworks Considered

Before settling on Hierarchical Bayesian modeling, we evaluated five statistical frameworks from the literature[^32]:

| Framework | Minimum n | Handles Simpson's Paradox | Uses State Data | Uncertainty | Selected? |
|-----------|-----------|--------------------------|-----------------|-------------|-----------|
| Standard OLS | 58+ | No | No | Misleading CIs | No |
| Robust Regression | 25+ | No | No | Bootstrap CIs | No |
| Rank-Based | 15-20 | Partial | No | Limited | No |
| Empirical Bayes | 20+ | Partial | Hyperparameters | Underestimates | No |
| **Hierarchical Bayes** | **6-10** | **Yes** | **Full** | **Full posterior** | **Yes** |

[^32]: Based on framework comparison in "Statistical Frameworks for Identifying Positive Deviant Schools with Small Samples and Differing Correlation Patterns" (2025).

---

## Appendix B: Model Diagnostics Output

### Convergence Diagnostics

```
================================================================================
CONVERGENCE DIAGNOSTICS
================================================================================

1. DIVERGENCE CHECK
------------------------------------------------------------
  ✓ No divergences detected (0/8000)

2. R-HAT VALUES (should be < 1.01)
------------------------------------------------------------
  ✓ mu_state: 1.0003
  ✓ sigma_district: 1.0012
  ✓ sigma_school: 1.0008
  ✓ sigma_y: 1.0001
  ✓ beta (max): 1.0015

3. BULK EFFECTIVE SAMPLE SIZE (should be > 400)
------------------------------------------------------------
  ✓ mu_state: 5234
  ✓ sigma_district: 4102
  ✓ sigma_school: 4567
  ✓ sigma_y: 6789

4. TAIL EFFECTIVE SAMPLE SIZE (should be > 400)
------------------------------------------------------------
  ✓ mu_state: 4123
  ✓ sigma_district: 3456
  ✓ sigma_school: 3789
  ✓ sigma_y: 5234

================================================================================
✓ CONVERGENCE GOOD - All diagnostics passed
================================================================================
```

### Posterior Predictive Checks

```
================================================================================
POSTERIOR PREDICTIVE CHECKS
================================================================================

Test Statistic: MEAN
  Observed mean: 91.23
  Replicated mean (median): 91.18
  Bayesian p-value: 0.482
  ✓ Model captures mean well

Test Statistic: STANDARD DEVIATION
  Observed SD: 6.45
  Replicated SD (median): 6.52
  Bayesian p-value: 0.534
  ✓ Model captures variance well

================================================================================
✓ POSTERIOR PREDICTIVE CHECKS PASSED
================================================================================
```

---

## Appendix C: Code Repository Structure

```
ky-education-kpi-pipeline/
├── analysis/
│   ├── bayesian_models/
│   │   ├── base_hierarchical_model.py    # Abstract base class with county-varying slopes
│   │   ├── base_model.py                 # Legacy base (deprecated)
│   │   ├── graduation_rate_model.py      # Graduation rate indicator
│   │   ├── reading_grade3_model.py       # 3rd grade reading proficiency
│   │   ├── math_grade8_model.py          # 8th grade math proficiency
│   │   ├── kindergarten_readiness_model.py
│   │   ├── postsecondary_enrollment_model.py
│   │   ├── postsecondary_readiness_model.py
│   │   ├── chronic_absenteeism_model.py
│   │   ├── el_progress_elementary_model.py
│   │   ├── el_progress_middle_model.py
│   │   ├── el_progress_high_model.py
│   │   ├── school_climate_model.py
│   │   ├── combine_results.py            # Combines all model outputs to JSON
│   │   ├── identify_bright_spots.py      # Posterior analysis (legacy)
│   │   └── visualize_results.py          # Forest plots, diagnostics
│   ├── config/
│   │   ├── __init__.py                   # Covariate catalog loader utilities
│   │   ├── covariate_catalog.yaml        # Master catalog of all covariates
│   │   └── student_groups.py             # QuantCrit student group definitions
│   ├── prior_analysis/
│   │   ├── indicators/
│   │   │   ├── graduation_rate/
│   │   │   │   └── historical_review.py  # Empirical prior generation
│   │   │   ├── chronic_absenteeism/
│   │   │   ├── reading_grade3/
│   │   │   └── [other indicators]/
│   │   └── aggregate_priors.py           # Combine all indicator priors
│   ├── datasets/
│   │   ├── graduation_analysis.csv       # Combined model data
│   │   ├── reading_grade3_analysis.csv
│   │   ├── math_grade8_analysis.csv
│   │   ├── [other indicator]_analysis.csv
│   │   └── demographic_predictors.csv    # Student demographics
│   ├── outputs/
│   │   ├── prior_analysis/
│   │   │   ├── graduation_rate/
│   │   │   │   ├── all_students/
│   │   │   │   │   └── recommended_priors.csv   # Group-specific priors
│   │   │   │   ├── african_american/
│   │   │   │   ├── economically_disadvantaged/
│   │   │   │   └── [other student groups]/
│   │   │   └── [other indicators]/
│   │   ├── models/
│   │   │   ├── graduation/
│   │   │   │   ├── all_students/             # Per-group model outputs
│   │   │   │   │   ├── graduation_trace.nc   # MCMC trace (ArviZ format)
│   │   │   │   │   ├── model_summary.csv     # Posterior parameter summaries
│   │   │   │   │   ├── covariate_effects.csv # Global beta coefficients
│   │   │   │   │   ├── county_covariate_effects.csv  # County-specific slopes
│   │   │   │   │   └── school_effects.csv    # School-level random effects
│   │   │   │   ├── african_american/
│   │   │   │   └── [other student groups]/
│   │   │   ├── reading_grade3/
│   │   │   ├── math_grade8/
│   │   │   └── [other indicators]/
│   │   └── diagnostics/
│   │       └── [indicator]/
│   │           ├── prior_predictive_check.png
│   │           └── posterior_predictive_check.png
│   ├── create_analysis_dataset.py        # Data preparation pipeline
│   ├── derive_demographics.py            # Demographic predictor extraction
│   └── BRIGHT_SPOTS_METHODOLOGY.md       # This document
├── data/
│   ├── bayesian/
│   │   └── bayesian_results.json         # Combined results for dashboard
│   ├── kpi/
│   │   └── kpi_master.csv                # All KDE indicators
│   └── external/
│       ├── census_saipe/                 # County-level economic data
│       ├── census_acs/
│       │   ├── census_acs_tracts_statewide_2022.csv  # 1,306 KY tracts
│       │   └── census_acs_tracts_fayette_2022.csv    # Fayette County tracts
│       └── school_tracts/
│           └── school_tracts_statewide_2021.csv      # 1,478 schools geocoded
└── etl/
    ├── school_tract_geocoding.py         # School-to-tract geocoding
    ├── census_acs.py                     # Census ACS 5-Year ETL
    ├── census_saipe.py                   # Census SAIPE ETL
    └── [other ETL pipelines]
```

### Output Files

Each model produces the following output files in `analysis/outputs/models/{indicator}/`:

| File | Description |
|------|-------------|
| `{indicator}_trace.nc` | Full MCMC trace in NetCDF format (ArviZ compatible) |
| `model_summary.csv` | Posterior summary statistics for key parameters |
| `school_effects.csv` | School-level random effects with 95% credible intervals and pooling diagnostics |
| `covariate_effects.csv` | Global predictor effects with shrinkage factors and interpretation flags |
| `county_covariate_effects.csv` | County-specific predictor effects with both statewide and county interpretation |

The `combine_results.py` script aggregates all model outputs into `data/bayesian/bayesian_results.json` for consumption by the Angular dashboard.

### Covariate Effect Interpretation Flags

Covariate effects include interpretation flags to help identify when effects require caveats or careful interpretation:

**`statewide_interpretation`**: Based on whether the statewide 95% CI excludes zero, combined with bivariate correlation analysis to detect suppression effects:
- `direct_effect`: CI excludes zero, same sign as bivariate correlation (safe to report)
- `suppressed_sign_flip`: CI excludes zero, but opposite sign from bivariate correlation (RED FLAG - requires strong caveat)
- `suppressed_weakened`: Same sign but model effect is <50% of bivariate correlation (multicollinearity weakened the effect)
- `conditional_only`: Near-zero bivariate correlation but significant model effect (effect only emerges after controlling for other factors)
- `not_significant`: 95% CI includes zero

**`county_interpretation`**: Same categories, but computed using the county-specific CI instead of the statewide CI. This is critical for county-focused reporting because:
- A predictor may be `not_significant` statewide but significant in a specific county (or vice versa)
- The dashboard should use `county_interpretation` when displaying Fayette-specific effects
- The dashboard should use `statewide_interpretation` only when showing statewide effects

The `county_covariate_effects.csv` includes both interpretation fields to enable proper caveat display regardless of which effect is being shown.

### Pooling Diagnostics

The `school_effects.csv` includes two pooling diagnostics that quantify how much each school's estimate is influenced by its own data vs. the statewide pattern:

**Global Pooling Factor (λ)**:
$$\lambda = \frac{\sigma^2_{school}}{\sigma^2_{school} + \sigma^2_y}$$

This ratio indicates how much weight is given to school-specific data across the model:
- **λ ≥ 0.7**: Estimates rely mostly on each school's own data (minimal shrinkage)
- **0.4 ≤ λ < 0.7**: Estimates balance school data with statewide patterns (moderate shrinkage)
- **λ < 0.4**: Estimates are heavily stabilized using statewide patterns (strong shrinkage)

The pooling factor is computed once per model and applies to all schools in that indicator.

**School-Specific Reliability**:
$$reliability_j = 1 - \frac{\sigma_{posterior,j}}{\sigma_{prior}}$$

This measures how much a school's estimate improved from prior to posterior:
- **≥ 0.5**: High reliability—the data substantially informed the estimate
- **0.25–0.5**: Medium reliability—moderate data contribution
- **< 0.25**: Low reliability—estimate relies heavily on borrowing from other schools

Schools with low reliability (shown with "⚡ Less certain" badges in the dashboard) should be interpreted with more caution, as their estimates are driven more by the statewide average than their own performance data.

---

*This methodology was developed in collaboration with the Fayette County Board of Education Equity Council Commission and follows guidelines from the Bayesian Analysis Reporting Guidelines (BARG)[^33] and best practices from the Stan and PyMC communities.*

[^33]: Kruschke, J. K., & Liddell, T. M. (2018). "The Bayesian New Statistics: Hypothesis testing, estimation, meta-analysis, and power analysis from a Bayesian perspective." *Psychonomic Bulletin & Review*, 25(1), 178-206.

[^34]: Based on Census ACS 5-Year Estimates (2018-2022) for Fayette County census tracts. See `data/external/census_acs/census_acs_tracts_fayette_2022.csv`.

[^35]: U.S. Census Bureau. "Census Geocoder." https://geocoding.geo.census.gov/geocoder/. The geocoder accepts latitude/longitude coordinates and returns geographic identifiers including state, county, and tract FIPS codes.

[^36]: Statewide geocoding completed November 25, 2025. See `etl/school_tract_geocoding.py` for implementation and `data/external/school_tracts/school_tracts_statewide_2021.csv` for output.

[^37]: EdWorkingPapers (2025). "Heterogeneous Effects of Closing the Digital Divide During COVID-19 on Student Engagement and Achievement." https://edworkingpapers.com/ai25-1153. Key finding: broadband access boosted achievement for high-performing students but reduced engagement and achievement for low-performing pupils.

[^38]: Van de Schoot, R., & Miočević, M. (Eds.). (2020). *Small Sample Size Solutions: A Guide for Applied Researchers and Practitioners*. Routledge. Chapter 10: "Eliciting Informative Priors by Pooling Data from Similar Studies." https://doi.org/10.4324/9780429273872

[^39]: Piironen, J., & Vehtari, A. (2017). "Sparsity information and regularization in the horseshoe and other shrinkage priors." *Electronic Journal of Statistics*, 11(2), 5018-5051. https://doi.org/10.1214/17-EJS1337SI. Introduces the regularized (Finnish) horseshoe with slab regularization and the data-dependent τ₀ formula.

[^40]: Polson, N. G., & Scott, J. G. (2012). "On the half-Cauchy prior for a global scale parameter." *Bayesian Analysis*, 7(4), 887-902. https://doi.org/10.1214/12-BA730. Original development of the horseshoe prior for sparse signal recovery.

[^41]: Gelman, A., Simpson, D., & Betancourt, M. (2017). "The prior can often only be understood in the context of the likelihood." *Entropy*, 19(10), 555. https://doi.org/10.3390/e19100555. Discusses principled approaches to prior specification using data to inform weakly informative priors.
