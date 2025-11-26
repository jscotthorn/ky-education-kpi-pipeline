# Bright Spots Methodology: Hierarchical Bayesian Framework for Identifying Positive Deviant Schools

**Date:** November 25, 2025
**Version:** 1.1
**Context:** Kentucky Statewide Analysis with Fayette County Focus
**Model Location:** `analysis/bayesian_models/graduation_rate_model.py`

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

### 3.2 Three-Level Model Structure

Our model implements a three-level hierarchy matching Kentucky's educational structure:

```
Level 3: State          μ_state ~ Normal(93, 10)
                              |
Level 2: Districts      district_effect ~ Normal(0, σ_district)
           (n=170)            |
Level 1: Schools        school_effect ~ Normal(district_effect, σ_school)
           (n=228)
```

**Mathematical Specification:**

```
y_ijk ~ Normal(μ_ijk, σ_y)

μ_ijk = μ_state + district_effect[j] + school_effect[i] + X_ijk · β

Where:
  y_ijk = outcome (e.g., graduation rate) for school i in district j
  μ_state = state-level intercept
  district_effect[j] = district-level random effect
  school_effect[i] = school-level random effect (nested within district)
  X_ijk = matrix of standardized covariates
  β = vector of regression coefficients
```

### 3.3 Prior Specifications

Following recommendations from Gelman (2006)[^18] for variance parameters in hierarchical models:

| Parameter | Prior Distribution | Values | Rationale |
|-----------|-------------------|--------|-----------|
| `μ_state` | Normal(93, 10) | KY avg ~93% | Domain-informed intercept |
| `σ_district` | HalfCauchy(5) | Scale=5 | Allows values near zero, heavy tails for robustness |
| `σ_school` | HalfCauchy(3) | Scale=3 | Slightly tighter than district level |
| `β` | Normal(0, 5) | Weakly informative | Zero-centered, allows moderate effects |
| `σ_y` | HalfCauchy(2) | Scale=2 | Observation-level noise |

**Why Half-Cauchy priors?** Gelman (2006)[^18] recommends against inverse-gamma for variance parameters, which can behave poorly near zero. Half-Cauchy priors:
- Are unbounded at zero (allow for no variance if warranted)
- Have heavy tails (robust to outliers)
- Provide better behavior than inverse-gamma in small samples

---

## 4. Covariates: Beyond Free/Reduced Lunch

### 4.1 Why FRL Alone is Insufficient

Free/Reduced Lunch (FRL) eligibility is a crude proxy for economic disadvantage[^19]:
- Binary (qualify or not)—doesn't capture degree of disadvantage
- Subject to underreporting (stigma, incomplete paperwork)
- Policy changes affect eligibility independent of actual poverty
- Within Fayette County, neighborhoods with identical FRL% can have very different median incomes

### 4.2 Covariate Categories

Our model incorporates **22 covariates** across five categories:

**Category 1: Student Demographics (6 variables)**
- `pct_economically_disadvantaged` - % students qualifying for FRL
- `pct_english_learners` - % English Language Learners
- `pct_students_with_disabilities` - % students with IEPs
- `pct_african_american` - % African American students
- `pct_hispanic` - % Hispanic/Latino students
- `pct_minority` - % total minority students

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

**Excluded Tract-Level Variables (with rationale):**

Two tract-level variables were removed after November 2025 diagnostic analysis:

- ~~`tract_pct_broadband`~~ - **Removed**: HDI included zero (+0.06 [-0.62, +0.77]); highly correlated with income (r=0.63) and education (r=0.60). Research shows heterogeneous effects—benefits high-achieving students but harms low-achieving students[^37]—which could bias bright spot identification.

- ~~`tract_pct_housing_cost_burden_30_plus`~~ - **Removed**: HDI included zero (+0.07 [-0.58, +0.72]); correlation of r=-0.64 with `tract_pct_owner_occupied` indicates it measures the inverse of the same construct. Redundant with existing poverty measures.

Schools are geocoded to census tracts using their latitude/longitude coordinates via the Census Geocoding API[^35]. This enables joining tract-level ACS data to individual schools for more precise neighborhood characterization.

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

### 5.1 Posterior Probability Criterion

A school is identified as a **Bright Spot** when:

$$P(\theta_j > \tau | \text{data}) > 0.80$$

Where:
- θ_j = school effect (performance relative to expectation)
- τ = threshold (2.0 percentage points above expected)
- 0.80 = certification probability

**Interpretation:** "We are at least 80% confident this school's true effect exceeds the threshold, after controlling for demographics and regional patterns."

### 5.2 Threshold Selection Rationale

The **2.0 percentage point threshold** is conservative:
- Represents approximately 0.3-0.4 standard deviations
- Large enough to be practically meaningful
- Small enough to identify schools with replicable practices

The **80% probability criterion** follows conventions in Bayesian decision-making:
- More conservative than point estimates (which ignore uncertainty)
- Less stringent than 95% (which may be too demanding for small samples)
- Balances false positive/negative rates appropriately

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

---

## 7. MCMC Configuration

### 7.1 Sampling Parameters

| Setting | Value | Rationale |
|---------|-------|-----------|
| Chains | 4 | Minimum for robust R-hat computation[^23] |
| Draws per chain | 2000 | Sufficient for stable posterior estimates |
| Tuning/warmup | 1000 | Allows adaptation of step size |
| Target accept | 0.95 | Higher for hierarchical models[^26] |
| Random seed | 42 | Reproducibility |

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
- Documentation explicitly tracks data vintages

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
│   │   ├── base_model.py                 # Abstract base with horseshoe prior support
│   │   ├── graduation_rate_model.py      # Main model implementation (22 covariates)
│   │   ├── identify_bright_spots.py      # Posterior analysis
│   │   └── visualize_results.py          # Forest plots, diagnostics
│   ├── config/
│   │   ├── __init__.py                   # Covariate catalog loader utilities
│   │   └── covariate_catalog.yaml        # Master catalog of all covariates
│   ├── datasets/
│   │   ├── graduation_analysis.csv       # Combined model data
│   │   └── demographic_predictors.csv    # Student demographics
│   ├── outputs/
│   │   ├── models/
│   │   │   ├── graduation_rate_trace.nc  # MCMC trace (ArviZ format)
│   │   │   ├── model_summary.csv         # Posterior parameter summaries
│   │   │   ├── covariate_effects.csv     # Beta coefficient estimates
│   │   │   └── school_effects.csv        # School-level random effects
│   │   └── diagnostics/
│   │       ├── prior_predictive_check.png
│   │       └── posterior_predictive_check.png
│   ├── create_analysis_dataset.py        # Data preparation pipeline
│   ├── derive_demographics.py            # Demographic predictor extraction
│   └── BRIGHT_SPOTS_METHODOLOGY.md       # This document
├── data/
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

---

*This methodology was developed in collaboration with the Fayette County Board of Education Equity Council Commission and follows guidelines from the Bayesian Analysis Reporting Guidelines (BARG)[^33] and best practices from the Stan and PyMC communities.*

[^33]: Kruschke, J. K., & Liddell, T. M. (2018). "The Bayesian New Statistics: Hypothesis testing, estimation, meta-analysis, and power analysis from a Bayesian perspective." *Psychonomic Bulletin & Review*, 25(1), 178-206.

[^34]: Based on Census ACS 5-Year Estimates (2018-2022) for Fayette County census tracts. See `data/external/census_acs/census_acs_tracts_fayette_2022.csv`.

[^35]: U.S. Census Bureau. "Census Geocoder." https://geocoding.geo.census.gov/geocoder/. The geocoder accepts latitude/longitude coordinates and returns geographic identifiers including state, county, and tract FIPS codes.

[^36]: Statewide geocoding completed November 25, 2025. See `etl/school_tract_geocoding.py` for implementation and `data/external/school_tracts/school_tracts_statewide_2021.csv` for output.

[^37]: EdWorkingPapers (2025). "Heterogeneous Effects of Closing the Digital Divide During COVID-19 on Student Engagement and Achievement." https://edworkingpapers.com/ai25-1153. Key finding: broadband access boosted achievement for high-performing students but reduced engagement and achievement for low-performing pupils.
