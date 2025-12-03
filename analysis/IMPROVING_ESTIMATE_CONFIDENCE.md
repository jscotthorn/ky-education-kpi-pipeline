# Improving Bayesian Estimate Confidence: An Evidence-Based Guide

This document provides academically-grounded strategies for improving the precision and confidence of school-level Bayesian estimates in the FCPS Equity Dashboard bright spots analysis.

## Table of Contents

1. [Understanding the Problem](#understanding-the-problem)
2. [How Confidence Is Currently Calculated](#how-confidence-is-currently-calculated)
3. [Root Causes of Low/Medium Confidence](#root-causes-of-lowmedium-confidence)
4. [Evidence-Based Strategies](#evidence-based-strategies)
   - [Strategy 1: Empirically-Informed Priors](#strategy-1-empirically-informed-priors)
   - [Strategy 2: Optimize the Regularized Horseshoe Prior](#strategy-2-optimize-the-regularized-horseshoe-prior)
   - [Strategy 3: Tune Hierarchical Variance Priors](#strategy-3-tune-hierarchical-variance-priors)
   - [Strategy 4: Increase Partial Pooling](#strategy-4-increase-partial-pooling)
   - [Strategy 5: Reduce Model Complexity](#strategy-5-reduce-model-complexity)
   - [Strategy 6: Extend Temporal Coverage](#strategy-6-extend-temporal-coverage)
5. [Implementation Recommendations](#implementation-recommendations)
6. [References](#references)

---

## Understanding the Problem

The confidence issues observed in the FCPS Equity Dashboard are fundamentally a **small area estimation** problem. With only 6 Fayette County high schools and limited years of demographic-specific data, we are in a well-documented statistical scenario where precision is inherently constrained.

As documented by Rao (1999) in the *Survey Methodology* journal:

> "Direct survey estimates for small areas are likely to yield unacceptably large standard errors due to the smallness of sample sizes in the areas. This makes it necessary to **borrow strength** from related areas to find more accurate estimates for a given area."

The hierarchical Bayesian model already implements "borrowing strength" through partial pooling—this is exactly the right methodological approach. The question is how to optimize this borrowing to achieve tighter credible intervals while maintaining statistical validity.

---

## How Confidence Is Currently Calculated

### School Effect Reliability

The reliability metric measures how much the posterior estimate has been informed by the data versus the prior:

```python
school_reliability = 1 - (posterior_std / prior_std)
```

**Interpretation:**
- **High reliability (≥ 0.5)**: Posterior variance reduced by >50% from prior—estimate is well-informed by data
- **Medium reliability (0.25–0.5)**: Moderate data information
- **Low reliability (< 0.25)**: Estimate relies heavily on borrowing from other schools

### Covariate Effect Confidence

For predictor effects, confidence is based on the ratio of effect size to credible interval width:

```python
ratio = |effect_mean| / ci_width
```

**Interpretation:**
- **High confidence (ratio > 0.8)**: Effect is large relative to uncertainty
- **Medium confidence (0.4–0.8)**: Moderate precision
- **Low confidence (≤ 0.4)**: Wide uncertainty relative to effect size

---

## Root Causes of Low/Medium Confidence

| Factor | Impact | Example |
|--------|--------|---------|
| **Small sample size** | High | Only 6 Fayette high schools |
| **Limited years of data** | High | Postsecondary readiness: 2022–2025 (4 years, ~24 obs/school) |
| **Demographic subgroups** | High | Fewer students per group → more suppression → fewer valid observations |
| **Model complexity** | Medium | County-varying slopes: 120 counties × n predictors = many extra parameters |
| **High outcome variance** | Medium | Some indicators have high natural variability |

---

## Evidence-Based Strategies

### Strategy 1: Empirically-Informed Priors

**Impact: HIGH | Effort: MEDIUM**

#### Academic Foundation

Van de Schoot & Miočević (2017) in *Applied Developmental Science* demonstrate that:

> "In Bayesian estimation, prior information can be included, which increases the precision of the posterior distribution... The use of properly constructed informative hyperpriors can enhance parameter estimates, especially with small sample sizes."

The Frontiers in Psychology study (2020) on multilevel latent variable models confirms:

> "Bayesian approaches for estimating multilevel models can be beneficial in small samples. Prior distributions can be used to overcome small sample problems when priors that increase the accuracy of estimation are chosen."

#### Implementation

The `analysis/prior_analysis/` directory contains historical review scripts that compute distributional parameters from historical data. These can inform tighter priors:

```python
# Current (weakly informative):
mu_state ~ Normal(0, 10)
sigma_school ~ HalfCauchy(5)

# Empirically-informed (from historical review):
mu_state ~ Normal(historical_mean, historical_std * 0.5)
sigma_school ~ HalfCauchy(historical_between_school_sd)
```

#### Cautions

The Frontiers study notes an important caveat:

> "When a researcher is very certain about an incorrect belief, adopting an incorrect informative prior may distort posterior estimates."

However, research shows that even inaccurate priors with relatively large variances still yield more precise estimates than noninformative priors. The key is to use priors informed by historical data rather than pure guesswork.

---

### Strategy 2: Optimize the Regularized Horseshoe Prior

**Impact: MEDIUM | Effort: LOW**

#### Academic Foundation

Piironen & Vehtari (2017) in the *Electronic Journal of Statistics* introduced the regularized horseshoe (also called the "Finnish horseshoe"):

> "The regularized horseshoe allows us to specify a minimum level of regularization to the largest values... The ability to regularize those parameters that are far from zero is useful especially when the parameters are only weakly identified by the data."

The key innovation is the concept of **effective number of nonzero parameters** (`m_eff`):

> "We introduce a concept of effective number of nonzero parameters, show an intuitive way of formulating the prior for the global hyperparameter based on the sparsity assumptions, and argue that the previous default choices are dubious based on their tendency to favor solutions with more unshrunk parameters than we typically expect a priori."

#### Implementation

The global shrinkage parameter `tau` should be set based on expected sparsity:

```python
# Set tau0 based on expected sparsity
m_eff = 6  # Expected number of non-zero coefficients out of 27
tau0 = (m_eff / (n_predictors - m_eff)) * (sigma_y / np.sqrt(n))

# Prior specification
tau ~ HalfCauchy(tau0)
```

With 27 predictors, if domain knowledge suggests only 5–8 are truly important, setting `m_eff = 6` provides appropriate regularization. This gives tighter estimates for weakly-supported coefficients while allowing strong effects to remain unshrunk.

---

### Strategy 3: Tune Hierarchical Variance Priors

**Impact: MEDIUM | Effort: LOW**

#### Academic Foundation

Gelman (2006) in *Bayesian Analysis* established best practices for variance parameter priors:

> "I recommend working within the half-t family of prior distributions, which are more flexible and have better behavior near 0, compared to the inverse-gamma family. A reasonable starting point is the half-Cauchy family, with scale set to a value that is high but not off the scale."

Polson & Scott (2012) reinforced this recommendation:

> "The half-Cauchy distribution should replace the inverse-Gamma distribution as a default prior for a top-level scale parameter in Bayesian hierarchical models."

#### Scale Parameter Guidelines

From Gelman (2006):

> "Gelman suggested half-Cauchy with mode at 0 and scale set to a large value (in the 8-schools example, they used the value 25), or with the scale estimated from data in a hierarchical-hierarchical setting."

**Practical guidance:**
- Scale of 25: Appropriate when variance is expected to be large
- Scale of 1–5: Appropriate for standardized data

For outcomes on a 0–100% scale (like proficiency rates), a `HalfCauchy(2–4)` is more appropriate than `HalfCauchy(5–10)`.

#### Implementation

```python
# Current setting (may be too diffuse):
sigma_school ~ HalfCauchy(5)

# Tighter setting for percentage-scale outcomes:
sigma_school ~ HalfCauchy(3)
```

---

### Strategy 4: Increase Partial Pooling

**Impact: MEDIUM | Effort: LOW**

#### Academic Foundation

Gelman & Hill (2006) in *Data Analysis Using Regression and Multilevel/Hierarchical Models* explain the mechanics of partial pooling:

> "In the partially-pooled model, estimates in small-sample-size counties are informed by the population parameters – hence more precise estimates. Moreover, the smaller the sample size, the more regression towards the overall mean – hence less extreme estimates."

The PyMC documentation elaborates:

> "This allows information to be pooled across participants such that each individual-level estimate influences its corresponding group-level mean and standard deviation estimates, which in turn influence all other individual-level estimates. This interplay between the individual- and group-level parameters is the hierarchical pooling, a core feature of hierarchical models, which increases the precision of individual-level estimates."

#### The Shrinkage Mechanism

As explained in the Stan documentation on hierarchical models:

> "The individual county effects are distributed around a county mean, with a spread controlled by the hierarchical standard deviation parameter. This constraint serves to shrink county estimates toward the overall mean, to a degree proportional to the county sample size."

#### Implementation

For Fayette schools with limited data, stronger pooling can be achieved by using smaller scale parameters:

```python
# Current: More variation allowed
sigma_school ~ HalfCauchy(4)

# Stronger pooling: More shrinkage toward mean
sigma_school ~ HalfCauchy(2)
```

**Trade-off:** Stronger pooling produces narrower credible intervals but also pulls estimates closer to the statewide mean, potentially underestimating true outliers.

---

### Strategy 5: Reduce Model Complexity

**Impact: MEDIUM | Effort: LOW**

#### Academic Foundation

The Fay-Herriot model for small area estimation achieves precision through shared parameters. From Rao (1999):

> "The Fay-Herriot model enhances the precision of SAE by 'borrowing' strength across areas, employing what is known as 'linking' models. This improvement is achieved by assuming shared regression coefficients in the linking model for area totals."

The principle is that **fewer parameters to estimate** means **more data per parameter** and therefore **tighter estimates**.

#### Implementation

The current model includes county-varying slopes, adding `120 × n_predictors` parameters. For indicators with limited Fayette data, consider:

```python
# Full model with county-varying slopes
model.run(county_varying_slopes=True)  # Many parameters

# Simplified model without county-varying slopes
model.run(county_varying_slopes=False)  # Fewer parameters, tighter estimates
```

This maintains school-level random effects while eliminating county-specific predictor slopes, substantially reducing the parameter count.

---

### Strategy 6: Extend Temporal Coverage

**Impact: HIGH | Effort: MEDIUM**

#### Academic Foundation

Research on sparse data in educational settings (PMC, 2021) confirms:

> "Sparse data are common in online educational environments. Bayesian hierarchical priors are used to overcome the sparse data and small sample problems."

The most direct way to increase effective sample size is adding more years of observation.

#### Implementation

For postsecondary readiness, the current year range is 2022–2025 (4 years). If earlier data exists in the KPI master file:

```python
# In postsecondary_readiness_analysis.py
def get_year_range(self) -> tuple:
    # Extended from (2022, 2025)
    return (2019, 2025)  # 7 years if data available
```

**Note:** Data quality and comparability across years should be verified before extending the range.

---

## Implementation Recommendations

### Priority Order

| Priority | Strategy | Rationale |
|----------|----------|-----------|
| 1 | **Extend temporal coverage** | Direct sample size increase; highest impact if data available |
| 2 | **Empirically-informed priors** | Well-supported in literature; uses existing historical review infrastructure |
| 3 | **Tune horseshoe `m_eff`** | Low effort; aligns regularization with domain knowledge |
| 4 | **Adjust variance prior scales** | Low effort; appropriate scaling for percentage-scale outcomes |
| 5 | **Disable county-varying slopes** | Trade-off: loses county-specific interpretation for precision |

### Recommended Prior Settings

Based on the literature review, the following prior settings are recommended for percentage-scale educational outcomes:

```python
# State-level intercept (informed by historical mean)
mu_state ~ Normal(historical_mean, max(5, historical_std * 0.7))

# School-level variance (appropriate scale for percentages)
sigma_school ~ HalfCauchy(3)

# County-level variance
sigma_county ~ HalfCauchy(2)

# Residual variance
sigma ~ HalfCauchy(5)

# Horseshoe global shrinkage (assuming ~6 of 27 predictors are important)
m_eff = 6
tau0 = (m_eff / (27 - m_eff)) * (sigma_y / np.sqrt(n))
tau ~ HalfCauchy(tau0)

# Regularized horseshoe slab scale (for non-zero effects)
c2 ~ InverseGamma(2, 8)  # Regularizes large effects
```

### Sensitivity Analysis

Before deploying new prior settings, run sensitivity analyses:

1. Compare posterior distributions under current vs. proposed priors
2. Check that credible intervals shrink without substantially changing point estimates
3. Verify that known relationships (e.g., poverty → lower outcomes) are preserved
4. Examine leave-one-out cross-validation metrics

---

## References

### Primary Academic Sources

1. **Gelman, A. (2006).** Prior distributions for variance parameters in hierarchical models (comment on article by Browne and Draper). *Bayesian Analysis*, 1(3), 515–534.
   - URL: https://projecteuclid.org/journals/bayesian-analysis/volume-1/issue-3/Prior-distributions-for-variance-parameters-in-hierarchical-models-comment-on/10.1214/06-BA117A.full
   - PDF: https://sites.stat.columbia.edu/gelman/research/published/taumain.pdf

2. **Piironen, J., & Vehtari, A. (2017).** Sparsity information and regularization in the horseshoe and other shrinkage priors. *Electronic Journal of Statistics*, 11(2), 5018–5051.
   - URL: https://projecteuclid.org/journals/electronic-journal-of-statistics/volume-11/issue-2/Sparsity-information-and-regularization-in-the-horseshoe-and-other-shrinkage/10.1214/17-EJS1337SI.full
   - arXiv: https://arxiv.org/abs/1707.01694

3. **Van de Schoot, R., & Miočević, M. (2017).** Where do priors come from? Applying guidelines to construct informative priors in small sample research. *Applied Developmental Science*.
   - URL: https://www.tandfonline.com/doi/full/10.1080/15427609.2017.1370966

4. **Polson, N. G., & Scott, J. G. (2012).** On the half-Cauchy prior for a global scale parameter. *Bayesian Analysis*, 7(4), 887–902.
   - URL: https://projecteuclid.org/journals/bayesian-analysis/volume-7/issue-4/On-the-Half-Cauchy-Prior-for-a-Global-Scale-Parameter/10.1214/12-BA730.pdf
   - arXiv: https://arxiv.org/abs/1104.4937

5. **Rao, J. N. K. (1999).** Some recent advances in model-based small area estimation. *Survey Methodology*, 25(2), 175–186.
   - URL: https://www150.statcan.gc.ca/n1/pub/12-001-x/1999002/article/4880-eng.pdf

6. **Gelman, A., & Hill, J. (2006).** *Data Analysis Using Regression and Multilevel/Hierarchical Models*. Cambridge University Press.
   - Publisher: https://www.cambridge.org/core/books/data-analysis-using-regression-and-multilevelhierarchical-models/32A29531C7FD730C3A68951A17C9D983

### Supporting Sources

7. **Frontiers in Psychology (2020).** Prior specification for more stable Bayesian estimation of multilevel latent variable models in small samples: A comparative investigation of two different approaches.
   - URL: https://www.frontiersin.org/journals/psychology/articles/10.3389/fpsyg.2020.611267/full

8. **PMC (2021).** Bayesian hierarchical multidimensional item response modeling of small sample, sparse data for personalized developmental surveillance.
   - URL: https://pmc.ncbi.nlm.nih.gov/articles/PMC8377345/

9. **PMC (2015).** The use of sampling weights in Bayesian hierarchical models for small area estimation.
   - URL: https://pmc.ncbi.nlm.nih.gov/articles/PMC4357363/

10. **ScienceDirect (2025).** A Bayesian approach to small samples: Mixed-effects modeling in L2 interventional research.
    - URL: https://www.sciencedirect.com/science/article/abs/pii/S2772766125000527

### Implementation Resources

11. **PyMC Documentation.** A Primer on Bayesian Methods for Multilevel Modeling.
    - URL: https://www.pymc.io/projects/examples/en/stable/case_studies/multilevel_modeling.html

12. **Stan Documentation.** Hierarchical Partial Pooling for Repeated Binary Trials.
    - URL: https://mc-stan.org/learn-stan/case-studies/pool-binary-trials.html

13. **Stan Wiki.** Prior Choice Recommendations.
    - URL: https://github.com/stan-dev/stan/wiki/Prior-Choice-Recommendations

14. **Austin Rochford.** The Hierarchical Regularized Horseshoe Prior in PyMC3.
    - URL: https://austinrochford.com/posts/2021-05-29-horseshoe-pymc3.html

15. **Aki Vehtari.** Regularized Horseshoe Slides.
    - URL: https://avehtari.github.io/modelselection/regularizedhorseshoe_slides.pdf

### Additional Reading

16. **Gelman, A., & Pardoe, I. (2006).** Bayesian measures of explained variance and pooling in multilevel (hierarchical) models. *Technometrics*, 48(2), 241–251.
    - URL: https://www.tandfonline.com/doi/abs/10.1198/004017005000000517

17. **U.S. Census Bureau.** Small Area Estimation.
    - URL: https://www.census.gov/topics/research/stat-research/expertise/small-area-est.html

18. **arXiv (2025).** Small Area Estimation of Education Levels in Low- and Middle-Income Countries.
    - URL: https://arxiv.org/html/2502.07946

---

## Document History

- **Created:** 2025-11-30
- **Author:** FCPS Equity Analysis Team
- **Purpose:** Evidence-based guidance for improving Bayesian estimate precision in small-sample educational research
