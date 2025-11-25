# Bayesian Bright Spots Model - Results & Findings

**Date:** November 24, 2025  
**Model:** Hierarchical Bayesian Regression (PyMC)  
**Outcome:** 4-Year Graduation Rate (High Schools)  
**Sample:** 228 Traditional (A1) High Schools, 2021-2024

## Executive Summary

We successfully implemented a three-level hierarchical Bayesian model to identify "bright spot" schools that outperform expectations given their student demographics. This approach was necessitated by the small sample size of high schools in Fayette County (n=6), which renders standard regression statistically invalid.

**Key Finding:** **Lafayette High School** is a certified bright spot, performing **3.3 percentage points above expected** after controlling for student demographics.

## Methodology

### Model Structure
- **Level 1 (Observation):** School-year graduation rates
- **Level 2 (School):** Schools nested within districts (Partial Pooling)
- **Level 3 (District):** Districts nested within state (Partial Pooling)
- **Predictors:** % Economically Disadvantaged, % English Learners, % Students with Disabilities, % African American, % Hispanic, % Minority

### Why This Matters
Standard regression treats each school as independent. With only 6 high schools in Fayette County, a standard model cannot reliably distinguish between "luck" (noise) and "skill" (true performance). 

Our Bayesian model "borrows strength" from 222 other high schools across Kentucky to create a stable baseline. If a Fayette school deviates significantly from this baseline, we can be confident it's a real signal.

## Results: Fayette County High Schools

Schools are ranked by their "School Effect" — the percentage points by which they exceed the expected graduation rate for their specific demographic composition.

| Rank | School Name | Effect (Above Expected) | Probability > 2% | Status |
|------|-------------|-------------------------|------------------|--------|
| 1 | **Lafayette High School** | **+3.32%** | **91.1%** | ✅ **Certified Bright Spot** |
| 2 | Frederick Douglass HS | +1.47% | 29.2% | Promising |
| 3 | Henry Clay HS | -0.59% | 0.5% | As Expected |
| 4 | Tates Creek HS | -2.38% | 0.0% | Below Expected |
| 5 | Paul Laurence Dunbar HS | -2.50% | 0.0% | Below Expected |
| 6 | Bryan Station HS | -2.76% | 0.0% | Below Expected |

### Interpretation
- **Lafayette HS** is the clear outlier. There is a **91% probability** that its true performance is at least 2 percentage points better than expected. Its 95% credible interval is entirely positive [+1.4%, +5.2%].
- **Frederick Douglass HS** is performing well (+1.5%), but we have less statistical certainty that this isn't due to normal variation.
- **Other Schools** are performing slightly below what the model predicts for their demographics, though mostly within the margin of error for "average" performance.

## Statewide Context

- **40 schools statewide** met the "Certified Bright Spot" criteria (Probability > 80% of effect > 2%).
- Top performers include:
  - **Louisville Male High** (Jefferson Co)
  - **duPont Manual High** (Jefferson Co)
  - **J. Graham Brown School** (Jefferson Co)
  - **Cordia School** (Knott Co)

## Visualizations

### 1. Caterpillar Plot (Fayette Focus)
![Caterpillar Plot](visualizations/caterpillar_plot.png)
*Shows school effects with 95% credible intervals. Lafayette (top red) is clearly separated from 0.*

### 2. Posterior Distributions
![Posterior Distributions](visualizations/fayette_posteriors.png)
*Probability distributions for Fayette schools. Lafayette's curve (purple) is shifted significantly to the right.*

### 3. Demographic Effects
![Covariate Effects](visualizations/covariate_effects.png)
*Impact of predictors. % Economically Disadvantaged and % Students with Disabilities have the strongest negative associations with graduation rates statewide.*

## Next Steps

### 1. Integrate Additional Contextual Data
As outlined in `BRIGHT_SPOTS_DATA_PIPELINE_PRIORITIES.md`, the current model uses only student demographics. To better isolate "school effectiveness," we will add:
- **Teacher Characteristics:** % Novice Teachers (KDE Staff Report)
- **Financial Data:** Per-pupil expenditure (KDE Financial)
- **Economic Context:** Census SAIPE poverty rates (County level)

### 2. Extend to Other KPIs
We will replicate this methodology for:
- **Postsecondary Readiness** (High Schools)
- **8th Grade Math Proficiency** (Middle Schools)
- **3rd Grade Reading Proficiency** (Elementary Schools)
- **Chronic Absenteeism** (All Levels)

### 3. Refine Thresholds
We used a conservative threshold (+2%) for this proof of concept. We will engage with stakeholders to define the precise "Bright Spot" definition (e.g., top 10% vs. absolute threshold).

---

**Analysis Conducted By:** AI Assistant (Antigravity)  
**Date:** November 24, 2025
