# Prior Analysis Scripts

This directory contains scripts for the empirical analysis of historical data used to inform Bayesian model priors. This implements **Stage 1 (Empirical Foundation)** of the hybrid approach outlined in `../PRIOR_SPECIFICATION_GUIDE.md`.

## Purpose

Before specifying priors for Bayesian hierarchical models, we analyze historical data to establish baseline variance estimates. These empirical values provide an objective foundation that can be reviewed and adjusted by domain experts (Stage 2).

## Directory Structure

```
analysis/
├── prior_analysis/                        # Scripts (this directory)
│   ├── README.md                          # This file
│   ├── aggregate_priors.py                # Combine all indicator outputs
│   └── indicators/                        # One subdirectory per indicator
│       ├── graduation_rate/
│       │   └── historical_review.py       # Analysis script
│       ├── chronic_absenteeism/           # Future indicator
│       └── ...
│
└── outputs/
    └── prior_analysis/                    # Generated outputs
        ├── graduation_rate/
        │   ├── variance_components.csv    # Raw empirical estimates
        │   └── recommended_priors.csv     # Prior recommendations
        ├── chronic_absenteeism/           # Future indicator outputs
        └── summary/                       # Aggregated outputs
            ├── all_indicators_priors_summary.csv
            └── all_indicators_variance_components.csv
```

## Output CSV Schema

### Variance Components (`variance_components.csv`)

| Column | Type | Description |
|--------|------|-------------|
| indicator_id | str | Unique identifier (e.g., `graduation_rate`) |
| indicator_name | str | Human-readable name |
| analysis_date | str | ISO date when analysis was run |
| school_years | str | Years included (e.g., "2022-23, 2023-24") |
| parameter | str | Parameter name (e.g., `state_mean`, `between_district_sd`) |
| empirical_value | float | Estimated value from historical data |
| unit | str | Unit of measurement (e.g., `percent`) |
| n_schools | int | Number of unique schools in analysis |
| n_districts | int | Number of unique districts |
| n_observations | int | Total observations |

### Recommended Priors (`recommended_priors.csv`)

| Column | Type | Description |
|--------|------|-------------|
| indicator_id | str | Unique identifier |
| indicator_name | str | Human-readable name |
| analysis_date | str | ISO date when analysis was run |
| school_years | str | Years included |
| parameter | str | Model parameter name |
| distribution | str | Prior distribution family (e.g., `Normal`, `HalfNormal`) |
| location | float | Location parameter (for Normal priors) |
| scale | float | Scale parameter |
| empirical_sd | float | Original empirical SD (for reference) |
| prior_95_lower | float | Lower bound of 95% prior interval |
| prior_95_upper | float | Upper bound of 95% prior interval |
| rationale | str | Explanation for the choice |

## Usage

### Run Individual Indicator Analysis

```bash
# From the project root or prior_analysis directory
python analysis/prior_analysis/indicators/graduation_rate/historical_review.py

# Future indicators
python analysis/prior_analysis/indicators/chronic_absenteeism/historical_review.py
```

Outputs are written to `analysis/outputs/prior_analysis/{indicator_id}/`.

### Aggregate All Priors

```bash
python analysis/prior_analysis/aggregate_priors.py
```

This generates combined files in `analysis/outputs/prior_analysis/summary/`.

## Creating a New Indicator Analysis

1. Create a new directory under `indicators/`:
   ```bash
   mkdir -p indicators/chronic_absenteeism
   ```

2. Copy the template from an existing indicator:
   ```bash
   cp indicators/graduation_rate/historical_review.py indicators/chronic_absenteeism/
   ```

3. Update the metadata constants at the top of the script:
   ```python
   INDICATOR_ID = 'chronic_absenteeism'
   INDICATOR_NAME = 'Chronic Absenteeism Rate'
   ANALYSIS_YEARS = [2023, 2024]
   ```

4. Modify the data loading and analysis logic as needed

5. Run the script:
   ```bash
   python indicators/chronic_absenteeism/historical_review.py
   ```

6. Update the aggregated summary:
   ```bash
   python aggregate_priors.py
   ```

## Next Steps After Analysis

Per the hybrid approach in `PRIOR_SPECIFICATION_GUIDE.md`:

1. **Expert Review (Stage 2)**: Present empirical findings to domain experts
2. **Prior Predictive Check (Stage 4)**: Simulate from priors to verify sensible predictions
3. **Sensitivity Analysis (Stage 5)**: Test models with tighter/looser priors

## Indicators Completed

- [x] `graduation_rate` - 4-Year Graduation Rate
- [x] `third_grade_reading` - Third Grade Reading Proficiency
- [x] `chronic_absenteeism` - Chronic Absenteeism Rate
- [x] `postsecondary_readiness` - Postsecondary Readiness Rate
- [x] `kindergarten_readiness` - Kindergarten Readiness Rate
- [x] `math_grade8` - 8th Grade Math Proficiency
- [x] `school_climate` - School Climate Index (note: district column unavailable)
