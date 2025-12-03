# MLflow Experiment Tracking for Bayesian Models

This document describes how to use MLflow for tracking Bayesian model experiments, enabling sensitivity analysis and prior specification comparison.

## Quick Start

```bash
# Run a model experiment with tracking
python run_experiment.py graduation --student-group all_students

# Start the MLflow UI to view results
python run_experiment.py --ui
# Then open http://localhost:5000
```

## Why MLflow?

Previously, model outputs were overwritten on each run, making it impossible to:
- Compare different prior specifications
- Track sensitivity analyses
- Maintain a history of model configurations

MLflow solves this by:
1. **Tracking every run** with parameters, metrics, and artifacts
2. **Organizing experiments** by indicator (graduation, chronic_absenteeism, etc.)
3. **Enabling comparison** across runs with different configurations
4. **Storing artifacts** (traces, plots, summaries) for each run

## Running Experiments

### Basic Usage

```bash
# Run with default settings (Finnish horseshoe priors)
python run_experiment.py graduation --student-group all_students

# Run with a descriptive name
python run_experiment.py graduation -g all_students -n baseline_2024

# Run for a specific demographic group
python run_experiment.py graduation -g african_american -n african_american_baseline
```

### Sensitivity Analysis

The `--prior-scale` flag multiplies all prior scales, enabling easy sensitivity testing:

```bash
# Baseline (scale=1.0)
python run_experiment.py graduation -g all_students -n baseline

# Tighter priors (scale=0.75) - more regularization
python run_experiment.py graduation -g all_students -n tighter_priors -s 0.75

# Looser priors (scale=1.5) - less regularization
python run_experiment.py graduation -g all_students -n looser_priors -s 1.5
```

### Prior Type Comparison

```bash
# Finnish horseshoe (default)
python run_experiment.py graduation -g all_students -n finnish_baseline -p finnish

# Classic horseshoe
python run_experiment.py graduation -g all_students -n horseshoe_test -p horseshoe

# Normal priors (no shrinkage)
python run_experiment.py graduation -g all_students -n normal_priors -p normal
```

### Quick Runs for Testing

```bash
# Reduce MCMC iterations for faster testing
python run_experiment.py graduation -g all_students --draws 1000 --tune 500

# Skip LOO-CV (saves ~5-10 minutes)
python run_experiment.py graduation -g all_students --skip-loo
```

## Viewing Results

### MLflow UI

```bash
python run_experiment.py --ui
```

This starts a local web server at http://localhost:5000 where you can:
- Browse all experiments and runs
- Compare metrics across runs
- View logged artifacts (plots, summaries)
- Filter runs by parameters or tags

### Command Line Comparison

```bash
# Compare recent runs for an indicator
python run_experiment.py graduation --compare
```

## Key Metrics Tracked

### Convergence Diagnostics
- `n_divergences`: Number of divergent transitions
- `max_rhat`: Maximum R-hat value (should be < 1.01)
- `min_ess_bulk`: Minimum bulk ESS (should be > 400)
- `min_ess_tail`: Minimum tail ESS (should be > 400)
- `convergence_passed`: Overall convergence status

### Model Fit
- `loo_elpd`: Expected log pointwise predictive density
- `loo_se`: Standard error of ELPD
- `loo_p_loo`: Effective number of parameters
- `loo_reliable`: Whether LOO estimates are reliable

### Posterior Summaries
- `state_mean_posterior`: Posterior mean of state-level intercept
- `sigma_district_posterior`: Posterior mean of district variance
- `sigma_school_posterior`: Posterior mean of school variance
- `n_significant_covariates`: Covariates with 95% CI excluding zero

## Artifacts Stored

Each run stores:
- `model_outputs/`: Model trace, summaries, covariate effects
- `diagnostics/`: Prior/posterior predictive checks, LOO diagnostics

## File Structure

```
analysis/
├── mlruns.db              # SQLite database for MLflow tracking
├── mlartifacts/           # Stored artifacts from runs
├── bayesian_models/
│   ├── mlflow_tracking.py # MLflow integration module
│   ├── run_experiment.py  # Experiment runner script
│   └── EXPERIMENT_TRACKING.md  # This file
```

## Programmatic Usage

```python
from mlflow_tracking import MLflowTracker, create_run_config_from_model

# Set up tracker
tracker = MLflowTracker(experiment_name="bayesian_graduation")

# Start a run
with tracker.start_run(run_name="my_experiment"):
    # ... run model ...
    tracker.log_config(config)
    tracker.log_metrics(metrics)
    tracker.log_artifacts(output_dir)

# Compare runs
comparison = tracker.compare_runs(run_ids=["abc123", "def456"])

# Get best run
best = tracker.get_best_run(metric="loo_elpd")
```

## Sensitivity Analysis Workflow

Per the IMPROVING_ESTIMATE_CONFIDENCE.md guide:

1. **Run baseline with current priors**
   ```bash
   python run_experiment.py graduation -g all_students -n baseline
   ```

2. **Run with tighter priors (0.75x)**
   ```bash
   python run_experiment.py graduation -g all_students -n tighter -s 0.75
   ```

3. **Run with looser priors (1.5x)**
   ```bash
   python run_experiment.py graduation -g all_students -n looser -s 1.5
   ```

4. **Compare in MLflow UI**
   - Check that credible intervals shrink without substantially changing point estimates
   - Verify known relationships (e.g., poverty -> lower outcomes) are preserved
   - Compare LOO-ELPD across runs

## Publishing Runs for Production

Once you've validated a run through sensitivity analysis, publish it for use in the portal:

### Publish a Run

```bash
# Publish a specific run by ID
python run_experiment.py graduation --publish <RUN_ID> -g all_students

# The run ID is shown after each experiment, or find it with:
python run_experiment.py graduation --compare
```

### List Published Runs

```bash
# List all published runs across all indicators
python run_experiment.py --list-published

# List published runs for a specific indicator
python run_experiment.py graduation --list-published
```

### Unpublish a Run

```bash
# Unpublish the current published run for an indicator/group
python run_experiment.py graduation --unpublish -g all_students
```

### Combine Published Results for Portal

```bash
# Generate bayesian_results.json from published runs
python combine_results.py --from-mlflow

# Preview what would be combined
python combine_results.py --from-mlflow --list
```

### Publishing Rules

1. **One published run per indicator/student_group** - Publishing a new run automatically unpublishes the previous one
2. **Convergence warnings** - You'll be prompted if publishing a run with convergence issues
3. **Metadata tracking** - Published runs include timestamps and configuration details in the output JSON

## Future Integration with Optuna

The MLflow infrastructure is designed to integrate with Optuna for Bayesian hyperparameter optimization:

```python
import optuna
from optuna.integration import MLflowCallback

def objective(trial):
    prior_scale = trial.suggest_float("prior_scale", 0.5, 2.0)
    # Run model and return LOO-ELPD
    return loo_elpd

study = optuna.create_study(direction="maximize")
study.optimize(objective, n_trials=50, callbacks=[MLflowCallback()])
```

This enables automated search for optimal prior specifications.
