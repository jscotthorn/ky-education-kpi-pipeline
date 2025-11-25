#!/usr/bin/env python3
"""
Bayesian Hierarchical Model for Graduation Rates

Implements a three-level hierarchical model:
- Level 1: School-year observations
- Level 2: Schools within districts  
- Level 3: Districts within state

This model uses partial pooling to borrow strength from statewide data,
making it appropriate for small sample sizes (e.g., Fayette County's 6 high schools).
"""

from pathlib import Path
import pandas as pd
import numpy as np
import pymc as pm
import arviz as az
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

# Paths
BASE_DIR = Path(__file__).parent.parent.parent
DATA_FILE = BASE_DIR / "analysis" / "datasets" / "graduation_analysis.csv"
OUTPUT_DIR = BASE_DIR / "analysis" / "outputs"
MODEL_DIR = OUTPUT_DIR / "models"
DIAG_DIR = OUTPUT_DIR / "diagnostics"

# Create output directories
MODEL_DIR.mkdir(exist_ok=True, parents=True)
DIAG_DIR.mkdir(exist_ok=True, parents=True)

print(f"Data file: {DATA_FILE}")
print(f"Model output: {MODEL_DIR}")
print(f"Diagnostics: {DIAG_DIR}")


def load_and_prepare_data():
    """Load and prepare data for modeling."""
    print("\n" + "="*80)
    print("LOADING AND PREPARING DATA")
    print("="*80)
    
    df = pd.read_csv(DATA_FILE)
    print(f"Loaded {len(df):,} observations")
    
    # Create categorical indices for hierarchical structure
    df['district_cat'] = df['district'].astype('category')
    df['school_cat'] = df['school_id'].astype('category')
    
    # Get indices
    district_idx = df['district_cat'].cat.codes.values
    school_idx = df['school_cat'].cat.codes.values
    
    n_districts = len(df['district_cat'].cat.categories)
    n_schools = len(df['school_cat'].cat.categories)
    
    print(f"Districts: {n_districts}")
    print(f"Schools: {n_schools}")
    print(f"Observations per school: {len(df) / n_schools:.1f}")
    
    # Prepare predictors (demographic percentages)
    predictor_cols = [
        'pct_economically_disadvantaged',
        'pct_english_learners', 
        'pct_students_with_disabilities',
        'pct_african_american',
        'pct_hispanic',
        'pct_minority'
    ]
    
    # Check which predictors are available
    available_predictors = [col for col in predictor_cols if col in df.columns]
    print(f"\nAvailable predictors: {len(available_predictors)}")
    for col in available_predictors:
        print(f"  - {col}: mean={df[col].mean():.2f}, std={df[col].std():.2f}")
    
    X = df[available_predictors].values
    
    # Standardize predictors for numerical stability
    X_mean = X.mean(axis=0)
    X_std = X.std(axis=0)
    X_scaled = (X - X_mean) / X_std
    
    # Outcome
    y = df['graduation_rate'].values
    
    print(f"\nOutcome (graduation_rate):")
    print(f"  Mean: {y.mean():.2f}")
    print(f"  Std: {y.std():.2f}")
    print(f"  Range: [{y.min():.2f}, {y.max():.2f}]")
    
    # Create mapping from school to district
    school_to_district = df.groupby('school_cat')['district_cat'].first().cat.codes.values
    
    return {
        'df': df,
        'y': y,
        'X_scaled': X_scaled,
        'X_mean': X_mean,
        'X_std': X_std,
        'predictor_names': available_predictors,
        'district_idx': district_idx,
        'school_idx': school_idx,
        'school_to_district': school_to_district,
        'n_districts': n_districts,
        'n_schools': n_schools,
        'n_predictors': len(available_predictors)
    }


def build_hierarchical_model(data):
    """Build the hierarchical Bayesian model."""
    print("\n" + "="*80)
    print("BUILDING HIERARCHICAL MODEL")
    print("="*80)
    
    with pm.Model() as model:
        # Data
        y_obs = pm.Data('y_obs', data['y'])
        X = pm.Data('X', data['X_scaled'])
        district_idx = pm.Data('district_idx', data['district_idx'])
        school_idx = pm.Data('school_idx', data['school_idx'])
        school_to_district = pm.Data('school_to_district', data['school_to_district'])
        
        # Hyperpriors (state level)
        mu_state = pm.Normal('mu_state', mu=93, sigma=10)  # KY avg ~93%
        
        # District random effects (Level 2)
        sigma_district = pm.HalfCauchy('sigma_district', beta=5)
        district_effect = pm.Normal('district_effect', 
                                    mu=0, 
                                    sigma=sigma_district,
                                    shape=data['n_districts'])
        
        # School random effects (Level 1) 
        # Schools are nested within districts
        sigma_school = pm.HalfCauchy('sigma_school', beta=3)
        school_effect = pm.Normal('school_effect',
                                  mu=district_effect[school_to_district],
                                  sigma=sigma_school,
                                  shape=data['n_schools'])
        
        # Regression coefficients for demographics
        beta = pm.Normal('beta', mu=0, sigma=5, shape=data['n_predictors'])
        
        # Expected graduation rate
        mu = mu_state + district_effect[district_idx] + school_effect[school_idx] + pm.math.dot(X, beta)
        
        # Likelihood (observation-level noise)
        sigma_y = pm.HalfCauchy('sigma_y', beta=2)
        likelihood = pm.Normal('y', mu=mu, sigma=sigma_y, observed=y_obs)
        
    print("\nModel structure:")
    print(f"  State mean: mu_state")
    print(f"  District effects: {data['n_districts']} (σ_district)")
    print(f"  School effects: {data['n_schools']} (σ_school, nested in districts)")
    print(f"  Predictors: {data['n_predictors']} demographic variables")
    print(f"  Observation noise: σ_y")
    
    return model


def sample_posterior(model, data):
    """Run MCMC sampling."""
    print("\n" + "="*80)
    print("SAMPLING FROM POSTERIOR")
    print("="*80)
    
    print("\nMCMC configuration:")
    print("  Chains: 4")
    print("  Draws per chain: 2000")
    print("  Tuning steps: 1000")
    print("  Target accept: 0.95")
    
    print("\nStarting MCMC sampling...")
    print("(This may take 20-30 minutes)")
    
    with model:
        trace = pm.sample(
            draws=2000,
            tune=1000,
            chains=4,
            target_accept=0.95,
            return_inferencedata=True,
            random_seed=42
        )
    
    print("\n✓ Sampling complete!")
    
    return trace


def check_convergence(trace, data):
    """Check MCMC convergence diagnostics."""
    print("\n" + "="*80)
    print("CONVERGENCE DIAGNOSTICS")
    print("="*80)
    
    # R-hat (should be < 1.01)
    rhat = az.rhat(trace)
    
    print("\nR-hat values (should be < 1.01):")
    for var in ['mu_state', 'sigma_district', 'sigma_school', 'sigma_y']:
        if var in rhat:
            val = float(rhat[var].values)
            status = "✓" if val < 1.01 else "✗"
            print(f"  {status} {var}: {val:.4f}")
    
    # Check beta coefficients
    if 'beta' in rhat:
        beta_rhat = rhat['beta'].values
        max_beta_rhat = beta_rhat.max()
        status = "✓" if max_beta_rhat < 1.01 else "✗"
        print(f"  {status} beta (max): {max_beta_rhat:.4f}")
    
    # Effective sample size
    ess = az.ess(trace)
    
    print("\nEffective Sample Size (should be > 400):")
    for var in ['mu_state', 'sigma_district', 'sigma_school', 'sigma_y']:
        if var in ess:
            val = float(ess[var].values)
            status = "✓" if val > 400 else "✗"
            print(f"  {status} {var}: {val:.0f}")
    
    # Overall assessment
    all_rhat_good = all(float(rhat[var].values) < 1.01 
                       for var in ['mu_state', 'sigma_district', 'sigma_school', 'sigma_y']
                       if var in rhat)
    
    print("\n" + "="*80)
    if all_rhat_good:
        print("✓ CONVERGENCE GOOD - All diagnostics passed")
    else:
        print("✗ CONVERGENCE WARNING - Some diagnostics failed")
    print("="*80)
    
    return all_rhat_good


def save_results(trace, data, model):
    """Save model and trace."""
    print("\n" + "="*80)
    print("SAVING RESULTS")
    print("="*80)
    
    # Save trace as NetCDF
    trace_file = MODEL_DIR / "graduation_rate_trace.nc"
    print(f"\nSaving trace to: {trace_file}")
    trace.to_netcdf(str(trace_file))
    
    # Save model summary
    summary = az.summary(trace, var_names=['mu_state', 'sigma_district', 'sigma_school', 
                                           'sigma_y', 'beta'])
    summary_file = MODEL_DIR / "model_summary.csv"
    print(f"Saving summary to: {summary_file}")
    summary.to_csv(summary_file)
    
    # Save posterior samples for school effects
    school_effects = trace.posterior['school_effect'].values  # shape: (chains, draws, schools)
    school_effects_mean = school_effects.mean(axis=(0, 1))  # average across chains and draws
    school_effects_std = school_effects.std(axis=(0, 1))
    
    # Get school names
    school_names = data['df'].groupby('school_cat')['school_name'].first().values
    district_names = data['df'].groupby('school_cat')['district'].first().values
    is_fayette = data['df'].groupby('school_cat')['is_fayette'].first().values
    
    school_effects_df = pd.DataFrame({
        'school_name': school_names,
        'district': district_names,
        'is_fayette': is_fayette,
        'effect_mean': school_effects_mean,
        'effect_std': school_effects_std
    })
    
    effects_file = MODEL_DIR / "school_effects.csv"
    print(f"Saving school effects to: {effects_file}")
    school_effects_df.to_csv(effects_file, index=False)
    
    print("\n✓ All results saved")
    
    return school_effects_df


def main():
    """Main execution."""
    print("="*80)
    print("BAYESIAN HIERARCHICAL MODEL - GRADUATION RATES")
    print("="*80)
    
    # Load data
    data = load_and_prepare_data()
    
    # Build model
    model = build_hierarchical_model(data)
    
    # Sample posterior
    trace = sample_posterior(model, data)
    
    # Check convergence
    converged = check_convergence(trace, data)
    
    # Save results
    school_effects_df = save_results(trace, data, model)
    
    # Show Fayette County results
    print("\n" + "="*80)
    print("FAYETTE COUNTY SCHOOLS")
    print("="*80)
    
    fayette_df = school_effects_df[school_effects_df['is_fayette'] == 1].copy()
    fayette_df = fayette_df.sort_values('effect_mean', ascending=False)
    
    print(f"\nSchool effects (positive = above expected given demographics):")
    print(fayette_df.to_string(index=False))
    
    print("\n" + "="*80)
    print("MODEL FITTING COMPLETE")
    print("="*80)
    
    print(f"\nNext steps:")
    print(f"1. Review diagnostics in {DIAG_DIR}")
    print(f"2. Run identify_bright_spots.py to calculate posterior probabilities")
    print(f"3. Generate visualizations with visualize_results.py")


if __name__ == "__main__":
    main()
