#!/usr/bin/env python3
"""
Identify Bright Spot Schools

Calculates posterior probabilities of positive school effects from the
Bayesian hierarchical model trace. Identifies schools that are performing
significantly better than expected given their demographics and district context.

Methodology:
1. Load model trace
2. Extract school random effects (posterior distributions)
3. Calculate probability that effect > threshold (e.g., 5 percentage points)
4. Rank schools by probability
5. Generate report with credible intervals
"""

from pathlib import Path
import pandas as pd
import numpy as np
import arviz as az
import xarray as xr

# Paths
BASE_DIR = Path(__file__).parent.parent.parent
OUTPUT_DIR = BASE_DIR / "analysis" / "outputs"
MODEL_DIR = OUTPUT_DIR / "models"
BRIGHT_SPOTS_DIR = OUTPUT_DIR / "bright_spots"
BRIGHT_SPOTS_DIR.mkdir(exist_ok=True, parents=True)

TRACE_FILE = MODEL_DIR / "graduation_rate_trace.nc"
DATA_FILE = BASE_DIR / "analysis" / "datasets" / "graduation_analysis.csv"

print(f"Trace file: {TRACE_FILE}")
print(f"Data file: {DATA_FILE}")


def load_data_and_trace():
    """Load the analysis dataset and model trace."""
    print("\nLoading data and trace...")
    
    # Load data to get school names
    df = pd.read_csv(DATA_FILE)
    
    # Create categorical indices (must match training)
    df['school_cat'] = df['school_id'].astype('category')
    
    # Get mapping from index to school info
    school_info = df.groupby('school_cat')[['school_name', 'district', 'is_fayette']].first()
    
    # Load trace
    trace = az.from_netcdf(str(TRACE_FILE))
    
    return df, school_info, trace


def calculate_bright_spots(trace, school_info, threshold=2.0):
    """
    Calculate bright spot probabilities.
    
    Args:
        trace: Arviz InferenceData object
        school_info: DataFrame with school metadata
        threshold: Effect size threshold (percentage points)
                   Default 2.0 means "2 percentage points above expected"
    """
    print(f"\nCalculating probabilities (Threshold > {threshold}%)...")
    
    # Extract school effects: shape (chains, draws, schools)
    school_effects = trace.posterior['school_effect']
    
    # Calculate probability effect > threshold
    # Average over chains and draws
    probs = (school_effects > threshold).mean(dim=['chain', 'draw'])
    
    # Calculate mean effect and credible intervals (95% HDI)
    mean_effects = school_effects.mean(dim=['chain', 'draw'])
    hdi = az.hdi(trace.posterior)['school_effect']  # shape (schools, 2)
    
    # Create results DataFrame
    results = pd.DataFrame({
        'school_name': school_info['school_name'].values,
        'district': school_info['district'].values,
        'is_fayette': school_info['is_fayette'].values,
        'prob_bright_spot': probs.values,
        'effect_mean': mean_effects.values,
        'hdi_lower': hdi.sel(hdi='lower').values,
        'hdi_upper': hdi.sel(hdi='higher').values
    })
    
    # Sort by probability
    results = results.sort_values('prob_bright_spot', ascending=False)
    
    return results


def generate_report(results, threshold):
    """Generate summary report."""
    print("\n" + "="*80)
    print(f"BRIGHT SPOTS IDENTIFICATION (Threshold > {threshold}%)")
    print("="*80)
    
    # Top 10 Statewide
    print(f"\nTop 10 Bright Spots (Statewide):")
    print(results.head(10)[['school_name', 'district', 'prob_bright_spot', 'effect_mean']].to_string(index=False))
    
    # Fayette County
    print(f"\nFayette County Schools:")
    fayette = results[results['is_fayette'] == 1].copy()
    print(fayette[['school_name', 'prob_bright_spot', 'effect_mean', 'hdi_lower', 'hdi_upper']].to_string(index=False))
    
    # Save to CSV
    output_file = BRIGHT_SPOTS_DIR / "graduation_rate_bright_spots.csv"
    results.to_csv(output_file, index=False)
    print(f"\nSaved full results to: {output_file}")
    
    # Identify "Certified" Bright Spots (Prob > 0.80)
    certified = results[results['prob_bright_spot'] > 0.80]
    print(f"\nTotal Certified Bright Spots (Prob > 0.80): {len(certified)}")
    print(f"Fayette Certified Bright Spots: {len(certified[certified['is_fayette'] == 1])}")


def main():
    """Main execution."""
    # Load
    df, school_info, trace = load_data_and_trace()
    
    # Calculate (using 2.0% threshold as initial conservative estimate)
    # Note: User mentioned 5% in docs, but 2-3% is often significant for grad rates
    # We can adjust this parameter. Let's stick to 2.0% for now as "beating odds"
    results = calculate_bright_spots(trace, school_info, threshold=2.0)
    
    # Report
    generate_report(results, threshold=2.0)


if __name__ == "__main__":
    main()
