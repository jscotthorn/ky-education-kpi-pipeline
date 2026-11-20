#!/usr/bin/env python3
"""
Exploratory Data Analysis for Bayesian Bright Spots Methodology

This script performs initial data exploration to prepare for Hierarchical Bayesian
modeling of Kentucky education data. It analyzes:
1. Sample sizes by school level (elementary/middle/high)
2. Data completeness and suppression patterns
3. Hierarchical structure (ICC calculations)
4. Demographic correlations (statewide vs. Fayette County)
5. KPI appropriateness for Bayesian modeling

Based on BRIGHT_SPOTS_BAYESIAN_APPROACH.md methodology.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from scipy import stats
from typing import Dict, List, Tuple
import warnings

warnings.filterwarnings('ignore')

# Set plotting style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['font.size'] = 10

# Paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
KPI_FILE = DATA_DIR / "kpi" / "kpi_master.csv"
OUTPUT_DIR = BASE_DIR / "analysis" / "bayesian_eda_outputs"
OUTPUT_DIR.mkdir(exist_ok=True)

print(f"Loading KPI data from: {KPI_FILE}")
print(f"Output directory: {OUTPUT_DIR}")


def load_kpi_sample(nrows: int = 50000000) -> pd.DataFrame:
    """Load a sample of KPI data for initial exploration."""
    print(f"\nLoading {nrows:,} rows from KPI master file...")
    df = pd.read_csv(KPI_FILE, nrows=nrows)
    print(f"Loaded {len(df):,} rows, {len(df.columns)} columns")
    print(f"Columns: {df.columns.tolist()}")
    return df


def analyze_sample_sizes(df: pd.DataFrame, outcome_kpis: List[str]) -> pd.DataFrame:
    """
    Analyze sample sizes by school level for outcome KPIs.
    
    Returns DataFrame with counts by school_type and metric.
    """
    print("\n" + "="*80)
    print("SAMPLE SIZE ANALYSIS")
    print("="*80)
    
    results = []
    
    for kpi in outcome_kpis:
        kpi_df = df[
            (df['metric'] == kpi) & 
            (df['student_group'] == 'All Students') &
            (df['suppressed'] == 'N')
        ]
        
        if len(kpi_df) == 0:
            print(f"\nWarning: No data found for {kpi}")
            continue
            
        print(f"\n{kpi}:")
        print(f"  Total schools: {kpi_df['school_name'].nunique()}")
        print(f"  Total districts: {kpi_df['district'].nunique()}")
        print(f"  Years available: {sorted(kpi_df['year'].unique())}")
        
        # By school type
        if 'school_type' in kpi_df.columns and kpi_df['school_type'].notna().any():
            by_type = kpi_df.groupby('school_type').agg({
                'school_name': 'nunique',
                'value': ['count', 'mean', 'std']
            }).round(2)
            print(f"\n  By school type:")
            print(by_type)
        else:
            print(f"\n  Warning: school_type column not available or all null")
        
        # Fayette County specific
        fayette_df = kpi_df[kpi_df['district'] == 'Fayette County']
        if len(fayette_df) > 0:
            print(f"\n  Fayette County:")
            print(f"    Total schools: {fayette_df['school_name'].nunique()}")
            if 'school_type' in fayette_df.columns and fayette_df['school_type'].notna().any():
                fayette_by_type = fayette_df.groupby('school_type')['school_name'].nunique()
                for school_type, count in fayette_by_type.items():
                    print(f"    {school_type}: {count} schools")
        
        # Store results
        if 'school_type' in kpi_df.columns:
            for school_type in kpi_df['school_type'].dropna().unique():
                type_df = kpi_df[kpi_df['school_type'] == school_type]
                fayette_type_df = fayette_df[fayette_df['school_type'] == school_type] if len(fayette_df) > 0 else pd.DataFrame()
                
                results.append({
                    'metric': kpi,
                    'school_type': school_type,
                    'statewide_schools': type_df['school_name'].nunique(),
                    'fayette_schools': fayette_type_df['school_name'].nunique() if len(fayette_type_df) > 0 else 0,
                    'mean_value': type_df['value'].mean(),
                    'std_value': type_df['value'].std(),
                    'suppression_rate': (type_df['suppressed'] == 'Y').mean() * 100
                })
    
    return pd.DataFrame(results)


def calculate_icc(df: pd.DataFrame, outcome: str, grouping_var: str) -> float:
    """
    Calculate Intraclass Correlation Coefficient (ICC).
    
    ICC = Var(between) / [Var(between) + Var(within)]
    
    If ICC >= 0.10, hierarchical modeling is justified.
    """
    outcome_df = df[
        (df['metric'] == outcome) & 
        (df['student_group'] == 'All Students') &
        (df['suppressed'] == 'N')
    ].copy()
    
    if len(outcome_df) == 0:
        return np.nan
    
    # Calculate group means
    group_means = outcome_df.groupby(grouping_var)['value'].mean()
    grand_mean = outcome_df['value'].mean()
    
    # Between-group variance
    n_per_group = outcome_df.groupby(grouping_var).size()
    var_between = ((group_means - grand_mean) ** 2 * n_per_group).sum() / (len(group_means) - 1)
    
    # Within-group variance
    outcome_df['group_mean'] = outcome_df[grouping_var].map(group_means)
    var_within = ((outcome_df['value'] - outcome_df['group_mean']) ** 2).sum() / (len(outcome_df) - len(group_means))
    
    # ICC
    icc = var_between / (var_between + var_within)
    
    return icc


def analyze_hierarchical_structure(df: pd.DataFrame, outcome_kpis: List[str]) -> pd.DataFrame:
    """
    Analyze hierarchical structure by calculating ICCs at different levels.
    """
    print("\n" + "="*80)
    print("HIERARCHICAL STRUCTURE ANALYSIS (ICC)")
    print("="*80)
    print("\nICC Interpretation:")
    print("  ICC >= 0.10: Hierarchical modeling justified")
    print("  ICC >= 0.20: Strong hierarchical structure")
    print("  ICC >= 0.30: Very strong hierarchical structure")
    
    results = []
    
    for kpi in outcome_kpis:
        print(f"\n{kpi}:")
        
        # District-level ICC
        icc_district = calculate_icc(df, kpi, 'district')
        print(f"  ICC (District): {icc_district:.3f}")
        
        # County-level ICC
        if 'county_name' in df.columns:
            icc_county = calculate_icc(df, kpi, 'county_name')
            print(f"  ICC (County): {icc_county:.3f}")
        else:
            icc_county = np.nan
        
        results.append({
            'metric': kpi,
            'icc_district': icc_district,
            'icc_county': icc_county,
            'hierarchical_justified': icc_district >= 0.10
        })
    
    return pd.DataFrame(results)


def compare_correlations(df: pd.DataFrame, outcome: str, predictor: str = 'pct_econdis') -> Dict:
    """
    Compare correlations between outcome and predictor for statewide vs. Fayette County.
    
    Tests for Simpson's Paradox: different within vs. between correlations.
    """
    # This is a placeholder - we need to derive demographic percentages from enrollment data
    print(f"\nNote: Demographic percentages need to be derived from enrollment by student_group")
    print(f"This will be implemented after we understand the data structure better")
    
    return {
        'outcome': outcome,
        'predictor': predictor,
        'statewide_corr': np.nan,
        'fayette_corr': np.nan,
        'simpsons_paradox': False
    }


def create_summary_visualizations(df: pd.DataFrame, sample_size_df: pd.DataFrame):
    """Create summary visualizations for EDA report."""
    print("\n" + "="*80)
    print("CREATING VISUALIZATIONS")
    print("="*80)
    
    if len(sample_size_df) == 0:
        print("Warning: No sample size data available for visualization")
        return
    
    # 1. Sample sizes by school type
    fig, ax = plt.subplots(figsize=(12, 6))
    
    pivot_df = sample_size_df.pivot(
        index='school_type', 
        columns='metric', 
        values='statewide_schools'
    )
    
    pivot_df.plot(kind='bar', ax=ax)
    ax.set_title('Sample Sizes by School Type and Metric (Statewide)', fontsize=14, fontweight='bold')
    ax.set_xlabel('School Type')
    ax.set_ylabel('Number of Schools')
    ax.legend(title='Metric', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'sample_sizes_by_type.png', dpi=300, bbox_inches='tight')
    print(f"Saved: sample_sizes_by_type.png")
    plt.close()
    
    # 2. Fayette County sample sizes
    fig, ax = plt.subplots(figsize=(10, 6))
    
    fayette_pivot = sample_size_df.pivot(
        index='school_type',
        columns='metric',
        values='fayette_schools'
    )
    
    fayette_pivot.plot(kind='bar', ax=ax, color='coral')
    ax.set_title('Fayette County Sample Sizes by School Type', fontsize=14, fontweight='bold')
    ax.set_xlabel('School Type')
    ax.set_ylabel('Number of Schools')
    ax.axhline(y=6, color='red', linestyle='--', alpha=0.5, label='High School Count (n=6)')
    ax.axhline(y=20, color='orange', linestyle='--', alpha=0.5, label='Minimum for Regression (n=20)')
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / 'fayette_sample_sizes.png', dpi=300, bbox_inches='tight')
    print(f"Saved: fayette_sample_sizes.png")
    plt.close()



def main():
    """Main EDA workflow."""
    print("="*80)
    print("BAYESIAN BRIGHT SPOTS - EXPLORATORY DATA ANALYSIS")
    print("="*80)
    
    # Load sample data
    df = load_kpi_sample()  # Uses default of 5M rows
    
    # Define outcome KPIs based on BRIGHT_SPOTS_BAYESIAN_APPROACH.md
    outcome_kpis = [
        'graduation_rate_4_year',  # High schools
        'postsecondary_readiness_rate',  # High schools
        'postsecondary_enrollment_total_ky_college_rate',  # High schools
        'kentucky_summative_assessment_math_proficient_distinguished_rate_grade_8',  # Middle
        'kentucky_summative_assessment_reading_proficient_distinguished_rate_grade_3',  # Elementary
        'chronic_absenteeism_rate_all_grades',  # All levels (inverse outcome)
    ]
    
    # Check which KPIs are available in the sample
    available_kpis = [kpi for kpi in outcome_kpis if kpi in df['metric'].values]
    print(f"\nAvailable outcome KPIs in sample: {len(available_kpis)}/{len(outcome_kpis)}")
    for kpi in available_kpis:
        print(f"  ✓ {kpi}")
    
    missing_kpis = [kpi for kpi in outcome_kpis if kpi not in df['metric'].values]
    if missing_kpis:
        print(f"\nMissing KPIs (may need larger sample):")
        for kpi in missing_kpis:
            print(f"  ✗ {kpi}")
    
    # 1. Sample size analysis
    sample_size_df = analyze_sample_sizes(df, available_kpis)
    sample_size_df.to_csv(OUTPUT_DIR / 'sample_sizes.csv', index=False)
    print(f"\nSaved: sample_sizes.csv")
    
    # 2. Hierarchical structure analysis
    icc_df = analyze_hierarchical_structure(df, available_kpis)
    icc_df.to_csv(OUTPUT_DIR / 'icc_analysis.csv', index=False)
    print(f"\nSaved: icc_analysis.csv")
    
    # 3. Create visualizations
    create_summary_visualizations(df, sample_size_df)
    
    # 4. Print summary recommendations
    print("\n" + "="*80)
    print("SUMMARY AND RECOMMENDATIONS")
    print("="*80)
    
    if len(icc_df) > 0:
        print("\nKPIs with sufficient hierarchical structure (ICC >= 0.10):")
        justified_kpis = icc_df[icc_df['hierarchical_justified'] == True]
        if len(justified_kpis) > 0:
            for _, row in justified_kpis.iterrows():
                print(f"  ✓ {row['metric']} (ICC: {row['icc_district']:.3f})")
        else:
            print("  (None found in sample)")
    else:
        print("\nNo KPIs available for ICC analysis")
    
    if len(sample_size_df) > 0:
        print("\nFayette County sample size concerns:")
        fayette_concerns = sample_size_df[sample_size_df['fayette_schools'] < 20]
        if len(fayette_concerns) > 0:
            for _, row in fayette_concerns.iterrows():
                print(f"  ⚠ {row['metric']} ({row['school_type']}): n={row['fayette_schools']} (< 20 minimum)")
        else:
            print("  (All Fayette County samples meet minimum threshold)")
    else:
        print("\nNo sample size data available")
    
    print("\n" + "="*80)
    print("EDA COMPLETE")
    print("="*80)
    print(f"\nOutputs saved to: {OUTPUT_DIR}")
    print("\nNext steps:")
    print("1. Review sample_sizes.csv and icc_analysis.csv")
    print("2. Examine visualizations in bayesian_eda_outputs/")
    print("3. Determine which KPIs are appropriate for Bayesian modeling")
    print("4. Derive demographic percentages from enrollment data")
    print("5. Create full analysis dataset with all predictors")


if __name__ == "__main__":
    main()
