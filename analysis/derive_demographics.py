#!/usr/bin/env python3
"""
Derive School-Level Demographic Percentages

This script processes enrollment data from the KPI master file to calculate
demographic percentages at the school-year level. These percentages will be
used as predictors in the Bayesian hierarchical models.

Output:
- analysis/datasets/demographic_predictors.csv
"""

from pathlib import Path
import pandas as pd
import numpy as np
from typing import Dict, List

# Paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
KPI_FILE = DATA_DIR / "kpi" / "kpi_master.csv"
OUTPUT_DIR = BASE_DIR / "analysis" / "datasets"
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

OUTPUT_FILE = OUTPUT_DIR / "demographic_predictors.csv"

print(f"Loading KPI data from: {KPI_FILE}")
print(f"Output directory: {OUTPUT_DIR}")


# Demographic group mappings
# Map student_group values to demographic categories
DEMOGRAPHIC_GROUPS = {
    'economically_disadvantaged': [
        'Economically Disadvantaged',
        'Free or Reduced-Price Lunch',
        'Free Lunch',
        'Reduced-Price Lunch'
    ],
    'english_learners': [
        'English Learner',
        'English Learners',
        'Limited English Proficiency'
    ],
    'students_with_disabilities': [
        'Students with Disabilities',
        'Students with Disabilities (IEP)',
        'Special Education'
    ],
    'african_american': [
        'African American',
        'Black or African American',
        'Black (non-Hispanic)'
    ],
    'hispanic': [
        'Hispanic',
        'Hispanic or Latino',
        'Hispanic/Latino'
    ],
    'white': [
        'White',
        'White (non-Hispanic)'
    ],
    'asian': [
        'Asian',
        'Asian (non-Hispanic)'
    ],
    'american_indian': [
        'American Indian',
        'American Indian or Alaska Native'
    ],
    'native_hawaiian': [
        'Native Hawaiian',
        'Native Hawaiian or Other Pacific Islander'
    ],
    'two_or_more_races': [
        'Two or More Races',
        'Two or More Races (non-Hispanic)'
    ]
}


def load_enrollment_data(nrows=None) -> pd.DataFrame:
    """Load enrollment data from KPI master file."""
    print(f"\nLoading enrollment data...")
    
    # Read KPI file
    df = pd.read_csv(KPI_FILE, nrows=nrows)
    
    # Filter to enrollment metrics
    enrollment_metrics = [
        'enrollment_total',
        'enrollment_by_gender',
        'enrollment_by_race_ethnicity',
        'enrollment_by_program'
    ]
    
    # Also look for any metric with 'enrollment' or 'count' in the name
    enrollment_df = df[
        (df['metric'].str.contains('enrollment', case=False, na=False)) |
        (df['metric'].str.contains('count', case=False, na=False)) |
        (df['metric'].isin(enrollment_metrics))
    ].copy()
    
    print(f"Loaded {len(enrollment_df):,} enrollment records")
    print(f"Unique metrics: {enrollment_df['metric'].nunique()}")
    print(f"Sample metrics: {enrollment_df['metric'].unique()[:10].tolist()}")
    
    return enrollment_df


def standardize_demographic_group(group: str) -> str:
    """Standardize demographic group names."""
    if pd.isna(group):
        return 'All Students'
    
    group_str = str(group).strip()
    
    # Check each category
    for category, variants in DEMOGRAPHIC_GROUPS.items():
        if group_str in variants:
            return category
    
    # Special case: All Students
    if group_str in ['All Students', 'All', 'Total']:
        return 'all_students'
    
    # Return as-is if not mapped (will filter later)
    return group_str


def calculate_demographics(enrollment_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate demographic percentages at school-year level.
    
    Strategy:
    1. Get total enrollment per school-year (All Students)
    2. Get enrollment by demographic group
    3. Calculate percentages
    """
    print("\n" + "="*80)
    print("CALCULATING DEMOGRAPHIC PERCENTAGES")
    print("="*80)
    
    # Standardize demographic groups
    enrollment_df['demographic_category'] = enrollment_df['student_group'].apply(
        standardize_demographic_group
    )
    
    # Convert value to numeric
    enrollment_df['enrollment'] = pd.to_numeric(enrollment_df['value'], errors='coerce')
    
    # Remove suppressed values
    enrollment_df = enrollment_df[enrollment_df['suppressed'] != 'Y'].copy()
    
    # Group by school-year and demographic
    school_demo_df = enrollment_df.groupby([
        'year', 'school_id', 'school_name', 'district', 'district_number',
        'county_number', 'county_name', 'demographic_category'
    ])['enrollment'].sum().reset_index()
    
    # Get total enrollment per school-year
    total_enrollments = school_demo_df[
        school_demo_df['demographic_category'] == 'all_students'
    ][['year', 'school_id', 'enrollment']].rename(columns={'enrollment': 'total_enrollment'})
    
    # Merge total back in
    demo_with_total = school_demo_df.merge(
        total_enrollments,
        on=['year', 'school_id'],
        how='left'
    )
    
    # Calculate percentages
    demo_with_total['pct'] = (
        demo_with_total['enrollment'] / demo_with_total['total_enrollment'] * 100
    )
    
    # Filter to our target demographics
    target_demos = list(DEMOGRAPHIC_GROUPS.keys())
    demo_pcts = demo_with_total[
        demo_with_total['demographic_category'].isin(target_demos)
    ].copy()
    
    print(f"\nDemographic records: {len(demo_pcts):,}")
    print(f"Unique schools: {demo_pcts['school_id'].nunique()}")
    print(f"Years: {sorted(demo_pcts['year'].unique())}")
    
    # Pivot to wide format (one row per school-year)
    demo_wide = demo_pcts.pivot_table(
        index=['year', 'school_id', 'school_name', 'district', 'district_number',
               'county_number', 'county_name'],
        columns='demographic_category',
        values='pct',
        aggfunc='first'  # Take first if duplicates
    ).reset_index()
    
    # Rename columns to have pct_ prefix
    rename_mapping = {col: f'pct_{col}' for col in DEMOGRAPHIC_GROUPS.keys()}
    demo_wide = demo_wide.rename(columns=rename_mapping)
    
    # Add total enrollment
    demo_wide = demo_wide.merge(
        total_enrollments,
        on=['year', 'school_id'],
        how='left'
    )
    
    return demo_wide


def add_derived_features(demo_df: pd.DataFrame) -> pd.DataFrame:
    """Add additional derived features."""
    
    # Minority percentage (non-white)
    race_cols = ['pct_african_american', 'pct_hispanic', 'pct_asian',
                 'pct_american_indian', 'pct_native_hawaiian', 'pct_two_or_more_races']
    
    available_race_cols = [col for col in race_cols if col in demo_df.columns]
    if available_race_cols:
        demo_df['pct_minority'] = demo_df[available_race_cols].sum(axis=1, min_count=1)
    
    # School size category
    if 'total_enrollment' in demo_df.columns:
        demo_df['enrollment_category'] = pd.cut(
            demo_df['total_enrollment'],
            bins=[0, 250, 500, 1000, 10000],
            labels=['small', 'medium', 'large', 'very_large']
        )
    
    return demo_df


def validate_demographics(demo_df: pd.DataFrame):
    """Validate demographic percentages."""
    print("\n" + "="*80)
    print("DATA QUALITY VALIDATION")
    print("="*80)
    
    # Check for missingness
    print("\nMissingness by column:")
    missing = demo_df.isnull().sum()
    missing_pct = (missing / len(demo_df) * 100).round(1)
    for col in demo_df.columns:
        if missing[col] > 0:
            print(f"  {col}: {missing[col]} ({missing_pct[col]}%)")
    
    # Check percentage ranges
    pct_cols = [col for col in demo_df.columns if col.startswith('pct_')]
    print(f"\nPercentage columns found: {len(pct_cols)}")
    
    for col in pct_cols:
        if col in demo_df.columns:
            valid_range = ((demo_df[col] >= 0) & (demo_df[col] <= 100)).sum()
            total = demo_df[col].notna().sum()
            if total > 0:
                pct_valid = valid_range / total * 100
                print(f"  {col}: {pct_valid:.1f}% in valid range [0, 100]")
                
                if pct_valid < 100:
                    invalid = demo_df[col][(demo_df[col] < 0) | (demo_df[col] > 100)]
                    print(f"    WARNING: {len(invalid)} invalid values found")
    
    # Summary statistics
    print(f"\nOverall Statistics:")
    print(f"  Total schools: {demo_df['school_id'].nunique()}")
    print(f"  Total school-years: {len(demo_df)}")
    print(f"  Years: {sorted(demo_df['year'].unique())}")
    print(f"  Districts: {demo_df['district'].nunique()}")


def main():
    """Main execution."""
    print("="*80)
    print("DERIVING SCHOOL-LEVEL DEMOGRAPHIC PERCENTAGES")
    print("="*80)
    
    # Load enrollment data
    enrollment_df = load_enrollment_data()
    
    if len(enrollment_df) == 0:
        print("\nERROR: No enrollment data found in KPI master file")
        print("Please verify enrollment metrics are present")
        return
    
    # Calculate demographics
    demo_df = calculate_demographics(enrollment_df)
    
    # Add derived features
    demo_df = add_derived_features(demo_df)
    
    # Validate
    validate_demographics(demo_df)
    
    # Save
    print(f"\nSaving to: {OUTPUT_FILE}")
    demo_df.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved {len(demo_df):,} rows")
    
    # Show sample
    print("\nSample data (first 5 rows):")
    print(demo_df.head().to_string())
    
    print("\n" + "="*80)
    print("DEMOGRAPHIC DERIVATION COMPLETE")
    print("="*80)


if __name__ == "__main__":
    main()
