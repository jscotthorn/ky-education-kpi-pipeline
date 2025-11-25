#!/usr/bin/env python3
"""
Create Analysis Dataset for Graduation Rate Model

This script combines:
- Graduation rate outcomes (from KPI master)
- Demographic predictors (from derive_demographics.py)
- School metadata and hierarchical identifiers

Output:
- analysis/datasets/graduation_analysis.csv
"""

from pathlib import Path
import pandas as pd
import numpy as np

# Paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
KPI_FILE = DATA_DIR / "kpi" / "kpi_master.csv"
DEMO_FILE = BASE_DIR / "analysis" / "datasets" / "demographic_predictors.csv"
OUTPUT_DIR = BASE_DIR / "analysis" / "datasets"
OUTPUT_FILE = OUTPUT_DIR / "graduation_analysis.csv"

print(f"KPI file: {KPI_FILE}")
print(f"Demographics file: {DEMO_FILE}")
print(f"Output file: {OUTPUT_FILE}")


def load_graduation_data() -> pd.DataFrame:
    """Load graduation rate data from KPI master."""
    print("\n" + "="*80)
    print("LOADING GRADUATION RATE DATA")
    print("="*80)
    
    # Read only graduation rate rows
    print("Reading KPI master file...")
    df = pd.read_csv(KPI_FILE, low_memory=False)
    
    # Filter to graduation rate
    grad_df = df[df['metric'] == 'graduation_rate_4_year'].copy()
    
    print(f"Found {len(grad_df):,} graduation rate records")
    
    # Convert value to numeric
    grad_df['graduation_rate'] = pd.to_numeric(grad_df['value'], errors='coerce')
    
    # Remove suppressed values
    grad_df = grad_df[grad_df['suppressed'] != 'Y'].copy()
    
    print(f"After removing suppressed: {len(grad_df):,} records")
    
    # Filter to All Students only (for initial model)
    grad_df = grad_df[grad_df['student_group'] == 'All Students'].copy()
    
    print(f"All Students only: {len(grad_df):,} records")
    
    # Filter to years 2021-2025
    grad_df['year'] = pd.to_numeric(grad_df['year'], errors='coerce')
    grad_df = grad_df[grad_df['year'].isin([2021, 2022, 2023, 2024, 2025])].copy()
    
    print(f"Years 2021-2025: {len(grad_df):,} records")
    print(f"Years: {sorted(grad_df['year'].unique())}")
    
    # Filter to Type A1 schools (traditional high schools)
    grad_df = grad_df[grad_df['school_type'] == 'A1'].copy()
    
    print(f"Type A1 schools only: {len(grad_df):,} records")
    print(f"Unique schools: {grad_df['school_id'].nunique()}")
    
    # Select and rename columns
    grad_clean = grad_df[[
        'year', 'school_id', 'school_name', 'district', 'district_number',
        'county_number', 'county_name', 'graduation_rate', 'school_type'
    ]].copy()
    
    return grad_clean


def load_demographics() -> pd.DataFrame:
    """Load demographic predictors."""
    print("\n" + "="*80)
    print("LOADING DEMOGRAPHIC PREDICTORS")
    print("="*80)
    
    demo_df = pd.read_csv(DEMO_FILE)
    
    print(f"Loaded {len(demo_df):,} demographic records")
    print(f"Years: {sorted(demo_df['year'].unique())}")
    
    # Clean invalid percentage values (cap at 0-100)
    pct_cols = [col for col in demo_df.columns if col.startswith('pct_')]
    
    for col in pct_cols:
        if col in demo_df.columns:
            # Cap at 0-100
            demo_df[col] = demo_df[col].clip(lower=0, upper=100)
    
    return demo_df


def add_region_mapping(df: pd.DataFrame) -> pd.DataFrame:
    """Add Kentucky region classification."""
    # Simplified region mapping based on county
    # This is a placeholder - you may want to refine this
    
    # Bluegrass region (Fayette and surrounding)
    bluegrass_counties = ['FAYETTE', 'BOURBON', 'CLARK', 'JESSAMINE', 'MADISON', 
                          'SCOTT', 'WOODFORD', 'FRANKLIN', 'ANDERSON']
    
    # Northern Kentucky
    northern_counties = ['BOONE', 'CAMPBELL', 'KENTON', 'GALLATIN', 'GRANT', 'PENDLETON']
    
    # Louisville Metro
    louisville_counties = ['JEFFERSON', 'BULLITT', 'OLDHAM', 'SHELBY']
    
    # Eastern/Appalachian
    appalachian_counties = ['BELL', 'FLOYD', 'HARLAN', 'JOHNSON', 'KNOTT', 'LESLIE',
                            'LETCHER', 'MAGOFFIN', 'MARTIN', 'PERRY', 'PIKE', 'WHITLEY']
    
    def classify_region(county):
        if pd.isna(county):
            return 'Other'
        county_upper = str(county).upper().strip()
        
        if county_upper in bluegrass_counties:
            return 'Bluegrass'
        elif county_upper in northern_counties:
            return 'Northern'
        elif county_upper in louisville_counties:
            return 'Louisville'
        elif county_upper in appalachian_counties:
            return 'Appalachian'
        else:
            return 'Other'
    
    df['region'] = df['county_name'].apply(classify_region)
    
    print(f"\nRegion distribution:")
    print(df['region'].value_counts())
    
    return df


def create_analysis_dataset() -> pd.DataFrame:
    """Combine graduation data with demographics."""
    print("\n" + "="*80)
    print("CREATING ANALYSIS DATASET")
    print("="*80)
    
    # Load data
    grad_df = load_graduation_data()
    demo_df = load_demographics()
    
    # Merge on year and school_id
    print("\nMerging graduation data with demographics...")
    analysis_df = grad_df.merge(
        demo_df,
        on=['year', 'school_id'],
        how='left',
        suffixes=('', '_demo')
    )
    
    print(f"Merged dataset: {len(analysis_df):,} rows")
    
    # Add region
    analysis_df = add_region_mapping(analysis_df)
    
    # Create is_fayette flag
    analysis_df['is_fayette'] = (analysis_df['district'] == 'Fayette County').astype(int)
    
    print(f"\nFayette County schools: {analysis_df['is_fayette'].sum()}")
    
    # Handle missing demographics - use mean imputation for now
    pct_cols = [col for col in analysis_df.columns if col.startswith('pct_')]
    
    print(f"\nHandling missing demographics...")
    for col in pct_cols:
        missing_count = analysis_df[col].isna().sum()
        if missing_count > 0:
            mean_val = analysis_df[col].mean()
            analysis_df[col] = analysis_df[col].fillna(mean_val)
            print(f"  {col}: Filled {missing_count} missing values with mean {mean_val:.2f}")
    
    # Select final columns
    final_cols = [
        'year', 'school_id', 'school_name', 'district', 'district_number',
        'county_number', 'county_name', 'region', 'school_type',
        'graduation_rate', 'is_fayette', 'total_enrollment'
    ] + pct_cols
    
    # Keep only columns that exist
    final_cols = [col for col in final_cols if col in analysis_df.columns]
    
    analysis_df = analysis_df[final_cols].copy()
    
    return analysis_df


def validate_dataset(df: pd.DataFrame):
    """Validate the analysis dataset."""
    print("\n" + "="*80)
    print("DATA VALIDATION")
    print("="*80)
    
    print(f"\nDataset shape: {df.shape}")
    print(f"Schools: {df['school_id'].nunique()}")
    print(f"Districts: {df['district'].nunique()}")
    print(f"School-years: {len(df)}")
    print(f"Years: {sorted(df['year'].unique())}")
    
    print(f"\nGraduation rate statistics:")
    print(df['graduation_rate'].describe())
    
    print(f"\nMissing values:")
    missing = df.isnull().sum()
    if missing.sum() > 0:
        print(missing[missing > 0])
    else:
        print("  No missing values!")
    
    print(f"\nFayette County:")
    fayette_df = df[df['is_fayette'] == 1]
    print(f"  Schools: {fayette_df['school_id'].nunique()}")
    print(f"  School-years: {len(fayette_df)}")
    print(f"  Mean graduation rate: {fayette_df['graduation_rate'].mean():.2f}")
    
    print(f"\nStatewide (non-Fayette):")
    other_df = df[df['is_fayette'] == 0]
    print(f"  Schools: {other_df['school_id'].nunique()}")
    print(f"  School-years: {len(other_df)}")
    print(f"  Mean graduation rate: {other_df['graduation_rate'].mean():.2f}")


def main():
    """Main execution."""
    print("="*80)
    print("CREATE GRADUATION RATE ANALYSIS DATASET")
    print("="*80)
    
    # Create dataset
    analysis_df = create_analysis_dataset()
    
    # Validate
    validate_dataset(analysis_df)
    
    # Save
    print(f"\nSaving to: {OUTPUT_FILE}")
    analysis_df.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved {len(analysis_df):,} rows")
    
    # Show sample
    print("\nSample data (first 5 rows):")
    print(analysis_df.head().to_string())
    
    print("\n" + "="*80)
    print("ANALYSIS DATASET COMPLETE")
    print("="*80)
    
    print(f"\nNext step: Run Bayesian model on {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
