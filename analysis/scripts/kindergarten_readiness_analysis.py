#!/usr/bin/env python3
"""
Kindergarten Readiness Analysis Dataset

Creates analysis dataset for kindergarten readiness outcomes using:
- Kindergarten screening data (processed)
- Student demographics computed from enrollment
- Teacher quality, financial, and census covariates

Kindergarten readiness is an early childhood indicator that measures
the percentage of entering kindergartners who demonstrate readiness
across multiple domains.

Output: analysis/datasets/kindergarten_readiness_analysis.csv
"""

import pandas as pd
import numpy as np
from pathlib import Path

from base_analysis_dataset import BaseAnalysisDataset


class KindergartenReadinessAnalysisDataset(BaseAnalysisDataset):
    """
    Analysis dataset for kindergarten readiness rates.

    Filters:
    - metric == 'kindergarten_readiness_rate'
    - All Students only
    - School-level (not district totals)
    - Unsuppressed values
    """

    OUTCOME_NAME = 'kindergarten_readiness_rate'
    OUTPUT_FILENAME = 'kindergarten_readiness_analysis.csv'

    # Target metric in the kindergarten readiness file
    TARGET_METRIC = 'kindergarten_readiness_rate'

    def get_merge_strategy(self) -> str:
        """Use school_id matching (IDs now have consistent format with leading zeros)."""
        return 'school_id'

    def use_precomputed_demographics(self) -> bool:
        """Compute demographics from enrollment data."""
        return False

    def load_outcome_data(self) -> pd.DataFrame:
        """Load kindergarten readiness data from processed file."""
        self.log("LOADING KINDERGARTEN READINESS DATA", header=True)

        kinder_file = self.PROCESSED_DIR / "kindergarten_readiness.csv"
        if not kinder_file.exists():
            self.log(f"  Error: {kinder_file} not found")
            return pd.DataFrame()

        self.log(f"  Loading from: {kinder_file}")
        df = pd.read_csv(kinder_file, low_memory=False)
        self.log(f"  Raw records: {len(df):,}")

        # Filter to target metric
        kinder_df = df[df['metric'] == self.TARGET_METRIC].copy()
        self.log(f"  Kindergarten readiness rate records: {len(kinder_df):,}")

        # Filter to All Students only
        kinder_df = kinder_df[kinder_df['student_group'] == 'All Students'].copy()
        self.log(f"  All Students only: {len(kinder_df):,}")

        # Filter to school-level (not district totals)
        kinder_df = kinder_df[kinder_df['school_name'] != '---District Total---'].copy()
        self.log(f"  School-level only: {len(kinder_df):,}")

        # Remove suppressed values
        kinder_df = kinder_df[kinder_df['suppressed'] != 'Y'].copy()
        self.log(f"  After removing suppressed: {len(kinder_df):,}")

        # Rename value to kindergarten_readiness_rate
        kinder_df = kinder_df.rename(columns={'value': 'kindergarten_readiness_rate'})

        # Convert to numeric
        kinder_df['kindergarten_readiness_rate'] = pd.to_numeric(
            kinder_df['kindergarten_readiness_rate'], errors='coerce'
        )

        # Drop rows with missing data
        kinder_df = kinder_df.dropna(subset=['kindergarten_readiness_rate'])
        self.log(f"  After dropping missing values: {len(kinder_df):,}")

        # Select key columns
        cols_to_keep = ['year', 'school_id', 'school_name', 'district', 'district_number',
                        'county_number', 'county_name', 'kindergarten_readiness_rate']

        # Add school_code if available
        if 'school_code' in kinder_df.columns:
            cols_to_keep.append('school_code')

        result = kinder_df[cols_to_keep].copy()

        # Rename school_code to school_number for compatibility
        if 'school_code' in result.columns:
            result = result.rename(columns={'school_code': 'school_number'})

        # Print summary by year
        for year in sorted(result['year'].unique()):
            year_count = len(result[result['year'] == year])
            self.log(f"  {year}: {year_count} schools")

        self.log(f"\nTotal observations: {len(result)}")
        self.log(f"Unique schools: {result['school_id'].nunique()}")
        self.log(f"Readiness rate: mean={result['kindergarten_readiness_rate'].mean():.1f}%, "
                 f"std={result['kindergarten_readiness_rate'].std():.1f}%")

        return result


def main():
    """Main execution."""
    print("=" * 60)
    print("CREATE KINDERGARTEN READINESS ANALYSIS DATASET")
    print("=" * 60)

    dataset = KindergartenReadinessAnalysisDataset(verbose=True)
    df = dataset.create_dataset()

    print("\n" + "=" * 60)
    print("KINDERGARTEN READINESS ANALYSIS DATASET COMPLETE")
    print("=" * 60)

    print(f"\nNext step: Run Bayesian model on {dataset.OUTPUT_DIR / dataset.OUTPUT_FILENAME}")

    return df


if __name__ == "__main__":
    main()
