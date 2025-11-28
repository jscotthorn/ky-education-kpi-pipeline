#!/usr/bin/env python3
"""
Postsecondary Readiness Analysis Dataset

Creates analysis dataset for postsecondary readiness rates using:
- KPI master file for readiness rates
- Precomputed demographics (or enrollment-based)
- Teacher quality, financial, and census covariates

Postsecondary readiness measures whether students are academically
prepared for college or career pathways upon graduation.

Output: analysis/datasets/postsecondary_readiness_analysis.csv
"""

import pandas as pd
import numpy as np
from pathlib import Path

from base_analysis_dataset import BaseAnalysisDataset


class PostsecondaryReadinessAnalysisDataset(BaseAnalysisDataset):
    """
    Analysis dataset for postsecondary readiness rates.

    Filters:
    - school_type == 'A1' (traditional high schools)
    - years 2022-2025
    - All Students only
    - Unsuppressed values
    """

    OUTCOME_NAME = 'postsecondary_readiness_rate'
    OUTPUT_FILENAME = 'postsecondary_readiness_analysis.csv'

    def get_merge_strategy(self) -> str:
        """Use school_id for KPI data (consistent IDs in KPI master)."""
        return 'school_id'

    def get_year_range(self) -> tuple:
        """Filter to 2022-2025 (available years)."""
        return (2022, 2025)

    def get_school_type_filter(self) -> str:
        """Filter to A1 (traditional high schools)."""
        return 'A1'

    def use_precomputed_demographics(self) -> bool:
        """Use precomputed demographics file if available."""
        return True

    def load_outcome_data(self) -> pd.DataFrame:
        """Load postsecondary readiness rate data from KPI master."""
        self.log("LOADING POSTSECONDARY READINESS RATE DATA", header=True)

        # Use chunked reading to efficiently load from large KPI file
        ps_df = self.read_kpi_chunked(['postsecondary_readiness_rate'])
        self.log(f"Found {len(ps_df):,} postsecondary readiness rate records")

        # Convert value to numeric
        ps_df['postsecondary_readiness_rate'] = pd.to_numeric(ps_df['value'], errors='coerce')

        # Remove suppressed values
        ps_df = ps_df[ps_df['suppressed'] != 'Y'].copy()
        self.log(f"After removing suppressed: {len(ps_df):,} records")

        # Filter to All Students only
        ps_df = ps_df[ps_df['student_group'] == 'All Students'].copy()
        self.log(f"All Students only: {len(ps_df):,} records")

        # Filter to years 2022-2025
        ps_df['year'] = pd.to_numeric(ps_df['year'], errors='coerce')
        year_range = self.get_year_range()
        ps_df = ps_df[ps_df['year'].between(year_range[0], year_range[1])].copy()
        self.log(f"Years {year_range[0]}-{year_range[1]}: {len(ps_df):,} records")
        self.log(f"Years: {sorted(ps_df['year'].unique())}")

        # Filter to Type A1 schools (traditional high schools)
        school_type = self.get_school_type_filter()
        ps_df = ps_df[ps_df['school_type'] == school_type].copy()
        self.log(f"Type {school_type} schools only: {len(ps_df):,} records")
        self.log(f"Unique schools: {ps_df['school_id'].nunique()}")

        # Select and return columns
        result = ps_df[[
            'year', 'school_id', 'school_name', 'district', 'district_number',
            'county_number', 'county_name', 'postsecondary_readiness_rate', 'school_type'
        ]].copy()

        return result


def main():
    """Main execution."""
    print("=" * 60)
    print("CREATE POSTSECONDARY READINESS ANALYSIS DATASET")
    print("=" * 60)

    dataset = PostsecondaryReadinessAnalysisDataset(verbose=True)
    df = dataset.create_dataset()

    print("\n" + "=" * 60)
    print("POSTSECONDARY READINESS ANALYSIS DATASET COMPLETE")
    print("=" * 60)

    print(f"\nNext step: Run Bayesian model on {dataset.OUTPUT_DIR / dataset.OUTPUT_FILENAME}")

    return df


if __name__ == "__main__":
    main()
