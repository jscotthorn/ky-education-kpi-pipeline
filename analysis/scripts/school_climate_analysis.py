#!/usr/bin/env python3
"""
School Climate Index Analysis Dataset

Creates analysis dataset for school climate index scores using:
- KPI master file for climate index scores
- Precomputed demographics (or enrollment-based)
- Teacher quality, financial, and census covariates

School climate index measures student perceptions of safety,
engagement, and environment. Scores typically range 0-100.

Output: analysis/datasets/school_climate_analysis.csv
"""

import pandas as pd
import numpy as np
from pathlib import Path

from base_analysis_dataset import BaseAnalysisDataset


class SchoolClimateAnalysisDataset(BaseAnalysisDataset):
    """
    Analysis dataset for school climate index scores.

    Filters:
    - metric == 'climate_index_score'
    - All Students only
    - Unsuppressed values
    """

    OUTCOME_NAME = 'school_climate_index'
    OUTPUT_FILENAME = 'school_climate_analysis.csv'

    def get_merge_strategy(self) -> str:
        """Use school_id for KPI data."""
        return 'school_id'

    def get_year_range(self) -> tuple:
        """Filter to available years."""
        return (2022, 2025)

    def get_school_type_filter(self) -> str:
        """No school type filter - include all schools."""
        return None

    def use_precomputed_demographics(self) -> bool:
        """Use precomputed demographics file if available."""
        return True

    def load_outcome_data(self) -> pd.DataFrame:
        """Load school climate index data from KPI master."""
        self.log("LOADING SCHOOL CLIMATE INDEX DATA", header=True)

        # Use chunked reading to efficiently load from large KPI file
        sc_df = self.read_kpi_chunked(['climate_index_score'])
        self.log(f"Found {len(sc_df):,} school climate index records")

        # Convert value to numeric
        sc_df['school_climate_index'] = pd.to_numeric(sc_df['value'], errors='coerce')

        # Remove suppressed values
        sc_df = sc_df[sc_df['suppressed'] != 'Y'].copy()
        self.log(f"After removing suppressed: {len(sc_df):,} records")

        # Filter to All Students only
        sc_df = sc_df[sc_df['student_group'] == 'All Students'].copy()
        self.log(f"All Students only: {len(sc_df):,} records")

        # Filter years
        sc_df['year'] = pd.to_numeric(sc_df['year'], errors='coerce')
        year_range = self.get_year_range()
        sc_df = sc_df[sc_df['year'].between(year_range[0], year_range[1])].copy()
        self.log(f"Years {year_range[0]}-{year_range[1]}: {len(sc_df):,} records")
        self.log(f"Years: {sorted(sc_df['year'].unique())}")

        # Exclude district totals
        sc_df = sc_df[sc_df['school_name'] != '---District Total---'].copy()
        self.log(f"After excluding district totals: {len(sc_df):,} records")
        self.log(f"Unique schools: {sc_df['school_id'].nunique()}")

        # Select and return columns
        result = sc_df[[
            'year', 'school_id', 'school_name', 'district', 'district_number',
            'county_number', 'county_name', 'school_climate_index', 'school_type'
        ]].copy()

        return result


def main():
    """Main execution."""
    print("=" * 60)
    print("CREATE SCHOOL CLIMATE INDEX ANALYSIS DATASET")
    print("=" * 60)

    dataset = SchoolClimateAnalysisDataset(verbose=True)
    df = dataset.create_dataset()

    print("\n" + "=" * 60)
    print("SCHOOL CLIMATE INDEX ANALYSIS DATASET COMPLETE")
    print("=" * 60)

    print(f"\nNext step: Run Bayesian model on {dataset.OUTPUT_DIR / dataset.OUTPUT_FILENAME}")

    return df


if __name__ == "__main__":
    main()
