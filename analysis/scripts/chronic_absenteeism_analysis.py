#!/usr/bin/env python3
"""
Chronic Absenteeism Analysis Dataset

Creates analysis dataset for chronic absenteeism rates using:
- KPI master file for absenteeism rates
- Precomputed demographics (or enrollment-based)
- Teacher quality, financial, and census covariates

Chronic absenteeism measures the percentage of students who miss
10% or more of school days. Lower rates are better.

Output: analysis/datasets/chronic_absenteeism_analysis.csv
"""

import pandas as pd
import numpy as np
from pathlib import Path

from base_analysis_dataset import BaseAnalysisDataset


class ChronicAbsenteeismAnalysisDataset(BaseAnalysisDataset):
    """
    Analysis dataset for chronic absenteeism rates.

    Filters:
    - All school types (all grades metric)
    - years 2023-2025
    - All Students only
    - Unsuppressed values
    """

    OUTCOME_NAME = 'chronic_absenteeism_rate'
    OUTPUT_FILENAME = 'chronic_absenteeism_analysis.csv'

    def get_merge_strategy(self) -> str:
        """Use school_id for KPI data (consistent IDs in KPI master)."""
        return 'school_id'

    def get_year_range(self) -> tuple:
        """Filter to 2023-2025 (available years)."""
        return (2023, 2025)

    def get_school_type_filter(self) -> str:
        """No school type filter - include all schools."""
        return None

    def use_precomputed_demographics(self) -> bool:
        """Use precomputed demographics file if available."""
        return True

    def load_outcome_data(self) -> pd.DataFrame:
        """Load chronic absenteeism rate data from KPI master."""
        self.log("LOADING CHRONIC ABSENTEEISM RATE DATA", header=True)

        # Read KPI master file
        self.log(f"Reading KPI master file: {self.KPI_FILE}")
        df = pd.read_csv(self.KPI_FILE, low_memory=False)

        # Filter to chronic absenteeism rate (all grades)
        ca_df = df[df['metric'] == 'chronic_absenteeism_rate_all_grades'].copy()
        self.log(f"Found {len(ca_df):,} chronic absenteeism rate records")

        # Convert value to numeric
        ca_df['chronic_absenteeism_rate'] = pd.to_numeric(ca_df['value'], errors='coerce')

        # Remove suppressed values
        ca_df = ca_df[ca_df['suppressed'] != 'Y'].copy()
        self.log(f"After removing suppressed: {len(ca_df):,} records")

        # Filter to All Students only
        ca_df = ca_df[ca_df['student_group'] == 'All Students'].copy()
        self.log(f"All Students only: {len(ca_df):,} records")

        # Filter to years 2023-2025
        ca_df['year'] = pd.to_numeric(ca_df['year'], errors='coerce')
        year_range = self.get_year_range()
        ca_df = ca_df[ca_df['year'].between(year_range[0], year_range[1])].copy()
        self.log(f"Years {year_range[0]}-{year_range[1]}: {len(ca_df):,} records")
        self.log(f"Years: {sorted(ca_df['year'].unique())}")

        # Exclude district totals
        ca_df = ca_df[ca_df['school_name'] != '---District Total---'].copy()
        self.log(f"After excluding district totals: {len(ca_df):,} records")
        self.log(f"Unique schools: {ca_df['school_id'].nunique()}")

        # Select and return columns
        result = ca_df[[
            'year', 'school_id', 'school_name', 'district', 'district_number',
            'county_number', 'county_name', 'chronic_absenteeism_rate', 'school_type'
        ]].copy()

        return result


def main():
    """Main execution."""
    print("=" * 60)
    print("CREATE CHRONIC ABSENTEEISM ANALYSIS DATASET")
    print("=" * 60)

    dataset = ChronicAbsenteeismAnalysisDataset(verbose=True)
    df = dataset.create_dataset()

    print("\n" + "=" * 60)
    print("CHRONIC ABSENTEEISM ANALYSIS DATASET COMPLETE")
    print("=" * 60)

    print(f"\nNext step: Run Bayesian model on {dataset.OUTPUT_DIR / dataset.OUTPUT_FILENAME}")

    return df


if __name__ == "__main__":
    main()
