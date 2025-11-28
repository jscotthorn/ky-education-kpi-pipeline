#!/usr/bin/env python3
"""
Graduation Rate Analysis Dataset

Creates analysis dataset for 4-year graduation rate outcomes using:
- KPI master file for graduation rates
- Precomputed demographics (or enrollment-based)
- Teacher quality, financial, and census covariates

Output: analysis/datasets/graduation_analysis.csv
"""

import pandas as pd
import numpy as np
from pathlib import Path

from base_analysis_dataset import BaseAnalysisDataset


class GraduationAnalysisDataset(BaseAnalysisDataset):
    """
    Analysis dataset for 4-year graduation rates.

    Filters:
    - school_type == 'A1' (traditional high schools)
    - years 2021-2025
    - All Students only
    - Unsuppressed values
    """

    OUTCOME_NAME = 'graduation_rate'
    OUTPUT_FILENAME = 'graduation_analysis.csv'

    def get_merge_strategy(self) -> str:
        """Use school_id for graduation data (consistent IDs in KPI master)."""
        return 'school_id'

    def get_year_range(self) -> tuple:
        """Filter to 2021-2025."""
        return (2021, 2025)

    def get_school_type_filter(self) -> str:
        """Filter to A1 (traditional high schools)."""
        return 'A1'

    def use_precomputed_demographics(self) -> bool:
        """Use precomputed demographics file if available."""
        return True

    def load_outcome_data(self) -> pd.DataFrame:
        """Load graduation rate data from KPI master."""
        self.log("LOADING GRADUATION RATE DATA", header=True)

        # Use chunked reading to efficiently load from large KPI file
        grad_df = self.read_kpi_chunked(['graduation_rate_4_year'])
        self.log(f"Found {len(grad_df):,} graduation rate records")

        # Convert value to numeric
        grad_df['graduation_rate'] = pd.to_numeric(grad_df['value'], errors='coerce')

        # Remove suppressed values
        grad_df = grad_df[grad_df['suppressed'] != 'Y'].copy()
        self.log(f"After removing suppressed: {len(grad_df):,} records")

        # Filter to All Students only
        grad_df = grad_df[grad_df['student_group'] == 'All Students'].copy()
        self.log(f"All Students only: {len(grad_df):,} records")

        # Filter to years 2021-2025
        grad_df['year'] = pd.to_numeric(grad_df['year'], errors='coerce')
        year_range = self.get_year_range()
        grad_df = grad_df[grad_df['year'].between(year_range[0], year_range[1])].copy()
        self.log(f"Years {year_range[0]}-{year_range[1]}: {len(grad_df):,} records")
        self.log(f"Years: {sorted(grad_df['year'].unique())}")

        # Filter to Type A1 schools (traditional high schools)
        school_type = self.get_school_type_filter()
        grad_df = grad_df[grad_df['school_type'] == school_type].copy()
        self.log(f"Type {school_type} schools only: {len(grad_df):,} records")
        self.log(f"Unique schools: {grad_df['school_id'].nunique()}")

        # Select and return columns
        result = grad_df[[
            'year', 'school_id', 'school_name', 'district', 'district_number',
            'county_number', 'county_name', 'graduation_rate', 'school_type'
        ]].copy()

        return result


def main():
    """Main execution."""
    print("=" * 60)
    print("CREATE GRADUATION RATE ANALYSIS DATASET")
    print("=" * 60)

    dataset = GraduationAnalysisDataset(verbose=True)
    df = dataset.create_dataset()

    print("\n" + "=" * 60)
    print("GRADUATION ANALYSIS DATASET COMPLETE")
    print("=" * 60)

    print(f"\nNext step: Run Bayesian model on {dataset.OUTPUT_DIR / dataset.OUTPUT_FILENAME}")

    return df


if __name__ == "__main__":
    main()
