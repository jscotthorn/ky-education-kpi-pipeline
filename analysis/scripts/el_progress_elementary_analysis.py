#!/usr/bin/env python3
"""
English Learner Progress Analysis Dataset - Elementary Schools

Creates analysis dataset for EL progress rates (score 140 = proficiency) using:
- KPI master file for EL progress scores
- Precomputed demographics (or enrollment-based)
- Teacher quality, financial, and census covariates

Score 140 indicates students achieving English proficiency.

Output: analysis/datasets/el_progress_elementary_analysis.csv
"""

import pandas as pd
import numpy as np
from pathlib import Path

from base_analysis_dataset import BaseAnalysisDataset


class ELProgressElementaryAnalysisDataset(BaseAnalysisDataset):
    """
    Analysis dataset for elementary school EL progress rates.

    Filters:
    - metric == 'english_learner_score_140_elementary'
    - All Students only
    - Unsuppressed values
    """

    OUTCOME_NAME = 'el_progress_rate'
    OUTPUT_FILENAME = 'el_progress_elementary_analysis.csv'

    def get_merge_strategy(self) -> str:
        """Use school_id for KPI data."""
        return 'school_id'

    def get_year_range(self) -> tuple:
        """Filter to available years."""
        return (2022, 2025)

    def get_school_type_filter(self) -> str:
        """No school type filter - metric is already level-specific."""
        return None

    def use_precomputed_demographics(self) -> bool:
        """Use precomputed demographics file if available."""
        return True

    def load_outcome_data(self) -> pd.DataFrame:
        """Load EL progress data from KPI master."""
        self.log("LOADING EL PROGRESS DATA (ELEMENTARY)", header=True)

        # Use chunked reading to efficiently load from large KPI file
        el_df = self.read_kpi_chunked(['english_learner_score_140_elementary'])
        self.log(f"Found {len(el_df):,} EL progress elementary records")

        # Convert value to numeric
        el_df['el_progress_rate'] = pd.to_numeric(el_df['value'], errors='coerce')

        # Remove suppressed values
        el_df = el_df[el_df['suppressed'] != 'Y'].copy()
        self.log(f"After removing suppressed: {len(el_df):,} records")

        # Filter to All Students only
        el_df = el_df[el_df['student_group'] == 'All Students'].copy()
        self.log(f"All Students only: {len(el_df):,} records")

        # Filter years
        el_df['year'] = pd.to_numeric(el_df['year'], errors='coerce')
        year_range = self.get_year_range()
        el_df = el_df[el_df['year'].between(year_range[0], year_range[1])].copy()
        self.log(f"Years {year_range[0]}-{year_range[1]}: {len(el_df):,} records")
        self.log(f"Years: {sorted(el_df['year'].unique())}")

        # Exclude district totals
        el_df = el_df[el_df['school_name'] != '---District Total---'].copy()
        self.log(f"After excluding district totals: {len(el_df):,} records")
        self.log(f"Unique schools: {el_df['school_id'].nunique()}")

        # Select and return columns
        result = el_df[[
            'year', 'school_id', 'school_name', 'district', 'district_number',
            'county_number', 'county_name', 'el_progress_rate', 'school_type'
        ]].copy()

        return result


def main():
    """Main execution."""
    print("=" * 60)
    print("CREATE EL PROGRESS ELEMENTARY ANALYSIS DATASET")
    print("=" * 60)

    dataset = ELProgressElementaryAnalysisDataset(verbose=True)
    df = dataset.create_dataset()

    print("\n" + "=" * 60)
    print("EL PROGRESS ELEMENTARY ANALYSIS DATASET COMPLETE")
    print("=" * 60)

    print(f"\nNext step: Run Bayesian model on {dataset.OUTPUT_DIR / dataset.OUTPUT_FILENAME}")

    return df


if __name__ == "__main__":
    main()
