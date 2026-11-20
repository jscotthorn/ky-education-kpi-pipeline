#!/usr/bin/env python3
"""
Graduation Rate Analysis Dataset

Creates analysis dataset for 4-year graduation rate outcomes using:
- KPI master file for graduation rates
- Precomputed demographics (or enrollment-based)
- Teacher quality, financial, and census covariates

Output: analysis/datasets/graduation_analysis_{student_group}.csv

Usage:
    python graduation_analysis.py                           # All Students (default)
    python graduation_analysis.py --student-group african_american
    python graduation_analysis.py --all-groups              # Run for all target groups
"""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path
import sys

# Add config directory to path for imports
CONFIG_DIR = Path(__file__).parent.parent / "config"
sys.path.insert(0, str(CONFIG_DIR))
from student_groups import ALL_GROUP_SLUGS, TARGET_GROUP_SLUGS

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

        # Filter to specified student group
        grad_df = grad_df[grad_df['student_group'] == self.student_group_name].copy()
        self.log(f"{self.student_group_name}: {len(grad_df):,} records")

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


def run_for_group(student_group: str) -> pd.DataFrame:
    """Run analysis for a single student group."""
    print("=" * 60)
    print(f"CREATE GRADUATION RATE ANALYSIS DATASET")
    print(f"Student Group: {student_group}")
    print("=" * 60)

    dataset = GraduationAnalysisDataset(verbose=True, student_group=student_group)
    df = dataset.create_dataset()

    print("\n" + "=" * 60)
    print("GRADUATION ANALYSIS DATASET COMPLETE")
    print("=" * 60)

    print(f"\nNext step: Run Bayesian model on {dataset.OUTPUT_DIR / dataset.output_filename}")

    return df


def main():
    """Main execution."""
    parser = argparse.ArgumentParser(
        description="Create graduation rate analysis dataset for specified student group"
    )
    parser.add_argument(
        '--student-group',
        type=str,
        default='all_students',
        choices=ALL_GROUP_SLUGS,
        help=f"Student group to analyze. Choices: {ALL_GROUP_SLUGS}"
    )
    parser.add_argument(
        '--all-groups',
        action='store_true',
        help="Run for all student groups (all_students + target demographics)"
    )
    args = parser.parse_args()

    # Determine which groups to run
    if args.all_groups:
        groups_to_run = ['all_students'] + TARGET_GROUP_SLUGS
        print(f"\nRunning for {len(groups_to_run)} student groups: {groups_to_run}\n")
    else:
        groups_to_run = [args.student_group]

    # Run for each group
    results = {}
    for i, group in enumerate(groups_to_run, 1):
        if len(groups_to_run) > 1:
            print(f"\n{'#' * 60}")
            print(f"# GROUP {i}/{len(groups_to_run)}: {group}")
            print(f"{'#' * 60}\n")
        results[group] = run_for_group(group)

    if len(groups_to_run) > 1:
        print(f"\n{'=' * 60}")
        print(f"ALL GROUPS COMPLETE: {len(results)} datasets created")
        print("=" * 60)

    return results if len(results) > 1 else list(results.values())[0]


if __name__ == "__main__":
    main()
