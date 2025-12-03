#!/usr/bin/env python3
"""
English Learner Progress Analysis Dataset - High Schools

Creates analysis dataset for EL progress rates (score 140 = proficiency) using:
- KPI master file for EL progress scores
- Precomputed demographics (or enrollment-based)
- Teacher quality, financial, and census covariates

Score 140 indicates students achieving English proficiency.

Output: analysis/datasets/el_progress_high_analysis_{student_group}.csv

Usage:
    python el_progress_high_analysis.py                           # All Students (default)
    python el_progress_high_analysis.py --student-group african_american
    python el_progress_high_analysis.py --all-groups              # Run for all target groups
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


class ELProgressHighAnalysisDataset(BaseAnalysisDataset):
    """
    Analysis dataset for high school EL progress rates.

    Filters:
    - metric == 'english_learner_score_140_high'
    - All Students only
    - Unsuppressed values
    """

    OUTCOME_NAME = 'el_progress_rate'
    OUTPUT_FILENAME = 'el_progress_high_analysis.csv'

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
        self.log("LOADING EL PROGRESS DATA (HIGH)", header=True)

        # Use chunked reading to efficiently load from large KPI file
        el_df = self.read_kpi_chunked(['english_learner_score_140_high'])
        self.log(f"Found {len(el_df):,} EL progress high records")

        # Convert value to numeric
        el_df['el_progress_rate'] = pd.to_numeric(el_df['value'], errors='coerce')

        # Remove suppressed values
        el_df = el_df[el_df['suppressed'] != 'Y'].copy()
        self.log(f"After removing suppressed: {len(el_df):,} records")

        # Filter to specified student group
        el_df = el_df[el_df['student_group'] == self.student_group_name].copy()
        self.log(f"{self.student_group_name}: {len(el_df):,} records")

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


def run_for_group(student_group: str) -> pd.DataFrame:
    """Run analysis for a single student group."""
    print("=" * 60)
    print(f"CREATE EL PROGRESS HIGH ANALYSIS DATASET")
    print(f"Student Group: {student_group}")
    print("=" * 60)

    dataset = ELProgressHighAnalysisDataset(verbose=True, student_group=student_group)
    df = dataset.create_dataset()

    print("\n" + "=" * 60)
    print("EL PROGRESS HIGH ANALYSIS DATASET COMPLETE")
    print("=" * 60)

    print(f"\nNext step: Run Bayesian model on {dataset.OUTPUT_DIR / dataset.output_filename}")

    return df


def main():
    """Main execution."""
    parser = argparse.ArgumentParser(
        description="Create EL progress high analysis dataset for specified student group"
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
