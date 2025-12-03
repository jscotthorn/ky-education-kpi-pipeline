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

Output: analysis/datasets/kindergarten_readiness_analysis_{student_group}.csv

Usage:
    python kindergarten_readiness_analysis.py                           # All Students (default)
    python kindergarten_readiness_analysis.py --student-group african_american
    python kindergarten_readiness_analysis.py --all-groups              # Run for all target groups
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

        # Filter to specified student group
        kinder_df = kinder_df[kinder_df['student_group'] == self.student_group_name].copy()
        self.log(f"  {self.student_group_name}: {len(kinder_df):,}")

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


def run_for_group(student_group: str) -> pd.DataFrame:
    """Run analysis for a single student group."""
    print("=" * 60)
    print(f"CREATE KINDERGARTEN READINESS ANALYSIS DATASET")
    print(f"Student Group: {student_group}")
    print("=" * 60)

    dataset = KindergartenReadinessAnalysisDataset(verbose=True, student_group=student_group)
    df = dataset.create_dataset()

    print("\n" + "=" * 60)
    print("KINDERGARTEN READINESS ANALYSIS DATASET COMPLETE")
    print("=" * 60)

    print(f"\nNext step: Run Bayesian model on {dataset.OUTPUT_DIR / dataset.output_filename}")

    return df


def main():
    """Main execution."""
    parser = argparse.ArgumentParser(
        description="Create kindergarten readiness analysis dataset for specified student group"
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
