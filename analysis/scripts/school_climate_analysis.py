#!/usr/bin/env python3
"""
School Climate Index Analysis Dataset

Creates analysis dataset for school climate index scores using:
- KPI master file for climate index scores
- Precomputed demographics (or enrollment-based)
- Teacher quality, financial, and census covariates

School climate index measures student perceptions of safety,
engagement, and environment. Scores typically range 0-100.

Level choice (Scott, 2026-10-19): keep one row per school, year, and
level. The published names are climate_index_score_elementary,
climate_index_score_middle, and climate_index_score_high. There is no
unsuffixed climate_index_score. A grade-span pick would drop the other
level when one school code publishes two, as Anchorage Independent
Public School 006010 does. Years are 2023-2026. KYRC23 is the 2023
index. The 2022 accountability number is
climate_safety_indicator_rate_{level}, and the 2023 accountability
number is climate_safety_combined_indicator_rate_{level}. Those are
not this outcome. The calculated survey averages
climate_index_score_calculated and safety_index_score_calculated are
not loaded. Scott removed them on 2026-10-20.

Output: analysis/datasets/school_climate_analysis_{student_group}.csv

Usage:
    python school_climate_analysis.py                           # All Students (default)
    python school_climate_analysis.py --student-group african_american
    python school_climate_analysis.py --all-groups              # Run for all target groups
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


# One outcome column, with level kept beside it. Safety indexes are covariates
# in the catalog, not this outcome.
CLIMATE_INDEX_METRICS = (
    'climate_index_score_elementary',
    'climate_index_score_middle',
    'climate_index_score_high',
)
LEVEL_BY_METRIC = {
    'climate_index_score_elementary': 'elementary',
    'climate_index_score_middle': 'middle',
    'climate_index_score_high': 'high',
}


class SchoolClimateAnalysisDataset(BaseAnalysisDataset):
    """
    Analysis dataset for school climate index scores.

    Level choice: per-level rows, not one collapsed index and not a
    grade-span pick. Each published level stays on its own row.

    Filters:
    - metric in climate_index_score_{elementary,middle,high}
    - The student group passed to the constructor
    - Unsuppressed values
    - Years 2023-2026
    - School rows (district and state totals named ---District Total--- are out)
    """

    OUTCOME_NAME = 'school_climate_index'
    OUTPUT_FILENAME = 'school_climate_analysis.csv'

    def get_merge_strategy(self) -> str:
        """Use school_id for KPI data."""
        return 'school_id'

    def get_year_range(self) -> tuple:
        """KYRC23 through KYRC26. The 2022 indicator rate is a different metric."""
        return (2023, 2026)

    def get_dedupe_subset(self) -> list:
        """Keep elementary and middle when one school publishes both."""
        return ['year', 'school_id', 'level']

    def get_school_type_filter(self) -> str:
        """No school type filter - include all schools."""
        return None

    def use_precomputed_demographics(self) -> bool:
        """Use precomputed demographics file if available."""
        return True

    def load_outcome_data(self) -> pd.DataFrame:
        """Load school climate index data from KPI master."""
        self.log("LOADING SCHOOL CLIMATE INDEX DATA", header=True)

        # Chunked read of the three published climate indexes. No unsuffixed alias.
        sc_df = self.read_kpi_chunked(list(CLIMATE_INDEX_METRICS))
        self.log(f"Found {len(sc_df):,} school climate index records")
        if sc_df.empty:
            return pd.DataFrame(columns=[
                'year', 'school_id', 'school_name', 'district', 'district_number',
                'county_number', 'county_name', 'school_climate_index', 'school_type',
                'level', 'metric',
            ])

        sc_df['level'] = sc_df['metric'].map(LEVEL_BY_METRIC)
        sc_df['school_climate_index'] = pd.to_numeric(sc_df['value'], errors='coerce')

        # Remove suppressed values
        sc_df = sc_df[sc_df['suppressed'] != 'Y'].copy()
        self.log(f"After removing suppressed: {len(sc_df):,} records")

        # Filter to specified student group
        sc_df = sc_df[sc_df['student_group'] == self.student_group_name].copy()
        self.log(f"{self.student_group_name}: {len(sc_df):,} records")

        # Filter years
        sc_df['year'] = pd.to_numeric(sc_df['year'], errors='coerce')
        year_range = self.get_year_range()
        sc_df = sc_df[sc_df['year'].between(year_range[0], year_range[1])].copy()
        self.log(f"Years {year_range[0]}-{year_range[1]}: {len(sc_df):,} records")
        self.log(f"Years: {sorted(sc_df['year'].dropna().unique())}")

        # Exclude district totals. KYRC state rows use this same school name.
        sc_df = sc_df[sc_df['school_name'] != '---District Total---'].copy()
        self.log(f"After excluding district totals: {len(sc_df):,} records")
        self.log(f"Unique schools: {sc_df['school_id'].nunique()}")
        self._log_rows_by_year_and_level(sc_df)

        result = sc_df[[
            'year', 'school_id', 'school_name', 'district', 'district_number',
            'county_number', 'county_name', 'school_climate_index', 'school_type',
            'level', 'metric',
        ]].copy()

        return result

    def _log_rows_by_year_and_level(self, sc_df: pd.DataFrame) -> None:
        """Print the rows kept for the analysis, one line per year and level."""
        self.log("Rows loaded per year and level:")
        if sc_df.empty:
            self.log("  none")
            return
        counts = (
            sc_df.groupby(['year', 'level'], dropna=False)
            .size()
            .reset_index(name='rows')
            .sort_values(['year', 'level'])
        )
        for record in counts.itertuples(index=False):
            year = int(record.year) if pd.notna(record.year) else record.year
            self.log(f"  {year} {record.level}: {int(record.rows):,}")


def run_for_group(student_group: str) -> pd.DataFrame:
    """Run analysis for a single student group."""
    print("=" * 60)
    print(f"CREATE SCHOOL CLIMATE INDEX ANALYSIS DATASET")
    print(f"Student Group: {student_group}")
    print("=" * 60)

    dataset = SchoolClimateAnalysisDataset(verbose=True, student_group=student_group)
    df = dataset.create_dataset()

    print("\n" + "=" * 60)
    print("SCHOOL CLIMATE INDEX ANALYSIS DATASET COMPLETE")
    print("=" * 60)

    print(f"\nNext step: Run Bayesian model on {dataset.OUTPUT_DIR / dataset.output_filename}")

    return df


def main():
    """Main execution."""
    parser = argparse.ArgumentParser(
        description="Create school climate analysis dataset for specified student group"
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
