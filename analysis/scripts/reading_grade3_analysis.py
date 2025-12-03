#!/usr/bin/env python3
"""
Grade 3 Reading Proficiency Analysis Dataset

Creates analysis dataset for 3rd grade reading proficiency outcomes using:
- Kentucky Summative Assessment data (processed)
- Student demographics computed from enrollment
- Teacher quality, financial, and census covariates

Uses name-based matching (school_name + district) instead of school_id
due to ID format differences between assessment and tract data.

Output: analysis/datasets/reading_grade3_analysis_{student_group}.csv

Usage:
    python reading_grade3_analysis.py                           # All Students (default)
    python reading_grade3_analysis.py --student-group african_american
    python reading_grade3_analysis.py --all-groups              # Run for all target groups
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


class ReadingGrade3AnalysisDataset(BaseAnalysisDataset):
    """
    Analysis dataset for Grade 3 reading proficiency rates.

    Filters:
    - metric == 'kentucky_summative_assessment_reading_proficient_distinguished_rate_grade_3'
    - All Students only
    - School-level (not district totals)
    - Unsuppressed values
    """

    OUTCOME_NAME = 'reading_proficiency_rate'
    OUTPUT_FILENAME = 'reading_grade3_analysis.csv'

    # Target metric in the assessment file
    TARGET_METRIC = 'kentucky_summative_assessment_reading_proficient_distinguished_rate_grade_3'
    CHUNK_SIZE = 100_000

    def get_merge_strategy(self) -> str:
        """Use school_id matching (IDs now have consistent format with leading zeros)."""
        return 'school_id'

    def use_precomputed_demographics(self) -> bool:
        """Compute demographics from enrollment data."""
        return False

    def load_outcome_data(self) -> pd.DataFrame:
        """Load Grade 3 Reading assessment data using chunked loading."""
        self.log("LOADING GRADE 3 READING ASSESSMENT DATA", header=True)

        assessment_file = self.PROCESSED_DIR / "kentucky_summative_assessment.csv"
        if not assessment_file.exists():
            self.log(f"  Error: {assessment_file} not found")
            return pd.DataFrame()

        self.log(f"  Loading from: {assessment_file}")
        self.log("  Reading file in chunks (filtering as we go)...")

        # Read in chunks, filter each chunk, then concatenate
        filtered_chunks = []
        total_rows_read = 0

        for chunk_num, chunk in enumerate(pd.read_csv(assessment_file, chunksize=self.CHUNK_SIZE)):
            total_rows_read += len(chunk)

            # Filter this chunk immediately to save memory
            chunk_filtered = chunk[
                (chunk['metric'] == self.TARGET_METRIC) &
                (chunk['student_group'] == self.student_group_name) &
                (chunk['school_name'] != '---District Total---') &
                (chunk['suppressed'] != 'Y')
            ]

            if len(chunk_filtered) > 0:
                filtered_chunks.append(chunk_filtered)

            # Progress update every 5 chunks
            if (chunk_num + 1) % 5 == 0:
                self.log(f"    Processed {total_rows_read:,} rows, found {sum(len(c) for c in filtered_chunks):,} matching records...")

        self.log(f"  Total rows processed: {total_rows_read:,}")

        if not filtered_chunks:
            self.log("  Warning: No matching Grade 3 Reading records found!")
            return pd.DataFrame()

        g3_reading = pd.concat(filtered_chunks, ignore_index=True)
        self.log(f"  Grade 3 Reading records (unsuppressed): {len(g3_reading):,}")

        # Rename value to reading_proficiency_rate
        g3_reading = g3_reading.rename(columns={'value': 'reading_proficiency_rate'})

        # Convert to numeric
        g3_reading['reading_proficiency_rate'] = pd.to_numeric(
            g3_reading['reading_proficiency_rate'], errors='coerce'
        )

        # Drop rows with missing data
        g3_reading = g3_reading.dropna(subset=['reading_proficiency_rate'])

        # Select key columns
        result = g3_reading[[
            'year', 'school_id', 'school_name', 'school_code',
            'district', 'district_number', 'county_number', 'county_name',
            'reading_proficiency_rate'
        ]].copy()

        # Rename school_code to school_number for compatibility
        result = result.rename(columns={'school_code': 'school_number'})

        # Print summary by year
        for year in sorted(result['year'].unique()):
            year_count = len(result[result['year'] == year])
            self.log(f"  {year}: {year_count} schools")

        self.log(f"\nTotal observations: {len(result)}")
        self.log(f"Unique schools: {result['school_id'].nunique()}")
        self.log(f"Proficiency rate: mean={result['reading_proficiency_rate'].mean():.1f}%, "
                 f"std={result['reading_proficiency_rate'].std():.1f}%")

        return result


def run_for_group(student_group: str) -> pd.DataFrame:
    """Run analysis for a single student group."""
    print("=" * 60)
    print(f"CREATE GRADE 3 READING ANALYSIS DATASET")
    print(f"Student Group: {student_group}")
    print("=" * 60)

    dataset = ReadingGrade3AnalysisDataset(verbose=True, student_group=student_group)
    df = dataset.create_dataset()

    print("\n" + "=" * 60)
    print("READING ANALYSIS DATASET COMPLETE")
    print("=" * 60)

    print(f"\nNext step: Run Bayesian model on {dataset.OUTPUT_DIR / dataset.output_filename}")

    return df


def main():
    """Main execution."""
    parser = argparse.ArgumentParser(
        description="Create Grade 3 reading analysis dataset for specified student group"
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
