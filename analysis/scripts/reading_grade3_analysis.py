#!/usr/bin/env python3
"""
Grade 3 Reading Proficiency Analysis Dataset

Creates analysis dataset for 3rd grade reading proficiency outcomes using:
- Kentucky Summative Assessment data (processed)
- Student demographics computed from enrollment
- Teacher quality, financial, and census covariates

Uses name-based matching (school_name + district) instead of school_id
due to ID format differences between assessment and tract data.

Output: analysis/datasets/reading_grade3_analysis.csv
"""

import pandas as pd
import numpy as np
from pathlib import Path

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
                (chunk['student_group'] == 'All Students') &
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


def main():
    """Main execution."""
    print("=" * 60)
    print("CREATE GRADE 3 READING ANALYSIS DATASET")
    print("=" * 60)

    dataset = ReadingGrade3AnalysisDataset(verbose=True)
    df = dataset.create_dataset()

    print("\n" + "=" * 60)
    print("READING ANALYSIS DATASET COMPLETE")
    print("=" * 60)

    print(f"\nNext step: Run Bayesian model on {dataset.OUTPUT_DIR / dataset.OUTPUT_FILENAME}")

    return df


if __name__ == "__main__":
    main()
