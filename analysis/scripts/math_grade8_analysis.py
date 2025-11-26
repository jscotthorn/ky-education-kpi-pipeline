#!/usr/bin/env python3
"""
Grade 8 Math Proficiency Analysis Dataset

Creates analysis dataset for 8th grade math proficiency outcomes using:
- Kentucky Summative Assessment data (processed)
- Student demographics computed from enrollment
- Teacher quality, financial, and census covariates

Uses school_id matching for consistency with other models.

Output: analysis/datasets/math_grade8_analysis.csv
"""

import pandas as pd
import numpy as np
from pathlib import Path

from base_analysis_dataset import BaseAnalysisDataset


class MathGrade8AnalysisDataset(BaseAnalysisDataset):
    """
    Analysis dataset for Grade 8 math proficiency rates.

    Filters:
    - metric == 'kentucky_summative_assessment_math_proficient_distinguished_rate_grade_8'
    - All Students only
    - School-level (not district totals)
    - Unsuppressed values
    """

    OUTCOME_NAME = 'math_proficiency_rate'
    OUTPUT_FILENAME = 'math_grade8_analysis.csv'

    # Target metric in the assessment file
    TARGET_METRIC = 'kentucky_summative_assessment_math_proficient_distinguished_rate_grade_8'
    CHUNK_SIZE = 100_000

    def get_merge_strategy(self) -> str:
        """Use school_id matching (IDs now have consistent format with leading zeros)."""
        return 'school_id'

    def use_precomputed_demographics(self) -> bool:
        """Compute demographics from enrollment data."""
        return False

    def load_outcome_data(self) -> pd.DataFrame:
        """Load Grade 8 Math assessment data using chunked loading."""
        self.log("LOADING GRADE 8 MATH ASSESSMENT DATA", header=True)

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
            self.log("  Warning: No matching Grade 8 Math records found!")
            return pd.DataFrame()

        g8_math = pd.concat(filtered_chunks, ignore_index=True)
        self.log(f"  Grade 8 Math records (unsuppressed): {len(g8_math):,}")

        # Rename value to math_proficiency_rate
        g8_math = g8_math.rename(columns={'value': 'math_proficiency_rate'})

        # Convert to numeric
        g8_math['math_proficiency_rate'] = pd.to_numeric(
            g8_math['math_proficiency_rate'], errors='coerce'
        )

        # Drop rows with missing data
        g8_math = g8_math.dropna(subset=['math_proficiency_rate'])

        # Select key columns
        result = g8_math[[
            'year', 'school_id', 'school_name', 'school_code',
            'district', 'district_number', 'county_number', 'county_name',
            'math_proficiency_rate'
        ]].copy()

        # Rename school_code to school_number for compatibility
        result = result.rename(columns={'school_code': 'school_number'})

        # Print summary by year
        for year in sorted(result['year'].unique()):
            year_count = len(result[result['year'] == year])
            self.log(f"  {year}: {year_count} schools")

        self.log(f"\nTotal observations: {len(result)}")
        self.log(f"Unique schools: {result['school_id'].nunique()}")
        self.log(f"Proficiency rate: mean={result['math_proficiency_rate'].mean():.1f}%, "
                 f"std={result['math_proficiency_rate'].std():.1f}%")

        return result


def main():
    """Main execution."""
    print("=" * 60)
    print("CREATE GRADE 8 MATH ANALYSIS DATASET")
    print("=" * 60)

    dataset = MathGrade8AnalysisDataset(verbose=True)
    df = dataset.create_dataset()

    print("\n" + "=" * 60)
    print("MATH ANALYSIS DATASET COMPLETE")
    print("=" * 60)

    print(f"\nNext step: Run Bayesian model on {dataset.OUTPUT_DIR / dataset.OUTPUT_FILENAME}")

    return df


if __name__ == "__main__":
    main()
