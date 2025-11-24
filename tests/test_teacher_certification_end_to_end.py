"""
End-to-end validation test for teacher certification ETL pipeline.
Tests that sample rows from each source file are correctly transformed to KPI format.
"""
import pytest
import pandas as pd
import numpy as np
from etl.constants import KPI_COLUMNS
from pathlib import Path
from etl.teacher_certification import transform, TeacherCertificationETL


class TestTeacherCertificationEndToEnd:
    """Test complete transformation from raw data to KPI format."""

    def setup_method(self):
        """Setup ETL instance for testing."""
        self.etl = TeacherCertificationETL('teacher_certification')

    def test_source_to_kpi_transformation(self):
        """Test that 10 random rows from each source file are correctly represented in processed file."""
        # Paths to actual data files
        raw_data_dir = Path("data/raw/teacher_certification")
        processed_file = Path("data/processed/teacher_certification.csv")

        # Ensure processed file exists
        if not processed_file.exists():
            pytest.skip("Processed teacher_certification.csv not found. Run ETL pipeline first.")

        # Load processed KPI data
        kpi_df = pd.read_csv(processed_file)

        # Test each source file
        for source_file in raw_data_dir.glob("*.csv"):
            print(f"\nTesting {source_file.name}...")
            self._test_source_file_transformation(source_file, kpi_df)

    def _test_source_file_transformation(self, source_file: Path, kpi_df: pd.DataFrame):
        """Test transformation of a single source file."""
        # Load source data
        source_df = pd.read_csv(source_file, encoding='utf-8-sig')

        if source_df.empty:
            pytest.skip(f"Source file {source_file.name} is empty")

        # Filter to rows with certification data (non-suppressed)
        cert_cols = [col for col in source_df.columns if 'provisional' in col.lower() or 'national board' in col.lower() or 'certified' in col.lower()]
        if not cert_cols:
            pytest.skip(f"Source file {source_file.name} has no certification columns")

        has_data = source_df[cert_cols].notna().any(axis=1)
        source_df_with_data = source_df[has_data]

        if source_df_with_data.empty:
            pytest.skip(f"Source file {source_file.name} has no rows with certification data")

        # Take 10 random rows (or all if less than 10)
        sample_size = min(10, len(source_df_with_data))
        sample_rows = source_df_with_data.sample(n=sample_size, random_state=42)

        print(f"Testing {sample_size} rows from {source_file.name}")

        # Process sample rows through the same transformation pipeline
        processed_sample = self._process_sample_rows(sample_rows, source_file.name)

        if processed_sample.empty:
            print(f"  No KPI rows produced (likely all suppressed)")
            return

        # Validate each processed row exists in KPI data
        for _, expected_row in processed_sample.iterrows():
            self._validate_kpi_row_exists(expected_row, kpi_df, source_file.name)

    def _process_sample_rows(self, sample_df: pd.DataFrame, source_filename: str) -> pd.DataFrame:
        """Process sample rows through the transformation pipeline."""
        # Apply the same transformations as the ETL pipeline
        df = sample_df.copy()

        # Apply normalization
        df = self.etl.normalize_column_names(df)
        df = self.etl.standardize_missing_values(df)

        # Add source file for tracking
        df['source_file'] = source_filename

        # Convert to KPI format
        kpi_df = self.etl.convert_to_kpi_format(df, source_filename)

        return kpi_df

    def _validate_kpi_row_exists(self, expected_row: pd.Series, kpi_df: pd.DataFrame, source_filename: str):
        """Validate that a specific KPI row exists in the processed data."""
        # Use direct matching for reliability
        matching_rows = self._direct_match(expected_row, kpi_df)

        # Assert that we found matching rows
        assert len(matching_rows) > 0, (
            f"No KPI row found for {source_filename}\n"
            f"Expected: school_id={expected_row['school_id']}, year={expected_row['year']}, "
            f"student_group={expected_row['student_group']}, metric={expected_row['metric']}\n"
            f"Source file: {expected_row.get('source_file', 'unknown')}"
        )

        # Validate the value is correct (within tolerance for floating point)
        if 'value' in expected_row and pd.notna(expected_row['value']):
            expected_value = float(expected_row['value'])
            actual_values = matching_rows['value'].values

            # Check if any matching row has the expected value (within tolerance)
            value_match = any(abs(actual_val - expected_value) < 0.01 for actual_val in actual_values if pd.notna(actual_val))

            assert value_match, (
                f"Value mismatch for {source_filename}. "
                f"Expected value: {expected_value}, "
                f"Actual values: {actual_values}"
            )

    def _direct_match(self, expected_row: pd.Series, kpi_df: pd.DataFrame) -> pd.DataFrame:
        """Direct matching on key fields."""
        matches = kpi_df

        # Match on school_id
        if 'school_id' in expected_row and pd.notna(expected_row['school_id']):
            school_id_str = str(expected_row['school_id'])
            matches = matches[matches['school_id'].astype(str) == school_id_str]

        # Match on year (handle both string and int)
        if 'year' in expected_row and pd.notna(expected_row['year']):
            year_val = str(expected_row['year'])
            matches = matches[matches['year'].astype(str) == year_val]

        # Match on student_group
        if 'student_group' in expected_row and pd.notna(expected_row['student_group']):
            matches = matches[matches['student_group'] == expected_row['student_group']]

        # Match on metric
        if 'metric' in expected_row and pd.notna(expected_row['metric']):
            matches = matches[matches['metric'] == expected_row['metric']]

        # Match on source_file
        if 'source_file' in expected_row and pd.notna(expected_row['source_file']):
            matches = matches[matches['source_file'] == expected_row['source_file']]

        return matches

    def test_all_expected_metrics_present(self):
        """Test that all certification metrics are present in the processed data."""
        processed_file = Path("data/processed/teacher_certification.csv")

        if not processed_file.exists():
            pytest.skip("Processed teacher_certification.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        # Expected metrics
        expected_metrics = [
            'emergency_provisional_teacher_rate',
            'emergency_provisional_teacher_count',
            'national_board_certified_rate',
            'national_board_certified_count'
        ]

        actual_metrics = set(kpi_df['metric'].unique())

        for expected_metric in expected_metrics:
            assert expected_metric in actual_metrics, f"Missing expected metric: {expected_metric}"

        print(f"✓ All {len(expected_metrics)} expected certification metrics found")

    def test_student_group_consistency(self):
        """Test that all records have 'All Students' as the student group."""
        processed_file = Path("data/processed/teacher_certification.csv")

        if not processed_file.exists():
            pytest.skip("Processed teacher_certification.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        # All records should have "All Students" as student_group
        unique_groups = kpi_df['student_group'].unique()
        assert len(unique_groups) == 1, f"Expected only 'All Students', found: {unique_groups}"
        assert unique_groups[0] == 'All Students'

        print(f"✓ All {len(kpi_df)} records have 'All Students' as student_group")

    def test_value_ranges(self):
        """Test that certification values are within reasonable ranges."""
        processed_file = Path("data/processed/teacher_certification.csv")

        if not processed_file.exists():
            pytest.skip("Processed teacher_certification.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        # Filter to rate metrics
        rate_df = kpi_df[kpi_df['metric'].str.contains('rate')]
        rate_values = rate_df['value'].dropna()

        # Rates should be 0-100
        assert (rate_values >= 0).all(), "Found negative certification rates"
        assert (rate_values <= 100).all(), "Found certification rates above 100%"

        # Filter to count metrics
        count_df = kpi_df[kpi_df['metric'].str.contains('count')]
        count_values = count_df['value'].dropna()

        # Counts should be non-negative
        assert (count_values >= 0).all(), "Found negative teacher counts"

        print(f"✓ Certification rate range: {rate_values.min():.1f}% - {rate_values.max():.1f}%")
        print(f"✓ Count range: {count_values.min():.0f} - {count_values.max():.0f}")

    def test_required_kpi_columns(self):
        """Test that all required KPI columns are present."""
        processed_file = Path("data/processed/teacher_certification.csv")

        if not processed_file.exists():
            pytest.skip("Processed teacher_certification.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        for required_col in KPI_COLUMNS:
            assert required_col in kpi_df.columns, f"Missing required KPI column: {required_col}"

        print(f"✓ All {len(KPI_COLUMNS)} required KPI columns present")

    def test_years_coverage(self):
        """Test that data from all expected years is present."""
        processed_file = Path("data/processed/teacher_certification.csv")

        if not processed_file.exists():
            pytest.skip("Processed teacher_certification.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        # Expected years (2021-2025)
        expected_years = {'2021', '2022', '2023', '2024', '2025'}
        actual_years = set(kpi_df['year'].astype(str).unique())

        for year in expected_years:
            assert year in actual_years, f"Missing data for year: {year}"

        print(f"✓ Data present for all {len(expected_years)} expected years: {sorted(actual_years)}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
