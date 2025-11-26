"""
End-to-end validation test for district school list ETL pipeline.
Tests that sample rows from each source file are correctly transformed to KPI format.
"""
import pytest
import pandas as pd
import numpy as np
from etl.constants import KPI_COLUMNS
from pathlib import Path
from etl.district_school_list import transform, DistrictSchoolListETL


class TestDistrictSchoolListEndToEnd:
    """Test complete transformation from raw data to KPI format."""

    def setup_method(self):
        """Setup ETL instance for testing."""
        self.etl = DistrictSchoolListETL('district_school_list')

    def test_source_to_kpi_transformation(self):
        """Test that 10 random rows from each source file are correctly represented in processed file."""
        # Paths to actual data files
        raw_data_dir = Path("data/raw/district_school_list")
        processed_file = Path("data/processed/district_school_list.csv")

        # Ensure processed file exists
        if not processed_file.exists():
            pytest.skip("Processed district_school_list.csv not found. Run ETL pipeline first.")

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

        # Get column names in lowercase for flexible matching
        source_cols_lower = {col.lower(): col for col in source_df.columns}

        # Filter to rows with valid coordinates and not district totals
        lat_col = source_cols_lower.get('latitude')
        lon_col = source_cols_lower.get('longitude')
        school_name_col = source_cols_lower.get('school name')

        if not lat_col or not lon_col:
            pytest.skip(f"Source file {source_file.name} missing coordinate columns")

        has_coords = source_df[lat_col].notna() & source_df[lon_col].notna()

        # Filter out district totals
        if school_name_col:
            not_district_total = ~source_df[school_name_col].str.contains(
                'District Total|All Schools', case=False, na=False
            )
            has_coords = has_coords & not_district_total

        source_df_with_data = source_df[has_coords]

        if source_df_with_data.empty:
            pytest.skip(f"Source file {source_file.name} has no rows with valid coordinates")

        # Take 10 random rows (or all if less than 10)
        sample_size = min(10, len(source_df_with_data))
        sample_rows = source_df_with_data.sample(n=sample_size, random_state=42)

        print(f"Testing {sample_size} rows from {source_file.name}")

        # Process sample rows through the same transformation pipeline
        processed_sample = self._process_sample_rows(sample_rows, source_file.name)

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
            value_match = any(abs(actual_val - expected_value) < 0.0001 for actual_val in actual_values if pd.notna(actual_val))

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
        """Test that all coordinate metrics are present in the processed data."""
        processed_file = Path("data/processed/district_school_list.csv")

        if not processed_file.exists():
            pytest.skip("Processed district_school_list.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        # Expected metrics
        expected_metrics = [
            'school_latitude',
            'school_longitude'
        ]

        actual_metrics = set(kpi_df['metric'].unique())

        for expected_metric in expected_metrics:
            assert expected_metric in actual_metrics, f"Missing expected metric: {expected_metric}"

        print(f"All {len(expected_metrics)} expected coordinate metrics found")

    def test_student_group_consistency(self):
        """Test that all records have 'All Students' as the student group."""
        processed_file = Path("data/processed/district_school_list.csv")

        if not processed_file.exists():
            pytest.skip("Processed district_school_list.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        # All records should have "All Students" as student_group
        unique_groups = kpi_df['student_group'].unique()
        assert len(unique_groups) == 1, f"Expected only 'All Students', found: {unique_groups}"
        assert unique_groups[0] == 'All Students'

        print(f"All {len(kpi_df)} records have 'All Students' as student_group")

    def test_coordinate_value_ranges(self):
        """Test that coordinate values are within reasonable Kentucky ranges."""
        processed_file = Path("data/processed/district_school_list.csv")

        if not processed_file.exists():
            pytest.skip("Processed district_school_list.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        # Check latitude range (Kentucky is ~36-39 degrees)
        lat_values = kpi_df[kpi_df['metric'] == 'school_latitude']['value'].dropna()
        assert (lat_values >= 35.0).all(), "Found latitude below 35 degrees"
        assert (lat_values <= 40.0).all(), "Found latitude above 40 degrees"

        # Check longitude range (Kentucky is ~-89 to -82 degrees)
        lon_values = kpi_df[kpi_df['metric'] == 'school_longitude']['value'].dropna()
        assert (lon_values >= -90.0).all(), "Found longitude below -90 degrees"
        assert (lon_values <= -80.0).all(), "Found longitude above -80 degrees"

        # Log some statistics
        print(f"Latitude range: {lat_values.min():.6f} to {lat_values.max():.6f}")
        print(f"Longitude range: {lon_values.min():.6f} to {lon_values.max():.6f}")

    def test_required_kpi_columns(self):
        """Test that all required KPI columns are present."""
        processed_file = Path("data/processed/district_school_list.csv")

        if not processed_file.exists():
            pytest.skip("Processed district_school_list.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        for required_col in KPI_COLUMNS:
            assert required_col in kpi_df.columns, f"Missing required KPI column: {required_col}"

        print(f"All {len(KPI_COLUMNS)} required KPI columns present")

    def test_year_coverage(self):
        """Test that data spans multiple years."""
        processed_file = Path("data/processed/district_school_list.csv")

        if not processed_file.exists():
            pytest.skip("Processed district_school_list.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        years = sorted(kpi_df['year'].unique())
        print(f"Years present: {years}")

        # Should have data from at least 2020-2025
        assert len(years) >= 5, f"Expected at least 5 years, found: {len(years)}"

    def test_latitude_longitude_pairs(self):
        """Test that each school/year has both latitude and longitude."""
        processed_file = Path("data/processed/district_school_list.csv")

        if not processed_file.exists():
            pytest.skip("Processed district_school_list.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        # Group by school_id, year, source_file and check for both metrics
        grouped = kpi_df.groupby(['school_id', 'year', 'source_file'])

        mismatched = []
        for (school_id, year, source_file), group in grouped:
            metrics = set(group['metric'].unique())
            if metrics != {'school_latitude', 'school_longitude'}:
                mismatched.append((school_id, year, source_file, metrics))

        assert len(mismatched) == 0, (
            f"Found {len(mismatched)} school/year combinations with mismatched metrics:\n"
            f"Examples: {mismatched[:5]}"
        )

        print(f"All {len(grouped)} school/year combinations have both latitude and longitude")

    def test_school_count_reasonable(self):
        """Test that we have a reasonable number of schools per year."""
        processed_file = Path("data/processed/district_school_list.csv")

        if not processed_file.exists():
            pytest.skip("Processed district_school_list.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        # Count unique schools per year (divide by 2 since we have lat and lon)
        schools_by_year = kpi_df.groupby('year')['school_id'].nunique()

        print(f"Schools per year:\n{schools_by_year}")

        # Kentucky has ~1,400-1,500 schools
        for year, count in schools_by_year.items():
            assert count >= 1000, f"Year {year} has fewer than 1000 schools: {count}"
            assert count <= 2000, f"Year {year} has more than 2000 schools: {count}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
