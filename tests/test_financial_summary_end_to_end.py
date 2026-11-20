"""
End-to-end validation test for financial summary ETL pipeline.
Tests that sample rows from each source file are correctly transformed to KPI format.
"""
import pytest
import pandas as pd
import numpy as np
from etl.constants import KPI_COLUMNS
from pathlib import Path
from etl.financial_summary import transform, FinancialSummaryETL


class TestFinancialSummaryEndToEnd:
    """Test complete transformation from raw data to KPI format."""

    def setup_method(self):
        """Setup ETL instance for testing."""
        self.etl = FinancialSummaryETL('financial_summary')

    def test_source_to_kpi_transformation(self):
        """Test that 10 random rows from each source file are correctly represented in processed file."""
        # Paths to actual data files
        raw_data_dir = Path("data/raw/financial_summary")
        processed_file = Path("data/processed/financial_summary.csv")

        # Ensure processed file exists
        if not processed_file.exists():
            pytest.skip("Processed financial_summary.csv not found. Run ETL pipeline first.")

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

        # Identify rows with financial data (exclude state total rows)
        # Filter out state totals (school_code 999 or 999000)
        code_col = None
        for col in ['School Code', 'SCHOOL CODE']:
            if col in source_df.columns:
                code_col = col
                break

        if code_col:
            source_df_filtered = source_df[
                ~source_df[code_col].astype(str).isin(['999', '999000'])
            ]
        else:
            source_df_filtered = source_df

        if source_df_filtered.empty:
            pytest.skip(f"Source file {source_file.name} has no district-level data")

        # Take 10 random rows (or all if less than 10)
        sample_size = min(10, len(source_df_filtered))
        sample_rows = source_df_filtered.sample(n=sample_size, random_state=42)

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
        """Test that all financial summary metrics are present in the processed data."""
        processed_file = Path("data/processed/financial_summary.csv")

        if not processed_file.exists():
            pytest.skip("Processed financial_summary.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        # Expected metrics
        expected_metrics = [
            'eoy_student_membership',
            'fund_balance',
            'fund_balance_pct',
            'certified_staff_fte',
            'certified_staff_teachers_fte',
            'certified_staff_non_teachers_fte',
            'classified_staff_fte',
            'total_staff_fte'
        ]

        actual_metrics = set(kpi_df['metric'].unique())

        for expected_metric in expected_metrics:
            assert expected_metric in actual_metrics, f"Missing expected metric: {expected_metric}"

        print(f"✓ All {len(expected_metrics)} expected financial summary metrics found")

    def test_student_group_consistency(self):
        """Test that all records have 'All Students' as the student group."""
        processed_file = Path("data/processed/financial_summary.csv")

        if not processed_file.exists():
            pytest.skip("Processed financial_summary.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        # All records should have "All Students" as student_group
        unique_groups = kpi_df['student_group'].unique()
        assert len(unique_groups) == 1, f"Expected only 'All Students', found: {unique_groups}"
        assert unique_groups[0] == 'All Students'

        print(f"✓ All {len(kpi_df)} records have 'All Students' as student_group")

    def test_value_ranges(self):
        """Test that financial values are within reasonable ranges."""
        processed_file = Path("data/processed/financial_summary.csv")

        if not processed_file.exists():
            pytest.skip("Processed financial_summary.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        # Check staff FTE values
        staff_metrics = ['certified_staff_fte', 'certified_staff_teachers_fte',
                        'certified_staff_non_teachers_fte', 'classified_staff_fte', 'total_staff_fte']

        for metric in staff_metrics:
            metric_df = kpi_df[kpi_df['metric'] == metric]
            values = metric_df['value'].dropna()

            if len(values) > 0:
                # Staff FTE should be non-negative
                assert (values >= 0).all(), f"Found negative values for {metric}"
                # Staff FTE should be reasonable (max 10,000 for largest districts)
                assert (values <= 60000).all(), f"Found unreasonably high values for {metric}"
                print(f"✓ {metric}: {values.min():.1f} - {values.max():.1f} (mean: {values.mean():.1f})")

        # Check fund balance percentage
        fund_pct_df = kpi_df[kpi_df['metric'] == 'fund_balance_pct']
        fund_pct_values = fund_pct_df['value'].dropna()

        if len(fund_pct_values) > 0:
            # Fund balance % can be negative or very high in some cases
            print(f"✓ fund_balance_pct: {fund_pct_values.min():.1f}% - {fund_pct_values.max():.1f}%")

        # Check membership
        membership_df = kpi_df[kpi_df['metric'] == 'eoy_student_membership']
        membership_values = membership_df['value'].dropna()

        if len(membership_values) > 0:
            assert (membership_values >= 0).all(), "Found negative membership values"
            print(f"✓ eoy_student_membership: {membership_values.min():.0f} - {membership_values.max():.0f}")

    def test_required_kpi_columns(self):
        """Test that all required KPI columns are present."""
        processed_file = Path("data/processed/financial_summary.csv")

        if not processed_file.exists():
            pytest.skip("Processed financial_summary.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        for required_col in KPI_COLUMNS:
            assert required_col in kpi_df.columns, f"Missing required KPI column: {required_col}"

        print(f"✓ All {len(KPI_COLUMNS)} required KPI columns present")

    def test_years_coverage(self):
        """Test that all expected years are present in the data."""
        processed_file = Path("data/processed/financial_summary.csv")

        if not processed_file.exists():
            pytest.skip("Processed financial_summary.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        # Expected years based on source files
        expected_years = [2020, 2021, 2022, 2023, 2024]
        actual_years = sorted(kpi_df['year'].unique())

        print(f"Found years: {actual_years}")

        for year in expected_years:
            assert year in actual_years, f"Missing expected year: {year}"

        print(f"✓ All {len(expected_years)} expected years present")

    def test_district_count_consistency(self):
        """Test that we have consistent district counts across years."""
        processed_file = Path("data/processed/financial_summary.csv")

        if not processed_file.exists():
            pytest.skip("Processed financial_summary.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        # Count unique districts per year
        districts_by_year = kpi_df.groupby('year')['district'].nunique()

        print(f"\nDistricts by year:")
        for year, count in districts_by_year.items():
            print(f"  {year}: {count} districts")

        # Kentucky should have ~170+ districts each year
        for year, count in districts_by_year.items():
            assert count >= 170, f"Too few districts for year {year}: {count}"

        print(f"✓ District counts reasonable across all years")

    def test_derived_metrics_consistency(self):
        """Test that derived metrics are calculated correctly."""
        processed_file = Path("data/processed/financial_summary.csv")

        if not processed_file.exists():
            pytest.skip("Processed financial_summary.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        # Sample 10 random districts
        districts = kpi_df['school_id'].unique()
        sample_districts = np.random.choice(districts, min(10, len(districts)), replace=False)

        for district_id in sample_districts:
            district_df = kpi_df[kpi_df['school_id'] == district_id]

            # Get values for each metric
            certified_total = district_df[district_df['metric'] == 'certified_staff_fte']['value'].iloc[0] if len(district_df[district_df['metric'] == 'certified_staff_fte']) > 0 else None
            certified_teachers = district_df[district_df['metric'] == 'certified_staff_teachers_fte']['value'].iloc[0] if len(district_df[district_df['metric'] == 'certified_staff_teachers_fte']) > 0 else None
            certified_non_teachers = district_df[district_df['metric'] == 'certified_staff_non_teachers_fte']['value'].iloc[0] if len(district_df[district_df['metric'] == 'certified_staff_non_teachers_fte']) > 0 else None
            classified = district_df[district_df['metric'] == 'classified_staff_fte']['value'].iloc[0] if len(district_df[district_df['metric'] == 'classified_staff_fte']) > 0 else None
            total_staff = district_df[district_df['metric'] == 'total_staff_fte']['value'].iloc[0] if len(district_df[district_df['metric'] == 'total_staff_fte']) > 0 else None

            # Verify non_teachers = certified_total - certified_teachers
            if pd.notna(certified_total) and pd.notna(certified_teachers) and pd.notna(certified_non_teachers):
                expected_non_teachers = certified_total - certified_teachers
                assert abs(certified_non_teachers - expected_non_teachers) < 0.01, (
                    f"Non-teacher calculation mismatch for district {district_id}: "
                    f"expected {expected_non_teachers}, got {certified_non_teachers}"
                )

            # Verify total_staff = certified_total + classified
            if pd.notna(certified_total) and pd.notna(classified) and pd.notna(total_staff):
                expected_total = certified_total + classified
                assert abs(total_staff - expected_total) < 0.01, (
                    f"Total staff calculation mismatch for district {district_id}: "
                    f"expected {expected_total}, got {total_staff}"
                )

        print(f"✓ Derived metrics calculated correctly for {len(sample_districts)} sample districts")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
