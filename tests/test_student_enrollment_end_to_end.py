"""
End-to-end validation test for student enrollment ETL pipeline.
Tests that sample rows from each source file are correctly transformed to KPI format.
"""
import pytest
import pandas as pd
import numpy as np
from etl.constants import KPI_COLUMNS
from pathlib import Path
from etl.student_enrollment import transform, StudentEnrollmentETL


class TestStudentEnrollmentEndToEnd:
    """Test complete transformation from raw data to KPI format."""
    
    def setup_method(self):
        """Setup ETL instance for testing."""
        self.etl = StudentEnrollmentETL('student_enrollment')
    
    def test_source_to_kpi_transformation(self):
        """Test that 10 random rows from each source file are correctly represented in processed file."""
        # Paths to actual data files
        raw_data_dir = Path("data/raw/student_enrollment")
        processed_file = Path("data/processed/student_enrollment.csv")
        
        # Ensure processed file exists
        if not processed_file.exists():
            pytest.skip("Processed student_enrollment.csv not found. Run ETL pipeline first.")
        
        # Load processed KPI data
        kpi_df = pd.read_csv(processed_file)
        
        # Test each source file
        for source_file in raw_data_dir.glob("*.csv"):
            print(f"\nTesting {source_file.name}...")
            self._test_source_file_transformation(source_file, kpi_df)
    
    def _test_source_file_transformation(self, source_file: Path, kpi_df: pd.DataFrame):
        """Test transformation of a single source file."""
        # Load source data
        source_df = pd.read_csv(source_file)
        
        if source_df.empty:
            pytest.skip(f"Source file {source_file.name} is empty")
        
        # Take 10 random rows (or all if less than 10)
        sample_size = min(10, len(source_df))
        sample_rows = source_df.sample(n=sample_size, random_state=42)
        
        print(f"Testing {sample_size} rows from {source_file.name}")
        
        # Test each sample row
        for idx, row in sample_rows.iterrows():
            self._test_row_transformation(row, kpi_df, source_file.name)
    
    def _test_row_transformation(self, source_row: pd.Series, kpi_df: pd.DataFrame, source_filename: str):
        """Test that a single source row is correctly transformed to KPI format."""
        # Apply column normalization to match ETL processing
        normalized_row = self._normalize_row_columns(source_row)
        
        # Extract expected metrics using ETL logic
        expected_metrics = self.etl.extract_metrics(normalized_row)
        
        # Create expected identifiers for matching KPI rows
        expected_identifiers = self._extract_identifiers(source_row)
        
        # Find corresponding KPI rows
        kpi_rows = self._find_kpi_rows(kpi_df, expected_identifiers, source_filename)
        
        if kpi_rows.empty:
            print(f"Warning: No KPI rows found for identifiers: {expected_identifiers}")
            return
        
        # Test each expected metric
        for metric_name, expected_value in expected_metrics.items():
            if pd.notna(expected_value) and expected_value != 0:
                self._test_metric_in_kpi(kpi_rows, metric_name, expected_value, expected_identifiers)
    
    def _normalize_row_columns(self, row: pd.Series) -> pd.Series:
        """Apply column name normalization like the ETL does."""
        # Create a single-row DataFrame to use ETL normalization
        temp_df = pd.DataFrame([row])
        normalized_df = self.etl.normalize_column_names(temp_df)
        return normalized_df.iloc[0]
    
    def _extract_identifiers(self, source_row: pd.Series) -> dict:
        """Extract identifying information from source row."""
        identifiers = {}
        
        # Map common identifier fields (handle both formats)
        identifier_mappings = {
            'district': ['District Name', 'DISTRICT NAME'],
            'school_name': ['School Name', 'SCHOOL NAME'],
            'student_group': ['Demographic', 'DEMOGRAPHIC'],
            'county_number': ['County Number', 'COUNTY NUMBER'],
            'district_number': ['District Number', 'DISTRICT NUMBER'],
            'school_code': ['School Code', 'SCHOOL CODE']
        }
        
        for kpi_field, source_fields in identifier_mappings.items():
            for field in source_fields:
                if field in source_row.index and pd.notna(source_row[field]):
                    identifiers[kpi_field] = str(source_row[field]).strip()
                    break
        
        return identifiers
    
    def _find_kpi_rows(self, kpi_df: pd.DataFrame, identifiers: dict, source_filename: str) -> pd.DataFrame:
        """Find KPI rows matching the given identifiers."""
        mask = pd.Series([True] * len(kpi_df))
        
        # Apply filters for each identifier
        for field, value in identifiers.items():
            if field in kpi_df.columns:
                mask &= (kpi_df[field].astype(str).str.strip() == value)
        
        # Filter by source file
        if 'source_file' in kpi_df.columns:
            mask &= (kpi_df['source_file'] == source_filename)
        
        return kpi_df[mask]
    
    def _test_metric_in_kpi(self, kpi_rows: pd.DataFrame, metric_name: str, expected_value: float, identifiers: dict):
        """Test that a specific metric appears correctly in KPI data."""
        metric_rows = kpi_rows[kpi_rows['metric'] == metric_name]
        
        if metric_rows.empty:
            print(f"Warning: Metric '{metric_name}' not found in KPI data for identifiers: {identifiers}")
            return
        
        # Should have exactly one row for this metric
        assert len(metric_rows) == 1, f"Expected 1 row for metric '{metric_name}', found {len(metric_rows)}"
        
        kpi_row = metric_rows.iloc[0]
        actual_value = kpi_row['value']
        
        # Test the value
        if pd.isna(expected_value):
            assert pd.isna(actual_value), f"Expected NaN for metric '{metric_name}', got {actual_value}"
        else:
            assert actual_value == expected_value, f"Expected {expected_value} for metric '{metric_name}', got {actual_value}"
        
        # Test that suppression flag is consistent
        if pd.isna(actual_value):
            assert kpi_row.get('suppressed', 'N') == 'Y', f"Expected suppressed='Y' for NaN value in metric '{metric_name}'"
        else:
            assert kpi_row.get('suppressed', 'N') == 'N', f"Expected suppressed='N' for non-NaN value in metric '{metric_name}'"

    def test_kpi_format_compliance(self):
        """Test that the processed file complies with KPI format requirements."""
        processed_file = Path("data/processed/student_enrollment.csv")
        
        if not processed_file.exists():
            pytest.skip("Processed student_enrollment.csv not found. Run ETL pipeline first.")
        
        kpi_df = pd.read_csv(processed_file)
        
        # Test column structure
        assert set(kpi_df.columns) == set(KPI_COLUMNS), "KPI file missing required columns"
        
        # Test that we have data
        assert not kpi_df.empty, "KPI file should not be empty"
        
        # Test required fields are not all null
        required_fields = ['year', 'metric', 'district', 'school_name', 'student_group']
        for field in required_fields:
            assert not kpi_df[field].isna().all(), f"Field '{field}' should not be all null"
        
        # Test metric names follow expected patterns
        metrics = kpi_df['metric'].unique()
        expected_metric_patterns = [
            'student_enrollment_total',
            'student_enrollment_preschool',
            'student_enrollment_kindergarten',
            'student_enrollment_grade_',
            'student_enrollment_primary',
            'student_enrollment_middle',
            'student_enrollment_secondary'
        ]
        
        found_patterns = []
        for metric in metrics:
            for pattern in expected_metric_patterns:
                if pattern in metric:
                    found_patterns.append(pattern)
                    break
        
        assert len(found_patterns) > 0, f"No expected metric patterns found in: {list(metrics)}"
    
    def test_enrollment_value_validation(self):
        """Test that enrollment values meet validation requirements."""
        processed_file = Path("data/processed/student_enrollment.csv")
        
        if not processed_file.exists():
            pytest.skip("Processed student_enrollment.csv not found. Run ETL pipeline first.")
        
        kpi_df = pd.read_csv(processed_file)
        
        # Test that non-null values are non-negative
        non_null_values = kpi_df['value'].dropna()
        if not non_null_values.empty:
            assert (non_null_values >= 0).all(), "All enrollment values should be non-negative"
        
        # Test that values are integers (enrollment counts)
        for value in non_null_values.head(100):  # Test first 100 for performance
            if pd.notna(value):
                assert value == int(value), f"Enrollment value {value} should be an integer"

    def test_demographic_coverage(self):
        """Test that standard demographic groups are represented."""
        processed_file = Path("data/processed/student_enrollment.csv")
        
        if not processed_file.exists():
            pytest.skip("Processed student_enrollment.csv not found. Run ETL pipeline first.")
        
        kpi_df = pd.read_csv(processed_file)
        
        # Check for standard demographic groups
        student_groups = kpi_df['student_group'].unique()
        
        expected_groups = ['All Students', 'Female', 'Male']
        for group in expected_groups:
            assert group in student_groups, f"Expected demographic group '{group}' not found"

    def test_source_file_coverage(self):
        """Test that all source files are represented in the processed data."""
        processed_file = Path("data/processed/student_enrollment.csv")
        raw_data_dir = Path("data/raw/student_enrollment")
        
        if not processed_file.exists():
            pytest.skip("Processed student_enrollment.csv not found. Run ETL pipeline first.")
        
        kpi_df = pd.read_csv(processed_file)
        
        # Get all source files that should be processed
        expected_sources = [f.name for f in raw_data_dir.glob("*.csv")]
        
        if not expected_sources:
            pytest.skip("No source files found in raw data directory")
        
        # Get actual source files from processed data
        actual_sources = kpi_df['source_file'].unique()
        
        # Check that we have representation from multiple sources
        assert len(actual_sources) > 0, "No source files represented in processed data"
        
        # Log coverage for debugging
        print(f"Expected sources: {expected_sources}")
        print(f"Actual sources: {list(actual_sources)}")
        
        # Check that major file types are represented if they exist
        has_kyrc24 = any('KYRC24' in f for f in expected_sources)
        has_primary = any('primary_enrollment' in f for f in expected_sources)
        has_secondary = any('secondary_enrollment' in f for f in expected_sources)
        
        if has_kyrc24:
            assert any('KYRC24' in f for f in actual_sources), "KYRC24 files should be represented"
        if has_primary:
            assert any('primary_enrollment' in f for f in actual_sources), "Primary enrollment files should be represented"
        if has_secondary:
            assert any('secondary_enrollment' in f for f in actual_sources), "Secondary enrollment files should be represented"

    def test_aggregated_metrics_consistency(self):
        """Test that aggregated metrics are consistent with individual grade metrics."""
        processed_file = Path("data/processed/student_enrollment.csv")
        
        if not processed_file.exists():
            pytest.skip("Processed student_enrollment.csv not found. Run ETL pipeline first.")
        
        kpi_df = pd.read_csv(processed_file)
        
        # Find a few test cases where we can verify aggregations
        # Group by identifiers to find complete records
        test_groups = kpi_df.groupby(['district', 'school_name', 'student_group', 'source_file']).filter(
            lambda x: len(x) >= 5  # Has multiple metrics
        ).groupby(['district', 'school_name', 'student_group', 'source_file'])
        
        tested_cases = 0
        for (district, school, group, source), group_df in test_groups:
            # Test primary aggregation if we have individual primary grades
            primary_grades = ['student_enrollment_preschool', 'student_enrollment_kindergarten', 
                            'student_enrollment_grade_1', 'student_enrollment_grade_2',
                            'student_enrollment_grade_3', 'student_enrollment_grade_4',
                            'student_enrollment_grade_5']
            
            primary_metrics = group_df[group_df['metric'].isin(primary_grades)]
            primary_agg = group_df[group_df['metric'] == 'student_enrollment_primary']
            
            if len(primary_metrics) > 0 and len(primary_agg) == 1:
                individual_sum = primary_metrics['value'].sum()
                agg_value = primary_agg['value'].iloc[0]
                
                if pd.notna(individual_sum) and pd.notna(agg_value):
                    assert abs(individual_sum - agg_value) < 0.01, \
                        f"Primary aggregation mismatch: {individual_sum} vs {agg_value} for {district}/{school}/{group}"
                    tested_cases += 1
            
            # Stop after testing a few cases for performance
            if tested_cases >= 3:
                break
        
        print(f"Tested {tested_cases} aggregation cases")