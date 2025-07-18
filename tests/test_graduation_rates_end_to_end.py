"""
End-to-end validation test for graduation rates ETL pipeline.
Tests that sample rows from each source file are correctly transformed to KPI format.
"""
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from etl.graduation_rates import transform, normalize_column_names, convert_to_kpi_format


class TestGraduationRatesEndToEnd:
    """Test complete transformation from raw data to KPI format."""
    
    def test_source_to_kpi_transformation(self):
        """Test that 10 random rows from each source file are correctly represented in processed file."""
        # Paths to actual data files
        raw_data_dir = Path("/Users/scott/Projects/equity-etl/data/raw/graduation_rates")
        processed_file = Path("/Users/scott/Projects/equity-etl/data/processed/graduation_rates.csv")
        
        # Ensure processed file exists
        if not processed_file.exists():
            pytest.skip("Processed graduation_rates.csv not found. Run ETL pipeline first.")
        
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
        df = normalize_column_names(df)
        
        # Add source file for tracking
        df['source_file'] = source_filename
        
        # Convert to KPI format
        kpi_df = convert_to_kpi_format(df)
        
        return kpi_df
    
    def _validate_kpi_row_exists(self, expected_row: pd.Series, kpi_df: pd.DataFrame, source_filename: str):
        """Validate that a specific KPI row exists in the processed data."""
        # Build query conditions - be more flexible with matching
        conditions = []
        
        # Match on key identifying fields
        if 'school_id' in expected_row and pd.notna(expected_row['school_id']):
            # Convert to string for comparison
            school_id_str = str(expected_row['school_id'])
            conditions.append(f"school_id == '{school_id_str}'")
        
        if 'year' in expected_row and pd.notna(expected_row['year']):
            conditions.append(f"year == '{expected_row['year']}'")
        
        if 'student_group' in expected_row and pd.notna(expected_row['student_group']):
            # Escape single quotes in student group names
            student_group = str(expected_row['student_group']).replace("'", "\\'")
            conditions.append(f"student_group == '{student_group}'")
        
        if 'metric' in expected_row and pd.notna(expected_row['metric']):
            conditions.append(f"metric == '{expected_row['metric']}'")
        
        if 'source_file' in expected_row and pd.notna(expected_row['source_file']):
            conditions.append(f"source_file == '{expected_row['source_file']}'")
        
        # Use direct matching for reliability
        matching_rows = self._direct_match(expected_row, kpi_df)
        
        # Assert that we found matching rows
        assert len(matching_rows) > 0, (
            f"No KPI row found for {source_filename} with conditions: {conditions}\n"
            f"Expected: {expected_row.to_dict()}\n"
            f"Available school_ids for this student_group: {self._get_debug_info(expected_row, kpi_df)}"
        )
        
        # Validate the value is correct (within tolerance for floating point)
        if 'value' in expected_row and pd.notna(expected_row['value']):
            expected_value = float(expected_row['value'])
            actual_values = matching_rows['value'].values
            
            # Check if any matching row has the expected value (within tolerance)
            value_match = any(abs(actual_val - expected_value) < 0.01 for actual_val in actual_values)
            
            assert value_match, (
                f"Value mismatch for {source_filename}. "
                f"Expected: {expected_value}, Found: {actual_values.tolist()}"
            )
        
        print(f"✓ Validated KPI row: {expected_row['school_id']} - {expected_row['student_group']} - {expected_row['metric']} = {expected_row['value']}")
    
    def _direct_match(self, expected_row: pd.Series, kpi_df: pd.DataFrame) -> pd.DataFrame:
        """Direct matching fallback when query fails."""
        mask = pd.Series(True, index=kpi_df.index)
        
        if 'school_id' in expected_row and pd.notna(expected_row['school_id']):
            # Handle both string and numeric school_id comparisons
            expected_school_id = str(expected_row['school_id'])
            # Remove .0 if present in expected value
            if expected_school_id.endswith('.0'):
                expected_school_id = expected_school_id[:-2]
            # Convert KPI school_id to int then string to remove .0
            mask &= kpi_df['school_id'].astype(int).astype(str) == expected_school_id
        
        if 'year' in expected_row and pd.notna(expected_row['year']):
            # Convert year to int for comparison  
            expected_year = int(expected_row['year'])
            mask &= kpi_df['year'] == expected_year
        
        if 'student_group' in expected_row and pd.notna(expected_row['student_group']):
            mask &= kpi_df['student_group'] == str(expected_row['student_group'])
        
        if 'metric' in expected_row and pd.notna(expected_row['metric']):
            mask &= kpi_df['metric'] == str(expected_row['metric'])
        
        if 'source_file' in expected_row and pd.notna(expected_row['source_file']):
            mask &= kpi_df['source_file'] == str(expected_row['source_file'])
        
        return kpi_df[mask]
    
    def _get_debug_info(self, expected_row: pd.Series, kpi_df: pd.DataFrame) -> list:
        """Get debug information for troubleshooting."""
        if 'student_group' in expected_row and 'source_file' in expected_row:
            matching_df = kpi_df[
                (kpi_df['student_group'] == expected_row['student_group']) &
                (kpi_df['source_file'] == expected_row['source_file'])
            ]
            return matching_df['school_id'].unique().tolist()[:5]  # Return first 5 for brevity
        return []


class TestGraduationRatesDataQuality:
    """Test data quality of the processed graduation rates."""
    
    def test_kpi_format_compliance(self):
        """Test that processed file follows KPI format requirements."""
        processed_file = Path("/Users/scott/Projects/equity-etl/data/processed/graduation_rates.csv")
        
        if not processed_file.exists():
            pytest.skip("Processed graduation_rates.csv not found. Run ETL pipeline first.")
        
        kpi_df = pd.read_csv(processed_file)
        
        # Test required columns exist
        required_columns = ['district', 'school_id', 'school_name', 'year', 
                           'student_group', 'metric', 'value', 'source_file', 'last_updated']
        
        for col in required_columns:
            assert col in kpi_df.columns, f"Required column '{col}' missing from KPI file"
        
        # Test data types
        assert pd.api.types.is_numeric_dtype(kpi_df['value']), "Value column should be numeric"
        assert pd.api.types.is_object_dtype(kpi_df['metric']), "Metric column should be string"
        assert pd.api.types.is_object_dtype(kpi_df['student_group']), "Student_group column should be string"
        
        # Test no completely empty rows
        assert not kpi_df.isnull().all(axis=1).any(), "Found completely empty rows"
        
        # Test graduation rate values are reasonable
        grad_rate_rows = kpi_df[kpi_df['metric'].str.contains('graduation_rate', na=False)]
        if len(grad_rate_rows) > 0:
            assert grad_rate_rows['value'].min() >= 0, "Graduation rates should be >= 0"
            assert grad_rate_rows['value'].max() <= 100, "Graduation rates should be <= 100"
    
    def test_metric_coverage(self):
        """Test that expected metrics are present."""
        processed_file = Path("/Users/scott/Projects/equity-etl/data/processed/graduation_rates.csv")
        
        if not processed_file.exists():
            pytest.skip("Processed graduation_rates.csv not found. Run ETL pipeline first.")
        
        kpi_df = pd.read_csv(processed_file)
        
        # Test expected metrics exist
        metrics = kpi_df['metric'].unique()
        
        # Rate metrics should always exist
        assert 'graduation_rate_4_year' in metrics, "4-year graduation rate metric missing"
        
        # 5-year rate should exist for some years (2021 data has it)
        if len(kpi_df[kpi_df['source_file'].str.contains('2021', na=False)]) > 0:
            assert 'graduation_rate_5_year' in metrics, "5-year graduation rate metric missing from 2021 data"
        
        # Count metrics should exist for files that have count data (2021 data)
        count_data_files = kpi_df[kpi_df['source_file'].str.contains('2021', na=False)]
        if len(count_data_files) > 0:
            assert 'graduation_count_4_year' in metrics, "4-year graduation count metric missing from 2021 data"
            assert 'graduation_total_4_year' in metrics, "4-year graduation total metric missing from 2021 data"
            assert 'graduation_count_5_year' in metrics, "5-year graduation count metric missing from 2021 data"
            assert 'graduation_total_5_year' in metrics, "5-year graduation total metric missing from 2021 data"
        
        print(f"Found metrics: {list(metrics)}")
        
        # Test metric naming convention
        rate_metrics = [m for m in metrics if m.endswith('_rate_4_year') or m.endswith('_rate_5_year')]
        count_metrics = [m for m in metrics if m.endswith('_count_4_year') or m.endswith('_count_5_year')]
        total_metrics = [m for m in metrics if m.endswith('_total_4_year') or m.endswith('_total_5_year')]
        
        print(f"Rate metrics: {len(rate_metrics)}")
        print(f"Count metrics: {len(count_metrics)}")
        print(f"Total metrics: {len(total_metrics)}")
        
        # Verify count and total metrics have integer values
        if count_metrics:
            count_values = kpi_df[kpi_df['metric'].isin(count_metrics)]['value']
            assert all(count_values % 1 == 0), "Count metrics should have integer values"
            
        if total_metrics:
            total_values = kpi_df[kpi_df['metric'].isin(total_metrics)]['value']
            assert all(total_values % 1 == 0), "Total metrics should have integer values"
    
    def test_source_file_tracking(self):
        """Test that source file tracking is working correctly."""
        processed_file = Path("/Users/scott/Projects/equity-etl/data/processed/graduation_rates.csv")
        
        if not processed_file.exists():
            pytest.skip("Processed graduation_rates.csv not found. Run ETL pipeline first.")
        
        kpi_df = pd.read_csv(processed_file)
        
        # Test source files are tracked
        source_files = kpi_df['source_file'].unique()
        
        expected_files = [
            'KYRC24_ACCT_4_Year_High_School_Graduation.csv',
            'graduation_rate_2021.csv',
            'graduation_rate_2022.csv', 
            'graduation_rate_2023.csv'
        ]
        
        for expected_file in expected_files:
            assert expected_file in source_files, f"Expected source file {expected_file} not found in tracking"
        
        print(f"Source files tracked: {list(source_files)}")
    
    def test_student_group_consistency(self):
        """Test that student groups are consistently named."""
        processed_file = Path("/Users/scott/Projects/equity-etl/data/processed/graduation_rates.csv")
        
        if not processed_file.exists():
            pytest.skip("Processed graduation_rates.csv not found. Run ETL pipeline first.")
        
        kpi_df = pd.read_csv(processed_file)
        
        # Test common student groups exist
        student_groups = kpi_df['student_group'].unique()
        
        expected_groups = ['All Students', 'Female', 'Male']
        
        for expected_group in expected_groups:
            assert expected_group in student_groups, f"Expected student group '{expected_group}' not found"
        
        # Test no null or empty student groups
        assert not kpi_df['student_group'].isnull().any(), "Found null student groups"
        assert not (kpi_df['student_group'] == '').any(), "Found empty student groups"
        
        print(f"Student groups found: {len(student_groups)} unique groups")
        print(f"Sample groups: {list(student_groups)[:10]}")
    
    def test_expanded_kpi_format(self):
        """Test that expanded KPI format with counts and totals is working correctly."""
        processed_file = Path("/Users/scott/Projects/equity-etl/data/processed/graduation_rates.csv")
        
        if not processed_file.exists():
            pytest.skip("Processed graduation_rates.csv not found. Run ETL pipeline first.")
        
        kpi_df = pd.read_csv(processed_file)
        
        # Focus on 2021 data which should have count information
        data_2021 = kpi_df[kpi_df['source_file'].str.contains('2021', na=False)]
        
        if len(data_2021) == 0:
            pytest.skip("No 2021 data found (count data unavailable)")
        
        # Test that we have rate, count, and total metrics for the same school/group combinations
        # Group by school_id, student_group, and year
        grouped = data_2021.groupby(['school_id', 'student_group', 'year'])
        
        complete_metric_sets = 0
        
        for (school_id, student_group, year), group in grouped:
            metrics = set(group['metric'].values)
            
            # Check for 4-year graduation metrics
            if 'graduation_rate_4_year' in metrics:
                has_count = 'graduation_count_4_year' in metrics
                has_total = 'graduation_total_4_year' in metrics
                
                if has_count and has_total:
                    complete_metric_sets += 1
                    
                    # Verify rate calculation consistency
                    rate_row = group[group['metric'] == 'graduation_rate_4_year']
                    count_row = group[group['metric'] == 'graduation_count_4_year']
                    total_row = group[group['metric'] == 'graduation_total_4_year']
                    
                    if len(rate_row) > 0 and len(count_row) > 0 and len(total_row) > 0:
                        rate_value = rate_row['value'].iloc[0]
                        count_value = count_row['value'].iloc[0]
                        total_value = total_row['value'].iloc[0]
                        
                        # Calculate expected rate
                        if total_value > 0:
                            expected_rate = (count_value / total_value) * 100
                            # Allow for small floating point differences
                            assert abs(rate_value - expected_rate) < 0.1, (
                                f"Rate calculation inconsistent for {school_id}-{student_group}: "
                                f"Rate={rate_value}, Count={count_value}, Total={total_value}, "
                                f"Expected Rate={expected_rate:.2f}"
                            )
        
        print(f"Found {complete_metric_sets} complete metric sets (rate + count + total)")
        assert complete_metric_sets > 0, "No complete metric sets found in 2021 data"


if __name__ == "__main__":
    # Run specific test
    test = TestGraduationRatesEndToEnd()
    test.test_source_to_kpi_transformation()
    
    # Run data quality tests
    quality_test = TestGraduationRatesDataQuality()
    quality_test.test_kpi_format_compliance()
    quality_test.test_metric_coverage()
    quality_test.test_source_file_tracking()
    quality_test.test_student_group_consistency()
    
    print("All tests passed!")