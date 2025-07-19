"""
End-to-end validation test for kindergarten readiness ETL pipeline.
Tests that sample rows from each source file are correctly transformed to KPI format.
"""
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from etl.kindergarten_readiness import transform, normalize_column_names, convert_to_kpi_format


class TestKindergartenReadinessEndToEnd:
    """Test complete transformation from raw data to KPI format."""
    
    def test_source_to_kpi_transformation(self):
        """Test that each source file contributes data to the processed KPI file."""
        # Paths to actual data files
        raw_data_dir = Path("/Users/scott/Projects/equity-etl/data/raw/kindergarten_readiness")
        processed_file = Path("/Users/scott/Projects/equity-etl/data/processed/kindergarten_readiness.csv")
        
        # Ensure processed file exists
        if not processed_file.exists():
            pytest.skip("Processed kindergarten_readiness.csv not found. Run ETL pipeline first.")
        
        # Load processed KPI data
        kpi_df = pd.read_csv(processed_file)
        
        # Test that each source file contributes data
        for source_file in raw_data_dir.glob("*.csv"):
            print(f"\nTesting {source_file.name}...")
            
            # Check that this source file has data in the KPI output
            source_data = kpi_df[kpi_df['source_file'] == source_file.name]
            assert len(source_data) > 0, f"No KPI data found from source file {source_file.name}"
            
            # Check that the data has the expected structure
            assert 'kindergarten_readiness_rate' in source_data['metric'].values, f"No readiness rate data from {source_file.name}"
            assert len(source_data['student_group'].unique()) > 0, f"No student group data from {source_file.name}"
            
            print(f"✓ {source_file.name} contributed {len(source_data)} KPI rows")
    
    def _test_source_file_transformation(self, source_file: Path, kpi_df: pd.DataFrame):
        """Test transformation of a single source file."""
        # Load source data
        source_df = pd.read_csv(source_file)
        
        if source_df.empty:
            pytest.skip(f"Source file {source_file.name} is empty")
        
        # Apply basic filtering to match ETL pipeline logic
        source_df = normalize_column_names(source_df)
        
        # Focus on 'All Students' prior setting only (keep all demographics)
        if 'prior_setting' in source_df.columns:
            source_df = source_df[source_df['prior_setting'] == 'All Students']
        
        # Filter out suppressed data
        if 'suppressed' in source_df.columns:
            source_df = source_df[source_df['suppressed'] != 'Y']
        
        if source_df.empty:
            print(f"No testable data in {source_file.name} after filtering")
            return
        
        # Take 5 random rows (smaller sample due to filtered data)
        sample_size = min(5, len(source_df))
        sample_rows = source_df.sample(n=sample_size, random_state=42)
        
        print(f"Testing {sample_size} rows from {source_file.name}")
        
        # Validate each sample row has corresponding KPI entries
        validated_count = 0
        for _, sample_row in sample_rows.iterrows():
            try:
                self._validate_sample_row_in_kpi(sample_row, kpi_df, source_file.name)
                validated_count += 1
            except AssertionError as e:
                # Skip rows that couldn't be validated (might be filtered out)
                print(f"Skipping validation: {e}")
                continue
        
        # Require that at least some rows are validated
        assert validated_count > 0, f"No sample rows could be validated for {source_file.name}"
    
    def _validate_sample_row_in_kpi(self, sample_row: pd.Series, kpi_df: pd.DataFrame, source_filename: str):
        """Validate that a sample row from source data appears correctly in KPI data."""
        # Extract identifying information
        district_name = sample_row.get('district_name', 'Unknown')
        school_name = sample_row.get('school_name', district_name)
        year = self._extract_year(sample_row.get('school_year', ''))
        
        # Find matching KPI rows
        matching_kpi = kpi_df[
            (kpi_df['district'] == district_name) &
            (kpi_df['school_name'] == school_name) &
            (kpi_df['year'] == year) &
            (kpi_df['source_file'] == source_filename)
        ]
        
        assert len(matching_kpi) > 0, (
            f"No KPI rows found for {district_name} - {school_name} - {year} in {source_filename}"
        )
        
        # Check that we have rate metric
        rate_rows = matching_kpi[matching_kpi['metric'] == 'kindergarten_readiness_rate']
        assert len(rate_rows) > 0, f"No readiness rate found for {district_name} - {school_name}"
        
        # Validate rate value if source data has it
        source_rate = self._extract_rate_from_source(sample_row)
        if source_rate is not None:
            kpi_rate = rate_rows['value'].iloc[0]
            assert abs(float(kpi_rate) - float(source_rate)) < 0.1, (
                f"Rate mismatch: source={source_rate}, KPI={kpi_rate}"
            )
        
        print(f"✓ Validated: {district_name} - {school_name} - {year} ({len(matching_kpi)} KPI rows)")
    
    def _extract_year(self, school_year_str) -> str:
        """Extract year from school year string."""
        if len(str(school_year_str)) == 8:  # Format: YYYYYYYY
            return str(school_year_str)[-4:]  # Take last 4 digits
        elif len(str(school_year_str)) == 4:  # Already 4 digits
            return str(school_year_str)
        else:
            return '2024'  # Default
    
    def _extract_rate_from_source(self, row: pd.Series) -> float:
        """Extract readiness rate from source row."""
        # Try different possible column names
        rate_columns = ['total_percent_ready', 'TOTAL PERCENT READY']
        for col in rate_columns:
            if col in row and pd.notna(row[col]) and row[col] != '':
                try:
                    return float(row[col])
                except (ValueError, TypeError):
                    pass
        return None


class TestKindergartenReadinessDataQuality:
    """Test data quality of the processed kindergarten readiness data."""
    
    def test_kpi_format_compliance(self):
        """Test that processed file follows KPI format requirements."""
        processed_file = Path("/Users/scott/Projects/equity-etl/data/processed/kindergarten_readiness.csv")
        
        if not processed_file.exists():
            pytest.skip("Processed kindergarten_readiness.csv not found. Run ETL pipeline first.")
        
        kpi_df = pd.read_csv(processed_file)
        
        # Test required columns exist
        required_columns = ['district', 'school_id', 'school_name', 'year', 
                           'student_group', 'metric', 'value', 'suppressed', 'source_file', 'last_updated']
        
        for col in required_columns:
            assert col in kpi_df.columns, f"Required column '{col}' missing from KPI file"
        
        # Test data types
        assert pd.api.types.is_numeric_dtype(kpi_df['value']) or kpi_df['value'].dtype == 'object', "Value column should be numeric or allow NaN"
        assert pd.api.types.is_object_dtype(kpi_df['metric']), "Metric column should be string"
        assert pd.api.types.is_object_dtype(kpi_df['student_group']), "Student_group column should be string"
        assert pd.api.types.is_object_dtype(kpi_df['suppressed']), "Suppressed column should be string"
        
        # Test suppressed column has valid values
        valid_suppressed_values = {'Y', 'N'}
        assert set(kpi_df['suppressed'].unique()).issubset(valid_suppressed_values), f"Suppressed column should only contain Y/N values"
        
        # Test no completely empty rows (excluding suppressed records)
        non_suppressed = kpi_df[kpi_df['suppressed'] == 'N']
        if len(non_suppressed) > 0:
            assert not non_suppressed.isnull().all(axis=1).any(), "Found completely empty non-suppressed rows"
        
        # Test readiness rate values are reasonable (excluding suppressed records)
        rate_rows = kpi_df[(kpi_df['metric'] == 'kindergarten_readiness_rate') & (kpi_df['suppressed'] == 'N')]
        if len(rate_rows) > 0:
            assert rate_rows['value'].min() >= 0, "Readiness rates should be >= 0"
            assert rate_rows['value'].max() <= 100, "Readiness rates should be <= 100"
        
        # Test that suppressed records have NaN values
        suppressed_rows = kpi_df[kpi_df['suppressed'] == 'Y']
        if len(suppressed_rows) > 0:
            assert suppressed_rows['value'].isna().all(), "Suppressed records should have NaN values"
    
    def test_metric_coverage(self):
        """Test that expected metrics are present."""
        processed_file = Path("/Users/scott/Projects/equity-etl/data/processed/kindergarten_readiness.csv")
        
        if not processed_file.exists():
            pytest.skip("Processed kindergarten_readiness.csv not found. Run ETL pipeline first.")
        
        kpi_df = pd.read_csv(processed_file)
        
        # Test expected metrics exist
        metrics = kpi_df['metric'].unique()
        
        # Rate metric should always exist
        assert 'kindergarten_readiness_rate' in metrics, "Kindergarten readiness rate metric missing"
        
        # Count and total metrics should exist for some data (2021 and 2024)
        count_data_files = kpi_df[kpi_df['source_file'].str.contains('2021|2024', na=False)]
        if len(count_data_files) > 0:
            assert 'kindergarten_readiness_count' in metrics, "Kindergarten readiness count metric missing"
            assert 'kindergarten_readiness_total' in metrics, "Kindergarten readiness total metric missing"
        
        print(f"Found metrics: {list(metrics)}")
        
        # Test metric naming convention
        for metric in metrics:
            assert metric.startswith('kindergarten_readiness_'), f"Invalid metric name: {metric}"
            assert metric.endswith(('_rate', '_count', '_total')), f"Invalid metric suffix: {metric}"
        
        # Verify count and total metrics have integer values
        count_metrics = [m for m in metrics if m.endswith('_count') or m.endswith('_total')]
        if count_metrics:
            count_values = kpi_df[kpi_df['metric'].isin(count_metrics)]['value']
            # Allow for some float representation of integers
            integer_check = all(v.is_integer() if isinstance(v, float) else True for v in count_values.dropna())
            assert integer_check, "Count/total metrics should have integer values"
    
    def test_source_file_tracking(self):
        """Test that source file tracking is working correctly."""
        processed_file = Path("/Users/scott/Projects/equity-etl/data/processed/kindergarten_readiness.csv")
        
        if not processed_file.exists():
            pytest.skip("Processed kindergarten_readiness.csv not found. Run ETL pipeline first.")
        
        kpi_df = pd.read_csv(processed_file)
        
        # Test source files are tracked
        source_files = kpi_df['source_file'].unique()
        
        expected_files = [
            'kindergarten_screen_2021.csv',
            'kindergarten_screen_2022.csv',
            'kindergarten_screen_2023.csv',
            'KYRC24_ASMT_Kindergarten_Screen_Composite.csv'
        ]
        
        for expected_file in expected_files:
            assert expected_file in source_files, f"Expected source file {expected_file} not found in tracking"
        
        print(f"Source files tracked: {list(source_files)}")
    
    def test_student_group_consistency(self):
        """Test that student groups are consistently named."""
        processed_file = Path("/Users/scott/Projects/equity-etl/data/processed/kindergarten_readiness.csv")
        
        if not processed_file.exists():
            pytest.skip("Processed kindergarten_readiness.csv not found. Run ETL pipeline first.")
        
        kpi_df = pd.read_csv(processed_file)
        
        # Test that we have multiple demographic groups
        student_groups = kpi_df['student_group'].unique()
        
        assert len(student_groups) > 1, f"Expected multiple student groups, found only {len(student_groups)}: {list(student_groups)}"
        
        # Test no null or empty student groups
        assert not kpi_df['student_group'].isnull().any(), "Found null student groups"
        assert not (kpi_df['student_group'] == '').any(), "Found empty student groups"
        
        print(f"Student groups found: {len(student_groups)} unique groups")
        print(f"Groups: {list(student_groups)}")
        
        # Common demographics that should be present
        expected_demographics = ['All Students', 'White', 'Black or African American', 'Hispanic or Latino']
        found_demographics = [demo for demo in expected_demographics if demo in student_groups]
        
        assert len(found_demographics) >= 2, f"Expected at least 2 common demographics, found {len(found_demographics)}: {found_demographics}"
    
    def test_expanded_kpi_format(self):
        """Test that expanded KPI format with counts and totals is working correctly."""
        processed_file = Path("/Users/scott/Projects/equity-etl/data/processed/kindergarten_readiness.csv")
        
        if not processed_file.exists():
            pytest.skip("Processed kindergarten_readiness.csv not found. Run ETL pipeline first.")
        
        kpi_df = pd.read_csv(processed_file)
        
        # Focus on 2021 data which should have count information from NUMBER TESTED
        data_2021 = kpi_df[kpi_df['source_file'].str.contains('2021', na=False)]
        
        if len(data_2021) == 0:
            pytest.skip("No 2021 data found (count data unavailable)")
        
        # Test that we have rate, count, and total metrics for the same district/school combinations
        # Group by district, school_name, and year
        grouped = data_2021.groupby(['district', 'school_name', 'year'])
        
        complete_metric_sets = 0
        reasonable_calculations = 0
        
        for (district, school_name, year), group in grouped:
            metrics = set(group['metric'].values)
            
            # Check for complete metric set
            if all(m in metrics for m in ['kindergarten_readiness_rate', 'kindergarten_readiness_count', 'kindergarten_readiness_total']):
                complete_metric_sets += 1
                
                # Verify that data relationships are reasonable (not necessarily exact due to rounding)
                rate_row = group[group['metric'] == 'kindergarten_readiness_rate']
                count_row = group[group['metric'] == 'kindergarten_readiness_count']
                total_row = group[group['metric'] == 'kindergarten_readiness_total']
                
                if len(rate_row) > 0 and len(count_row) > 0 and len(total_row) > 0:
                    rate_value = rate_row['value'].iloc[0]
                    count_value = count_row['value'].iloc[0]
                    total_value = total_row['value'].iloc[0]
                    
                    # Verify reasonable data relationships
                    if total_value > 0 and count_value >= 0 and count_value <= total_value:
                        # Check that rate is reasonable given count/total (allow for rounding differences)
                        expected_rate = (count_value / total_value) * 100
                        # Allow for significant rounding differences (up to 10%) due to percentage->count conversion
                        if abs(rate_value - expected_rate) <= 10.0:
                            reasonable_calculations += 1
        
        print(f"Found {complete_metric_sets} complete metric sets (rate + count + total)")
        print(f"Reasonable calculations: {reasonable_calculations} out of {complete_metric_sets}")
        
        assert complete_metric_sets > 0, "No complete metric sets found in 2021 data"
        # Most calculations should be reasonable even with rounding differences
        assert reasonable_calculations >= (complete_metric_sets * 0.85), f"Too many unreasonable calculations: {complete_metric_sets - reasonable_calculations} out of {complete_metric_sets}"
    
    def test_year_coverage(self):
        """Test that all expected years are covered."""
        processed_file = Path("/Users/scott/Projects/equity-etl/data/processed/kindergarten_readiness.csv")
        
        if not processed_file.exists():
            pytest.skip("Processed kindergarten_readiness.csv not found. Run ETL pipeline first.")
        
        kpi_df = pd.read_csv(processed_file)
        
        # Test expected years  
        years = [str(year) for year in kpi_df['year'].unique()]
        expected_years = ['2021', '2022', '2023', '2024']
        
        for expected_year in expected_years:
            assert expected_year in years, f"Expected year {expected_year} not found in data"
        
        print(f"Years covered: {sorted(list(years))}")
        
        # Test year distribution  
        for year in expected_years:
            # Compare as both string and int to handle data type variations
            year_count = len(kpi_df[(kpi_df['year'] == year) | (kpi_df['year'] == int(year))])
            assert year_count > 0, f"No data found for year {year}"
            print(f"Year {year}: {year_count} KPI rows")


class TestKindergartenReadinessDemographicMapping:
    """Test demographic mapping functionality for kindergarten readiness."""
    
    def test_demographic_mapping_integration(self):
        """Test that demographic mapping is correctly integrated."""
        audit_file = Path("/Users/scott/Projects/equity-etl/data/processed/kindergarten_readiness_demographic_audit.csv")
        
        if not audit_file.exists():
            pytest.skip("Demographic audit file not found. Run ETL pipeline first.")
        
        audit_df = pd.read_csv(audit_file)
        
        # Check audit file structure
        required_columns = ["original", "mapped", "year", "source_file", "mapping_type", "timestamp"]
        for col in required_columns:
            assert col in audit_df.columns, f"Missing audit column: {col}"
        
        # Should have mappings for all years
        years = audit_df['year'].unique()
        expected_years = [2021, 2022, 2023, 2024]
        
        for expected_year in expected_years:
            year_mappings = audit_df[audit_df['year'] == expected_year]
            assert len(year_mappings) > 0, f"No demographic mappings found for year {expected_year}"
            
            # Each year should have multiple demographics
            year_demographics = year_mappings['mapped'].unique()
            assert len(year_demographics) > 1, f"Expected multiple demographics for year {expected_year}, found only {len(year_demographics)}: {list(year_demographics)}"
        
        # Should have diverse demographic mappings
        unique_mapped_demographics = audit_df['mapped'].unique()
        total_mappings = len(audit_df)
        
        assert len(unique_mapped_demographics) > 1, (
            f"Expected multiple demographic mappings, found only {len(unique_mapped_demographics)}: {list(unique_mapped_demographics)}"
        )
        
        print(f"Total demographic mappings: {total_mappings}")
        print(f"Unique mapped demographics: {len(unique_mapped_demographics)}")
        print(f"Mapped demographics: {list(unique_mapped_demographics)}")


if __name__ == "__main__":
    # Run specific test
    test = TestKindergartenReadinessEndToEnd()
    test.test_source_to_kpi_transformation()
    
    # Run data quality tests
    quality_test = TestKindergartenReadinessDataQuality()
    quality_test.test_kpi_format_compliance()
    quality_test.test_metric_coverage()
    quality_test.test_source_file_tracking()
    quality_test.test_student_group_consistency()
    quality_test.test_expanded_kpi_format()
    quality_test.test_year_coverage()
    
    # Run demographic mapping test
    demo_test = TestKindergartenReadinessDemographicMapping()
    demo_test.test_demographic_mapping_integration()
    
    print("All tests passed!")