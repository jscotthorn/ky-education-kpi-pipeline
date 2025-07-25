"""
End-to-end validation test for postsecondary readiness ETL pipeline.
Tests that sample rows from each source file are correctly transformed to KPI format.
"""
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from etl.postsecondary_readiness import transform, PostsecondaryReadinessETL


class TestPostsecondaryReadinessEndToEnd:
    """Test complete transformation from raw data to KPI format."""
    
    def setup_method(self):
        """Setup ETL instance for testing."""
        self.etl = PostsecondaryReadinessETL('postsecondary_readiness')
    
    def test_source_to_kpi_transformation(self):
        """Test that 10 random rows from each source file are correctly represented in processed file."""
        # Paths to actual data files
        raw_data_dir = Path("data/raw/postsecondary_readiness")
        processed_file = Path("data/processed/postsecondary_readiness.csv")
        
        # Ensure processed file exists
        if not processed_file.exists():
            pytest.skip("Processed postsecondary_readiness.csv not found. Run ETL pipeline first.")
        
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
        df = self.etl.normalize_column_names(df)
        df = self.etl.standardize_missing_values(df)
        
        # Add source file for tracking
        df['source_file'] = source_filename
        
        # Convert to KPI format
        kpi_df = self.etl.convert_to_kpi_format(df, source_filename)
        
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
            value_match = any(abs(actual_val - expected_value) < 0.01 for actual_val in actual_values if pd.notna(actual_val))
            
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


class TestPostsecondaryReadinessDataQuality:
    """Test data quality of the processed postsecondary readiness data."""
    
    def test_kpi_format_compliance(self):
        """Test that processed file follows KPI format requirements."""
        processed_file = Path("data/processed/postsecondary_readiness.csv")
        
        if not processed_file.exists():
            pytest.skip("Processed postsecondary_readiness.csv not found. Run ETL pipeline first.")
        
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
        
        # Test postsecondary readiness rate values are reasonable (excluding suppressed records)
        rate_rows = kpi_df[(kpi_df['metric'].str.contains('postsecondary_readiness_rate', na=False)) & (kpi_df['suppressed'] == 'N')]
        if len(rate_rows) > 0:
            assert rate_rows['value'].min() >= 0, "Postsecondary readiness rates should be >= 0"
            
            # Base rate should not exceed 100%, but bonus rate can exceed 100%
            base_rate_rows = rate_rows[rate_rows['metric'] == 'postsecondary_readiness_rate']
            bonus_rate_rows = rate_rows[rate_rows['metric'] == 'postsecondary_readiness_rate_with_bonus']
            
            if len(base_rate_rows) > 0:
                assert base_rate_rows['value'].max() <= 100, "Base postsecondary readiness rates should be <= 100%"
            
            if len(bonus_rate_rows) > 0:
                # Bonus rates can exceed 100% due to bonus points, but should be reasonable (< 150%)
                assert bonus_rate_rows['value'].max() < 150, "Bonus postsecondary readiness rates should be < 150%"
        
        # Test that suppressed records have NaN values
        suppressed_rows = kpi_df[kpi_df['suppressed'] == 'Y']
        if len(suppressed_rows) > 0:
            assert suppressed_rows['value'].isna().all(), "Suppressed records should have NaN values"
            
        # CRITICAL: Test suppressed record retention (no data loss)
        print(f"Suppressed records: {len(suppressed_rows)}")
        print(f"Non-suppressed records: {len(kpi_df[kpi_df['suppressed'] == 'N'])}")
        
        # Verify we have substantial suppressed data (regression prevention)
        total_records = len(kpi_df)
        suppressed_percentage = len(suppressed_rows) / total_records * 100 if total_records > 0 else 0
        assert suppressed_percentage > 40, f"Suppressed records should be >40% of data (got {suppressed_percentage:.1f}%), indicating potential data loss"
    
    def test_metric_coverage(self):
        """Test that expected metrics are present."""
        processed_file = Path("data/processed/postsecondary_readiness.csv")
        
        if not processed_file.exists():
            pytest.skip("Processed postsecondary_readiness.csv not found. Run ETL pipeline first.")
        
        kpi_df = pd.read_csv(processed_file)
        
        # Test expected metrics exist
        metrics = kpi_df['metric'].unique()
        
        # Rate metrics should always exist
        assert 'postsecondary_readiness_rate' in metrics, "Base postsecondary readiness rate metric missing"
        assert 'postsecondary_readiness_rate_with_bonus' in metrics, "Bonus postsecondary readiness rate metric missing"
        
        print(f"Found metrics: {list(metrics)}")
        
        # Test metric naming convention
        rate_metrics = [m for m in metrics if m.endswith('_rate') or m.endswith('_rate_with_bonus')]
        
        print(f"Rate metrics: {len(rate_metrics)}")
        
        # Verify all rate values are valid percentages (excluding suppressed records and NaN values)
        for metric in rate_metrics:
            metric_data = kpi_df[kpi_df['metric'] == metric]
            non_suppressed = metric_data[metric_data['suppressed'] == 'N']['value']
            # Exclude NaN values from validation
            non_suppressed_valid = non_suppressed.dropna()
            
            if len(non_suppressed_valid) > 0:
                assert all(non_suppressed_valid >= 0), f"All {metric} values should be >= 0%"
                
                # Different validation for base vs bonus rates
                if metric == 'postsecondary_readiness_rate':
                    assert all(non_suppressed_valid <= 100), f"Base {metric} values should be <= 100%"
                elif metric == 'postsecondary_readiness_rate_with_bonus':
                    # Bonus rates can exceed 100% but should be reasonable
                    assert all(non_suppressed_valid < 150), f"Bonus {metric} values should be < 150%"
                else:
                    # Default check for any other rate metrics
                    assert all(non_suppressed_valid <= 100), f"Rate {metric} values should be <= 100%"
            
            # Test suppressed records have NaN values
            suppressed_values = metric_data[metric_data['suppressed'] == 'Y']['value']
            if len(suppressed_values) > 0:
                assert all(suppressed_values.isna()), f"Suppressed {metric} values should be NaN"
    
    def test_source_file_tracking(self):
        """Test that source file tracking is working correctly."""
        processed_file = Path("data/processed/postsecondary_readiness.csv")
        
        if not processed_file.exists():
            pytest.skip("Processed postsecondary_readiness.csv not found. Run ETL pipeline first.")
        
        kpi_df = pd.read_csv(processed_file)
        
        # Test source files are tracked
        source_files = kpi_df['source_file'].unique()
        
        expected_files = [
            'KYRC24_ACCT_Postsecondary_Readiness.csv',
            'postsecondary_readiness_2022.csv',
            'postsecondary_readiness_2023.csv'
        ]
        
        for expected_file in expected_files:
            assert expected_file in source_files, f"Expected source file {expected_file} not found in tracking"
        
        print(f"Source files tracked: {list(source_files)}")
    
    def test_student_group_consistency(self):
        """Test that student groups are consistently named."""
        processed_file = Path("data/processed/postsecondary_readiness.csv")
        
        if not processed_file.exists():
            pytest.skip("Processed postsecondary_readiness.csv not found. Run ETL pipeline first.")
        
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
    
    def test_year_coverage(self):
        """Test that expected years are present."""
        processed_file = Path("data/processed/postsecondary_readiness.csv")
        
        if not processed_file.exists():
            pytest.skip("Processed postsecondary_readiness.csv not found. Run ETL pipeline first.")
        
        kpi_df = pd.read_csv(processed_file)
        
        # Test expected years exist (2022-2024)
        years = kpi_df['year'].unique()
        expected_years = [2022, 2023, 2024]
        
        for expected_year in expected_years:
            assert expected_year in years, f"Expected year {expected_year} not found"
        
        print(f"Years found: {sorted(years)}")
        
        # Test each year has both base and bonus rate metrics
        for year in expected_years:
            year_data = kpi_df[kpi_df['year'] == year]
            year_metrics = year_data['metric'].unique()
            
            assert 'postsecondary_readiness_rate' in year_metrics, f"Base rate missing for year {year}"
            assert 'postsecondary_readiness_rate_with_bonus' in year_metrics, f"Bonus rate missing for year {year}"
            
            # Validate bonus rate enhancement logic
            base_rates = year_data[year_data['metric'] == 'postsecondary_readiness_rate']
            bonus_rates = year_data[year_data['metric'] == 'postsecondary_readiness_rate_with_bonus']
            
            # Both metrics should have same number of records (same schools/demographics)
            assert len(base_rates) == len(bonus_rates), f"Year {year}: Mismatch in base vs bonus rate record counts"
            
            print(f"Year {year}: {len(base_rates)} base rates, {len(bonus_rates)} bonus rates")


class TestPostsecondaryReadinessAdvanced:
    """Advanced validation tests for edge cases and data quality."""
    
    def test_bonus_rate_enhancement_validation(self):
        """Test that bonus rates are properly enhanced and can exceed 100%."""
        processed_file = Path("data/processed/postsecondary_readiness.csv")
        
        if not processed_file.exists():
            pytest.skip("Processed postsecondary_readiness.csv not found. Run ETL pipeline first.")
        
        kpi_df = pd.read_csv(processed_file)
        
        # Find records where both base and bonus rates exist for same school/demographic/year
        base_rates = kpi_df[kpi_df['metric'] == 'postsecondary_readiness_rate'].copy()
        bonus_rates = kpi_df[kpi_df['metric'] == 'postsecondary_readiness_rate_with_bonus'].copy()
        
        if len(base_rates) == 0 or len(bonus_rates) == 0:
            pytest.skip("No base or bonus rate data found")
        
        # Merge on identifying fields to compare base vs bonus
        base_rates['merge_key'] = base_rates['school_id'].astype(str) + '_' + base_rates['student_group'] + '_' + base_rates['year'].astype(str)
        bonus_rates['merge_key'] = bonus_rates['school_id'].astype(str) + '_' + bonus_rates['student_group'] + '_' + bonus_rates['year'].astype(str)
        
        merged = pd.merge(
            base_rates[['merge_key', 'value', 'suppressed']].rename(columns={'value': 'base_rate'}),
            bonus_rates[['merge_key', 'value', 'suppressed']].rename(columns={'value': 'bonus_rate'}),
            on='merge_key',
            suffixes=('_base', '_bonus')
        )
        
        # Test non-suppressed records where both rates exist
        non_suppressed = merged[
            (merged['suppressed_base'] == 'N') & 
            (merged['suppressed_bonus'] == 'N') &
            pd.notna(merged['base_rate']) & 
            pd.notna(merged['bonus_rate'])
        ].copy()
        
        if len(non_suppressed) > 0:
            # Bonus rate should generally be >= base rate
            enhancement = non_suppressed['bonus_rate'] - non_suppressed['base_rate']
            positive_enhancement = enhancement >= 0
            positive_percentage = positive_enhancement.mean() * 100
            
            print(f"Records with bonus >= base rate: {positive_percentage:.1f}%")
            assert positive_percentage > 85, f"Expected >85% of bonus rates to be >= base rates, got {positive_percentage:.1f}%"
            
            # Test for legitimate >100% bonus rates
            over_100_bonus = non_suppressed[non_suppressed['bonus_rate'] > 100]
            if len(over_100_bonus) > 0:
                print(f"Bonus rates >100%: {len(over_100_bonus)} records (max: {over_100_bonus['bonus_rate'].max():.1f}%)")
                
                # Verify corresponding base rates are reasonable
                base_rates_for_over_100 = over_100_bonus['base_rate']
                assert base_rates_for_over_100.max() <= 100, "Base rates should never exceed 100% even when bonus rates do"
                assert base_rates_for_over_100.min() > 50, "Base rates for >100% bonus should be reasonable (>50%)"
    
    def test_school_id_consistency_validation(self):
        """Test that school IDs are consistently formatted and used."""
        processed_file = Path("data/processed/postsecondary_readiness.csv")
        
        if not processed_file.exists():
            pytest.skip("Processed postsecondary_readiness.csv not found. Run ETL pipeline first.")
        
        kpi_df = pd.read_csv(processed_file)
        
        # Test school ID format consistency
        school_ids = kpi_df['school_id'].unique()
        
        # Should be numeric-like strings without .0 suffix
        for school_id in school_ids[:10]:  # Test sample
            if pd.notna(school_id) and school_id != 'unknown':
                assert not str(school_id).endswith('.0'), f"School ID {school_id} has .0 suffix - format inconsistency"
                
        # Test school ID longitudinal consistency
        # Same school should have same ID across years
        school_name_groups = kpi_df.groupby('school_name')['school_id'].nunique()
        multiple_ids = school_name_groups[school_name_groups > 1]
        
        if len(multiple_ids) > 0:
            print(f"Schools with multiple IDs: {len(multiple_ids)}")
            # Allow some variance for district totals and aggregated records
            assert len(multiple_ids) < len(school_name_groups) * 0.1, "Too many schools have inconsistent IDs across years"
        
        print(f"School ID consistency: {len(school_ids)} unique IDs across {kpi_df['school_name'].nunique()} schools")
    
    def test_suppressed_record_retention_detailed(self):
        """Detailed test of suppressed record handling to prevent regression."""
        processed_file = Path("data/processed/postsecondary_readiness.csv")
        
        if not processed_file.exists():
            pytest.skip("Processed postsecondary_readiness.csv not found. Run ETL pipeline first.")
        
        kpi_df = pd.read_csv(processed_file)
        
        # Count suppressed vs non-suppressed by metric
        metric_suppression = kpi_df.groupby('metric')['suppressed'].value_counts().unstack(fill_value=0)
        
        for metric in metric_suppression.index:
            suppressed_count = metric_suppression.loc[metric, 'Y'] if 'Y' in metric_suppression.columns else 0
            non_suppressed_count = metric_suppression.loc[metric, 'N'] if 'N' in metric_suppression.columns else 0
            total_count = suppressed_count + non_suppressed_count
            
            print(f"{metric}: {suppressed_count} suppressed, {non_suppressed_count} non-suppressed")
            
            # Critical: Ensure we have substantial suppressed data (prevents regression)
            if total_count > 100:  # Only test metrics with substantial data
                suppressed_percentage = suppressed_count / total_count * 100
                assert suppressed_percentage > 30, f"{metric} has only {suppressed_percentage:.1f}% suppressed records - possible data loss"
        
        # Test suppressed records have correct values
        suppressed_records = kpi_df[kpi_df['suppressed'] == 'Y']
        assert len(suppressed_records) > 0, "No suppressed records found - critical data loss"
        assert suppressed_records['value'].isna().all(), "All suppressed records must have NaN values"
        assert (suppressed_records['suppressed'] == 'Y').all(), "All suppressed records must have suppressed='Y'"


if __name__ == "__main__":
    # Run specific test
    test = TestPostsecondaryReadinessEndToEnd()
    test.test_source_to_kpi_transformation()
    
    # Run data quality tests
    quality_test = TestPostsecondaryReadinessDataQuality()
    quality_test.test_kpi_format_compliance()
    quality_test.test_metric_coverage()
    quality_test.test_source_file_tracking()
    quality_test.test_student_group_consistency()
    quality_test.test_year_coverage()
    
    print("All tests passed!")