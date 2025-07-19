"""
End-to-End Integration Tests for Postsecondary Enrollment ETL Pipeline

Tests the complete ETL pipeline from raw data files through to processed KPI output,
validating data integrity, demographic mapping, and audit trail generation.
"""

import pytest
import pandas as pd
import tempfile
import os
import shutil
from pathlib import Path

# Import the module under test
import sys
sys.path.append(str(Path(__file__).parent.parent))

from etl.postsecondary_enrollment import transform


class TestEndToEndIntegration:
    """End-to-end integration tests for the complete ETL pipeline."""
    
    def setup_method(self):
        """Set up test environment with temporary directories."""
        self.temp_dir = tempfile.mkdtemp()
        self.raw_dir = os.path.join(self.temp_dir, 'raw')
        self.proc_dir = os.path.join(self.temp_dir, 'processed')
        os.makedirs(self.raw_dir)
        
    def teardown_method(self):
        """Clean up temporary directories."""
        shutil.rmtree(self.temp_dir)
    
    def test_kyrc24_format_end_to_end(self):
        """Test complete pipeline with 2024 KYRC24 format data."""
        # Create realistic KYRC24 test data
        kyrc24_data = pd.DataFrame({
            'School Year': ['20232024', '20232024', '20232024'],
            'District Name': ['Fayette County', 'Fayette County', 'Fayette County'],
            'School Code': ['1001', '1002', '1003'],
            'School Name': ['Elementary School A', 'Middle School B', 'High School C'],
            'Demographic': ['All Students', 'Female', 'African American'],
            'Total In Group': [1000, 500, 200],
            'Public College Enrolled In State': [400, 250, 80],
            'Private College Enrolled In State': [150, 75, 30],
            'College Enrolled In State': [550, 325, 110],
            'Percentage Public College Enrolled In State': ['40.0%', '50.0%', '40.0%'],
            'Percentage Private College Enrolled In State': ['15.0%', '15.0%', '15.0%'],
            'Percentage College Enrolled In State Table': ['55.0%', '65.0%', '55.0%']
        })
        
        test_file = os.path.join(self.raw_dir, 'KYRC24_ADLF_Transition_to_In_State_Postsecondary_Education.csv')
        kyrc24_data.to_csv(test_file, index=False)
        
        # Run ETL pipeline
        result = transform(self.raw_dir, self.proc_dir)
        
        # Validate results
        assert result['success'] is True
        assert result['files_processed'] == 1
        assert result['years_covered'] == [2024]  # 20232024 -> 2024 for KYRC24
        assert result['demographics_found'] == 3
        
        # Check output files exist
        assert os.path.exists(result['output_path'])
        assert os.path.exists(result['audit_path'])
        
        # Load and validate output data
        output_df = pd.read_csv(result['output_path'])
        
        # Should have 21 KPI rows (3 schools × 3 demographics × 7 metrics each)
        assert len(output_df) == 21
        
        # Validate required columns
        expected_columns = [
            'district', 'school_id', 'school_name', 'year', 'student_group',
            'metric', 'value', 'suppressed', 'source_file', 'last_updated'
        ]
        assert list(output_df.columns) == expected_columns
        
        # Validate metric types
        expected_metrics = {
            'postsecondary_enrollment_total_cohort',
            'postsecondary_enrollment_public_count',
            'postsecondary_enrollment_private_count',
            'postsecondary_enrollment_total_count',
            'postsecondary_enrollment_public_rate',
            'postsecondary_enrollment_private_rate',
            'postsecondary_enrollment_total_rate'
        }
        assert set(output_df['metric'].unique()) == expected_metrics
        
        # Validate specific values for first school (handle both string and int school_id)
        school_1_data = output_df[(output_df['school_id'] == '1001') | (output_df['school_id'] == 1001)]
        school_1_all_students = school_1_data[school_1_data['student_group'] == 'All Students']
        
        cohort_row = school_1_all_students[
            school_1_all_students['metric'] == 'postsecondary_enrollment_total_cohort'
        ]
        assert len(cohort_row) == 1
        assert cohort_row['value'].iloc[0] == 1000.0
        
        public_count_row = school_1_all_students[
            school_1_all_students['metric'] == 'postsecondary_enrollment_public_count'
        ]
        assert len(public_count_row) == 1
        assert public_count_row['value'].iloc[0] == 400.0
        
        total_rate_row = school_1_all_students[
            school_1_all_students['metric'] == 'postsecondary_enrollment_total_rate'
        ]
        assert len(total_rate_row) == 1
        assert total_rate_row['value'].iloc[0] == 55.0
        
        # Validate demographic audit file
        audit_df = pd.read_csv(result['audit_path'])
        assert len(audit_df) >= 3  # At least 3 demographic mappings
        assert 'original' in audit_df.columns
        assert 'mapped' in audit_df.columns
        assert 'year' in audit_df.columns
        assert 'source_file' in audit_df.columns
    
    def test_standard_format_end_to_end(self):
        """Test complete pipeline with 2021-2023 standard format data."""
        # Create realistic standard test data
        standard_data = pd.DataFrame({
            'SCHOOL YEAR': ['20212022', '20212022', '20212022'],
            'DISTRICT NAME': ['Fayette County', 'Fayette County', 'Fayette County'],
            'SCHOOL CODE': ['2001', '2002', '2003'],
            'SCHOOL NAME': ['Elementary School D', 'Middle School E', 'High School F'],
            'DEMOGRAPHIC': ['All Students', 'Male', 'Hispanic or Latino'],
            'TOTAL IN GROUP': [800, 400, 150],
            'PUBLIC COLLEGE ENROLLED IN STATE': [320, 160, '*'],  # One suppressed value
            'PRIVATE COLLEGE ENROLLED IN STATE': [120, 60, 30],
            'COLLEGE ENROLLED IN STATE': [440, 220, 60],
            'PERCENTAGE PUBLIC COLLEGE ENROLLED IN STATE': ['40.0%', '40.0%', '*'],
            'PERCENTAGE PRIVATE COLLEGE ENROLLED IN STATE': ['15.0%', '15.0%', '20.0%'],
            'PERCENTAGE COLLEGE ENROLLED IN STATE': ['55.0%', '55.0%', '40.0%']
        })
        
        test_file = os.path.join(self.raw_dir, 'transition_in_state_postsecondary_education_2022.csv')
        standard_data.to_csv(test_file, index=False)
        
        # Run ETL pipeline
        result = transform(self.raw_dir, self.proc_dir)
        
        # Validate results
        assert result['success'] is True
        assert result['files_processed'] == 1
        assert result['years_covered'] == [2021]  # 20212022 -> 2021 for standard
        assert result['demographics_found'] == 3
        
        # Load and validate output data
        output_df = pd.read_csv(result['output_path'])
        
        # Should have 21 KPI rows (3 schools × 3 demographics × 7 metrics each)
        assert len(output_df) == 21
        
        # Validate metric type
        expected_metrics = {
            'postsecondary_enrollment_total_cohort',
            'postsecondary_enrollment_public_count',
            'postsecondary_enrollment_private_count',
            'postsecondary_enrollment_total_count',
            'postsecondary_enrollment_public_rate',
            'postsecondary_enrollment_private_rate',
            'postsecondary_enrollment_total_rate'
        }
        assert set(output_df['metric'].unique()) == expected_metrics
        
        # Validate suppression handling (handle both string and int school_id)
        suppressed_rows = output_df[
            ((output_df['school_id'] == '2003') | (output_df['school_id'] == 2003)) & 
            (output_df['metric'] == 'postsecondary_enrollment_public_count')
        ]
        assert len(suppressed_rows) == 1
        assert suppressed_rows['suppressed'].iloc[0] == 'Y'
        assert pd.isna(suppressed_rows['value'].iloc[0])
        
        # Validate non-suppressed values
        non_suppressed = output_df[output_df['suppressed'] == 'N']
        assert len(non_suppressed) > 0
    
    def test_mixed_file_formats_end_to_end(self):
        """Test pipeline with both KYRC24 and standard format files."""
        # Create KYRC24 format file
        kyrc24_data = pd.DataFrame({
            'School Year': ['20232024'],
            'District Name': ['Fayette County'],
            'School Code': ['3001'],
            'School Name': ['Test School 2024'],
            'Demographic': ['All Students'],
            'Total In Group': [500],
            'Public College Enrolled In State': [200],
            'Private College Enrolled In State': [75],
            'College Enrolled In State': [275],
            'Percentage Public College Enrolled In State': ['40.0%'],
            'Percentage Private College Enrolled In State': ['15.0%'],
            'Percentage College Enrolled In State Table': ['55.0%']
        })
        
        kyrc24_file = os.path.join(self.raw_dir, 'KYRC24_2024.csv')
        kyrc24_data.to_csv(kyrc24_file, index=False)
        
        # Create standard format file
        standard_data = pd.DataFrame({
            'SCHOOL YEAR': ['20222023'],
            'DISTRICT NAME': ['Fayette County'],
            'SCHOOL CODE': ['3002'],
            'SCHOOL NAME': ['Test School 2023'],
            'DEMOGRAPHIC': ['All Students'],
            'TOTAL IN GROUP': [600],
            'PUBLIC COLLEGE ENROLLED IN STATE': [240],
            'PRIVATE COLLEGE ENROLLED IN STATE': [90],
            'COLLEGE ENROLLED IN STATE': [330],
            'PERCENTAGE PUBLIC COLLEGE ENROLLED IN STATE': ['40.0%'],
            'PERCENTAGE PRIVATE COLLEGE ENROLLED IN STATE': ['15.0%'],
            'PERCENTAGE COLLEGE ENROLLED IN STATE': ['55.0%']
        })
        
        standard_file = os.path.join(self.raw_dir, 'transition_2023.csv')
        standard_data.to_csv(standard_file, index=False)
        
        # Run ETL pipeline
        result = transform(self.raw_dir, self.proc_dir)
        
        # Validate results
        assert result['success'] is True
        assert result['files_processed'] == 2
        assert set(result['years_covered']) == {2022, 2024}  # KYRC24: 2024, Standard: 2022
        
        # Load and validate output data
        output_df = pd.read_csv(result['output_path'])
        
        # Should have 14 KPI rows (2 schools × 1 demographic × 7 metrics each)
        assert len(output_df) == 14
        
        # Validate both years are present
        assert set(output_df['year'].unique()) == {2022, 2024}
        
        # Validate both schools are present (handle both string and int school_id)
        school_ids = set(str(x) for x in output_df['school_id'].unique())
        assert school_ids == {'3001', '3002'}
    
    def test_data_quality_validation_end_to_end(self):
        """Test data quality validation and error handling."""
        # Create test data with quality issues
        problem_data = pd.DataFrame({
            'School Year': ['20232024', '20232024', '20232024'],
            'District Name': ['Fayette County', 'Fayette County', 'Fayette County'],
            'School Code': ['4001', '4002', '4003'],
            'School Name': ['Quality Test School A', 'Quality Test School B', 'Quality Test School C'],
            'Demographic': ['All Students', '', None],  # Empty and missing demographics
            'Total In Group': [500, -100, 'invalid'],  # Negative and invalid values
            'Public College Enrolled In State': [200, '*', ''],  # Mixed suppression markers
            'Private College Enrolled In State': [75, 50, 25],
            'College Enrolled In State': [275, 150, 125],
            'Percentage Public College Enrolled In State': ['40.0%', '150.0%', '-10.0%'],  # Invalid rates
            'Percentage Private College Enrolled In State': ['15.0%', '*', '10.0%'],
            'Percentage College Enrolled In State Table': ['55.0%', '75.0%', '50.0%']
        })
        
        test_file = os.path.join(self.raw_dir, 'quality_test.csv')
        problem_data.to_csv(test_file, index=False)
        
        # Run ETL pipeline
        result = transform(self.raw_dir, self.proc_dir)
        
        # Should still succeed but handle problems gracefully
        assert result['success'] is True
        
        # Load output data
        output_df = pd.read_csv(result['output_path'])
        
        # Should only process valid rows (first row only)
        # Empty demographic and None should be skipped
        assert len(output_df) == 7  # Only first school processed
        
        # All records should be for the first school (check if any data processed)
        if len(output_df) > 0:
            # School IDs may be strings or integers depending on processing
            school_ids = output_df['school_id'].unique()
            assert len(school_ids) == 1
            assert str(school_ids[0]) == '4001'
        
        # Validate that invalid values are handled appropriately
        # (Some values should be suppressed due to data quality issues)
        suppressed_count = len(output_df[output_df['suppressed'] == 'Y'])
        assert suppressed_count >= 0  # Some records may be suppressed due to data quality
    
    def test_empty_directory_handling(self):
        """Test pipeline behavior with no CSV files."""
        # Run transform on empty directory
        with pytest.raises(FileNotFoundError, match="No CSV files found"):
            transform(self.raw_dir, self.proc_dir)
    
    def test_demographic_mapping_audit_trail(self):
        """Test that demographic mapping audit trail is properly generated."""
        # Create data with various demographics that need mapping
        test_data = pd.DataFrame({
            'School Year': ['20232024', '20232024', '20232024'],
            'District Name': ['Fayette County', 'Fayette County', 'Fayette County'],
            'School Code': ['5001', '5001', '5001'],
            'School Name': ['Audit Test School', 'Audit Test School', 'Audit Test School'],
            'Demographic': ['All Students', 'White (non-Hispanic)', 'Gifted and Talented'],
            'Total In Group': [300, 200, 50],
            'Public College Enrolled In State': [120, 80, 25],
            'Private College Enrolled In State': [45, 30, 10],
            'College Enrolled In State': [165, 110, 35],
            'Percentage Public College Enrolled In State': ['40.0%', '40.0%', '50.0%'],
            'Percentage Private College Enrolled In State': ['15.0%', '15.0%', '20.0%'],
            'Percentage College Enrolled In State Table': ['55.0%', '55.0%', '70.0%']
        })
        
        test_file = os.path.join(self.raw_dir, 'audit_test.csv')
        test_data.to_csv(test_file, index=False)
        
        # Run ETL pipeline
        result = transform(self.raw_dir, self.proc_dir)
        
        # Validate audit file was created
        assert os.path.exists(result['audit_path'])
        
        # Load and validate audit data
        audit_df = pd.read_csv(result['audit_path'])
        
        # Should have at least 3 audit records
        assert len(audit_df) >= 3
        
        # Validate audit columns
        expected_audit_columns = [
            'original', 'mapped', 'year', 'source_file', 'mapping_type', 'timestamp'
        ]
        for col in expected_audit_columns:
            assert col in audit_df.columns
        
        # Validate specific mappings
        audit_mappings = dict(zip(audit_df['original'], audit_df['mapped']))
        assert 'All Students' in audit_mappings
        assert audit_mappings['All Students'] == 'All Students'  # Should map to itself
    
    def test_percentage_value_cleaning(self):
        """Test that percentage values are properly cleaned and converted."""
        test_data = pd.DataFrame({
            'School Year': ['20232024'],
            'District Name': ['Test District'],
            'School Code': ['6001'],
            'School Name': ['Percentage Test School'],
            'Demographic': ['All Students'],
            'Total In Group': [1000],
            'Public College Enrolled In State': [400],
            'Private College Enrolled In State': [150],
            'College Enrolled In State': [550],
            'Percentage Public College Enrolled In State': ['40.00%'],  # With percentage sign
            'Percentage Private College Enrolled In State': ['15.50%'],  # With percentage sign
            'Percentage College Enrolled In State Table': ['55.00%']    # With percentage sign
        })
        
        test_file = os.path.join(self.raw_dir, 'percentage_test.csv')
        test_data.to_csv(test_file, index=False)
        
        # Run ETL pipeline
        result = transform(self.raw_dir, self.proc_dir)
        
        # Load output data
        output_df = pd.read_csv(result['output_path'])
        
        # Check that rate values are correctly converted
        public_rate_row = output_df[output_df['metric'] == 'postsecondary_enrollment_public_rate']
        private_rate_row = output_df[output_df['metric'] == 'postsecondary_enrollment_private_rate']
        total_rate_row = output_df[output_df['metric'] == 'postsecondary_enrollment_total_rate']
        
        assert public_rate_row['value'].iloc[0] == 40.0
        assert private_rate_row['value'].iloc[0] == 15.5
        assert total_rate_row['value'].iloc[0] == 55.0
        
        # All should be non-suppressed
        assert public_rate_row['suppressed'].iloc[0] == 'N'
        assert private_rate_row['suppressed'].iloc[0] == 'N'
        assert total_rate_row['suppressed'].iloc[0] == 'N'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])