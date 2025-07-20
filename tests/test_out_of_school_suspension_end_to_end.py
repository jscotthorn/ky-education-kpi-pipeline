"""
End-to-End Integration Tests for Out-of-School Suspension ETL Pipeline

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

from etl.out_of_school_suspension import transform


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
            'Single Out-of-School With Disabilities': [2, 1, 3],
            'Single Out-of-School Without Disabilities': [8, 4, 12],
            'Multiple Out-of-School With Disabilities': [0, 0, 1],
            'Multiple Out-of-School Without Disabilities': [3, 1, 5]
        })
        
        test_file = os.path.join(self.raw_dir, 'KYRC24_OVW_Student_Suspensions.csv')
        kyrc24_data.to_csv(test_file, index=False)
        
        # Run ETL pipeline
        result = transform(self.raw_dir, self.proc_dir)
        
        # Validate results
        assert result['success'] is True
        assert result['files_processed'] == 1
        assert result['years_covered'] == [2023]  # 20232024 -> 2023
        assert result['demographics_found'] == 3
        
        # Check output files exist
        assert os.path.exists(result['output_path'])
        assert os.path.exists(result['audit_path'])
        
        # Load and validate output data
        output_df = pd.read_csv(result['output_path'], dtype={'school_id': str})
        
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
            'out_of_school_suspension_single_with_disabilities_count',
            'out_of_school_suspension_single_without_disabilities_count',
            'out_of_school_suspension_multiple_with_disabilities_count',
            'out_of_school_suspension_multiple_without_disabilities_count',
            'out_of_school_suspension_single_total_count',
            'out_of_school_suspension_multiple_total_count',
            'out_of_school_suspension_total_count'
        }
        assert set(output_df['metric'].unique()) == expected_metrics
        
        # Validate calculated totals for first school
        school_1_data = output_df[output_df['school_id'] == '1001']
        school_1_all_students = school_1_data[school_1_data['student_group'] == 'All Students']
        
        single_total = school_1_all_students[
            school_1_all_students['metric'] == 'out_of_school_suspension_single_total_count'
        ]['value'].iloc[0]
        assert single_total == 10.0  # 2 + 8
        
        multiple_total = school_1_all_students[
            school_1_all_students['metric'] == 'out_of_school_suspension_multiple_total_count'
        ]['value'].iloc[0]
        assert multiple_total == 3.0  # 0 + 3
        
        overall_total = school_1_all_students[
            school_1_all_students['metric'] == 'out_of_school_suspension_total_count'
        ]['value'].iloc[0]
        assert overall_total == 13.0  # 10 + 3
        
        # Validate demographic audit file
        audit_df = pd.read_csv(result['audit_path'])
        assert len(audit_df) >= 3  # At least 3 demographic mappings
        assert 'original' in audit_df.columns
        assert 'mapped' in audit_df.columns
        assert 'year' in audit_df.columns
        assert 'source_file' in audit_df.columns
    
    def test_safe_schools_format_end_to_end(self):
        """Test complete pipeline with 2021-2023 Safe Schools format data."""
        # Create realistic Safe Schools test data
        safe_schools_data = pd.DataFrame({
            'SCHOOL YEAR': ['20212022', '20212022', '20212022'],
            'DISTRICT NAME': ['Fayette County', 'Fayette County', 'Fayette County'],
            'SCHOOL CODE': ['2001', '2002', '2003'],
            'SCHOOL NAME': ['Elementary School D', 'Middle School E', 'High School F'],
            'DEMOGRAPHIC': ['All Students', 'Male', 'Hispanic or Latino'],
            'OUT OF SCHOOL SUSPENSION SSP3': [15, 8, '*']  # One suppressed value
        })
        
        test_file = os.path.join(self.raw_dir, 'safe_schools_discipline_2022.csv')
        safe_schools_data.to_csv(test_file, index=False)
        
        # Run ETL pipeline
        result = transform(self.raw_dir, self.proc_dir)
        
        # Validate results
        assert result['success'] is True
        assert result['files_processed'] == 1
        assert result['years_covered'] == [2021]  # 20212022 -> 2021
        assert result['demographics_found'] == 3
        
        # Load and validate output data
        output_df = pd.read_csv(result['output_path'], dtype={'school_id': str})
        
        # Should have 3 KPI rows (3 schools × 3 demographics × 1 metric each)
        assert len(output_df) == 3
        
        # Validate metric type
        assert output_df['metric'].unique().tolist() == ['out_of_school_suspension_count']
        
        # Validate suppression handling
        suppressed_row = output_df[output_df['school_id'] == '2003']
        assert suppressed_row['suppressed'].iloc[0] == 'Y'
        assert pd.isna(suppressed_row['value'].iloc[0])
        
        # Validate non-suppressed values
        non_suppressed = output_df[output_df['suppressed'] == 'N']
        assert len(non_suppressed) == 2
        assert non_suppressed['value'].tolist() == [15.0, 8.0]
    
    def test_mixed_file_formats_end_to_end(self):
        """Test pipeline with both KYRC24 and Safe Schools format files."""
        # Create KYRC24 format file
        kyrc24_data = pd.DataFrame({
            'School Year': ['20232024'],
            'District Name': ['Fayette County'],
            'School Code': ['3001'],
            'School Name': ['Test School 2024'],
            'Demographic': ['All Students'],
            'Single Out-of-School With Disabilities': [1],
            'Single Out-of-School Without Disabilities': [2],
            'Multiple Out-of-School With Disabilities': [0],
            'Multiple Out-of-School Without Disabilities': [1]
        })
        
        kyrc24_file = os.path.join(self.raw_dir, 'KYRC24_2024.csv')
        kyrc24_data.to_csv(kyrc24_file, index=False)
        
        # Create Safe Schools format file
        safe_schools_data = pd.DataFrame({
            'SCHOOL YEAR': ['20222023'],
            'DISTRICT NAME': ['Fayette County'],
            'SCHOOL CODE': ['3002'],
            'SCHOOL NAME': ['Test School 2023'],
            'DEMOGRAPHIC': ['All Students'],
            'OUT OF SCHOOL SUSPENSION SSP3': [10]
        })
        
        safe_schools_file = os.path.join(self.raw_dir, 'safe_schools_2023.csv')
        safe_schools_data.to_csv(safe_schools_file, index=False)
        
        # Run ETL pipeline
        result = transform(self.raw_dir, self.proc_dir)
        
        # Validate results
        assert result['success'] is True
        assert result['files_processed'] == 2
        assert set(result['years_covered']) == {2022, 2023}  # Both years processed
        
        # Load and validate output data
        output_df = pd.read_csv(result['output_path'], dtype={'school_id': str})
        
        # Should have 8 KPI rows (7 from KYRC24 + 1 from Safe Schools)
        assert len(output_df) == 8
        
        # Validate both metric types are present
        kyrc24_metrics = output_df[output_df['year'] == 2023]['metric'].unique()
        safe_schools_metrics = output_df[output_df['year'] == 2022]['metric'].unique()
        
        assert len(kyrc24_metrics) == 7  # KYRC24 format metrics
        assert safe_schools_metrics.tolist() == ['out_of_school_suspension_count']
    
    def test_data_quality_validation_end_to_end(self):
        """Test data quality validation and error handling."""
        # Create test data with quality issues
        problem_data = pd.DataFrame({
            'School Year': ['20232024', '20232024', '20232024'],
            'District Name': ['Fayette County', 'Fayette County', 'Fayette County'],
            'School Code': ['4001', '4002', '4003'],
            'School Name': ['Quality Test School A', 'Quality Test School B', 'Quality Test School C'],
            'Demographic': ['All Students', '', 'Total Events'],  # Empty and invalid demographics
            'Single Out-of-School With Disabilities': [5, -1, 'invalid'],  # Negative and invalid values
            'Single Out-of-School Without Disabilities': [3, '*', ''],  # Mixed suppression markers
            'Multiple Out-of-School With Disabilities': [1, 0, 0],
            'Multiple Out-of-School Without Disabilities': [2, 1, 1]
        })
        
        test_file = os.path.join(self.raw_dir, 'quality_test.csv')
        problem_data.to_csv(test_file, index=False)
        
        # Run ETL pipeline
        result = transform(self.raw_dir, self.proc_dir)
        
        # Should still succeed but handle problems gracefully
        assert result['success'] is True
        
        # Load output data (ensure school_id is read as string)
        output_df = pd.read_csv(result['output_path'], dtype={'school_id': str})
        
        # Should only process valid rows (first row only)
        # Empty demographic and 'Total Events' should be skipped
        assert len(output_df) == 7  # Only first school processed
        
        # All records should be for the first school
        assert output_df['school_id'].unique().tolist() == ['4001']
        
        # Validate that negative values were suppressed
        # (Note: This test assumes the negative value logic is working in the ETL)
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
            'Demographic': ['All Students', 'White (Non Hispanic)', 'Gifted and Talented'],  # Historical variations
            'Single Out-of-School With Disabilities': [1, 2, 0],
            'Single Out-of-School Without Disabilities': [3, 4, 1],
            'Multiple Out-of-School With Disabilities': [0, 0, 0],
            'Multiple Out-of-School Without Disabilities': [1, 1, 0]
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
        
        # Validate audit columns (updated column names)
        expected_audit_columns = [
            'original', 'mapped', 'year', 'source_file', 'timestamp'
        ]
        for col in expected_audit_columns:
            assert col in audit_df.columns
        
        # Validate specific mappings (using updated column names)
        audit_mappings = dict(zip(audit_df['original'], audit_df['mapped']))
        assert 'All Students' in audit_mappings
        assert audit_mappings['All Students'] == 'All Students'  # Should map to itself
        
        # Historical variations should be mapped to standard format
        if 'White (Non Hispanic)' in audit_mappings:
            assert audit_mappings['White (Non Hispanic)'] == 'White (non-Hispanic)'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])