"""
Unit tests for Postsecondary Enrollment ETL Module

Tests data transformation, validation, demographic mapping, and edge cases
for both 2024 KYRC24 format and 2021-2023 standard format.
"""

import pytest
import pandas as pd
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, MagicMock

# Import the module under test
import sys
sys.path.append(str(Path(__file__).parent.parent))

from etl.postsecondary_enrollment import (
    normalize_column_names,
    detect_data_format,
    add_derived_fields,
    standardize_missing_values,
    clean_percentage_values,
    clean_numeric_values,
    convert_to_kpi_format,
    transform
)
from etl.demographic_mapper import DemographicMapper


class TestNormalizeColumnNames:
    """Test column name normalization functionality."""
    
    def test_kyrc24_format_normalization(self):
        """Test normalization of 2024 KYRC24 format columns."""
        df = pd.DataFrame(columns=[
            'School Year', 'District Name', 'School Name', 'Demographic',
            'Total In Group', 'Public College Enrolled In State', 'Private College Enrolled In State'
        ])
        
        result = normalize_column_names(df)
        
        expected_columns = [
            'school_year', 'district_name', 'school_name', 'demographic',
            'total_in_group', 'public_college_enrolled', 'private_college_enrolled'
        ]
        
        assert list(result.columns) == expected_columns
    
    def test_standard_format_normalization(self):
        """Test normalization of standard format columns."""
        df = pd.DataFrame(columns=[
            'SCHOOL YEAR', 'DISTRICT NAME', 'DEMOGRAPHIC',
            'TOTAL IN GROUP', 'COLLEGE ENROLLED IN STATE'
        ])
        
        result = normalize_column_names(df)
        
        expected_columns = [
            'school_year', 'district_name', 'demographic',
            'total_in_group', 'college_enrolled_total'
        ]
        
        assert list(result.columns) == expected_columns
    
    def test_bom_handling(self):
        """Test handling of BOM (Byte Order Mark) in column names."""
        df = pd.DataFrame(columns=['﻿School Year', 'District Name'])
        
        result = normalize_column_names(df)
        
        assert 'school_year' in result.columns
        assert '﻿school_year' not in result.columns


class TestDetectDataFormat:
    """Test data format detection logic."""
    
    def test_kyrc24_format_detection_by_filename(self):
        """Test KYRC24 format detection by filename."""
        df = pd.DataFrame()
        
        result = detect_data_format(df, 'KYRC24_ADLF_Transition_to_In_State_Postsecondary_Education.csv')
        
        assert result == 'kyrc24'
    
    def test_standard_format_detection_by_columns(self):
        """Test standard format detection by column presence."""
        df = pd.DataFrame(columns=['county_number', 'county_name'])
        
        result = detect_data_format(df, 'transition_in_state_postsecondary_education_2023.csv')
        
        assert result == 'standard'
    
    def test_default_to_standard_format(self):
        """Test that unknown format defaults to standard."""
        df = pd.DataFrame()
        
        result = detect_data_format(df, 'unknown_file.csv')
        
        assert result == 'standard'


class TestAddDerivedFields:
    """Test derived field addition functionality."""
    
    def test_source_file_addition(self):
        """Test that source file is added correctly."""
        df = pd.DataFrame({'test_col': [1, 2, 3]})
        source_file = 'test_file.csv'
        
        result = add_derived_fields(df, source_file)
        
        assert 'source_file' in result.columns
        assert all(result['source_file'] == source_file)
    
    def test_year_extraction_from_school_year(self):
        """Test year extraction from school_year column."""
        df = pd.DataFrame({
            'school_year': ['20232024', '20222023', '20212022'],
            'test_col': [1, 2, 3]
        })
        
        result = add_derived_fields(df, 'KYRC24_test.csv')
        
        expected_years = [2024, 2023, 2022]  # KYRC24 should use ending year
        assert list(result['year']) == expected_years
    
    def test_year_extraction_standard_format(self):
        """Test year extraction for standard format files."""
        df = pd.DataFrame({
            'school_year': ['20232024', '20222023', '20212022'],
            'test_col': [1, 2, 3]
        })
        
        result = add_derived_fields(df, 'transition_2023.csv')
        
        expected_years = [2023, 2022, 2021]  # Standard should use starting year
        assert list(result['year']) == expected_years
    
    def test_year_extraction_handles_invalid_values(self):
        """Test that invalid year values are handled gracefully."""
        df = pd.DataFrame({
            'school_year': ['invalid', '', None],
            'test_col': [1, 2, 3]
        })
        
        result = add_derived_fields(df, 'test.csv')
        
        assert 'year' in result.columns
        assert result['year'].isna().all()


class TestStandardizeMissingValues:
    """Test missing value standardization."""
    
    def test_missing_value_indicators_standardized(self):
        """Test that various missing value indicators are standardized."""
        df = pd.DataFrame({
            'col1': ['*', '**', '', 'N/A', 'valid_value'],
            'col2': ['---', '--', '<10', 'valid_value', '*'],
            'numeric_col': [1, 2, 3, 4, 5]  # Should remain unchanged
        })
        
        result = standardize_missing_values(df)
        
        # Check that missing indicators are replaced with pandas NA
        assert result['col1'].isna().sum() == 4
        assert result['col2'].isna().sum() == 4
        assert not result['col1'].iloc[4] is pd.NA  # valid_value should remain
        assert not result['col2'].iloc[3] is pd.NA  # valid_value should remain
        
        # Numeric columns should remain unchanged
        assert list(result['numeric_col']) == [1, 2, 3, 4, 5]


class TestCleanPercentageValues:
    """Test percentage value cleaning."""
    
    def test_percentage_sign_removal(self):
        """Test that percentage signs are removed and values converted to numeric."""
        df = pd.DataFrame({
            'college_enrollment_rate': ['45.5%', '32.1%', '67.8%'],
            'public_college_rate': ['23.4%', '18.9%', '41.2%'],
            'other_col': ['not_a_rate', 'value', 'test']
        })
        
        result = clean_percentage_values(df)
        
        assert result['college_enrollment_rate'].tolist() == [45.5, 32.1, 67.8]
        assert result['public_college_rate'].tolist() == [23.4, 18.9, 41.2]
        # Non-rate columns should remain as strings
        assert result['other_col'].dtype == 'object'


class TestCleanNumericValues:
    """Test numeric value cleaning."""
    
    def test_comma_removal_from_numeric_columns(self):
        """Test that commas are removed from numeric columns."""
        df = pd.DataFrame({
            'total_in_group': ['"1,234"', '2,567', '890'],
            'public_college_enrolled': ['456', '1,123', '789'],
            'other_col': ['not_numeric', 'value', 'test']
        })
        
        result = clean_numeric_values(df)
        
        assert result['total_in_group'].tolist() == [1234.0, 2567.0, 890.0]
        assert result['public_college_enrolled'].tolist() == [456.0, 1123.0, 789.0]
        # Non-numeric columns should remain unchanged
        assert result['other_col'].dtype == 'object'


class TestConvertToKpiFormat:
    """Test KPI format conversion functionality."""
    
    @patch('etl.postsecondary_enrollment.DemographicMapper')
    def test_kyrc24_format_conversion(self, mock_mapper_class):
        """Test conversion of KYRC24 format data to KPI format."""
        # Mock demographic mapper
        mock_mapper = MagicMock()
        mock_mapper.map_demographic.return_value = "All Students"
        mock_mapper_class.return_value = mock_mapper
        
        df = pd.DataFrame({
            'district_name': ['Test District'],
            'school_code': ['1234'],
            'school_name': ['Test School'],
            'year': [2024],
            'demographic': ['All Students'],
            'total_in_group': [1000],
            'public_college_enrolled': [400],
            'private_college_enrolled': [150],
            'college_enrolled_total': [550],
            'public_college_rate': [40.0],
            'private_college_rate': [15.0],
            'college_enrollment_rate': [55.0]
        })
        
        result = convert_to_kpi_format(df, 'KYRC24_test.csv', mock_mapper)
        
        # Should have 7 metrics per row
        assert len(result) == 7
        
        # Check metric names
        expected_metrics = {
            'postsecondary_enrollment_total_cohort',
            'postsecondary_enrollment_public_count',
            'postsecondary_enrollment_private_count',
            'postsecondary_enrollment_total_count',
            'postsecondary_enrollment_public_rate',
            'postsecondary_enrollment_private_rate',
            'postsecondary_enrollment_total_rate'
        }
        
        assert set(result['metric'].unique()) == expected_metrics
        
        # Check values are correctly assigned
        cohort_row = result[result['metric'] == 'postsecondary_enrollment_total_cohort']
        assert cohort_row['value'].iloc[0] == 1000.0
        
        public_rate_row = result[result['metric'] == 'postsecondary_enrollment_public_rate']
        assert public_rate_row['value'].iloc[0] == 40.0
    
    @patch('etl.postsecondary_enrollment.DemographicMapper')
    def test_suppression_handling(self, mock_mapper_class):
        """Test that suppressed values are handled correctly."""
        # Mock demographic mapper
        mock_mapper = MagicMock()
        mock_mapper.map_demographic.return_value = "All Students"
        mock_mapper_class.return_value = mock_mapper
        
        df = pd.DataFrame({
            'district_name': ['Test District'],
            'school_code': ['1234'], 
            'school_name': ['Test School'],
            'year': [2023],
            'demographic': ['All Students'],
            'total_in_group': ['*'],  # Suppressed value
            'public_college_enrolled': [100],
            'private_college_enrolled': ['*'],
            'college_enrolled_total': [100],
            'public_college_rate': [50.0],
            'private_college_rate': ['*'],
            'college_enrollment_rate': [50.0]
        })
        
        result = convert_to_kpi_format(df, 'standard_test.csv', mock_mapper)
        
        # Check suppressed values
        suppressed_metrics = result[result['suppressed'] == 'Y']['metric'].unique()
        expected_suppressed = {'postsecondary_enrollment_total_cohort', 'postsecondary_enrollment_private_count', 'postsecondary_enrollment_private_rate'}
        
        assert set(suppressed_metrics) == expected_suppressed
        
        # Check that suppressed values are NaN
        suppressed_rows = result[result['suppressed'] == 'Y']
        assert suppressed_rows['value'].isna().all()
    
    @patch('etl.postsecondary_enrollment.DemographicMapper')
    def test_required_columns_validation(self, mock_mapper_class):
        """Test that output contains all required columns."""
        # Mock demographic mapper
        mock_mapper = MagicMock()
        mock_mapper.map_demographic.return_value = "All Students"
        mock_mapper_class.return_value = mock_mapper
        
        df = pd.DataFrame({
            'district_name': ['Test District'],
            'school_code': ['1234'],
            'school_name': ['Test School'],
            'year': [2023],
            'demographic': ['All Students'],
            'total_in_group': [500],
            'public_college_enrolled': [200],
            'private_college_enrolled': [100],
            'college_enrolled_total': [300],
            'public_college_rate': [40.0],
            'private_college_rate': [20.0],
            'college_enrollment_rate': [60.0]
        })
        
        result = convert_to_kpi_format(df, 'test.csv', mock_mapper)
        
        expected_columns = [
            'district', 'school_id', 'school_name', 'year', 'student_group',
            'metric', 'value', 'suppressed', 'source_file', 'last_updated'
        ]
        
        assert list(result.columns) == expected_columns
    
    @patch('etl.postsecondary_enrollment.DemographicMapper')
    def test_invalid_rate_values_suppressed(self, mock_mapper_class):
        """Test that invalid rate values (outside 0-100%) are suppressed."""
        # Mock demographic mapper
        mock_mapper = MagicMock()
        mock_mapper.map_demographic.return_value = "All Students"
        mock_mapper_class.return_value = mock_mapper
        
        df = pd.DataFrame({
            'district_name': ['Test District'],
            'school_code': ['1234'],
            'school_name': ['Test School'],
            'year': [2023],
            'demographic': ['All Students'],
            'total_in_group': [500],
            'public_college_enrolled': [200],
            'private_college_enrolled': [100],
            'college_enrolled_total': [300],
            'public_college_rate': [150.0],  # Invalid rate > 100%
            'private_college_rate': [-10.0],  # Invalid rate < 0%
            'college_enrollment_rate': [60.0]
        })
        
        result = convert_to_kpi_format(df, 'test.csv', mock_mapper)
        
        # Check that invalid rates are suppressed
        public_rate_row = result[result['metric'] == 'postsecondary_enrollment_public_rate']
        private_rate_row = result[result['metric'] == 'postsecondary_enrollment_private_rate']
        
        assert public_rate_row['suppressed'].iloc[0] == 'Y'
        assert private_rate_row['suppressed'].iloc[0] == 'Y'
        assert pd.isna(public_rate_row['value'].iloc[0])
        assert pd.isna(private_rate_row['value'].iloc[0])


class TestTransformIntegration:
    """Test the complete transform function with integration scenarios."""
    
    def test_transform_with_empty_directory(self):
        """Test transform function with empty directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            raw_dir = os.path.join(temp_dir, 'raw')
            proc_dir = os.path.join(temp_dir, 'processed')
            os.makedirs(raw_dir)
            
            with pytest.raises(FileNotFoundError, match="No CSV files found"):
                transform(raw_dir, proc_dir)
    
    def test_transform_creates_output_directory(self):
        """Test that transform creates output directory if it doesn't exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            raw_dir = os.path.join(temp_dir, 'raw')
            proc_dir = os.path.join(temp_dir, 'processed')
            os.makedirs(raw_dir)
            
            # Create a test CSV file
            test_data = pd.DataFrame({
                'School Year': ['20232024'],
                'District Name': ['Test District'],
                'School Code': ['1234'],
                'School Name': ['Test School'],
                'Demographic': ['All Students'],
                'Total In Group': [100],
                'Public College Enrolled In State': [40],
                'Private College Enrolled In State': [20],
                'College Enrolled In State': [60],
                'Percentage Public College Enrolled In State': ['40.0%'],
                'Percentage Private College Enrolled In State': ['20.0%'],
                'Percentage College Enrolled In State': ['60.0%']
            })
            test_file = os.path.join(raw_dir, 'test.csv')
            test_data.to_csv(test_file, index=False)
            
            # Run transform
            result = transform(raw_dir, proc_dir)
            
            # Check that output directory was created
            assert os.path.exists(proc_dir)
            assert result['success'] is True
    
    def test_transform_handles_mixed_file_formats(self):
        """Test transform function with mixed file formats."""
        with tempfile.TemporaryDirectory() as temp_dir:
            raw_dir = os.path.join(temp_dir, 'raw')
            proc_dir = os.path.join(temp_dir, 'processed')
            os.makedirs(raw_dir)
            
            # Create KYRC24 format file
            kyrc24_data = pd.DataFrame({
                'School Year': ['20232024'],
                'District Name': ['Test District'],
                'School Code': ['1234'],
                'School Name': ['Test School'],
                'Demographic': ['All Students'],
                'Total In Group': [100],
                'Public College Enrolled In State': [40],
                'Private College Enrolled In State': [20],
                'College Enrolled In State': [60],
                'Percentage Public College Enrolled In State': ['40.0%'],
                'Percentage Private College Enrolled In State': ['20.0%'],
                'Percentage College Enrolled In State Table': ['60.0%']
            })
            kyrc24_file = os.path.join(raw_dir, 'KYRC24_test.csv')
            kyrc24_data.to_csv(kyrc24_file, index=False)
            
            # Create standard format file
            standard_data = pd.DataFrame({
                'SCHOOL YEAR': ['20222023'],
                'DISTRICT NAME': ['Test District'],
                'SCHOOL CODE': ['5678'],
                'SCHOOL NAME': ['Another School'],
                'DEMOGRAPHIC': ['All Students'],
                'TOTAL IN GROUP': [200],
                'PUBLIC COLLEGE ENROLLED IN STATE': [80],
                'PRIVATE COLLEGE ENROLLED IN STATE': [40],
                'COLLEGE ENROLLED IN STATE': [120],
                'PERCENTAGE PUBLIC COLLEGE ENROLLED IN STATE': ['40.0%'],
                'PERCENTAGE PRIVATE COLLEGE ENROLLED IN STATE': ['20.0%'],
                'PERCENTAGE COLLEGE ENROLLED IN STATE': ['60.0%']
            })
            standard_file = os.path.join(raw_dir, 'transition_2023.csv')
            standard_data.to_csv(standard_file, index=False)
            
            # Run transform
            result = transform(raw_dir, proc_dir)
            
            # Check results
            assert result['success'] is True
            assert result['files_processed'] == 2
            assert set(result['years_covered']) == {2022, 2024}  # KYRC24: 2024, Standard: 2022
            
            # Check that output files exist
            assert os.path.exists(result['output_path'])
            assert os.path.exists(result['audit_path'])


class TestEdgeCases:
    """Test edge cases and error conditions."""
    
    @patch('etl.postsecondary_enrollment.DemographicMapper')
    def test_negative_count_values_are_suppressed(self, mock_mapper_class):
        """Test that negative count values are treated as suppressed."""
        mock_mapper = MagicMock()
        mock_mapper.map_demographic.return_value = "All Students"
        mock_mapper_class.return_value = mock_mapper
        
        df = pd.DataFrame({
            'district_name': ['Test District'],
            'school_code': ['1234'],
            'school_name': ['Test School'],
            'year': [2023],
            'demographic': ['All Students'],
            'total_in_group': [-1],  # Negative value
            'public_college_enrolled': [50],
            'private_college_enrolled': [25],
            'college_enrolled_total': [75],
            'public_college_rate': [50.0],
            'private_college_rate': [25.0],
            'college_enrollment_rate': [75.0]
        })
        
        result = convert_to_kpi_format(df, 'test.csv', mock_mapper)
        
        cohort_row = result[result['metric'] == 'postsecondary_enrollment_total_cohort']
        assert cohort_row['suppressed'].iloc[0] == 'Y'
        assert pd.isna(cohort_row['value'].iloc[0])
    
    @patch('etl.postsecondary_enrollment.DemographicMapper')
    def test_missing_demographic_values_are_skipped(self, mock_mapper_class):
        """Test that rows with missing demographic values are skipped."""
        mock_mapper = MagicMock()
        mock_mapper_class.return_value = mock_mapper
        
        df = pd.DataFrame({
            'district_name': ['Test District', 'Test District'],
            'school_code': ['1234', '1234'],
            'school_name': ['Test School', 'Test School'],
            'year': [2023, 2023],
            'demographic': ['All Students', None],  # One missing demographic
            'total_in_group': [100, 200],
            'public_college_enrolled': [40, 80],
            'private_college_enrolled': [20, 40],
            'college_enrolled_total': [60, 120],
            'public_college_rate': [40.0, 40.0],
            'private_college_rate': [20.0, 20.0],
            'college_enrollment_rate': [60.0, 60.0]
        })
        
        result = convert_to_kpi_format(df, 'test.csv', mock_mapper)
        
        # Should only process the row with valid demographic
        assert len(result) == 7  # 7 metrics for 1 row
        assert mock_mapper.map_demographic.call_count == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])