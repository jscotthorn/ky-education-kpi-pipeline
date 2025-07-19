"""
Unit tests for Out-of-School Suspension ETL Module

Tests data transformation, validation, demographic mapping, and edge cases
for both 2024 KYRC24 format and 2021-2023 Safe Schools format.
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

from etl.out_of_school_suspension import (
    normalize_column_names,
    detect_data_format,
    add_derived_fields,
    standardize_missing_values,
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
            'Single Out-of-School With Disabilities', 'Single Out-of-School Without Disabilities'
        ])
        
        result = normalize_column_names(df)
        
        expected_columns = [
            'school_year', 'district_name', 'school_name', 'demographic',
            'single_out_of_school_with_disabilities', 'single_out_of_school_without_disabilities'
        ]
        
        assert list(result.columns) == expected_columns
    
    def test_safe_schools_format_normalization(self):
        """Test normalization of Safe Schools format columns."""
        df = pd.DataFrame(columns=[
            'SCHOOL YEAR', 'DISTRICT NAME', 'DEMOGRAPHIC',
            'OUT OF SCHOOL SUSPENSION SSP3', 'TOTAL DISCIPLINE RESOLUTIONS'
        ])
        
        result = normalize_column_names(df)
        
        expected_columns = [
            'school_year', 'district_name', 'demographic',
            'out_of_school_suspension', 'total_discipline_resolutions'
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
        
        result = detect_data_format(df, 'KYRC24_OVW_Student_Suspensions.csv')
        
        assert result == 'kyrc24'
    
    def test_kyrc24_format_detection_by_columns(self):
        """Test KYRC24 format detection by column presence."""
        df = pd.DataFrame(columns=['single_out_of_school_with_disabilities'])
        
        result = detect_data_format(df, 'student_suspensions.csv')
        
        assert result == 'kyrc24'
    
    def test_safe_schools_format_detection_by_columns(self):
        """Test Safe Schools format detection by column presence."""
        df = pd.DataFrame(columns=['out_of_school_suspension'])
        
        result = detect_data_format(df, 'discipline_data.csv')
        
        assert result == 'safe_schools'
    
    def test_safe_schools_format_detection_by_filename(self):
        """Test Safe Schools format detection by filename."""
        df = pd.DataFrame()
        
        result = detect_data_format(df, 'safe_schools_discipline_2023.csv')
        
        assert result == 'safe_schools'
    
    def test_unknown_format_raises_error(self):
        """Test that unknown format raises ValueError."""
        df = pd.DataFrame()
        
        with pytest.raises(ValueError, match="Unable to detect data format"):
            detect_data_format(df, 'unknown_file.csv')


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
            'school_year': ['20222023', '20212022', '2020-2021'],
            'test_col': [1, 2, 3]
        })
        
        result = add_derived_fields(df, 'test.csv')
        
        expected_years = [2022, 2021, 2020]  # Should extract first 4 digits, then convert to ending year for 8-digit
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
            'col2': ['---', '--', 'n/a', 'valid_value', '*'],
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


class TestConvertToKpiFormat:
    """Test KPI format conversion functionality."""
    
    @patch('etl.out_of_school_suspension.DemographicMapper')
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
            'single_out_of_school_with_disabilities': [1],
            'single_out_of_school_without_disabilities': [2],
            'multiple_out_of_school_with_disabilities': [0],
            'multiple_out_of_school_without_disabilities': [1]
        })
        
        result = convert_to_kpi_format(df, 'KYRC24_test.csv', mock_mapper)
        
        # Should have 7 metrics per row
        assert len(result) == 7
        
        # Check metric names
        expected_metrics = [
            'out_of_school_suspension_single_with_disabilities_count',
            'out_of_school_suspension_single_without_disabilities_count',
            'out_of_school_suspension_multiple_with_disabilities_count',
            'out_of_school_suspension_multiple_without_disabilities_count',
            'out_of_school_suspension_single_total_count',
            'out_of_school_suspension_multiple_total_count',
            'out_of_school_suspension_total_count'
        ]
        
        assert set(result['metric'].unique()) == set(expected_metrics)
        
        # Check totals are calculated correctly
        single_total_row = result[result['metric'] == 'out_of_school_suspension_single_total_count']
        assert single_total_row['value'].iloc[0] == 3.0  # 1 + 2
        
        multiple_total_row = result[result['metric'] == 'out_of_school_suspension_multiple_total_count']
        assert multiple_total_row['value'].iloc[0] == 1.0  # 0 + 1
        
        overall_total_row = result[result['metric'] == 'out_of_school_suspension_total_count']
        assert overall_total_row['value'].iloc[0] == 4.0  # 3 + 1
    
    @patch('etl.out_of_school_suspension.DemographicMapper')
    def test_safe_schools_format_conversion(self, mock_mapper_class):
        """Test conversion of Safe Schools format data to KPI format."""
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
            'out_of_school_suspension': [5]
        })
        
        result = convert_to_kpi_format(df, 'safe_schools_test.csv', mock_mapper)
        
        # Should have 1 metric per row
        assert len(result) == 1
        assert result['metric'].iloc[0] == 'out_of_school_suspension_count'
        assert result['value'].iloc[0] == 5.0
    
    @patch('etl.out_of_school_suspension.DemographicMapper')
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
            'out_of_school_suspension': ['*']  # Suppressed value
        })
        
        result = convert_to_kpi_format(df, 'safe_schools_test.csv', mock_mapper)
        
        assert result['suppressed'].iloc[0] == 'Y'
        assert pd.isna(result['value'].iloc[0])
    
    @patch('etl.out_of_school_suspension.DemographicMapper')
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
            'out_of_school_suspension': [5]
        })
        
        result = convert_to_kpi_format(df, 'test.csv', mock_mapper)
        
        expected_columns = [
            'district', 'school_id', 'school_name', 'year', 'student_group',
            'metric', 'value', 'suppressed', 'source_file', 'last_updated'
        ]
        
        assert list(result.columns) == expected_columns
    
    @patch('etl.out_of_school_suspension.DemographicMapper')
    def test_skips_total_events_rows(self, mock_mapper_class):
        """Test that 'Total Events' rows are skipped."""
        # Mock demographic mapper
        mock_mapper = MagicMock()
        mock_mapper_class.return_value = mock_mapper
        
        df = pd.DataFrame({
            'district_name': ['Test District', 'Test District'],
            'school_code': ['1234', '1234'],
            'school_name': ['Test School', 'Test School'],
            'year': [2023, 2023],
            'demographic': ['All Students', 'Total Events'],
            'out_of_school_suspension': [5, 10]
        })
        
        result = convert_to_kpi_format(df, 'test.csv', mock_mapper)
        
        # Should only process the 'All Students' row, not 'Total Events'
        assert len(result) == 1
        assert mock_mapper.map_demographic.call_count == 1


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
                'Out of School Suspension SSP3': [5]
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
                'Single Out-of-School With Disabilities': [1],
                'Single Out-of-School Without Disabilities': [2],
                'Multiple Out-of-School With Disabilities': [0],
                'Multiple Out-of-School Without Disabilities': [1]
            })
            kyrc24_file = os.path.join(raw_dir, 'KYRC24_test.csv')
            kyrc24_data.to_csv(kyrc24_file, index=False)
            
            # Create Safe Schools format file
            safe_schools_data = pd.DataFrame({
                'SCHOOL YEAR': ['20222023'],
                'DISTRICT NAME': ['Test District'],
                'SCHOOL CODE': ['5678'],
                'SCHOOL NAME': ['Another School'],
                'DEMOGRAPHIC': ['All Students'],
                'OUT OF SCHOOL SUSPENSION SSP3': [3]
            })
            safe_schools_file = os.path.join(raw_dir, 'safe_schools_2023.csv')
            safe_schools_data.to_csv(safe_schools_file, index=False)
            
            # Run transform
            result = transform(raw_dir, proc_dir)
            
            # Check results
            assert result['success'] is True
            assert result['files_processed'] == 2
            assert set(result['years_covered']) == {2022, 2023}
            
            # Check that output files exist
            assert os.path.exists(result['output_path'])
            assert os.path.exists(result['audit_path'])


class TestEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_negative_suspension_counts_are_suppressed(self):
        """Test that negative suspension counts are treated as suppressed."""
        with patch('etl.out_of_school_suspension.DemographicMapper') as mock_mapper_class:
            mock_mapper = MagicMock()
            mock_mapper.map_demographic.return_value = "All Students"
            mock_mapper_class.return_value = mock_mapper
            
            df = pd.DataFrame({
                'district_name': ['Test District'],
                'school_code': ['1234'],
                'school_name': ['Test School'],
                'year': [2023],
                'demographic': ['All Students'],
                'out_of_school_suspension': [-1]  # Negative value
            })
            
            result = convert_to_kpi_format(df, 'test.csv', mock_mapper)
            
            assert result['suppressed'].iloc[0] == 'Y'
            assert pd.isna(result['value'].iloc[0])
    
    def test_non_numeric_values_are_suppressed(self):
        """Test that non-numeric values are treated as suppressed."""
        with patch('etl.out_of_school_suspension.DemographicMapper') as mock_mapper_class:
            mock_mapper = MagicMock()
            mock_mapper.map_demographic.return_value = "All Students"
            mock_mapper_class.return_value = mock_mapper
            
            df = pd.DataFrame({
                'district_name': ['Test District'],
                'school_code': ['1234'],
                'school_name': ['Test School'],
                'year': [2023],
                'demographic': ['All Students'],
                'out_of_school_suspension': ['not_a_number']
            })
            
            result = convert_to_kpi_format(df, 'test.csv', mock_mapper)
            
            assert result['suppressed'].iloc[0] == 'Y'
            assert pd.isna(result['value'].iloc[0])
    
    def test_missing_demographic_values_are_skipped(self):
        """Test that rows with missing demographic values are skipped."""
        with patch('etl.out_of_school_suspension.DemographicMapper') as mock_mapper_class:
            mock_mapper = MagicMock()
            mock_mapper_class.return_value = mock_mapper
            
            df = pd.DataFrame({
                'district_name': ['Test District', 'Test District'],
                'school_code': ['1234', '1234'],
                'school_name': ['Test School', 'Test School'],
                'year': [2023, 2023],
                'demographic': ['All Students', None],  # One missing demographic
                'out_of_school_suspension': [5, 3]
            })
            
            result = convert_to_kpi_format(df, 'test.csv', mock_mapper)
            
            # Should only process the row with valid demographic
            assert len(result) == 1
            assert mock_mapper.map_demographic.call_count == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])