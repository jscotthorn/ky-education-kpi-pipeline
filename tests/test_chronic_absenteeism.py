"""
Tests for Chronic Absenteeism ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
import shutil
from etl.chronic_absenteeism import (
    transform, normalize_column_names, standardize_missing_values, 
    clean_numeric_values, standardize_suppression_field, normalize_grade_field,
    convert_to_kpi_format
)


class TestChronicAbsenteeismETL:
    
    def setup_method(self):
        """Setup test directories and sample data."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        
        # Create sample raw data directory
        self.sample_dir = self.raw_dir / "chronic_absenteeism"
        self.sample_dir.mkdir(parents=True)
    
    def teardown_method(self):
        """Clean up test directories."""
        shutil.rmtree(self.test_dir)
    
    def create_sample_2024_data(self):
        """Create sample 2024 format data."""
        data = pd.DataFrame({
            'School Year': ['20232024', '20232024', '20232024', '20232024', '20232024'],
            'County Number': ['034', '034', '034', '034', '034'],
            'County Name': ['FAYETTE', 'FAYETTE', 'FAYETTE', 'FAYETTE', 'FAYETTE'],
            'District Number': ['165', '165', '165', '165', '165'],
            'District Name': ['Fayette County', 'Fayette County', 'Fayette County', 'Fayette County', 'Fayette County'],
            'School Number': ['', '', '', '', ''],
            'School Name': ['All Schools', 'All Schools', 'All Schools', 'All Schools', 'All Schools'],
            'School Code': ['999000', '999000', '999000', '999000', '999000'],
            'State School Id': ['', '', '', '', ''],
            'NCES ID': ['', '', '', '', ''],
            'CO-OP': ['909', '909', '909', '909', '909'],
            'CO-OP Code': ['', '', '', '', ''],
            'School Type': ['', '', '', '', ''],
            'Grade': ['All Grades', 'All Grades', 'Grade 1', 'Grade 12', 'Grade 9'],
            'Demographic': ['All Students', 'Female', 'Hispanic or Latino', 'White (non-Hispanic)', 'African American'],
            'Suppressed': ['No', 'No', 'No', 'No', 'Yes'],
            'Chronically Absent Students': ['186,415', '91,571', '1,250', '2,100', '*'],
            'Students Enrolled 10 or More Days': ['664,910', '321,064', '4,200', '7,500', '*'],
            'Chronic Absenteeism Rate': [28.0, 28.5, 29.8, 28.0, '*']
        })
        return data
    
    def create_sample_2023_data(self):
        """Create sample 2023 format data (different column names)."""
        data = pd.DataFrame({
            'SCHOOL YEAR': ['20222023', '20222023', '20222023'],
            'COUNTY NUMBER': ['034', '034', '034'],
            'COUNTY NAME': ['FAYETTE', 'FAYETTE', 'FAYETTE'],
            'DISTRICT NUMBER': ['165', '165', '165'],
            'DISTRICT NAME': ['Fayette County', 'Fayette County', 'Fayette County'],
            'SCHOOL NUMBER': ['', '', ''],
            'SCHOOL NAME': ['All Schools', 'All Schools', 'All Schools'],
            'SCHOOL CODE': ['999000', '999000', '999000'],
            'STATE SCHOOL ID': ['', '', ''],
            'NCES ID': ['', '', ''],
            'CO-OP': ['909', '909', '909'],
            'CO-OP CODE': ['', '', ''],
            'SCHOOL TYPE': ['', '', ''],
            'DEMOGRAPHIC': ['All Students', 'Male', 'White (non-Hispanic)'],
            'CHRONIC ABSENTEE COUNT': ['12,640', '6,328', '5,309'],
            'ENROLLMENT COUNT OF STUDENTS WITH 10+ ENROLLED DAYS': ['42,814', '22,003', '19,015'],
            'PERCENT CHRONICALLY ABSENT': [29.5, 28.76, 27.92]
        })
        return data
    
    def create_sample_suppressed_data(self):
        """Create sample data with suppressed records."""
        data = pd.DataFrame({
            'School Year': ['20232024', '20232024'],
            'County Name': ['FAYETTE', 'FAYETTE'],
            'District Name': ['Fayette County', 'Fayette County'],
            'School Name': ['Test School', 'Test School'],
            'School Code': ['123456', '123456'],
            'Grade': ['All Grades', 'Grade 9'],
            'Demographic': ['American Indian or Alaska Native', 'Native Hawaiian or Pacific Islander'],
            'Suppressed': ['Yes', 'Yes'],
            'Chronically Absent Students': ['*', '*'],
            'Students Enrolled 10 or More Days': ['*', '*'],
            'Chronic Absenteeism Rate': ['*', '*']
        })
        return data
    
    def test_normalize_column_names_2024_format(self):
        """Test column name normalization for 2024 format."""
        df = self.create_sample_2024_data()
        normalized_df = normalize_column_names(df)
        
        assert 'school_year' in normalized_df.columns
        assert 'county_name' in normalized_df.columns
        assert 'demographic' in normalized_df.columns
        assert 'grade' in normalized_df.columns
        assert 'suppressed' in normalized_df.columns
        assert 'chronically_absent_count' in normalized_df.columns
        assert 'enrollment_count' in normalized_df.columns
        assert 'chronic_absenteeism_rate' in normalized_df.columns
    
    def test_normalize_column_names_2023_format(self):
        """Test column name normalization for 2023 format (uppercase)."""
        df = self.create_sample_2023_data()
        normalized_df = normalize_column_names(df)
        
        assert 'school_year' in normalized_df.columns
        assert 'county_name' in normalized_df.columns
        assert 'demographic' in normalized_df.columns
        assert 'chronically_absent_count' in normalized_df.columns
        assert 'enrollment_count' in normalized_df.columns
        assert 'chronic_absenteeism_rate' in normalized_df.columns
    
    def test_standardize_missing_values(self):
        """Test missing value standardization."""
        df = pd.DataFrame({
            'chronically_absent_count': ['1,250', '', '*', '""'],
            'chronic_absenteeism_rate': [29.8, 'N/A', '*', '']
        })
        
        cleaned_df = standardize_missing_values(df)
        
        # Check that empty strings and markers are converted to NaN
        assert pd.isna(cleaned_df.loc[1, 'chronically_absent_count'])
        assert pd.isna(cleaned_df.loc[2, 'chronically_absent_count'])
        assert pd.isna(cleaned_df.loc[3, 'chronically_absent_count'])
        assert pd.isna(cleaned_df.loc[3, 'chronic_absenteeism_rate'])
    
    def test_clean_numeric_values(self):
        """Test numeric value cleaning and validation."""
        df = pd.DataFrame({
            'chronically_absent_count': ['1,250', '150', '-5', 'invalid'],
            'enrollment_count': ['4,200', '500', '-10', 'text'],
            'chronic_absenteeism_rate': [29.8, 150, -5, 'invalid']
        })
        
        cleaned_df = clean_numeric_values(df)
        
        # Valid values should be cleaned (commas removed)
        assert cleaned_df.loc[0, 'chronically_absent_count'] == 1250
        assert cleaned_df.loc[0, 'enrollment_count'] == 4200
        assert cleaned_df.loc[0, 'chronic_absenteeism_rate'] == 29.8
        
        # Invalid values should be NaN
        assert pd.isna(cleaned_df.loc[2, 'chronically_absent_count'])  # negative count
        assert pd.isna(cleaned_df.loc[2, 'enrollment_count'])  # negative count
        assert pd.isna(cleaned_df.loc[1, 'chronic_absenteeism_rate'])  # rate > 100
        assert pd.isna(cleaned_df.loc[2, 'chronic_absenteeism_rate'])  # negative rate
        assert pd.isna(cleaned_df.loc[3, 'chronic_absenteeism_rate'])  # invalid text
    
    def test_standardize_suppression_field(self):
        """Test suppression field standardization."""
        df = pd.DataFrame({
            'suppressed': ['Yes', 'No', True, False, 'Y', 'N', 'unknown'],
            'chronic_absenteeism_rate': [None, 28.5, None, 30.0, None, 25.0, 22.0],
            'demographic': ['A', 'B', 'C', 'D', 'E', 'F', 'G']
        })
        
        standardized_df = standardize_suppression_field(df)
        
        # Check standardization to Y/N
        assert standardized_df.loc[0, 'suppressed'] == 'Y'
        assert standardized_df.loc[1, 'suppressed'] == 'N'
        assert standardized_df.loc[2, 'suppressed'] == 'Y'
        assert standardized_df.loc[3, 'suppressed'] == 'N'
        assert standardized_df.loc[4, 'suppressed'] == 'Y'
        assert standardized_df.loc[5, 'suppressed'] == 'N'
        assert standardized_df.loc[6, 'suppressed'] == 'N'  # unknown maps to N
    
    def test_normalize_grade_field(self):
        """Test grade field normalization."""
        df = pd.DataFrame({
            'grade': ['All Grades', 'Grade 1', 'Grade 12', 'Kindergarten', 'Pre-K', 'Unknown']
        })
        
        normalized_df = normalize_grade_field(df)
        
        assert normalized_df.loc[0, 'grade'] == 'all_grades'
        assert normalized_df.loc[1, 'grade'] == 'grade_1'
        assert normalized_df.loc[2, 'grade'] == 'grade_12'
        assert normalized_df.loc[3, 'grade'] == 'kindergarten'
        assert normalized_df.loc[4, 'grade'] == 'pre_k'
        assert normalized_df.loc[5, 'grade'] == 'unknown'
    
    def test_convert_to_kpi_format_normal_data(self):
        """Test KPI format conversion for normal (non-suppressed) data."""
        df = self.create_sample_2024_data()
        df = normalize_column_names(df)
        df = standardize_missing_values(df)
        df = clean_numeric_values(df)
        df = standardize_suppression_field(df)
        df = normalize_grade_field(df)
        df['source_file'] = 'test_file.csv'
        
        # Test non-suppressed records only
        df_normal = df[df['suppressed'] == 'N'].copy()
        kpi_df = convert_to_kpi_format(df_normal)
        
        assert not kpi_df.empty
        assert len(kpi_df.columns) == 10  # Standard KPI format
        
        # Check required columns
        required_columns = ['district', 'school_id', 'school_name', 'year', 'student_group', 
                          'metric', 'value', 'suppressed', 'source_file', 'last_updated']
        for col in required_columns:
            assert col in kpi_df.columns
        
        # Check that metrics are created correctly
        metrics = kpi_df['metric'].unique()
        expected_metric_patterns = [
            'chronic_absenteeism_rate_',
            'chronic_absenteeism_count_',
            'chronic_absenteeism_enrollment_'
        ]
        
        for pattern in expected_metric_patterns:
            matching_metrics = [m for m in metrics if pattern in m]
            assert len(matching_metrics) > 0, f"Missing metric pattern: {pattern}"
        
        # Check that grades are included in metrics
        grade_suffixes = ['all_grades', 'grade_1', 'grade_12']
        for suffix in grade_suffixes:
            grade_metrics = [m for m in metrics if m.endswith(suffix)]
            assert len(grade_metrics) > 0, f"Should have metrics for {suffix}"
        
        # Check that values are appropriate types
        rate_metrics = kpi_df[kpi_df['metric'].str.contains('_rate_')]
        count_metrics = kpi_df[kpi_df['metric'].str.contains('_count_')]
        enrollment_metrics = kpi_df[kpi_df['metric'].str.contains('_enrollment_')]
        
        # Rates should be between 0-100
        if len(rate_metrics) > 0:
            rate_values = rate_metrics['value'].dropna()
            assert all(0 <= v <= 100 for v in rate_values), "Rates should be percentages 0-100"
        
        # Counts should be non-negative integers
        if len(count_metrics) > 0:
            count_values = count_metrics['value'].dropna()
            assert all(v >= 0 for v in count_values), "Counts should be non-negative"
        
        # Check suppressed field is 'N' for normal data
        assert all(kpi_df['suppressed'] == 'N')
    
    def test_convert_to_kpi_format_suppressed_data(self):
        """Test KPI format conversion for suppressed data."""
        df = self.create_sample_suppressed_data()
        df = normalize_column_names(df)
        df = standardize_missing_values(df)
        df = clean_numeric_values(df)
        df = standardize_suppression_field(df)
        df = normalize_grade_field(df)
        df['source_file'] = 'test_suppressed.csv'
        
        kpi_df = convert_to_kpi_format(df)
        
        assert not kpi_df.empty
        
        # Check that all values are NaN for suppressed records
        assert all(pd.isna(kpi_df['value']))
        
        # Check that suppressed field is 'Y' for all records
        assert all(kpi_df['suppressed'] == 'Y')
        
        # Check that metrics are still created
        metrics = kpi_df['metric'].unique()
        assert len(metrics) > 0
    
    def test_school_id_handling(self):
        """Test school ID extraction and formatting."""
        df = pd.DataFrame({
            'school_year': ['20232024'] * 3,
            'county_name': ['FAYETTE'] * 3,
            'district_name': ['Fayette County'] * 3,
            'school_name': ['Test School'] * 3,
            'state_school_id': ['123.0', '', '456'],
            'nces_id': ['', '789.0', ''],
            'school_code': ['999', '888', '777'],
            'grade': ['All Grades'] * 3,
            'demographic': ['All Students'] * 3,
            'suppressed': ['N'] * 3,
            'chronically_absent_count': [100] * 3,
            'enrollment_count': [400] * 3,
            'chronic_absenteeism_rate': [25.0] * 3,
            'source_file': ['test.csv'] * 3
        })
        
        kpi_df = convert_to_kpi_format(df)
        
        # Check school_id values (may be read as different types from CSV)
        school_ids = kpi_df['school_id'].unique()
        
        # Should prefer state_school_id, then nces_id, then school_code
        school_id_strs = [str(sid) for sid in school_ids]
        assert '123' in school_id_strs  # state_school_id without .0
        assert '888' in school_id_strs  # school_code fallback when state_school_id is empty
        assert '456' in school_id_strs  # state_school_id when present
    
    def test_year_extraction(self):
        """Test year extraction from school_year field."""
        df = pd.DataFrame({
            'school_year': ['20232024', '20222023', '2024'],
            'county_name': ['FAYETTE'] * 3,
            'district_name': ['Fayette County'] * 3,
            'school_name': ['Test School'] * 3,
            'school_code': ['123456'] * 3,
            'grade': ['All Grades'] * 3,
            'demographic': ['All Students'] * 3,
            'suppressed': ['N'] * 3,
            'chronically_absent_count': [100] * 3,
            'enrollment_count': [400] * 3,
            'chronic_absenteeism_rate': [25.0] * 3,
            'source_file': ['test.csv'] * 3
        })
        
        kpi_df = convert_to_kpi_format(df)
        
        years = kpi_df['year'].unique()
        
        # Should extract last 4 digits or use as-is for 4-digit years
        assert '2024' in years
        assert '2023' in years
    
    def test_metric_naming_with_grades(self):
        """Test that metrics include grade suffixes correctly."""
        df = pd.DataFrame({
            'school_year': ['20232024'] * 4,
            'county_name': ['FAYETTE'] * 4,
            'district_name': ['Fayette County'] * 4,
            'school_name': ['Test School'] * 4,
            'school_code': ['123456'] * 4,
            'grade': ['All Grades', 'Grade 1', 'Grade 12', 'Kindergarten'],
            'demographic': ['All Students'] * 4,
            'suppressed': ['N'] * 4,
            'chronically_absent_count': [100] * 4,
            'enrollment_count': [400] * 4,
            'chronic_absenteeism_rate': [25.0] * 4,
            'source_file': ['test.csv'] * 4
        })
        
        # Apply transformations
        df = normalize_grade_field(df)
        kpi_df = convert_to_kpi_format(df)
        
        # Check that grade suffixes are included
        metrics = kpi_df['metric'].unique()
        
        # Should have metrics for each grade level
        assert any('_all_grades' in m for m in metrics)
        assert any('_grade_1' in m for m in metrics)
        assert any('_grade_12' in m for m in metrics)
        assert any('_kindergarten' in m for m in metrics)
    
    def test_full_transform_pipeline(self):
        """Test the complete transform pipeline."""
        # Create sample data file
        df = self.create_sample_2024_data()
        sample_file = self.sample_dir / "test_chronic_absenteeism.csv"
        df.to_csv(sample_file, index=False)
        
        # Run transform
        config = {"derive": {"processing_date": "2024-07-19"}}
        transform(self.raw_dir, self.proc_dir, config)
        
        # Check output files exist
        output_file = self.proc_dir / "chronic_absenteeism.csv"
        audit_file = self.proc_dir / "chronic_absenteeism_demographic_audit.csv"
        
        assert output_file.exists()
        assert audit_file.exists()
        
        # Check output format
        output_df = pd.read_csv(output_file)
        assert not output_df.empty
        assert len(output_df.columns) == 10  # Standard KPI format
        
        # Verify required columns
        required_columns = ['district', 'school_id', 'school_name', 'year', 'student_group', 
                          'metric', 'value', 'suppressed', 'source_file', 'last_updated']
        for col in required_columns:
            assert col in output_df.columns
    
    def test_empty_directory_handling(self):
        """Test handling of empty source directory."""
        # Create empty directory
        empty_dir = self.raw_dir / "empty_source"
        empty_dir.mkdir(parents=True)
        
        # Should not raise error
        config = {"derive": {}}
        transform(self.raw_dir, self.proc_dir, config)
        
        # Output file should not exist
        output_file = self.proc_dir / "empty_source.csv"
        assert not output_file.exists()
    
    def test_multiple_files_processing(self):
        """Test processing multiple CSV files."""
        # Create multiple sample files
        df1 = self.create_sample_2024_data()
        df2 = self.create_sample_2023_data()
        
        file1 = self.sample_dir / "chronic_absenteeism_2024.csv"
        file2 = self.sample_dir / "chronic_absenteeism_2023.csv"
        
        df1.to_csv(file1, index=False)
        df2.to_csv(file2, index=False)
        
        # Run transform
        config = {"derive": {"processing_date": "2024-07-19"}}
        transform(self.raw_dir, self.proc_dir, config)
        
        # Check output
        output_file = self.proc_dir / "chronic_absenteeism.csv"
        assert output_file.exists()
        
        output_df = pd.read_csv(output_file)
        
        # Should have data from both files
        source_files = output_df['source_file'].unique()
        assert 'chronic_absenteeism_2024.csv' in source_files
        assert 'chronic_absenteeism_2023.csv' in source_files
        
        # Should have multiple years
        years = output_df['year'].astype(str).unique()
        assert '2024' in years
        assert '2023' in years
    
    def test_comma_removal_in_numbers(self):
        """Test that commas are properly removed from numeric values."""
        df = pd.DataFrame({
            'chronically_absent_count': ['1,250', '91,571', '186,415'],
            'enrollment_count': ['4,200', '321,064', '664,910'],
            'chronic_absenteeism_rate': ['29.8', '28.5', '28.0']
        })
        
        cleaned_df = clean_numeric_values(df)
        
        # Check that commas are removed and values are numeric
        assert cleaned_df.loc[0, 'chronically_absent_count'] == 1250
        assert cleaned_df.loc[1, 'chronically_absent_count'] == 91571
        assert cleaned_df.loc[2, 'chronically_absent_count'] == 186415
        
        assert cleaned_df.loc[0, 'enrollment_count'] == 4200
        assert cleaned_df.loc[1, 'enrollment_count'] == 321064
        assert cleaned_df.loc[2, 'enrollment_count'] == 664910