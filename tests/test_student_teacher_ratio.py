"""
Tests for Student-Teacher Ratio ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
from etl.constants import KPI_COLUMNS
import shutil
from etl.student_teacher_ratio import transform, parse_ratio, StudentTeacherRatioETL


class TestParseRatio:
    """Test the parse_ratio function."""
    
    def test_standard_ratio_format(self):
        """Test parsing standard ratio format like '15:01'."""
        assert parse_ratio("15:01") == 15.0
        assert parse_ratio("20:01") == 20.0
        assert parse_ratio("12:01") == 12.0
    
    def test_decimal_ratio_format(self):
        """Test parsing ratios with decimals like '15.5:01'."""
        assert parse_ratio("15.5:01") == 15.5
        assert parse_ratio("12.3:01") == 12.3
    
    def test_plain_number(self):
        """Test parsing plain numbers (in case format changes)."""
        assert parse_ratio("15") == 15.0
        assert parse_ratio("20.5") == 20.5
    
    def test_missing_value(self):
        """Test handling of missing values."""
        assert pd.isna(parse_ratio(pd.NA))
        assert pd.isna(parse_ratio(None))
    
    def test_invalid_format(self):
        """Test handling of invalid formats."""
        assert pd.isna(parse_ratio("invalid"))
        assert pd.isna(parse_ratio(""))
        assert pd.isna(parse_ratio("abc:def"))


class TestStudentTeacherRatioETL:
    
    def setup_method(self):
        """Setup test directories and sample data."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        
        # Create sample raw data directory
        self.sample_dir = self.raw_dir / "student_teacher_ratio"
        self.sample_dir.mkdir(parents=True)
        
        # Create ETL instance for testing
        self.etl = StudentTeacherRatioETL('student_teacher_ratio')
    
    def teardown_method(self):
        """Clean up test directories."""
        shutil.rmtree(self.test_dir)
    
    def create_sample_2024_data(self):
        """Create sample KYRC24 format data."""
        data = pd.DataFrame({
            'School Year': ['20232024', '20232024', '20232024'],
            'County Number': ['001', '001', '001'],
            'County Name': ['Adair', 'Adair', 'Adair'],
            'District Number': ['001', '001', '001'],
            'District Name': ['Adair County', 'Adair County', 'Adair County'],
            'School Number': ['010', '011', '999'],
            'School Name': ['Adair County High School', 'Adair County Elementary', 'All Schools'],
            'School Code': ['001010', '001011', '001000'],
            'State School Id': ['001001010', '001001011', ''],
            'NCES ID': ['210003000001', '210003000002', ''],
            'CO-OP': ['GRREC', 'GRREC', 'GRREC'],
            'CO-OP Code': ['902', '902', '902'],
            'School Type': ['A1', 'A1', ''],
            'Student Teacher Ratio': ['15:01', '18:01', '16:01']
        })
        return data
    
    def create_sample_2025_data(self):
        """Create sample KYRC25 format data."""
        data = pd.DataFrame({
            'School Year': ['20242025', '20242025'],
            'County Number': ['001', '001'],
            'District Number': ['001', '001'],
            'School Number': ['010', '999'],
            'School Code': ['001010', '001000'],
            'District Name': ['Adair County', 'Adair County'],
            'School Name': ['Adair County High School', 'All Schools'],
            'Student Teacher Ratio': ['14.5:01', '15.8:01']
        })
        return data

    def test_column_mappings(self):
        """Test that column mappings include required fields."""
        mappings = self.etl.module_column_mappings
        
        assert 'Student Teacher Ratio' in mappings
        assert mappings['Student Teacher Ratio'] == 'ratio'

    def test_extract_metrics(self):
        """Test metric extraction from sample data."""
        sample_data = self.create_sample_2024_data()
        sample_data = self.etl.normalize_column_names(sample_data)
        
        # Test first row (15:01)
        row = sample_data.iloc[0]
        metrics = self.etl.extract_metrics(row)
        
        assert 'student_teacher_ratio' in metrics
        assert metrics['student_teacher_ratio'] == 15.0
        
        # Test second row (18:01)
        row = sample_data.iloc[1]
        metrics = self.etl.extract_metrics(row)
        assert metrics['student_teacher_ratio'] == 18.0

    def test_extract_metrics_with_decimal(self):
        """Test extraction of ratios with decimals."""
        sample_data = self.create_sample_2025_data()
        sample_data = self.etl.normalize_column_names(sample_data)
        
        row = sample_data.iloc[0]
        metrics = self.etl.extract_metrics(row)
        assert metrics['student_teacher_ratio'] == 14.5

    def test_extract_metrics_with_missing_data(self):
        """Test that rows with missing data are handled correctly."""
        data = pd.DataFrame({
            'School Year': ['20232024'],
            'County Number': ['001'],
            'District Number': ['001'],
            'School Number': ['010'],
            'School Code': ['001010'],
            'District Name': ['Test'],
            'School Name': ['Test'],
            'Student Teacher Ratio': [pd.NA]  # Missing
        })
        
        data = self.etl.normalize_column_names(data)
        row = data.iloc[0]
        
        # Should skip row with missing ratio
        assert self.etl.should_skip_row(row)

    def test_convert_to_kpi_format(self):
        """Test conversion to KPI format."""
        sample_data = self.create_sample_2024_data()
        sample_data = self.etl.normalize_column_names(sample_data)
        
        kpi_df = self.etl.convert_to_kpi_format(sample_data, "test_file.csv")
        
        # Check that DataFrame has correct columns
        assert set(kpi_df.columns).issubset(set(KPI_COLUMNS))
        
        # Check that we have the right number of KPI rows
        assert len(kpi_df) == 3  # 3 schools
        
        # Check specific metric exists
        metrics = kpi_df['metric'].unique()
        assert 'student_teacher_ratio' in metrics
        
        # Check values are correct
        high_school_row = kpi_df[kpi_df['school_id'] == '001010'].iloc[0]
        assert high_school_row['value'] == 15.0
        assert high_school_row['year'] == '2024'

    def test_suppressed_metric_defaults(self):
        """Test suppressed metric defaults."""
        sample_row = pd.Series({'ratio': '15:01'})
        defaults = self.etl.get_suppressed_metric_defaults(sample_row)
        
        assert 'student_teacher_ratio' in defaults
        assert pd.isna(defaults['student_teacher_ratio'])

    def test_should_skip_row(self):
        """Test row skipping logic."""
        # Row with no ratio should be skipped
        row_no_ratio = pd.Series({
            'ratio': pd.NA,
            'demographic': 'All Students'
        })
        assert self.etl.should_skip_row(row_no_ratio)
        
        # Valid row should not be skipped
        row_valid = pd.Series({
            'ratio': '15:01',
            'demographic': 'All Students'
        })
        assert not self.etl.should_skip_row(row_valid)

    def test_transform_integration(self):
        """Test complete transform function integration."""
        # Create sample files
        sample_2024 = self.create_sample_2024_data()
        sample_2025 = self.create_sample_2025_data()
        
        # Save to test directory
        sample_2024.to_csv(self.sample_dir / "KYRC24_OVW_Student_to_Teacher_Ratio.csv", index=False)
        sample_2025.to_csv(self.sample_dir / "KYRC25_OVW_Student_to_Teacher_Ratio.csv", index=False)
        
        # Run transform
        config = {}
        transform(self.raw_dir, self.proc_dir, config)
        
        # Check output file was created
        output_file = self.proc_dir / "student_teacher_ratio.csv"
        assert output_file.exists()
        
        # Load and validate output
        result_df = pd.read_csv(output_file)
        assert not result_df.empty
        assert set(result_df.columns).issubset(set(KPI_COLUMNS))
        
        # Check that we have data from both source files
        source_files = result_df['source_file'].unique()
        assert any('KYRC24' in f for f in source_files)
        assert any('KYRC25' in f for f in source_files)
        
        # Check years (years are integers in the DataFrame)
        years = result_df['year'].unique()
        assert 2024 in years
        assert 2025 in years

    def test_various_ratio_formats(self):
        """Test that various ratio formats are handled correctly."""
        data = pd.DataFrame({
            'School Year': ['20232024'] * 5,
            'County Number': ['001'] * 5,
            'District Number': ['001'] * 5,
            'School Number': ['010', '011', '012', '013', '014'],
            'School Code': ['001010', '001011', '001012', '001013', '001014'],
            'District Name': ['Test'] * 5,
            'School Name': ['A', 'B', 'C', 'D', 'E'],
            'Student Teacher Ratio': ['15:01', '20.5:01', '12:01', 'invalid', pd.NA]
        })
        
        data = self.etl.normalize_column_names(data)
        kpi_df = self.etl.convert_to_kpi_format(data, "test.csv")
        
        # Should have 3 valid rows (two invalid excluded)
        assert len(kpi_df) == 3
        assert 15.0 in kpi_df['value'].values
        assert 20.5 in kpi_df['value'].values
        assert 12.0 in kpi_df['value'].values
