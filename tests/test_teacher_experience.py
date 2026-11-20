"""
Tests for Teacher Experience ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
from etl.constants import KPI_COLUMNS
import shutil
from etl.teacher_experience import transform, TeacherExperienceETL


class TestTeacherExperienceETL:
    
    def setup_method(self):
        """Setup test directories and sample data."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        
        # Create sample raw data directory
        self.sample_dir = self.raw_dir / "teacher_experience"
        self.sample_dir.mkdir(parents=True)
        
        # Create ETL instance for testing
        self.etl = TeacherExperienceETL('teacher_experience')
    
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
            'School Number': ['010', '010', '999'],
            'School Name': ['Adair County High School', 'Adair County Middle', 'All Schools'],
            'School Code': ['001010', '001011', '001000'],
            'State School Id': ['001001010', '001001011', ''],
            'NCES ID': ['210003000001', '210003000002', ''],
            'CO-OP': ['GRREC', 'GRREC', 'GRREC'],
            'CO-OP Code': ['902', '902', '902'],
            'School Type': ['A1', 'A1', ''],
            'Educator Count': ['45', '32', '177'],
            'Average Years of Experience': ['12.5', '8.3', '10.2']
        })
        return data
    
    def create_sample_historical_data(self):
        """Create sample historical format data (lowercase columns)."""
        data = pd.DataFrame({
            'School Year': ['20222023', '20222023'],
            'County Number': ['001', '001'],
            'County Name': ['Adair', 'Adair'],
            'District Number': ['001', '001'],
            'District Name': ['Adair County', 'Adair County'],
            'School Number': ['010', '999'],
            'School Name': ['Adair County High School', 'All Schools'],
            'School Code': ['001010', '001000'],
            'State School Id': ['001001010', ''],
            'NCES ID': ['210003000001', ''],
            'CO-OP': ['GRREC', 'GRREC'],
            'CO-OP Code': ['902', '902'],
            'School Type': ['A1', ''],
            'Educator Count': ['43', '170'],
            'Average Years of Experience': ['11.8', '9.9']
        })
        return data

    def test_column_mappings(self):
        """Test that column mappings include required fields."""
        mappings = self.etl.module_column_mappings
        
        assert 'Educator Count' in mappings
        assert mappings['Educator Count'] == 'educator_count'
        assert 'Average Years of Experience' in mappings
        assert mappings['Average Years of Experience'] == 'average_years_experience'

    def test_extract_metrics(self):
        """Test metric extraction from sample data."""
        sample_data = self.create_sample_2024_data()
        sample_data = self.etl.normalize_column_names(sample_data)
        
        # Test first row (high school)
        row = sample_data.iloc[0]
        metrics = self.etl.extract_metrics(row)
        
        assert 'teacher_average_years_experience' in metrics
        assert metrics['teacher_average_years_experience'] == 12.5
        
        # Test second row (middle school)
        row = sample_data.iloc[1]
        metrics = self.etl.extract_metrics(row)
        assert metrics['teacher_average_years_experience'] == 8.3

    def test_extract_metrics_with_missing_data(self):
        """Test that rows with missing data are handled correctly."""
        data = pd.DataFrame({
            'School Year': ['20232024'],
            'County Number': ['001'],
            'District Number': ['001'],
            'School Number': ['010'],
            'School Code': ['001010'],
            'District Name': ['Test District'],
            'School Name': ['Test School'],
            'Educator Count': ['0'],  # No educators
            'Average Years of Experience': [pd.NA]  # Missing
        })
        
        data = self.etl.normalize_column_names(data)
        row = data.iloc[0]
        
        # Should skip row if educator count is 0 or missing
        assert self.etl.should_skip_row(row)

    def test_negative_values_excluded(self):
        """Test that negative experience values are excluded."""
        data = pd.DataFrame({
            'School Year': ['20232024'],
            'County Number': ['001'],
            'District Number': ['001'],
            'School Number': ['010'],
            'School Code': ['001010'],
            'District Name': ['Test'],
            'School Name': ['Test'],
            'Educator Count': ['10'],
            'Average Years of Experience': ['-5.0']  # Invalid negative
        })
        
        data = self.etl.normalize_column_names(data)
        row = data.iloc[0]
        metrics = self.etl.extract_metrics(row)
        
        # Negative values should be excluded
        assert 'teacher_average_years_experience' not in metrics

    def test_convert_to_kpi_format(self):
        """Test conversion to KPI format."""
        sample_data = self.create_sample_2024_data()
        sample_data = self.etl.normalize_column_names(sample_data)
        
        kpi_df = self.etl.convert_to_kpi_format(sample_data, "test_file.csv")
        
        # Check that DataFrame has correct columns
        assert set(kpi_df.columns).issubset(set(KPI_COLUMNS))
        
        # Check that we have the right number of KPI rows (one metric per school)
        assert len(kpi_df) == 3  # 3 schools
        
        # Check specific metric exists
        metrics = kpi_df['metric'].unique()
        assert 'teacher_average_years_experience' in metrics
        
        # Check values are correct
        high_school_row = kpi_df[kpi_df['school_id'] == '001010'].iloc[0]
        assert high_school_row['value'] == 12.5
        assert high_school_row['year'] == '2024'

    def test_suppressed_metric_defaults(self):
        """Test suppressed metric defaults (inherited from base)."""
        sample_row = pd.Series({'educator_count': 5, 'average_years_experience': 10.0})
        defaults = self.etl.get_suppressed_metric_defaults(sample_row)
        
        # Base class returns empty dict for institutional data without demographics
        assert isinstance(defaults, dict)

    def test_should_skip_row(self):
        """Test row skipping logic."""
        # Row with no educator count should be skipped
        row_no_educators = pd.Series({
            'educator_count': pd.NA,
            'average_years_experience': 10.0,
            'demographic': 'All Students'
        })
        assert self.etl.should_skip_row(row_no_educators)
        
        # Row with zero educators should be skipped  
        row_zero_educators = pd.Series({
            'educator_count': 0,
            'average_years_experience': 10.0,
            'demographic': 'All Students'
        })
        assert self.etl.should_skip_row(row_zero_educators)
        
        # Row with no experience data should be skipped
        row_no_experience = pd.Series({
            'educator_count': 10,
            'average_years_experience': pd.NA,
            'demographic': 'All Students'
        })
        assert self.etl.should_skip_row(row_no_experience)
        
        # Valid row should not be skipped
        row_valid = pd.Series({
            'educator_count': 10,
            'average_years_experience': 12.5,
            'demographic': 'All Students'
        })
        assert not self.etl.should_skip_row(row_valid)

    def test_transform_integration(self):
        """Test complete transform function integration."""
        # Create sample files
        sample_2024 = self.create_sample_2024_data()
        sample_historical = self.create_sample_historical_data()
        
        # Save to test directory
        sample_2024.to_csv(self.sample_dir / "KYRC24_OVW_Average_Years_School_Experience.csv", index=False)
        sample_historical.to_csv(self.sample_dir / "average_years_school_experience_2023.csv", index=False)
        
        # Run transform
        config = {}
        transform(self.raw_dir, self.proc_dir, config)
        
        # Check output file was created
        output_file = self.proc_dir / "teacher_experience.csv"
        assert output_file.exists()
        
        # Load and validate output
        result_df = pd.read_csv(output_file)
        assert not result_df.empty
        assert set(result_df.columns).issubset(set(KPI_COLUMNS))
        
        # Check that we have data from both source files
        source_files = result_df['source_file'].unique()
        assert any('KYRC24' in f for f in source_files)
        assert any('2023' in f for f in source_files)
        
        # Check years (years are integers in the DataFrame)
        years = result_df['year'].unique()
        assert 2024 in years
        assert 2023 in years

    def test_data_type_validation(self):
        """Test that experience values are properly validated as numeric."""
        sample_data = pd.DataFrame({
            'School Year': ['20232024', '20232024', '20232024'],
            'County Number': ['001', '001', '001'],
            'District Number': ['001', '001', '001'],
            'School Number': ['010', '011', '012'],
            'School Code': ['001010', '001011', '001012'],
            'District Name': ['Test', 'Test', 'Test'],
            'School Name': ['School A', 'School B', 'School C'],
            'Educator Count': ['10', '15', '20'],
            'Average Years of Experience': ['12.5', 'invalid', '8.3']  # One invalid
        })
        
        sample_data = self.etl.normalize_column_names(sample_data)
        
        # Process through convert_to_kpi_format
        kpi_df = self.etl.convert_to_kpi_format(sample_data, "test.csv")
        
        # Should only have 2 valid rows (invalid excluded)
        assert len(kpi_df) == 2
        assert 12.5 in kpi_df['value'].values
        assert 8.3 in kpi_df['value'].values
