"""
Tests for Novice Teachers ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
from etl.constants import KPI_COLUMNS
import shutil
from etl.novice_teachers import transform, NoviceTeachersETL


class TestNoviceTeachersETL:
    
    def setup_method(self):
        """Setup test directories and sample data."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        
        # Create sample raw data directory
        self.sample_dir = self.raw_dir / "novice_teachers"
        self.sample_dir.mkdir(parents=True)
        
        # Create ETL instance for testing
        self.etl = NoviceTeachersETL('novice_teachers')
    
    def teardown_method(self):
        """Clean up test directories."""
        shutil.rmtree(self.test_dir)
    
    def create_sample_institutional_data(self):
        """Create sample institutional novice teacher file."""
        data = pd.DataFrame({
            'School Year': ['20232024', '20232024', '20232024'],
            'County Number': ['001', '001', '001'],
            'County Name': ['Adair', 'Adair', 'Adair'],
            'District Number': ['001', '001', '001'],
            'District Name': ['Adair County', 'Adair County', 'Adair County'],
            'School Number': ['010', '011', '999'],
            'School Name': ['High School', 'Elementary', 'All Schools'],
            'School Code': ['001010', '001011', '001000'],
            'State School Id': ['001001010', '001001011', ''],
            'NCES ID': ['210003000001', '210003000002', ''],
            'CO-OP': ['GRREC', 'GRREC', 'GRREC'],
            'CO-OP Code': ['902', '902', '902'],
            'School Type': ['A1', 'A1', ''],
            'Teacher Count': ['45', '32', '177'],
            'Total New Teachers With 1 3 Years Experience': ['8', '6', '28'],
            'Percent Of Teachers With 1 3 Years Experience': ['17.8', '18.8', '15.8'],
            'Total New Teachers With Less Than 1 Year Experience': ['3', '2', '10'],
            'Percent Of Teachers With Less Than 1 Year Experience': ['6.7', '6.3', '5.6']
        })
        return data
    
    def create_sample_equity_data(self):
        """Create sample equity file showing students taught by inexperienced teachers."""
        data = pd.DataFrame({
            'School Year': ['20232024'] * 9,
            'County Number': ['001'] * 9,
            'County Name': ['Adair'] * 9,
            'District Number': ['001'] * 9,
            'District Name': ['Adair County'] * 9,
            'School Number': ['010'] * 9,
            'School Name': ['High School'] * 9,
            'School Code': ['001010'] * 9,
            'State School Id': ['001001010'] * 9,
            'NCES ID': ['210003000001'] * 9,
            'CO-OP': ['GRREC'] * 9,
            'CO-OP Code': ['902'] * 9,
            'School Type': ['A1'] * 9,
            'Title_I_Status': ['Title 1', 'Title 1', 'Title 1', 'Not Title 1', 'Not Title 1', 'Not Title 1', 'Equity Gap', 'Equity Gap', 'Equity Gap'],
            'All Students': ['66.0', pd.NA, pd.NA, '35.4', pd.NA, pd.NA, '30.6', pd.NA, pd.NA],
            'Non-White': [pd.NA, '70.2', pd.NA, pd.NA, '38.1', pd.NA, pd.NA, '32.1', pd.NA],
            'White': [pd.NA, pd.NA, '64.5', pd.NA, pd.NA, '34.2', pd.NA, pd.NA, '30.3'],
            'Economically Disadvantaged': [pd.NA] * 9,
            'Non-Economically Disadvantaged': [pd.NA] * 9,
            'Students with Disabilities (IEP)': [pd.NA] * 9,
            'Student without Disabilities (IEP)': [pd.NA] * 9,
            'English Learner': [pd.NA] * 9,
            'Non-English Learner': [pd.NA] * 9
        })
        return data

    def test_column_mappings(self):
        """Test that column mappings include all required fields."""
        mappings = self.etl.module_column_mappings
        
        # Institutional file columns
        assert 'Teacher Count' in mappings
        assert 'Total New Teachers With 1 3 Years Experience' in mappings
        assert 'Percent Of Teachers With 1 3 Years Experience' in mappings
        
        # Equity file columns
        assert 'Title_I_Status' in mappings
        assert 'All Students' in mappings
        assert 'Non-White' in mappings

    def test_extract_metrics_institutional(self):
        """Test metric extraction from institutional file."""
        sample_data = self.create_sample_institutional_data()
        sample_data = self.etl.normalize_column_names(sample_data)
        
        # Test first row (high school)
        row = sample_data.iloc[0]
        metrics = self.etl.extract_metrics(row)
        
        assert 'novice_teacher_rate_less_than_1_year' in metrics
        assert metrics['novice_teacher_rate_less_than_1_year'] == 6.7
        assert 'novice_teacher_rate_1_to_3_years' in metrics
        assert metrics['novice_teacher_rate_1_to_3_years'] == 17.8

    def test_extract_metrics_equity_title_1(self):
        """Test metric extraction from equity file for Title I schools."""
        sample_data = self.create_sample_equity_data()
        sample_data = self.etl.normalize_column_names(sample_data)
        
        # Test Title 1 row with all students data
        row = sample_data.iloc[0]
        metrics = self.etl.extract_metrics(row)
        
        assert 'students_taught_by_inexperienced_teachers_rate_title_1__all_students' in metrics
        assert metrics['students_taught_by_inexperienced_teachers_rate_title_1__all_students'] == 66.0
        
        # Test Title 1 row with non-white data
        row = sample_data.iloc[1]
        metrics = self.etl.extract_metrics(row)
        assert 'students_taught_by_inexperienced_teachers_rate_title_1__non_white' in metrics
        assert metrics['students_taught_by_inexperienced_teachers_rate_title_1__non_white'] == 70.2

    def test_extract_metrics_equity_not_title_1(self):
        """Test metric extraction from equity file for non-Title I schools."""
        sample_data = self.create_sample_equity_data()
        sample_data = self.etl.normalize_column_names(sample_data)
        
        row = sample_data.iloc[3]  # Not Title 1, All Students
        metrics = self.etl.extract_metrics(row)
        
        assert 'students_taught_by_inexperienced_teachers_rate_not_title_1__all_students' in metrics
        assert metrics['students_taught_by_inexperienced_teachers_rate_not_title_1__all_students'] == 35.4

    def test_extract_metrics_equity_gap(self):
        """Test metric extraction for equity gap calculations."""
        sample_data = self.create_sample_equity_data()
        sample_data = self.etl.normalize_column_names(sample_data)
        
        row = sample_data.iloc[6]  # Equity Gap, All Students
        metrics = self.etl.extract_metrics(row)
        
        assert 'students_taught_by_inexperienced_teachers_rate_equity_gap__all_students' in metrics
        assert metrics['students_taught_by_inexperienced_teachers_rate_equity_gap__all_students'] == 30.6

    def test_convert_to_kpi_format_institutional(self):
        """Test conversion of institutional file to KPI format."""
        sample_data = self.create_sample_institutional_data()
        sample_data = self.etl.normalize_column_names(sample_data)
        
        kpi_df = self.etl.convert_to_kpi_format(sample_data, "test_file.csv")
        
        # Check that DataFrame has correct columns
        assert set(kpi_df.columns).issubset(set(KPI_COLUMNS))
        
        # Each school should generate 2 metrics (less than 1 year, 1-3 years)
        assert len(kpi_df) == 6  # 3 schools × 2 metrics
        
        # Check specific metrics exist
        metrics = kpi_df['metric'].unique()
        assert 'novice_teacher_rate_less_than_1_year' in metrics
        assert 'novice_teacher_rate_1_to_3_years' in metrics

    def test_convert_to_kpi_format_equity(self):
        """Test conversion of equity file to KPI format."""
        sample_data = self.create_sample_equity_data()
        sample_data = self.etl.normalize_column_names(sample_data)
        
        kpi_df = self.etl.convert_to_kpi_format(sample_data, "equity_file.csv")
        
        # Should have metrics for each Title I status × demographic combo with data
        assert len(kpi_df) > 0
        
        # Check for Title I specific metrics (base names only)
        metrics = kpi_df['metric'].unique()
        assert 'students_taught_by_inexperienced_teachers_rate_title_1' in metrics
        assert 'students_taught_by_inexperienced_teachers_rate_not_title_1' in metrics
        assert 'students_taught_by_inexperienced_teachers_rate_equity_gap' in metrics
        
        # Check that student_group is populated correctly
        student_groups = kpi_df['student_group'].unique()
        assert 'All Students' in student_groups
        assert 'Non-White' in student_groups
        assert 'White (non-Hispanic)' in student_groups

    def test_suppressed_metric_defaults_institutional(self):
        """Test suppressed metric defaults for institutional file."""
        sample_row = pd.Series({
            'teacher_count': 10,
            'percent_new_teachers_less_than_1_year': 5.0,
            'percent_new_teachers_1_to_3_years': 15.0
        })
        defaults = self.etl.get_suppressed_metric_defaults(sample_row)
        
        assert 'novice_teacher_rate_less_than_1_year' in defaults
        assert 'novice_teacher_rate_1_to_3_years' in defaults
        assert pd.isna(defaults['novice_teacher_rate_less_than_1_year'])

    def test_suppressed_metric_defaults_equity(self):
        """Test suppressed metric defaults for equity file."""
        sample_row = pd.Series({
            'title_i_status': 'Title 1',
            'all_students': 50.0,
            'non_white': 55.0
        })
        defaults = self.etl.get_suppressed_metric_defaults(sample_row)
        
        # Should have defaults for Title 1 metrics with double underscore
        assert any('title_1' in k and '__' in k for k in defaults.keys())

    def test_should_skip_row(self):
        """Test row skipping logic."""
        # Institutional row with no teacher count
        row_no_teachers = pd.Series({
            'teacher_count': pd.NA,
            'title_i_status': '',
            'demographic': 'All Students'
        })
        assert self.etl.should_skip_row(row_no_teachers)
        
        # Equity row with unrecognized Title I status
        row_invalid_title_i = pd.Series({
            'title_i_status': 'Unknown',
            'demographic': 'All Students'
        })
        assert self.etl.should_skip_row(row_invalid_title_i)
        
        # Valid institutional row
        row_valid_inst = pd.Series({
            'teacher_count': 10,
            'title_i_status': '',
            'demographic': 'All Students'
        })
        assert not self.etl.should_skip_row(row_valid_inst)
        
        # Valid equity row
        row_valid_equity = pd.Series({
            'title_i_status': 'Title 1',
            'demographic': 'All Students'
        })
        assert not self.etl.should_skip_row(row_valid_equity)

    def test_transform_integration(self):
        """Test complete transform function integration with both file types."""
        # Create sample files
        institutional = self.create_sample_institutional_data()
        equity = self.create_sample_equity_data()
        
        # Save to test directory
        institutional.to_csv(self.sample_dir / "KYRC24_OVW_Inexperienced_Teachers.csv", index=False)
        equity.to_csv(self.sample_dir / "KYRC24_OVW_Students_Taught_by_Inexperienced_Teachers.csv", index=False)
        
        # Run transform
        config = {}
        transform(self.raw_dir, self.proc_dir, config)
        
        # Check output file was created
        output_file = self.proc_dir / "novice_teachers.csv"
        assert output_file.exists()
        
        # Load and validate output
        result_df = pd.read_csv(output_file)
        assert not result_df.empty
        assert set(result_df.columns).issubset(set(KPI_COLUMNS))
        
        # Check that we have both types of metrics
        metrics = result_df['metric'].unique()
        assert any('novice_teacher_rate' in m for m in metrics)  # Institutional
        assert any('students_taught_by_inexperienced' in m for m in metrics)  # Equity
        assert any('title_1' in m for m in metrics)  # Title I breakdowns
        
        # Check student groups
        student_groups = result_df['student_group'].unique()
        assert 'All Students' in student_groups
        assert 'Non-White' in student_groups

    def test_multiple_demographics_processed(self):
        """Test that multiple demographic columns are processed correctly."""
        equity_data = self.create_sample_equity_data()
        equity_data = self.etl.normalize_column_names(equity_data)
        
        # Count unique metric names that should be generated
        kpi_df = self.etl.convert_to_kpi_format(equity_data, "equity.csv")
        
        # Check student groups instead of metric names
        student_groups = kpi_df['student_group'].unique()
        assert 'All Students' in student_groups
        assert 'Non-White' in student_groups
        assert 'White (non-Hispanic)' in student_groups
        
        # Check metrics
        metrics = kpi_df['metric'].unique()
        assert 'students_taught_by_inexperienced_teachers_rate_title_1' in metrics
        assert 'students_taught_by_inexperienced_teachers_rate_not_title_1' in metrics
        assert 'students_taught_by_inexperienced_teachers_rate_equity_gap' in metrics
    
    def test_negative_values_excluded(self):
        """Test that negative percentage values are excluded."""
        data = pd.DataFrame({
            'School Year': ['20232024'],
            'County Number': ['001'],
            'District Number': ['001'],
            'School Number': ['010'],
            'School Code': ['001010'],
            'District Name': ['Test'],
            'School Name': ['Test'],
            'Teacher Count': ['10'],
            'Total New Teachers With 1 3 Years Experience': ['2'],
            'Percent Of Teachers With 1 3 Years Experience': ['-5.0'],  # Invalid
            'Total New Teachers With Less Than 1 Year Experience': ['1'],
            'Percent Of Teachers With Less Than 1 Year Experience': ['10.0']
        })
        
        data = self.etl.normalize_column_names(data)
        kpi_df = self.etl.convert_to_kpi_format(data, "test.csv")
        
        # Should only have 1 valid metric (less than 1 year)
        assert len(kpi_df) == 1
        assert kpi_df.iloc[0]['metric'] == 'novice_teacher_rate_less_than_1_year'
