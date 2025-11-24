"""
Tests for Teacher Turnover ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
from etl.constants import KPI_COLUMNS
import shutil
from etl.teacher_turnover import transform, TeacherTurnoverETL


class TestTeacherTurnoverETL:

    def setup_method(self):
        """Setup test directories and sample data."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)

        # Create sample raw data directory
        self.sample_dir = self.raw_dir / "teacher_turnover"
        self.sample_dir.mkdir(parents=True)

        # Create ETL instance for testing
        self.etl = TeacherTurnoverETL('teacher_turnover')

    def teardown_method(self):
        """Clean up test directories."""
        shutil.rmtree(self.test_dir)

    def create_sample_2024_data(self):
        """Create sample KYRC24/25 format data."""
        data = pd.DataFrame({
            'School Year': ['20232024', '20232024', '20232024'],
            'County Number': ['001', '001', '001'],
            'County Name': ['Adair', 'Adair', 'Adair'],
            'District Number': ['001', '001', '001'],
            'District Name': ['Adair County', 'Adair County', 'Adair County'],
            'School Number': ['010', '011', ''],
            'School Name': ['Adair County High School', 'Adair County Middle', 'All Schools'],
            'School Code': ['001010', '001011', '001000'],
            'State School Id': ['001001010', '001001011', ''],
            'NCES ID': ['210003000001', '210003000002', ''],
            'CO-OP': ['GRREC', 'GRREC', 'GRREC'],
            'CO-OP Code': ['902', '902', '902'],
            'School Type': ['A1', 'A1', ''],
            'Teacher Count': ['50', '35', '185'],
            'Teacher Turnover Count': ['8', '5', '25'],
            'Turnover Percent': ['16.0', '14.3', '13.5']
        })
        return data

    def create_sample_historical_data(self):
        """Create sample historical format data (uppercase columns)."""
        data = pd.DataFrame({
            'SCHOOL YEAR': ['20202021', '20202021'],
            'COUNTY NUMBER': ['001', '001'],
            'COUNTY NAME': ['ADAIR', 'ADAIR'],
            'DISTRICT NUMBER': ['001', '001'],
            'DISTRICT NAME': ['Adair County', 'Adair County'],
            'SCHOOL NUMBER': ['010', ''],
            'SCHOOL NAME': ['Adair County High School', '---District Total---'],
            'SCHOOL CODE': ['001010', '001'],
            'STATE SCHOOL ID': ['001001010', ''],
            'NCES ID': ['210003000001', ''],
            'CO-OP': ['GRREC', 'GRREC'],
            'CO-OP CODE': ['902', '902'],
            'SCHOOL TYPE': ['A1', ''],
            'TEACHER COUNT': ['48', '183'],
            'TEACHER TURNOVER COUNT': ['3', '23'],
            'TURNOVER PERCENT': ['6.3', '12.6']
        })
        return data

    def test_column_mappings(self):
        """Test that column mappings include required fields."""
        mappings = self.etl.module_column_mappings

        assert 'Teacher Count' in mappings
        assert mappings['Teacher Count'] == 'teacher_count'
        assert 'Teacher Turnover Count' in mappings
        assert mappings['Teacher Turnover Count'] == 'teacher_turnover_count'
        assert 'Turnover Percent' in mappings
        assert mappings['Turnover Percent'] == 'turnover_percent'

        # Test uppercase variants
        assert 'TEACHER COUNT' in mappings
        assert 'TEACHER TURNOVER COUNT' in mappings
        assert 'TURNOVER PERCENT' in mappings

    def test_extract_metrics(self):
        """Test metric extraction from sample data."""
        sample_data = self.create_sample_2024_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        # Test first row (high school)
        row = sample_data.iloc[0]
        metrics = self.etl.extract_metrics(row)

        assert 'teacher_turnover_rate' in metrics
        assert metrics['teacher_turnover_rate'] == 16.0
        assert 'teacher_turnover_count' in metrics
        assert metrics['teacher_turnover_count'] == 8.0
        assert 'teacher_count' in metrics
        assert metrics['teacher_count'] == 50.0

    def test_extract_metrics_with_commas(self):
        """Test that comma-separated values are handled correctly."""
        data = pd.DataFrame({
            'School Year': ['20232024'],
            'County Number': ['001'],
            'District Number': ['001'],
            'School Code': ['001000'],
            'District Name': ['Test District'],
            'School Name': ['All Schools'],
            'Teacher Count': ['10,612'],  # Comma-separated
            'Teacher Turnover Count': ['2,154'],
            'Turnover Percent': ['20.3']
        })

        data = self.etl.normalize_column_names(data)
        row = data.iloc[0]
        metrics = self.etl.extract_metrics(row)

        assert metrics['teacher_count'] == 10612.0
        assert metrics['teacher_turnover_count'] == 2154.0

    def test_extract_metrics_with_missing_data(self):
        """Test that rows with missing data return empty metrics."""
        data = pd.DataFrame({
            'School Year': ['20232024'],
            'County Number': ['001'],
            'District Number': ['001'],
            'School Code': ['001010'],
            'District Name': ['Test District'],
            'School Name': ['Test School'],
            'Teacher Count': [pd.NA],
            'Teacher Turnover Count': [pd.NA],
            'Turnover Percent': [pd.NA]
        })

        data = self.etl.normalize_column_names(data)
        row = data.iloc[0]
        metrics = self.etl.extract_metrics(row)

        # Should return empty dict when all values are missing
        assert len(metrics) == 0

    def test_negative_values_excluded(self):
        """Test that negative values are excluded."""
        data = pd.DataFrame({
            'School Year': ['20232024'],
            'County Number': ['001'],
            'District Number': ['001'],
            'School Code': ['001010'],
            'District Name': ['Test'],
            'School Name': ['Test'],
            'Teacher Count': ['50'],
            'Teacher Turnover Count': ['-5'],  # Invalid negative
            'Turnover Percent': ['10.0']
        })

        data = self.etl.normalize_column_names(data)
        row = data.iloc[0]
        metrics = self.etl.extract_metrics(row)

        # Negative values should be excluded
        assert 'teacher_turnover_count' not in metrics
        # Other metrics should still be present
        assert 'teacher_count' in metrics
        assert 'teacher_turnover_rate' in metrics

    def test_should_skip_row(self):
        """Test row skipping logic."""
        # Row with no school_code should be skipped
        row_no_code = pd.Series({
            'school_code': pd.NA,
            'teacher_count': 50,
            'turnover_percent': 15.0
        })
        assert self.etl.should_skip_row(row_no_code)

        # Row with empty school_code should be skipped
        row_empty_code = pd.Series({
            'school_code': '',
            'teacher_count': 50,
            'turnover_percent': 15.0
        })
        assert self.etl.should_skip_row(row_empty_code)

        # Valid row should not be skipped
        row_valid = pd.Series({
            'school_code': '001010',
            'teacher_count': 50,
            'turnover_percent': 15.0
        })
        assert not self.etl.should_skip_row(row_valid)

    def test_student_group_always_all_students(self):
        """Test that student_group is always 'All Students' for institutional data."""
        sample_data = self.create_sample_2024_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        kpi_df = self.etl.convert_to_kpi_format(sample_data, "test_file.csv")

        # All rows should have 'All Students' as student_group
        assert all(kpi_df['student_group'] == 'All Students')

    def test_convert_to_kpi_format(self):
        """Test conversion to KPI format."""
        sample_data = self.create_sample_2024_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        kpi_df = self.etl.convert_to_kpi_format(sample_data, "test_file.csv")

        # Check that DataFrame has correct columns
        assert set(kpi_df.columns).issubset(set(KPI_COLUMNS))

        # Check that we have the right number of KPI rows (3 metrics per school)
        assert len(kpi_df) == 9  # 3 schools × 3 metrics

        # Check specific metrics exist
        metrics = kpi_df['metric'].unique()
        assert 'teacher_turnover_rate' in metrics
        assert 'teacher_turnover_count' in metrics
        assert 'teacher_count' in metrics

        # Check values are correct
        high_school_rows = kpi_df[kpi_df['school_id'] == '001010']
        rate_row = high_school_rows[high_school_rows['metric'] == 'teacher_turnover_rate'].iloc[0]
        assert rate_row['value'] == 16.0
        assert rate_row['year'] == '2024'

    def test_suppressed_metric_defaults(self):
        """Test suppressed metric defaults."""
        sample_row = pd.Series({
            'turnover_percent': 15.0,
            'teacher_turnover_count': 10,
            'teacher_count': 50
        })
        defaults = self.etl.get_suppressed_metric_defaults(sample_row)

        assert 'teacher_turnover_rate' in defaults
        assert 'teacher_turnover_count' in defaults
        assert 'teacher_count' in defaults
        assert pd.isna(defaults['teacher_turnover_rate'])

    def test_transform_integration(self):
        """Test complete transform function integration."""
        # Create sample files
        sample_2024 = self.create_sample_2024_data()
        sample_historical = self.create_sample_historical_data()

        # Save to test directory
        sample_2024.to_csv(self.sample_dir / "KYRC24_OVW_Teacher_Turnover.csv", index=False)
        sample_historical.to_csv(self.sample_dir / "teacher_turnover_2021.csv", index=False)

        # Run transform
        config = {}
        transform(self.raw_dir, self.proc_dir, config)

        # Check output file was created
        output_file = self.proc_dir / "teacher_turnover.csv"
        assert output_file.exists()

        # Load and validate output
        result_df = pd.read_csv(output_file)
        assert not result_df.empty
        assert set(result_df.columns).issubset(set(KPI_COLUMNS))

        # Check that we have data from both source files
        source_files = result_df['source_file'].unique()
        assert any('KYRC24' in f for f in source_files)
        assert any('2021' in f for f in source_files)

        # Check years
        years = result_df['year'].unique()
        assert 2024 in years or '2024' in years
        assert 2021 in years or '2021' in years

    def test_historical_uppercase_columns(self):
        """Test that historical uppercase columns are properly mapped."""
        sample_data = self.create_sample_historical_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        # Check columns were normalized
        assert 'teacher_count' in sample_data.columns
        assert 'teacher_turnover_count' in sample_data.columns
        assert 'turnover_percent' in sample_data.columns

        # Test metric extraction
        row = sample_data.iloc[0]
        metrics = self.etl.extract_metrics(row)

        assert metrics['teacher_turnover_rate'] == 6.3
        assert metrics['teacher_turnover_count'] == 3.0
        assert metrics['teacher_count'] == 48.0
