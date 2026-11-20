"""
Tests for Teacher Certification ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
from etl.constants import KPI_COLUMNS
import shutil
from etl.teacher_certification import transform, TeacherCertificationETL


class TestTeacherCertificationETL:

    def setup_method(self):
        """Setup test directories and sample data."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)

        # Create sample raw data directory
        self.sample_dir = self.raw_dir / "teacher_certification"
        self.sample_dir.mkdir(parents=True)

        # Create ETL instance for testing
        self.etl = TeacherCertificationETL('teacher_certification')

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
            'Emergency Provisional Teacher Count': ['3', '2', '8'],
            'Percent Emergency Provisional Teachers': ['6.0', '5.7', '4.3'],
            'National Board Certified Count': ['5', '2', '15'],
            'Percent National Board Certified Teacher': ['10.0', '5.7', '8.1']
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
            'TEACHER COUNT': ['51', '172'],
            'EMERGENCY/PROVISIONAL TEACHER COUNT': ['3', '6'],
            'PERCENT EMERGENCY/PROVISIONAL TEACHERS': ['5.88', '3.49'],
            'NATIONAL BOARD CERTIFIED COUNT': ['4', '14'],
            'PERCENT NATIONAL BOARD CERTIFIED TEACHERS': ['7.84', '8.14']
        })
        return data

    def test_column_mappings(self):
        """Test that column mappings include required fields."""
        mappings = self.etl.module_column_mappings

        # Test title case (KYRC24/25)
        assert 'Teacher Count' in mappings
        assert mappings['Teacher Count'] == 'teacher_count'
        assert 'Emergency Provisional Teacher Count' in mappings
        assert mappings['Emergency Provisional Teacher Count'] == 'emergency_provisional_count'
        assert 'National Board Certified Count' in mappings
        assert mappings['National Board Certified Count'] == 'national_board_certified_count'

        # Test uppercase variants (historical)
        assert 'EMERGENCY/PROVISIONAL TEACHER COUNT' in mappings
        assert 'PERCENT EMERGENCY/PROVISIONAL TEACHERS' in mappings
        assert 'NATIONAL BOARD CERTIFIED COUNT' in mappings

    def test_extract_metrics(self):
        """Test metric extraction from sample data."""
        sample_data = self.create_sample_2024_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        # Test first row (high school)
        row = sample_data.iloc[0]
        metrics = self.etl.extract_metrics(row)

        assert 'emergency_provisional_teacher_rate' in metrics
        assert metrics['emergency_provisional_teacher_rate'] == 6.0
        assert 'emergency_provisional_teacher_count' in metrics
        assert metrics['emergency_provisional_teacher_count'] == 3.0
        assert 'national_board_certified_rate' in metrics
        assert metrics['national_board_certified_rate'] == 10.0
        assert 'national_board_certified_count' in metrics
        assert metrics['national_board_certified_count'] == 5.0

    def test_extract_metrics_with_suppressed_values(self):
        """Test that suppressed values (asterisks) result in missing metrics."""
        data = pd.DataFrame({
            'School Year': ['20232024'],
            'County Number': ['001'],
            'District Number': ['001'],
            'School Code': ['001010'],
            'District Name': ['Test District'],
            'School Name': ['Test School'],
            'Teacher Count': ['10'],
            'Emergency Provisional Teacher Count': ['*'],  # Suppressed
            'Percent Emergency Provisional Teachers': ['*'],
            'National Board Certified Count': ['3'],
            'Percent National Board Certified Teacher': ['30.0']
        })

        data = self.etl.normalize_column_names(data)
        data = self.etl.standardize_missing_values(data)
        row = data.iloc[0]
        metrics = self.etl.extract_metrics(row)

        # Suppressed values should not be in metrics
        assert 'emergency_provisional_teacher_rate' not in metrics
        assert 'emergency_provisional_teacher_count' not in metrics
        # Non-suppressed values should be present
        assert 'national_board_certified_rate' in metrics
        assert 'national_board_certified_count' in metrics

    def test_extract_metrics_with_missing_data(self):
        """Test that rows with all missing data return empty metrics."""
        data = pd.DataFrame({
            'School Year': ['20232024'],
            'County Number': ['001'],
            'District Number': ['001'],
            'School Code': ['001010'],
            'District Name': ['Test District'],
            'School Name': ['Test School'],
            'Teacher Count': [pd.NA],
            'Emergency Provisional Teacher Count': [pd.NA],
            'Percent Emergency Provisional Teachers': [pd.NA],
            'National Board Certified Count': [pd.NA],
            'Percent National Board Certified Teacher': [pd.NA]
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
            'Emergency Provisional Teacher Count': ['-5'],  # Invalid negative
            'Percent Emergency Provisional Teachers': ['10.0'],
            'National Board Certified Count': ['3'],
            'Percent National Board Certified Teacher': ['6.0']
        })

        data = self.etl.normalize_column_names(data)
        row = data.iloc[0]
        metrics = self.etl.extract_metrics(row)

        # Negative values should be excluded
        assert 'emergency_provisional_teacher_count' not in metrics
        # Other metrics should still be present
        assert 'emergency_provisional_teacher_rate' in metrics
        assert 'national_board_certified_count' in metrics

    def test_should_skip_row(self):
        """Test row skipping logic."""
        # Row with no school_code should be skipped
        row_no_code = pd.Series({
            'school_code': pd.NA,
            'emergency_provisional_count': 5,
            'emergency_provisional_percent': 10.0
        })
        assert self.etl.should_skip_row(row_no_code)

        # Row with empty school_code should be skipped
        row_empty_code = pd.Series({
            'school_code': '',
            'emergency_provisional_count': 5,
            'emergency_provisional_percent': 10.0
        })
        assert self.etl.should_skip_row(row_empty_code)

        # Valid row should not be skipped
        row_valid = pd.Series({
            'school_code': '001010',
            'emergency_provisional_count': 5,
            'emergency_provisional_percent': 10.0
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

        # Check that we have the right number of KPI rows (4 metrics per school)
        assert len(kpi_df) == 12  # 3 schools × 4 metrics

        # Check specific metrics exist
        metrics = kpi_df['metric'].unique()
        assert 'emergency_provisional_teacher_rate' in metrics
        assert 'emergency_provisional_teacher_count' in metrics
        assert 'national_board_certified_rate' in metrics
        assert 'national_board_certified_count' in metrics

        # Check values are correct
        high_school_rows = kpi_df[kpi_df['school_id'] == '001010']
        ep_rate_row = high_school_rows[high_school_rows['metric'] == 'emergency_provisional_teacher_rate'].iloc[0]
        assert ep_rate_row['value'] == 6.0
        assert ep_rate_row['year'] == '2024'

    def test_suppressed_metric_defaults(self):
        """Test suppressed metric defaults."""
        sample_row = pd.Series({
            'emergency_provisional_percent': 5.0,
            'emergency_provisional_count': 3,
            'national_board_certified_percent': 10.0,
            'national_board_certified_count': 5
        })
        defaults = self.etl.get_suppressed_metric_defaults(sample_row)

        assert 'emergency_provisional_teacher_rate' in defaults
        assert 'emergency_provisional_teacher_count' in defaults
        assert 'national_board_certified_rate' in defaults
        assert 'national_board_certified_count' in defaults
        assert pd.isna(defaults['emergency_provisional_teacher_rate'])

    def test_transform_integration(self):
        """Test complete transform function integration."""
        # Create sample files
        sample_2024 = self.create_sample_2024_data()
        sample_historical = self.create_sample_historical_data()

        # Save to test directory
        sample_2024.to_csv(self.sample_dir / "KYRC24_OVW_Teacher_Certification_Data.csv", index=False)
        sample_historical.to_csv(self.sample_dir / "teacher_certifications_2021.csv", index=False)

        # Run transform
        config = {}
        transform(self.raw_dir, self.proc_dir, config)

        # Check output file was created
        output_file = self.proc_dir / "teacher_certification.csv"
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
        assert 'emergency_provisional_count' in sample_data.columns
        assert 'emergency_provisional_percent' in sample_data.columns
        assert 'national_board_certified_count' in sample_data.columns
        assert 'national_board_certified_percent' in sample_data.columns

        # Test metric extraction
        row = sample_data.iloc[0]
        metrics = self.etl.extract_metrics(row)

        assert metrics['emergency_provisional_teacher_rate'] == 5.88
        assert metrics['emergency_provisional_teacher_count'] == 3.0
        assert metrics['national_board_certified_rate'] == 7.84
        assert metrics['national_board_certified_count'] == 4.0
