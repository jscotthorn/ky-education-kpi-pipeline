"""
Tests for Students Taught by Out-of-Field Teachers ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
from etl.constants import KPI_COLUMNS
import shutil
from etl.students_taught_by_out_of_field_teachers import (
    transform, StudentsTaughtByOutOfFieldTeachersETL
)


class TestStudentsTaughtByOutOfFieldTeachersETL:

    def setup_method(self):
        """Setup test directories and sample data."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)

        # Create sample raw data directory
        self.sample_dir = self.raw_dir / "students_taught_by_out_of_field_teachers"
        self.sample_dir.mkdir(parents=True)

        # Create ETL instance for testing
        self.etl = StudentsTaughtByOutOfFieldTeachersETL('students_taught_by_out_of_field_teachers')

    def teardown_method(self):
        """Clean up test directories."""
        shutil.rmtree(self.test_dir)

    def create_sample_kyrc25_data(self):
        """Create sample KYRC25 format data with Title I breakdown."""
        data = pd.DataFrame({
            'School Year': ['20242025', '20242025', '20242025', '20242025'],
            'County Number': ['001', '001', '001', '001'],
            'County Name': ['Adair', 'Adair', 'Adair', 'Adair'],
            'District Number': ['001', '001', '001', '001'],
            'District Name': ['Adair County', 'Adair County', 'Adair County', 'Adair County'],
            'School Number': ['', '', '010', '010'],
            'School Name': ['All Schools', 'All Schools', 'Adair County High', 'Adair County High'],
            'School Code': ['001000', '001000', '001010', '001010'],
            'State School Id': ['', '', '001001010', '001001010'],
            'NCES ID': ['', '', '210003000001', '210003000001'],
            'CO-OP': ['GRREC', 'GRREC', 'GRREC', 'GRREC'],
            'CO-OP Code': ['902', '902', '902', '902'],
            'School Type': ['', '', 'A1', 'A1'],
            'Title_I_Status': ['Title 1', 'Not Title 1', 'Title 1', 'Not Title 1'],
            'All Students': ['5.5', '3.1', '6.8', '2.0'],
            'Non-White': ['6.2', '3.5', '7.0', '2.5'],
            'White': ['5.0', '3.0', '6.5', '1.8'],
            'Economically Disadvantaged': ['7.0', '4.0', '8.2', '3.0'],
            'Non-Economically Disadvantaged': ['4.0', '2.0', '5.0', '1.5'],
            'Students with Disabilities (IEP)': ['8.0', '5.0', '9.5', '4.0'],
            'Student without Disabilities (IEP)': ['5.0', '2.5', '6.0', '1.5'],
            'English Learner': ['9.0', '6.0', '10.0', '5.0'],
            'Non-English Learner': ['5.0', '2.5', '6.5', '1.8']
        })
        return data

    def create_sample_historical_data(self):
        """Create sample historical format data (uppercase columns)."""
        data = pd.DataFrame({
            'SCHOOL YEAR': ['20202021', '20202021', '20202021'],
            'COUNTY NUMBER': ['001', '001', '001'],
            'COUNTY NAME': ['ADAIR', 'ADAIR', 'ADAIR'],
            'DISTRICT NUMBER': ['001', '001', '001'],
            'DISTRICT NAME': ['Adair County', 'Adair County', 'Adair County'],
            'SCHOOL NUMBER': ['', '', ''],
            'SCHOOL NAME': ['---District Total---', '---District Total---', '---District Total---'],
            'SCHOOL CODE': ['001', '001', '001'],
            'STATE SCHOOL ID': ['', '', ''],
            'NCES ID': ['', '', ''],
            'CO-OP': ['GRREC', 'GRREC', 'GRREC'],
            'CO-OP CODE': ['902', '902', '902'],
            'SCHOOL TYPE': ['', '', ''],
            'SCHOOL TYPE': ['', '', ''],  # Duplicate column in historical data
            'TITLE I STATUS': ['Title 1', 'Not Title 1', 'Equity Gap'],
            '% STUDENTS TAUGHT BY OUT OF FIELD TCHRS': ['5.5', '3.0', '2.5'],
            '% NON-WHITE STUDENTS TAUGHT BY OUT OF FIELD TCHRS': ['6.0', '3.5', '2.5'],
            '% WHITE STUDENTS TAUGHT  BY OUT OF FIELD TCHRS': ['5.0', '2.8', '2.2'],  # Double space
            'OUT OF FIELD GAP % NON-WHITE ': ['', '', ''],  # Trailing space
            '% ECONOMICALLY DISADVANTAGED TAUGHT BY OUT OF FIELD TCHRS': ['7.0', '4.0', '3.0'],
            '% NON-ECONOMICALLY DISADVANTAGED TAUGHT BY OUT OF FIELD TCHRS': ['4.0', '2.0', '2.0'],
            'OUT OF FIELD GAP % ECONOMICALLY DISADVANTAGED': ['', '', ''],
            '% STUDENTS WITH DISABILITIES TAUGHT BY OUT OF FIELD TCHRS': ['8.0', '5.0', '3.0'],
            '% NON-STUDENTS WITH DISABILITIES TAUGHT BY OUT OF FIELD TCHRS': ['5.0', '2.5', '2.5'],
            'OUT OF FIELD GAP % STUDENTS WITH DISABILITIES': ['', '', ''],
            '% ENGLISH LEARNER STUDENTS TAUGHT BY OUT OF FIELD TCHRS': ['9.0', '6.0', '3.0'],
            '% NON-ENGLISH LEARNER STUDENTS TAUGHT BY OUT OF FIELD TCHRS': ['5.0', '2.5', '2.5'],
            'OUT OF FIELD GAP % ENGLISH LEARNERS': ['', '', '']
        })
        return data

    def create_sample_kyrc24_corrupted_data(self):
        """Create sample KYRC24 format data with corrupted headers."""
        data = pd.DataFrame({
            'School Year': ['20232024', '20232024', '20232024'],
            'County Number': ['001', '001', '001'],
            'County Name': ['Adair', 'Adair', 'Adair'],
            'District Number': ['001', '001', '001'],
            'District Name': ['Adair County', 'Adair County', 'Adair County'],
            'School Number': ['', '', ''],
            'School Name': ['All Schools', 'All Schools', 'All Schools'],
            'School Code': ['001000', '001000', '001000'],
            'State School Id': ['', '', ''],
            'NCES ID': ['', '', ''],
            'CO-OP': ['GRREC', 'GRREC', 'GRREC'],
            'CO-OP Code': ['902', '902', '902'],
            'School Type': ['', '', ''],
            'Unnamed: 13': ['Equity Gap', 'Not Title 1', 'Title 1'],  # Corrupted column
            'Inexperienced Value': ['3.1', '5.6', '8.9'],
            'Inexperienced Value.1': ['4.0', '5.7', '9.1'],
            'Inexperienced Value.2': ['2.7', '5.5', '8.8'],
            'Inexperienced Value.3': ['4.3', '4.7', '9.0'],
            'Inexperienced Value.4': ['1.8', '6.2', '8.6'],
            'Inexperienced Value.5': ['3.7', '5.0', '10.0'],
            'Inexperienced Value.6': ['3.0', '5.7', '8.6'],
            'Inexperienced Value.7': ['0.0', '12.1', '9.3'],
            'Inexperienced Value.8': ['3.1', '5.3', '8.8']
        })
        return data

    def test_column_mappings(self):
        """Test that column mappings include required fields."""
        mappings = self.etl.module_column_mappings

        # Test title case (KYRC25)
        assert 'Title_I_Status' in mappings
        assert mappings['Title_I_Status'] == 'title_i_status'
        assert 'All Students' in mappings
        assert mappings['All Students'] == 'all_students'
        assert 'Non-White' in mappings
        assert mappings['Non-White'] == 'non_white'

        # Test uppercase variants (historical)
        assert 'TITLE I STATUS' in mappings
        assert '% STUDENTS TAUGHT BY OUT OF FIELD TCHRS' in mappings
        assert '% NON-WHITE STUDENTS TAUGHT BY OUT OF FIELD TCHRS' in mappings

        # Test corrupted KYRC24 column fixes
        assert 'Unnamed: 13' in mappings
        assert mappings['Unnamed: 13'] == 'title_i_status'
        assert 'Inexperienced Value' in mappings
        assert mappings['Inexperienced Value'] == 'all_students'

    def test_demographic_display_map(self):
        """Test demographic display mapping."""
        display_map = self.etl.DEMOGRAPHIC_DISPLAY_MAP

        assert display_map['all_students'] == 'All Students'
        assert display_map['white'] == 'White (non-Hispanic)'
        assert display_map['non_white'] == 'Non-White'
        assert display_map['economically_disadvantaged'] == 'Economically Disadvantaged'
        assert display_map['students_with_disabilities'] == 'Students with Disabilities (IEP)'
        assert display_map['english_learner'] == 'English Learner'

    def test_extract_metrics_title_1(self):
        """Test metric extraction for Title 1 schools."""
        sample_data = self.create_sample_kyrc25_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        # Test Title 1 row
        row = sample_data[sample_data['title_i_status'] == 'Title 1'].iloc[0]
        metrics = self.etl.extract_metrics(row)

        # Should have metrics with title_1 suffix and demographic keys
        assert 'students_taught_by_out_of_field_teachers_rate_title_1__all_students' in metrics
        assert metrics['students_taught_by_out_of_field_teachers_rate_title_1__all_students'] == 5.5
        assert 'students_taught_by_out_of_field_teachers_rate_title_1__non_white' in metrics
        assert metrics['students_taught_by_out_of_field_teachers_rate_title_1__non_white'] == 6.2

    def test_extract_metrics_not_title_1(self):
        """Test metric extraction for Not Title 1 schools."""
        sample_data = self.create_sample_kyrc25_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        # Test Not Title 1 row
        row = sample_data[sample_data['title_i_status'] == 'Not Title 1'].iloc[0]
        metrics = self.etl.extract_metrics(row)

        # Should have metrics with not_title_1 suffix
        assert 'students_taught_by_out_of_field_teachers_rate_not_title_1__all_students' in metrics
        assert metrics['students_taught_by_out_of_field_teachers_rate_not_title_1__all_students'] == 3.1

    def test_extract_metrics_equity_gap(self):
        """Test metric extraction for Equity Gap rows."""
        sample_data = self.create_sample_historical_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        # Test Equity Gap row
        row = sample_data[sample_data['title_i_status'] == 'Equity Gap'].iloc[0]
        metrics = self.etl.extract_metrics(row)

        # Should have metrics with equity_gap suffix
        assert 'students_taught_by_out_of_field_teachers_rate_equity_gap__all_students' in metrics
        assert metrics['students_taught_by_out_of_field_teachers_rate_equity_gap__all_students'] == 2.5

    def test_kyrc24_corrupted_headers(self):
        """Test that corrupted KYRC24 headers are handled correctly."""
        sample_data = self.create_sample_kyrc24_corrupted_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        # Verify column mapping worked
        assert 'title_i_status' in sample_data.columns
        assert 'all_students' in sample_data.columns

        # Test Title 1 row
        row = sample_data[sample_data['title_i_status'] == 'Title 1'].iloc[0]
        metrics = self.etl.extract_metrics(row)

        assert 'students_taught_by_out_of_field_teachers_rate_title_1__all_students' in metrics
        assert metrics['students_taught_by_out_of_field_teachers_rate_title_1__all_students'] == 8.9

    def test_should_skip_row_no_title_i(self):
        """Test that rows without Title I status are skipped."""
        row_no_title_i = pd.Series({
            'school_code': '001010',
            'title_i_status': '',
            'all_students': 5.5
        })
        assert self.etl.should_skip_row(row_no_title_i)

        row_with_title_i = pd.Series({
            'school_code': '001010',
            'title_i_status': 'Title 1',
            'all_students': 5.5
        })
        assert not self.etl.should_skip_row(row_with_title_i)

    def test_convert_to_kpi_format_demographics(self):
        """Test that KPI format correctly sets student_group from demographics."""
        sample_data = self.create_sample_kyrc25_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        kpi_df = self.etl.convert_to_kpi_format(sample_data, "test_file.csv")

        # Check that DataFrame has correct columns
        assert set(kpi_df.columns).issubset(set(KPI_COLUMNS))

        # Check that student_group is properly populated
        student_groups = kpi_df['student_group'].unique()
        assert 'All Students' in student_groups
        assert 'Non-White' in student_groups
        assert 'White (non-Hispanic)' in student_groups
        assert 'Economically Disadvantaged' in student_groups
        assert 'English Learner' in student_groups

        # Check metric names don't include demographic (it's in student_group)
        metrics = kpi_df['metric'].unique()
        assert 'students_taught_by_out_of_field_teachers_rate_title_1' in metrics
        assert 'students_taught_by_out_of_field_teachers_rate_not_title_1' in metrics

    def test_convert_to_kpi_format_values(self):
        """Test that KPI format has correct values."""
        sample_data = self.create_sample_kyrc25_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        kpi_df = self.etl.convert_to_kpi_format(sample_data, "test_file.csv")

        # Filter to Title 1, All Students for district total
        district_title1_all = kpi_df[
            (kpi_df['metric'] == 'students_taught_by_out_of_field_teachers_rate_title_1') &
            (kpi_df['student_group'] == 'All Students') &
            (kpi_df['school_id'] == '001000')
        ]

        assert len(district_title1_all) == 1
        assert district_title1_all.iloc[0]['value'] == 5.5

    def test_suppressed_metric_defaults(self):
        """Test suppressed metric defaults for Title I data."""
        sample_row = pd.Series({
            'title_i_status': 'Title 1',
            'all_students': pd.NA,
            'non_white': pd.NA,
            'white': pd.NA,
            'economically_disadvantaged': pd.NA,
            'non_economically_disadvantaged': pd.NA,
            'students_with_disabilities': pd.NA,
            'student_without_disabilities': pd.NA,
            'english_learner': pd.NA,
            'non_english_learner': pd.NA
        })
        defaults = self.etl.get_suppressed_metric_defaults(sample_row)

        # Should have defaults for all demographic columns present
        assert 'students_taught_by_out_of_field_teachers_rate_title_1__all_students' in defaults
        assert pd.isna(defaults['students_taught_by_out_of_field_teachers_rate_title_1__all_students'])

    def test_transform_integration(self):
        """Test complete transform function integration."""
        # Create sample files
        sample_kyrc25 = self.create_sample_kyrc25_data()
        sample_historical = self.create_sample_historical_data()

        # Save to test directory
        sample_kyrc25.to_csv(
            self.sample_dir / "KYRC25_OVW_Students_Taught_by_Out_of_Field_Teachers.csv",
            index=False
        )
        sample_historical.to_csv(
            self.sample_dir / "students_taught_by_out_of_field_teachers_2021.csv",
            index=False
        )

        # Run transform
        config = {}
        transform(self.raw_dir, self.proc_dir, config)

        # Check output file was created
        output_file = self.proc_dir / "students_taught_by_out_of_field_teachers.csv"
        assert output_file.exists()

        # Load and validate output
        result_df = pd.read_csv(output_file)
        assert not result_df.empty
        assert set(result_df.columns).issubset(set(KPI_COLUMNS))

        # Check that we have data from both source files
        source_files = result_df['source_file'].unique()
        assert any('KYRC25' in f for f in source_files)
        assert any('2021' in f for f in source_files)

        # Check years
        years = result_df['year'].unique()
        assert 2025 in years or '2025' in years
        assert 2021 in years or '2021' in years

        # Check demographics are properly set
        student_groups = result_df['student_group'].unique()
        assert 'All Students' in student_groups
        assert len(student_groups) >= 5  # Should have multiple demographics

    def test_negative_equity_gap_values(self):
        """Test that negative values in equity gap are handled correctly."""
        data = pd.DataFrame({
            'School Year': ['20242025'],
            'County Number': ['001'],
            'County Name': ['Adair'],
            'District Number': ['001'],
            'District Name': ['Adair County'],
            'School Number': [''],
            'School Name': ['All Schools'],
            'School Code': ['001000'],
            'State School Id': [''],
            'NCES ID': [''],
            'CO-OP': ['GRREC'],
            'CO-OP Code': ['902'],
            'School Type': [''],
            'Title_I_Status': ['Equity Gap'],
            'All Students': ['-2.5'],  # Negative gap
            'Non-White': ['-1.5'],
            'White': ['-3.0'],
            'Economically Disadvantaged': ['1.0'],
            'Non-Economically Disadvantaged': ['-1.0'],
            'Students with Disabilities (IEP)': ['2.0'],
            'Student without Disabilities (IEP)': ['-2.0'],
            'English Learner': ['3.0'],
            'Non-English Learner': ['-3.0']
        })

        data = self.etl.normalize_column_names(data)
        kpi_df = self.etl.convert_to_kpi_format(data, "test_file.csv")

        # Negative values should be included
        gap_all_students = kpi_df[
            (kpi_df['metric'] == 'students_taught_by_out_of_field_teachers_rate_equity_gap') &
            (kpi_df['student_group'] == 'All Students')
        ]
        assert len(gap_all_students) == 1
        assert gap_all_students.iloc[0]['value'] == -2.5

    def test_zero_values_included(self):
        """Test that zero values are included in output."""
        data = pd.DataFrame({
            'School Year': ['20242025'],
            'County Number': ['001'],
            'County Name': ['Adair'],
            'District Number': ['001'],
            'District Name': ['Adair County'],
            'School Number': ['010'],
            'School Name': ['Test School'],
            'School Code': ['001010'],
            'State School Id': ['001001010'],
            'NCES ID': ['210003000001'],
            'CO-OP': ['GRREC'],
            'CO-OP Code': ['902'],
            'School Type': ['A1'],
            'Title_I_Status': ['Title 1'],
            'All Students': ['0.0'],  # Zero value
            'Non-White': ['0.0'],
            'White': ['0.0'],
            'Economically Disadvantaged': ['0.0'],
            'Non-Economically Disadvantaged': ['0.0'],
            'Students with Disabilities (IEP)': ['0.0'],
            'Student without Disabilities (IEP)': ['0.0'],
            'English Learner': ['0.0'],
            'Non-English Learner': ['0.0']
        })

        data = self.etl.normalize_column_names(data)
        kpi_df = self.etl.convert_to_kpi_format(data, "test_file.csv")

        # Zero values should be included
        assert len(kpi_df) == 9  # 9 demographic groups
        assert all(kpi_df['value'] == 0.0)
