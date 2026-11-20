"""
Tests for Students Taught by Ineffective Teachers ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
from etl.constants import KPI_COLUMNS
import shutil
from etl.students_taught_by_ineffective_teachers import (
    transform, StudentsTaughtByIneffectiveTeachersETL
)


class TestStudentsTaughtByIneffectiveTeachersETL:

    def setup_method(self):
        """Setup test directories and sample data."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)

        # Create sample raw data directory
        self.sample_dir = self.raw_dir / "students_taught_by_ineffective_teachers"
        self.sample_dir.mkdir(parents=True)

        # Create ETL instance for testing
        self.etl = StudentsTaughtByIneffectiveTeachersETL('students_taught_by_ineffective_teachers')

    def teardown_method(self):
        """Clean up test directories."""
        shutil.rmtree(self.test_dir)

    def create_sample_2024_data(self):
        """Create sample KYRC24/25 format data with Title I breakdown."""
        data = pd.DataFrame({
            'School Year': ['20232024', '20232024', '20232024', '20232024'],
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
            'Title I Status': ['Title 1', 'Not Title 1', 'Title 1', 'Not Title 1'],
            'All Students': ['0.5', '0.1', '0.8', '0.0'],
            'Non-White': ['0.6', '0.2', '1.0', '0.0'],
            'White (non-Hispanic)': ['0.4', '0.1', '0.7', '0.0'],
            'Economically Disadvantaged': ['0.7', '0.2', '1.2', '0.0'],
            'Non Economically Disadvantaged': ['0.3', '0.0', '0.4', '0.0'],
            'Students with Disabilities (IEP)': ['0.8', '0.3', '1.5', '0.0'],
            'Student without Disabilities (IEP)': ['0.4', '0.1', '0.6', '0.0'],
            'English Learner': ['0.9', '0.4', '2.0', '0.0'],
            'Non English Learner': ['0.4', '0.1', '0.7', '0.0']
        })
        return data

    def create_sample_historical_data(self):
        """Create sample historical format data (uppercase columns with % prefix)."""
        data = pd.DataFrame({
            'SCHOOL YEAR': ['20202021', '20202021'],
            'COUNTY NUMBER': ['001', '001'],
            'COUNTY NAME': ['ADAIR', 'ADAIR'],
            'DISTRICT NUMBER': ['001', '001'],
            'DISTRICT NAME': ['Adair County', 'Adair County'],
            'SCHOOL NUMBER': ['', ''],
            'SCHOOL NAME': ['---District Total---', '---District Total---'],
            'SCHOOL CODE': ['001', '001'],
            'STATE SCHOOL ID': ['', ''],
            'NCES ID': ['', ''],
            'CO-OP': ['GRREC', 'GRREC'],
            'CO-OP CODE': ['902', '902'],
            'SCHOOL TYPE': ['', ''],
            'TITLE I STATUS': ['Title 1', 'Not Title 1'],
            '% STUDENTS TAUGHT BY INEFFECTIVE TCHRS': ['0.5%', '0.1%'],
            '% NON-WHITE STUDENTS TAUGHT BY INEFFECTIVE TCHRS': ['0.6%', '0.2%'],
            '% WHITE STUDENTS TAUGHT BY INEFFECTIVE TCHRS': ['0.4%', '0.1%'],
            '% ECONOMICALLY DISADVANTAGED TAUGHT BY INEFFECTIVE TCHRS': ['0.7%', '0.2%'],
            '% NON-ECONOMICALLY DISADVANTAGED TAUGHT BY INEFFECTIVE TCHRS': ['0.3%', '0.0%'],
            '% STUDENTS WITH DISABILITIES TAUGHT BY INEFFECTIVE TCHRS': ['0.8%', '0.3%'],
            '% NON-STUDENTS WITH DISABILITIES TAUGHT BY INEFFECTIVE TCHRS': ['0.4%', '0.1%'],
            '% ENGLISH LEARNER STUDENTS TAUGHT BY INEFFECTIVE TCHRS': ['0.9%', '0.4%'],
            '% NON-ENGLISH LEARNER STUDENTS TAUGHT BY INEFFECTIVE TCHRS': ['0.4%', '0.1%']
        })
        return data

    def test_column_mappings(self):
        """Test that column mappings include required fields."""
        mappings = self.etl.module_column_mappings

        # Test title case (KYRC24/25)
        assert 'Title I Status' in mappings
        assert mappings['Title I Status'] == 'title_i_status'
        assert 'All Students' in mappings
        assert mappings['All Students'] == 'all_students'
        assert 'Non-White' in mappings
        assert mappings['Non-White'] == 'non_white'

        # Test uppercase variants (historical)
        assert 'TITLE I STATUS' in mappings
        assert '% STUDENTS TAUGHT BY INEFFECTIVE TCHRS' in mappings
        assert '% NON-WHITE STUDENTS TAUGHT BY INEFFECTIVE TCHRS' in mappings

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
        sample_data = self.create_sample_2024_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        # Test Title 1 row
        row = sample_data[sample_data['title_i_status'] == 'Title 1'].iloc[0]
        metrics = self.etl.extract_metrics(row)

        # Should have metrics with title_1 suffix and demographic keys
        assert 'students_taught_by_ineffective_teachers_rate_title_1__all_students' in metrics
        assert metrics['students_taught_by_ineffective_teachers_rate_title_1__all_students'] == 0.5
        assert 'students_taught_by_ineffective_teachers_rate_title_1__non_white' in metrics
        assert metrics['students_taught_by_ineffective_teachers_rate_title_1__non_white'] == 0.6

    def test_extract_metrics_not_title_1(self):
        """Test metric extraction for Not Title 1 schools."""
        sample_data = self.create_sample_2024_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        # Test Not Title 1 row
        row = sample_data[sample_data['title_i_status'] == 'Not Title 1'].iloc[0]
        metrics = self.etl.extract_metrics(row)

        # Should have metrics with not_title_1 suffix
        assert 'students_taught_by_ineffective_teachers_rate_not_title_1__all_students' in metrics
        assert metrics['students_taught_by_ineffective_teachers_rate_not_title_1__all_students'] == 0.1

    def test_extract_metrics_with_percentage_values(self):
        """Test that percentage values with % suffix are handled correctly."""
        sample_data = self.create_sample_historical_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        row = sample_data.iloc[0]
        metrics = self.etl.extract_metrics(row)

        # Values should be stripped of % and converted to float
        assert 'students_taught_by_ineffective_teachers_rate_title_1__all_students' in metrics
        assert metrics['students_taught_by_ineffective_teachers_rate_title_1__all_students'] == 0.5

    def test_should_skip_row_no_title_i(self):
        """Test that rows without Title I status are skipped."""
        row_no_title_i = pd.Series({
            'school_code': '001010',
            'title_i_status': '',
            'all_students': 0.5
        })
        assert self.etl.should_skip_row(row_no_title_i)

        row_with_title_i = pd.Series({
            'school_code': '001010',
            'title_i_status': 'Title 1',
            'all_students': 0.5
        })
        assert not self.etl.should_skip_row(row_with_title_i)

    def test_convert_to_kpi_format_demographics(self):
        """Test that KPI format correctly sets student_group from demographics."""
        sample_data = self.create_sample_2024_data()
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
        assert 'students_taught_by_ineffective_teachers_rate_title_1' in metrics
        assert 'students_taught_by_ineffective_teachers_rate_not_title_1' in metrics

    def test_convert_to_kpi_format_values(self):
        """Test that KPI format has correct values."""
        sample_data = self.create_sample_2024_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        kpi_df = self.etl.convert_to_kpi_format(sample_data, "test_file.csv")

        # Filter to Title 1, All Students for district total
        district_title1_all = kpi_df[
            (kpi_df['metric'] == 'students_taught_by_ineffective_teachers_rate_title_1') &
            (kpi_df['student_group'] == 'All Students') &
            (kpi_df['school_id'] == '001000')
        ]

        assert len(district_title1_all) == 1
        assert district_title1_all.iloc[0]['value'] == 0.5

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
        assert 'students_taught_by_ineffective_teachers_rate_title_1__all_students' in defaults
        assert pd.isna(defaults['students_taught_by_ineffective_teachers_rate_title_1__all_students'])

    def test_transform_integration(self):
        """Test complete transform function integration."""
        # Create sample files
        sample_2024 = self.create_sample_2024_data()
        sample_historical = self.create_sample_historical_data()

        # Save to test directory
        sample_2024.to_csv(
            self.sample_dir / "KYRC24_OVW_Students_Taught_by_Ineffective_Teachers.csv",
            index=False
        )
        sample_historical.to_csv(
            self.sample_dir / "students_taught_by_ineffective_teachers_2021.csv",
            index=False
        )

        # Run transform
        config = {}
        transform(self.raw_dir, self.proc_dir, config)

        # Check output file was created
        output_file = self.proc_dir / "students_taught_by_ineffective_teachers.csv"
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

        # Check demographics are properly set
        student_groups = result_df['student_group'].unique()
        assert 'All Students' in student_groups
        assert len(student_groups) >= 5  # Should have multiple demographics

    def test_equity_gap_handling(self):
        """Test that Equity Gap rows are processed correctly."""
        data = pd.DataFrame({
            'School Year': ['20232024'],
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
            'Title I Status': ['Equity Gap'],
            'All Students': ['0.3'],
            'Non-White': ['0.4'],
            'White (non-Hispanic)': ['0.2'],
            'Economically Disadvantaged': ['0.5'],
            'Non Economically Disadvantaged': ['0.1'],
            'Students with Disabilities (IEP)': ['0.6'],
            'Student without Disabilities (IEP)': ['0.2'],
            'English Learner': ['0.7'],
            'Non English Learner': ['0.2']
        })

        data = self.etl.normalize_column_names(data)
        kpi_df = self.etl.convert_to_kpi_format(data, "test_file.csv")

        # Check Equity Gap metrics
        metrics = kpi_df['metric'].unique()
        assert 'students_taught_by_ineffective_teachers_rate_equity_gap' in metrics

        # Verify values
        gap_all_students = kpi_df[
            (kpi_df['metric'] == 'students_taught_by_ineffective_teachers_rate_equity_gap') &
            (kpi_df['student_group'] == 'All Students')
        ]
        assert len(gap_all_students) == 1
        assert gap_all_students.iloc[0]['value'] == 0.3

    def test_zero_values_included(self):
        """Test that zero values are included in output."""
        data = pd.DataFrame({
            'School Year': ['20232024'],
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
            'Title I Status': ['Title 1'],
            'All Students': ['0.0'],  # Zero value
            'Non-White': ['0.0'],
            'White (non-Hispanic)': ['0.0'],
            'Economically Disadvantaged': ['0.0'],
            'Non Economically Disadvantaged': ['0.0'],
            'Students with Disabilities (IEP)': ['0.0'],
            'Student without Disabilities (IEP)': ['0.0'],
            'English Learner': ['0.0'],
            'Non English Learner': ['0.0']
        })

        data = self.etl.normalize_column_names(data)
        kpi_df = self.etl.convert_to_kpi_format(data, "test_file.csv")

        # Zero values should be included
        assert len(kpi_df) == 9  # 9 demographic groups
        assert all(kpi_df['value'] == 0.0)
