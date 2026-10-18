"""
Tests for Students Taught by Out-of-Field Teachers ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
import yaml
from etl.constants import KPI_COLUMNS
import shutil
from etl.students_taught_by_out_of_field_teachers import (
    transform, StudentsTaughtByOutOfFieldTeachersETL
)

ROOT = Path(__file__).resolve().parents[1]


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
        """A published negative equity-gap cell is stored.

        In 2020-2023 the gap is Title I minus Not Title I, so a negative
        cell is one where Title I is lower. KYRC26 follows that difference,
        including the state English learner gap of -6.0. KYRC24 and KYRC25
        do not follow one rule on every cell. Issue #59
        (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/59)
        tracks the signed gap. This module stores the published cell. The
        drop is not suppression ``Y``.
        """
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

    def _production_config(self) -> dict:
        config = yaml.safe_load((ROOT / "config" / "mappings.yaml").read_text())
        return config["sources"]["students_taught_by_out_of_field_teachers"]

    def test_kyrc26_file_is_accepted(self):
        """The permanent KYRC files and the historical names, including 2020, stay in scope."""
        assert self.etl.include_kyrc26 is True
        assert self.etl.module_column_mappings["Title_I_Status"] == "title_i_status"
        assert self.etl.module_column_mappings["All Students"] == "all_students"
        assert self.etl.module_column_mappings["Non-White"] == "non_white"
        assert self.etl.module_column_mappings["Unnamed: 13"] == "title_i_status"
        assert self.etl.module_column_mappings["Inexperienced Value"] == "all_students"
        assert self.etl.module_column_mappings["% STUDENTS TAUGHT BY OUT OF FIELD TCHRS"] == "all_students"

        accepted = [
            "KYRC26_OVW_Students_Taught_by_Out_of_Field_Teachers.csv",
            "KYRC25_OVW_Students_Taught_by_Out_of_Field_Teachers.csv",
            "KYRC24_OVW_Students_Taught_by_Out_of_Field_Teachers.csv",
            "students_taught_by_out_of_field_teachers_2023.csv",
            "students_taught_by_out_of_field_teachers_2022.csv",
            "students_taught_by_out_of_field_teachers_2021.csv",
            "students_taught_by_out_of_field_teachers_2020.csv",
        ]
        for name in accepted:
            (self.sample_dir / name).write_text(
                "School Year,School Code,Title I Status,All Students\n20252026,999000,Title 1,7.3\n"
            )
        (self.sample_dir / "KYRC23_OVW_Students_Taught_by_Out_of_Field_Teachers.csv").write_text(
            "School Year,School Code,Title I Status,All Students\n20222023,001010,Title 1,6.6\n"
        )

        found = sorted(path.name for path, _sheet in self.etl._find_data_files(self.sample_dir))
        assert found == sorted(accepted)

    def test_kyrc23_republish_is_skipped(self):
        """A leftover KYRC23 file must not write a second copy of 2023.

        The datasets file is school year 20222023. The school codes match
        the historical 2023 extract once district codes are padded. Many
        cells differ. Accepting it would write 2023 twice.
        """
        republish = (
            "School Year,District Name,School Name,School Code,Title I Status,All Students\n"
            "20222023,Adair County,Adair County High School,001010,Title 1,6.6\n"
        )
        (self.sample_dir / "KYRC23_OVW_Students_Taught_by_Out_of_Field_Teachers.csv").write_text(republish)
        (self.sample_dir / "KYRC26_OVW_Students_Taught_by_Out_of_Field_Teachers.csv").write_text(
            "School Year,District Name,School Name,School Code,Title I Status,All Students\n"
            "20252026,All Districts,All Schools,999000,Title 1,7.3\n"
        )
        transform(self.raw_dir, self.proc_dir, self._production_config())
        result = pd.read_csv(self.proc_dir / "students_taught_by_out_of_field_teachers.csv", dtype=str)
        assert set(result["year"]) == {"2026"}
        assert set(result["source_file"]) == {"KYRC26_OVW_Students_Taught_by_Out_of_Field_Teachers.csv"}
        assert float(result["value"].iloc[0]) == 7.3
        assert result["school_id"].iloc[0] == "999000"
        assert result["district"].iloc[0] == "All Districts"

    def test_na_row_emits_nothing_and_zero_is_kept(self):
        """N/A is not a percent. A published 0 is kept.

        No registered file uses N/A, #N/A, or a blank measure cell. There
        is no student-count column, so that text is not a zero. notes/8: a
        row that is not suppressed and has an empty value emits nothing.
        A sibling cell of 0 does not fill the N/A cells. A published 0,
        including an all-zero Title I row, is kept.
        """
        text = (
            "School Year,District Name,School Name,School Code,Title I Status,All Students,Non-White\n"
            "20222023,Example District,Empty School,012012,Not Title 1,N/A,N/A\n"
            "20202021,Example District,Hash School,011030,Not Title 1,#N/A,#N/A\n"
            "20192020,Example District,Partial School,236021,Not Title 1,N/A,0.00\n"
            "20252026,Adair County,Adair County High School,001010,Title 1,0.0,0.0\n"
        )
        (self.sample_dir / "KYRC26_OVW_Students_Taught_by_Out_of_Field_Teachers.csv").write_text(text)
        transform(self.raw_dir, self.proc_dir, self._production_config())
        result = pd.read_csv(self.proc_dir / "students_taught_by_out_of_field_teachers.csv", dtype=str)
        assert "012012" not in set(result["school_id"])
        assert "011030" not in set(result["school_id"])
        partial = result[result["school_id"] == "236021"]
        assert set(partial["student_group"]) == {"Non-White"}
        assert float(partial["value"].iloc[0]) == 0.0
        assert partial["suppressed"].iloc[0] == "N"
        assert "All Students" not in set(partial["student_group"])
        adair = result[result["school_id"] == "001010"]
        assert len(adair) == 2
        assert set(adair["value"].astype(float)) == {0.0}
        assert set(adair["suppressed"]) == {"N"}
        assert set(adair["year"]) == {"2026"}

    def test_disability_only_zero_is_kept(self):
        """A students-with-disabilities 0 is kept when the other cells are N/A.

        The 2020 ineffective-teacher file has rows whose only number is
        students with disabilities 0.00. This file does not. Every mapped
        percent in 2020-2026 is numeric. A constructed row with that shape
        still stores the published 0. Dropping it would invent a rule this
        source does not need.
        """
        text = (
            "School Year,District Name,School Name,School Code,Title I Status,"
            "All Students,Non-White,White (non-Hispanic),"
            "Students with Disabilities (IEP)\n"
            "20192020,Example District,Placeholder School,236021,Not Title 1,N/A,N/A,N/A,0.00\n"
            "20252026,Fayette County,Bryan Station High School,165170,Title 1,N/A,N/A,N/A,0.0\n"
        )
        (self.sample_dir / "KYRC26_OVW_Students_Taught_by_Out_of_Field_Teachers.csv").write_text(text)
        transform(self.raw_dir, self.proc_dir, self._production_config())
        result = pd.read_csv(
            self.proc_dir / "students_taught_by_out_of_field_teachers.csv", dtype=str
        )

        def kept(school_id: str) -> pd.Series:
            rows = result[result["school_id"] == school_id]
            assert len(rows) == 1
            return rows.iloc[0]

        for school_id, metric in (
            ("236021", "students_taught_by_out_of_field_teachers_rate_not_title_1"),
            ("165170", "students_taught_by_out_of_field_teachers_rate_title_1"),
        ):
            row = kept(school_id)
            assert row["student_group"] == "Students with Disabilities (IEP)"
            assert float(row["value"]) == 0.0
            assert row["suppressed"] == "N"
            assert row["metric"] == metric

    def test_suppressed_row_is_kept_empty(self):
        """A suppressed Title I row is stored with an empty value.

        These files have no Suppressed column and no starred cell. The
        constructed Y row is the notes/8 shape: the value is empty and the
        row is not dropped.
        """
        text = (
            "School Year,District Name,School Name,School Code,Title I Status,All Students,Suppressed\n"
            "20252026,All Districts,All Schools,999000,Title 1,,Y\n"
            "20252026,Fayette County,Bryan Station High School,165170,Title 1,37.8,N\n"
        )
        (self.sample_dir / "KYRC26_OVW_Students_Taught_by_Out_of_Field_Teachers.csv").write_text(text)
        transform(self.raw_dir, self.proc_dir, self._production_config())
        result = pd.read_csv(self.proc_dir / "students_taught_by_out_of_field_teachers.csv", dtype=str)
        suppressed = result[result["suppressed"] == "Y"]
        assert len(suppressed) == 1
        assert suppressed.iloc[0]["school_id"] == "999000"
        assert suppressed.iloc[0]["metric"] == "students_taught_by_out_of_field_teachers_rate_title_1"
        assert pd.isna(suppressed.iloc[0]["value"]) or suppressed.iloc[0]["value"] == ""
        kept = result[result["school_id"] == "165170"].iloc[0]
        assert float(kept["value"]) == 37.8
        assert kept["suppressed"] == "N"

    def test_percent_parsing_is_dtype_agnostic(self):
        """pandas 3 StringDtype and pandas 2 object follow the same rules.

        ``read_csv(..., dtype=str)`` returns StringDtype on pandas 3 and
        object on pandas 2.3.3. ``N/A`` and ``#N/A`` stay missing. ``0``
        stays 0. A negative stays negative. A comma is stripped.
        ``bool(pd.NA)`` is not used to detect the dtype. No registered
        percent contains a comma.
        """
        frame = pd.DataFrame({
            "title_i_status": pd.Series(
                ["Title 1", "Title 1", "Not Title 1", "Title 1", "Equity Gap", "Equity Gap"],
                dtype="string",
            ),
            "all_students": pd.Series(["7.3", "0", "N/A", "1,234.5", "#N/A", "-6.0"], dtype="string"),
            "suppressed": pd.Series(["N", "N", "N", "N", "N", "N"], dtype="string"),
        })
        assert not self.etl.should_skip_row(frame.iloc[0])
        assert not self.etl.should_skip_row(frame.iloc[1])
        assert self.etl.should_skip_row(frame.iloc[2])
        assert not self.etl.should_skip_row(frame.iloc[3])
        assert self.etl.should_skip_row(frame.iloc[4])
        assert not self.etl.should_skip_row(frame.iloc[5])
        assert self.etl.extract_metrics(frame.iloc[0]) == {
            "students_taught_by_out_of_field_teachers_rate_title_1__all_students": 7.3
        }
        assert self.etl.extract_metrics(frame.iloc[1]) == {
            "students_taught_by_out_of_field_teachers_rate_title_1__all_students": 0.0
        }
        assert self.etl.extract_metrics(frame.iloc[2]) == {}
        assert self.etl.extract_metrics(frame.iloc[3]) == {
            "students_taught_by_out_of_field_teachers_rate_title_1__all_students": 1234.5
        }
        assert self.etl.extract_metrics(frame.iloc[4]) == {}
        assert self.etl.extract_metrics(frame.iloc[5]) == {
            "students_taught_by_out_of_field_teachers_rate_equity_gap__all_students": -6.0
        }

        as_object = frame.astype(object)
        assert self.etl.should_skip_row(as_object.iloc[2])
        assert self.etl.extract_metrics(as_object.iloc[1])[
            "students_taught_by_out_of_field_teachers_rate_title_1__all_students"
        ] == 0.0
        assert self.etl.extract_metrics(as_object.iloc[5])[
            "students_taught_by_out_of_field_teachers_rate_equity_gap__all_students"
        ] == -6.0


def test_registered_sources_are_the_permanent_files():
    """KYRC24, KYRC25, KYRC26, and 2020-2023 are registered. KYRC23 is not."""
    config = yaml.safe_load((ROOT / "config" / "kde_sources.yaml").read_text())
    names = []
    for entry in config["raw_directories"]["students_taught_by_out_of_field_teachers"]:
        names.append(entry["file"] if isinstance(entry, dict) else entry)

    assert names == [
        "KYRC24_OVW_Students_Taught_by_Out_of_Field_Teachers.csv",
        "KYRC25_OVW_Students_Taught_by_Out_of_Field_Teachers.csv",
        "KYRC26_OVW_Students_Taught_by_Out_of_Field_Teachers.csv",
        "students_taught_by_out_of_field_teachers_2023.csv",
        "students_taught_by_out_of_field_teachers_2022.csv",
        "students_taught_by_out_of_field_teachers_2021.csv",
        "students_taught_by_out_of_field_teachers_2020.csv",
    ]
    assert not any("KYRC23" in name for name in names)
    assert not any(name.startswith("Students_Taught_by_Out_of_Field") for name in names)
