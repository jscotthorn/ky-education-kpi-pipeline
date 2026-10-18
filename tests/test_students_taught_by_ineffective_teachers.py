"""
Tests for Students Taught by Ineffective Teachers ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
import yaml
from etl.constants import KPI_COLUMNS
import shutil
from etl.students_taught_by_ineffective_teachers import (
    transform, StudentsTaughtByIneffectiveTeachersETL
)

ROOT = Path(__file__).resolve().parents[1]


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


class TestKyrc26OptIn:
    """KYRC26 opt-in. KYRC23 republishes 2022-23 and is skipped."""

    def setup_method(self):
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        self.sample_dir = self.raw_dir / "students_taught_by_ineffective_teachers"
        self.sample_dir.mkdir(parents=True)
        self.etl = StudentsTaughtByIneffectiveTeachersETL(
            "students_taught_by_ineffective_teachers"
        )

    def teardown_method(self):
        shutil.rmtree(self.test_dir)

    def _production_config(self) -> dict:
        config = yaml.safe_load((ROOT / "config" / "mappings.yaml").read_text())
        return config["sources"]["students_taught_by_ineffective_teachers"]

    def test_kyrc26_file_is_accepted(self):
        """The permanent KYRC files and the historical names, including 2020, stay in scope."""
        assert self.etl.include_kyrc26 is True
        assert self.etl.module_column_mappings["Title I Status"] == "title_i_status"
        assert self.etl.module_column_mappings["All Students"] == "all_students"
        assert self.etl.module_column_mappings["Non-White"] == "non_white"
        assert "African American" not in self.etl.module_column_mappings
        assert self.etl.module_column_mappings["% STUDENTS TAUGHT BY INEFFECTIVE TCHRS"] == "all_students"

        accepted = [
            "KYRC26_OVW_Students_Taught_by_Ineffective_Teachers.csv",
            "KYRC25_OVW_Students_Taught_by_Ineffective_Teachers.csv",
            "KYRC24_OVW_Students_Taught_by_Ineffective_Teachers.csv",
            "students_taught_by_ineffective_teachers_2023.csv",
            "students_taught_by_ineffective_teachers_2022.csv",
            "students_taught_by_ineffective_teachers_2021.csv",
            "students_taught_by_ineffective_teachers_2020.csv",
        ]
        for name in accepted:
            (self.sample_dir / name).write_text(
                "School Year,School Code,Title I Status,All Students\n20252026,999000,Title 1,0.5\n"
            )
        (self.sample_dir / "KYRC23_OVW_Students_Taught_by_Ineffective_Teachers.csv").write_text(
            "School Year,School Code,Title I Status,All Students\n20222023,001010,Title 1,0.9\n"
        )

        found = sorted(path.name for path, _sheet in self.etl._find_data_files(self.sample_dir))
        assert found == sorted(accepted)

    def test_kyrc23_republish_is_skipped(self):
        """A leftover KYRC23 file must not write a second copy of 2023.

        The datasets file is school year 20222023. Shared percents match the
        historical 2023 extract. Accepting it would write 2023 twice.
        """
        republish = (
            "School Year,District Name,School Name,School Code,Title I Status,All Students\n"
            "20222023,Adair County,Adair County High School,001010,Title 1,0.9\n"
        )
        (self.sample_dir / "KYRC23_OVW_Students_Taught_by_Ineffective_Teachers.csv").write_text(republish)
        (self.sample_dir / "KYRC26_OVW_Students_Taught_by_Ineffective_Teachers.csv").write_text(
            "School Year,District Name,School Name,School Code,Title I Status,All Students\n"
            "20252026,All Districts,All Schools,999000,Title 1,0.5\n"
        )
        transform(self.raw_dir, self.proc_dir, self._production_config())
        result = pd.read_csv(self.proc_dir / "students_taught_by_ineffective_teachers.csv", dtype=str)
        assert set(result["year"]) == {"2026"}
        assert set(result["source_file"]) == {"KYRC26_OVW_Students_Taught_by_Ineffective_Teachers.csv"}
        assert float(result["value"].iloc[0]) == 0.5
        assert result["school_id"].iloc[0] == "999000"
        assert result["district"].iloc[0] == "All Districts"

    def test_na_row_emits_nothing_and_zero_is_kept(self):
        """N/A and #N/A are not a percent. A published 0 is kept.

        2020-2023 store an insufficient result as the text N/A or #N/A.
        There is no student-count column, so that text is not a zero.
        notes/8: a row that is not suppressed and has an empty value emits
        nothing. A sibling cell of 0 does not fill the N/A cells. This
        Sunshine line puts 0.00 on Non-White, so it is a measured zero on
        that group. The 2020 placeholder, where every group except students
        with disabilities is N/A, is the fixture row in the end-to-end test.
        """
        text = (
            "School Year,District Name,School Name,School Code,Title I Status,All Students,Non-White\n"
            "20222023,Ashland Independent,Ashland Head Start,012012,Not Title 1,N/A,N/A\n"
            "20202021,Anderson County,APEX Academy,011030,Not Title 1,#N/A,#N/A\n"
            "20192020,Harlan Independent,Sunshine School,236021,Not Title 1,N/A,0.00\n"
            "20252026,Adair County,Adair County High School,001010,Title 1,0.0,0.0\n"
            "20242025,Adair County,All Schools,001000,Title 1 - Schoolwide School,0.0,0.0\n"
        )
        (self.sample_dir / "KYRC26_OVW_Students_Taught_by_Ineffective_Teachers.csv").write_text(text)
        transform(self.raw_dir, self.proc_dir, self._production_config())
        result = pd.read_csv(self.proc_dir / "students_taught_by_ineffective_teachers.csv", dtype=str)
        assert "012012" not in set(result["school_id"])
        assert "011030" not in set(result["school_id"])
        assert "001000" not in set(result["school_id"])
        sunshine = result[result["school_id"] == "236021"]
        assert set(sunshine["student_group"]) == {"Non-White"}
        assert float(sunshine["value"].iloc[0]) == 0.0
        assert sunshine["suppressed"].iloc[0] == "N"
        assert "All Students" not in set(sunshine["student_group"])
        adair = result[result["school_id"] == "001010"]
        assert len(adair) == 2
        assert set(adair["value"].astype(float)) == {0.0}
        assert set(adair["suppressed"]) == {"N"}
        assert set(adair["year"]) == {"2026"}

    def test_disability_only_zero_is_kept_after_2020(self):
        """A students-with-disabilities 0 is kept when the year is not 2020.

        The 2020 skip applies only when every other percent is missing.
        The same shape in 2021, and in KYRC26, is a published 0 and is
        stored. Removing the year check drops these rows, so this test
        fails.
        """
        text = (
            "School Year,District Name,School Name,School Code,Title I Status,"
            "All Students,Non-White,White (non-Hispanic),"
            "Students with Disabilities (IEP)\n"
            "20192020,Harlan Independent,Sunshine School,236021,Not Title 1,N/A,N/A,N/A,0.00\n"
            "20202021,Anderson County,APEX Academy,011030,Not Title 1,N/A,N/A,N/A,0.0\n"
            "20252026,Fayette County,Bryan Station High School,165170,Title 1,N/A,N/A,N/A,0.0\n"
        )
        (self.sample_dir / "KYRC26_OVW_Students_Taught_by_Ineffective_Teachers.csv").write_text(text)
        transform(self.raw_dir, self.proc_dir, self._production_config())
        result = pd.read_csv(
            self.proc_dir / "students_taught_by_ineffective_teachers.csv", dtype=str
        )
        assert "236021" not in set(result["school_id"])

        def kept(school_id: str, year: str) -> pd.Series:
            rows = result[
                (result["school_id"] == school_id) & (result["year"] == year)
            ]
            assert len(rows) == 1
            return rows.iloc[0]

        for school_id, year in (("011030", "2021"), ("165170", "2026")):
            row = kept(school_id, year)
            assert row["student_group"] == "Students with Disabilities (IEP)"
            assert float(row["value"]) == 0.0
            assert row["suppressed"] == "N"
            assert row["metric"] == (
                "students_taught_by_ineffective_teachers_rate_not_title_1"
                if year == "2021"
                else "students_taught_by_ineffective_teachers_rate_title_1"
            )

    def test_suppressed_row_is_kept_empty(self):
        """A suppressed Title I row is stored with an empty value.

        These files have no Suppressed column and no starred cell. The
        constructed Y row is the notes/8 shape: the value is empty and the
        row is not dropped.
        """
        text = (
            "School Year,District Name,School Name,School Code,Title I Status,All Students,Suppressed\n"
            "20252026,All Districts,All Schools,999000,Title 1,,Y\n"
            "20252026,Fayette County,Bryan Station High School,165170,Title 1,0.0,N\n"
        )
        (self.sample_dir / "KYRC26_OVW_Students_Taught_by_Ineffective_Teachers.csv").write_text(text)
        transform(self.raw_dir, self.proc_dir, self._production_config())
        result = pd.read_csv(self.proc_dir / "students_taught_by_ineffective_teachers.csv", dtype=str)
        suppressed = result[result["suppressed"] == "Y"]
        assert len(suppressed) == 1
        assert suppressed.iloc[0]["school_id"] == "999000"
        assert suppressed.iloc[0]["metric"] == "students_taught_by_ineffective_teachers_rate_title_1"
        assert pd.isna(suppressed.iloc[0]["value"]) or suppressed.iloc[0]["value"] == ""
        kept = result[result["school_id"] == "165170"].iloc[0]
        assert float(kept["value"]) == 0.0
        assert kept["suppressed"] == "N"

    def test_percent_parsing_is_dtype_agnostic(self):
        """pandas 3 StringDtype and pandas 2 object follow the same rules.

        ``read_csv(..., dtype=str)`` returns StringDtype on pandas 3 and
        object on pandas 2.3.3. ``N/A`` and ``#N/A`` stay missing. ``0``
        stays 0. A comma is stripped. ``bool(pd.NA)`` is not used to detect
        the dtype. No registered percent contains a comma.
        """
        frame = pd.DataFrame({
            "title_i_status": pd.Series(
                ["Title 1", "Title 1", "Not Title 1", "Title 1", "Equity Gap"],
                dtype="string",
            ),
            "all_students": pd.Series(["0.5", "0", "N/A", "1,234.5", "#N/A"], dtype="string"),
            "suppressed": pd.Series(["N", "N", "N", "N", "N"], dtype="string"),
        })
        assert not self.etl.should_skip_row(frame.iloc[0])
        assert not self.etl.should_skip_row(frame.iloc[1])
        assert self.etl.should_skip_row(frame.iloc[2])
        assert not self.etl.should_skip_row(frame.iloc[3])
        assert self.etl.should_skip_row(frame.iloc[4])
        assert self.etl.extract_metrics(frame.iloc[0]) == {
            "students_taught_by_ineffective_teachers_rate_title_1__all_students": 0.5
        }
        assert self.etl.extract_metrics(frame.iloc[1]) == {
            "students_taught_by_ineffective_teachers_rate_title_1__all_students": 0.0
        }
        assert self.etl.extract_metrics(frame.iloc[2]) == {}
        assert self.etl.extract_metrics(frame.iloc[3]) == {
            "students_taught_by_ineffective_teachers_rate_title_1__all_students": 1234.5
        }
        assert self.etl.extract_metrics(frame.iloc[4]) == {}

        as_object = frame.astype(object)
        assert self.etl.should_skip_row(as_object.iloc[2])
        assert self.etl.extract_metrics(as_object.iloc[1])[
            "students_taught_by_ineffective_teachers_rate_title_1__all_students"
        ] == 0.0

    def test_negative_equity_gap_cell_is_not_stored(self):
        """A negative cell is omitted. A positive cell on the same row is kept.

        In 2021-2023 the equity gap is Not Title I minus Title I, so dropping
        a negative removes the cells where Title I is worse. In 2020 the gap
        is Title I minus Not Title I on 4 of the 5 rows where the two sides
        differ, so the same check keeps those worse-Title-I cells and drops
        the cells where Title I is lower. The 2020 state All Students gap is
        -1.19 and matches neither difference. Non-White on that row is 0.09,
        which is Title I minus Not Title I, and it is kept. The parser still
        requires ``value >= 0``. Issue #59
        (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/59)
        tracks whether the signed gap is stored. This test records the
        current cells so a later change does not mark them ``suppressed``
        ``Y``.
        """
        text = (
            "SCHOOL YEAR,DISTRICT NAME,SCHOOL NAME,SCHOOL CODE,TITLE I STATUS,"
            "% STUDENTS TAUGHT BY INEFFECTIVE TCHRS,% NON-WHITE STUDENTS TAUGHT BY INEFFECTIVE TCHRS\n"
            "20192020,State,---State Total---,999,Equity Gap,-1.19,0.09\n"
            "20222023,Jefferson County,Shelby Academy,275610,Title 1,0.91%,-19.55%\n"
        )
        (self.sample_dir / "students_taught_by_ineffective_teachers_2020.csv").write_text(text)
        transform(self.raw_dir, self.proc_dir, self._production_config())
        result = pd.read_csv(self.proc_dir / "students_taught_by_ineffective_teachers.csv", dtype=str)
        state = result[result["school_id"] == "999"]
        assert set(state["student_group"]) == {"Non-White"}
        assert state["metric"].iloc[0] == "students_taught_by_ineffective_teachers_rate_equity_gap"
        assert float(state["value"].iloc[0]) == 0.09
        assert state["suppressed"].iloc[0] == "N"
        shelby = result[result["school_id"] == "275610"]
        assert set(shelby["student_group"]) == {"All Students"}
        assert float(shelby["value"].iloc[0]) == 0.91
        assert set(result["suppressed"]) == {"N"}


def test_registered_sources_are_the_permanent_files():
    """KYRC24, KYRC25, KYRC26, and 2020-2023 are registered. KYRC23 is not."""
    config = yaml.safe_load((ROOT / "config" / "kde_sources.yaml").read_text())
    names = []
    for entry in config["raw_directories"]["students_taught_by_ineffective_teachers"]:
        names.append(entry["file"] if isinstance(entry, dict) else entry)

    assert names == [
        "KYRC24_OVW_Students_Taught_by_Ineffective_Teachers.csv",
        "KYRC25_OVW_Students_Taught_by_Ineffective_Teachers.csv",
        "KYRC26_OVW_Students_Taught_by_Ineffective_Teachers.csv",
        "students_taught_by_ineffective_teachers_2023.csv",
        "students_taught_by_ineffective_teachers_2022.csv",
        "students_taught_by_ineffective_teachers_2021.csv",
        "students_taught_by_ineffective_teachers_2020.csv",
    ]
    assert not any("KYRC23" in name for name in names)
    assert not any(name.startswith("Students_Taught_by_Ineffective") for name in names)
