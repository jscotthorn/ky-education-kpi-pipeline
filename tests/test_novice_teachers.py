"""
Tests for Novice Teachers ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
import yaml
from etl.constants import KPI_COLUMNS
import shutil
from etl.novice_teachers import transform, NoviceTeachersETL

ROOT = Path(__file__).resolve().parents[1]


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


class TestKyrc26NoviceTeachers:
    """KYRC26 opt-in. KYRC23 republishes 2022-23 and is skipped."""

    def setup_method(self):
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        self.sample_dir = self.raw_dir / "novice_teachers"
        self.sample_dir.mkdir(parents=True)
        self.etl = NoviceTeachersETL("novice_teachers")

    def teardown_method(self):
        shutil.rmtree(self.test_dir)

    def _production_config(self):
        config = yaml.safe_load((ROOT / "config" / "mappings.yaml").read_text())
        return config["sources"]["novice_teachers"]

    def test_kyrc26_file_is_accepted(self):
        """The permanent KYRC files, the historical names, and the 2018-19 workbook stay in scope."""
        assert self.etl.include_kyrc26 is True
        assert self.etl.module_column_mappings["Percent Of Teachers With Less Than 1 Year Experience"] == (
            "percent_new_teachers_less_than_1_year"
        )
        assert self.etl.module_column_mappings["PERCENT OF TEACHERS WITH 1-3 YEARS EXPERIENCE"] == (
            "percent_new_teachers_1_to_3_years"
        )
        assert self.etl.module_column_mappings["NEWPCT"] == "percent_new_teachers_less_than_1_year"
        assert "PERCENT INEXPIENCED TEACHERS" not in self.etl.module_column_mappings
        assert "TOTAL NEW TEACHERS" not in self.etl.module_column_mappings

        accepted = [
            "KYRC26_OVW_Inexperienced_Teachers.csv",
            "KYRC26_OVW_Students_Taught_by_Inexperienced_Teachers.csv",
            "KYRC25_OVW_Inexperienced_Teachers.csv",
            "KYRC24_OVW_Inexperienced_Teachers.csv",
            "inexperienced_teachers_2023.csv",
            "inexperienced_teachers_2020.csv",
            "students_taught_by_inexperienced_teachers_2023.csv",
            "NEW_TEACHER_COUNT_18-19.xlsx",
        ]
        for name in accepted:
            (self.sample_dir / name).write_text("School Year,Teacher Count\n20252026,10\n")
        (self.sample_dir / "KYRC23_OVW_Inexperienced_Teachers.csv").write_text(
            "School Year,School Code,Teacher Count\n20222023,001010,10\n"
        )
        (self.sample_dir / "KYRC23_OVW_Students_Taught_by_Inexperienced_Teachers.csv").write_text(
            "School Year,School Code,Title_I_Status\n20222023,001010,Title 1\n"
        )

        found = sorted(path.name for path, _sheet in self.etl._find_data_files(self.sample_dir))
        assert found == sorted(accepted)

    def test_kyrc23_republish_is_skipped(self):
        """A leftover KYRC23 file must not write a second copy of 2023.

        Both KYRC23 files are school year 20222023. School codes match the
        registered historical files after district codes are padded, so
        accepting them would duplicate those KPI rows.
        """
        (self.sample_dir / "KYRC23_OVW_Inexperienced_Teachers.csv").write_text(
            "School Year,District Name,School Name,School Code,Teacher Count,"
            "Percent Of Teachers With 1 3 Years Experience,"
            "Percent Of Teachers With Less Than 1 Year Experience\n"
            "20222023,Adair County,Adair County High School,001010,52,11.50,0.00\n"
        )
        (self.sample_dir / "KYRC23_OVW_Students_Taught_by_Inexperienced_Teachers.csv").write_text(
            "School Year,District Name,School Name,School Code,Title_I_Status,All Students\n"
            "20222023,Adair County,Adair County High School,001010,Title 1,62.7\n"
        )
        (self.sample_dir / "KYRC26_OVW_Inexperienced_Teachers.csv").write_text(
            "School Year,District Name,School Name,School Code,Teacher Count,"
            "Total New Teachers With 1 3 Years Experience,"
            "Percent Of Teachers With 1 3 Years Experience,"
            "Total New Teachers With Less Than 1 Year Experience,"
            "Percent Of Teachers With Less Than 1 Year Experience\n"
            '20252026,All Districts,All Schools,999000,39242,"6,016",15.30,"2,217",5.60\n'
        )
        transform(self.raw_dir, self.proc_dir, self._production_config())
        result = pd.read_csv(self.proc_dir / "novice_teachers.csv", dtype=str)
        assert set(result["year"]) == {"2026"}
        assert set(result["source_file"]) == {"KYRC26_OVW_Inexperienced_Teachers.csv"}
        assert set(result["metric"]) == {
            "novice_teacher_rate_less_than_1_year",
            "novice_teacher_rate_1_to_3_years",
        }
        less = result[result["metric"] == "novice_teacher_rate_less_than_1_year"].iloc[0]
        assert float(less["value"]) == 5.60
        assert less["suppressed"] == "N"
        assert less["district"] == "All Districts"
        assert less["school_id"] == "999000"

    def test_blank_row_emits_nothing(self):
        """An empty measure row is not offered and emits no KPI row.

        notes/8: a row that is not suppressed and has every value empty is
        the absence of a row. The 2022 Kentucky School for the Blind district
        total has a teacher count and blank rates. Kentucky Tech has every
        measure empty. A suppressed row is kept with an empty value.
        """
        text = (
            "School Year,District Name,School Name,School Code,Teacher Count,"
            "Percent Of Teachers With 1 3 Years Experience,"
            "Percent Of Teachers With Less Than 1 Year Experience\n"
            "20252026,Kentucky School for the Blind District,---District Total---,602000,26,,\n"
            "20252026,Kentucky Tech System,---District Total---,777000,,,\n"
            "20252026,Fayette County,Bryan Station High School,165170,133,9.80,3.80\n"
        )
        (self.sample_dir / "KYRC26_OVW_Inexperienced_Teachers.csv").write_text(text)
        equity = (
            "School Year,District Name,School Name,School Code,Title_I_Status,"
            "All Students,Non-White,White,Economically Disadvantaged,"
            "Non-Economically Disadvantaged,Students with Disabilities (IEP),"
            "Student without Disabilities (IEP),English Learner,Non-English Learner\n"
            "20252026,Adair County,All Schools,001000,Title 1,,,,,,,,,\n"
            "20252026,Fayette County,Bryan Station High School,165170,Title 1,80.0,81.0,76.8,80.8,77.9,79.6,80.1,80.2,79.9\n"
        )
        (self.sample_dir / "KYRC26_OVW_Students_Taught_by_Inexperienced_Teachers.csv").write_text(equity)
        transform(self.raw_dir, self.proc_dir, self._production_config())
        result = pd.read_csv(self.proc_dir / "novice_teachers.csv", dtype=str)
        assert "602000" not in set(result["school_id"])
        assert "777000" not in set(result["school_id"])
        assert "001000" not in set(result["school_id"])
        bryan_rates = result[
            (result["school_id"] == "165170")
            & (result["metric"].str.startswith("novice_teacher_rate"))
        ]
        assert set(bryan_rates["value"].astype(float)) == {9.80, 3.80}
        bryan_equity = result[
            (result["school_id"] == "165170")
            & (result["metric"] == "students_taught_by_inexperienced_teachers_rate_title_1")
            & (result["student_group"] == "All Students")
        ]
        assert float(bryan_equity["value"].iloc[0]) == 80.0
        assert set(result["suppressed"]) == {"N"}

    def test_suppressed_row_is_kept(self):
        """A suppressed row is stored with an empty value.

        These files have no Suppressed column. The row is constructed so a
        future ``Y`` is not dropped as not-offered.
        """
        frame = pd.DataFrame({
            "school_year": ["20252026"],
            "district_name": ["Adair County"],
            "school_name": ["Adair County High School"],
            "school_code": ["001010"],
            "teacher_count": [54],
            "percent_new_teachers_1_to_3_years": [pd.NA],
            "percent_new_teachers_less_than_1_year": [pd.NA],
            "suppressed": ["Y"],
        })
        kpi = self.etl.convert_to_kpi_format(frame, "KYRC26_OVW_Inexperienced_Teachers.csv")
        assert len(kpi) == 2
        assert set(kpi["metric"]) == {
            "novice_teacher_rate_less_than_1_year",
            "novice_teacher_rate_1_to_3_years",
        }
        assert set(kpi["suppressed"]) == {"Y"}
        assert kpi["value"].isna().all()
        assert kpi["year"].iloc[0] == "2026"

    def test_comma_removal_string_dtype(self):
        """pandas 3 StringDtype keeps a thousands separator until it is stripped.

        The KYRC26 state new-teacher counts are ``6,016`` and ``2,217``.
        ``read_csv(..., dtype=str)`` returns StringDtype on pandas 3 and
        object on pandas 2.3.3. Both have to survive the production Int64
        cast. ``bool(pd.NA)`` is not used to detect that dtype. The rate
        columns in the registered files have no commas.
        """
        frame = pd.DataFrame({
            "teacher_count": pd.Series(["39,242", "2,919", ""], dtype="string"),
            "new_teachers_1_to_3_years": pd.Series(["6,016", "306", ""], dtype="string"),
            "new_teachers_less_than_1_year": pd.Series(["2,217", "149", ""], dtype="string"),
            "percent_new_teachers_1_to_3_years": pd.Series(["15.30", "10.50", ""], dtype="string"),
            "percent_new_teachers_less_than_1_year": pd.Series(["5.60", "5.10", ""], dtype="string"),
        })
        cleaned = self.etl.standardize_missing_values(frame)
        assert cleaned["teacher_count"].iloc[0] == 39242
        assert cleaned["new_teachers_1_to_3_years"].iloc[0] == 6016
        assert cleaned["new_teachers_less_than_1_year"].iloc[0] == 2217
        assert cleaned["percent_new_teachers_1_to_3_years"].iloc[0] == 15.30
        assert cleaned["percent_new_teachers_less_than_1_year"].iloc[1] == 5.10
        assert pd.isna(cleaned["teacher_count"].iloc[2])
        assert pd.isna(cleaned["percent_new_teachers_less_than_1_year"].iloc[2])

        cleaned["teacher_count"] = pd.to_numeric(cleaned["teacher_count"], errors="coerce").astype("Int64")
        cleaned["new_teachers_1_to_3_years"] = pd.to_numeric(
            cleaned["new_teachers_1_to_3_years"], errors="coerce"
        ).astype("Int64")
        cleaned["new_teachers_less_than_1_year"] = pd.to_numeric(
            cleaned["new_teachers_less_than_1_year"], errors="coerce"
        ).astype("Int64")
        assert not self.etl.should_skip_row(cleaned.iloc[0])
        assert self.etl.should_skip_row(cleaned.iloc[2])
        metrics = self.etl.extract_metrics(cleaned.iloc[0])
        assert metrics["novice_teacher_rate_1_to_3_years"] == 15.30
        assert metrics["novice_teacher_rate_less_than_1_year"] == 5.60

    def test_negative_equity_gap_cell_is_not_a_rate(self):
        """An equity-gap difference below zero is not stored.

        KDE publishes the gap as Title I minus Not Title I, so the cell can
        be negative. The parser still requires ``value >= 0`` for every
        equity cell. That predates KYRC26 and drops real gap cells. This
        test records that deviation so a later change does not silently
        turn the cell into ``suppressed`` ``Y`` or drop the rest of the row.
        A follow-up should decide whether the negative gap is stored.
        Adair County district ``001000`` is negative on every demographic.
        Bardstown Independent ``017000`` is negative only for English learners.
        """
        text = (
            "School Year,District Name,School Name,School Code,Title_I_Status,"
            "All Students,Non-White,White,Economically Disadvantaged,"
            "Non-Economically Disadvantaged,Students with Disabilities (IEP),"
            "Student without Disabilities (IEP),English Learner,Non-English Learner\n"
            "20252026,Adair County,All Schools,001000,Equity Gap,-35.5,-34.1,-35.1,-41.2,-38.2,-43.6,-33.2,-42.2,-35.3\n"
            "20252026,Bardstown Independent,All Schools,017000,Equity Gap,46.6,45.6,47.3,39.1,58.1,33.1,49.8,-3.6,46.8\n"
        )
        (self.sample_dir / "KYRC26_OVW_Students_Taught_by_Inexperienced_Teachers.csv").write_text(text)
        transform(self.raw_dir, self.proc_dir, self._production_config())
        result = pd.read_csv(self.proc_dir / "novice_teachers.csv", dtype=str)
        adair = result[result["school_id"] == "001000"]
        assert adair.empty
        bardstown = result[result["school_id"] == "017000"]
        groups = set(bardstown["student_group"])
        assert "All Students" in groups
        assert "English Learner" not in groups
        assert float(bardstown.loc[bardstown["student_group"] == "All Students", "value"].iloc[0]) == 46.6
        assert set(bardstown["suppressed"]) == {"N"}
        assert set(bardstown["metric"]) == {"students_taught_by_inexperienced_teachers_rate_equity_gap"}


def test_registered_sources_are_the_permanent_files():
    """KYRC24, KYRC25, KYRC26, and the 2018-19 workbook are registered. KYRC23 is not."""
    config = yaml.safe_load((ROOT / "config" / "kde_sources.yaml").read_text())
    names = []
    for entry in config["raw_directories"]["novice_teachers"]:
        names.append(entry["file"] if isinstance(entry, dict) else entry)

    assert names == [
        "KYRC24_OVW_Inexperienced_Teachers.csv",
        "KYRC25_OVW_Inexperienced_Teachers.csv",
        "KYRC26_OVW_Inexperienced_Teachers.csv",
        "inexperienced_teachers_2023.csv",
        "inexperienced_teachers_2022.csv",
        "inexperienced_teachers_2021.csv",
        "inexperienced_teachers_2020.csv",
        "NEW_TEACHER_COUNT_18-19.xlsx",
        "KYRC24_OVW_Students_Taught_by_Inexperienced_Teachers.csv",
        "KYRC25_OVW_Students_Taught_by_Inexperienced_Teachers.csv",
        "KYRC26_OVW_Students_Taught_by_Inexperienced_Teachers.csv",
        "students_taught_by_inexperienced_teachers_2023.csv",
        "students_taught_by_inexperienced_teachers_2022.csv",
        "students_taught_by_inexperienced_teachers_2021.csv",
        "students_taught_by_inexperienced_teachers_2020.csv",
    ]
    assert not any("KYRC23" in name for name in names)
    assert not any(name.startswith("Inexperienced_Teachers_20") for name in names)
