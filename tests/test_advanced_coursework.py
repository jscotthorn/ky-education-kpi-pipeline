"""
Tests for Advanced Coursework ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
import yaml
from etl.constants import KPI_COLUMNS
import shutil
from etl.advanced_coursework import transform, AdvancedCourseworkETL, clean_numeric_with_commas

ROOT = Path(__file__).parent.parent
FIXTURES = ROOT / "tests" / "fixtures" / "advanced_coursework"


class TestAdvancedCourseworkETL:

    def setup_method(self):
        """Setup test directories and sample data."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)

        # Create sample raw data directory
        self.sample_dir = self.raw_dir / "advanced_coursework"
        self.sample_dir.mkdir(parents=True)

        # Create ETL instance for testing
        self.etl = AdvancedCourseworkETL('advanced_coursework')

    def teardown_method(self):
        """Clean up test directories."""
        shutil.rmtree(self.test_dir)

    def create_participation_data(self):
        """Create sample participation/performance format data."""
        data = pd.DataFrame({
            'School Year': ['20232024', '20232024', '20232024', '20232024'],
            'County Number': ['001', '001', '001', '001'],
            'County Name': ['Adair', 'Adair', 'Adair', 'Adair'],
            'District Number': ['001', '001', '001', '001'],
            'District Name': ['Adair County', 'Adair County', 'Adair County', 'Adair County'],
            'School Number': ['', '', '', ''],
            'School Name': ['---District Total---', '---District Total---', '---District Total---', '---District Total---'],
            'School Code': ['001', '001', '001', '001'],
            'State School Id': ['', '', '', ''],
            'NCES ID': ['', '', '', ''],
            'CO-OP': ['GRREC', 'GRREC', 'GRREC', 'GRREC'],
            'CO-OP Code': ['902', '902', '902', '902'],
            'School Type': ['', '', '', ''],
            'Demographic': ['All Students', 'All Students', 'Female', 'Female'],
            'Advanced Course Type': ['AP', 'IB', 'AP', 'IB'],
            'Course Enrollment': ['150', '50', '85', '30'],
            'Course Completers': ['140', '48', '80', '28'],
            'Number Tested': ['120', '40', '70', '25'],
            'Earned Qualifying Score': ['80', '35', '45', '20'],
        })
        return data

    def create_historical_data(self):
        """Create sample historical format data (uppercase columns)."""
        data = pd.DataFrame({
            'SCHOOL YEAR': ['20212022', '20212022', '20212022'],
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
            'DEMOGRAPHIC': ['All Students', 'All Students', 'Female'],
            'ADVANCED COURSE TYPE': ['AP', 'IB', 'AP'],
            'COURSE ENROLLMENT': ['100', '20', '55'],
            'COURSE COMPLETERS': ['95', '18', '50'],
            'NUMBER TESTED': ['85', '15', '45'],
            'EARNED QUALIFYING SCORE': ['60', '12', '35'],
        })
        return data

    def test_normalize_column_names(self):
        """Test column name normalization."""
        df = self.create_participation_data()
        df_normalized = self.etl.normalize_column_names(df)

        assert 'school_year' in df_normalized.columns
        assert 'county_name' in df_normalized.columns
        assert 'course_type' in df_normalized.columns
        assert 'course_enrollment' in df_normalized.columns
        assert 'Course Enrollment' not in df_normalized.columns

    def test_clean_numeric_with_commas(self):
        """Test numeric value cleaning with commas."""
        series = pd.Series(['56,374', '100', '*', '', '---'])
        result = clean_numeric_with_commas(series)

        assert result.iloc[0] == 56374.0
        assert result.iloc[1] == 100.0
        assert pd.isna(result.iloc[2])
        assert pd.isna(result.iloc[3])

    def test_extract_metrics_ap(self):
        """Test metric extraction from AP row."""
        row = pd.Series({
            'course_type': 'AP',
            'course_enrollment': '150',
            'course_completers': '140',
            'number_tested': '120',
            'qualifying_score': '80',
        })

        metrics = self.etl.extract_metrics(row)

        assert 'ap_course_enrollment' in metrics
        assert metrics['ap_course_enrollment'] == 150
        assert metrics['ap_completion_count'] == 140
        assert metrics['ap_tested_count'] == 120
        assert metrics['ap_qualifying_score_count'] == 80
        assert 'ap_qualifying_score_rate' in metrics
        assert round(metrics['ap_qualifying_score_rate'], 1) == 66.7

    def test_extract_metrics_ib(self):
        """Test metric extraction from IB row."""
        row = pd.Series({
            'course_type': 'IB',
            'course_enrollment': '50',
            'course_completers': '48',
            'number_tested': '40',
            'qualifying_score': '35',
        })

        metrics = self.etl.extract_metrics(row)

        assert 'ib_course_enrollment' in metrics
        assert metrics['ib_course_enrollment'] == 50
        assert 'ib_qualifying_score_rate' in metrics
        assert metrics['ib_qualifying_score_rate'] == 87.5

    def test_extract_metrics_skips_dual_credit(self):
        """Test that dual credit rows return no metrics."""
        row = pd.Series({
            'course_type': 'Dual Credit',
            'course_enrollment': '100',
        })

        metrics = self.etl.extract_metrics(row)
        assert len(metrics) == 0

    def test_transform_participation_data(self):
        """Test transform with participation format data."""
        data = self.create_participation_data()
        data.to_csv(self.sample_dir / "advanced_courses_participation_and_performance_2024.csv", index=False)

        config = {
            "derive": {
                "processing_date": "2025-11-24"
            }
        }

        transform(self.raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "advanced_coursework.csv"
        assert output_file.exists()

        df = pd.read_csv(output_file)

        # Verify KPI format columns
        for col in KPI_COLUMNS:
            assert col in df.columns, f"Required KPI column '{col}' missing"

        # Verify expected metrics are present
        metrics = df['metric'].unique()
        assert 'ap_course_enrollment' in metrics
        assert 'ib_course_enrollment' in metrics

        # Verify demographics are present
        student_groups = df['student_group'].unique()
        assert 'All Students' in student_groups
        assert 'Female' in student_groups

    def test_transform_historical_data(self):
        """Test transform with historical format data."""
        data = self.create_historical_data()
        data.to_csv(self.sample_dir / "advanced_courses_participation_and_performance_2022.csv", index=False)

        config = {
            "derive": {
                "processing_date": "2025-11-24"
            }
        }

        transform(self.raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "advanced_coursework.csv"
        assert output_file.exists()

        df = pd.read_csv(output_file)

        # Verify metrics
        metrics = df['metric'].unique()
        assert 'ap_course_enrollment' in metrics

        # Verify year extraction
        assert '2022' in df['year'].astype(str).values

    def test_suppressed_values(self):
        """Test handling of suppressed values."""
        data = pd.DataFrame({
            'School Year': ['20232024'],
            'County Number': ['001'],
            'County Name': ['Adair'],
            'District Number': ['001'],
            'District Name': ['Adair County'],
            'School Number': [''],
            'School Name': ['---District Total---'],
            'School Code': ['001'],
            'State School Id': [''],
            'NCES ID': [''],
            'CO-OP': [''],
            'CO-OP Code': [''],
            'School Type': [''],
            'Demographic': ['All Students'],
            'Advanced Course Type': ['AP'],
            'Course Enrollment': ['*'],
            'Course Completers': ['*'],
            'Number Tested': ['*'],
            'Earned Qualifying Score': ['*'],
        })
        data.to_csv(self.sample_dir / "test_suppressed.csv", index=False)

        config = {"derive": {"processing_date": "2025-11-24"}}
        transform(self.raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "advanced_coursework.csv"
        df = pd.read_csv(output_file)

        # A star is stored. The value is missing and suppressed is Y.
        assert len(df) > 0
        assert df['value'].isna().all()
        assert set(df['suppressed']) == {'Y'}
        assert 'ap_course_enrollment' in set(df['metric'])


class TestAdvancedCourseworkHelpers:
    """Test helper functions independently."""

    def setup_method(self):
        """Setup ETL instance for testing."""
        self.etl = AdvancedCourseworkETL('advanced_coursework')

    def test_get_suppressed_metric_defaults_ap(self):
        """Test suppressed metric defaults for AP."""
        row = pd.Series({'course_type': 'AP'})

        defaults = self.etl.get_suppressed_metric_defaults(row)

        assert 'ap_course_enrollment' in defaults
        assert pd.isna(defaults['ap_course_enrollment'])

    def test_get_suppressed_metric_defaults_ib(self):
        """Test suppressed metric defaults for IB."""
        row = pd.Series({'course_type': 'IB'})

        defaults = self.etl.get_suppressed_metric_defaults(row)

        assert 'ib_course_enrollment' in defaults
        assert pd.isna(defaults['ib_course_enrollment'])

    def test_should_skip_row_with_demographic(self):
        """A participation group is kept. An offered row does not need one.

        A participation row with a demographic and a measure is kept. A
        courses-offered row has a subject and no demographic, and a
        missing demographic does not skip it. A row with no measure at
        all is blank and is skipped.
        """
        row_with_demo = pd.Series({
            'demographic': 'All Students',
            'course_type': 'AP',
            'course_enrollment': '10',
        })
        row_no_demo = pd.Series({
            'demographic': pd.NA,
            'course_type': 'AP',
            'subject': 'Mathematics',
            'course_code': '270501',
            'course_enrollment': '10',
        })
        blank = pd.Series({
            'demographic': 'All Students',
            'course_type': 'AP',
        })

        assert not self.etl.should_skip_row(row_with_demo)
        assert not self.etl.should_skip_row(row_no_demo)
        assert self.etl.should_skip_row(blank)


def _run(raw_dir: Path, proc_dir: Path) -> pd.DataFrame:
    transform(raw_dir, proc_dir, {"derive": {"processing_date": "2025-11-24"}})
    output = proc_dir / "advanced_coursework.csv"
    return pd.read_csv(
        output,
        dtype={"school_id": str, "school_code": str, "nces_id": str, "state_school_id": str},
    )


def _cell(df: pd.DataFrame, year: str, school_id: str, group: str, metric: str) -> pd.Series:
    matched = df[
        (df["year"].astype(str) == year)
        & (df["school_id"].astype(str) == school_id)
        & (df["student_group"] == group)
        & (df["metric"] == metric)
    ]
    assert len(matched) == 1, f"{year} {school_id} {group} {metric} matched {len(matched)}"
    return matched.iloc[0]


class TestAdvancedCourseworkKyrc26:
    """KYRC26 acceptance, the DC skip, and the ALL/CAI split."""

    def setup_method(self):
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        sample = self.raw_dir / "advanced_coursework"
        sample.mkdir(parents=True)
        self.sample_dir = sample
        self.etl = AdvancedCourseworkETL("advanced_coursework")

    def teardown_method(self):
        shutil.rmtree(self.test_dir)

    def _copy(self, name: str) -> None:
        target = self.sample_dir / name
        target.write_bytes((FIXTURES / name).read_bytes())

    def test_kyrc26_file_is_accepted(self):
        """KYRC26 is accepted. The repeated overview editions are not.

        KYRC23 overview is the registered 2020-21 collection. The two
        KYRC23 EDOP names, and the KYRC24 and KYRC25 overview names,
        repeat a year this module already loads.
        """
        assert self.etl.include_kyrc26 is True
        assert self.etl.accepts_data_file(Path("KYRC26_EDOP_Advanced_Courses_Participation_and_Performance.csv"))
        assert self.etl.accepts_data_file(Path("KYRC26_OVW_Advanced_Coursework.csv"))
        assert self.etl.accepts_data_file(Path("KYRC23_OVW_Advanced_Coursework.csv"))
        assert not self.etl.accepts_data_file(Path("KYRC23_EDOP_Advanced_Courses_Offered.csv"))
        assert not self.etl.accepts_data_file(
            Path("KYRC23_EDOP_Advanced_Courses_Participation_and_Performance.csv")
        )
        assert not self.etl.accepts_data_file(Path("KYRC24_OVW_Advanced_Coursework.csv"))
        assert not self.etl.accepts_data_file(Path("KYRC25_OVW_Advanced_Coursework.csv"))

    def test_dc_is_skipped_and_all_does_not_share_a_name_with_cai(self):
        """Course type DC is not stored. ALL and CAI use different metrics.

        The 2023 state row is the collision in issue #74: ALL enrollment
        68,238, CAI 203, and DC 45,873 used to share advanced_course_enrollment.
        """
        self._copy("participation_2023_state.csv")
        df = _run(self.raw_dir, self.proc_dir)
        keys = df.groupby(["year", "school_id", "student_group", "metric"]).size()
        assert int(keys.max()) == 1
        assert "dual_credit_enrollment" not in set(df["metric"])
        assert "advanced_course_enrollment" in set(df["metric"])
        all_row = _cell(df, "2023", "999", "All Students", "advanced_course_enrollment")
        assert float(all_row["value"]) == 68238.0
        assert all_row["suppressed"] == "N"
        cai = _cell(df, "2023", "999", "All Students", "cambridge_course_enrollment")
        assert float(cai["value"]) == 203.0
        ap = _cell(df, "2023", "999", "All Students", "ap_course_enrollment")
        assert float(ap["value"]) == 33608.0
        # DC is the whole skipped row, including its starred tested cell.
        dc = df[df["metric"].str.contains("dual_credit")]
        assert dc.empty
        # ALL qualifying includes dual-credit grades, so no rate is derived.
        all_rates = df[df["metric"] == "advanced_qualifying_score_rate"]
        assert all_rates.empty
        # The DC tested star is not written on a Cambridge or AP metric.
        tested = _cell(df, "2023", "999", "All Students", "cambridge_tested_count")
        assert float(tested["value"]) == 141.0

    def test_kyrc26_participation_anchors(self):
        """State, district, school, and a starred group from the KYRC26 file."""
        self._copy("kyrc26_participation.csv")
        df = _run(self.raw_dir, self.proc_dir)
        state = _cell(df, "2026", "999000", "All Students", "ap_course_enrollment")
        assert float(state["value"]) == 37212.0
        assert state["district"] == "All Districts"
        assert state["school_name"] == "---District Total---"
        assert state["suppressed"] == "N"
        rate = _cell(df, "2026", "999000", "All Students", "ap_qualifying_score_rate")
        assert float(rate["value"]) == 65.6
        cambridge = _cell(df, "2026", "999000", "All Students", "cambridge_course_enrollment")
        assert float(cambridge["value"]) == 248.0
        fayette = _cell(df, "2026", "165000", "All Students", "ap_course_enrollment")
        assert float(fayette["value"]) == 3450.0
        assert fayette["nces_id"] == "2101860"
        bryan = _cell(df, "2026", "165170", "All Students", "ap_course_enrollment")
        assert float(bryan["value"]) == 530.0
        assert bryan["co_op"] == "CKEC"
        starred = _cell(df, "2026", "001010", "African American", "ap_course_enrollment")
        assert pd.isna(starred["value"])
        assert starred["suppressed"] == "Y"
        completers = _cell(df, "2026", "001010", "African American", "ap_completion_count")
        assert pd.isna(completers["value"])
        assert completers["suppressed"] == "Y"
        # A fully starred group has no derived rate.
        rates = df[
            (df["school_id"] == "001010")
            & (df["metric"] == "ap_qualifying_score_rate")
        ]
        assert rates.empty

    def test_kyrc26_offered_does_not_use_participation_names(self):
        """Courses offered keep the subject or the course code in the name."""
        self._copy("kyrc26_offered.csv")
        df = _run(self.raw_dir, self.proc_dir)
        keys = df.groupby(["year", "school_id", "student_group", "metric"]).size()
        assert int(keys.max()) == 1
        assert "ap_course_enrollment" not in set(df["metric"])
        total = _cell(df, "2026", "999000", "All Students", "advanced_offered_enrollment_all_enrollments")
        assert float(total["value"]) == 62531.0
        math = _cell(df, "2026", "999000", "All Students", "advanced_offered_enrollment_mathematics")
        assert float(math["value"]) == 7843.0
        ap_total = _cell(df, "2026", "999000", "All Students", "ap_offered_enrollment_all_enrollments")
        assert float(ap_total["value"]) == 60118.0
        course = _cell(df, "2026", "999000", "All Students", "ap_offered_enrollment_270510_ap_precalculus")
        assert float(course["value"]) == 2641.0
        # One AP course row in this fixture, not the All Enrollments total.
        count = _cell(df, "2026", "999000", "All Students", "num_ap_courses_offered")
        assert float(count["value"]) == 1.0
        assert count["suppressed"] == "N"

    def test_kyrc26_overview_keeps_a_published_zero_and_skips_dual_credit(self):
        """A published 0 stays 0. Dual credit overview rows are not loaded."""
        self._copy("kyrc26_overview.csv")
        df = _run(self.raw_dir, self.proc_dir)
        # Collected year 20212022, report-card year 20252026. Stored as 2022.
        assert set(df["year"].astype(str)) == {"2022"}
        total = _cell(df, "2022", "001010", "All Students", "ap_participation_count")
        assert float(total["value"]) == 63.0
        rate = _cell(df, "2022", "001010", "All Students", "ap_participation_rate")
        assert float(rate["value"]) == 8.1
        female = _cell(df, "2022", "001010", "All Students", "ap_participation_rate_female")
        assert float(female["value"]) == 9.1
        zero = _cell(df, "2022", "001010", "American Indian or Alaska Native", "ap_participation_count")
        assert float(zero["value"]) == 0.0
        assert zero["suppressed"] == "N"
        zero_rate = _cell(df, "2022", "001010", "American Indian or Alaska Native", "ap_participation_rate")
        assert float(zero_rate["value"]) == 0.0
        two = _cell(df, "2022", "001010", "Two or More Races", "ap_participation_count")
        assert float(two["value"]) == 1.0
        assert not df["metric"].str.startswith("dual_credit").any()
        assert "999000" not in set(df["school_id"])

    def test_blank_overview_row_is_skipped_and_a_zero_is_kept(self):
        """A blank row is not offered, in either collection year.

        The skip is not limited to one year. A published 0 on the same
        school is a different course type and is stored. The KPI year
        is the collection year, so collected ``20202021`` is 2021.
        """
        self._copy("kyrc25_overview_blank_and_zero.csv")
        blank_2022 = pd.read_csv(FIXTURES / "kyrc25_overview_blank_and_zero.csv", dtype=str)
        blank_2022 = blank_2022.iloc[[0]].copy()
        blank_2022["School Year"] = "20252026"
        blank_2022["Collected School Year"] = "20212022"
        blank_2022.to_csv(self.sample_dir / "overview_blank_2022.csv", index=False)
        df = _run(self.raw_dir, self.proc_dir)
        ib = df[(df["school_id"] == "001010") & (df["metric"].str.startswith("ib_"))]
        assert ib.empty
        assert "2022" not in set(df["year"].astype(str))
        zero = _cell(df, "2021", "001010", "African American", "ap_participation_count")
        assert float(zero["value"]) == 0.0
        assert zero["suppressed"] == "N"
        rate = _cell(df, "2021", "001010", "African American", "ap_participation_rate")
        assert float(rate["value"]) == 0.0
        kept = _cell(df, "2021", "001010", "All Students", "ap_participation_count")
        assert float(kept["value"]) == 61.0

    def test_published_zero_count_is_kept_on_participation(self):
        """A constructed enrollment of 0 is stored. The real files have no 0 in 2020-2023."""
        row = pd.DataFrame({
            "School Year": ["20252026"],
            "District Name": ["Adair County"],
            "School Name": ["Adair County High School"],
            "School Code": ["001010"],
            "Demographic": ["All Students"],
            "Advanced Course Type": ["AP"],
            "Course Enrollment": ["0"],
            "Course Completers": ["0"],
            "Number Tested": ["0"],
            "Earned Qualifying Score": ["0"],
        })
        row.to_csv(self.sample_dir / "zero_participation.csv", index=False)
        df = _run(self.raw_dir, self.proc_dir)
        enrollment = _cell(df, "2026", "001010", "All Students", "ap_course_enrollment")
        assert float(enrollment["value"]) == 0.0
        assert enrollment["suppressed"] == "N"
        # A zero tested count does not produce a rate.
        assert df[df["metric"] == "ap_qualifying_score_rate"].empty

    def test_comma_count_survives_string_dtype_and_int64(self):
        """pandas 3 StringDtype keeps 37,212 through an Int64 cast."""
        series = pd.Series(["37,212", "0", "*"], dtype="string")
        frame = pd.DataFrame({"course_enrollment": series, "course_type": ["AP", "AP", "AP"]})
        cleaned = self.etl.standardize_missing_values(frame)
        assert bool(cleaned.loc[0, "_star_course_enrollment"]) is False
        assert bool(cleaned.loc[2, "_star_course_enrollment"]) is True
        cast = pd.to_numeric(cleaned["course_enrollment"], errors="coerce").astype("Int64")
        assert int(cast.iloc[0]) == 37212
        assert int(cast.iloc[1]) == 0
        assert pd.isna(cast.iloc[2])

    def test_repeated_school_key_keeps_the_larger_enrollment(self):
        """A repeated participation key cannot be written twice."""
        frame = pd.DataFrame({
            "School Year": ["20252026", "20252026"],
            "District Name": ["Adair County", "Adair County"],
            "School Name": ["Adair County High School", "Adair County High School"],
            "School Code": ["001010", "001010"],
            "Demographic": ["All Students", "All Students"],
            "Advanced Course Type": ["AP", "AP"],
            "Course Enrollment": ["10", "40"],
            "Course Completers": ["9", "30"],
            "Number Tested": ["8", "20"],
            "Earned Qualifying Score": ["4", "10"],
        })
        frame.to_csv(self.sample_dir / "repeated.csv", index=False)
        df = _run(self.raw_dir, self.proc_dir)
        enrollment = _cell(df, "2026", "001010", "All Students", "ap_course_enrollment")
        assert float(enrollment["value"]) == 40.0
        assert len(df[df["metric"] == "ap_course_enrollment"]) == 1

    def test_2019_disability_label_is_not_the_consolidated_group(self):
        """ACD's label is the disability count. CSG's label is not.

        The shared map would drop code ACD and call code CSG the
        disability group. This sheet's coversheet says the opposite.
        DEMO_ABBREV is not read.
        """
        frame = pd.DataFrame({
            "SCH_YEAR": ["20182019", "20182019"],
            "DIST_NAME": ["State", "State"],
            "SCH_NAME": ["---State Total---", "---State Total---"],
            "SCH_CD": ["999", "999"],
            "DISAGGGROUP": [
                "Students with Disability with IEP",
                "Consolidated Student Group",
            ],
            "DEMO_ABBREV": ["ACD", "CSG"],
            "COURSE_TYPE": ["AP", "AP"],
            "ENROLLED_CNT": ["1204", "9999"],
            "COMPLETERS_CNT": ["1100", "1"],
            "TEST_TAKERS_CNT": ["800", "1"],
            "EARNED_QUALIFYING_SCORE_CNT": ["400", "1"],
        })
        frame.to_csv(self.sample_dir / "disability_2019.csv", index=False)
        df = _run(self.raw_dir, self.proc_dir)
        disability = _cell(df, "2019", "999", "Students with Disabilities (IEP)", "ap_course_enrollment")
        assert float(disability["value"]) == 1204.0
        consolidated = _cell(df, "2019", "999", "Consolidated Student Group", "ap_course_enrollment")
        assert float(consolidated["value"]) == 9999.0

    def test_kyrc24_trailing_space_header_keeps_the_course(self):
        """The registered KYRC24 offered file has a trailing space on Number Tested."""
        self._copy("kyrc24_offered_state_ap.csv")
        df = _run(self.raw_dir, self.proc_dir)
        enrollment = _cell(
            df, "2024", "999000", "All Students", "ap_offered_enrollment_110701_ap_computer_science_a"
        )
        assert float(enrollment["value"]) == 419.0
        tested = _cell(
            df, "2024", "999000", "All Students", "ap_offered_tested_count_110701_ap_computer_science_a"
        )
        assert float(tested["value"]) == 268.0

    def test_two_course_names_with_one_code_stay_distinct(self):
        """2023 state code 110701 is two courses, 525 and 55.

        The metric carries the course name. Collapsing on the code
        would keep only the larger count.
        """
        frame = pd.DataFrame({
            "School Year": ["20222023", "20222023"],
            "District Name": ["State", "State"],
            "School Name": ["---State Total---", "---State Total---"],
            "School Code": ["999", "999"],
            "State Course Code": ["110701", "110701"],
            "State Course Name": ["AP Computer Science A", "KVHS-AP COMP. SCI"],
            "Advanced Course Type": ["AP", "AP"],
            "Subject": ["Information Technology", "Information Technology"],
            "Course Enrollment": ["525", "55"],
            "Course Completers": ["500", "50"],
            "Number Tested": ["400", "40"],
            "Students With Qualifying Score": ["300", "30"],
        })
        frame.to_csv(self.sample_dir / "two_names.csv", index=False)
        df = _run(self.raw_dir, self.proc_dir)
        keys = df.groupby(["year", "school_id", "student_group", "metric"]).size()
        assert int(keys.max()) == 1
        larger = _cell(df, "2023", "999", "All Students", "ap_offered_enrollment_110701_ap_computer_science_a")
        smaller = _cell(df, "2023", "999", "All Students", "ap_offered_enrollment_110701_kvhs_ap_comp_sci")
        assert float(larger["value"]) == 525.0
        assert float(smaller["value"]) == 55.0
        count = _cell(df, "2023", "999", "All Students", "num_ap_courses_offered")
        assert float(count["value"]) == 2.0

    def test_unrecognized_course_type_is_not_advanced(self):
        """A course type this module does not know is not written as advanced_*.

        HONORS is not AP, IB, Cambridge, ALL, or dual credit. Folding it
        into advanced would store the enrollment 40 on
        advanced_course_enrollment.
        """
        frame = pd.DataFrame({
            "School Year": ["20222023", "20222023"],
            "District Name": ["Adair County", "Adair County"],
            "School Name": ["Adair County High School", "Adair County High School"],
            "School Code": ["001010", "001010"],
            "Demographic": ["All Students", "All Students"],
            "Advanced Course Type": ["HONORS", "AP"],
            "Course Enrollment": ["40", "10"],
            "Course Completers": ["30", "9"],
            "Number Tested": ["20", "8"],
            "Earned Qualifying Score": ["10", "4"],
        })
        frame.to_csv(self.sample_dir / "honors.csv", index=False)
        df = _run(self.raw_dir, self.proc_dir)
        assert df[df["metric"].str.startswith("advanced_")].empty
        assert float(_cell(df, "2023", "001010", "All Students", "ap_course_enrollment")["value"]) == 10.0
        assert 40.0 not in set(pd.to_numeric(df["value"], errors="coerce").dropna())

    def test_2019_state_offered_code_000_is_stored_as_999(self):
        """The 2019 courses-offered state total uses 000. Participation uses 999.

        A district total on the same sheet keeps its own code.
        """
        frame = pd.DataFrame({
            "SCH_YEAR": ["20182019", "20182019"],
            "DIST_NAME": ["State", "Adair County"],
            "SCH_NAME": ["---State Total---", "---District Total---"],
            "SCH_CD": ["000", "001"],
            "COURSE_TYPE": ["AP", "AP"],
            "STATE_COURSE_CODE": ["110701", "110701"],
            "STATE_COURSE_NAME": ["AP Computer Science A", "AP Computer Science A"],
            "SUBJECT": ["Information Technology", "Information Technology"],
            "TOTAL_ENROLLMENT_CNT": ["419", "12"],
            "COMPLETERS_CNT": ["400", "10"],
            "TEST_TAKERS_CNT": ["268", "8"],
            "EARNED_QUALIFYING_SCORE_CNT": ["171", "4"],
        })
        frame.to_csv(self.sample_dir / "offered_2019_state.csv", index=False)
        df = _run(self.raw_dir, self.proc_dir)
        state = _cell(
            df, "2019", "999", "All Students", "ap_offered_enrollment_110701_ap_computer_science_a"
        )
        assert float(state["value"]) == 419.0
        assert state["school_code"] == "999"
        assert "000" not in set(df["school_id"].astype(str))
        district = _cell(
            df, "2019", "001", "All Students", "ap_offered_enrollment_110701_ap_computer_science_a"
        )
        assert float(district["value"]) == 12.0

    def test_2026_ib_qualifying_rate_is_not_stored(self):
        """KYRC26 IB tested equals qualifying. The derived 100.0 is not stored.

        A 2025 IB row still gets a rate. The courses-offered rate is a
        different metric and is still derived. The published KYRC26
        state All Enrollments cells are 240 tested and 172 qualifying.
        """
        self._copy("kyrc26_participation.csv")
        earlier = pd.DataFrame({
            "School Year": ["20242025"],
            "District Name": ["Adair County"],
            "School Name": ["Adair County High School"],
            "School Code": ["001010"],
            "Demographic": ["All Students"],
            "Advanced Course Type": ["IB"],
            "Course Enrollment": ["20"],
            "Course Completers": ["18"],
            "Number Tested": ["10"],
            "Earned Qualifying Score": ["8"],
        })
        earlier.to_csv(self.sample_dir / "ib_2025.csv", index=False)
        offered = pd.DataFrame({
            "School Year": ["20252026"],
            "District Name": ["All Districts"],
            "School Name": ["All Schools"],
            "School Code": ["999000"],
            "State Course Code": ["N/A"],
            "State Course Name": ["International Baccalaureate"],
            "Advanced Course Type": ["IB"],
            "Subject": ["All Enrollments"],
            "Course Enrollment": ["2104"],
            "Course Completers": ["1990"],
            "Number Tested": ["240"],
            "Students With Qualifying Score": ["172"],
        })
        offered.to_csv(self.sample_dir / "ib_offered_2026.csv", index=False)
        df = _run(self.raw_dir, self.proc_dir)
        tested = _cell(df, "2026", "999000", "All Students", "ib_tested_count")
        qualifying = _cell(df, "2026", "999000", "All Students", "ib_qualifying_score_count")
        assert float(tested["value"]) == 42.0
        assert float(qualifying["value"]) == 42.0
        rate_2026 = df[(df["year"].astype(str) == "2026") & (df["metric"] == "ib_qualifying_score_rate")]
        assert rate_2026.empty
        rate_2025 = _cell(df, "2025", "001010", "All Students", "ib_qualifying_score_rate")
        assert float(rate_2025["value"]) == 80.0
        offered_rate = _cell(
            df, "2026", "999000", "All Students", "ib_offered_qualifying_score_rate_all_enrollments"
        )
        assert float(offered_rate["value"]) == round(172 / 240 * 100, 1)

    def test_overview_collection_year_does_not_collide_with_participation(self):
        """2020-21 overview is year 2021 and does not reuse a participation name.

        The report-card year on the KYRC23 row is 2023. The collection
        year is 2021. Participation for that school stays on
        ap_course_enrollment.
        """
        self._copy("kyrc23_overview_collection.csv")
        participation = pd.DataFrame({
            "School Year": ["20202021"],
            "District Name": ["Fayette County"],
            "School Name": ["FCPS Virtual Academy"],
            "School Code": ["165645"],
            "Demographic": ["All Students"],
            "Advanced Course Type": ["AP"],
            "Course Enrollment": ["70"],
            "Course Completers": ["60"],
            "Number Tested": ["40"],
            "Earned Qualifying Score": ["20"],
        })
        participation.to_csv(self.sample_dir / "participation_2021_school.csv", index=False)
        df = _run(self.raw_dir, self.proc_dir)
        keys = df.groupby(["year", "school_id", "student_group", "metric"]).size()
        assert int(keys.max()) == 1
        overview = _cell(df, "2021", "165645", "All Students", "ap_participation_count")
        assert float(overview["value"]) == 66.0
        assert overview["year"] == "2021" or str(overview["year"]) == "2021"
        enrolled = _cell(df, "2021", "165645", "All Students", "ap_course_enrollment")
        assert float(enrolled["value"]) == 70.0
        adair = _cell(df, "2021", "001010", "All Students", "ap_participation_count")
        assert float(adair["value"]) == 61.0
        assert df[df["metric"].str.startswith("dual_credit")].empty
        assert "2023" not in set(df["year"].astype(str))
        school = df[(df["year"].astype(str) == "2021") & (df["school_id"].astype(str) == "165645")]
        overview_names = set(school.loc[school["metric"].str.contains("participation_"), "metric"])
        participation_names = set(school.loc[~school["metric"].str.contains("participation_"), "metric"])
        assert "ap_participation_count" in overview_names
        assert "ap_course_enrollment" in participation_names
        assert overview_names.isdisjoint(participation_names)

    def test_real_rows_match_the_downloaded_files(self):
        """Every committed fixture is a verbatim row from a downloaded file."""
        for name in (
            "kyrc26_participation.csv",
            "kyrc26_offered.csv",
            "kyrc26_overview.csv",
            "kyrc23_overview_collection.csv",
            "participation_2023_state.csv",
            "participation_2020_state.csv",
            "kyrc24_offered_state_ap.csv",
        ):
            self._copy(name)
        df = _run(self.raw_dir, self.proc_dir)
        keys = df.groupby(["year", "school_id", "student_group", "metric"]).size()
        assert int(keys.max()) == 1
        assert float(_cell(df, "2026", "999000", "All Students", "ap_course_enrollment")["value"]) == 37212.0
        assert float(_cell(df, "2026", "999000", "All Students", "ib_course_enrollment")["value"]) == 1006.0
        assert float(_cell(df, "2020", "999", "All Students", "advanced_course_enrollment")["value"]) == 65555.0
        assert float(_cell(df, "2020", "999", "All Students", "cambridge_course_enrollment")["value"]) == 171.0
        assert float(_cell(df, "2020", "999", "All Students", "ap_course_enrollment")["value"]) == 40582.0
        overview = _cell(df, "2022", "001010", "All Students", "ap_participation_count")
        assert float(overview["value"]) == 63.0
        collection = _cell(df, "2021", "165645", "All Students", "ap_participation_count")
        assert float(collection["value"]) == 66.0
        dc_2020 = df[(df["year"].astype(str) == "2020") & (df["metric"].str.startswith("dual_credit"))]
        assert dc_2020.empty


def test_registered_sources_are_the_permanent_files():
    """KYRC23 overview is the 2020-21 edition. KYRC24 and KYRC25 overview are not."""
    config = yaml.safe_load((ROOT / "config" / "kde_sources.yaml").read_text())
    names = []
    for entry in config["raw_directories"]["advanced_coursework"]:
        names.append(entry["file"] if isinstance(entry, dict) else entry)
    assert "KYRC26_EDOP_Advanced_Courses_Participation_and_Performance.csv" in names
    assert "KYRC26_OVW_Advanced_Coursework.csv" in names
    assert "KYRC23_OVW_Advanced_Coursework.csv" in names
    assert "advanced_courses_participation_and_performance_2020.csv" in names
    assert "ADVANCED_COURSES_EXAMS_BY_STUDENT_18-19.xlsx" in names
    assert "KYRC24_OVW_Advanced_Coursework.csv" not in names
    assert "KYRC25_OVW_Advanced_Coursework.csv" not in names
    assert "KYRC23_EDOP_Advanced_Courses_Offered.csv" not in names
    assert "KYRC23_EDOP_Advanced_Courses_Participation_and_Performance.csv" not in names
    assert not any(name.startswith("Advanced_Coursework_") for name in names)
