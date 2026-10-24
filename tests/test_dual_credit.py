"""
Tests for Dual Credit ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
import yaml
from etl.constants import KPI_COLUMNS
import shutil
from etl.dual_credit import transform, DualCreditETL, clean_numeric_with_commas, subject_slug

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "dual_credit"


class TestDualCreditETL:

    def setup_method(self):
        """Setup test directories and sample data."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)

        # Create sample raw data directory
        self.sample_dir = self.raw_dir / "dual_credit"
        self.sample_dir.mkdir(parents=True)

        # Create ETL instance for testing
        self.etl = DualCreditETL('dual_credit')

    def teardown_method(self):
        """Clean up test directories."""
        shutil.rmtree(self.test_dir)

    def create_participation_data(self):
        """Create sample dual credit participation data."""
        data = pd.DataFrame({
            'School Year': ['20242025', '20242025', '20242025', '20242025'],
            'County Number': ['001', '001', '001', '001'],
            'County Name': ['Adair', 'Adair', 'Adair', 'Adair'],
            'District Number': ['001', '001', '001', '001'],
            'District Name': ['Adair County', 'Adair County', 'Adair County', 'Adair County'],
            'School Number': ['', '', '', ''],
            'School Name': ['All Schools', 'All Schools', 'All Schools', 'All Schools'],
            'School Code': ['999000', '999000', '999000', '999000'],
            'State School Id': ['', '', '', ''],
            'NCES ID': ['', '', '', ''],
            'CO-OP': ['', '', '', ''],
            'CO-OP Code': ['', '', '', ''],
            'School Type': ['', '', '', ''],
            'Demographic': ['All Students', 'Female', 'Male', 'Economically Disadvantaged'],
            'Advanced Course Type': ['Dual Credit', 'Dual Credit', 'Dual Credit', 'Dual Credit'],
            'Subject': ['All Enrollments', 'All Enrollments', 'All Enrollments', 'All Enrollments'],
            'Course Enrollment': ['52,170', '28,500', '23,670', '18,000'],
            'Course Completers': ['48,729', '26,800', '21,929', '16,500'],
            'Students With Qualifying Score': ['46,828', '25,500', '21,328', '15,800'],
        })
        return data

    def create_offered_data(self):
        """Create sample dual credit courses offered data."""
        data = pd.DataFrame({
            'School Year': ['20242025', '20242025', '20242025'],
            'County Number': ['', '', ''],
            'County Name': ['', '', ''],
            'District Number': ['999', '999', '999'],
            'District Name': ['All Districts', 'All Districts', 'All Districts'],
            'School Number': ['', '', ''],
            'School Name': ['All Schools', 'All Schools', 'All Schools'],
            'School Code': ['999000', '999000', '999000'],
            'State School Id': ['', '', ''],
            'NCES ID': ['', '', ''],
            'CO-OP': ['', '', ''],
            'CO-OP Code': ['', '', ''],
            'School Type': ['', '', ''],
            'Advanced Course Type': ['Dual Credit', 'Dual Credit', 'Dual Credit'],
            'Subject': ['All Enrollments', 'Mathematics', 'English/Language Arts'],
            'Course Enrollment': ['136,572', '952', '15,000'],
            'Course Completers': ['124,028', '836', '14,200'],
            'Students With Qualifying Score': ['117,355', '737', '13,800'],
        })
        return data

    def test_normalize_column_names(self):
        """Test column name normalization."""
        df = self.create_participation_data()
        df_normalized = self.etl.normalize_column_names(df)

        assert 'school_year' in df_normalized.columns
        assert 'course_type' in df_normalized.columns
        assert 'course_enrollment' in df_normalized.columns

    def test_clean_numeric_with_commas(self):
        """Test numeric value cleaning with commas."""
        series = pd.Series(['52,170', '100', '*', '', '---'])
        result = clean_numeric_with_commas(series)

        assert result.iloc[0] == 52170.0
        assert result.iloc[1] == 100.0
        assert pd.isna(result.iloc[2])
        assert pd.isna(result.iloc[3])

    def test_extract_metrics(self):
        """Test metric extraction from dual credit row."""
        row = pd.Series({
            'course_enrollment': '1000',
            'course_completers': '950',
            'qualifying_score': '900',
        })

        metrics = self.etl.extract_metrics(row)

        assert 'dual_credit_enrollment' in metrics
        assert metrics['dual_credit_enrollment'] == 1000
        assert metrics['dual_credit_completion_count'] == 950
        assert metrics['dual_credit_qualifying_score_count'] == 900
        assert 'dual_credit_completion_rate' in metrics
        assert metrics['dual_credit_completion_rate'] == 95.0
        assert 'has_dual_credit_program' not in metrics

    def test_extract_metrics_with_qualifying_rate(self):
        """Test qualifying score rate calculation."""
        row = pd.Series({
            'course_enrollment': '100',
            'course_completers': '80',
            'qualifying_score': '72',
        })

        metrics = self.etl.extract_metrics(row)

        # Rate is based on completers, not enrollment
        assert 'dual_credit_qualifying_score_rate' in metrics
        assert metrics['dual_credit_qualifying_score_rate'] == 90.0

    def test_transform_participation_data(self):
        """Test transform with participation data."""
        data = self.create_participation_data()
        data.to_csv(self.sample_dir / "KYRC25_EDOP_Dual_Credit_Participation_and_Performance.csv", index=False)

        config = {
            "derive": {
                "processing_date": "2025-11-24"
            }
        }

        transform(self.raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "dual_credit.csv"
        assert output_file.exists()

        df = pd.read_csv(output_file)

        # Verify KPI format columns
        for col in KPI_COLUMNS:
            assert col in df.columns, f"Required KPI column '{col}' missing"

        # Verify expected metrics are present
        metrics = df['metric'].unique()
        assert 'dual_credit_enrollment' in metrics
        assert 'dual_credit_completion_count' in metrics

        # Verify demographics are present
        student_groups = df['student_group'].unique()
        assert 'All Students' in student_groups
        assert 'Female' in student_groups

    def test_transform_offered_data(self):
        """Test transform with courses offered data."""
        data = self.create_offered_data()
        data.to_csv(self.sample_dir / "KYRC25_EDOP_Dual_Credit_Courses_Offered.csv", index=False)

        config = {
            "derive": {
                "processing_date": "2025-11-24"
            }
        }

        transform(self.raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "dual_credit.csv"
        assert output_file.exists()

        df = pd.read_csv(output_file)

        # Courses offered is a duplicated course count. The subject is part
        # of the metric name, so it does not collide with the unduplicated
        # student metric dual_credit_enrollment.
        assert len(df) > 0
        assert 'dual_credit_course_enrollment_all_enrollments' in df['metric'].values
        assert 'dual_credit_course_enrollment_mathematics' in df['metric'].values
        assert 'dual_credit_course_enrollment_english_language_arts' in df['metric'].values
        assert 'dual_credit_enrollment' not in df['metric'].values

    def test_suppressed_values(self):
        """Test handling of suppressed values."""
        data = pd.DataFrame({
            'School Year': ['20242025'],
            'County Number': ['001'],
            'County Name': ['Adair'],
            'District Number': ['001'],
            'District Name': ['Adair County'],
            'School Number': [''],
            'School Name': ['All Schools'],
            'School Code': ['001'],
            'State School Id': [''],
            'NCES ID': [''],
            'CO-OP': [''],
            'CO-OP Code': [''],
            'School Type': [''],
            'Demographic': ['All Students'],
            'Advanced Course Type': ['Dual Credit'],
            'Subject': ['All Enrollments'],
            'Course Enrollment': ['*'],
            'Course Completers': ['*'],
            'Students With Qualifying Score': ['*'],
        })
        data.to_csv(self.sample_dir / "test_suppressed.csv", index=False)

        config = {"derive": {"processing_date": "2025-11-24"}}
        transform(self.raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "dual_credit.csv"
        assert output_file.exists()
        df = pd.read_csv(output_file)
        # A fully starred participation row is stored. notes/8: value NA, suppressed Y.
        # Three counts. Rates are omitted because neither component was published.
        # has_dual_credit_program is not emitted.
        assert len(df) == 3
        assert set(df['metric']) == {
            'dual_credit_enrollment',
            'dual_credit_completion_count',
            'dual_credit_qualifying_score_count',
        }
        assert (df['suppressed'] == 'Y').all()
        assert df['value'].isna().all()


class TestDualCreditHelpers:
    """Test helper functions independently."""

    def setup_method(self):
        """Setup ETL instance for testing."""
        self.etl = DualCreditETL('dual_credit')

    def test_get_suppressed_metric_defaults(self):
        """Test suppressed metric defaults."""
        row = pd.Series({})

        defaults = self.etl.get_suppressed_metric_defaults(row)

        assert 'dual_credit_enrollment' in defaults
        assert pd.isna(defaults['dual_credit_enrollment'])
        assert 'dual_credit_completion_count' in defaults
        assert pd.isna(defaults['dual_credit_completion_count'])
        assert 'has_dual_credit_program' not in defaults

    def test_should_skip_row_with_demographic(self):
        """Test row skipping with demographic column."""
        row_with_demo = pd.Series({
            'demographic': 'All Students',
            'course_enrollment': '10',
            'course_completers': '8',
            'qualifying_score': '7',
        })
        row_blank_demo = pd.Series({
            'demographic': pd.NA,
            'course_enrollment': '10',
        })
        row_offered = pd.Series({
            'subject': 'All Enrollments',
            'course_enrollment': '10',
            'course_completers': '8',
            'qualifying_score': '7',
        })

        assert not self.etl.should_skip_row(row_with_demo)
        # A participation row with no demographic is not a courses-offered row.
        assert self.etl.should_skip_row(row_blank_demo)
        # Courses offered has a subject and no demographic column.
        assert not self.etl.should_skip_row(row_offered)


class TestDualCreditKyrc26:
    """KYRC26 acceptance, real rows, stars, and the two-measure split."""

    def setup_method(self):
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        self.sample_dir = self.raw_dir / "dual_credit"
        self.sample_dir.mkdir(parents=True)
        self.etl = DualCreditETL("dual_credit")

    def teardown_method(self):
        shutil.rmtree(self.test_dir)

    def _production_config(self) -> dict:
        config = yaml.safe_load((ROOT / "config" / "mappings.yaml").read_text())
        return config["sources"]["dual_credit"]

    def _copy_fixtures(self) -> None:
        for path in FIXTURE_DIR.glob("*.csv"):
            (self.sample_dir / path.name).write_bytes(path.read_bytes())

    def test_kyrc26_file_is_accepted(self):
        """KYRC26 is in scope. The typo headers from earlier files stay mapped."""
        assert self.etl.include_kyrc26 is True
        assert self.etl.accepts_data_file(Path("KYRC26_EDOP_Dual_Credit_Courses_Offered.csv"))
        assert self.etl.accepts_data_file(Path("KYRC26_EDOP_Dual_Credit_Participation_and_Performance.csv"))
        assert self.etl.accepts_data_file(Path("KYRC23_EDOP_Dual_Credit_Courses_Offered.csv"))
        assert self.etl.accepts_data_file(Path("KYRC24_EDOP_Dual_Credit_Courses_Offered.csv"))
        mappings = self.etl.module_column_mappings
        assert mappings["Course Completers"] == "course_completers"
        assert mappings["Couse Completers"] == "course_completers"
        assert mappings["Couse Completers "] == "course_completers"
        assert subject_slug("Arts & Humanities") == "arts_and_humanities"
        assert subject_slug("English/Language Arts") == "english_language_arts"

    def test_blank_row_is_skipped_in_every_year(self):
        """A blank measure row is not offered. No registered year has one.

        The skip is not limited to one year. A starred cell on the same
        shape is stored.
        """
        blank = pd.DataFrame({
            "School Year": ["20232024", "20252026"],
            "District Name": ["Adair County", "Adair County"],
            "School Name": ["Adair County High School", "Adair County High School"],
            "School Code": ["001010", "001010"],
            "Demographic": ["All Students", "All Students"],
            "Advanced Course Type": ["Dual Credit", "Dual Credit"],
            "Course Enrollment": ["", ""],
            "Course Completers": ["", ""],
            "Students With Qualifying Score": ["", ""],
        })
        starred = pd.DataFrame({
            "School Year": ["20252026"],
            "District Name": ["Adair County"],
            "School Name": ["Adair County High School"],
            "School Code": ["001010"],
            "Demographic": ["Homeless"],
            "Advanced Course Type": ["Dual Credit"],
            "Course Enrollment": ["*"],
            "Course Completers": ["*"],
            "Students With Qualifying Score": ["*"],
        })
        blank.to_csv(self.sample_dir / "blank_rows.csv", index=False)
        starred.to_csv(self.sample_dir / "starred_2026.csv", index=False)
        transform(self.raw_dir, self.proc_dir, self._production_config())
        df = pd.read_csv(self.proc_dir / "dual_credit.csv", dtype=str)
        assert set(df["year"]) == {"2026"}
        assert set(df["student_group"]) == {"Homeless"}
        assert (df["suppressed"] == "Y").all()
        assert (df["value"].fillna("") == "").all()

    def test_published_zero_count_is_kept(self):
        """A published 0 is a count. The registered files have none.

        Dropping zeros would still pass the real-row fixtures. This row
        keeps enrollment, completers, and qualifying at 0, suppressed N,
        on both the participation metrics and a courses-offered subject.
        """
        participation = pd.DataFrame({
            "School Year": ["20252026"],
            "District Name": ["Adair County"],
            "School Name": ["Adair County High School"],
            "School Code": ["001010"],
            "Demographic": ["All Students"],
            "Advanced Course Type": ["Dual Credit"],
            "Course Enrollment": ["0"],
            "Course Completers": ["0"],
            "Students With Qualifying Score": ["0"],
        })
        offered = pd.DataFrame({
            "School Year": ["20252026"],
            "District Name": ["Adair County"],
            "School Name": ["Adair County High School"],
            "School Code": ["001010"],
            "Advanced Course Type": ["Dual Credit"],
            "Subject": ["Mathematics"],
            "Course Enrollment": ["12"],
            "Course Completers": ["0"],
            "Students With Qualifying Score": ["0"],
        })
        participation.to_csv(self.sample_dir / "zero_participation.csv", index=False)
        offered.to_csv(self.sample_dir / "zero_offered.csv", index=False)
        transform(self.raw_dir, self.proc_dir, self._production_config())
        df = pd.read_csv(self.proc_dir / "dual_credit.csv", dtype=str)
        counts = {
            "dual_credit_enrollment": "0",
            "dual_credit_completion_count": "0",
            "dual_credit_qualifying_score_count": "0",
            "dual_credit_course_enrollment_mathematics": "12",
            "dual_credit_course_completion_count_mathematics": "0",
            "dual_credit_course_qualifying_score_count_mathematics": "0",
        }
        for metric, expected in counts.items():
            rows = df[df["metric"] == metric]
            assert len(rows) == 1, metric
            assert rows.iloc[0]["suppressed"] == "N"
            assert float(rows.iloc[0]["value"]) == float(expected)
        # A zero enrollment does not invent a rate, and the program flag is not stored.
        assert "dual_credit_completion_rate" not in set(df["metric"])
        assert "has_dual_credit_program" not in set(df["metric"])

    def test_repeated_school_key_keeps_the_larger_enrollment(self):
        """2023-2026 do not repeat a key. A synthetic pair must not both be stored.

        The larger enrollment is kept. A number beats a star.
        """
        data = pd.DataFrame({
            "School Year": ["20252026", "20252026", "20252026"],
            "District Name": ["Adair County", "Adair County", "Adair County"],
            "School Name": ["Adair County High School", "Adair County High School", "Adair County High School"],
            "School Code": ["001010", "001010", "001011"],
            "Demographic": ["All Students", "All Students", "Male"],
            "Advanced Course Type": ["Dual Credit", "Dual Credit", "Dual Credit"],
            "Course Enrollment": ["10", "40", "*"],
            "Course Completers": ["9", "30", "4"],
            "Students With Qualifying Score": ["8", "20", "4"],
        })
        # Second copy of 001011 is a star on enrollment and a smaller story.
        extra = pd.DataFrame({
            "School Year": ["20252026"],
            "District Name": ["Adair County"],
            "School Name": ["Adair County Middle"],
            "School Code": ["001011"],
            "Demographic": ["Male"],
            "Advanced Course Type": ["Dual Credit"],
            "Course Enrollment": ["12"],
            "Course Completers": ["11"],
            "Students With Qualifying Score": ["10"],
        })
        data = pd.concat([data, extra], ignore_index=True)
        data.to_csv(self.sample_dir / "duplicates.csv", index=False)
        transform(self.raw_dir, self.proc_dir, self._production_config())
        df = pd.read_csv(self.proc_dir / "dual_credit.csv", dtype=str)
        keys = ["year", "school_id", "student_group", "metric"]
        assert df.groupby(keys).size().max() == 1
        kept = df[(df["school_id"] == "001010") & (df["metric"] == "dual_credit_enrollment")]
        assert len(kept) == 1
        assert float(kept.iloc[0]["value"]) == 40.0
        middle = df[(df["school_id"] == "001011") & (df["metric"] == "dual_credit_enrollment")]
        assert len(middle) == 1
        assert float(middle.iloc[0]["value"]) == 12.0
        assert middle.iloc[0]["suppressed"] == "N"

    def test_comma_count_survives_string_dtype_and_int64(self):
        """pandas 3 dtype=str is StringDtype. ``55,553`` has to survive an Int64 cast.

        Production mappings.yaml has no dtype cast for this source. The
        cleaner still strips the comma so a later cast does not drop the
        state total.
        """
        raw = pd.Series(["55,553", "360", "*", ""], dtype="string")
        frame = pd.DataFrame({"course_enrollment": raw, "course_completers": raw, "qualifying_score": raw})
        cleaned = self.etl.standardize_missing_values(frame.copy())
        assert cleaned["course_enrollment"].iloc[0] == "55553"
        assert bool(cleaned["_star_course_enrollment"].iloc[2])
        assert not bool(cleaned["_star_course_enrollment"].iloc[0])
        cast = cleaned["course_enrollment"].astype("Int64")
        assert int(cast.iloc[0]) == 55553
        assert int(cast.iloc[1]) == 360
        assert pd.isna(cast.iloc[2])
        assert pd.isna(cast.iloc[3])

    def test_real_rows_match_the_downloaded_files(self):
        """State, district, school, a full star, and a partial star.

        Participation counts are unduplicated students. Courses-offered
        counts are duplicated course enrollments and use a different
        metric name. A starred cell is value NA and suppressed Y. A blank
        is not in these fixtures. The qualifying rate uses completers as
        the denominator.
        """
        self._copy_fixtures()
        transform(self.raw_dir, self.proc_dir, self._production_config())
        result = pd.read_csv(self.proc_dir / "dual_credit.csv", dtype=str)
        published = result["value"].fillna("").ne("")
        assert (result.loc[published, "suppressed"] == "N").all()
        assert (result.loc[~published, "suppressed"] == "Y").all()
        keys = ["year", "school_id", "student_group", "metric"]
        assert result.groupby(keys).size().max() == 1
        assert "has_dual_credit_program" not in set(result["metric"])

        def cell(year, school_id, group, metric):
            rows = result[
                (result["year"] == year)
                & (result["school_id"] == school_id)
                & (result["student_group"] == group)
                & (result["metric"] == metric)
            ]
            assert len(rows) == 1, f"{year} {school_id} {group} {metric} -> {len(rows)}"
            return rows.iloc[0]

        def suppressed(year, school_id, group, metric):
            row = cell(year, school_id, group, metric)
            assert row["suppressed"] == "Y"
            assert row["value"] == "" or pd.isna(row["value"])
            return row

        state = cell("2026", "999000", "All Students", "dual_credit_enrollment")
        assert float(state["value"]) == 55553.0
        assert state["district"] == "All Districts"
        assert state["school_name"] == "---District Total---"
        assert state["source_file"] == "KYRC26_EDOP_Dual_Credit_Participation_and_Performance.csv"
        assert state["suppressed"] == "N"
        assert float(cell("2026", "999000", "All Students", "dual_credit_completion_count")["value"]) == 52219.0
        assert float(cell("2026", "999000", "All Students", "dual_credit_qualifying_score_count")["value"]) == 50002.0
        assert float(cell("2026", "999000", "All Students", "dual_credit_completion_rate")["value"]) == 94.0
        assert float(cell("2026", "999000", "All Students", "dual_credit_qualifying_score_rate")["value"]) == 95.8

        offered = cell("2026", "999000", "All Students", "dual_credit_course_enrollment_all_enrollments")
        assert float(offered["value"]) == 143848.0
        assert offered["source_file"] == "KYRC26_EDOP_Dual_Credit_Courses_Offered.csv"
        assert float(cell("2026", "999000", "All Students", "dual_credit_course_completion_count_all_enrollments")["value"]) == 132208.0
        assert float(cell("2026", "999000", "All Students", "dual_credit_course_qualifying_score_count_all_enrollments")["value"]) == 124596.0
        assert float(cell("2026", "999000", "All Students", "dual_credit_course_completion_rate_all_enrollments")["value"]) == 91.9
        assert float(cell("2026", "999000", "All Students", "dual_credit_course_qualifying_score_rate_all_enrollments")["value"]) == 94.2
        assert float(cell("2026", "999000", "All Students", "dual_credit_course_enrollment_mathematics")["value"]) == 945.0

        fayette = cell("2026", "165000", "All Students", "dual_credit_enrollment")
        assert float(fayette["value"]) == 2793.0
        assert fayette["district"] == "Fayette County"
        assert fayette["nces_id"] == "2101860"
        assert fayette["co_op"] == "CKEC"
        assert fayette["co_op_code"] == "901"
        assert float(cell("2026", "165000", "All Students", "dual_credit_course_enrollment_all_enrollments")["value"]) == 7684.0

        bryan = cell("2026", "165170", "All Students", "dual_credit_enrollment")
        assert float(bryan["value"]) == 360.0
        assert bryan["school_name"] == "Bryan Station High School"
        assert bryan["nces_id"] == "210186000388"
        assert bryan["state_school_id"] == "034165170"
        assert bryan["school_type"] == "A1"
        assert float(cell("2026", "165170", "All Students", "dual_credit_completion_count")["value"]) == 355.0
        assert float(cell("2026", "165170", "All Students", "dual_credit_qualifying_score_count")["value"]) == 340.0
        assert float(cell("2026", "165170", "All Students", "dual_credit_course_enrollment_all_enrollments")["value"]) == 886.0

        adair = cell("2026", "001010", "All Students", "dual_credit_enrollment")
        assert float(adair["value"]) == 299.0
        assert adair["co_op"] == "GRREC"
        assert adair["co_op_code"] == "902"
        suppressed("2026", "001010", "African American", "dual_credit_enrollment")
        suppressed("2026", "001010", "African American", "dual_credit_completion_count")
        suppressed("2026", "001010", "African American", "dual_credit_qualifying_score_count")

        partial = cell("2026", "011020", "African American", "dual_credit_enrollment")
        assert float(partial["value"]) == 3.0
        assert partial["suppressed"] == "N"
        suppressed("2026", "011020", "African American", "dual_credit_completion_count")
        suppressed("2026", "011020", "African American", "dual_credit_qualifying_score_count")
        rates = result[
            (result["year"] == "2026")
            & (result["school_id"] == "011020")
            & (result["student_group"] == "African American")
            & (result["metric"].str.endswith("_rate"))
        ]
        assert len(rates) == 0

        middle = cell("2026", "165899", "All Students", "dual_credit_completion_count")
        assert float(middle["value"]) == 114.0
        suppressed("2026", "165899", "All Students", "dual_credit_enrollment")
        assert float(cell("2026", "165899", "All Students", "dual_credit_qualifying_score_rate")["value"]) == 99.1
        completion = result[
            (result["year"] == "2026")
            & (result["school_id"] == "165899")
            & (result["metric"] == "dual_credit_completion_rate")
        ]
        assert len(completion) == 0

        scott = suppressed("2026", "291120", "All Students", "dual_credit_course_enrollment_all_enrollments")
        assert scott["school_name"] == "Scott High School"
        assert float(cell("2026", "291120", "All Students", "dual_credit_course_completion_count_all_enrollments")["value"]) == 927.0
        assert float(cell("2026", "291120", "All Students", "dual_credit_course_qualifying_score_count_all_enrollments")["value"]) == 834.0
        assert float(cell("2026", "291120", "All Students", "dual_credit_course_qualifying_score_rate_all_enrollments")["value"]) == 90.0

        eastern = cell("2026", "275007", "All Students", "dual_credit_enrollment")
        assert float(eastern["value"]) == 454.0
        assert eastern["co_op"] == "GLEC"
        assert eastern["co_op_code"] == "909"

        # Prior-year anchors. Published values, not a same-branch diff.
        assert float(cell("2025", "999000", "All Students", "dual_credit_enrollment")["value"]) == 52170.0
        assert float(cell("2025", "999000", "All Students", "dual_credit_completion_count")["value"]) == 48729.0
        assert float(cell("2025", "999000", "All Students", "dual_credit_qualifying_score_count")["value"]) == 46828.0
        assert float(cell("2025", "165000", "All Students", "dual_credit_enrollment")["value"]) == 2315.0
        bryan_2025 = cell("2025", "165170", "All Students", "dual_credit_enrollment")
        assert float(bryan_2025["value"]) == 456.0
        assert bryan_2025["nces_id"] == "210186000000"
        suppressed("2025", "001010", "African American", "dual_credit_enrollment")
        offered_2025 = cell("2025", "999000", "All Students", "dual_credit_course_enrollment_all_enrollments")
        assert float(offered_2025["value"]) == 136572.0
        assert offered_2025["source_file"] == "KYRC25_EDOP_Dual_Credit_Courses_Offered.csv"
        assert float(cell("2025", "999000", "All Students", "dual_credit_course_completion_count_all_enrollments")["value"]) == 124028.0
        assert float(cell("2025", "999000", "All Students", "dual_credit_course_qualifying_score_count_all_enrollments")["value"]) == 117355.0

        assert float(cell("2024", "999000", "All Students", "dual_credit_enrollment")["value"]) == 49134.0
        assert float(cell("2024", "999000", "All Students", "dual_credit_course_enrollment_all_enrollments")["value"]) == 129088.0
        assert float(cell("2024", "999000", "All Students", "dual_credit_course_completion_count_all_enrollments")["value"]) == 116984.0
        foster = cell("2024", "001010", "Foster Care", "dual_credit_enrollment")
        assert float(foster["value"]) == 4.0
        suppressed("2024", "001010", "Foster Care", "dual_credit_completion_count")
        suppressed("2024", "001010", "Foster Care", "dual_credit_qualifying_score_count")

        assert float(cell("2023", "999000", "All Students", "dual_credit_enrollment")["value"]) == 45873.0
        assert float(cell("2023", "999000", "All Students", "dual_credit_course_enrollment_all_enrollments")["value"]) == 117586.0
        assert float(cell("2023", "999000", "All Students", "dual_credit_course_completion_count_all_enrollments")["value"]) == 106041.0
        assert cell("2023", "999000", "All Students", "dual_credit_enrollment")["source_file"].startswith("KYRC23_")


def test_registered_sources_are_the_permanent_files():
    """KYRC23 through KYRC26 are registered. No temporary OAA name is.

    KYRC23 is the 2022-23 extract on the datasets host. It is not a
    republish of KYRC24.
    """
    config = yaml.safe_load((ROOT / "config" / "kde_sources.yaml").read_text())
    names = []
    for entry in config["raw_directories"]["dual_credit"]:
        names.append(entry["file"] if isinstance(entry, dict) else entry)

    assert names == [
        "KYRC23_EDOP_Dual_Credit_Courses_Offered.csv",
        "KYRC23_EDOP_Dual_Credit_Participation_and_Performance.csv",
        "KYRC24_EDOP_Dual_Credit_Courses_Offered.csv",
        "KYRC24_EDOP_Dual_Credit_Participation_and_Performance.csv",
        "KYRC25_EDOP_Dual_Credit_Courses_Offered.csv",
        "KYRC26_EDOP_Dual_Credit_Courses_Offered.csv",
        "KYRC25_EDOP_Dual_Credit_Participation_and_Performance.csv",
        "KYRC26_EDOP_Dual_Credit_Participation_and_Performance.csv",
    ]
    assert not any(name.startswith("Dual_Credit") for name in names)

