"""Tests for the school courses ETL."""
from pathlib import Path

import pandas as pd
import pytest
import yaml

from etl.constants import KPI_COLUMNS
from etl.school_courses import SchoolCoursesETL, clean_numeric_with_commas, transform

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "school_courses"


def _config():
    """Production mappings.yaml entry. This source has no dtype cast."""
    loaded = yaml.safe_load((ROOT / "config" / "mappings.yaml").read_text())
    return loaded["sources"]["school_courses"]


def _run(tmp_path: Path, names_and_bytes: list) -> pd.DataFrame:
    raw_dir = tmp_path / "raw"
    source_dir = raw_dir / "school_courses"
    source_dir.mkdir(parents=True)
    proc_dir = tmp_path / "processed"
    proc_dir.mkdir()
    for name, payload in names_and_bytes:
        (source_dir / name).write_bytes(payload)
    transform(raw_dir, proc_dir, _config())
    output = proc_dir / "school_courses.csv"
    assert output.exists(), [name for name, _payload in names_and_bytes]
    return pd.read_csv(output, dtype=str, keep_default_na=False)


def _fixture(name: str) -> bytes:
    return (FIXTURE_DIR / name).read_bytes()


def _rows(df: pd.DataFrame, year: str, school_id: str) -> pd.DataFrame:
    return df[(df["year"] == year) & (df["school_id"] == school_id)]


_OFFERED_METRICS = {
    "courses_offered_count",
    "courses_offered_suppressed_count",
    "courses_offered_count_cte",
    "courses_offered_count_hpe",
    "courses_offered_count_vpa",
    "courses_offered_count_wl",
    "courses_offered_count_other",
}


def _one(df: pd.DataFrame, year: str, school_id: str, metric: str) -> pd.Series:
    """The single aggregate row for this school, year, and metric."""
    rows = _rows(df, year, school_id)
    matched = rows[rows["metric"] == metric]
    assert len(matched) == 1, (year, school_id, metric, len(matched))
    return matched.iloc[0]


def _assert_unique_keys(df: pd.DataFrame) -> None:
    """(year, school_id, student_group, metric) is unique."""
    keys = ["year", "school_id", "student_group", "metric"]
    assert not df.duplicated(keys).any()
    assert len(df) == len(df.drop_duplicates(keys))


class TestSchoolCoursesETL:
    def setup_method(self):
        self.etl = SchoolCoursesETL("school_courses")

    def test_kyrc26_file_is_accepted(self):
        """include_kyrc26 is set, so the 2025-26 file is not skipped."""
        assert self.etl.include_kyrc26 is True
        assert self.etl.accepts_data_file(Path("KYRC26_EDOP_School_Courses_Summary.csv"))
        assert not self.etl.accepts_data_file(Path("KYRC23_EDOP_School_Courses_Summary.csv"))

    def test_kyrc_headers_map_the_enrollment_total(self):
        """KYRC names the total All Students and the grade Grade 8."""
        frame = pd.DataFrame(columns=["All Students", "Grade 8", "State Course Name"])
        renamed = self.etl.normalize_column_names(frame)
        assert "student_count" in renamed.columns
        assert "grade8_count" in renamed.columns
        assert "course_name" in renamed.columns

    def test_comma_count_survives_string_dtype_and_int64(self):
        """pandas 3 StringDtype keeps 1,284 through an Int64 cast."""
        series = pd.Series(["1,284", "0", "*"], dtype="string")
        frame = pd.DataFrame({"student_count": series})
        cleaned = self.etl.standardize_missing_values(frame)
        assert bool(cleaned.loc[0, "_star_student_count"]) is False
        assert bool(cleaned.loc[1, "_star_student_count"]) is False
        assert bool(cleaned.loc[2, "_star_student_count"]) is True
        cast = pd.to_numeric(cleaned["student_count"], errors="coerce").astype("Int64")
        assert int(cast.iloc[0]) == 1284
        assert int(cast.iloc[1]) == 0
        assert pd.isna(cast.iloc[2])

    def test_clean_numeric_with_commas(self):
        series = pd.Series(["1,284", "37", "*", "", "0"], dtype="string")
        result = clean_numeric_with_commas(series)
        assert result.iloc[0] == 1284.0
        assert result.iloc[1] == 37.0
        assert pd.isna(result.iloc[2])
        assert pd.isna(result.iloc[3])
        assert result.iloc[4] == 0.0

    def test_star_is_counted_and_blank_is_not(self, tmp_path):
        """A star counts. A blank total does not. A published 0 counts.

        The school has four course rows: a star, a blank, a published 0,
        and a published 12. The blank is left out, so the count is 3 and
        the suppressed count is 1. Treating the blank as a star would
        make the suppressed count 2. Every count is stored with
        suppressed N. There is no per-course 1/NA row.
        """
        frame = pd.DataFrame({
            "School Year": ["20252026", "20252026", "20252026", "20252026"],
            "District Name": ["Adair County"] * 4,
            "School Name": ["Adair County High School"] * 4,
            "School Code": ["001010"] * 4,
            "State Course Name": ["Welding", "Nursing", "Empty", "Band"],
            "All Students": ["*", "", "0", "12"],
            "Grade 8": ["", "", "", ""],
        })
        raw = tmp_path / "raw" / "school_courses"
        raw.mkdir(parents=True)
        frame.to_csv(raw / "sample.csv", index=False)
        proc = tmp_path / "processed"
        proc.mkdir()
        transform(tmp_path / "raw", proc, _config())
        df = pd.read_csv(proc / "school_courses.csv", dtype=str, keep_default_na=False)
        assert set(df["metric"]) == _OFFERED_METRICS
        assert len(df) == 7
        assert set(df["suppressed"]) == {"N"}
        assert _one(df, "2026", "001010", "courses_offered_count")["value"] == "3.0"
        assert _one(df, "2026", "001010", "courses_offered_suppressed_count")["value"] == "1.0"
        assert _one(df, "2026", "001010", "courses_offered_count_other")["value"] == "3.0"
        for metric in (
            "courses_offered_count_cte",
            "courses_offered_count_hpe",
            "courses_offered_count_vpa",
            "courses_offered_count_wl",
        ):
            assert _one(df, "2026", "001010", metric)["value"] == "0.0"

    def test_blank_total_is_not_counted_as_a_star(self, tmp_path):
        """One star and one blank total. The blank is not a second star.

        courses_offered_count is 1 and courses_offered_suppressed_count
        is 1. Counting the blank as a star makes both 2 and fails this
        test. The blank is not stored as its own row.
        """
        frame = pd.DataFrame({
            "School Year": ["20252026", "20252026"],
            "District Name": ["Adair County", "Adair County"],
            "School Name": ["Adair County High School", "Adair County High School"],
            "School Code": ["001010", "001010"],
            "Course Category": ["CTE", "CTE"],
            "State Course Name": ["Welding", "Empty"],
            "All Students": ["*", ""],
        })
        raw = tmp_path / "raw" / "school_courses"
        raw.mkdir(parents=True)
        frame.to_csv(raw / "blank.csv", index=False)
        proc = tmp_path / "processed"
        proc.mkdir()
        transform(tmp_path / "raw", proc, _config())
        df = pd.read_csv(proc / "school_courses.csv", dtype=str, keep_default_na=False)
        assert len(df) == 7
        assert set(df["suppressed"]) == {"N"}
        assert _one(df, "2026", "001010", "courses_offered_count")["value"] == "1.0"
        assert _one(df, "2026", "001010", "courses_offered_suppressed_count")["value"] == "1.0"
        assert _one(df, "2026", "001010", "courses_offered_count_cte")["value"] == "1.0"
        assert _one(df, "2026", "001010", "courses_offered_count_other")["value"] == "0.0"
        _assert_unique_keys(df)

    def test_algebra_grade_8_follows_the_cell(self, tmp_path):
        """Algebra metrics follow grade 8. A blank grade is not a star."""
        frame = pd.DataFrame({
            "School Year": ["20252026", "20252026", "20252026", "20252026"],
            "District Name": ["Adair County"] * 4,
            "School Name": ["Adair County Middle School"] * 4,
            "School Code": ["001020"] * 4,
            "State Course Name": ["Algebra I", "Algebra 1", "Algebra I", "Welding"],
            "All Students": ["20", "4", "9", "15"],
            "Grade 8": ["11", "0", "*", ""],
        })
        raw = tmp_path / "raw" / "school_courses"
        raw.mkdir(parents=True)
        frame.to_csv(raw / "algebra.csv", index=False)
        proc = tmp_path / "processed"
        proc.mkdir()
        transform(tmp_path / "raw", proc, _config())
        df = pd.read_csv(proc / "school_courses.csv", dtype=str, keep_default_na=False)
        offered = df[df["metric"] == "courses_offered_count"]
        assert len(offered) == 1
        assert offered.iloc[0]["value"] == "4.0"
        assert offered.iloc[0]["suppressed"] == "N"
        assert _one(df, "2026", "001020", "courses_offered_suppressed_count")["value"] == "0.0"
        assert _one(df, "2026", "001020", "courses_offered_count_other")["value"] == "4.0"

        def metric(name):
            rows = df[df["metric"] == name]
            return rows

        counts = metric("algebra_1_grade_8_count")
        access = metric("has_algebra_1_access")
        # Three algebra rows. Welding has a blank grade 8 and emits neither.
        assert len(counts) == 3
        assert len(access) == 3
        published_counts = counts[counts["suppressed"] == "N"]["value"].tolist()
        assert "11.0" in published_counts
        assert "0.0" in published_counts
        assert len(counts[counts["suppressed"] == "Y"]) == 1
        assert counts[counts["suppressed"] == "Y"].iloc[0]["value"] == ""
        access_values = access[access["suppressed"] == "N"]["value"].tolist()
        assert "1.0" in access_values
        assert "0.0" in access_values

    def test_kyrc23_republish_does_not_duplicate_2023(self, tmp_path):
        """A leftover KYRC23 file is the same school year and is skipped."""
        historical = _fixture("historical_2023_adair.csv")
        kyrc23 = historical  # same rows; the filename is what the guard sees
        df = _run(tmp_path, [
            ("school_courses_summary_2023.csv", historical),
            ("KYRC23_EDOP_School_Courses_Summary.csv", kyrc23),
        ])
        assert len(df) == 7
        assert set(df["year"]) == {"2023"}
        assert set(df["source_file"]) == {"school_courses_summary_2023.csv"}
        assert _one(df, "2023", "001010", "courses_offered_count")["value"] == "2.0"
        assert _one(df, "2023", "001010", "courses_offered_suppressed_count")["value"] == "1.0"
        _assert_unique_keys(df)

    def test_2019_workbook_has_no_demographic_abbreviation(self, tmp_path):
        """The 2018-19 sheet has no DEMO or DGROUP column.

        The coversheet categories are CTE, HPE, VPA, and WL. There is no
        student-group abbreviation to swap. The shared demographic map is
        not used. student_group stays All Students.
        """
        df = _run(tmp_path, [(
            "SCHOOL_COURSES_SUMMARY_18-19.csv",
            _fixture("historical_2019_adair_elementary.csv"),
        )])
        assert len(df) == 7
        row = _one(df, "2019", "001016", "courses_offered_count")
        assert row["district"] == "Adair County"
        assert row["student_group"] == "All Students"
        assert row["value"] == "1.0"
        assert row["suppressed"] == "N"
        assert _one(df, "2019", "001016", "courses_offered_suppressed_count")["value"] == "0.0"
        assert _one(df, "2019", "001016", "courses_offered_count_hpe")["value"] == "1.0"
        assert _one(df, "2019", "001016", "courses_offered_count_cte")["value"] == "0.0"
        assert set(df["suppressed"]) == {"N"}
        assert "demographic" not in pd.read_csv(
            FIXTURE_DIR / "historical_2019_adair_elementary.csv", nrows=0
        ).columns

    def test_real_rows(self, tmp_path):
        """Real excerpts collapse to one row per school, year, and metric.

        There is no School Code 999000 in any registered file. Bryan Station
        165170 is the school row. Adair County is the district on that
        school's rows; the file has no district total. A grade cell of *
        does not remove a published total from the school count. Two
        courses at one school are one courses_offered_count, not two rows
        of 1. The 2019, 2021, and 2023 values are those counts.
        """
        df = _run(tmp_path, [
            ("KYRC26_EDOP_School_Courses_Summary.csv", _fixture("kyrc26_real_rows.csv")),
            ("KYRC25_EDOP_School_Courses_Summary.csv", _fixture("kyrc25_adair.csv")),
            ("school_courses_summary_2023.csv", _fixture("historical_2023_adair.csv")),
            ("school_courses_summary_2021.csv", _fixture("historical_2021_adair.csv")),
        ])
        for column in KPI_COLUMNS:
            assert column in df.columns

        bryan = _one(df, "2026", "165170", "courses_offered_count")
        assert bryan["value"] == "1.0"
        assert bryan["suppressed"] == "N"
        assert bryan["district"] == "Fayette County"
        assert bryan["school_name"] == "Bryan Station High School"
        assert bryan["student_group"] == "All Students"
        assert bryan["co_op"] == "CKEC"
        assert bryan["co_op_code"] == "901"
        assert bryan["nces_id"] == "210186000388"
        assert bryan["state_school_id"] == "034165170"
        assert _one(df, "2026", "165170", "courses_offered_count_hpe")["value"] == "1.0"
        assert _one(df, "2026", "165170", "courses_offered_suppressed_count")["value"] == "0.0"
        assert "999000" not in set(df["school_id"])

        # Two Adair courses, one starred. Both are CTE. The count is 2.
        assert _one(df, "2026", "001010", "courses_offered_count")["value"] == "2.0"
        assert _one(df, "2026", "001010", "courses_offered_suppressed_count")["value"] == "1.0"
        assert _one(df, "2026", "001010", "courses_offered_count_cte")["value"] == "2.0"
        assert _one(df, "2026", "001010", "courses_offered_count")["district"] == "Adair County"
        assert _one(df, "2026", "001010", "courses_offered_count")["suppressed"] == "N"

        # South Warren total is 1,284. The star is Grade 14, not Grade 12.
        # Grade 12 is the published 4. The grade star does not remove the
        # course from the school count and does not raise the suppressed count.
        source = pd.read_csv(
            FIXTURE_DIR / "kyrc26_real_rows.csv", dtype=str, keep_default_na=False
        )
        warren_source = source[source["School Code"] == "571003"].iloc[0]
        assert warren_source["All Students"] == "1,284"
        assert warren_source["Grade 12"] == "4"
        assert warren_source["Grade 14"] == "*"
        assert _one(df, "2026", "571003", "courses_offered_count")["value"] == "1.0"
        assert _one(df, "2026", "571003", "courses_offered_suppressed_count")["value"] == "0.0"
        assert _one(df, "2026", "571003", "courses_offered_count_vpa")["value"] == "1.0"
        assert _one(df, "2026", "571003", "courses_offered_count")["suppressed"] == "N"

        # Echo Trail is the KYRC26 co-op spelling GLEC, and the total has a comma.
        echo = _one(df, "2026", "275255", "courses_offered_count")
        assert echo["value"] == "1.0"
        assert echo["suppressed"] == "N"
        assert echo["co_op"] == "GLEC"
        assert echo["co_op_code"] == "909"
        assert _one(df, "2026", "275255", "courses_offered_count_cte")["value"] == "1.0"

        # Poage's total is * while kindergarten is 44 and grade 1 is 54.
        # The course is counted, and the star is the suppressed count.
        # The grade cells are not summed into a replacement total.
        poage = _one(df, "2026", "012110", "courses_offered_count")
        assert poage["value"] == "1.0"
        assert poage["suppressed"] == "N"
        assert _one(df, "2026", "012110", "courses_offered_suppressed_count")["value"] == "1.0"
        assert _one(df, "2026", "012110", "courses_offered_count_vpa")["value"] == "1.0"

        # Prior-year anchors. The value is the course count, not 1.
        # KYRC25 NCES id 210003000000 is the published rounded id.
        assert _one(df, "2025", "001010", "courses_offered_count")["value"] == "2.0"
        assert _one(df, "2025", "001010", "courses_offered_suppressed_count")["value"] == "1.0"
        assert _one(df, "2025", "001010", "courses_offered_count")["nces_id"] == "210003000000"
        assert _one(df, "2025", "001010", "courses_offered_count_cte")["value"] == "2.0"

        assert _one(df, "2023", "001010", "courses_offered_count")["value"] == "2.0"
        assert _one(df, "2023", "001010", "courses_offered_suppressed_count")["value"] == "1.0"
        assert _one(df, "2021", "001010", "courses_offered_count")["value"] == "2.0"
        assert _one(df, "2021", "001010", "courses_offered_suppressed_count")["value"] == "1.0"

        assert set(df["suppressed"]) == {"N"}
        assert "algebra_1_grade_8_count" not in set(df["metric"])
        assert "has_algebra_1_access" not in set(df["metric"])
        _assert_unique_keys(df)

    def test_output_has_no_duplicate_keys(self, tmp_path):
        """The collapsed output has one row per school, year, group, and metric."""
        df = _run(tmp_path, [
            ("KYRC26_EDOP_School_Courses_Summary.csv", _fixture("kyrc26_real_rows.csv")),
            ("KYRC25_EDOP_School_Courses_Summary.csv", _fixture("kyrc25_adair.csv")),
            ("school_courses_summary_2023.csv", _fixture("historical_2023_adair.csv")),
            ("school_courses_summary_2021.csv", _fixture("historical_2021_adair.csv")),
            ("SCHOOL_COURSES_SUMMARY_18-19.csv", _fixture("historical_2019_adair_elementary.csv")),
        ])
        _assert_unique_keys(df)
        assert set(df["metric"]) == _OFFERED_METRICS
        # Five KYRC26 schools, plus one Adair school in each earlier year.
        assert len(df) == 7 * (5 + 4)


def test_registered_sources_are_the_permanent_files():
    """2020, 2022, and KYRC24 fill gaps. KYRC23 and the OAA names are absent.

    KYRC24 is registered on the datasets host. The historical host returns
    404 for that name. KYRC23 republishes 2022-23 and is not registered.
    """
    config = yaml.safe_load((ROOT / "config" / "kde_sources.yaml").read_text())
    names = []
    hosts = {}
    for entry in config["raw_directories"]["school_courses"]:
        if isinstance(entry, dict):
            names.append(entry["file"])
            hosts[entry["file"]] = entry.get("url")
        else:
            names.append(entry)
    assert names == [
        "KYRC24_EDOP_School_Courses_Summary.csv",
        "KYRC25_EDOP_School_Courses_Summary.csv",
        "KYRC26_EDOP_School_Courses_Summary.csv",
        "school_courses_summary_2023.csv",
        "school_courses_summary_2022.csv",
        "school_courses_summary_2021.csv",
        "school_courses_summary_2020.csv",
        "SCHOOL_COURSES_SUMMARY_18-19.xlsx",
    ]
    assert hosts["KYRC24_EDOP_School_Courses_Summary.csv"] == "kyrc25"
    assert hosts["KYRC26_EDOP_School_Courses_Summary.csv"] == "kyrc26"
    assert "KYRC23_EDOP_School_Courses_Summary.csv" not in names
    assert "SCHOOL_COURSES_SUMMARY_17-18.xlsx" not in names
    assert not any(name.lower().startswith("school_courses_summary_2024") for name in names)
