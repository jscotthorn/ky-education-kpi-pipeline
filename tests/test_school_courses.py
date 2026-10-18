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

    def test_star_is_suppressed_and_blank_is_skipped(self, tmp_path):
        """A star is NA with suppressed Y. A blank total is not a row.

        A published 0 is kept. This file has no Suppressed column.
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
        frame.to_csv(tmp_path / "rows.csv", index=False)
        # transform reads data/raw/school_courses, so stage the file there
        raw = tmp_path / "raw" / "school_courses"
        raw.mkdir(parents=True)
        frame.to_csv(raw / "sample.csv", index=False)
        proc = tmp_path / "processed"
        proc.mkdir()
        transform(tmp_path / "raw", proc, _config())
        df = pd.read_csv(proc / "school_courses.csv", dtype=str, keep_default_na=False)
        assert set(df["metric"]) == {"courses_offered_count"}
        assert len(df) == 3
        starred = df[df["suppressed"] == "Y"]
        assert len(starred) == 1
        assert starred.iloc[0]["value"] == ""
        zeros = df[df["value"] == "0.0"]
        assert len(zeros) == 1
        assert zeros.iloc[0]["suppressed"] == "N"
        published = df[df["value"] == "1.0"]
        assert len(published) == 1
        assert published.iloc[0]["suppressed"] == "N"

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
        assert len(offered) == 4
        assert set(offered["value"]) == {"1.0"}

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
        assert len(df) == 2
        assert set(df["year"]) == {"2023"}
        assert set(df["source_file"]) == {"school_courses_summary_2023.csv"}

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
        assert len(df) == 1
        row = df.iloc[0]
        assert row["year"] == "2019"
        assert row["school_id"] == "001016"
        assert row["district"] == "Adair County"
        assert row["student_group"] == "All Students"
        assert row["metric"] == "courses_offered_count"
        assert row["value"] == "1.0"
        assert row["suppressed"] == "N"
        assert "demographic" not in pd.read_csv(
            FIXTURE_DIR / "historical_2019_adair_elementary.csv", nrows=0
        ).columns

    def test_real_rows(self, tmp_path):
        """Real excerpts: a school, a star, a comma, a blank grade, prior years.

        There is no School Code 999000 in any registered file. Bryan Station
        165170 is the school row. Adair County is the district on that
        school's rows; the file has no district total. A grade cell of *
        does not suppress a published total. Two courses at one school are
        two rows of courses_offered_count 1. That repeated key is the open
        design question in journal 104: summing them would change every
        published 1.
        """
        df = _run(tmp_path, [
            ("KYRC26_EDOP_School_Courses_Summary.csv", _fixture("kyrc26_real_rows.csv")),
            ("KYRC25_EDOP_School_Courses_Summary.csv", _fixture("kyrc25_adair.csv")),
            ("school_courses_summary_2023.csv", _fixture("historical_2023_adair.csv")),
            ("school_courses_summary_2021.csv", _fixture("historical_2021_adair.csv")),
        ])
        for column in KPI_COLUMNS:
            assert column in df.columns

        bryan = _rows(df, "2026", "165170")
        assert len(bryan) == 1
        assert bryan.iloc[0]["metric"] == "courses_offered_count"
        assert bryan.iloc[0]["value"] == "1.0"
        assert bryan.iloc[0]["suppressed"] == "N"
        assert bryan.iloc[0]["district"] == "Fayette County"
        assert bryan.iloc[0]["school_name"] == "Bryan Station High School"
        assert bryan.iloc[0]["student_group"] == "All Students"
        assert bryan.iloc[0]["co_op"] == "CKEC"
        assert bryan.iloc[0]["co_op_code"] == "901"
        assert bryan.iloc[0]["nces_id"] == "210186000388"
        assert bryan.iloc[0]["state_school_id"] == "034165170"
        assert "999000" not in set(df["school_id"])

        adair_2026 = _rows(df, "2026", "001010")
        assert len(adair_2026) == 2
        assert len(adair_2026[adair_2026["suppressed"] == "N"]) == 1
        assert adair_2026[adair_2026["suppressed"] == "N"].iloc[0]["value"] == "1.0"
        starred = adair_2026[adair_2026["suppressed"] == "Y"]
        assert len(starred) == 1
        assert starred.iloc[0]["value"] == ""
        assert starred.iloc[0]["district"] == "Adair County"

        # South Warren total is 1,284. A grade-12 star does not suppress it.
        warren = _rows(df, "2026", "571003")
        assert len(warren) == 1
        assert warren.iloc[0]["value"] == "1.0"
        assert warren.iloc[0]["suppressed"] == "N"

        # Echo Trail is the KYRC26 co-op spelling GLEC, and the total has a comma.
        echo = _rows(df, "2026", "275255")
        assert len(echo) == 1
        assert echo.iloc[0]["value"] == "1.0"
        assert echo.iloc[0]["suppressed"] == "N"
        assert echo.iloc[0]["co_op"] == "GLEC"
        assert echo.iloc[0]["co_op_code"] == "909"

        # Poage's total is * while kindergarten is 44 and grade 1 is 54.
        # The starred total is stored as NA / Y. Whether a published grade
        # should count the course as offered is the open design question
        # in journal 104. This assertion records that choice.
        poage = _rows(df, "2026", "012110")
        assert len(poage) == 1
        assert poage.iloc[0]["value"] == ""
        assert poage.iloc[0]["suppressed"] == "Y"

        # Prior-year anchors. The published total becomes 1, not the headcount.
        adair_2025 = _rows(df, "2025", "001010")
        assert len(adair_2025[adair_2025["suppressed"] == "N"]) == 1
        assert adair_2025[adair_2025["suppressed"] == "N"].iloc[0]["value"] == "1.0"
        assert adair_2025[adair_2025["suppressed"] == "N"].iloc[0]["nces_id"] == "210003000000"
        assert len(adair_2025[adair_2025["suppressed"] == "Y"]) == 1

        adair_2023 = _rows(df, "2023", "001010")
        assert len(adair_2023) == 2
        assert adair_2023[adair_2023["suppressed"] == "N"].iloc[0]["value"] == "1.0"
        assert len(adair_2023[adair_2023["suppressed"] == "Y"]) == 1

        adair_2021 = _rows(df, "2021", "001010")
        assert adair_2021[adair_2021["suppressed"] == "N"].iloc[0]["value"] == "1.0"
        assert len(adair_2021[adair_2021["suppressed"] == "Y"]) == 1

        assert "algebra_1_grade_8_count" not in set(df["metric"])
        assert "has_algebra_1_access" not in set(df["metric"])


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
