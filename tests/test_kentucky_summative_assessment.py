"""Tests for Kentucky Summative Assessment ETL module"""
import shutil
import tempfile
from pathlib import Path

import pandas as pd
import pytest
import yaml

from etl.constants import KPI_COLUMNS
from etl.kentucky_summative_assessment import (
    KentuckySummativeAssessmentETL,
    _classify_cell,
    transform,
)

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "kentucky_summative_assessment"


class TestKentuckySummativeAssessmentETL:
    def setup_method(self):
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        self.sample_dir = self.raw_dir / "kentucky_summative_assessment"
        self.sample_dir.mkdir(parents=True)
        self.etl = KentuckySummativeAssessmentETL("kentucky_summative_assessment")

    def teardown_method(self):
        shutil.rmtree(self.test_dir)

    def create_sample_2024_data(self):
        return pd.DataFrame({
            "School Year": ["20232024"],
            "County Name": ["ADAIR"],
            "District Name": ["Adair County"],
            "School Name": ["All Schools"],
            "School Code": ["001000"],
            "Level": ["Elementary School"],
            "Subject": ["Mathematics"],
            "Demographic": ["All Students"],
            "Suppressed": ["N"],
            "Novice": [28],
            "Apprentice": [32],
            "Proficient": [31],
            "Distinguished": [10],
            "Proficient / Distinguished": [41],
            "Content Index": [58.7],
        })

    def create_sample_2023_data(self):
        return pd.DataFrame({
            "SCHOOL YEAR": ["20222023"],
            "COUNTY NAME": ["ADAIR"],
            "DISTRICT NAME": ["Adair County"],
            "SCHOOL NAME": ["All Schools"],
            "SCHOOL CODE": ["001000"],
            "GRADE": ["03"],
            "SUBJECT": ["MA"],
            "DEMOGRAPHIC": ["All Students"],
            "SUPPRESSED": ["N"],
            "NOVICE": [28],
            "APPRENTICE": [42],
            "PROFICIENT": [22],
            "DISTINGUISHED": [7],
            "PROFICIENT/DISTINGUISHED": [29],
        })

    def test_normalize_column_names(self):
        df = self.create_sample_2024_data()
        normalized = self.etl.normalize_column_names(df)
        assert "subject" in normalized.columns
        assert "novice" in normalized.columns

    def test_extract_metrics(self):
        df = self.create_sample_2024_data()
        df = self.etl.normalize_column_names(df)
        row = df.iloc[0]
        metrics = self.etl.extract_metrics(row)
        assert metrics["kentucky_summative_assessment_math_novice_rate_elementary"] == 28.0
        assert metrics["kentucky_summative_assessment_math_content_index_score_elementary"] == 58.7

    def test_convert_to_kpi_format(self):
        df = self.create_sample_2024_data()
        df = self.etl.normalize_column_names(df)
        df = self.etl.standardize_missing_values(df)
        df["source_file"] = "test.csv"
        kpi = self.etl.convert_to_kpi_format(df, "test.csv")
        assert not kpi.empty
        assert set(kpi["metric"].unique()) == {
            "kentucky_summative_assessment_math_novice_rate_elementary",
            "kentucky_summative_assessment_math_apprentice_rate_elementary",
            "kentucky_summative_assessment_math_proficient_rate_elementary",
            "kentucky_summative_assessment_math_distinguished_rate_elementary",
            "kentucky_summative_assessment_math_proficient_distinguished_rate_elementary",
            "kentucky_summative_assessment_math_content_index_score_elementary",
        }

    def test_full_transform_pipeline(self):
        df = self.create_sample_2024_data()
        sample_file = self.sample_dir / "sample.csv"
        df.to_csv(sample_file, index=False)
        transform(self.raw_dir, self.proc_dir, {"derive": {}})
        output = self.proc_dir / "kentucky_summative_assessment.csv"
        assert output.exists()
        out_df = pd.read_csv(output)
        assert not out_df.empty

    def test_grade_and_level_metrics(self):
        df_level = self.create_sample_2024_data()
        df_grade = self.create_sample_2023_data()
        df_level.to_csv(self.sample_dir / "level.csv", index=False)
        df_grade.to_csv(self.sample_dir / "grade.csv", index=False)
        transform(self.raw_dir, self.proc_dir, {"derive": {}})
        output = self.proc_dir / "kentucky_summative_assessment.csv"
        assert output.exists()
        out_df = pd.read_csv(output)
        metrics = out_df["metric"].unique()
        assert any(m.endswith("elementary") for m in metrics)
        assert any("grade_3" in m for m in metrics)


def _config():
    """Production mappings.yaml entry. This source has no dtype cast."""
    loaded = yaml.safe_load((ROOT / "config" / "mappings.yaml").read_text())
    return loaded["sources"]["kentucky_summative_assessment"]


def _fixture(name: str) -> bytes:
    return (FIXTURE_DIR / name).read_bytes()


def _run(tmp_path: Path, names: list) -> pd.DataFrame:
    raw_dir = tmp_path / "raw"
    source_dir = raw_dir / "kentucky_summative_assessment"
    source_dir.mkdir(parents=True)
    proc_dir = tmp_path / "processed"
    proc_dir.mkdir()
    for name, payload in names:
        (source_dir / name).write_bytes(payload)
    transform(raw_dir, proc_dir, _config())
    output = proc_dir / "kentucky_summative_assessment.csv"
    text = output.read_text() if output.exists() else ""
    if not text.strip():
        return pd.DataFrame(columns=KPI_COLUMNS)
    return pd.read_csv(output, dtype=str, keep_default_na=False)


def _metric(subject: str, measure: str, period: str) -> str:
    return f"kentucky_summative_assessment_{subject}_{measure}_{period}"


def _one(df: pd.DataFrame, year: str, school_id: str, group: str, metric: str) -> pd.Series:
    matched = df[
        (df["year"] == year)
        & (df["school_id"] == school_id)
        & (df["student_group"] == group)
        & (df["metric"] == metric)
    ]
    assert len(matched) == 1, (year, school_id, group, metric, len(matched))
    return matched.iloc[0]


def _assert_num(df: pd.DataFrame, year: str, school_id: str, group: str, metric: str, value: float) -> None:
    row = _one(df, year, school_id, group, metric)
    assert row["suppressed"] == "N"
    assert float(row["value"]) == pytest.approx(value)


def _assert_star(df: pd.DataFrame, year: str, school_id: str, group: str, metric: str) -> None:
    row = _one(df, year, school_id, group, metric)
    assert row["suppressed"] == "Y"
    assert row["value"] == ""


def _assert_unique(df: pd.DataFrame) -> None:
    if df.empty:
        return
    keys = ["year", "school_id", "student_group", "metric"]
    assert not df.duplicated(keys).any()


class TestKentuckySummativeAssessmentKyrc26:
    def test_kyrc26_file_is_accepted(self):
        """include_kyrc26 is set. A leftover temporary extract is not."""
        etl = KentuckySummativeAssessmentETL("kentucky_summative_assessment")
        assert etl.include_kyrc26 is True
        assert etl.accepts_data_file(Path("KYRC26_ACCT_Kentucky_Summative_Assessment.csv"))
        assert etl.accepts_data_file(Path("KYRC26_ASMT_Kentucky_Summative_Assessment.csv"))
        rejected = [
            "Assessment_Performance_by_Grade_2024.CSV",
            "Assessment_Performance_by_Grade_2025.CSV",
            "Assessment_Performance_by_Grade_2026.CSV",
            "Accountable_Assessment_Performance_2024.CSV",
            "Accountable_Assessment_Performance_2025.CSV",
            "Accountable_Assessment_Performance_2026.CSV",
            "KYRC23_ACCT_Kentucky_Summative_Assessment.csv",
            "KYRC23_ASMT_Kentucky_Summative_Assessment.csv",
        ]
        for name in rejected:
            assert not etl.accepts_data_file(Path(name))
            assert not etl.accepts_data_file(Path(name.lower()))

    def test_registered_sources_are_the_permanent_files(self):
        """KYRC25 replaces the 404 temporary grade file. Level workbooks are new.

        KYRC23 republishes 2022-23. The 2026 temporary names are not registered.
        """
        config = yaml.safe_load((ROOT / "config" / "kde_sources.yaml").read_text())
        names = []
        hosts = {}
        for entry in config["raw_directories"]["kentucky_summative_assessment"]:
            if isinstance(entry, dict):
                names.append(entry["file"])
                hosts[entry["file"]] = entry.get("url")
            else:
                names.append(entry)
        assert names == [
            "KYRC24_ACCT_Kentucky_Summative_Assessment.csv",
            "KYRC25_ACCT_Kentucky_Summative_Assessment.csv",
            "KYRC26_ACCT_Kentucky_Summative_Assessment.csv",
            "KYRC24_ASMT_Kentucky_Summative_Assessment.csv",
            "KYRC25_ASMT_Kentucky_Summative_Assessment.csv",
            "KYRC26_ASMT_Kentucky_Summative_Assessment.csv",
            "accountable_assessment_performance_2023.csv",
            "accountable_assessment_performance_2022.csv",
            "asmt_performance_by_level_2021.csv",
            "asmt_performance_by_grade_2023.csv",
            "asmt_performance_by_grade_2022.csv",
            "ASSESSMENT_PROFICIENCY_GRADE_18-19.xlsx",
            "ASSESSMENT_PROFICIENCY_GRADE_17-18.xlsx",
            "ASSESSMENT_PROFICIENCY_LEVEL_18-19.xlsx",
            "ASSESSMENT_PROFICIENCY_LEVEL_17-18.xlsx",
            "asmt_performance_by_grade_2021.csv",
        ]
        assert hosts["KYRC25_ACCT_Kentucky_Summative_Assessment.csv"] == "kyrc25"
        assert hosts["KYRC26_ACCT_Kentucky_Summative_Assessment.csv"] == "kyrc26"
        assert hosts["KYRC25_ASMT_Kentucky_Summative_Assessment.csv"] == "kyrc25"
        assert hosts["KYRC26_ASMT_Kentucky_Summative_Assessment.csv"] == "kyrc26"
        assert "Assessment_Performance_by_Grade_2025.CSV" not in names
        assert "Assessment_Performance_by_Grade_2026.CSV" not in names
        assert "KYRC23_ACCT_Kentucky_Summative_Assessment.csv" not in names

    def test_real_rows_keep_published_cells(self, tmp_path):
        """State, Adair, a star, a blank, a content index, a zero, and grades.

        Reading proficient/distinguished is 50 / 48 / 49. Combined writing
        is 41 / 47 / 45. ``3rd`` and ``10th`` land on grade_3 and grade_10.
        A grade percent is not also stored as the level rate.
        """
        names = [
            ("KYRC26_ACCT_Kentucky_Summative_Assessment.csv", _fixture("kyrc26_acct_state.csv")),
            ("kyrc26_adair.csv", _fixture("kyrc26_adair.csv")),
            ("kyrc26_suppressed.csv", _fixture("kyrc26_suppressed.csv")),
            ("kyrc26_blank.csv", _fixture("kyrc26_blank.csv")),
            ("kyrc26_content_index.csv", _fixture("kyrc26_content_index.csv")),
            ("kyrc26_zero.csv", _fixture("kyrc26_zero.csv")),
            ("KYRC26_ASMT_Kentucky_Summative_Assessment.csv", _fixture("kyrc26_grades.csv")),
            ("kyrc26_grade_school.csv", _fixture("kyrc26_grade_school.csv")),
            ("KYRC24_ACCT_Kentucky_Summative_Assessment.csv", _fixture("kyrc24_reading_anchor.csv")),
            ("kyrc24_gifted_na.csv", _fixture("kyrc24_gifted_na.csv")),
            ("asmt_performance_by_grade_2023.csv", _fixture("grade_2023_gifted_na.csv")),
            ("ASSESSMENT_PROFICIENCY_GRADE_18-19.csv", _fixture("proficiency_2019_grade3_reading.csv")),
        ]
        df = _run(tmp_path, names)
        assert list(df.columns) == KPI_COLUMNS
        _assert_unique(df)
        assert "N/A" in _fixture("kyrc24_gifted_na.csv").decode()
        assert "N/A" in _fixture("grade_2023_gifted_na.csv").decode()

        state = _one(
            df,
            "2026",
            "999000",
            "All Students",
            _metric("reading", "proficient_distinguished_rate", "elementary"),
        )
        assert state["district"] == "All Districts"
        assert state["school_name"] == "---District Total---"
        assert state["year"] == "2026"
        for level, reading, writing in (
            ("elementary", 50, 41),
            ("middle", 48, 47),
            ("high", 49, 45),
        ):
            _assert_num(df, "2026", "999000", "All Students", _metric("reading", "proficient_distinguished_rate", level), reading)
            _assert_num(df, "2026", "999000", "All Students", _metric("combined_writing", "proficient_distinguished_rate", level), writing)
        _assert_num(df, "2026", "999000", "All Students", _metric("reading", "novice_rate", "elementary"), 23)
        _assert_num(df, "2026", "999000", "All Students", _metric("reading", "content_index_score", "elementary"), 68.7)
        _assert_num(df, "2026", "999000", "All Students", _metric("math", "proficient_distinguished_rate", "elementary"), 43)
        _assert_num(df, "2026", "999000", "All Students", _metric("math", "content_index_score", "high"), 56.9)

        _assert_num(df, "2026", "001000", "All Students", _metric("reading", "proficient_distinguished_rate", "elementary"), 54)
        _assert_num(df, "2026", "001000", "All Students", _metric("reading", "content_index_score", "elementary"), 71.7)
        assert _one(df, "2026", "001000", "All Students", _metric("reading", "proficient_distinguished_rate", "elementary"))["district"] == "Adair County"
        assert _one(df, "2026", "001000", "All Students", _metric("reading", "proficient_distinguished_rate", "elementary"))["school_name"] == "---District Total---"
        school = _one(df, "2026", "001016", "All Students", _metric("reading", "proficient_distinguished_rate", "elementary"))
        assert float(school["value"]) == 54
        assert school["school_name"] == "Adair County Elementary School"
        _assert_num(df, "2026", "001016", "All Students", _metric("reading", "content_index_score", "elementary"), 71.7)

        learner = df[(df["year"] == "2026") & (df["school_id"] == "001016") & (df["student_group"] == "English Learner")]
        assert len(learner) == 5
        assert set(learner["suppressed"]) == {"Y"}
        assert set(learner["value"]) == {""}
        assert not learner["metric"].str.contains("content_index").any()

        assert df[(df["student_group"] == "American Indian or Alaska Native")].empty

        pacific = df[
            (df["year"] == "2026")
            & (df["school_id"] == "999000")
            & (df["student_group"] == "Native Hawaiian or Pacific Islander")
        ]
        assert len(pacific) == 6
        _assert_num(df, "2026", "999000", "Native Hawaiian or Pacific Islander", _metric("science", "content_index_score", "middle"), 39.4)
        _assert_star(df, "2026", "999000", "Native Hawaiian or Pacific Islander", _metric("science", "novice_rate", "middle"))
        _assert_star(df, "2026", "999000", "Native Hawaiian or Pacific Islander", _metric("science", "proficient_distinguished_rate", "middle"))
        _assert_num(
            df,
            "2026",
            "001000",
            "Students with Disabilities (IEP)",
            _metric("combined_writing", "content_index_score", "elementary"),
            23.1,
        )
        _assert_star(
            df,
            "2026",
            "001000",
            "Students with Disabilities (IEP)",
            _metric("combined_writing", "proficient_distinguished_rate", "elementary"),
        )

        _assert_num(
            df,
            "2026",
            "999000",
            "English Learner including Monitored",
            _metric("science", "distinguished_rate", "middle"),
            0,
        )
        _assert_num(
            df,
            "2026",
            "999000",
            "English Learner including Monitored",
            _metric("science", "proficient_distinguished_rate", "middle"),
            5,
        )
        _assert_num(df, "2026", "999000", "English Learner", _metric("combined_writing", "distinguished_rate", "middle"), 0)

        _assert_num(df, "2026", "999000", "All Students", _metric("reading", "proficient_distinguished_rate", "grade_10"), 49)
        _assert_num(df, "2026", "999000", "All Students", _metric("reading", "proficient_distinguished_rate", "grade_3"), 49)
        _assert_num(df, "2026", "999000", "All Students", _metric("math", "proficient_distinguished_rate", "grade_8"), 39)
        assert not df["metric"].str.contains("10th").any()
        assert not df["metric"].str.contains("3rd").any()
        _assert_num(df, "2026", "001016", "All Students", _metric("reading", "proficient_distinguished_rate", "grade_3"), 52)
        school_level = df[
            (df["school_id"] == "001016")
            & (df["student_group"] == "All Students")
            & (df["metric"] == _metric("reading", "proficient_distinguished_rate", "elementary"))
        ]
        assert len(school_level) == 1
        assert float(school_level.iloc[0]["value"]) == 54

        _assert_num(df, "2024", "999000", "All Students", _metric("reading", "proficient_distinguished_rate", "elementary"), 47)
        _assert_num(df, "2024", "999000", "All Students", _metric("reading", "proficient_rate", "elementary"), 30)
        _assert_num(df, "2024", "999000", "All Students", _metric("reading", "distinguished_rate", "elementary"), 18)
        gifted_2024 = df[(df["year"] == "2024") & (df["student_group"] == "Gifted and Talented")]
        assert gifted_2024.empty

        gifted_2023 = df[(df["year"] == "2023") & (df["student_group"] == "Gifted and Talented")]
        assert len(gifted_2023) == 5
        assert set(gifted_2023["suppressed"]) == {"Y"}
        assert set(gifted_2023["value"]) == {""}
        assert set(gifted_2023["school_id"]) == {"001"}
        assert not gifted_2023["metric"].str.contains("elementary").any()
        assert not gifted_2023["metric"].str.contains("content_index").any()

        _assert_num(df, "2019", "999", "All Students", _metric("reading", "proficient_distinguished_rate", "grade_3"), 52.7)
        _assert_num(df, "2019", "999", "Students with Disabilities (IEP)", _metric("reading", "proficient_distinguished_rate", "grade_3"), 34.7)
        _assert_num(df, "2019", "999", "Consolidated Student Group", _metric("reading", "proficient_distinguished_rate", "grade_3"), 37.9)
        _assert_num(df, "2019", "999", "Non-English Learner", _metric("reading", "proficient_distinguished_rate", "grade_3"), 54.3)
        _assert_num(df, "2019", "999", "English Learner", _metric("reading", "proficient_distinguished_rate", "grade_3"), 29.8)
        _assert_num(df, "2019", "999", "Non-Migrant", _metric("reading", "proficient_distinguished_rate", "grade_3"), 52.8)
        assert _one(df, "2019", "999", "All Students", _metric("reading", "proficient_distinguished_rate", "grade_3"))["district"] == "State"
        assert _one(df, "2019", "999", "All Students", _metric("reading", "proficient_distinguished_rate", "grade_3"))["school_name"] == "---State Total---"
        nineteen = df[df["year"] == "2019"]
        assert not nineteen["metric"].str.endswith("elementary").any()
        disability = nineteen[nineteen["student_group"] == "Students with Disabilities (IEP)"]
        assert float(disability.loc[disability["metric"].str.contains("proficient_distinguished"), "value"].iloc[0]) == pytest.approx(34.7)

    def test_leftover_temporary_file_does_not_duplicate_2026(self, tmp_path):
        """A leftover 2026 temporary extract must not write the year twice."""
        payload = _fixture("kyrc26_acct_state.csv")
        alone = _run(tmp_path / "alone", [("KYRC26_ACCT_Kentucky_Summative_Assessment.csv", payload)])
        both = _run(
            tmp_path / "both",
            [
                ("KYRC26_ACCT_Kentucky_Summative_Assessment.csv", payload),
                ("Assessment_Performance_by_Grade_2026.CSV", payload),
                ("KYRC23_ACCT_Kentucky_Summative_Assessment.csv", payload),
            ],
        )
        _assert_unique(both)
        assert len(both) == len(alone)
        assert set(both["source_file"]) == {"KYRC26_ACCT_Kentucky_Summative_Assessment.csv"}

    def test_string_dtype_comma_and_published_combined(self, tmp_path):
        """A thousands comma survives. A percent over 100 does not.

        The combined percent is the published cell. A file with no combined
        column does not store proficient plus distinguished. Content index
        may exceed 100.
        """
        kind, number = _classify_cell("1,023.4", False)
        assert kind == "number"
        assert number == pytest.approx(1023.4)
        assert _classify_cell("*", True) == ("star", None)
        assert _classify_cell("N/A", True)[0] == "blank"
        assert _classify_cell("0", True) == ("number", 0.0)
        assert _classify_cell("101", True)[0] == "blank"
        assert _classify_cell("106.0", False) == ("number", 106.0)

        header = (
            "School Year,District Name,School Name,School Code,Level,Subject,Demographic,"
            "Suppressed,Novice,Apprentice,Proficient,Distinguished,Proficient / Distinguished,Content Index\n"
        )
        row = (
            "20252026,All Districts,All Schools,999000,Elementary School,Reading,All Students,"
            'N,101,20,30,18,47,"1,023.4"\n'
        )
        over = (
            "20252026,All Districts,All Schools,999000,Middle School,Reading,All Students,"
            "N,10,20,30,18,48,106.0\n"
        )
        df = _run(tmp_path / "comma", [("synthetic_comma.csv", (header + row + over).encode())])
        _assert_unique(df)
        assert df[(df["metric"] == _metric("reading", "novice_rate", "elementary"))].empty
        _assert_num(df, "2026", "999000", "All Students", _metric("reading", "proficient_distinguished_rate", "elementary"), 47)
        _assert_num(df, "2026", "999000", "All Students", _metric("reading", "content_index_score", "elementary"), 1023.4)
        _assert_num(df, "2026", "999000", "All Students", _metric("reading", "content_index_score", "middle"), 106.0)

        no_combined = (
            "School Year,District Name,School Name,School Code,Level,Subject,Demographic,"
            "Suppressed,Novice,Apprentice,Proficient,Distinguished\n"
            "20252026,All Districts,All Schools,999000,Elementary School,Reading,All Students,"
            "N,20,30,30,18\n"
        )
        no_sum = _run(tmp_path / "sum", [("synthetic_no_combined.csv", no_combined.encode())])
        combined = _metric("reading", "proficient_distinguished_rate", "elementary")
        assert no_sum[no_sum["metric"] == combined].empty
        _assert_num(no_sum, "2026", "999000", "All Students", _metric("reading", "proficient_rate", "elementary"), 30)
        _assert_num(no_sum, "2026", "999000", "All Students", _metric("reading", "distinguished_rate", "elementary"), 18)

    def test_coop_totals_keep_codes_901_to_909(self, tmp_path):
        """Co-op totals stay on 901-909. Dropping or collapsing them fails.

        2019 publishes ``---COOP Total---`` with school code 000. Central
        Kentucky reading novice is 20.4 and Green River is 19.4. Both on
        school 000 would be one key. 2018 publishes the same totals as
        ``---District Total---`` with a blank district and school code
        901-909. They are stored with the 2019 name and abbreviation.
        Greater Louisville (909) has no abbreviation. A district total and
        Randall K. Cooper High School are not co-op rows.
        """
        published = _run(
            tmp_path / "2019",
            [("ASSESSMENT_PROFICIENCY_LEVEL_18-19.csv", _fixture("proficiency_2019_coop_total.csv"))],
        )
        _assert_unique(published)
        assert not published.empty
        assert set(published["school_id"]) == {"901", "902"}
        assert "000" not in set(published["school_id"])
        ckec = _one(
            published,
            "2019",
            "901",
            "All Students",
            _metric("reading", "novice_rate", "elementary"),
        )
        assert ckec["district"] == "Central Kentucky Educational Cooperative"
        assert ckec["school_name"] == "---COOP Total---"
        assert ckec["co_op"] == "CKEC"
        assert ckec["co_op_code"] == "901"
        assert float(ckec["value"]) == pytest.approx(20.4)
        grrec = _one(
            published,
            "2019",
            "902",
            "All Students",
            _metric("reading", "proficient_distinguished_rate", "elementary"),
        )
        assert grrec["district"] == "Green River Regional Educational Cooperative"
        assert grrec["school_name"] == "---COOP Total---"
        assert grrec["co_op"] == "GRREC"
        assert float(grrec["value"]) == pytest.approx(55.3)

        labelled = _run(
            tmp_path / "2018",
            [("ASSESSMENT_PROFICIENCY_LEVEL_17-18.csv", _fixture("proficiency_2018_coop_total.csv"))],
        )
        _assert_unique(labelled)
        assert set(labelled["school_id"]) == {"901", "909"}
        assert "000" not in set(labelled["school_id"])
        early = _one(
            labelled,
            "2018",
            "901",
            "All Students",
            _metric("reading", "novice_rate", "elementary"),
        )
        assert early["district"] == "Central Kentucky Educational Cooperative"
        assert early["school_name"] == "---COOP Total---"
        assert early["co_op"] == "CKEC"
        assert early["co_op_code"] == "901"
        assert float(early["value"]) == pytest.approx(21.2)
        louisville = _one(
            labelled,
            "2018",
            "909",
            "All Students",
            _metric("reading", "proficient_distinguished_rate", "elementary"),
        )
        assert louisville["district"] == "Greater Louisville Educational Cooperative"
        assert louisville["school_name"] == "---COOP Total---"
        assert louisville["co_op"] == ""
        assert louisville["co_op_code"] == "909"
        assert float(louisville["value"]) == pytest.approx(46.3)
        assert (labelled["school_name"] == "---District Total---").sum() == 0
        assert (labelled["district"] == "").sum() == 0

        header = (
            "SCH_YEAR,DIST_NUMBER,DIST_NAME,SCH_NAME,SCH_CD,LEVEL,SUBJECT,DEMOGRAPHIC,"
            "SUPPRESSED,NOVICE,APPRENTICE,PROFICIENT,DISTINGUISHED,PROFICIENT_DISTINGUISHED\n"
        )
        others = (
            "20172018,001,Adair County,---District Total---,001000,ES,RD,TST,"
            "N,10,20,30,40,70\n"
            "20172018,035,Boone County,Randall K. Cooper High School,035015,HS,RD,TST,"
            "N,11,21,31,41,72\n"
        )
        kept = _run(
            tmp_path / "schools",
            [("ASSESSMENT_PROFICIENCY_LEVEL_17-18.csv", (header + others).encode())],
        )
        district = _one(
            kept,
            "2018",
            "001000",
            "All Students",
            _metric("reading", "novice_rate", "elementary"),
        )
        assert district["district"] == "Adair County"
        assert district["school_name"] == "---District Total---"
        school = _one(
            kept,
            "2018",
            "035015",
            "All Students",
            _metric("reading", "novice_rate", "high"),
        )
        assert school["school_name"] == "Randall K. Cooper High School"
        assert school["district"] == "Boone County"

    def test_openpyxl_reader_keeps_cell_text(self, tmp_path):
        """A star and a percent stay text. The reader does not yield floats."""
        openpyxl = pytest.importorskip("openpyxl")
        path = tmp_path / "tiny.xlsx"
        workbook = openpyxl.Workbook()
        sheet = workbook.active
        sheet.title = "DATA"
        sheet.append(["SCH_CD", "NOVICE", "PROFICIENT_DISTINGUISHED"])
        sheet.append(["001016", "*", 54])
        sheet.append(["001016", None, None])
        workbook.save(path)
        etl = KentuckySummativeAssessmentETL("kentucky_summative_assessment")
        chunks = list(etl._iter_xlsx_chunks(path, None))
        assert len(chunks) == 1
        assert list(chunks[0].columns) == ["SCH_CD", "NOVICE", "PROFICIENT_DISTINGUISHED"]
        assert chunks[0].loc[0, "NOVICE"] == "*"
        assert isinstance(chunks[0].loc[0, "NOVICE"], str)
        assert isinstance(chunks[0].loc[0, "PROFICIENT_DISTINGUISHED"], str)
        assert chunks[0].loc[0, "PROFICIENT_DISTINGUISHED"] == "54"
        assert chunks[0].loc[1, "NOVICE"] == ""
        assert chunks[0].loc[1, "PROFICIENT_DISTINGUISHED"] == ""
