"""End-to-end tests for Kentucky Summative Assessment pipeline"""
import csv
import shutil
import sqlite3
import tempfile
from pathlib import Path

import pandas as pd
import pytest
import yaml

from etl.kentucky_summative_assessment import transform

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw" / "kentucky_summative_assessment"

# KPI rows from the registered files. Grade rows are not copied onto a level.
EXPECTED_ROWS = {
    "2018": 3901200,
    "2019": 4015120,
    "2021": 3268445,
    "2022": 4236689,
    "2023": 4238184,
    "2024": 3399577,
    "2025": 3414868,
    "2026": 3419707,
}
EXPECTED_SUPPRESSED = {
    "2018": 2421415,
    "2019": 3181875,
    "2021": 2672355,
    "2022": 2560975,
    "2023": 2531235,
    "2024": 1545605,
    "2025": 1475510,
    "2026": 1456755,
}


def _ready() -> bool:
    needed = [
        "KYRC24_ACCT_Kentucky_Summative_Assessment.csv",
        "KYRC24_ASMT_Kentucky_Summative_Assessment.csv",
        "KYRC25_ACCT_Kentucky_Summative_Assessment.csv",
        "KYRC25_ASMT_Kentucky_Summative_Assessment.csv",
        "KYRC26_ACCT_Kentucky_Summative_Assessment.csv",
        "KYRC26_ASMT_Kentucky_Summative_Assessment.csv",
        "accountable_assessment_performance_2023.csv",
        "accountable_assessment_performance_2022.csv",
        "asmt_performance_by_level_2021.csv",
        "asmt_performance_by_grade_2023.csv",
        "asmt_performance_by_grade_2022.csv",
        "asmt_performance_by_grade_2021.csv",
        "ASSESSMENT_PROFICIENCY_GRADE_18-19.xlsx",
        "ASSESSMENT_PROFICIENCY_GRADE_17-18.xlsx",
        "ASSESSMENT_PROFICIENCY_LEVEL_18-19.xlsx",
        "ASSESSMENT_PROFICIENCY_LEVEL_17-18.xlsx",
    ]
    return RAW_DIR.exists() and all((RAW_DIR / name).exists() for name in needed)


class TestKentuckySummativeAssessmentEndToEnd:
    def setup_method(self):
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        self.sample_dir = self.raw_dir / "kentucky_summative_assessment"
        self.sample_dir.mkdir(parents=True)

    def teardown_method(self):
        shutil.rmtree(self.test_dir)

    def create_data(self):
        return pd.DataFrame({
            "School Year": ["20232024", "20232024"],
            "County Name": ["ADAIR", "ADAIR"],
            "District Name": ["Adair County", "Adair County"],
            "School Name": ["All Schools", "All Schools"],
            "School Code": ["001000", "001000"],
            "Level": ["Elementary School", "Elementary School"],
            "Subject": ["Mathematics", "Mathematics"],
            "Demographic": ["All Students", "Hispanic"],
            "Suppressed": ["N", "Y"],
            "Novice": [28, "*"],
            "Apprentice": [32, "*"],
            "Proficient": [31, "*"],
            "Distinguished": [10, "*"],
            "Proficient / Distinguished": [41, "*"],
            "Content Index": [58.7, "*"],
        })

    def create_grade_data(self):
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
            "NOVICE": [11],
            "APPRENTICE": [22],
            "PROFICIENT": [33],
            "DISTINGUISHED": [34],
            "PROFICIENT/DISTINGUISHED": [60],
        })

    def test_end_to_end(self):
        df = self.create_data()
        sample_file = self.sample_dir / "sample.csv"
        df.to_csv(sample_file, index=False)
        transform(self.raw_dir, self.proc_dir, {"derive": {}})
        output = self.proc_dir / "kentucky_summative_assessment.csv"
        assert output.exists()
        df_out = pd.read_csv(output, dtype=str, keep_default_na=False)
        assert not df_out.empty
        assert set(df_out["suppressed"].unique()).issubset({"Y", "N"})
        stars = df_out[df_out["student_group"] == "Hispanic or Latino"]
        assert len(stars) == 6
        assert set(stars["suppressed"]) == {"Y"}
        assert set(stars["value"]) == {""}
        published = df_out[df_out["student_group"] == "All Students"]
        assert set(published["suppressed"]) == {"N"}
        novice = published[published["metric"] == "kentucky_summative_assessment_math_novice_rate_elementary"]
        assert len(novice) == 1
        assert float(novice.iloc[0]["value"]) == 28

    def test_grade_and_level(self):
        df_level = self.create_data().head(1)
        df_grade = self.create_grade_data()
        df_level.to_csv(self.sample_dir / "level.csv", index=False)
        df_grade.to_csv(self.sample_dir / "grade.csv", index=False)
        transform(self.raw_dir, self.proc_dir, {"derive": {}})
        output = self.proc_dir / "kentucky_summative_assessment.csv"
        assert output.exists()
        df_out = pd.read_csv(output, dtype=str, keep_default_na=False)
        keys = ["year", "school_id", "student_group", "metric"]
        assert not df_out.duplicated(keys).any()
        metrics = df_out["metric"].unique()
        assert any("grade_3" in m for m in metrics)
        assert any(m.endswith("elementary") for m in metrics)
        grade = df_out[df_out["metric"] == "kentucky_summative_assessment_math_novice_rate_grade_3"]
        level = df_out[df_out["metric"] == "kentucky_summative_assessment_math_novice_rate_elementary"]
        assert len(grade) == 1
        assert len(level) == 1
        assert float(grade.iloc[0]["value"]) == 11
        assert float(level.iloc[0]["value"]) == 28


def _metric(subject: str, measure: str, period: str) -> str:
    return f"kentucky_summative_assessment_{subject}_{measure}_{period}"


@pytest.mark.skipif(not _ready(), reason="Registered kentucky_summative_assessment files are not all present")
def test_registered_files_keep_published_cells(tmp_path):
    """Stream the real files. A grade percent is not stored again as a level.

    State reading proficient/distinguished for 2025-26 is 50 / 48 / 49.
    Combined writing is 41 / 47 / 45. KYRC23 and the temporary extracts
    are not written. The key (year, school, group, metric) is unique.
    """
    config = yaml.safe_load((ROOT / "config" / "mappings.yaml").read_text())
    proc_dir = tmp_path / "processed"
    proc_dir.mkdir()
    transform(ROOT / "data" / "raw", proc_dir, config["sources"]["kentucky_summative_assessment"])
    output = proc_dir / "kentucky_summative_assessment.csv"
    assert output.exists()

    db_path = tmp_path / "keys.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("CREATE TABLE k (y TEXT, s TEXT, g TEXT, m TEXT, PRIMARY KEY (y, s, g, m))")

    counts = {year: 0 for year in EXPECTED_ROWS}
    suppressed = {year: 0 for year in EXPECTED_ROWS}
    found = {}
    wanted = {
        ("2026", "999000", "All Students", _metric("reading", "proficient_distinguished_rate", "elementary")): ("50", "N", "All Districts", "---District Total---"),
        ("2026", "999000", "All Students", _metric("reading", "proficient_distinguished_rate", "middle")): ("48", "N", "All Districts", "---District Total---"),
        ("2026", "999000", "All Students", _metric("reading", "proficient_distinguished_rate", "high")): ("49", "N", "All Districts", "---District Total---"),
        ("2026", "999000", "All Students", _metric("combined_writing", "proficient_distinguished_rate", "elementary")): ("41", "N", "All Districts", "---District Total---"),
        ("2026", "999000", "All Students", _metric("combined_writing", "proficient_distinguished_rate", "middle")): ("47", "N", "All Districts", "---District Total---"),
        ("2026", "999000", "All Students", _metric("combined_writing", "proficient_distinguished_rate", "high")): ("45", "N", "All Districts", "---District Total---"),
        ("2026", "999000", "All Students", _metric("reading", "proficient_distinguished_rate", "grade_10")): ("49", "N", "All Districts", "---District Total---"),
        ("2026", "999000", "All Students", _metric("reading", "proficient_distinguished_rate", "grade_3")): ("49", "N", "All Districts", "---District Total---"),
        ("2026", "999000", "All Students", _metric("math", "proficient_distinguished_rate", "grade_8")): ("39", "N", "All Districts", "---District Total---"),
        ("2026", "001016", "All Students", _metric("reading", "proficient_distinguished_rate", "grade_3")): ("52", "N", "Adair County", "Adair County Elementary School"),
        ("2026", "001016", "All Students", _metric("reading", "proficient_distinguished_rate", "elementary")): ("54", "N", "Adair County", "Adair County Elementary School"),
        ("2026", "001000", "All Students", _metric("reading", "content_index_score", "elementary")): ("71.7", "N", "Adair County", "---District Total---"),
        ("2024", "999000", "All Students", _metric("reading", "proficient_distinguished_rate", "elementary")): ("47", "N", "All Districts", "---District Total---"),
        ("2023", "999", "All Students", _metric("reading", "proficient_distinguished_rate", "elementary")): ("47", "N", "State", "---State Total---"),
        ("2021", "999", "All Students", _metric("reading", "proficient_distinguished_rate", "elementary")): ("39.5", "N", "State", "---State Total---"),
        ("2019", "999", "Students with Disabilities (IEP)", _metric("reading", "proficient_distinguished_rate", "grade_3")): ("34.7", "N", "State", "---State Total---"),
        ("2019", "999", "Consolidated Student Group", _metric("reading", "proficient_distinguished_rate", "grade_3")): ("37.9", "N", "State", "---State Total---"),
        ("2018", "999", "All Students", _metric("math", "proficient_distinguished_rate", "elementary")): ("48.8", "N", "State", "---State Total---"),
    }
    batch = []
    dupes = 0
    bad_names = set()
    with output.open(newline="") as handle:
        for row in csv.DictReader(handle):
            year = row["year"]
            assert year in counts, year
            counts[year] += 1
            if row["suppressed"] == "Y":
                suppressed[year] += 1
            metric = row["metric"]
            assert "_cw_" not in metric
            assert "_em_" not in metric
            assert "10th" not in metric
            assert "3rd" not in metric
            source = row["source_file"]
            if "KYRC23" in source or "Assessment_Performance_by_Grade" in source or "Accountable_Assessment_Performance_202" in source:
                bad_names.add(source)
            key = (year, row["school_id"], row["student_group"], metric)
            if key in wanted and key not in found:
                found[key] = (row["value"], row["suppressed"], row["district"], row["school_name"])
            batch.append(key)
            if len(batch) >= 50000:
                before = conn.execute("SELECT COUNT(*) FROM k").fetchone()[0]
                conn.executemany("INSERT OR IGNORE INTO k VALUES (?,?,?,?)", batch)
                after = conn.execute("SELECT COUNT(*) FROM k").fetchone()[0]
                dupes += len(batch) - (after - before)
                batch.clear()
        if batch:
            before = conn.execute("SELECT COUNT(*) FROM k").fetchone()[0]
            conn.executemany("INSERT OR IGNORE INTO k VALUES (?,?,?,?)", batch)
            after = conn.execute("SELECT COUNT(*) FROM k").fetchone()[0]
            dupes += len(batch) - (after - before)

    assert counts == EXPECTED_ROWS
    assert suppressed == EXPECTED_SUPPRESSED
    assert dupes == 0
    assert not bad_names
    assert set(found) == set(wanted)
    for key, expected in wanted.items():
        value, flag, district, school = found[key]
        expect_value, expect_flag, expect_district, expect_school = expected
        assert flag == expect_flag
        assert district == expect_district
        assert school == expect_school
        assert float(value) == pytest.approx(float(expect_value))
