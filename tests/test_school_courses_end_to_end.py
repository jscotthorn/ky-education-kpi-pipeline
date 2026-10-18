"""End-to-end tests for school courses using the registered source files."""
from pathlib import Path

import pandas as pd
import pytest
import yaml

from etl.constants import KPI_COLUMNS
from etl.school_courses import transform

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw" / "school_courses"

# Source rows become one KPI row each. A published enrollment is suppressed
# N with value 1. A starred enrollment is suppressed Y. 2019 has no stars.
EXPECTED_COUNTS = {
    "2019": {"N": 29147, "Y": 0},
    "2020": {"N": 25838, "Y": 4020},
    "2021": {"N": 24944, "Y": 4160},
    "2022": {"N": 26391, "Y": 4462},
    "2023": {"N": 26259, "Y": 4429},
    "2024": {"N": 26666, "Y": 4668},
    "2025": {"N": 27729, "Y": 5064},
    "2026": {"N": 28140, "Y": 5245},
}


def _ready() -> bool:
    needed = [
        "SCHOOL_COURSES_SUMMARY_18-19.xlsx",
        "school_courses_summary_2020.csv",
        "school_courses_summary_2021.csv",
        "school_courses_summary_2022.csv",
        "school_courses_summary_2023.csv",
        "KYRC24_EDOP_School_Courses_Summary.csv",
        "KYRC25_EDOP_School_Courses_Summary.csv",
        "KYRC26_EDOP_School_Courses_Summary.csv",
    ]
    return RAW_DIR.exists() and all((RAW_DIR / name).exists() for name in needed)


@pytest.mark.skipif(not _ready(), reason="Registered school_courses files are not all present")
def test_registered_files_match_the_source_row_counts(tmp_path):
    """Every registered year writes one KPI row per source course row.

    2019, 2021, and 2023 numeric rows stay 29,147 / 24,944 / 26,259. The
    starred rows those years used to drop are now suppressed Y. KYRC25
    used to write nothing because All Students was unmapped.
    """
    config = yaml.safe_load((ROOT / "config" / "mappings.yaml").read_text())
    proc_dir = tmp_path / "processed"
    proc_dir.mkdir()
    transform(ROOT / "data" / "raw", proc_dir, config["sources"]["school_courses"])
    output = proc_dir / "school_courses.csv"
    assert output.exists()
    df = pd.read_csv(output, dtype=str, keep_default_na=False)
    for column in KPI_COLUMNS:
        assert column in df.columns
    assert set(df["metric"]) == {"courses_offered_count"}
    assert set(df["student_group"]) == {"All Students"}
    assert "999000" not in set(df["school_id"])

    published = df[df["suppressed"] == "N"]
    assert set(published["value"]) == {"1.0"}
    suppressed = df[df["suppressed"] == "Y"]
    assert set(suppressed["value"]) == {""}

    for year, counts in EXPECTED_COUNTS.items():
        year_rows = df[df["year"] == year]
        assert len(year_rows[year_rows["suppressed"] == "N"]) == counts["N"], year
        assert len(year_rows[year_rows["suppressed"] == "Y"]) == counts["Y"], year

    adair = df[(df["year"] == "2026") & (df["school_id"] == "001010")]
    assert len(adair) == 111
    assert len(adair[adair["suppressed"] == "Y"]) == 10
    assert set(adair["district"]) == {"Adair County"}

    bryan = df[(df["year"] == "2026") & (df["school_id"] == "165170")]
    assert len(bryan) == 109
    assert bryan.iloc[0]["co_op_code"] == "901"
    assert set(bryan["school_name"]) == {"Bryan Station High School"}
