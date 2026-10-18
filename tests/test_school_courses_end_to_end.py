"""End-to-end tests for school courses using the registered source files."""
from pathlib import Path

import pandas as pd
import pytest
import yaml

from etl.constants import KPI_COLUMNS
from etl.school_courses import transform

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw" / "school_courses"

# One KPI row per school, year, and metric. Seven metrics. The sums are
# the source course rows. A starred total is inside courses_offered_count
# and courses_offered_suppressed_count. A blank total is not counted.
# 2019 uses HPE. 2020-2026 spell that category HEPE. Both are _hpe.
# courses_offered_count_other is 0 in every registered year.
EXPECTED = {
    "2019": {
        "schools": 1361,
        "courses": 29147,
        "suppressed": 0,
        "cte": 18250,
        "hpe": 3794,
        "vpa": 5430,
        "wl": 1673,
        "other": 0,
    },
    "2020": {
        "schools": 1346,
        "courses": 29858,
        "suppressed": 4020,
        "cte": 18956,
        "hpe": 3726,
        "vpa": 5537,
        "wl": 1639,
        "other": 0,
    },
    "2021": {
        "schools": 1343,
        "courses": 29104,
        "suppressed": 4160,
        "cte": 18729,
        "hpe": 3615,
        "vpa": 5224,
        "wl": 1536,
        "other": 0,
    },
    "2022": {
        "schools": 1350,
        "courses": 30853,
        "suppressed": 4462,
        "cte": 19948,
        "hpe": 3829,
        "vpa": 5517,
        "wl": 1559,
        "other": 0,
    },
    "2023": {
        "schools": 1341,
        "courses": 30688,
        "suppressed": 4429,
        "cte": 19890,
        "hpe": 3744,
        "vpa": 5504,
        "wl": 1550,
        "other": 0,
    },
    "2024": {
        "schools": 1339,
        "courses": 31334,
        "suppressed": 4668,
        "cte": 20610,
        "hpe": 3767,
        "vpa": 5465,
        "wl": 1492,
        "other": 0,
    },
    "2025": {
        "schools": 1343,
        "courses": 32793,
        "suppressed": 5064,
        "cte": 22164,
        "hpe": 3737,
        "vpa": 5402,
        "wl": 1490,
        "other": 0,
    },
    "2026": {
        "schools": 1334,
        "courses": 33385,
        "suppressed": 5245,
        "cte": 22812,
        "hpe": 3700,
        "vpa": 5411,
        "wl": 1462,
        "other": 0,
    },
}

_METRICS = (
    "courses_offered_count",
    "courses_offered_suppressed_count",
    "courses_offered_count_cte",
    "courses_offered_count_hpe",
    "courses_offered_count_vpa",
    "courses_offered_count_wl",
    "courses_offered_count_other",
)


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


def _sum(year_rows: pd.DataFrame, metric: str) -> int:
    values = year_rows.loc[year_rows["metric"] == metric, "value"]
    return int(pd.to_numeric(values, errors="coerce").sum())


@pytest.mark.skipif(not _ready(), reason="Registered school_courses files are not all present")
def test_registered_files_collapse_to_one_row_per_school(tmp_path):
    """Each school-year is seven count rows, and the sums match the files.

    2019, 2021, and 2023 used to store courses_offered_count 1 on every
    course. Those values are now the school counts. Stars are inside the
    count and the suppressed count. There is no per-course 1/NA row.
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
    assert set(df["metric"]) == set(_METRICS)
    assert set(df["student_group"]) == {"All Students"}
    assert set(df["suppressed"]) == {"N"}
    assert "999000" not in set(df["school_id"])
    keys = ["year", "school_id", "student_group", "metric"]
    assert not df.duplicated(keys).any()

    for year, expected in EXPECTED.items():
        year_rows = df[df["year"] == year]
        assert year_rows["school_id"].nunique() == expected["schools"], year
        assert len(year_rows) == expected["schools"] * 7, year
        assert _sum(year_rows, "courses_offered_count") == expected["courses"], year
        assert _sum(year_rows, "courses_offered_suppressed_count") == expected["suppressed"], year
        assert _sum(year_rows, "courses_offered_count_cte") == expected["cte"], year
        assert _sum(year_rows, "courses_offered_count_hpe") == expected["hpe"], year
        assert _sum(year_rows, "courses_offered_count_vpa") == expected["vpa"], year
        assert _sum(year_rows, "courses_offered_count_wl") == expected["wl"], year
        assert _sum(year_rows, "courses_offered_count_other") == expected["other"], year
        category_sum = (
            expected["cte"] + expected["hpe"] + expected["vpa"] + expected["wl"] + expected["other"]
        )
        assert category_sum == expected["courses"], year

    adair = df[(df["year"] == "2026") & (df["school_id"] == "001010")]
    assert len(adair) == 7
    assert _sum(adair, "courses_offered_count") == 111
    assert _sum(adair, "courses_offered_suppressed_count") == 10
    assert set(adair["district"]) == {"Adair County"}
    assert set(adair["suppressed"]) == {"N"}

    bryan = df[(df["year"] == "2026") & (df["school_id"] == "165170")]
    assert len(bryan) == 7
    assert _sum(bryan, "courses_offered_count") == 109
    assert bryan.iloc[0]["co_op_code"] == "901"
    assert set(bryan["school_name"]) == {"Bryan Station High School"}
