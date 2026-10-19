"""End-to-end tests for career readiness indicators using the registered files."""
from pathlib import Path

import pandas as pd
import pytest
import yaml

from etl.constants import KPI_COLUMNS
from etl.career_readiness_indicators import transform

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw" / "career_readiness_indicators"

# One KPI row per published cell. Blanks are omitted. Stars are kept.
EXPECTED_ROWS = {
    "2019": 1760,
    "2022": 2297,
    "2023": 2443,
    "2024": 3829,
    "2025": 3184,
    "2026": 3176,
}
EXPECTED_ENTITIES = {
    "2019": 397,
    "2022": 397,
    "2023": 397,
    "2024": 547,
    "2025": 398,
    "2026": 397,
}
EXPECTED_SUPPRESSED = {
    "2019": 0,
    "2022": 0,
    "2023": 0,
    "2024": 0,
    "2025": 601,
    "2026": 586,
}


def _ready() -> bool:
    needed = [
        "CTE_TRANSITION_READINESS_18-19.xlsx",
        "career_readiness_indicators_2022.csv",
        "career_readiness_indicators_2023.csv",
        "KYRC24_CTE_Career_Readiness_Indicators.csv",
        "KYRC25_CTE_Career_Readiness_Indicators.csv",
        "KYRC26_CTE_Career_Readiness_Indicators.csv",
    ]
    return RAW_DIR.exists() and all((RAW_DIR / name).exists() for name in needed)


def _one(df: pd.DataFrame, year: str, school_id: str, metric: str) -> pd.Series:
    matched = df[(df["year"] == year) & (df["school_id"] == school_id) & (df["metric"] == metric)]
    assert len(matched) == 1, (year, school_id, metric, len(matched))
    return matched.iloc[0]


@pytest.mark.skipif(not _ready(), reason="Registered career_readiness_indicators files are not all present")
def test_registered_files_keep_published_cells(tmp_path):
    """Each published cell is one KPI row. Stars are suppressed. Blanks are not.

    The 3,001 values the previous parser stored for 2023-2025 are the same
    numbers. 2023 industry certifications were missing because the header
    is plural. KYRC25 and KYRC26 stars are stored. School id 999 is not
    padded. KYRC23 and the 2017-18 workbook are not in this directory.
    """
    config = yaml.safe_load((ROOT / "config" / "mappings.yaml").read_text())
    proc_dir = tmp_path / "processed"
    proc_dir.mkdir()
    transform(ROOT / "data" / "raw", proc_dir, config["sources"]["career_readiness_indicators"])
    output = proc_dir / "career_readiness_indicators.csv"
    assert output.exists()
    df = pd.read_csv(output, dtype=str, keep_default_na=False)
    for column in KPI_COLUMNS:
        assert column in df.columns
    assert set(df["student_group"]) == {"All Students"}
    keys = ["year", "school_id", "student_group", "metric"]
    assert not df.duplicated(keys).any()
    assert len(df) == 16689
    assert set(df["year"]) == set(EXPECTED_ROWS)

    for year, rows in EXPECTED_ROWS.items():
        year_rows = df[df["year"] == year]
        assert len(year_rows) == rows
        assert year_rows["school_id"].nunique() == EXPECTED_ENTITIES[year]
        assert int((year_rows["suppressed"] == "Y").sum()) == EXPECTED_SUPPRESSED[year]

    state = _one(df, "2026", "999000", "career_readiness_total_graduates_count")
    assert state["value"] == "49751.0"
    assert state["suppressed"] == "N"
    assert state["district"] == "All Districts"
    assert state["school_name"] == "---District Total---"
    assert _one(df, "2026", "999000", "career_readiness_industry_certification_count")["value"] == "18286.0"
    assert _one(df, "2026", "999000", "career_readiness_work_based_learning_count")["value"] == "5115.0"
    assert _one(df, "2026", "081010", "career_readiness_work_based_learning_count")["value"] == "34.0"
    assert _one(df, "2026", "032010", "career_readiness_apprenticeship_count")["suppressed"] == "Y"
    assert _one(df, "2026", "032010", "career_readiness_industry_certification_count")["value"] == "7.0"
    assert _one(df, "2026", "606460", "career_readiness_dual_credit_cte_count")["value"] == "5.0"
    assert df[(df["year"] == "2026") & (df["school_id"].str.startswith("880"))].empty

    assert _one(df, "2025", "999000", "career_readiness_industry_certification_count")["value"] == "16334.0"
    challenge = df[(df["year"] == "2025") & (df["school_id"] == "880000")]
    assert len(challenge) == 8
    assert set(challenge["suppressed"]) == {"Y"}

    assert _one(df, "2024", "999000", "career_readiness_industry_certification_count")["value"] == "13548.0"
    assert _one(df, "2024", "880100", "career_readiness_industry_certification_count")["value"] == "0.0"
    assert "career_readiness_total_graduates_count" not in set(df.loc[df["year"] == "2024", "metric"])

    assert _one(df, "2023", "999", "career_readiness_dual_credit_cte_count")["value"] == "12097.0"
    assert _one(df, "2023", "999", "career_readiness_industry_certification_count")["value"] == "14055.0"
    assert _one(df, "2023", "999", "career_readiness_industry_certification_count")["district"] == "State"
    assert _one(df, "2023", "061", "career_readiness_total_graduates_count")["value"] == "115.0"

    assert _one(df, "2022", "999", "career_readiness_total_graduates_count")["value"] == "47061.0"
    assert _one(df, "2019", "999", "career_readiness_exceptional_work_experience_count")["value"] == "68.0"
    assert _one(df, "2019", "999", "career_readiness_more_than_one_indicator_count")["value"] == "1.0"
    assert "999000" not in set(df.loc[df["year"] == "2019", "school_id"])
    assert "career_readiness_cte_end_of_program_assessment_count" not in set(df.loc[df["year"] == "2019", "metric"])

    twenty_six = df[df["year"] == "2026"]
    assert int((twenty_six["co_op"] == "GLEC").sum() > 0)
    assert "GREC" not in set(twenty_six["co_op"])
    assert twenty_six.loc[twenty_six["co_op"] == "GLEC", "school_id"].nunique() == 22

    phantom = {
        "career_readiness_cooperative_education_count",
        "career_readiness_internship_count",
        "career_readiness_advanced_placement_count",
        "career_readiness_transition_readiness_count",
    }
    assert phantom.isdisjoint(set(df["metric"]))
    assert "2018" not in set(df["year"])
