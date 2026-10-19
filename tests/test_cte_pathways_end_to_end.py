"""End-to-end tests for CTE pathways using the registered source files."""
from pathlib import Path

import pandas as pd
import pytest
import yaml

from etl.constants import KPI_COLUMNS
from etl.cte_pathways import transform

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw" / "cte_pathways"

# One row per entity, year, and published measure, plus the pathway count.
# 2019 has no concentrator column. No year stores a calculated completion rate.
EXPECTED_ROWS = {
    "2019": 1392,
    "2020": 1868,
    "2021": 1860,
    "2022": 1880,
    "2023": 1880,
    "2024": 1880,
    "2025": 1904,
    "2026": 1928,
}
EXPECTED_ENTITIES = {
    "2019": 464,
    "2020": 467,
    "2021": 465,
    "2022": 470,
    "2023": 470,
    "2024": 470,
    "2025": 476,
    "2026": 482,
}


def _ready() -> bool:
    needed = [
        "CAREER_PATHWAYS_18-19.xlsx",
        "career_pathways_2020.csv",
        "career_pathways_2021.csv",
        "career_pathways_2022.csv",
        "career_pathways_2023.csv",
        "KYRC24_CTE_Career_Pathways.csv",
        "KYRC25_CTE_Career_Pathways.csv",
        "KYRC26_CTE_Career_Pathways.csv",
    ]
    return RAW_DIR.exists() and all((RAW_DIR / name).exists() for name in needed)


def _one(df: pd.DataFrame, year: str, school_id: str, metric: str) -> pd.Series:
    matched = df[(df["year"] == year) & (df["school_id"] == school_id) & (df["metric"] == metric)]
    assert len(matched) == 1, (year, school_id, metric, len(matched))
    return matched.iloc[0]


@pytest.mark.skipif(not _ready(), reason="Registered cte_pathways files are not all present")
def test_registered_files_collapse_to_one_row_per_school(tmp_path):
    """Pathway names collapse to one row per school, year, and metric.

    The 2021-2025 per-pathway rows used to share a key. A starred
    component now suppresses the school total. 2019 keeps published
    zeros and does not read preparatory students as concentrators.
    """
    config = yaml.safe_load((ROOT / "config" / "mappings.yaml").read_text())
    proc_dir = tmp_path / "processed"
    proc_dir.mkdir()
    transform(ROOT / "data" / "raw", proc_dir, config["sources"]["cte_pathways"])
    output = proc_dir / "cte_pathways.csv"
    assert output.exists()
    df = pd.read_csv(output, dtype=str, keep_default_na=False)
    for column in KPI_COLUMNS:
        assert column in df.columns
    assert set(df["student_group"]) == {"All Students"}
    keys = ["year", "school_id", "student_group", "metric"]
    assert not df.duplicated(keys).any()
    assert len(df) == 14592
    assert "cte_pathway_completion_rate" not in set(df["metric"])

    for year, rows in EXPECTED_ROWS.items():
        year_rows = df[df["year"] == year]
        assert len(year_rows) == rows, year
        assert year_rows["school_id"].nunique() == EXPECTED_ENTITIES[year], year

    state = _one(df, "2026", "999000", "num_cte_pathways")
    assert state["value"] == "113.0"
    assert state["suppressed"] == "N"
    assert state["district"] == "All Districts"
    assert state["school_name"] == "---District Total---"
    assert _one(df, "2026", "999000", "cte_pathway_enrollment")["suppressed"] == "Y"
    assert _one(df, "2026", "999000", "cte_pathway_enrollment")["value"] == ""
    assert _one(df, "2026", "999000", "cte_concentrator_count")["suppressed"] == "Y"
    assert _one(df, "2026", "999000", "cte_pathway_completer_count")["suppressed"] == "Y"

    assert _one(df, "2026", "081010", "num_cte_pathways")["value"] == "7.0"
    assert _one(df, "2026", "081010", "cte_pathway_enrollment")["value"] == "556.0"
    assert _one(df, "2026", "081010", "cte_concentrator_count")["value"] == "413.0"
    assert _one(df, "2026", "081010", "cte_pathway_completer_count")["value"] == "143.0"
    assert set(
        df[(df["year"] == "2026") & (df["school_id"] == "081010")]["suppressed"]
    ) == {"N"}

    assert _one(df, "2026", "606000", "num_cte_pathways")["value"] == "2.0"
    assert _one(df, "2026", "606000", "cte_pathway_enrollment")["value"] == "51.0"
    assert _one(df, "2026", "606000", "cte_concentrator_count")["suppressed"] == "Y"
    assert _one(df, "2026", "606460", "cte_pathway_enrollment")["value"] == "51.0"
    assert _one(df, "2026", "606460", "num_cte_pathways")["school_name"] == "Model High School"

    assert _one(df, "2026", "032010", "cte_pathway_enrollment")["suppressed"] == "Y"
    assert _one(df, "2026", "032010", "cte_concentrator_count")["value"] == "41.0"
    assert _one(df, "2026", "165170", "num_cte_pathways")["value"] == "14.0"
    assert _one(df, "2026", "165170", "cte_pathway_enrollment")["suppressed"] == "Y"

    assert _one(df, "2019", "999", "num_cte_pathways")["value"] == "167.0"
    assert _one(df, "2019", "999", "cte_pathway_enrollment")["value"] == "249895.0"
    assert _one(df, "2019", "999", "cte_pathway_completer_count")["value"] == "22289.0"
    assert _one(df, "2019", "999", "num_cte_pathways")["district"] == "State"
    year_2019 = df[df["year"] == "2019"]
    assert set(year_2019["suppressed"]) == {"N"}
    assert "cte_concentrator_count" not in set(year_2019["metric"])
    assert "cte_pathway_completion_rate" not in set(year_2019["metric"])

    assert _one(df, "2025", "999000", "num_cte_pathways")["value"] == "107.0"
    assert _one(df, "2021", "999", "num_cte_pathways")["value"] == "121.0"
    assert _one(df, "2021", "999", "num_cte_pathways")["district"] == "State"
