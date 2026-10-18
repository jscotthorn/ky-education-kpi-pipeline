"""Real-row checks for the teacher experience KYRC26 opt-in.

Fixtures are verbatim excerpts. The production mappings.yaml config is used
so the Int64 cast of educator_count runs.
"""
from pathlib import Path

import pandas as pd
import yaml

from etl.teacher_experience import transform

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "teacher_experience"


def _run_fixtures(tmp_path: Path, filenames: list) -> pd.DataFrame:
    """Copy committed real-row excerpts into a raw directory and transform them."""
    raw_dir = tmp_path / "raw"
    source_dir = raw_dir / "teacher_experience"
    source_dir.mkdir(parents=True)
    proc_dir = tmp_path / "processed"
    proc_dir.mkdir()
    for name in filenames:
        (source_dir / name).write_bytes((FIXTURE_DIR / name).read_bytes())
    config = yaml.safe_load((ROOT / "config" / "mappings.yaml").read_text())
    transform(raw_dir, proc_dir, config["sources"]["teacher_experience"])
    output = proc_dir / "teacher_experience.csv"
    assert output.exists(), filenames
    return pd.read_csv(output, dtype=str)


def _comparable(df: pd.DataFrame) -> pd.DataFrame:
    """Drop the processing timestamp and sort so two runs can be compared."""
    columns = [column for column in df.columns if column != "last_updated"]
    return df[columns].sort_values(columns).reset_index(drop=True)


def _metric_value(df: pd.DataFrame, year: str, school_id: str) -> pd.Series:
    matches = df[(df["year"] == year) & (df["school_id"] == school_id)]
    assert len(matches) == 1, (year, school_id, len(matches))
    row = matches.iloc[0]
    assert row["metric"] == "teacher_average_years_experience"
    assert row["student_group"] == "All Students"
    assert row["suppressed"] == "N"
    return row


def test_kyrc26_real_rows_and_prior_year_anchors(tmp_path):
    """2025-26 KPIs from real rows, with earlier-year anchors.

    State All Students is 12.20. The educator count on that row is 39,242.
    KPI year is 2026, from school year 20252026. District is All Districts.
    There is no Suppressed column and no starred cell. A school with one
    teacher publishes its average. The 2021 Fayette count 2,589 and the
    2024 and 2025 state counts are comma-formatted in the source.
    """
    prior_names = [
        "average_years_school_experience_2020.csv",
        "average_years_school_experience_2021.csv",
        "average_years_school_experience_2022.csv",
        "average_years_school_experience_2023.csv",
        "KYRC24_OVW_Average_Years_School_Experience.csv",
        "KYRC25_OVW_Average_Years_School_Experience.csv",
    ]
    current_names = prior_names + ["KYRC26_OVW_Average_Years_School_Experience.csv"]
    prior_only = _run_fixtures(tmp_path / "prior", prior_names)
    combined = _run_fixtures(tmp_path / "combined", current_names)

    # Same-branch check only: adding the 2026 excerpt does not rewrite earlier rows.
    # The fixed anchors below are the prior-year regression.
    prior_slice = combined[combined["year"].isin(["2020", "2021", "2022", "2023", "2024", "2025"])]
    assert _comparable(prior_slice).equals(_comparable(prior_only))

    assert set(combined["source_file"]) == set(current_names)
    assert "20252026" not in set(combined["year"])
    assert set(combined.loc[combined["source_file"].str.startswith("KYRC26_"), "year"]) == {"2026"}

    state = _metric_value(combined, "2026", "999000")
    assert float(state["value"]) == 12.20
    assert state["district"] == "All Districts"
    assert state["school_name"] == "---District Total---"
    assert state["year"] == "2026"
    assert state["school_id"] == "999000"
    assert state["source_file"] == "KYRC26_OVW_Average_Years_School_Experience.csv"
    assert pd.isna(state["county_name"])

    district = _metric_value(combined, "2026", "165000")
    assert float(district["value"]) == 13.38
    assert district["district"] == "Fayette County"
    assert district["school_name"] == "---District Total---"
    assert district["school_id"] == "165000"
    assert district["co_op"] == "CKEC"
    assert district["co_op_code"] == "901"
    assert district["nces_id"] == "2101860"
    assert district["county_name"] == "FAYETTE"

    school = _metric_value(combined, "2026", "165170")
    assert float(school["value"]) == 14.29
    assert school["district"] == "Fayette County"
    assert school["school_name"] == "Bryan Station High School"
    assert school["state_school_id"] == "034165170"
    assert school["nces_id"] == "210186000388"
    assert school["school_type"] == "A1"
    assert school["county_name"] == "FAYETTE"
    assert school["co_op"] == "CKEC"
    assert school["co_op_code"] == "901"

    adair = _metric_value(combined, "2026", "001010")
    assert float(adair["value"]) == 13.37
    assert adair["district"] == "Adair County"
    assert adair["school_name"] == "Adair County High School"
    assert adair["nces_id"] == "210003000001"
    assert adair["school_type"] == "A1"
    assert adair["co_op"] == "GRREC"
    assert adair["co_op_code"] == "902"

    # One teacher. The average is published. There is no star in this file.
    one_teacher = _metric_value(combined, "2026", "147120")
    assert float(one_teacher["value"]) == 4.00
    assert one_teacher["school_name"] == "Regional School Programs"
    assert one_teacher["district"] == "Dayton Independent"
    assert one_teacher["school_type"] == "A5"
    assert one_teacher["nces_id"] == "210153002413"

    blind = _metric_value(combined, "2026", "602176")
    assert float(blind["value"]) == 18.00
    assert blind["school_name"] == "Kentucky School for the Blind"
    assert blind["district"] == "Kentucky School for the Blind District"
    assert blind["school_type"] == "D1"

    # Prior-year anchors. 2024 and 2025 state counts, and the 2021 Fayette
    # count, are comma-formatted. The averages are the file values.
    row = _metric_value(combined, "2025", "999000")
    assert float(row["value"]) == 12.28
    assert row["district"] == "All Districts"
    assert row["school_name"] == "---District Total---"
    assert row["year"] == "2025"
    assert row["source_file"] == "KYRC25_OVW_Average_Years_School_Experience.csv"
    assert float(_metric_value(combined, "2025", "165170")["value"]) == 14.32
    assert float(_metric_value(combined, "2025", "001010")["value"]) == 13.82
    # KYRC25 stores this NCES id as published. It is not rewritten.
    assert _metric_value(combined, "2025", "001010")["nces_id"] == "210003000000"

    row = _metric_value(combined, "2024", "999000")
    assert float(row["value"]) == 12.17
    assert row["district"] == "All Districts"
    assert row["school_name"] == "---District Total---"
    assert row["source_file"] == "KYRC24_OVW_Average_Years_School_Experience.csv"
    assert float(_metric_value(combined, "2024", "165170")["value"]) == 12.83

    row = _metric_value(combined, "2023", "999")
    assert float(row["value"]) == 12.06
    assert row["district"] == "State"
    assert row["school_name"] == "---State Total---"
    assert row["source_file"] == "average_years_school_experience_2023.csv"
    assert float(_metric_value(combined, "2023", "165170")["value"]) == 12.61
    # The historical file types this district total as 20022023. The ending
    # year is still 2023, and the average is the published 12.21.
    breathitt = _metric_value(combined, "2023", "061")
    assert float(breathitt["value"]) == 12.21
    assert breathitt["district"] == "Breathitt County"
    assert breathitt["school_name"] == "---District Total---"

    row = _metric_value(combined, "2022", "999")
    assert float(row["value"]) == 11.8
    assert row["district"] == "State"
    assert row["school_name"] == "---State Total---"

    row = _metric_value(combined, "2021", "999")
    assert float(row["value"]) == 12.1
    assert row["district"] == "State"
    assert row["school_name"] == "---State Total---"
    fayette = _metric_value(combined, "2021", "165")
    assert float(fayette["value"]) == 12.3
    assert fayette["district"] == "Fayette County"
    assert fayette["school_name"] == "---District Total---"
    assert fayette["co_op"] == "CKEC"

    row = _metric_value(combined, "2020", "999")
    assert float(row["value"]) == 12.0
    assert row["district"] == "State"
    assert row["school_name"] == "---State Total---"
    assert float(_metric_value(combined, "2020", "165170")["value"]) == 11.7
