"""Real-row checks for the student-teacher ratio KYRC26 opt-in.

Fixtures are verbatim excerpts. The production mappings.yaml config is used
so the str cast of the ratio column runs. KYRC26 has no state row and no
district ``All Schools`` row. The school rows are the file.
"""
from pathlib import Path

import pandas as pd
import yaml

from etl.student_teacher_ratio import transform

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "student_teacher_ratio"


def _run_fixtures(tmp_path: Path, filenames: list) -> pd.DataFrame:
    """Copy committed real-row excerpts into a raw directory and transform them."""
    raw_dir = tmp_path / "raw"
    source_dir = raw_dir / "student_teacher_ratio"
    source_dir.mkdir(parents=True)
    proc_dir = tmp_path / "processed"
    proc_dir.mkdir()
    for name in filenames:
        (source_dir / name).write_bytes((FIXTURE_DIR / name).read_bytes())
    config = yaml.safe_load((ROOT / "config" / "mappings.yaml").read_text())
    transform(raw_dir, proc_dir, config["sources"]["student_teacher_ratio"])
    output = proc_dir / "student_teacher_ratio.csv"
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
    assert row["metric"] == "student_teacher_ratio"
    assert row["student_group"] == "All Students"
    assert row["suppressed"] == "N"
    return row


def test_kyrc26_real_rows_and_prior_year_anchors(tmp_path):
    """2025-26 school ratios from real rows, with earlier-year anchors.

    KYRC26 has no ``999000`` row and no ``All Schools`` row. Bryan Station
    High School ``165170`` is 15 students per teacher. KPI year is 2026,
    from school year ``20252026``. There is no Suppressed column and no
    starred cell. The 2019-20 BAVEL ratio is stored as ``48:01:00``.
    """
    prior_names = [
        "student_teacher_ratio_2020.csv",
        "student_teacher_ratio_2021.csv",
        "student_teacher_ratio_2022.csv",
        "student_teacher_ratio_2023.csv",
        "KYRC24_OVW_Student_to_Teacher_Ratio.csv",
        "KYRC25_OVW_Student_to_Teacher_Ratio.csv",
    ]
    current_names = prior_names + ["KYRC26_OVW_Student_to_Teacher_Ratio.csv"]
    prior_only = _run_fixtures(tmp_path / "prior", prior_names)
    combined = _run_fixtures(tmp_path / "combined", current_names)

    # Same-branch check only: adding the 2026 excerpt does not rewrite earlier rows.
    # The fixed anchors below are the prior-year regression.
    prior_slice = combined[combined["year"].isin(["2020", "2021", "2022", "2023", "2024", "2025"])]
    assert _comparable(prior_slice).equals(_comparable(prior_only))

    assert set(combined["source_file"]) == set(current_names)
    assert "20252026" not in set(combined["year"])
    assert set(combined.loc[combined["source_file"].str.startswith("KYRC26_"), "year"]) == {"2026"}

    kyrc26 = combined[combined["year"] == "2026"]
    assert set(kyrc26["school_id"]) == {"001010", "165170", "275012"}
    assert not kyrc26["school_id"].str.endswith("000").any()
    assert "999000" not in set(kyrc26["school_id"])
    assert "---District Total---" not in set(kyrc26["school_name"])
    assert "All Schools" not in set(kyrc26["school_name"])

    school = _metric_value(combined, "2026", "165170")
    assert float(school["value"]) == 15.0
    assert school["district"] == "Fayette County"
    assert school["school_name"] == "Bryan Station High School"
    assert school["year"] == "2026"
    assert school["school_id"] == "165170"
    assert school["state_school_id"] == "034165170"
    assert school["nces_id"] == "210186000388"
    assert school["school_type"] == "A1"
    assert school["county_name"] == "FAYETTE"
    assert school["co_op"] == "CKEC"
    assert school["co_op_code"] == "901"
    assert school["source_file"] == "KYRC26_OVW_Student_to_Teacher_Ratio.csv"

    adair = _metric_value(combined, "2026", "001010")
    assert float(adair["value"]) == 15.0
    assert adair["district"] == "Adair County"
    assert adair["school_name"] == "Adair County High School"
    assert adair["nces_id"] == "210003000001"
    assert adair["state_school_id"] == "001001010"
    assert adair["school_type"] == "A1"
    assert adair["co_op"] == "GRREC"
    assert adair["co_op_code"] == "902"
    assert adair["county_name"] == "ADAIR"

    # Jefferson code 909 was named GREC in KYRC25 and GLEC in KYRC26.
    fern = _metric_value(combined, "2026", "275012")
    assert float(fern["value"]) == 14.0
    assert fern["district"] == "Jefferson County"
    assert fern["school_name"] == "Fern Creek High"
    assert fern["nces_id"] == "210299000628"
    assert fern["state_school_id"] == "056275012"
    assert fern["co_op"] == "GLEC"
    assert fern["co_op_code"] == "909"

    # Prior-year anchors. KYRC25 still has the state row and the Fayette
    # district total. KYRC25 NCES ids are stored as published.
    row = _metric_value(combined, "2025", "999000")
    assert float(row["value"]) == 15.0
    assert row["district"] == "All Districts"
    assert row["school_name"] == "---District Total---"
    assert row["year"] == "2025"
    assert row["source_file"] == "KYRC25_OVW_Student_to_Teacher_Ratio.csv"
    assert pd.isna(row["county_name"])

    fayette = _metric_value(combined, "2025", "165000")
    assert float(fayette["value"]) == 13.0
    assert fayette["district"] == "Fayette County"
    assert fayette["school_name"] == "---District Total---"
    assert fayette["nces_id"] == "2101860"
    assert fayette["co_op"] == "CKEC"
    assert fayette["co_op_code"] == "901"

    bryan = _metric_value(combined, "2025", "165170")
    assert float(bryan["value"]) == 15.0
    assert bryan["nces_id"] == "210186000000"
    assert bryan["school_name"] == "Bryan Station High School"
    assert float(_metric_value(combined, "2025", "001010")["value"]) == 14.0
    assert _metric_value(combined, "2025", "001010")["nces_id"] == "210003000000"
    fern_25 = _metric_value(combined, "2025", "275012")
    assert float(fern_25["value"]) == 14.0
    assert fern_25["co_op"] == "GREC"
    assert fern_25["co_op_code"] == "909"
    assert fern_25["nces_id"] == "210299000000"

    row = _metric_value(combined, "2024", "999000")
    assert float(row["value"]) == 15.0
    assert row["district"] == "All Districts"
    assert row["school_name"] == "---District Total---"
    assert row["source_file"] == "KYRC24_OVW_Student_to_Teacher_Ratio.csv"
    assert float(_metric_value(combined, "2024", "165170")["value"]) == 15.0
    assert float(_metric_value(combined, "2024", "001010")["value"]) == 14.0
    assert _metric_value(combined, "2024", "165170")["nces_id"] == "210186000388"

    row = _metric_value(combined, "2023", "999")
    assert float(row["value"]) == 15.0
    assert row["district"] == "State"
    assert row["school_name"] == "---State Total---"
    assert row["source_file"] == "student_teacher_ratio_2023.csv"
    assert float(_metric_value(combined, "2023", "165170")["value"]) == 16.0
    assert float(_metric_value(combined, "2023", "001010")["value"]) == 15.0
    # The historical file types this district total as 20022023. The ending
    # year is still 2023, and the ratio is the published 14.
    breathitt = _metric_value(combined, "2023", "061")
    assert float(breathitt["value"]) == 14.0
    assert breathitt["district"] == "Breathitt County"
    assert breathitt["school_name"] == "---District Total---"
    fayette_23 = _metric_value(combined, "2023", "165")
    assert float(fayette_23["value"]) == 13.0
    assert fayette_23["district"] == "Fayette County"
    assert fayette_23["school_name"] == "---District Total---"

    row = _metric_value(combined, "2022", "999")
    assert float(row["value"]) == 15.0
    assert row["district"] == "State"
    assert row["school_name"] == "---State Total---"
    assert float(_metric_value(combined, "2022", "165170")["value"]) == 18.0
    assert float(_metric_value(combined, "2022", "001010")["value"]) == 16.0

    row = _metric_value(combined, "2021", "999")
    assert float(row["value"]) == 16.0
    assert row["district"] == "State"
    assert row["school_name"] == "---State Total---"
    fayette_21 = _metric_value(combined, "2021", "165")
    assert float(fayette_21["value"]) == 14.0
    assert fayette_21["district"] == "Fayette County"
    assert fayette_21["school_name"] == "---District Total---"
    assert fayette_21["co_op"] == "CKEC"
    assert float(_metric_value(combined, "2021", "165170")["value"]) == 18.0

    row = _metric_value(combined, "2020", "999")
    assert float(row["value"]) == 16.0
    assert row["district"] == "State"
    assert row["school_name"] == "---State Total---"
    assert float(_metric_value(combined, "2020", "165170")["value"]) == 17.0
    assert float(_metric_value(combined, "2020", "001010")["value"]) == 16.0
    # ``48:01:00`` is 48 students per teacher. The old parser dropped it.
    bavel = _metric_value(combined, "2020", "021012")
    assert float(bavel["value"]) == 48.0
    assert bavel["district"] == "Barren County"
    assert bavel["school_name"] == "BAVEL"
    assert bavel["nces_id"] == "210030001970"
    assert bavel["source_file"] == "student_teacher_ratio_2020.csv"
