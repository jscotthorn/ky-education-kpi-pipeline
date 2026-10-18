"""Real-row checks for the novice teachers KYRC26 opt-in.

Fixtures are verbatim excerpts. The production mappings.yaml config is used
so the Int64 cast of the teacher counts runs.
"""
from pathlib import Path

import pandas as pd
import yaml

from etl.novice_teachers import transform

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "novice_teachers"


def _run_fixtures(tmp_path: Path, filenames: list) -> pd.DataFrame:
    """Copy committed real-row excerpts into a raw directory and transform them."""
    raw_dir = tmp_path / "raw"
    source_dir = raw_dir / "novice_teachers"
    source_dir.mkdir(parents=True)
    proc_dir = tmp_path / "processed"
    proc_dir.mkdir()
    for name in filenames:
        (source_dir / name).write_bytes((FIXTURE_DIR / name).read_bytes())
    config = yaml.safe_load((ROOT / "config" / "mappings.yaml").read_text())
    transform(raw_dir, proc_dir, config["sources"]["novice_teachers"])
    output = proc_dir / "novice_teachers.csv"
    assert output.exists(), filenames
    return pd.read_csv(output, dtype=str)


def _comparable(df: pd.DataFrame) -> pd.DataFrame:
    """Drop the processing timestamp and sort so two runs can be compared."""
    columns = [column for column in df.columns if column != "last_updated"]
    return df[columns].sort_values(columns).reset_index(drop=True)


def _rows(df: pd.DataFrame, year: str, school_id: str, metric: str, group: str = "All Students") -> pd.DataFrame:
    matches = df[
        (df["year"] == year)
        & (df["school_id"] == school_id)
        & (df["metric"] == metric)
        & (df["student_group"] == group)
    ]
    return matches


def _one(df: pd.DataFrame, year: str, school_id: str, metric: str, group: str = "All Students") -> pd.Series:
    matches = _rows(df, year, school_id, metric, group)
    assert len(matches) == 1, (year, school_id, metric, group, len(matches))
    row = matches.iloc[0]
    assert row["suppressed"] == "N"
    return row


def test_kyrc26_real_rows_and_prior_year_anchors(tmp_path):
    """2025-26 KPIs from real rows, with earlier-year anchors.

    State All Students is 15.30 percent with 1-3 years and 5.60 percent
    with less than 1 year. The new-teacher counts on that row are 6,016
    and 2,217. KPI year is 2026, from school year 20252026. District is
    All Districts. There is no Suppressed column and no starred cell.
    A published rate of 0 is kept. The 2022 blank district totals emit
    nothing.
    """
    prior_names = [
        "new_teacher_count_18-19.csv",
        "inexperienced_teachers_2020.csv",
        "inexperienced_teachers_2021.csv",
        "inexperienced_teachers_2022.csv",
        "inexperienced_teachers_2023.csv",
        "students_taught_by_inexperienced_teachers_2020.csv",
        "students_taught_by_inexperienced_teachers_2023.csv",
        "KYRC24_OVW_Inexperienced_Teachers.csv",
        "KYRC24_OVW_Students_Taught_by_Inexperienced_Teachers.csv",
        "KYRC25_OVW_Inexperienced_Teachers.csv",
        "KYRC25_OVW_Students_Taught_by_Inexperienced_Teachers.csv",
    ]
    current_names = prior_names + [
        "KYRC26_OVW_Inexperienced_Teachers.csv",
        "KYRC26_OVW_Students_Taught_by_Inexperienced_Teachers.csv",
    ]
    prior_only = _run_fixtures(tmp_path / "prior", prior_names)
    combined = _run_fixtures(tmp_path / "combined", current_names)

    prior_years = ["2019", "2020", "2021", "2022", "2023", "2024", "2025"]
    prior_slice = combined[combined["year"].isin(prior_years)]
    assert _comparable(prior_slice).equals(_comparable(prior_only))

    assert "20252026" not in set(combined["year"])
    kyrc26_files = combined[combined["source_file"].str.startswith("KYRC26_")]
    assert set(kyrc26_files["year"]) == {"2026"}
    assert set(kyrc26_files["suppressed"]) == {"N"}

    state_less = _one(combined, "2026", "999000", "novice_teacher_rate_less_than_1_year")
    assert float(state_less["value"]) == 5.60
    assert state_less["district"] == "All Districts"
    assert state_less["school_name"] == "---District Total---"
    assert state_less["year"] == "2026"
    assert state_less["source_file"] == "KYRC26_OVW_Inexperienced_Teachers.csv"
    state_band = _one(combined, "2026", "999000", "novice_teacher_rate_1_to_3_years")
    assert float(state_band["value"]) == 15.30

    district_less = _one(combined, "2026", "165000", "novice_teacher_rate_less_than_1_year")
    assert float(district_less["value"]) == 5.10
    assert district_less["district"] == "Fayette County"
    assert district_less["school_name"] == "---District Total---"
    assert district_less["co_op"] == "CKEC"
    assert district_less["co_op_code"] == "901"
    assert district_less["nces_id"] == "2101860"
    assert float(_one(combined, "2026", "165000", "novice_teacher_rate_1_to_3_years")["value"]) == 10.50

    school = _one(combined, "2026", "165170", "novice_teacher_rate_less_than_1_year")
    assert float(school["value"]) == 3.80
    assert school["district"] == "Fayette County"
    assert school["school_name"] == "Bryan Station High School"
    assert school["state_school_id"] == "034165170"
    assert school["nces_id"] == "210186000388"
    assert school["school_type"] == "A1"
    assert school["co_op"] == "CKEC"
    assert school["co_op_code"] == "901"
    assert float(_one(combined, "2026", "165170", "novice_teacher_rate_1_to_3_years")["value"]) == 9.80

    adair = _one(combined, "2026", "001010", "novice_teacher_rate_less_than_1_year")
    assert float(adair["value"]) == 9.30
    assert adair["district"] == "Adair County"
    assert adair["school_name"] == "Adair County High School"
    assert adair["nces_id"] == "210003000001"
    assert adair["co_op"] == "GRREC"
    assert adair["co_op_code"] == "902"
    assert float(_one(combined, "2026", "001010", "novice_teacher_rate_1_to_3_years")["value"]) == 9.30

    # A published zero is a rate. James E. Bazzell Middle School is 0.00
    # with less than 1 year and 20.00 with 1-3 years.
    zero = _one(combined, "2026", "005015", "novice_teacher_rate_less_than_1_year")
    assert float(zero["value"]) == 0.0
    assert zero["school_name"] == "James E Bazzell Middle School"
    assert zero["district"] == "Allen County"
    assert float(_one(combined, "2026", "005015", "novice_teacher_rate_1_to_3_years")["value"]) == 20.0

    # Equity state row. Title I, Not Title I, and Equity Gap are separate rows.
    title = _one(combined, "2026", "999000", "students_taught_by_inexperienced_teachers_rate_title_1")
    assert float(title["value"]) == 65.8
    assert title["district"] == "All Districts"
    assert title["source_file"] == "KYRC26_OVW_Students_Taught_by_Inexperienced_Teachers.csv"
    assert float(_one(
        combined, "2026", "999000", "students_taught_by_inexperienced_teachers_rate_not_title_1"
    )["value"]) == 34.2
    assert float(_one(
        combined, "2026", "999000", "students_taught_by_inexperienced_teachers_rate_equity_gap"
    )["value"]) == 31.6
    white = _one(
        combined, "2026", "999000",
        "students_taught_by_inexperienced_teachers_rate_title_1",
        "White (non-Hispanic)",
    )
    assert float(white["value"]) == 64.9

    bryan_students = _one(
        combined, "2026", "165170", "students_taught_by_inexperienced_teachers_rate_title_1"
    )
    assert float(bryan_students["value"]) == 80.0
    assert bryan_students["school_name"] == "Bryan Station High School"
    assert float(_one(combined, "2026", "001010", "students_taught_by_inexperienced_teachers_rate_title_1")["value"]) == 59.1

    # Adair County's equity-gap row is negative on every demographic. Those
    # cells are not rates bounded at zero, and the current parser does not
    # store them. Bardstown's English-learner gap is the only negative cell
    # on that row, so the other groups remain.
    assert _rows(
        combined, "2026", "001000", "students_taught_by_inexperienced_teachers_rate_equity_gap"
    ).empty
    assert float(_one(
        combined, "2026", "001000", "students_taught_by_inexperienced_teachers_rate_title_1"
    )["value"]) == 57.6
    assert _rows(
        combined, "2026", "017000",
        "students_taught_by_inexperienced_teachers_rate_equity_gap",
        "English Learner",
    ).empty
    assert float(_one(
        combined, "2026", "017000",
        "students_taught_by_inexperienced_teachers_rate_equity_gap",
    )["value"]) == 46.6

    # Prior-year anchors. 2024 equity is the historical file: state Title I
    # All Students is 11.3, not the datasets copy's 66.0.
    row = _one(combined, "2025", "999000", "novice_teacher_rate_less_than_1_year")
    assert float(row["value"]) == 5.7
    assert row["district"] == "All Districts"
    assert row["school_name"] == "---District Total---"
    assert row["source_file"] == "KYRC25_OVW_Inexperienced_Teachers.csv"
    assert float(_one(combined, "2025", "999000", "novice_teacher_rate_1_to_3_years")["value"]) == 15.6
    assert float(_one(combined, "2025", "165170", "novice_teacher_rate_less_than_1_year")["value"]) == 3.8
    assert float(_one(combined, "2025", "165170", "novice_teacher_rate_1_to_3_years")["value"]) == 6.8
    assert float(_one(combined, "2025", "001010", "novice_teacher_rate_less_than_1_year")["value"]) == 1.8
    assert float(_one(
        combined, "2025", "999000", "students_taught_by_inexperienced_teachers_rate_title_1"
    )["value"]) == 66.5
    assert float(_one(
        combined, "2025", "999000", "students_taught_by_inexperienced_teachers_rate_not_title_1"
    )["value"]) == 40.5
    assert float(_one(
        combined, "2025", "999000", "students_taught_by_inexperienced_teachers_rate_equity_gap"
    )["value"]) == 26.0
    assert float(_one(
        combined, "2025", "165170", "students_taught_by_inexperienced_teachers_rate_title_1"
    )["value"]) == 71.3

    row = _one(combined, "2024", "999000", "novice_teacher_rate_less_than_1_year")
    assert float(row["value"]) == 5.8
    assert row["district"] == "All Districts"
    assert row["source_file"] == "KYRC24_OVW_Inexperienced_Teachers.csv"
    assert float(_one(combined, "2024", "999000", "novice_teacher_rate_1_to_3_years")["value"]) == 15.5
    assert float(_one(combined, "2024", "165170", "novice_teacher_rate_less_than_1_year")["value"]) == 8.3
    assert float(_one(combined, "2024", "165170", "novice_teacher_rate_1_to_3_years")["value"]) == 8.3
    assert float(_one(
        combined, "2024", "999000", "students_taught_by_inexperienced_teachers_rate_title_1"
    )["value"]) == 11.3
    assert float(_one(
        combined, "2024", "999000", "students_taught_by_inexperienced_teachers_rate_not_title_1"
    )["value"]) == 1.3
    assert float(_one(
        combined, "2024", "999000", "students_taught_by_inexperienced_teachers_rate_equity_gap"
    )["value"]) == 9.9
    # Published zeros. The historical KYRC24 Bryan Station equity row is 0.
    assert float(_one(
        combined, "2024", "165170", "students_taught_by_inexperienced_teachers_rate_title_1"
    )["value"]) == 0.0

    row = _one(combined, "2023", "999", "novice_teacher_rate_less_than_1_year")
    assert float(row["value"]) == 7.13
    assert row["district"] == "State"
    assert row["school_name"] == "---State Total---"
    assert row["source_file"] == "inexperienced_teachers_2023.csv"
    assert float(_one(combined, "2023", "999", "novice_teacher_rate_1_to_3_years")["value"]) == 15.48
    # The historical file types this district total as 20022023. The ending
    # year is still 2023.
    breathitt = _one(combined, "2023", "061", "novice_teacher_rate_less_than_1_year")
    assert float(breathitt["value"]) == 13.04
    assert breathitt["district"] == "Breathitt County"
    assert breathitt["school_name"] == "---District Total---"
    assert float(_one(combined, "2023", "061", "novice_teacher_rate_1_to_3_years")["value"]) == 20.0
    assert float(_one(
        combined, "2023", "999", "students_taught_by_inexperienced_teachers_rate_title_1"
    )["value"]) == 26.16
    assert float(_one(
        combined, "2023", "061", "students_taught_by_inexperienced_teachers_rate_title_1"
    )["value"]) == 9.79

    row = _one(combined, "2022", "999", "novice_teacher_rate_less_than_1_year")
    assert float(row["value"]) == 4.91
    assert row["district"] == "State"
    assert row["school_name"] == "---State Total---"
    assert float(_one(combined, "2022", "999", "novice_teacher_rate_1_to_3_years")["value"]) == 17.81
    # Blank rate rows are not offered. The second Model Laboratory row is
    # blank and does not add a second copy of the published rates.
    assert _rows(combined, "2022", "602", "novice_teacher_rate_less_than_1_year").empty
    assert _rows(combined, "2022", "603", "novice_teacher_rate_1_to_3_years").empty
    assert _rows(combined, "2022", "777", "novice_teacher_rate_less_than_1_year").empty
    model = _one(combined, "2022", "606", "novice_teacher_rate_1_to_3_years")
    assert float(model["value"]) == 14.04
    assert float(_one(combined, "2022", "606", "novice_teacher_rate_less_than_1_year")["value"]) == 5.26
    assert len(_rows(combined, "2022", "606", "novice_teacher_rate_1_to_3_years")) == 1

    # 2020 has a new-teacher count and no rate. 2021 has one combined
    # inexperienced rate. Neither is one of the two split rates, so those
    # institutional files emit no novice-teacher KPI.
    assert combined.loc[
        combined["source_file"].isin([
            "inexperienced_teachers_2020.csv",
            "inexperienced_teachers_2021.csv",
        ])
    ].empty
    assert float(_one(
        combined, "2020", "999", "students_taught_by_inexperienced_teachers_rate_title_1"
    )["value"]) == 55.65
    assert _one(
        combined, "2020", "999", "students_taught_by_inexperienced_teachers_rate_title_1"
    )["district"] == "State"
    assert _one(
        combined, "2020", "999", "students_taught_by_inexperienced_teachers_rate_title_1"
    )["school_name"] == "---State Total---"

    # 2018-19 publishes the first-year rate only. The state school code is
    # built as 999000. A zero new-teacher count with a blank percent emits
    # nothing.
    first_year = _one(combined, "2019", "999000", "novice_teacher_rate_less_than_1_year")
    assert float(first_year["value"]) == 7.10346186
    assert first_year["district"] == "State"
    assert first_year["school_name"] == "---State Total---"
    assert first_year["source_file"] == "new_teacher_count_18-19.csv"
    assert _rows(combined, "2019", "999000", "novice_teacher_rate_1_to_3_years").empty
    assert float(_one(combined, "2019", "165170", "novice_teacher_rate_less_than_1_year")["value"]) == 7.5
    assert float(_one(combined, "2019", "165000", "novice_teacher_rate_less_than_1_year")["value"]) == 7.2
    assert float(_one(combined, "2019", "001010", "novice_teacher_rate_less_than_1_year")["value"]) == 1.9
    assert _rows(combined, "2019", "001012", "novice_teacher_rate_less_than_1_year").empty
