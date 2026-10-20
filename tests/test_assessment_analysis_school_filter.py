"""School-row filters for the grade 3 reading and grade 8 math analyses.

Co-op totals and state rows are aggregates. They are not schools.
"""
import sys
from pathlib import Path

import pandas as pd

SCRIPTS = Path(__file__).resolve().parents[1] / "analysis" / "scripts"
sys.path.insert(0, str(SCRIPTS))

from math_grade8_analysis import MathGrade8AnalysisDataset
from reading_grade3_analysis import ReadingGrade3AnalysisDataset


def _kpi_row(metric, school_name, district, value="52", suppressed="N", school_id="001016"):
    return {
        "year": "2019",
        "school_id": school_id,
        "school_name": school_name,
        "school_code": school_id,
        "district": district,
        "district_number": "001",
        "county_number": "001",
        "county_name": "ADAIR",
        "student_group": "All Students",
        "metric": metric,
        "value": value,
        "suppressed": suppressed,
    }


def _write_assessment(directory: Path) -> None:
    reading = "kentucky_summative_assessment_reading_proficient_distinguished_rate_grade_3"
    math = "kentucky_summative_assessment_math_proficient_distinguished_rate_grade_8"
    rows = [
        _kpi_row(reading, "Adair County Elementary School", "Adair County", "52", school_id="001016"),
        _kpi_row(reading, "---District Total---", "Adair County", "49", school_id="001000"),
        _kpi_row(reading, "---COOP Total---", "Central Kentucky Educational Cooperative", "55.1", school_id="901"),
        _kpi_row(reading, "---State Total---", "State", "52.7", school_id="999"),
        _kpi_row(reading, "---District Total---", "All Districts", "49", school_id="999000"),
        _kpi_row(reading, "Named Like A School", "State", "40", school_id="999"),
        _kpi_row(reading, "Another School", "All Districts", "41", school_id="999000"),
        _kpi_row(reading, "Suppressed School", "Adair County", "", suppressed="Y", school_id="001099"),
        _kpi_row(math, "Adair County Middle School", "Adair County", "39", school_id="001020"),
        _kpi_row(math, "---COOP Total---", "Green River Regional Educational Cooperative", "44", school_id="902"),
        _kpi_row(math, "---State Total---", "State", "37.9", school_id="999"),
        _kpi_row(math, "---District Total---", "All Districts", "39", school_id="999000"),
    ]
    frame = pd.DataFrame(rows)
    frame.to_csv(directory / "kentucky_summative_assessment.csv", index=False)


def _load(dataset_cls, processed: Path):
    dataset = dataset_cls(verbose=False)
    dataset.PROCESSED_DIR = processed
    return dataset.load_outcome_data()


def test_reading_and_math_drop_coop_and_state_rows(tmp_path):
    """A co-op total and a state row are not schools. A real school stays.

    Dropping only ``---District Total---`` leaves ``---COOP Total---`` and
    ``---State Total---`` in the All Students dataset.
    """
    _write_assessment(tmp_path)
    reading = _load(ReadingGrade3AnalysisDataset, tmp_path)
    math = _load(MathGrade8AnalysisDataset, tmp_path)

    assert list(reading["school_name"]) == ["Adair County Elementary School"]
    assert list(reading["district"]) == ["Adair County"]
    assert float(reading.iloc[0]["reading_proficiency_rate"]) == 52

    assert list(math["school_name"]) == ["Adair County Middle School"]
    assert list(math["district"]) == ["Adair County"]
    assert float(math.iloc[0]["math_proficiency_rate"]) == 39

    for frame in (reading, math):
        assert "---COOP Total---" not in set(frame["school_name"])
        assert "---State Total---" not in set(frame["school_name"])
        assert "---District Total---" not in set(frame["school_name"])
        assert "State" not in set(frame["district"])
        assert "All Districts" not in set(frame["district"])
