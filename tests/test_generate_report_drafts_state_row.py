"""State-row lookup in scripts/generate_report_drafts.py.

Files through 2023 label the state row district 'State' (school code 999).
KYRC-era files (2024+) label it 'All Districts' (school code 999000).
All fixture data is synthetic: 'Testville' and the values are not real.
"""
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from generate_report_drafts import get_state_average, load_data, state_row_mask  # noqa: E402

COLUMNS = ["year", "metric", "district", "school_name", "student_group",
           "value", "suppressed", "school_code"]


def _row(year, district, code, value):
    return [year, "synthetic_rate_test", district, "---District Total---",
            "All Students", value, "N", code]


@pytest.fixture
def kpi_csv(tmp_path):
    rows = [
        _row(2023, "State", "999", 61.5),
        _row(2024, "All Districts", "999000", 72.25),
        _row(2024, "Testville", "901000", 50.0),
    ]
    path = tmp_path / "kpi_synthetic.csv"
    pd.DataFrame(rows, columns=COLUMNS).to_csv(path, index=False)
    return path


def test_state_average_found_for_kyrc_all_districts_label(kpi_csv):
    _, state_df = load_data(kpi_csv, county="Testville")
    assert get_state_average(state_df, "synthetic_rate_test", 2024) == 72.25


def test_state_average_still_found_for_legacy_state_label(kpi_csv):
    _, state_df = load_data(kpi_csv, county="Testville")
    assert get_state_average(state_df, "synthetic_rate_test", 2023) == 61.5


def test_state_mask_prefers_school_code_over_label():
    df = pd.DataFrame({
        "district": ["Testville", "Testville"],
        "school_code": ["999000", "901000"],
    })
    assert state_row_mask(df).tolist() == [True, False]


def test_state_mask_falls_back_to_label_without_school_code():
    df = pd.DataFrame({"district": ["State", "All Districts", "Testville"]})
    assert state_row_mask(df).tolist() == [True, True, False]
