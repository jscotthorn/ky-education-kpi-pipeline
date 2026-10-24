"""State-row lookup in scripts/generate_report_drafts.py.

The row shapes follow the real ETL outputs:
- 2018-2023 files: district 'State', school code 999, school_name '---State Total---'.
- KYRC files (2024+, and the 2023 CTE rates): district 'All Districts',
  school code 999000, school_name '---District Total---'.
- Some outputs carry only the code (e.g. district_school_list 2026 uses
  district '---State---' with code 999000), so load_data must read school_code.
The district name 'Testville' and every value are synthetic.
"""
import os
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from generate_report_drafts import get_state_average, load_data, state_row_mask  # noqa: E402

COLUMNS = ["year", "metric", "district", "school_name", "student_group",
           "value", "suppressed", "school_code"]


def _row(year, district, code, value, school_name="---District Total---"):
    return [year, "synthetic_rate_test", district, school_name,
            "All Students", value, "N", code]


@pytest.fixture
def kpi_csv(tmp_path):
    rows = [
        _row(2023, "State", "999", 61.5, school_name="---State Total---"),
        _row(2024, "All Districts", "999000", 72.25),
        _row(2024, "Testville", "901000", 50.0),
        _row(2026, "---State---", "999000", 80.5, school_name="---State---"),
    ]
    path = tmp_path / "kpi_synthetic.csv"
    pd.DataFrame(rows, columns=COLUMNS).to_csv(path, index=False)
    return path


class _BigFilePath(type(Path())):
    """A real path whose stat() reports > 100 MB, to drive the chunked branch."""

    def stat(self, *args, **kwargs):
        real = super().stat(*args, **kwargs)
        return os.stat_result((real.st_mode, real.st_ino, real.st_dev, real.st_nlink,
                               real.st_uid, real.st_gid, 200_000_000,
                               int(real.st_atime), int(real.st_mtime), int(real.st_ctime)))


def test_state_average_found_for_kyrc_all_districts_label(kpi_csv):
    _, state_df = load_data(kpi_csv, county="Testville")
    assert get_state_average(state_df, "synthetic_rate_test", 2024) == 72.25


def test_state_average_found_for_legacy_state_total_row(kpi_csv):
    _, state_df = load_data(kpi_csv, county="Testville")
    assert get_state_average(state_df, "synthetic_rate_test", 2023) == 61.5


def test_district_average_ignores_state_rows(kpi_csv):
    from generate_report_drafts import get_district_data
    df, _ = load_data(kpi_csv, county="Testville")
    rows = get_district_data(df, "Testville", "synthetic_rate_test", 2024)
    assert rows["value"].tolist() == [50.0]


@pytest.mark.parametrize("chunked", [False, True], ids=["whole_file", "chunked"])
def test_load_data_reads_school_code_for_code_only_state_rows(kpi_csv, chunked):
    path = _BigFilePath(kpi_csv) if chunked else kpi_csv
    _, state_df = load_data(path, county="Testville")
    assert "school_code" in state_df.columns
    code_only = state_df[state_df["district"] == "---State---"]
    assert code_only["value"].tolist() == [80.5]


def test_state_mask_matches_school_code_regardless_of_label():
    df = pd.DataFrame({
        "district": ["Testville", "Testville"],
        "school_code": ["999000", "901000"],
    })
    assert state_row_mask(df).tolist() == [True, False]


def test_state_mask_keeps_label_rows_alongside_code_rows():
    # One frame (or one read_csv chunk) can hold both shapes, e.g. 2023 CTE
    # counts on State/999 and rates on All Districts/999000. A state label
    # with a blank or float-read code must not be dropped because another
    # row in the same frame matched by code.
    df = pd.DataFrame({
        "district": ["State", "All Districts", "State", "Testville"],
        "school_code": [999.0, "999000", None, "901000"],
    })
    assert state_row_mask(df).tolist() == [True, True, True, False]


def test_state_mask_falls_back_to_label_without_school_code():
    df = pd.DataFrame({"district": ["State", "All Districts", "Testville"]})
    assert state_row_mask(df).tolist() == [True, True, False]
