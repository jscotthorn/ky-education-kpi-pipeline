import pandas as pd
import tempfile
from pathlib import Path
import logging
import pytest

from etl.base_etl import BaseETL


class DummyETL(BaseETL):
    def __init__(self, source_name: str):
        super().__init__(source_name)
        self.logger = logging.getLogger(__name__)

    @property
    def module_column_mappings(self):
        return {}

    def extract_metrics(self, row: pd.Series):
        return {}

    def get_suppressed_metric_defaults(self, row: pd.Series):
        return {}


def test_extract_year():
    etl = DummyETL("dummy")
    data = [
        ({"school_year": "20232024"}, "2024"),
        ({"school_year": "2023"}, "2023"),
        ({"school_year": "bad"}, "2024"),
    ]
    for row, expected in data:
        result = etl.extract_year(pd.Series(row))
        assert result == expected


def test_extract_year_from_kyrc_filename():
    etl = DummyETL("dummy")
    row = pd.Series({"school_year": ""})
    assert etl.extract_year(row, "KYRC26_OVW_Chronic_Absenteeism.csv") == "2026"
    assert etl.extract_year(row, "Graduation_Rate_2025.CSV") == "2025"
    # An 8-digit school year still wins over the filename.
    assert etl.extract_year(pd.Series({"school_year": "20252026"}), "KYRC26_x.csv") == "2026"


def test_coop_header_aliases():
    etl = DummyETL("dummy")
    frame = pd.DataFrame({"CO-OP": ["A"], "Co-Op": ["B"], "Co-Op Code": ["1"]})
    renamed = etl.normalize_column_names(frame)
    assert list(renamed.columns) == ["co_op", "co_op", "co_op_code"]


def test_kyrc26_files_are_skipped_until_a_module_opts_in(tmp_path):
    source = tmp_path / "dummy"
    source.mkdir()
    (source / "KYRC25_OVW_Chronic_Absenteeism.csv").write_text("a\n")
    (source / "KYRC26_OVW_Chronic_Absenteeism.csv").write_text("a\n")

    etl = DummyETL("dummy")
    names = [path.name for path, _sheet in etl._find_data_files(source)]
    assert names == ["KYRC25_OVW_Chronic_Absenteeism.csv"]

    class ReadyETL(DummyETL):
        include_kyrc26 = True

    ready_names = sorted(path.name for path, _sheet in ReadyETL("dummy")._find_data_files(source))
    assert ready_names == [
        "KYRC25_OVW_Chronic_Absenteeism.csv",
        "KYRC26_OVW_Chronic_Absenteeism.csv",
    ]


def test_extract_school_id_requires_school_code():
    etl = DummyETL("dummy")
    # Primary school_code
    row = pd.Series({"school_code": "010203"})
    assert etl.extract_school_id(row) == "010203"

    # Any other identifier alone should cause an error
    row = pd.Series({"state_school_id": "123456"})
    with pytest.raises(ValueError):
        etl.extract_school_id(row)

    # Raise error when no identifiers present
    row = pd.Series({"school_code": "", "state_school_id": ""})
    with pytest.raises(ValueError):
        etl.extract_school_id(row)


def test_validate_demographics_logs(caplog):
    etl = DummyETL("dummy")
    df = pd.DataFrame({"year": ["2024"], "student_group": ["All Students"]})
    caplog.set_level(logging.INFO)
    results = etl._validate_demographics(df)
    assert results[0]["year"] == "2024"
    assert any("Year 2024" in rec.message for rec in caplog.records)


def test_save_outputs_creates_files(tmp_path, caplog):
    etl = DummyETL("dummy")
    kpi_df = pd.DataFrame(
        {
            "district": ["Test"],
            "school_id": ["100"],
            "school_name": ["Test School"],
            "year": ["2024"],
            "student_group": ["All Students"],
            "metric": ["demo_metric"],
            "value": [1.0],
            "suppressed": ["N"],
            "source_file": ["test.csv"],
            "last_updated": ["2025-01-01"],
        }
    )
    caplog.set_level(logging.INFO)
    etl._save_outputs(
        kpi_df,
        Path(tmp_path),
        [{"year": "2024", "valid": [], "missing_required": [], "missing_optional": [], "unexpected": []}],
    )
    assert (Path(tmp_path) / "dummy.csv").exists()
    assert (Path(tmp_path) / "dummy_demographic_report.md").exists()
    assert any("KPI data written" in rec.message for rec in caplog.records)
    assert any("Demographic report written" in rec.message for rec in caplog.records)
