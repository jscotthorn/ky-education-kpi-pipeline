import os
import tempfile
import shutil
from pathlib import Path
import pandas as pd
import pytest

from etl.constants import KPI_COLUMNS
import sys

sys.path.append(str(Path(__file__).parent.parent))

from etl_runner import (
    DuplicateKpiKeyError,
    _assert_no_duplicate_kpi_keys,
    combine_kpi_files,
)


class TestKpiCombination:
    def setup_method(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.proc_dir = self.temp_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        self.kpi_dir = self.temp_dir / "kpi"
        self.kpi_dir.mkdir(parents=True)

    def teardown_method(self):
        shutil.rmtree(self.temp_dir)

    def create_sample_files(self):
        df1 = pd.DataFrame(
            {
                "district": ["Fayette County", "Fayette County"],
                "school_id": ["1001", "1001"],
                "school_name": ["School A", "School A"],
                "year": [2024, 2024],
                "student_group": ["All Students", "All Students"],
                "county_number": ["01", "01"],
                "county_name": ["Fayette", "Fayette"],
                "district_number": ["111", "111"],
                "school_code": ["A1", "A1"],
                "state_school_id": ["1001", "1001"],
                "nces_id": ["999999001", "999999001"],
                "co_op": ["Central" , "Central"],
                "co_op_code": ["C1", "C1"],
                "school_type": ["HS", "HS"],
                "metric": ["sample_metric_rate_4_year", "sample_metric_count_4_year"],
                "value": [95.0, 50.0],
                "suppressed": ["N", "N"],
                "source_file": ["file1.csv", "file1.csv"],
                "last_updated": ["2025-07-20", "2025-07-20"],
            }
        )
        df2 = pd.DataFrame(
            {
                "district": ["Fayette County"],
                "school_id": ["1002"],
                "school_name": ["School B"],
                "year": [2024],
                "student_group": ["All Students"],
                "county_number": ["01"],
                "county_name": ["Fayette"],
                "district_number": ["111"],
                "school_code": ["B1"],
                "state_school_id": ["1002"],
                "nces_id": ["999999002"],
                "co_op": ["Central"],
                "co_op_code": ["C1"],
                "school_type": ["HS"],
                "metric": ["sample_metric_rate_4_year"],
                "value": [88.0],
                "suppressed": ["N"],
                "source_file": ["file2.csv"],
                "last_updated": ["2025-07-20"],
            }
        )
        # combine_kpi_files writes the KPI header and the row cells in file
        # order. Processed files use KPI_COLUMNS order. These fixtures do too,
        # so the duplicate-key check reads the same cells the header names.
        df1[KPI_COLUMNS].to_csv(self.proc_dir / "file1.csv", index=False)
        df2[KPI_COLUMNS].to_csv(self.proc_dir / "file2.csv", index=False)

    def test_combination_outputs_csv(self):
        self.create_sample_files()

        csv_path = self.kpi_dir / "kpi_master.csv"

        combine_kpi_files(self.proc_dir, csv_path)

        assert csv_path.exists()

        csv_df = pd.read_csv(csv_path)

        assert len(csv_df) == 3
        assert list(csv_df.columns) == KPI_COLUMNS

    def test_row_counts_match(self):
        """Combined files should match the total rows from processed CSVs."""
        self.create_sample_files()

        csv_path = self.kpi_dir / "kpi_master.csv"

        combine_kpi_files(self.proc_dir, csv_path)

        assert csv_path.exists()

        total_rows = 0
        for csv_file in self.proc_dir.glob("*.csv"):
            total_rows += len(pd.read_csv(csv_file))

        csv_df = pd.read_csv(csv_path)

        assert len(csv_df) == total_rows
    
    def test_csv_output_only(self):
        """Test that CSV output works with default chunk size."""
        self.create_sample_files()

        csv_path = self.kpi_dir / "kpi_master.csv"
        combine_kpi_files(self.proc_dir, csv_path)

        assert csv_path.exists()
        
        total_rows = 0
        for csv_file in self.proc_dir.glob("*.csv"):
            total_rows += len(pd.read_csv(csv_file))

        csv_df = pd.read_csv(csv_path)
        assert len(csv_df) == total_rows

    def test_chunked_processing(self):
        """Test that chunked processing works with small chunk size."""
        self.create_sample_files()

        csv_path = self.kpi_dir / "kpi_master.csv"
        # Use small chunk size to test chunking logic
        combine_kpi_files(self.proc_dir, csv_path, chunk_size=1)

        assert csv_path.exists()
        
        total_rows = 0
        for csv_file in self.proc_dir.glob("*.csv"):
            total_rows += len(pd.read_csv(csv_file))

        csv_df = pd.read_csv(csv_path)
        assert len(csv_df) == total_rows
        assert list(csv_df.columns) == KPI_COLUMNS

    def test_combine_fails_on_an_injected_duplicate_key(self):
        """A second copy of one pipeline key makes combine raise.

        The rows are the combination harness. The injected row repeats the
        first key in file1.csv: district, school_id, school_name, year,
        student_group, and metric. The allowlist is empty.
        """
        self.create_sample_files()
        frame = pd.read_csv(self.proc_dir / "file1.csv")
        duplicated = pd.concat([frame, frame.iloc[[0]]], ignore_index=True)
        duplicated.to_csv(self.proc_dir / "file1.csv", index=False)

        output = self.kpi_dir / "kpi_master.csv"
        with pytest.raises(DuplicateKpiKeyError, match="Duplicate KPI keys"):
            combine_kpi_files(self.proc_dir, output)
        assert not output.exists()
        assert list(self.kpi_dir.glob("*.partial")) == []

    def test_missing_master_file_is_an_error(self):
        """The key check does not treat a missing master as success."""
        missing = self.kpi_dir / "absent.csv"
        with pytest.raises(DuplicateKpiKeyError, match="missing"):
            _assert_no_duplicate_kpi_keys(missing)

    def test_same_hash_on_different_keys_is_not_a_duplicate(self, monkeypatch):
        """A shared 8-byte hash is confirmed against the key text."""
        import numpy as np

        self.create_sample_files()

        def constant_hash(frame):
            return np.zeros(len(frame), dtype=np.uint64)

        monkeypatch.setattr("etl_runner._hash_key_frame", constant_hash)
        output = self.kpi_dir / "kpi_master.csv"
        combine_kpi_files(self.proc_dir, output)
        assert len(pd.read_csv(output)) == 3

    def test_source_column_order_is_rewritten_to_the_kpi_header(self):
        """Cells follow the KPI header, not the source file's column order."""
        row = {column: f"v-{column}" for column in KPI_COLUMNS}
        row["district"] = "Fayette County"
        row["school_id"] = "165170"
        row["school_name"] = "Bryan Station High School"
        row["year"] = "2026"
        row["student_group"] = "All Students"
        row["metric"] = "student_teacher_ratio"
        row["value"] = "15.0"
        shuffled = list(reversed(KPI_COLUMNS))
        pd.DataFrame([row])[shuffled].to_csv(
            self.proc_dir / "shuffled.csv", index=False
        )
        output = self.kpi_dir / "kpi_master.csv"
        combine_kpi_files(self.proc_dir, output)
        result = pd.read_csv(output, dtype=str)
        assert list(result.columns) == list(KPI_COLUMNS)
        assert result.loc[0, "district"] == "Fayette County"
        assert result.loc[0, "school_id"] == "165170"
        assert result.loc[0, "metric"] == "student_teacher_ratio"
        assert result.loc[0, "value"] == "15.0"

    def test_missing_kpi_column_fails_and_leaves_no_master(self):
        """A short header is not written under the fixed KPI header."""
        pd.DataFrame(
            {"district": ["Fayette County"], "year": ["2023"]}
        ).to_csv(self.proc_dir / "short.csv", index=False)
        output = self.kpi_dir / "kpi_master.csv"
        with pytest.raises(ValueError, match="missing KPI columns"):
            combine_kpi_files(self.proc_dir, output)
        assert not output.exists()
        assert list(self.kpi_dir.glob("*.partial")) == []
