import os
import tempfile
import shutil
from pathlib import Path
import pandas as pd
import pytest

from etl.constants import KPI_COLUMNS
import sys

sys.path.append(str(Path(__file__).parent.parent))

from etl_runner import combine_kpi_files

# Check if pyarrow is available
try:
    import pyarrow
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False


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
        df1.to_csv(self.proc_dir / "file1.csv", index=False)
        df2.to_csv(self.proc_dir / "file2.csv", index=False)

    @pytest.mark.skipif(not PYARROW_AVAILABLE, reason="pyarrow not available")
    def test_combination_outputs_csv_and_parquet(self):
        self.create_sample_files()

        csv_path = self.kpi_dir / "kpi_master.csv"
        parquet_path = self.kpi_dir / "kpi_master.parquet"

        combine_kpi_files(self.proc_dir, csv_path, parquet_path)

        assert csv_path.exists()
        assert parquet_path.exists()

        csv_df = pd.read_csv(csv_path)
        parquet_df = pd.read_parquet(parquet_path)

        assert len(csv_df) == 3
        assert len(parquet_df) == 3
        assert list(csv_df.columns) == KPI_COLUMNS
        pd.testing.assert_frame_equal(csv_df, parquet_df[csv_df.columns])

    @pytest.mark.skipif(not PYARROW_AVAILABLE, reason="pyarrow not available")
    def test_row_counts_match(self):
        """Combined files should match the total rows from processed CSVs."""
        self.create_sample_files()

        csv_path = self.kpi_dir / "kpi_master.csv"

        # Only pass csv path to test default parquet path generation
        combine_kpi_files(self.proc_dir, csv_path)

        parquet_path = csv_path.with_suffix(".parquet")

        assert csv_path.exists()
        assert parquet_path.exists()

        total_rows = 0
        for csv_file in self.proc_dir.glob("*.csv"):
            total_rows += len(pd.read_csv(csv_file))

        csv_df = pd.read_csv(csv_path)
        parquet_df = pd.read_parquet(parquet_path)

        assert len(csv_df) == total_rows
        assert len(parquet_df) == total_rows
    
    def test_csv_output_only(self):
        """Test that CSV output works even without pyarrow."""
        self.create_sample_files()

        csv_path = self.kpi_dir / "kpi_master.csv"
        combine_kpi_files(self.proc_dir, csv_path)

        assert csv_path.exists()
        
        total_rows = 0
        for csv_file in self.proc_dir.glob("*.csv"):
            total_rows += len(pd.read_csv(csv_file))

        csv_df = pd.read_csv(csv_path)
        assert len(csv_df) == total_rows
