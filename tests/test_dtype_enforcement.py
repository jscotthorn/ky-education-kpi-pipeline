import tempfile
import shutil
from pathlib import Path
import pandas as pd
from etl_runner import combine_kpi_files

class TestDtypeEnforcement:
    def setup_method(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.proc_dir = self.temp_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        self.kpi_dir = self.temp_dir / "kpi"
        self.kpi_dir.mkdir(parents=True)

    def teardown_method(self):
        shutil.rmtree(self.temp_dir)

    def test_value_column_cast_to_float(self):
        df = pd.DataFrame({
            "district": ["Fayette"],
            "school_id": ["1001"],
            "school_name": ["School A"],
            "year": ["2024"],
            "student_group": ["All"],
            "metric": ["sample_metric"],
            "value": ["95.0"],
            "suppressed": ["N"],
            "source_file": ["file.csv"],
            "last_updated": ["2025-07-21"],
        })
        df.to_csv(self.proc_dir / "sample.csv", index=False)

        csv_path = self.kpi_dir / "kpi_master.csv"
        combine_kpi_files(self.proc_dir, csv_path)

        out_df = pd.read_csv(csv_path, dtype={"value": float, "year": str})
        assert out_df["value"].dtype == "float64"
        assert out_df["district"].dtype == "object"
        assert out_df["year"].dtype == "object"
