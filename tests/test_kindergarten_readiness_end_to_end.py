"""End-to-end integration tests for Kindergarten Readiness ETL"""
import shutil
import tempfile
from pathlib import Path

import pandas as pd
from etl.kindergarten_readiness import transform


class TestKindergartenReadinessEndToEnd:
    def setup_method(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.temp_dir / "raw"
        self.proc_dir = self.temp_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        self.sample_dir = self.raw_dir / "kindergarten_readiness"
        self.sample_dir.mkdir(parents=True)

    def teardown_method(self):
        shutil.rmtree(self.temp_dir)

    def create_counts_data(self):
        return pd.DataFrame({
            "School Year": ["20232024"],
            "District Name": ["Fayette County"],
            "School Code": ["1001"],
            "School Name": ["Test School"],
            "Demographic": ["All Students"],
            "Ready With Interventions": [10],
            "Ready": [20],
            "Ready With Enrichments": [5],
            "Total Ready": [35],
            "Suppressed": ["N"],
        })

    def create_percent_data(self):
        return pd.DataFrame({
            "SCHOOL YEAR": ["20212022"],
            "DISTRICT NAME": ["Fayette County"],
            "SCHOOL CODE": ["1001"],
            "SCHOOL NAME": ["Test School"],
            "DEMOGRAPHIC": ["All Students"],
            "TOTAL PERCENT READY": [55.0],
            "NUMBER TESTED": [100],
            "SUPPRESSED": ["N"],
        })

    def test_end_to_end_single_file(self):
        df = self.create_counts_data()
        df.to_csv(self.sample_dir / "counts.csv", index=False)
        transform(self.raw_dir, self.proc_dir, {"derive": {"processing_date": "2025-07-20"}})
        out_file = self.proc_dir / "kindergarten_readiness.csv"
        assert out_file.exists()
        result = pd.read_csv(out_file)
        assert len(result) == 3  # rate, count, total
        assert set(result["metric"].unique()) == {
            "kindergarten_readiness_rate",
            "kindergarten_readiness_count",
            "kindergarten_readiness_total",
        }

    def test_end_to_end_multiple_files(self):
        self.create_counts_data().to_csv(self.sample_dir / "counts.csv", index=False)
        self.create_percent_data().to_csv(self.sample_dir / "percent.csv", index=False)
        transform(self.raw_dir, self.proc_dir, {"derive": {"processing_date": "2025-07-20"}})
        out_file = self.proc_dir / "kindergarten_readiness.csv"
        assert out_file.exists()
        result = pd.read_csv(out_file)
        metrics = result["metric"].unique()
        assert "kindergarten_readiness_rate" in metrics
        assert "kindergarten_readiness_count" in metrics
        assert "kindergarten_readiness_total" in metrics

