"""End-to-end tests for postsecondary enrollment ETL"""
import pandas as pd
import tempfile
import shutil
from pathlib import Path
from etl.postsecondary_enrollment import transform


class TestPostsecondaryEnrollmentE2E:
    def setup_method(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.temp_dir / "raw"
        self.proc_dir = self.temp_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        self.source_dir = self.raw_dir / "postsecondary_enrollment"
        self.source_dir.mkdir(parents=True)

    def teardown_method(self):
        shutil.rmtree(self.temp_dir)

    def test_full_pipeline(self):
        data = pd.DataFrame({
            "School Year": ["20232024"],
            "District Name": ["Test District"],
            "School Code": ["1001"],
            "School Name": ["Test School"],
            "Demographic": ["All Students"],
            "Total In Group": [100],
            "Public College Enrolled In State": [40],
            "Private College Enrolled In State": [20],
            "College Enrolled In State": [60],
            "Percentage Public College Enrolled In State": ["40%"],
            "Percentage Private College Enrolled In State": ["20%"],
            "Percentage College Enrolled In State": ["60%"],
        })
        data.to_csv(self.source_dir / "sample.csv", index=False)

        transform(self.raw_dir, self.proc_dir, {"derive": {"processing_date": "2024-07-19"}})
        out_file = self.proc_dir / "postsecondary_enrollment.csv"
        assert out_file.exists()
        df = pd.read_csv(out_file)
        assert len(df) == 7
        assert set(df["metric"].unique()) == {
            "postsecondary_enrollment_total_cohort",
            "postsecondary_enrollment_public_count",
            "postsecondary_enrollment_private_count",
            "postsecondary_enrollment_total_count",
            "postsecondary_enrollment_public_rate",
            "postsecondary_enrollment_private_rate",
            "postsecondary_enrollment_total_rate",
        }

