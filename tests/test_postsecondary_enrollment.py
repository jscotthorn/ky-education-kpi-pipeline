"""Tests for Postsecondary Enrollment ETL module"""
import pandas as pd
from pathlib import Path
import tempfile
import shutil
from etl.postsecondary_enrollment import (
    PostsecondaryEnrollmentETL,
    transform,
)


class TestPostsecondaryEnrollmentETL:
    def setup_method(self):
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        self.sample_dir = self.raw_dir / "postsecondary_enrollment"
        self.sample_dir.mkdir(parents=True)
        self.etl = PostsecondaryEnrollmentETL("postsecondary_enrollment")

    def teardown_method(self):
        shutil.rmtree(self.test_dir)

    def create_sample_data(self):
        return pd.DataFrame({
            "School Year": ["20232024"],
            "District Name": ["Test District"],
            "School Code": ["1001"],
            "School Name": ["Test School"],
            "Demographic": ["All Students"],
            "Total In Group": [100],
            "Public College Enrolled In State": [40],
            "Private College Enrolled In State": [20],
            "College Enrolled In State": [60],
            "Percentage Public College Enrolled In State": ["40.0%"],
            "Percentage Private College Enrolled In State": ["20.0%"],
            "Percentage College Enrolled In State": ["60.0%"],
        })

    def test_normalize_column_names(self):
        df = self.create_sample_data()
        normalized = self.etl.normalize_column_names(df)
        assert "school_year" in normalized.columns
        assert "public_college_enrolled" in normalized.columns

    def test_standardize_missing_values(self):
        df = pd.DataFrame({
            "public_college_rate": ["40%", ""],
            "total_in_group": ["1,000", ""],
        })
        cleaned = self.etl.standardize_missing_values(df)
        assert cleaned.loc[0, "public_college_rate"] == 40.0
        assert pd.isna(cleaned.loc[1, "public_college_rate"])
        assert cleaned.loc[0, "total_in_group"] == 1000
        assert pd.isna(cleaned.loc[1, "total_in_group"])

    def test_convert_to_kpi_format(self):
        df = self.create_sample_data()
        df = self.etl.normalize_column_names(df)
        df = self.etl.standardize_missing_values(df)
        df["source_file"] = "test.csv"
        kpi = self.etl.convert_to_kpi_format(df, "test.csv")
        metrics = set(kpi["metric"].unique())
        assert "postsecondary_enrollment_total_ky_college_rate" in metrics
        assert len(kpi) == 7

    def test_transform_integration(self):
        df = self.create_sample_data()
        df.to_csv(self.sample_dir / "sample.csv", index=False)
        transform(self.raw_dir, self.proc_dir, {"derive": {"processing_date": "2024-07-19"}})
        output_file = self.proc_dir / "postsecondary_enrollment.csv"
        assert output_file.exists()
        out_df = pd.read_csv(output_file)
        assert len(out_df) == 7

