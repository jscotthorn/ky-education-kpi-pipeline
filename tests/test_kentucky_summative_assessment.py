"""Unit tests for Kentucky Summative Assessment ETL"""
import tempfile
import shutil
from pathlib import Path
import pandas as pd
from etl.kentucky_summative_assessment import (
    KentuckySummativeAssessmentETL,
    transform,
)


class TestKentuckySummativeAssessmentETL:
    def setup_method(self):
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        self.sample_dir = self.raw_dir / "kentucky_summative_assessment"
        self.sample_dir.mkdir(parents=True)
        self.etl = KentuckySummativeAssessmentETL("kentucky_summative_assessment")

    def teardown_method(self):
        shutil.rmtree(self.test_dir)

    def sample_grade_data(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "School Year": ["20232024"],
                "County Name": ["FAYETTE"],
                "District Name": ["Fayette County"],
                "School Name": ["Test School"],
                "School Code": ["165101"],
                "Grade": ["Grade 10"],
                "Subject": ["Mathematics"],
                "Demographic": ["All Students"],
                "Suppressed": ["N"],
                "Novice": [30],
                "Apprentice": [25],
                "Proficient": [35],
                "Distinguished": [10],
                "Proficient / Distinguished": [45],
            }
        )

    def sample_level_data(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "SCHOOL YEAR": ["20222023"],
                "COUNTY NAME": ["FAYETTE"],
                "DISTRICT NAME": ["Fayette County"],
                "SCHOOL NAME": ["Test School"],
                "SCHOOL CODE": ["165101"],
                "LEVEL": ["HS"],
                "SUBJECT": ["MA"],
                "DEMOGRAPHIC": ["All Students"],
                "SUPPRESSED": ["N"],
                "NOVICE": [20],
                "APPRENTICE": [30],
                "PROFICIENT": [40],
                "DISTINGUISHED": [10],
                "PROFICIENT/DISTINGUISHED": [50],
            }
        )

    def test_normalize_column_names(self):
        df = self.sample_grade_data()
        norm = self.etl.normalize_column_names(df)
        assert "subject" in norm.columns
        assert "grade" in norm.columns
        df2 = self.sample_level_data()
        norm2 = self.etl.normalize_column_names(df2)
        assert "level" in norm2.columns
        assert "subject" in norm2.columns

    def test_extract_metrics_grade(self):
        df = self.sample_grade_data()
        df = self.etl.normalize_column_names(df)
        df = self.etl.standardize_missing_values(df)
        df = self.etl.normalize_grade_field(df)
        row = df.iloc[0]
        metrics = self.etl.extract_metrics(row)
        assert "mathematics_novice_rate_grade_10" in metrics
        assert metrics["mathematics_proficient_rate_grade_10"] == 35

    def test_convert_to_kpi_format_level(self):
        df = self.sample_level_data()
        df = self.etl.normalize_column_names(df)
        df = self.etl.standardize_missing_values(df)
        df["source_file"] = "test.csv"
        kpi = self.etl.convert_to_kpi_format(df, "test.csv")
        assert not kpi.empty
        metrics = kpi["metric"].unique().tolist()
        assert "mathematics_novice_rate_high" in metrics

    def test_full_transform(self):
        # create sample files
        grade_df = self.sample_grade_data()
        level_df = self.sample_level_data()
        grade_df.to_csv(self.sample_dir / "grade.csv", index=False)
        level_df.to_csv(self.sample_dir / "level.csv", index=False)
        config = {"derive": {"processing_date": "2024-07-22"}}
        transform(self.raw_dir, self.proc_dir, config)
        output = self.proc_dir / "kentucky_summative_assessment.csv"
        assert output.exists()
        out_df = pd.read_csv(output)
        assert not out_df.empty
        assert "mathematics_proficient_rate_grade_10" in out_df["metric"].values
        assert "mathematics_novice_rate_high" in out_df["metric"].values
