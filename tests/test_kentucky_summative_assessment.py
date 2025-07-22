"""Tests for Kentucky Summative Assessment ETL module"""
import shutil
import tempfile
from pathlib import Path

import pandas as pd
import pytest

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

    def create_sample_2024_data(self):
        return pd.DataFrame({
            "School Year": ["20232024"],
            "County Name": ["ADAIR"],
            "District Name": ["Adair County"],
            "School Name": ["All Schools"],
            "School Code": ["001000"],
            "Level": ["Elementary School"],
            "Subject": ["Mathematics"],
            "Demographic": ["All Students"],
            "Suppressed": ["N"],
            "Novice": [28],
            "Apprentice": [32],
            "Proficient": [31],
            "Distinguished": [10],
            "Proficient / Distinguished": [41],
            "Content Index": [58.7],
        })

    def create_sample_2023_data(self):
        return pd.DataFrame({
            "SCHOOL YEAR": ["20222023"],
            "COUNTY NAME": ["ADAIR"],
            "DISTRICT NAME": ["Adair County"],
            "SCHOOL NAME": ["All Schools"],
            "SCHOOL CODE": ["001000"],
            "GRADE": ["03"],
            "SUBJECT": ["MA"],
            "DEMOGRAPHIC": ["All Students"],
            "SUPPRESSED": ["N"],
            "NOVICE": [28],
            "APPRENTICE": [42],
            "PROFICIENT": [22],
            "DISTINGUISHED": [7],
            "PROFICIENT/DISTINGUISHED": [29],
        })

    def test_normalize_column_names(self):
        df = self.create_sample_2024_data()
        normalized = self.etl.normalize_column_names(df)
        assert "subject" in normalized.columns
        assert "novice" in normalized.columns

    def test_extract_metrics(self):
        df = self.create_sample_2024_data()
        df = self.etl.normalize_column_names(df)
        row = df.iloc[0]
        metrics = self.etl.extract_metrics(row)
        assert metrics["math_novice_rate_elementary"] == 28.0
        assert metrics["math_content_index_score_elementary"] == 58.7

    def test_convert_to_kpi_format(self):
        df = self.create_sample_2024_data()
        df = self.etl.normalize_column_names(df)
        df = self.etl.standardize_missing_values(df)
        df["source_file"] = "test.csv"
        kpi = self.etl.convert_to_kpi_format(df, "test.csv")
        assert not kpi.empty
        assert set(kpi["metric"].unique()) == {
            "math_novice_rate_elementary",
            "math_apprentice_rate_elementary",
            "math_proficient_rate_elementary",
            "math_distinguished_rate_elementary",
            "math_proficient_distinguished_rate_elementary",
            "math_content_index_score_elementary",
        }

    def test_full_transform_pipeline(self):
        df = self.create_sample_2024_data()
        sample_file = self.sample_dir / "sample.csv"
        df.to_csv(sample_file, index=False)
        transform(self.raw_dir, self.proc_dir, {"derive": {}})
        output = self.proc_dir / "kentucky_summative_assessment.csv"
        assert output.exists()
        out_df = pd.read_csv(output)
        assert not out_df.empty

    def test_grade_and_level_metrics(self):
        df_level = self.create_sample_2024_data()
        df_grade = self.create_sample_2023_data()
        df_level.to_csv(self.sample_dir / "level.csv", index=False)
        df_grade.to_csv(self.sample_dir / "grade.csv", index=False)
        transform(self.raw_dir, self.proc_dir, {"derive": {}})
        output = self.proc_dir / "kentucky_summative_assessment.csv"
        assert output.exists()
        out_df = pd.read_csv(output)
        metrics = out_df["metric"].unique()
        assert any(m.endswith("elementary") for m in metrics)
        assert any("grade_3" in m for m in metrics)
