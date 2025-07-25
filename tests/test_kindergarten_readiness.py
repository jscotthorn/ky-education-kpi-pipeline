"""Unit tests for Kindergarten Readiness ETL module"""
import shutil
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from etl.kindergarten_readiness import (
    transform,
    clean_readiness_data,
    KindergartenReadinessETL,
)


class TestKindergartenReadinessETL:
    def setup_method(self):
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        self.sample_dir = self.raw_dir / "kindergarten_readiness"
        self.sample_dir.mkdir(parents=True)
        self.etl = KindergartenReadinessETL("kindergarten_readiness")

    def teardown_method(self):
        shutil.rmtree(self.test_dir)

    def create_sample_counts_data(self):
        return pd.DataFrame({
            "School Year": ["20232024", "20232024", "20232024"],
            "District Name": ["Fayette County", "Fayette County", "Fayette County"],
            "School Code": ["1001", "1002", "1001"],
            "School Name": ["Test School A", "Test School B", "Test School A"],
            "Demographic": ["All Students", "Female", "All Students"],
            "Ready With Interventions": [10, 5, 8],
            "Ready": [20, 10, 15],
            "Ready With Enrichments": [5, 3, 4],
            "Total Ready": [35, 18, 27],
            "Suppressed": ["N", "Y", "N"],
            "Prior Setting": ["All Students", "All Students", "Child Care"],
        })

    def create_sample_percent_data(self):
        return pd.DataFrame({
            "SCHOOL YEAR": ["20212022"],
            "DISTRICT NAME": ["Fayette County"],
            "SCHOOL CODE": ["1001"],
            "SCHOOL NAME": ["Test School A"],
            "DEMOGRAPHIC": ["All Students"],
            "TOTAL PERCENT READY": [55.0],
            "NUMBER TESTED": [100],
            "SUPPRESSED": ["N"],
            "Prior Setting": ["All Students"],
        })

    def test_normalize_column_names(self):
        df = self.create_sample_counts_data()
        result = self.etl.normalize_column_names(df)
        assert "total_ready_count" in result.columns
        assert "ready_with_interventions_count" in result.columns
        assert "school_code" in result.columns

    def test_standardize_missing_values(self):
        df = pd.DataFrame({
            "total_ready_count": ["", "*", "35"],
            "ready_with_interventions_count": [10, "", "*"],
            "suppressed": ["Yes", "No", None],
        })
        cleaned = self.etl.standardize_missing_values(df)
        assert pd.isna(cleaned.loc[0, "total_ready_count"])
        assert pd.isna(cleaned.loc[1, "total_ready_count"])
        assert cleaned.loc[2, "total_ready_count"] == 35
        assert cleaned.loc[0, "suppressed"] == "Y"
        assert cleaned.loc[1, "suppressed"] == "N"
        assert cleaned.loc[2, "suppressed"] == "N"

    def test_extract_metrics_counts(self):
        df = self.create_sample_counts_data()
        df = self.etl.normalize_column_names(df)
        df = self.etl.standardize_missing_values(df)
        row = df.iloc[0]
        metrics = self.etl.extract_metrics(row)
        expected = {
            "kindergarten_readiness_rate_all_students",
            "kindergarten_readiness_count_all_students",
            "kindergarten_readiness_total_all_students",
            "kindergarten_ready_with_interventions_count_all_students",
            "kindergarten_ready_with_interventions_rate_all_students",
            "kindergarten_ready_count_all_students",
            "kindergarten_ready_rate_all_students",
            "kindergarten_ready_with_enrichments_count_all_students",
            "kindergarten_ready_with_enrichments_rate_all_students",
        }
        assert set(metrics.keys()) == expected

    def test_extract_metrics_percentage(self):
        df = self.create_sample_percent_data()
        df = self.etl.normalize_column_names(df)
        df = self.etl.standardize_missing_values(df)
        row = df.iloc[0]
        metrics = self.etl.extract_metrics(row)
        assert metrics["kindergarten_readiness_rate_all_students"] == 55.0
        assert metrics["kindergarten_readiness_total_all_students"] == 100
        assert metrics["kindergarten_readiness_count_all_students"] == 55

    def test_full_transform_pipeline(self):
        counts = self.create_sample_counts_data()
        perc = self.create_sample_percent_data()
        counts.to_csv(self.sample_dir / "counts.csv", index=False)
        perc.to_csv(self.sample_dir / "percent.csv", index=False)
        config = {"derive": {"processing_date": "2025-07-20"}}
        transform(self.raw_dir, self.proc_dir, config)
        output_file = self.proc_dir / "kindergarten_readiness.csv"
        assert output_file.exists()
        df = pd.read_csv(output_file)
        assert len(df.columns) == 10
        assert len(df) == 23
        metrics = set(df["metric"].unique())
        expected = {
            "kindergarten_ready_with_interventions_count_all_students",
            "kindergarten_ready_count_all_students",
            "kindergarten_ready_with_enrichments_count_all_students",
            "kindergarten_ready_with_interventions_rate_all_students",
            "kindergarten_ready_rate_all_students",
            "kindergarten_ready_with_enrichments_rate_all_students",
            "kindergarten_readiness_count_all_students",
            "kindergarten_readiness_total_all_students",
            "kindergarten_readiness_rate_all_students",
            "kindergarten_child_care_count_all_students",
            "kindergarten_child_care_rate_all_students",
        }
        assert metrics == expected

