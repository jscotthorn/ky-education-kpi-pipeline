"""
Postsecondary Enrollment ETL Module

Refactored to use BaseETL for standardized processing.
"""
from pathlib import Path
from typing import Dict, Any
import pandas as pd
import logging
import sys

etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from base_etl import BaseETL

logger = logging.getLogger(__name__)


def clean_percentage_values(df: pd.DataFrame) -> pd.DataFrame:
    """Remove percent signs and convert to numeric."""
    percentage_columns = [col for col in df.columns if "rate" in col.lower()]
    for col in percentage_columns:
        if col in df.columns:
            df[col] = (
                df[col].astype(str).str.replace("%", "").str.replace(",", "")
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def clean_numeric_values(df: pd.DataFrame) -> pd.DataFrame:
    """Remove commas and quotes from numeric columns."""
    numeric_columns = [
        "total_in_group",
        "public_college_enrolled",
        "private_college_enrolled",
        "college_enrolled_total",
    ]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(",", "").str.replace("\"", "")
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


class PostsecondaryEnrollmentETL(BaseETL):
    """ETL module for postsecondary enrollment data."""

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        """Extend base mappings with postsecondary enrollment specific fields."""
        return {
            "Total In Group": "total_in_group",
            "TOTAL IN GROUP": "total_in_group",
            "Public College Enrolled In State": "public_college_enrolled",
            "PUBLIC COLLEGE ENROLLED IN STATE": "public_college_enrolled",
            "Private College Enrolled In State": "private_college_enrolled",
            "PRIVATE COLLEGE ENROLLED IN STATE": "private_college_enrolled",
            "College Enrolled In State": "college_enrolled_total",
            "COLLEGE ENROLLED IN STATE": "college_enrolled_total",
            "Percentage Public College Enrolled In State": "public_college_rate",
            "PERCENTAGE PUBLIC COLLEGE ENROLLED IN STATE": "public_college_rate",
            "Percentage Private College Enrolled In State": "private_college_rate",
            "PERCENTAGE PRIVATE COLLEGE ENROLLED IN STATE": "private_college_rate",
            "Percentage College Enrolled In State": "college_enrollment_rate",
            "PERCENTAGE COLLEGE ENROLLED IN STATE": "college_enrollment_rate",
            "Percentage College Enrolled In State Table": "college_enrollment_rate",
        }

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        metrics = {}
        metrics["postsecondary_enrollment_total_in_cohort"] = row.get("total_in_group")
        metrics["postsecondary_enrollment_public_ky_college_count"] = row.get(
            "public_college_enrolled"
        )
        metrics["postsecondary_enrollment_private_ky_college_count"] = row.get(
            "private_college_enrolled"
        )
        metrics["postsecondary_enrollment_total_ky_college_count"] = row.get(
            "college_enrolled_total"
        )
        metrics["postsecondary_enrollment_public_ky_college_rate"] = row.get("public_college_rate")
        metrics["postsecondary_enrollment_private_ky_college_rate"] = row.get(
            "private_college_rate"
        )
        metrics["postsecondary_enrollment_total_ky_college_rate"] = row.get(
            "college_enrollment_rate"
        )
        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        return {
            "postsecondary_enrollment_total_in_cohort": pd.NA,
            "postsecondary_enrollment_public_ky_college_count": pd.NA,
            "postsecondary_enrollment_private_ky_college_count": pd.NA,
            "postsecondary_enrollment_total_ky_college_count": pd.NA,
            "postsecondary_enrollment_public_ky_college_rate": pd.NA,
            "postsecondary_enrollment_private_ky_college_rate": pd.NA,
            "postsecondary_enrollment_total_ky_college_rate": pd.NA,
        }

    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        df = super().standardize_missing_values(df)
        df = clean_percentage_values(df)
        df = clean_numeric_values(df)
        return df


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Entry point for running the ETL."""
    etl = PostsecondaryEnrollmentETL("postsecondary_enrollment")
    etl.process(raw_dir, proc_dir, cfg)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    project_root = Path(__file__).parent.parent
    raw = project_root / "data" / "raw"
    proc = project_root / "data" / "processed"
    proc.mkdir(exist_ok=True)
    transform(raw, proc, {"derive": {"processing_date": "2025-07-19"}})
