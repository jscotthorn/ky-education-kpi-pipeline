"""
Kindergarten Readiness ETL Module

Refactored to use BaseETL for standardized processing and KPI generation.
Handles kindergarten readiness data in both percentage and count formats
across multiple years.
"""
from pathlib import Path
import pandas as pd
from pydantic import BaseModel
from typing import Dict, Any, Union
import logging

import sys
from pathlib import Path

# Add etl directory to path for imports
etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from base_etl import BaseETL

logger = logging.getLogger(__name__)


class Config(BaseModel):
    rename: Dict[str, str] = {}
    dtype: Dict[str, str] = {}
    derive: Dict[str, Union[str, int, float]] = {}


def clean_readiness_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate readiness percentage and count fields."""
    percent_cols = [c for c in df.columns if "percent" in c.lower() and "ready" in c.lower()] + [
        "total_ready_count"
    ]
    for col in percent_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            invalid = (df[col] < 0) | (df[col] > 100)
            if invalid.any():
                df.loc[invalid, col] = pd.NA

    count_cols = [
        "ready_with_interventions_count",
        "ready_count",
        "ready_with_enrichments_count",
        "number_tested",
    ]
    for col in count_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            invalid = df[col] < 0
            if invalid.any():
                df.loc[invalid, col] = pd.NA

    return df


class KindergartenReadinessETL(BaseETL):
    """ETL module for kindergarten readiness."""

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            "TOTAL PERCENT READY": "total_percent_ready",
            "Total Percent Ready": "total_percent_ready",
            "Ready With Interventions": "ready_with_interventions_count",
            "Ready": "ready_count",
            "Ready With Enrichments": "ready_with_enrichments_count",
            "Total Ready": "total_ready_count",
            "NUMBER TESTED": "number_tested",
            "Number Tested": "number_tested",
            "Suppressed": "suppressed",
            "SUPPRESSED": "suppressed",
            "Prior Setting": "prior_setting",
            "PRIOR SETTING": "prior_setting",
        }

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        metrics: Dict[str, Any] = {}

        if (
            pd.notna(row.get("ready_with_interventions_count"))
            and pd.notna(row.get("ready_count"))
            and pd.notna(row.get("ready_with_enrichments_count"))
        ):
            # 2024 count format
            total_tested = (
                float(row["ready_with_interventions_count"])
                + float(row["ready_count"])
                + float(row["ready_with_enrichments_count"])
            )
            if pd.notna(row.get("total_ready_count")):
                ready = float(row["total_ready_count"])
                metrics["kindergarten_readiness_count"] = ready
                if total_tested > 0:
                    metrics["kindergarten_readiness_rate"] = (ready / total_tested) * 100
                metrics["kindergarten_readiness_total"] = total_tested
            return metrics

        # Percentage formats
        rate = row.get("total_percent_ready")
        if pd.isna(rate):
            rate = row.get("total_ready_count")  # some 2024 files label rate this way
        if pd.notna(rate):
            metrics["kindergarten_readiness_rate"] = float(rate)
        if pd.notna(row.get("number_tested")):
            total = float(row["number_tested"])
            metrics["kindergarten_readiness_total"] = total
            if pd.notna(rate):
                metrics["kindergarten_readiness_count"] = round((float(rate) / 100) * total)
        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        return {
            "kindergarten_readiness_rate": pd.NA,
            "kindergarten_readiness_count": pd.NA,
            "kindergarten_readiness_total": pd.NA,
        }

    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        df = super().standardize_missing_values(df)
        df = clean_readiness_data(df)
        if "suppressed" in df.columns:
            df["suppressed"] = df["suppressed"].replace({"Yes": "Y", "No": "N"}).fillna("N")
        else:
            df["suppressed"] = "N"
        return df


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Entry point for pipeline."""
    etl = KindergartenReadinessETL("kindergarten_readiness")
    etl.transform(raw_dir, proc_dir, cfg)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    root = Path(__file__).parent.parent
    transform(root / "data" / "raw", root / "data" / "processed", {"derive": {}})
