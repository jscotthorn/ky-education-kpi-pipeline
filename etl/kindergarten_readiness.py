"""
Kindergarten Readiness ETL Module

Refactored to use BaseETL for standardized processing and KPI generation.
Handles kindergarten readiness data in both percentage and count formats
across multiple years.
"""
from pathlib import Path
import pandas as pd
from typing import Dict, Any
import logging

import sys

# Add etl directory to path for imports
etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from base_etl import BaseETL, Config


def _slugify(value: Any) -> str:
    """Convert a string to a metric-friendly slug."""
    if pd.isna(value):
        return "unknown"
    slug = (
        str(value)
        .lower()
        .strip()
        .replace(" ", "_")
        .replace("/", "_")
        .replace("-", "_")
    )
    slug = slug.replace("__", "_")
    return "".join(ch for ch in slug if ch.isalnum() or ch == "_")

logger = logging.getLogger(__name__)


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

        prior_setting = row.get("prior_setting")
        prior_slug = _slugify(prior_setting) if pd.notna(prior_setting) else None
        is_prior_all = pd.isna(prior_setting) or str(prior_setting).strip().lower() == "all students"

        if (
            pd.notna(row.get("ready_with_interventions_count"))
            and pd.notna(row.get("ready_count"))
            and pd.notna(row.get("ready_with_enrichments_count"))
        ):
            # 2024 count format
            rwi = float(row["ready_with_interventions_count"])
            r = float(row["ready_count"])
            rwe = float(row["ready_with_enrichments_count"])
            total_tested = rwi + r + rwe

            if is_prior_all:
                metrics[
                    "kindergarten_ready_with_interventions_count"
                ] = rwi
                metrics["kindergarten_ready_count"] = r
                metrics[
                    "kindergarten_ready_with_enrichments_count"
                ] = rwe

                if total_tested > 0:
                    metrics[
                        "kindergarten_ready_with_interventions_rate"
                    ] = (rwi / total_tested) * 100
                    metrics["kindergarten_ready_rate"] = (
                        r / total_tested
                    ) * 100
                    metrics[
                        "kindergarten_ready_with_enrichments_rate"
                    ] = (rwe / total_tested) * 100

                if pd.notna(row.get("total_ready_count")):
                    ready = float(row["total_ready_count"])
                    metrics["kindergarten_readiness_count"] = ready
                    metrics["kindergarten_readiness_total"] = total_tested
                    if total_tested > 0:
                        metrics["kindergarten_readiness_rate"] = (
                            ready / total_tested
                        ) * 100

            if (
                prior_slug
                and not is_prior_all
                and str(row.get("demographic", "")).strip().lower() == "all students"
                and pd.notna(row.get("total_ready_count"))
            ):
                ready = float(row["total_ready_count"])
                metrics[f"kindergarten_{prior_slug}_count"] = ready
                if total_tested > 0:
                    metrics[f"kindergarten_{prior_slug}_rate"] = (
                        ready / total_tested
                    ) * 100
            return metrics

        # Percentage formats
        rate = row.get("total_percent_ready")
        if pd.isna(rate):
            rate = row.get("total_ready_count")  # some 2024 files label rate this way
        total = row.get("number_tested")
        if pd.notna(total):
            total = float(total)

        if is_prior_all and pd.notna(rate):
            metrics["kindergarten_readiness_rate"] = float(rate)
        if is_prior_all and pd.notna(total):
            metrics["kindergarten_readiness_total"] = total
            if pd.notna(rate):
                metrics["kindergarten_readiness_count"] = round(
                    (float(rate) / 100) * total
                )

        if (
            prior_slug
            and not is_prior_all
            and str(row.get("demographic", "")).strip().lower() == "all students"
            and pd.notna(total)
            and pd.notna(rate)
        ):
            metrics[f"kindergarten_{prior_slug}_rate"] = float(rate)
            metrics[f"kindergarten_{prior_slug}_count"] = round(
                (float(rate) / 100) * total
            )

        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        metrics = {
            "kindergarten_readiness_rate": pd.NA,
            "kindergarten_readiness_count": pd.NA,
            "kindergarten_readiness_total": pd.NA,
            "kindergarten_ready_with_interventions_count": pd.NA,
            "kindergarten_ready_with_interventions_rate": pd.NA,
            "kindergarten_ready_count": pd.NA,
            "kindergarten_ready_rate": pd.NA,
            "kindergarten_ready_with_enrichments_count": pd.NA,
            "kindergarten_ready_with_enrichments_rate": pd.NA,
        }

        prior_setting = row.get("prior_setting")
        if (
            pd.notna(prior_setting)
            and str(row.get("demographic", "")).strip().lower() == "all students"
            and str(prior_setting).strip().lower() != "all students"
            and ("total_percent_ready" in row.index or "number_tested" in row.index)
        ):
            slug = _slugify(prior_setting)
            metrics[f"kindergarten_{slug}_count"] = pd.NA
            metrics[f"kindergarten_{slug}_rate"] = pd.NA
            
        return metrics

    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        df = super().standardize_missing_values(df)
        df = clean_readiness_data(df)
        if "suppressed" in df.columns:
            df["suppressed"] = df["suppressed"].replace({"Yes": "Y", "No": "N"}).fillna("N")
        else:
            df["suppressed"] = "N"
        return df

    def create_kpi_template(self, row: pd.Series, source_file: str) -> Dict[str, Any]:
        """
        Override to handle kindergarten-specific suppression logic.
        
        For kindergarten readiness, KDE marks records as suppressed even when
        values exist. We override the suppression flag when actual data is present.
        """
        template = super().create_kpi_template(row, source_file)
        
        # Override suppression if we have actual readiness data
        # Check if total_percent_ready or component readiness values exist
        has_readiness_data = (
            pd.notna(row.get("total_percent_ready")) or
            pd.notna(row.get("total_ready_count")) or
            (pd.notna(row.get("ready_with_interventions_count")) and
             pd.notna(row.get("ready_count")) and 
             pd.notna(row.get("ready_with_enrichments_count")))
        )
        
        if has_readiness_data and template['suppressed'] == 'Y':
            template['suppressed'] = 'N'
            
        return template


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Entry point for pipeline."""
    etl = KindergartenReadinessETL("kindergarten_readiness")
    etl.process(raw_dir, proc_dir, cfg)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    root = Path(__file__).parent.parent
    conf = Config(derive={})
    transform(root / "data" / "raw", root / "data" / "processed", conf.dict())
