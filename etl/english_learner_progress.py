"""English Learner Progress ETL Module

Refactored to derive from BaseETL for standardized processing.
"""
from pathlib import Path
import pandas as pd
from typing import Dict, Any, Union
import logging
import sys

etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from base_etl import BaseETL, Config

logger = logging.getLogger(__name__)


def clean_percentage_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Convert percentage columns to numeric and validate 0-100 range."""
    percentage_columns = [c for c in df.columns if "percentage_score" in c]
    for col in percentage_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            invalid = (df[col] < 0) | (df[col] > 100)
            if invalid.any():
                logger.warning(
                    f"Found {invalid.sum()} invalid percentage scores in {col}"
                )
                df.loc[invalid, col] = pd.NA
    return df


class EnglishLearnerProgressETL(BaseETL):
    """ETL module for processing English Learner Progress data."""

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            # Education level column is unique to this dataset
            "Level": "level",
            "LEVEL": "level",

            # Percentage score columns for proficiency bands
            "Percentage Of Value Table Score Of 0": "percentage_score_0",
            "PERCENTAGE OF VALUE TABLE SCORE OF 0": "percentage_score_0",
            "Percentage Of Value Table Score Of 60 And 80": "percentage_score_60_80",
            "PERCENTAGE OF VALUE TABLE SCORE OF 60 AND 80": "percentage_score_60_80",
            "Percentage Of Value Table Score Of 100": "percentage_score_100",
            "PERCENTAGE OF VALUE TABLE SCORE OF 100": "percentage_score_100",
            "Percentage Of Value Table Score Of 140": "percentage_score_140",
            "PERCENTAGE OF VALUE TABLE SCORE OF 140": "percentage_score_140",
        }

    def _normalize_level(self, level: Union[str, None]) -> str:
        mapping = {
            "ES": "elementary",
            "Elementary School": "elementary",
            "Elementary": "elementary",
            "MS": "middle",
            "Middle School": "middle",
            "Middle": "middle",
            "HS": "high",
            "High School": "high",
            "High": "high",
            "All": "all",
        }
        if level is None or pd.isna(level) or level == "":
            level = "All"
        return mapping.get(str(level), "all")

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        level = self._normalize_level(row.get("level"))
        metrics: Dict[str, Any] = {}
        score_map = {
            "english_learner_score_0": row.get("percentage_score_0", pd.NA),
            "english_learner_score_60_80": row.get("percentage_score_60_80", pd.NA),
            "english_learner_score_100": row.get("percentage_score_100", pd.NA),
            "english_learner_score_140": row.get("percentage_score_140", pd.NA),
        }
        for base_name, value in score_map.items():
            if pd.notna(value):
                metrics[f"{base_name}_{level}"] = float(value)
        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        level = self._normalize_level(row.get("level"))
        return {
            f"english_learner_score_0_{level}": pd.NA,
            f"english_learner_score_60_80_{level}": pd.NA,
            f"english_learner_score_100_{level}": pd.NA,
            f"english_learner_score_140_{level}": pd.NA,
        }

    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        df = super().standardize_missing_values(df)
        df = clean_percentage_scores(df)
        return df


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Entry point used by etl_runner and tests."""
    etl = EnglishLearnerProgressETL("english_learner_progress")
    etl.transform(raw_dir, proc_dir, cfg)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    raw_dir = Path(__file__).parent.parent / "data" / "raw"
    proc_dir = Path(__file__).parent.parent / "data" / "processed"
    proc_dir.mkdir(exist_ok=True)

    test_config = Config(
        derive={"processing_date": "2025-07-19", "data_quality_flag": "reviewed"}
    ).dict()

    transform(raw_dir, proc_dir, test_config)

