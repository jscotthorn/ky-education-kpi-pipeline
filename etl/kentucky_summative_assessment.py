"""Kentucky Summative Assessment ETL Module

Processes accountability assessment performance files and assessment performance by grade
files to generate KPI metrics per subject and school level or grade.
"""
from pathlib import Path
from typing import Dict, Any
import pandas as pd
import logging
import sys

etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from base_etl import BaseETL, Config

logger = logging.getLogger(__name__)


class KentuckySummativeAssessmentETL(BaseETL):
    """ETL module for Kentucky Summative Assessment data."""

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            # Subject/grade/level fields
            "Subject": "subject",
            "SUBJECT": "subject",
            "Grade": "grade",
            "GRADE": "grade",
            "LEVEL": "level",
            # Proficiency columns
            "Novice": "novice",
            "NOVICE": "novice",
            "Apprentice": "apprentice",
            "APPRENTICE": "apprentice",
            "Proficient": "proficient",
            "PROFICIENT": "proficient",
            "Distinguished": "distinguished",
            "DISTINGUISHED": "distinguished",
            "Proficient / Distinguished": "proficient_distinguished",
            "PROFICIENT/DISTINGUISHED": "proficient_distinguished",
        }

    def _normalize_level(self, level: Any) -> str:
        mapping = {
            "ES": "elementary",
            "MS": "middle",
            "HS": "high",
            "Elementary School": "elementary",
            "Middle School": "middle",
            "High School": "high",
        }
        if level is None or pd.isna(level) or level == "":
            return "all"
        return mapping.get(str(level), str(level).lower())

    def _normalize_subject(self, subject: Any) -> str:
        mapping = {
            "MA": "mathematics",
            "Mathematics": "mathematics",
            "RD": "reading",
            "Reading": "reading",
            "WR": "writing",
            "On Demand Writing": "writing",
            "SC": "science",
            "Science": "science",
            "SS": "social_studies",
            "Social Studies": "social_studies",
            "CW": "editing_mechanics",
            "Editing and Mechanics": "editing_mechanics",
        }
        if subject is None or pd.isna(subject) or subject == "":
            return "unknown"
        return mapping.get(str(subject), str(subject).lower().replace(" ", "_"))

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        metrics: Dict[str, Any] = {}
        subject = self._normalize_subject(row.get("subject"))
        grade = row.get("grade")
        level = row.get("level")

        if pd.notna(grade) and grade != "":
            suffix = self.normalize_grade_field(pd.DataFrame({"grade": [grade]}))[
                "grade"
            ].iloc[0]
        else:
            suffix = self._normalize_level(level)

        score_map = {
            "novice": row.get("novice", pd.NA),
            "apprentice": row.get("apprentice", pd.NA),
            "proficient": row.get("proficient", pd.NA),
            "distinguished": row.get("distinguished", pd.NA),
            "proficient_distinguished": row.get("proficient_distinguished", pd.NA),
        }

        for score_name, value in score_map.items():
            if pd.notna(value):
                metrics[f"{subject}_{score_name}_rate_{suffix}"] = value
        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        subject = self._normalize_subject(row.get("subject"))
        grade = row.get("grade")
        level = row.get("level")
        if pd.notna(grade) and grade != "":
            suffix = self.normalize_grade_field(pd.DataFrame({"grade": [grade]}))[
                "grade"
            ].iloc[0]
        else:
            suffix = self._normalize_level(level)
        return {
            f"{subject}_novice_rate_{suffix}": pd.NA,
            f"{subject}_apprentice_rate_{suffix}": pd.NA,
            f"{subject}_proficient_rate_{suffix}": pd.NA,
            f"{subject}_distinguished_rate_{suffix}": pd.NA,
            f"{subject}_proficient_distinguished_rate_{suffix}": pd.NA,
        }

    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        df = super().standardize_missing_values(df)
        rate_cols = [
            c
            for c in [
                "novice",
                "apprentice",
                "proficient",
                "distinguished",
                "proficient_distinguished",
            ]
            if c in df.columns
        ]
        for col in rate_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            invalid = (df[col] < 0) | (df[col] > 100)
            if invalid.any():
                logger.warning(f"Invalid values in {col}: {invalid.sum()}")
                df.loc[invalid, col] = pd.NA
        return df


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    etl = KentuckySummativeAssessmentETL("kentucky_summative_assessment")
    etl.transform(raw_dir, proc_dir, cfg)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    raw_dir = Path(__file__).parent.parent / "data" / "raw"
    proc_dir = Path(__file__).parent.parent / "data" / "processed"
    proc_dir.mkdir(exist_ok=True)

    test_config = Config(derive={"processing_date": "2025-07-22"}).dict()
    transform(raw_dir, proc_dir, test_config)
