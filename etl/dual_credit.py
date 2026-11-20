"""
Dual Credit ETL Module

Handles Kentucky dual credit program data.
Normalizes column names, standardizes missing values, and transforms to KPI format.

Data includes:
- Dual credit courses offered by school
- Dual credit participation and performance by demographic
"""
from pathlib import Path
import pandas as pd
from typing import Dict, Any
import logging

import sys
from pathlib import Path

# Add etl directory to path for imports
etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from base_etl import BaseETL, Config

logger = logging.getLogger(__name__)


def clean_numeric_with_commas(series: pd.Series) -> pd.Series:
    """Convert strings with commas to numeric values."""
    cleaned = series.astype(str).str.replace(',', '').str.replace('"', '')
    return pd.to_numeric(cleaned, errors='coerce')


class DualCreditETL(BaseETL):
    """ETL module for processing dual credit program data."""

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            # Course offering fields
            'Advanced Course Type': 'course_type',
            'ADVANCED COURSE TYPE': 'course_type',
            'Subject': 'subject',
            'SUBJECT': 'subject',

            # Enrollment and performance metrics
            'Course Enrollment': 'course_enrollment',
            'COURSE ENROLLMENT': 'course_enrollment',
            'Course Completers': 'course_completers',
            'COURSE COMPLETERS': 'course_completers',
            'Couse Completers ': 'course_completers',  # Typo in some files
            'COUSE COMPLETERS ': 'course_completers',
            'Students With Qualifying Score': 'qualifying_score',
            'STUDENTS WITH QUALIFYING SCORE': 'qualifying_score',
            'Students With Qualifying Score ': 'qualifying_score',  # Extra space
        }

    def should_skip_row(self, row: pd.Series) -> bool:
        """
        Override to handle files with and without demographics.
        Dual credit "Courses Offered" files have no demographics.
        Dual credit "Participation and Performance" files have demographics.
        """
        # If there's a demographic column, use parent logic
        if 'demographic' in row.index and pd.notna(row.get('demographic')):
            return super().should_skip_row(row)

        # For files without demographics, don't skip any rows
        return False

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        metrics = {}

        # Extract enrollment
        enrollment = row.get('course_enrollment', pd.NA)
        if pd.notna(enrollment):
            enrollment_clean = clean_numeric_with_commas(pd.Series([enrollment])).iloc[0]
            if pd.notna(enrollment_clean) and enrollment_clean >= 0:
                metrics['dual_credit_enrollment'] = int(enrollment_clean)

        # Extract completers
        completers = row.get('course_completers', pd.NA)
        if pd.notna(completers):
            completers_clean = clean_numeric_with_commas(pd.Series([completers])).iloc[0]
            if pd.notna(completers_clean) and completers_clean >= 0:
                metrics['dual_credit_completion_count'] = int(completers_clean)

        # Extract qualifying scores
        qualifying = row.get('qualifying_score', pd.NA)
        if pd.notna(qualifying):
            qualifying_clean = clean_numeric_with_commas(pd.Series([qualifying])).iloc[0]
            if pd.notna(qualifying_clean) and qualifying_clean >= 0:
                metrics['dual_credit_qualifying_score_count'] = int(qualifying_clean)

        # Calculate completion rate if we have both enrollment and completers
        if 'dual_credit_enrollment' in metrics and 'dual_credit_completion_count' in metrics:
            if metrics['dual_credit_enrollment'] > 0:
                completion_rate = round((metrics['dual_credit_completion_count'] /
                                       metrics['dual_credit_enrollment']) * 100, 1)
                metrics['dual_credit_completion_rate'] = completion_rate

        # Calculate qualifying score rate if we have completers and qualifying scores
        if 'dual_credit_completion_count' in metrics and 'dual_credit_qualifying_score_count' in metrics:
            if metrics['dual_credit_completion_count'] > 0:
                qualifying_rate = round((metrics['dual_credit_qualifying_score_count'] /
                                       metrics['dual_credit_completion_count']) * 100, 1)
                metrics['dual_credit_qualifying_score_rate'] = qualifying_rate

        # Boolean indicator: has dual credit program if enrollment > 0
        if 'dual_credit_enrollment' in metrics and metrics['dual_credit_enrollment'] > 0:
            metrics['has_dual_credit_program'] = 1
        elif 'dual_credit_enrollment' in metrics:
            metrics['has_dual_credit_program'] = 0

        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get default metrics for suppressed dual credit records."""
        return {
            'dual_credit_enrollment': pd.NA,
            'dual_credit_completion_count': pd.NA,
            'dual_credit_completion_rate': pd.NA,
            'dual_credit_qualifying_score_count': pd.NA,
            'dual_credit_qualifying_score_rate': pd.NA,
            'has_dual_credit_program': pd.NA
        }

    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Override to include dual credit specific missing value handling."""
        # Apply base missing value standardization
        df = super().standardize_missing_values(df)

        # Handle comma-separated numbers
        numeric_columns = ['course_enrollment', 'course_completers', 'qualifying_score']
        for col in numeric_columns:
            if col in df.columns and df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.replace(',', '')

        return df


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Process dual credit data and convert to KPI format using BaseETL."""
    etl = DualCreditETL('dual_credit')
    etl.process(raw_dir, proc_dir, cfg)


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    from pathlib import Path
    raw_dir = Path(__file__).parent.parent / "data" / "raw"
    proc_dir = Path(__file__).parent.parent / "data" / "processed"
    proc_dir.mkdir(exist_ok=True)

    test_config = Config(
        derive={"processing_date": "2025-11-24", "data_quality_flag": "reviewed"}
    ).dict()

    transform(raw_dir, proc_dir, test_config)
