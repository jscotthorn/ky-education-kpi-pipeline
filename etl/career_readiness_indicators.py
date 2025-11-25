"""
Career Readiness Indicators ETL Module

Handles Kentucky CTE career readiness indicators data.
Normalizes column names, standardizes missing values, and transforms to KPI format.

Data includes career readiness metrics for CTE programs.
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


class CareerReadinessIndicatorsETL(BaseETL):
    """ETL module for processing career readiness indicators data."""

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            # Program identification
            'Program Area': 'program_area',
            'PROGRAM AREA': 'program_area',

            # Career readiness metrics
            'Industry Certification': 'industry_certification',
            'INDUSTRY CERTIFICATION': 'industry_certification',
            'Apprenticeship': 'apprenticeship',
            'APPRENTICESHIP': 'apprenticeship',
            'Cooperative Education': 'cooperative_education',
            'COOPERATIVE EDUCATION': 'cooperative_education',
            'Internship': 'internship',
            'INTERNSHIP': 'internship',
            'Advanced Placement': 'advanced_placement',
            'ADVANCED PLACEMENT': 'advanced_placement',
            'Dual Credit': 'dual_credit_cte',
            'DUAL CREDIT': 'dual_credit_cte',
            'Transition Readiness': 'transition_readiness',
            'TRANSITION READINESS': 'transition_readiness',
        }

    def should_skip_row(self, row: pd.Series) -> bool:
        """
        Override to handle data without demographics.
        Career readiness data is school/program-level, no demographic breakdowns.
        """
        # Don't skip rows - all records are valid
        return False

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        metrics = {}

        # Extract various career readiness indicators
        indicators = [
            'industry_certification',
            'apprenticeship',
            'cooperative_education',
            'internship',
            'advanced_placement',
            'dual_credit_cte',
            'transition_readiness'
        ]

        for indicator in indicators:
            value = row.get(indicator, pd.NA)
            if pd.notna(value):
                value_clean = clean_numeric_with_commas(pd.Series([value])).iloc[0]
                if pd.notna(value_clean) and value_clean >= 0:
                    metric_name = f'career_readiness_{indicator}_count'
                    metrics[metric_name] = int(value_clean)

        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get default metrics for suppressed records."""
        return {
            'career_readiness_industry_certification_count': pd.NA,
            'career_readiness_apprenticeship_count': pd.NA,
            'career_readiness_cooperative_education_count': pd.NA,
            'career_readiness_internship_count': pd.NA,
            'career_readiness_advanced_placement_count': pd.NA,
            'career_readiness_dual_credit_cte_count': pd.NA,
            'career_readiness_transition_readiness_count': pd.NA,
        }

    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Override to include career readiness specific missing value handling."""
        # Apply base missing value standardization
        df = super().standardize_missing_values(df)

        # Handle comma-separated numbers
        numeric_columns = [
            'industry_certification', 'apprenticeship', 'cooperative_education',
            'internship', 'advanced_placement', 'dual_credit_cte', 'transition_readiness'
        ]
        for col in numeric_columns:
            if col in df.columns and df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.replace(',', '')

        return df


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Process career readiness indicators data and convert to KPI format using BaseETL."""
    etl = CareerReadinessIndicatorsETL('career_readiness_indicators')
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
