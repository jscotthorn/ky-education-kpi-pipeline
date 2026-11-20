"""
CTE Pathways ETL Module

Handles Kentucky Career and Technical Education (CTE) pathway availability data.
Normalizes column names, standardizes missing values, and transforms to KPI format.

Data includes metrics by program area:
- Number of CTE pathways available
- Active enrollment in pathways
- CTE concentrator student counts
- Pathway completion counts and rates
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


class CTEPathwaysETL(BaseETL):
    """ETL module for processing CTE pathway availability data."""

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            # Program identification
            'Program Area': 'program_area',
            'PROGRAM AREA': 'program_area',

            # Pathway metrics
            'Pathways Available': 'pathways_available',
            'PATHWAYS AVAILABLE': 'pathways_available',

            # Active enrollment (2024/2025 format)
            'Active Enrollment': 'active_enrollment',
            'ACTIVE ENROLLMENT': 'active_enrollment',
            # Active enrollment (2021/2023 format)
            'Active Enrollments Count': 'active_enrollment',
            'ACTIVE ENROLLMENTS COUNT': 'active_enrollment',

            # Concentrator students (2024/2025 format)
            'Concentrator Students': 'concentrator_students',
            'CONCENTRATOR STUDENTS': 'concentrator_students',
            # Concentrator students (2021/2023 format)
            'Concentrator Students Count': 'concentrator_students',
            'CONCENTRATOR STUDENTS COUNT': 'concentrator_students',

            # Pathway completers (2024/2025 format)
            'Pathway Completers': 'pathway_completers',
            'PATHWAY COMPLETERS': 'pathway_completers',
            # Pathway completers (2021/2023 format)
            'Pathway Completers Count': 'pathway_completers',
            'PATHWAY COMPLETERS COUNT': 'pathway_completers',
        }

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        metrics = {}

        # Extract number of pathways available
        pathways = row.get('pathways_available', pd.NA)
        if pd.notna(pathways):
            pathways_clean = clean_numeric_with_commas(pd.Series([pathways])).iloc[0]
            if pd.notna(pathways_clean) and pathways_clean >= 0:
                metrics['num_cte_pathways'] = int(pathways_clean)

        # Extract active enrollment
        enrollment = row.get('active_enrollment', pd.NA)
        if pd.notna(enrollment):
            enrollment_clean = clean_numeric_with_commas(pd.Series([enrollment])).iloc[0]
            if pd.notna(enrollment_clean) and enrollment_clean >= 0:
                metrics['cte_pathway_enrollment'] = int(enrollment_clean)

        # Extract concentrator count
        concentrators = row.get('concentrator_students', pd.NA)
        if pd.notna(concentrators):
            concentrators_clean = clean_numeric_with_commas(pd.Series([concentrators])).iloc[0]
            if pd.notna(concentrators_clean) and concentrators_clean >= 0:
                metrics['cte_concentrator_count'] = int(concentrators_clean)

        # Extract completer count
        completers = row.get('pathway_completers', pd.NA)
        if pd.notna(completers):
            completers_clean = clean_numeric_with_commas(pd.Series([completers])).iloc[0]
            if pd.notna(completers_clean) and completers_clean >= 0:
                metrics['cte_pathway_completer_count'] = int(completers_clean)

        # Calculate completion rate if we have both concentrators and completers
        if 'cte_concentrator_count' in metrics and 'cte_pathway_completer_count' in metrics:
            if metrics['cte_concentrator_count'] > 0:
                completion_rate = round((metrics['cte_pathway_completer_count'] /
                                       metrics['cte_concentrator_count']) * 100, 1)
                metrics['cte_pathway_completion_rate'] = completion_rate

        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get default metrics for suppressed CTE pathway records."""
        return {
            'num_cte_pathways': pd.NA,
            'cte_pathway_enrollment': pd.NA,
            'cte_concentrator_count': pd.NA,
            'cte_pathway_completer_count': pd.NA,
            'cte_pathway_completion_rate': pd.NA
        }

    def should_skip_row(self, row: pd.Series) -> bool:
        """
        Override to handle data without demographics column.
        CTE pathway data is school-level only, no demographic breakdowns.
        """
        # Don't skip rows - all records are valid school-level data
        return False

    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Override to include CTE pathway specific missing value handling."""
        # Apply base missing value standardization
        df = super().standardize_missing_values(df)

        # Handle comma-separated numbers
        numeric_columns = ['pathways_available', 'active_enrollment',
                         'concentrator_students', 'pathway_completers']
        for col in numeric_columns:
            if col in df.columns and df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.replace(',', '')

        return df


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Process CTE pathway data and convert to KPI format using BaseETL."""
    etl = CTEPathwaysETL('cte_pathways')
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
