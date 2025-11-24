"""
Teacher Turnover ETL Module

Processes Kentucky teacher turnover data showing the percentage and count
of teachers who left each school/district year over year.

Data includes:
- Teacher count per institution
- Teacher turnover count (teachers who left)
- Turnover percentage

Note: This dataset contains school-level data only, with no demographic breakdowns.
All records are assigned "All Students" as the student group for consistency with KPI format.
"""
from pathlib import Path
import pandas as pd
from typing import Dict, Any
import logging
import sys

# Add etl directory to path for imports
etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from constants import KPI_COLUMNS
from base_etl import BaseETL, Config

logger = logging.getLogger(__name__)


class TeacherTurnoverETL(BaseETL):
    """ETL module for processing teacher turnover data."""

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        """
        Map teacher turnover columns to standardized names.

        Handles column name variations across years:
        - 2021-2023: Uppercase column names
        - 2024+: Title case column names
        """
        return {
            # Teacher count
            'Teacher Count': 'teacher_count',
            'TEACHER COUNT': 'teacher_count',

            # Turnover count
            'Teacher Turnover Count': 'teacher_turnover_count',
            'TEACHER TURNOVER COUNT': 'teacher_turnover_count',

            # Turnover percentage
            'Turnover Percent': 'turnover_percent',
            'TURNOVER PERCENT': 'turnover_percent',
        }

    def should_skip_row(self, row: pd.Series) -> bool:
        """
        Override to NOT skip rows without demographics.

        Teacher turnover data has no demographic dimension, so we process all rows
        and assign "All Students" as the student group.

        We still skip rows that have invalid/missing school identifiers.
        """
        # Don't skip based on demographics (this data has none)
        # Only skip if we can't identify the school
        school_code = row.get('school_code', '')
        if pd.isna(school_code) or school_code == '':
            return True

        return False

    def create_kpi_template(self, row: pd.Series, source_file: str) -> Dict[str, Any]:
        """
        Override to assign "All Students" demographic for all records.

        Teacher turnover data has no demographic breakdowns, so all records represent
        data for all students at the school level.
        """
        # Get base template from parent
        template = super().create_kpi_template(row, source_file)

        # Override student_group to always be "All Students"
        template['student_group'] = 'All Students'

        return template

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        """
        Extract teacher turnover metrics from a data row.

        Returns metrics:
        - teacher_turnover_rate: Percentage of teachers who left
        - teacher_turnover_count: Number of teachers who left
        - teacher_count: Total teacher count (denominator)
        """
        metrics = {}

        # Helper function to safely convert to numeric
        def safe_numeric(value):
            if pd.isna(value):
                return pd.NA
            try:
                # Handle comma-separated values (e.g., "10,612")
                if isinstance(value, str):
                    value = value.replace(',', '')
                return pd.to_numeric(value, errors='coerce')
            except:
                return pd.NA

        # Turnover rate (percentage)
        turnover_percent = safe_numeric(row.get('turnover_percent', pd.NA))
        if pd.notna(turnover_percent) and turnover_percent >= 0:
            metrics['teacher_turnover_rate'] = turnover_percent

        # Turnover count
        turnover_count = safe_numeric(row.get('teacher_turnover_count', pd.NA))
        if pd.notna(turnover_count) and turnover_count >= 0:
            metrics['teacher_turnover_count'] = turnover_count

        # Teacher count (denominator)
        teacher_count = safe_numeric(row.get('teacher_count', pd.NA))
        if pd.notna(teacher_count) and teacher_count >= 0:
            metrics['teacher_count'] = teacher_count

        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get default metrics for suppressed teacher turnover records."""
        defaults = {}

        # Only create defaults for metrics that exist in the source data
        if 'turnover_percent' in row.index:
            defaults['teacher_turnover_rate'] = pd.NA
        if 'teacher_turnover_count' in row.index:
            defaults['teacher_turnover_count'] = pd.NA
        if 'teacher_count' in row.index:
            defaults['teacher_count'] = pd.NA

        return defaults


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Read teacher turnover files, normalize, and convert to KPI format."""
    etl = TeacherTurnoverETL('teacher_turnover')
    etl.process(raw_dir, proc_dir, cfg)


def main():
    """Run teacher turnover ETL process."""
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


if __name__ == "__main__":
    main()
