"""
Teacher Certification ETL Module

Processes Kentucky teacher certification data showing emergency/provisional
certification rates and National Board certification rates by school/district.

Data includes:
- Teacher count per institution
- Emergency/provisional teacher count and percentage
- National Board certified teacher count and percentage

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


class TeacherCertificationETL(BaseETL):
    """ETL module for processing teacher certification data."""

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        """
        Map teacher certification columns to standardized names.

        Handles column name variations across years:
        - 2021-2023: Uppercase column names with EMERGENCY/PROVISIONAL
        - 2024+: Title case column names with Emergency Provisional
        """
        return {
            # Teacher count
            'Teacher Count': 'teacher_count',
            'TEACHER COUNT': 'teacher_count',

            # Emergency/Provisional - Count
            'Emergency Provisional Teacher Count': 'emergency_provisional_count',
            'EMERGENCY/PROVISIONAL TEACHER COUNT': 'emergency_provisional_count',

            # Emergency/Provisional - Percent
            'Percent Emergency Provisional Teachers': 'emergency_provisional_percent',
            'PERCENT EMERGENCY/PROVISIONAL TEACHERS': 'emergency_provisional_percent',

            # National Board Certified - Count
            'National Board Certified Count': 'national_board_certified_count',
            'NATIONAL BOARD CERTIFIED COUNT': 'national_board_certified_count',

            # National Board Certified - Percent
            'Percent National Board Certified Teacher': 'national_board_certified_percent',
            'PERCENT NATIONAL BOARD CERTIFIED TEACHERS': 'national_board_certified_percent',
        }

    def should_skip_row(self, row: pd.Series) -> bool:
        """
        Override to NOT skip rows without demographics.

        Teacher certification data has no demographic dimension, so we process all rows
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

        Teacher certification data has no demographic breakdowns, so all records represent
        data for all students at the school level.
        """
        # Get base template from parent
        template = super().create_kpi_template(row, source_file)

        # Override student_group to always be "All Students"
        template['student_group'] = 'All Students'

        return template

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        """
        Extract teacher certification metrics from a data row.

        Returns metrics:
        - emergency_provisional_teacher_rate: Percent on emergency/provisional certification
        - emergency_provisional_teacher_count: Count on emergency/provisional
        - national_board_certified_rate: Percent with National Board certification
        - national_board_certified_count: Count with National Board certification
        """
        metrics = {}

        # Helper function to safely convert to numeric
        def safe_numeric(value):
            if pd.isna(value):
                return pd.NA
            try:
                # Handle comma-separated values
                if isinstance(value, str):
                    value = value.replace(',', '')
                return pd.to_numeric(value, errors='coerce')
            except:
                return pd.NA

        # Emergency/Provisional rate (percentage)
        ep_percent = safe_numeric(row.get('emergency_provisional_percent', pd.NA))
        if pd.notna(ep_percent) and ep_percent >= 0:
            metrics['emergency_provisional_teacher_rate'] = ep_percent

        # Emergency/Provisional count
        ep_count = safe_numeric(row.get('emergency_provisional_count', pd.NA))
        if pd.notna(ep_count) and ep_count >= 0:
            metrics['emergency_provisional_teacher_count'] = ep_count

        # National Board Certified rate (percentage)
        nbc_percent = safe_numeric(row.get('national_board_certified_percent', pd.NA))
        if pd.notna(nbc_percent) and nbc_percent >= 0:
            metrics['national_board_certified_rate'] = nbc_percent

        # National Board Certified count
        nbc_count = safe_numeric(row.get('national_board_certified_count', pd.NA))
        if pd.notna(nbc_count) and nbc_count >= 0:
            metrics['national_board_certified_count'] = nbc_count

        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get default metrics for suppressed teacher certification records."""
        defaults = {}

        # Only create defaults for metrics that exist in the source data
        if 'emergency_provisional_percent' in row.index:
            defaults['emergency_provisional_teacher_rate'] = pd.NA
        if 'emergency_provisional_count' in row.index:
            defaults['emergency_provisional_teacher_count'] = pd.NA
        if 'national_board_certified_percent' in row.index:
            defaults['national_board_certified_rate'] = pd.NA
        if 'national_board_certified_count' in row.index:
            defaults['national_board_certified_count'] = pd.NA

        return defaults


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Read teacher certification files, normalize, and convert to KPI format."""
    etl = TeacherCertificationETL('teacher_certification')
    etl.process(raw_dir, proc_dir, cfg)


def main():
    """Run teacher certification ETL process."""
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
