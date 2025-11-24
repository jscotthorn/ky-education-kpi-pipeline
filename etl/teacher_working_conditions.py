"""
Teacher Working Conditions ETL Module

Processes Kentucky teacher working conditions survey data showing index scores
for different impact measures.

The data is pivoted with one row per school-measure combination:
- Managing Student Behavior
- School Climate
- School Leadership
- Teaching Environment (historical only)

Each measure's impact value becomes a separate KPI metric. This is school-level
data with no demographic breakdowns.
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


class TeacherWorkingConditionsETL(BaseETL):
    """ETL module for processing teacher working conditions survey data."""

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        """
        Map columns to standardized names.

        Handles column name variations across years:
        - 2021-2023: Uppercase (IMPACT MEASURE, IMPACT VALUE)
        - 2024+: Title case (Impact Measure, Impact Value)
        """
        return {
            # Impact measure and value columns
            'Impact Measure': 'impact_measure',
            'IMPACT MEASURE': 'impact_measure',
            'Impact Value': 'impact_value',
            'IMPACT VALUE': 'impact_value',
        }

    # Mapping from impact measure names to metric suffixes
    IMPACT_MEASURE_MAP = {
        # KYRC24/25 format
        'Managing Student Behavior': 'managing_student_behavior',
        'School Climate': 'school_climate',
        'School Leadership': 'school_leadership',
        # Historical format (2021-2023) with "Composite" suffix
        'Managing Student Behavior Composite': 'managing_student_behavior',
        'School Climate Composite': 'school_climate',
        'School Leadership Composite': 'school_leadership',
        'Teaching Environment Composite': 'teaching_environment',
    }

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        """
        Extract metrics from a data row.

        Each row represents one impact measure for a school. We create a metric
        based on the impact measure type.
        """
        metrics = {}

        # Helper function to safely convert to numeric
        def safe_numeric(value):
            if pd.isna(value):
                return pd.NA
            try:
                if isinstance(value, str):
                    value = value.replace('%', '').replace(',', '').strip()
                return pd.to_numeric(value, errors='coerce')
            except:
                return pd.NA

        # Get impact measure and value
        impact_measure = row.get('impact_measure', '')
        impact_value = safe_numeric(row.get('impact_value', pd.NA))

        if pd.notna(impact_value) and impact_measure:
            # Map the impact measure to a metric suffix
            metric_suffix = self.IMPACT_MEASURE_MAP.get(impact_measure)

            if metric_suffix:
                metric_name = f'teacher_working_conditions_{metric_suffix}'
                metrics[metric_name] = impact_value

        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get default metrics for suppressed records."""
        defaults = {}

        impact_measure = row.get('impact_measure', '')
        metric_suffix = self.IMPACT_MEASURE_MAP.get(impact_measure)

        if metric_suffix:
            metric_name = f'teacher_working_conditions_{metric_suffix}'
            defaults[metric_name] = pd.NA

        return defaults

    def should_skip_row(self, row: pd.Series) -> bool:
        """Skip rows that don't have valid working conditions data."""
        # Check if we have a valid impact measure
        impact_measure = row.get('impact_measure', '')
        has_measure = impact_measure in self.IMPACT_MEASURE_MAP

        # Skip if no valid measure
        if not has_measure:
            return True

        # Don't call super() - this data doesn't have traditional demographics column
        return False

    def create_kpi_template(self, row: pd.Series, source_file: str) -> Dict[str, Any]:
        """
        Override to set student_group to 'All Students' since this is school-level data.
        """
        from datetime import datetime

        # Extract school identification
        school_id = self.extract_school_id(row)
        year = self.extract_year(row, source_file)

        # Check if record is suppressed
        is_suppressed = row.get('suppressed', 'N') == 'Y'

        return {
            'district': row.get('district_name', 'Unknown District'),
            'school_id': school_id,
            'school_name': self.standardize_school_name(row.get('school_name', 'Unknown School')),
            'year': year,
            'student_group': 'All Students',  # School-level data only
            'county_number': row.get('county_number', pd.NA),
            'county_name': row.get('county_name', pd.NA),
            'district_number': row.get('district_number', pd.NA),
            'school_code': row.get('school_code', pd.NA),
            'state_school_id': row.get('state_school_id', pd.NA),
            'nces_id': row.get('nces_id', pd.NA),
            'co_op': row.get('co_op', pd.NA),
            'co_op_code': row.get('co_op_code', pd.NA),
            'school_type': row.get('school_type', pd.NA),
            'suppressed': 'Y' if is_suppressed else 'N',
            'source_file': source_file,
            'last_updated': datetime.now().isoformat()
        }


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Read teacher working conditions files, normalize, and convert to KPI format."""
    etl = TeacherWorkingConditionsETL('teacher_working_conditions')
    etl.process(raw_dir, proc_dir, cfg)


def main():
    """Run teacher working conditions ETL process."""
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
