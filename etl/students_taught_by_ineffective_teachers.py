"""
Students Taught by Ineffective Teachers ETL Module

Processes Kentucky data showing the percentage of students taught by
ineffective teachers, with equity breakdowns by Title I status and demographics.

This is an equity-focused dataset that helps identify disparities in teacher
quality across different student populations.

Data includes:
- Percentage of students taught by ineffective teachers
- Breakdowns by Title I status (Title 1, Not Title 1)
- Breakdowns by demographics (race, economic status, disability, EL status)

The demographic values are processed to populate the `student_group` column
in the KPI output, following the same pattern as novice_teachers.py.
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


class StudentsTaughtByIneffectiveTeachersETL(BaseETL):
    """ETL module for processing students taught by ineffective teachers data."""

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        """
        Map columns to standardized names.

        Handles column name variations across years:
        - 2021-2023: Uppercase with % prefix and TCHRS abbreviation
        - 2024+: Title case demographic column names
        """
        return {
            # Title I status
            'Title I Status': 'title_i_status',
            'TITLE I STATUS': 'title_i_status',

            # Demographics - Standard (KYRC24/25)
            'All Students': 'all_students',
            'Non-White': 'non_white',
            'White (non-Hispanic)': 'white',
            'White': 'white',
            'Economically Disadvantaged': 'economically_disadvantaged',
            'Non Economically Disadvantaged': 'non_economically_disadvantaged',
            'Non-Economically Disadvantaged': 'non_economically_disadvantaged',
            'Students with Disabilities (IEP)': 'students_with_disabilities',
            'Student without Disabilities (IEP)': 'student_without_disabilities',
            'English Learner': 'english_learner',
            'Non English Learner': 'non_english_learner',
            'Non-English Learner': 'non_english_learner',

            # Demographics - Historical (2021-2023)
            '% STUDENTS TAUGHT BY INEFFECTIVE TCHRS': 'all_students',
            '% NON-WHITE STUDENTS TAUGHT BY INEFFECTIVE TCHRS': 'non_white',
            '% WHITE STUDENTS TAUGHT BY INEFFECTIVE TCHRS': 'white',
            '% ECONOMICALLY DISADVANTAGED TAUGHT BY INEFFECTIVE TCHRS': 'economically_disadvantaged',
            '% NON-ECONOMICALLY DISADVANTAGED TAUGHT BY INEFFECTIVE TCHRS': 'non_economically_disadvantaged',
            '% STUDENTS WITH DISABILITIES TAUGHT BY INEFFECTIVE TCHRS': 'students_with_disabilities',
            '% NON-STUDENTS WITH DISABILITIES TAUGHT BY INEFFECTIVE TCHRS': 'student_without_disabilities',
            '% ENGLISH LEARNER STUDENTS TAUGHT BY INEFFECTIVE TCHRS': 'english_learner',
            '% NON-ENGLISH LEARNER STUDENTS TAUGHT BY INEFFECTIVE TCHRS': 'non_english_learner',

            # Gap columns (historical) - we'll ignore these for now
            'INEFFECTIVE GAP %AGE NON-WHITE': 'gap_non_white',
            'INEFFECTIVE GAP %AGE ECONOMICALLY DISADVANTAGED': 'gap_economically_disadvantaged',
            'INEFFECTIVE GAP %AGE STUDENTS WITH DISABILITIES': 'gap_students_with_disabilities',
            'INEFFECTIVE GAP %AGE ENGLISH LEARNERS': 'gap_english_learners',
        }

    # Mapping from internal demographic keys to standard display names
    DEMOGRAPHIC_DISPLAY_MAP = {
        'all_students': 'All Students',
        'white': 'White (non-Hispanic)',
        'non_white': 'Non-White',
        'economically_disadvantaged': 'Economically Disadvantaged',
        'non_economically_disadvantaged': 'Non-Economically Disadvantaged',
        'students_with_disabilities': 'Students with Disabilities (IEP)',
        'student_without_disabilities': 'Student without Disabilities (IEP)',
        'english_learner': 'English Learner',
        'non_english_learner': 'Non-English Learner',
    }

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        """
        Extract metrics from a data row.

        For each row with a Title I status, extract demographic values and create
        metrics with double-underscore separator to encode both the Title I status
        and demographic group in the metric key.
        """
        metrics = {}

        # Helper function to safely convert to numeric
        def safe_numeric(value):
            if pd.isna(value):
                return pd.NA
            try:
                # Handle percentage values with % suffix (historical data)
                if isinstance(value, str):
                    value = value.replace('%', '').replace(',', '').strip()
                return pd.to_numeric(value, errors='coerce')
            except:
                return pd.NA

        # Get Title I status
        title_i_status = row.get('title_i_status', '')

        # Process based on Title I status
        if title_i_status in ['Title 1', 'Title I', 'Not Title 1', 'Not Title I', 'Equity Gap']:
            # Normalize Title I status for metric naming
            title_i_suffix = title_i_status.lower().replace(' ', '_').replace('title_i', 'title_1')

            # Process each demographic group column
            demographic_columns = {
                'all_students': 'all_students',
                'non_white': 'non_white',
                'white': 'white',
                'economically_disadvantaged': 'economically_disadvantaged',
                'non_economically_disadvantaged': 'non_economically_disadvantaged',
                'students_with_disabilities': 'students_with_disabilities',
                'student_without_disabilities': 'student_without_disabilities',
                'english_learner': 'english_learner',
                'non_english_learner': 'non_english_learner',
            }

            for col_name, demo_key in demographic_columns.items():
                value = safe_numeric(row.get(col_name, pd.NA))
                if pd.notna(value) and value >= 0:
                    # Use double underscore separator to identify equity metrics and separate demographic key
                    metric_name = f'students_taught_by_ineffective_teachers_rate_{title_i_suffix}__{demo_key}'
                    metrics[metric_name] = value

        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get default metrics for suppressed records."""
        defaults = {}

        title_i_status = row.get('title_i_status', '')
        if title_i_status in ['Title 1', 'Title I', 'Not Title 1', 'Not Title I', 'Equity Gap']:
            title_i_suffix = title_i_status.lower().replace(' ', '_').replace('title_i', 'title_1')

            demographic_columns = [
                'all_students', 'non_white', 'white',
                'economically_disadvantaged', 'non_economically_disadvantaged',
                'students_with_disabilities', 'student_without_disabilities',
                'english_learner', 'non_english_learner',
            ]

            for demo_key in demographic_columns:
                if demo_key in row.index:
                    metric_name = f'students_taught_by_ineffective_teachers_rate_{title_i_suffix}__{demo_key}'
                    defaults[metric_name] = pd.NA

        return defaults

    def convert_to_kpi_format(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """
        Override base method to handle dynamic student_group assignment for equity metrics.
        """
        kpi_rows = []

        for _, row in df.iterrows():
            # Skip rows that shouldn't be processed
            if self.should_skip_row(row):
                continue

            # Create base KPI template
            kpi_template = self.create_kpi_template(row, source_file)

            # Extract metrics using module-specific logic
            metrics = self.extract_metrics(row)

            # Special handling for suppressed records
            if not metrics and kpi_template['suppressed'] == 'Y':
                metrics = self.get_suppressed_metric_defaults(row)

            # Create KPI rows for each metric
            for metric_key, value in metrics.items():
                kpi_record = kpi_template.copy()

                # Check if this is an equity metric (has double underscore separator)
                if '__' in metric_key:
                    base_metric, demo_key = metric_key.split('__')
                    # Update metric name and student_group
                    kpi_record['metric'] = base_metric
                    kpi_record['student_group'] = self.DEMOGRAPHIC_DISPLAY_MAP.get(demo_key, 'All Students')
                else:
                    # Standard metric
                    kpi_record['metric'] = metric_key
                    # student_group remains as set in template

                # Handle suppression and value assignment
                if kpi_template['suppressed'] == 'Y':
                    kpi_record['value'] = pd.NA
                    kpi_rows.append(kpi_record)
                else:
                    try:
                        if pd.notna(value) and value != '':
                            kpi_record['value'] = float(value)
                            kpi_rows.append(kpi_record)
                    except (ValueError, TypeError):
                        continue

        if not kpi_rows:
            logger.warning("No valid KPI rows created")
            return pd.DataFrame()

        # Create KPI DataFrame with consistent column order
        kpi_df = pd.DataFrame(kpi_rows)
        available_columns = [col for col in KPI_COLUMNS if col in kpi_df.columns]
        return kpi_df[available_columns]

    def should_skip_row(self, row: pd.Series) -> bool:
        """Skip rows that don't have ineffective teacher data."""
        # Check if we have a valid Title I status
        title_i_status = row.get('title_i_status', '')
        has_title_i = title_i_status in ['Title 1', 'Title I', 'Not Title 1', 'Not Title I', 'Equity Gap']

        # Skip if no Title I status
        if not has_title_i:
            return True

        # Don't call super() - this data doesn't have traditional demographics column
        return False


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Read students taught by ineffective teachers files, normalize, and convert to KPI format."""
    etl = StudentsTaughtByIneffectiveTeachersETL('students_taught_by_ineffective_teachers')
    etl.process(raw_dir, proc_dir, cfg)


def main():
    """Run students taught by ineffective teachers ETL process."""
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
