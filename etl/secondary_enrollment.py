"""
Secondary Enrollment ETL Module

Handles Kentucky secondary enrollment data (Grades 6-12) across years 2020-2025.
This is distinct from student_enrollment which captures all grades (PreK-12).
Secondary enrollment tracks students enrolled in secondary programs at schools
that offer secondary education.

Data includes enrollment counts by grade level:
- Grade 6 through Grade 12 enrollment counts
- Total secondary student counts by demographic group
- School-level and district-level aggregations
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


def clean_enrollment_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate secondary enrollment count values."""
    grade_columns = [
        'grade_6', 'grade_7', 'grade_8', 'grade_9', 'grade_10', 'grade_11', 'grade_12',
        'grade_14', 'all_grades', 'total_student_count'
    ]

    for col in grade_columns:
        if col in df.columns:
            # Convert to numeric, handling commas in numbers
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.replace(',', '').str.replace('"', '')

            # Convert to numeric, errors='coerce' will make invalid values NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')

            # Enrollment counts should be non-negative integers
            invalid_mask = df[col] < 0
            if invalid_mask.any():
                logger.warning(f"Found {invalid_mask.sum()} negative enrollment counts in {col}")
                df.loc[invalid_mask, col] = pd.NA

    return df


class SecondaryEnrollmentETL(BaseETL):
    """ETL module for processing secondary enrollment data."""

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            # KYRC25 format
            'All Grades': 'all_grades',
            'Grade 6': 'grade_6',
            'Grade 7': 'grade_7',
            'Grade 8': 'grade_8',
            'Grade 9': 'grade_9',
            'Grade 10': 'grade_10',
            'Grade 11': 'grade_11',
            'Grade 12': 'grade_12',
            'Grade 14': 'grade_14',

            # Historical format (uppercase)
            'TOTAL STUDENT COUNT': 'total_student_count',
            'GRADE6 COUNT': 'grade_6',
            'GRADE7 COUNT': 'grade_7',
            'GRADE8 COUNT': 'grade_8',
            'GRADE9 COUNT': 'grade_9',
            'GRADE10 COUNT': 'grade_10',
            'GRADE11 COUNT': 'grade_11',
            'GRADE12 COUNT': 'grade_12',
            'GRADE14 COUNT': 'grade_14',

            # Historical files also have these columns (always 0 for secondary)
            'PRESCHOOL COUNT': 'preschool',
            'KINDERGARTEN COUNT': 'k',
            'GRADE1 COUNT': 'grade_1',
            'GRADE2 COUNT': 'grade_2',
            'GRADE3 COUNT': 'grade_3',
            'GRADE4 COUNT': 'grade_4',
            'GRADE5 COUNT': 'grade_5',
        }

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        metrics = {}

        # Helper function to safely convert to numeric
        def safe_numeric(value: Any) -> Any:
            if pd.isna(value):
                return pd.NA
            try:
                return pd.to_numeric(value, errors='coerce')
            except Exception:
                return pd.NA

        # Total secondary enrollment
        total_val = row.get('all_grades') or row.get('total_student_count', pd.NA)
        total_numeric = safe_numeric(total_val)
        if pd.notna(total_numeric) and total_numeric > 0:
            metrics['secondary_enrollment_total'] = total_numeric

        # Individual grade levels (6-12 only)
        grade_metrics = {
            'secondary_enrollment_grade_6': safe_numeric(row.get('grade_6', pd.NA)),
            'secondary_enrollment_grade_7': safe_numeric(row.get('grade_7', pd.NA)),
            'secondary_enrollment_grade_8': safe_numeric(row.get('grade_8', pd.NA)),
            'secondary_enrollment_grade_9': safe_numeric(row.get('grade_9', pd.NA)),
            'secondary_enrollment_grade_10': safe_numeric(row.get('grade_10', pd.NA)),
            'secondary_enrollment_grade_11': safe_numeric(row.get('grade_11', pd.NA)),
            'secondary_enrollment_grade_12': safe_numeric(row.get('grade_12', pd.NA)),
        }

        # Add individual grade metrics if they have values > 0
        for metric_name, value in grade_metrics.items():
            if pd.notna(value) and value > 0:
                metrics[metric_name] = value

        # Grade 14 (rare but exists in some data)
        grade_14_val = safe_numeric(row.get('grade_14', pd.NA))
        if pd.notna(grade_14_val) and grade_14_val > 0:
            metrics['secondary_enrollment_grade_14'] = grade_14_val

        # Calculate aggregated metrics for school levels
        middle_grades = ['grade_6', 'grade_7', 'grade_8']
        high_school_grades = ['grade_9', 'grade_10', 'grade_11', 'grade_12']

        # Middle school enrollment (6-8)
        middle_counts = []
        for grade in middle_grades:
            value = row.get(grade, pd.NA)
            if pd.notna(value):
                try:
                    numeric_value = pd.to_numeric(value, errors='coerce')
                    if pd.notna(numeric_value) and numeric_value > 0:
                        middle_counts.append(numeric_value)
                except Exception:
                    pass
        if middle_counts:
            metrics['secondary_enrollment_middle'] = sum(middle_counts)

        # High school enrollment (9-12)
        high_school_counts = []
        for grade in high_school_grades:
            value = row.get(grade, pd.NA)
            if pd.notna(value):
                try:
                    numeric_value = pd.to_numeric(value, errors='coerce')
                    if pd.notna(numeric_value) and numeric_value > 0:
                        high_school_counts.append(numeric_value)
                except Exception:
                    pass
        if high_school_counts:
            metrics['secondary_enrollment_high_school'] = sum(high_school_counts)

        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get default metrics for suppressed secondary enrollment records."""
        defaults = {}

        # Only create defaults for metrics that exist in the source data
        if 'all_grades' in row.index or 'total_student_count' in row.index:
            defaults['secondary_enrollment_total'] = pd.NA

        # Check for individual grade level columns
        if 'grade_6' in row.index:
            defaults['secondary_enrollment_grade_6'] = pd.NA
        if 'grade_7' in row.index:
            defaults['secondary_enrollment_grade_7'] = pd.NA
        if 'grade_8' in row.index:
            defaults['secondary_enrollment_grade_8'] = pd.NA
        if 'grade_9' in row.index:
            defaults['secondary_enrollment_grade_9'] = pd.NA
        if 'grade_10' in row.index:
            defaults['secondary_enrollment_grade_10'] = pd.NA
        if 'grade_11' in row.index:
            defaults['secondary_enrollment_grade_11'] = pd.NA
        if 'grade_12' in row.index:
            defaults['secondary_enrollment_grade_12'] = pd.NA

        # Check for aggregate level columns
        grade_columns = [col for col in row.index if col.startswith('grade_')]
        if any(col in ['grade_6', 'grade_7', 'grade_8'] for col in grade_columns):
            defaults['secondary_enrollment_middle'] = pd.NA
        if any(col in ['grade_9', 'grade_10', 'grade_11', 'grade_12'] for col in grade_columns):
            defaults['secondary_enrollment_high_school'] = pd.NA

        return defaults

    def convert_to_kpi_format(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """Override to ensure all secondary enrollment metrics are created together."""
        kpi_rows = []

        for _, row in df.iterrows():
            if self.should_skip_row(row):
                continue

            kpi_template = self.create_kpi_template(row, source_file)
            metrics = self.extract_metrics(row)

            # Create separate KPI rows for each metric
            for metric_name, metric_value in metrics.items():
                if pd.notna(metric_value) and metric_value != 0:  # Skip zero enrollments
                    kpi_row = kpi_template.copy()
                    kpi_row['metric'] = metric_name
                    kpi_row['value'] = metric_value
                    kpi_rows.append(kpi_row)

        return pd.DataFrame(kpi_rows, columns=KPI_COLUMNS)

    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Override to include secondary enrollment specific missing value handling."""
        # Apply base missing value standardization
        df = super().standardize_missing_values(df)

        # Apply enrollment-specific cleaning
        df = clean_enrollment_data(df)

        return df


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Read secondary enrollment files, normalize, and convert to KPI format."""
    etl = SecondaryEnrollmentETL('secondary_enrollment')
    etl.process(raw_dir, proc_dir, cfg)


def main() -> None:
    """Run secondary enrollment ETL process."""
    logging.basicConfig(level=logging.INFO)

    raw_dir = Path(__file__).parent.parent / "data" / "raw"
    proc_dir = Path(__file__).parent.parent / "data" / "processed"
    proc_dir.mkdir(exist_ok=True)

    test_config = Config(
        derive={"processing_date": "2025-11-25", "data_quality_flag": "reviewed"}
    ).model_dump()

    transform(raw_dir, proc_dir, test_config)


if __name__ == "__main__":
    main()
