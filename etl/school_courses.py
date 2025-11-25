"""
School Courses ETL Module

Handles Kentucky general course offerings data.
Normalizes column names, standardizes missing values, and transforms to KPI format.

Data includes:
- Course offerings by grade level
- Algebra 1 access for middle schools
- Total courses offered
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


class SchoolCoursesETL(BaseETL):
    """ETL module for processing school course offerings data."""

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            # Course identification
            'Course Category': 'course_category',
            'COURSE CATEGORY': 'course_category',
            'State Course Code': 'course_code',
            'STATE COURSE CODE': 'course_code',
            'State Course Name': 'course_name',
            'STATE COURSE NAME': 'course_name',

            # Student counts by grade
            'Student Count': 'student_count',
            'STUDENT COUNT': 'student_count',
            'Preschool Count': 'preschool_count',
            'PRESCHOOL COUNT': 'preschool_count',
            'Kindergarten Count': 'kindergarten_count',
            'KINDERGARTEN COUNT': 'kindergarten_count',
            'Grade1 Count': 'grade1_count',
            'GRADE1 COUNT': 'grade1_count',
            'Grade2 Count': 'grade2_count',
            'GRADE2 COUNT': 'grade2_count',
            'Grade3 Count': 'grade3_count',
            'GRADE3 COUNT': 'grade3_count',
            'Grade4 Count': 'grade4_count',
            'GRADE4 COUNT': 'grade4_count',
            'Grade5 Count': 'grade5_count',
            'GRADE5 COUNT': 'grade5_count',
            'Grade6 Count': 'grade6_count',
            'GRADE6 COUNT': 'grade6_count',
            'Grade7 Count': 'grade7_count',
            'GRADE7 COUNT': 'grade7_count',
            'Grade8 Count': 'grade8_count',
            'GRADE8 COUNT': 'grade8_count',
            'Grade9 Count': 'grade9_count',
            'GRADE9 COUNT': 'grade9_count',
            'Grade10 Count': 'grade10_count',
            'GRADE10 COUNT': 'grade10_count',
            'Grade11 Count': 'grade11_count',
            'GRADE11 COUNT': 'grade11_count',
            'Grade12 Count': 'grade12_count',
            'GRADE12 COUNT': 'grade12_count',
            'Grade14 Count': 'grade14_count',
            'GRADE14 COUNT': 'grade14_count',
        }

    def should_skip_row(self, row: pd.Series) -> bool:
        """
        Override to handle data without demographics.
        School courses data is course-level, no demographic breakdowns.
        """
        # Don't skip rows - all records are valid course-level data
        return False

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        metrics = {}
        
        # Get course name for Algebra 1 detection
        course_name = str(row.get('course_name', '')).upper()
        course_code = str(row.get('course_code', ''))
        
        # Check if this is Algebra 1
        is_algebra_1 = 'ALGEBRA I' in course_name or 'ALGEBRA 1' in course_name
        
        # Extract grade 8 count for Algebra 1 access metric
        if is_algebra_1:
            grade8_count = row.get('grade8_count', pd.NA)
            if pd.notna(grade8_count):
                count_clean = clean_numeric_with_commas(pd.Series([grade8_count])).iloc[0]
                if pd.notna(count_clean) and count_clean > 0:
                    metrics['algebra_1_grade_8_count'] = int(count_clean)
                    metrics['has_algebra_1_access'] = 1
                elif pd.notna(count_clean):
                    metrics['has_algebra_1_access'] = 0
        
        # Extract total student count for this course
        student_count = row.get('student_count', pd.NA)
        if pd.notna(student_count):
            count_clean = clean_numeric_with_commas(pd.Series([student_count])).iloc[0]
            if pd.notna(count_clean) and count_clean > 0:
                # Count unique courses offered
                metrics['courses_offered_count'] = 1  # Will be aggregated

        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get default metrics for suppressed records."""
        return {
            'algebra_1_grade_8_count': pd.NA,
            'has_algebra_1_access': pd.NA,
            'courses_offered_count': pd.NA,
        }

    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Override to include school courses specific missing value handling."""
        # Apply base missing value standardization
        df = super().standardize_missing_values(df)

        # Handle comma-separated numbers in grade count columns
        grade_columns = [
            'student_count', 'preschool_count', 'kindergarten_count',
            'grade1_count', 'grade2_count', 'grade3_count', 'grade4_count',
            'grade5_count', 'grade6_count', 'grade7_count', 'grade8_count',
            'grade9_count', 'grade10_count', 'grade11_count', 'grade12_count',
            'grade14_count'
        ]
        for col in grade_columns:
            if col in df.columns and df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.replace(',', '')

        return df


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Process school courses data and convert to KPI format using BaseETL."""
    etl = SchoolCoursesETL('school_courses')
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
