"""
Advanced Coursework ETL Module

Handles Kentucky advanced coursework data including AP, IB, and Cambridge programs.
Normalizes column names, standardizes missing values, and transforms to KPI format.

Data includes:
- Advanced courses offered (course-level details)
- Participation and performance by demographic
- Overview statistics with gender breakdowns
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


class AdvancedCourseworkETL(BaseETL):
    """ETL module for processing advanced coursework data."""

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            # Course identification
            'State Course Code': 'course_code',
            'STATE COURSE CODE': 'course_code',
            'State Course Name': 'course_name',
            'STATE COURSE NAME': 'course_name',
            'Advanced Course Type': 'course_type',
            'ADVANCED COURSE TYPE': 'course_type',
            'Course Type': 'course_type',
            'COURSE TYPE': 'course_type',
            'Subject': 'subject',
            'SUBJECT': 'subject',

            # Enrollment and performance metrics
            'Course Enrollment': 'course_enrollment',
            'COURSE ENROLLMENT': 'course_enrollment',
            'Course Completers': 'course_completers',
            'COURSE COMPLETERS': 'course_completers',
            'Number Tested': 'number_tested',
            'NUMBER TESTED': 'number_tested',
            'Number Tested ': 'number_tested',  # Extra space
            'Students With Qualifying Score': 'qualifying_score',
            'STUDENTS WITH QUALIFYING SCORE': 'qualifying_score',
            'Students With Qualifying Score ': 'qualifying_score',  # Extra space
            'Earned Qualifying Score': 'qualifying_score',
            'EARNED QUALIFYING SCORE': 'qualifying_score',

            # Overview file columns (participation rates)
            'Total': 'total_count',
            'TOTAL': 'total_count',
            'Female': 'female_count',
            'FEMALE': 'female_count',
            'Male': 'male_count',
            'MALE': 'male_count',
            'Total %': 'total_rate',
            'TOTAL %': 'total_rate',
            'Female %': 'female_rate',
            'FEMALE %': 'female_rate',
            'Male %': 'male_rate',
            'MALE %': 'male_rate',
        }

    def should_skip_row(self, row: pd.Series) -> bool:
        """
        Override to handle files with and without demographics.
        """
        # If there's a demographic column, use parent logic
        if 'demographic' in row.index and pd.notna(row.get('demographic')):
            return super().should_skip_row(row)

        # For files without demographics, don't skip any rows
        return False

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        metrics = {}
        
        # Determine course type (AP, IB, Cambridge, Dual Credit)
        course_type = str(row.get('course_type', '')).upper()
        
        # Skip Dual Credit as it's handled in separate pipeline
        if 'DUAL CREDIT' in course_type:
            return metrics
        
        # Determine prefix based on course type
        prefix = None
        if 'AP' in course_type or 'ADVANCED PLACEMENT' in course_type:
            prefix = 'ap'
        elif 'IB' in course_type or 'INTERNATIONAL BACCALAUREATE' in course_type:
            prefix = 'ib'
        elif 'CAMBRIDGE' in course_type:
            prefix = 'cambridge'
        else:
            # Generic advanced coursework
            prefix = 'advanced'

        # Extract enrollment
        enrollment = row.get('course_enrollment', pd.NA)
        if pd.notna(enrollment):
            enrollment_clean = clean_numeric_with_commas(pd.Series([enrollment])).iloc[0]
            if pd.notna(enrollment_clean) and enrollment_clean >= 0:
                metrics[f'{prefix}_course_enrollment'] = int(enrollment_clean)

        # Extract completers
        completers = row.get('course_completers', pd.NA)
        if pd.notna(completers):
            completers_clean = clean_numeric_with_commas(pd.Series([completers])).iloc[0]
            if pd.notna(completers_clean) and completers_clean >= 0:
                metrics[f'{prefix}_completion_count'] = int(completers_clean)

        # Extract number tested
        tested = row.get('number_tested', pd.NA)
        if pd.notna(tested):
            tested_clean = clean_numeric_with_commas(pd.Series([tested])).iloc[0]
            if pd.notna(tested_clean) and tested_clean >= 0:
                metrics[f'{prefix}_tested_count'] = int(tested_clean)

        # Extract qualifying scores
        qualifying = row.get('qualifying_score', pd.NA)
        if pd.notna(qualifying):
            qualifying_clean = clean_numeric_with_commas(pd.Series([qualifying])).iloc[0]
            if pd.notna(qualifying_clean) and qualifying_clean >= 0:
                metrics[f'{prefix}_qualifying_score_count'] = int(qualifying_clean)

        # Calculate qualifying score rate
        if f'{prefix}_tested_count' in metrics and f'{prefix}_qualifying_score_count' in metrics:
            if metrics[f'{prefix}_tested_count'] > 0:
                rate = round((metrics[f'{prefix}_qualifying_score_count'] /
                            metrics[f'{prefix}_tested_count']) * 100, 1)
                metrics[f'{prefix}_qualifying_score_rate'] = rate

        # Extract participation rates from overview file
        total_rate = row.get('total_rate', pd.NA)
        if pd.notna(total_rate):
            rate_clean = clean_numeric_with_commas(pd.Series([total_rate])).iloc[0]
            if pd.notna(rate_clean) and 0 <= rate_clean <= 100:
                metrics[f'{prefix}_participation_rate'] = rate_clean

        # Extract gender-specific participation rates
        female_rate = row.get('female_rate', pd.NA)
        if pd.notna(female_rate):
            rate_clean = clean_numeric_with_commas(pd.Series([female_rate])).iloc[0]
            if pd.notna(rate_clean) and 0 <= rate_clean <= 100:
                metrics[f'{prefix}_participation_rate_female'] = rate_clean

        male_rate = row.get('male_rate', pd.NA)
        if pd.notna(male_rate):
            rate_clean = clean_numeric_with_commas(pd.Series([male_rate])).iloc[0]
            if pd.notna(rate_clean) and 0 <= rate_clean <= 100:
                metrics[f'{prefix}_participation_rate_male'] = rate_clean

        # Count courses offered (from courses offered file)
        if 'course_code' in row.index and pd.notna(row.get('course_code')):
            metrics[f'num_{prefix}_courses_offered'] = 1  # Will be aggregated later

        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get default metrics for suppressed records."""
        # Determine course type for appropriate defaults
        course_type = str(row.get('course_type', '')).upper()
        
        if 'AP' in course_type or 'ADVANCED PLACEMENT' in course_type:
            prefix = 'ap'
        elif 'IB' in course_type:
            prefix = 'ib'
        elif 'CAMBRIDGE' in course_type:
            prefix = 'cambridge'
        else:
            prefix = 'advanced'
        
        return {
            f'{prefix}_course_enrollment': pd.NA,
            f'{prefix}_completion_count': pd.NA,
            f'{prefix}_tested_count': pd.NA,
            f'{prefix}_qualifying_score_count': pd.NA,
            f'{prefix}_qualifying_score_rate': pd.NA,
            f'{prefix}_participation_rate': pd.NA,
            f'{prefix}_participation_rate_female': pd.NA,
            f'{prefix}_participation_rate_male': pd.NA,
            f'num_{prefix}_courses_offered': pd.NA,
        }

    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Override to include advanced coursework specific missing value handling."""
        # Apply base missing value standardization
        df = super().standardize_missing_values(df)

        # Handle comma-separated numbers
        numeric_columns = ['course_enrollment', 'course_completers', 'number_tested',
                         'qualifying_score', 'total_count', 'female_count', 'male_count',
                         'total_rate', 'female_rate', 'male_rate']
        for col in numeric_columns:
            if col in df.columns and df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.replace(',', '')

        return df


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Process advanced coursework data and convert to KPI format using BaseETL."""
    etl = AdvancedCourseworkETL('advanced_coursework')
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
