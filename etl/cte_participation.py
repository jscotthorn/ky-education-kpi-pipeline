"""
CTE Participation ETL Module

Handles Kentucky Career and Technical Education (CTE) participation data across years 2022-2024.
Normalizes column names, handles schema variations, standardizes missing values, and
applies demographic label standardization for consistent longitudinal reporting.

Data includes three metrics:
- CTE Participation Rate: Percentage of students participating in CTE courses
- Grade 12 CTE Eligible Completer Count: Number of eligible CTE completers in grade 12
- Grade 12 CTE Completion Rate: Percentage of grade 12 students completing CTE programs
"""
from pathlib import Path
import pandas as pd
from typing import Dict, Any, Union
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
    # Remove commas and quotes, then convert to numeric
    cleaned = series.astype(str).str.replace(',', '').str.replace('"', '')
    return pd.to_numeric(cleaned, errors='coerce')


def clean_cte_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate CTE participation values."""
    # Clean participation rates
    if 'cte_participation_rate' in df.columns:
        df['cte_participation_rate'] = clean_numeric_with_commas(df['cte_participation_rate'])

        invalid_mask = (df['cte_participation_rate'] < 0) | (df['cte_participation_rate'] > 100)
        if invalid_mask.any():
            logger.warning(f"Found {invalid_mask.sum()} invalid CTE participation rates (outside 0-100%)")
            df.loc[invalid_mask, 'cte_participation_rate'] = pd.NA
    
    # Clean completion rates
    if 'cte_completion_rate' in df.columns:
        df['cte_completion_rate'] = clean_numeric_with_commas(df['cte_completion_rate'])

        invalid_mask = (df['cte_completion_rate'] < 0) | (df['cte_completion_rate'] > 100)
        if invalid_mask.any():
            logger.warning(f"Found {invalid_mask.sum()} invalid CTE completion rates (outside 0-100%)")
            df.loc[invalid_mask, 'cte_completion_rate'] = pd.NA
    
    # Clean eligible completer count
    if 'cte_eligible_completer_count' in df.columns:
        df['cte_eligible_completer_count'] = clean_numeric_with_commas(df['cte_eligible_completer_count'])
        
        # Ensure counts are non-negative
        invalid_mask = df['cte_eligible_completer_count'] < 0
        if invalid_mask.any():
            logger.warning(f"Found {invalid_mask.sum()} negative CTE eligible completer counts")
            df.loc[invalid_mask, 'cte_eligible_completer_count'] = pd.NA
    
    # Clean total student count
    if 'total_student_count' in df.columns:
        df['total_student_count'] = clean_numeric_with_commas(df['total_student_count'])
        
        # Ensure counts are non-negative
        invalid_mask = df['total_student_count'] < 0
        if invalid_mask.any():
            logger.warning(f"Found {invalid_mask.sum()} negative total student counts")
            df.loc[invalid_mask, 'total_student_count'] = pd.NA
    
    return df


class CTEParticipationETL(BaseETL):
    """ETL module for processing CTE participation data."""
    
    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            # Participation rate columns
            'CTE Participants in All Grades': 'cte_participation_rate',
            'CTE PARTICIPANTS IN ALL GRADES': 'cte_participation_rate',
            
            # Eligible completer count columns
            'Grade 12 CTE Eligible Completer': 'cte_eligible_completer_count',
            'GRADE 12 CTE ELIGIBLE COMPLETER': 'cte_eligible_completer_count',
            
            # Completion rate columns
            'Grade 12 CTE Completers': 'cte_completion_rate',
            'GRADE 12 CTE COMPLETERS': 'cte_completion_rate',
            
            # Total student count for calculating rates when needed
            'Total Number of Student': 'total_student_count',
            'TOTAL NUMBER OF STUDENT': 'total_student_count',
        }
    
    def detect_data_format(self, df: pd.DataFrame) -> dict:
        """Detect whether columns contain rates (0-100) or counts (>100)."""
        format_info = {}
        
        # Check participation rates
        if 'cte_participation_rate' in df.columns:
            participation_values = clean_numeric_with_commas(df['cte_participation_rate']).dropna()
            if len(participation_values) > 0:
                max_val = participation_values.max()
                # If most values are > 100, it's likely counts; if <= 100, it's likely rates
                format_info['participation_is_rate'] = max_val <= 100
            else:
                format_info['participation_is_rate'] = True  # Default assumption
        
        # Check completion rates
        if 'cte_completion_rate' in df.columns:
            completion_values = clean_numeric_with_commas(df['cte_completion_rate']).dropna()
            if len(completion_values) > 0:
                max_val = completion_values.max()
                format_info['completion_is_rate'] = max_val <= 100
            else:
                format_info['completion_is_rate'] = True  # Default assumption
                
        return format_info
    
    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        metrics = {}
        
        # Extract CTE participation (rate or count)
        participation_value = row.get('cte_participation_rate', pd.NA)
        total_students = row.get('total_student_count', pd.NA)
        
        if pd.notna(participation_value):
            # Determine if this is a rate or count based on the value and data availability
            if participation_value <= 100:
                # This appears to be a rate (0-100%)
                metrics['cte_participation_rate'] = participation_value
            else:
                # This appears to be a count (>100) - convert to rate if we have total
                if pd.notna(total_students) and total_students > 0:
                    rate = round((participation_value / total_students) * 100, 1)
                    metrics['cte_participation_rate'] = rate
                else:
                    # Can't convert to rate due to missing/suppressed total
                    # Store as count with different metric name for historical data
                    metrics['cte_participation_count'] = participation_value
        
        # Extract Grade 12 CTE eligible completer count
        eligible_count = row.get('cte_eligible_completer_count', pd.NA)
        if pd.notna(eligible_count):
            metrics['cte_eligible_completer_count_grade_12'] = eligible_count
        
        # Extract Grade 12 CTE completion (rate or count)
        completion_value = row.get('cte_completion_rate', pd.NA)
        if pd.notna(completion_value):
            if completion_value <= 100:
                # This appears to be a rate (0-100%)
                metrics['cte_completion_rate_grade_12'] = completion_value
            else:
                # This appears to be a count (>100) - convert to rate if we have eligible count
                if pd.notna(eligible_count) and eligible_count > 0:
                    rate = round((completion_value / eligible_count) * 100, 1)
                    metrics['cte_completion_rate_grade_12'] = rate
                else:
                    # Can't convert to rate due to missing eligible count
                    # Store as count with different metric name for historical data
                    metrics['cte_completion_count_grade_12'] = completion_value
        
        return metrics
    
    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get default metrics for suppressed CTE participation records."""
        return {
            'cte_participation_rate': pd.NA,
            'cte_participation_count': pd.NA,
            'cte_eligible_completer_count_grade_12': pd.NA,
            'cte_completion_rate_grade_12': pd.NA,
            'cte_completion_count_grade_12': pd.NA
        }
    
    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Override to include CTE participation specific missing value handling."""
        # Apply base missing value standardization
        df = super().standardize_missing_values(df)
        
        # Handle comma-separated numbers before cleaning
        numeric_columns = ['cte_participation_rate', 'cte_eligible_completer_count', 'cte_completion_rate']
        for col in numeric_columns:
            if col in df.columns and df[col].dtype == 'object':
                # Remove commas from string values
                df[col] = df[col].astype(str).str.replace(',', '')
        
        # Apply CTE-specific cleaning
        df = clean_cte_data(df)
        
        return df


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Read newest CTE participation files, normalize, and convert to KPI format with demographic standardization using BaseETL."""
    # Use BaseETL for consistent processing
    etl = CTEParticipationETL('cte_participation')
    etl.process(raw_dir, proc_dir, cfg)


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    
    from pathlib import Path
    raw_dir = Path(__file__).parent.parent / "data" / "raw"
    proc_dir = Path(__file__).parent.parent / "data" / "processed"
    proc_dir.mkdir(exist_ok=True)
    
    test_config = Config(
        derive={"processing_date": "2025-07-21", "data_quality_flag": "reviewed"}
    ).dict()

    transform(raw_dir, proc_dir, test_config)