"""
Chronic Absenteeism ETL Module

Handles chronic absenteeism data from Kentucky across years 2023-2024.
Processes chronic absenteeism rates, counts, and enrollment data with grade-level
and demographic breakdowns following standardized KPI format.
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


def clean_numeric_values(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and convert numeric values, handling commas and invalid data."""
    numeric_columns = ['chronically_absent_count', 'enrollment_count', 'chronic_absenteeism_rate']
    
    for col in numeric_columns:
        if col in df.columns:
            # Remove commas from numbers
            df[col] = df[col].astype(str).str.replace(',', '').replace('nan', pd.NA)
            
            # Convert to numeric
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Validate ranges
            if col == 'chronic_absenteeism_rate':
                # Rates should be between 0 and 100
                invalid_mask = (df[col] < 0) | (df[col] > 100)
                if invalid_mask.any():
                    logger.warning(f"Found {invalid_mask.sum()} invalid chronic absenteeism rates in {col}")
                    df.loc[invalid_mask, col] = pd.NA
            elif col in ['chronically_absent_count', 'enrollment_count']:
                # Counts should be non-negative
                invalid_mask = df[col] < 0
                if invalid_mask.any():
                    logger.warning(f"Found {invalid_mask.sum()} negative counts in {col}")
                    df.loc[invalid_mask, col] = pd.NA
    
    return df


def standardize_suppression_field(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize suppression indicators to Y/N format."""
    if 'suppressed' in df.columns:
        # Map various suppression indicators to standard Y/N
        suppression_mapping = {
            'Yes': 'Y',
            'No': 'N',
            'Y': 'Y',
            'N': 'N',
            True: 'Y',
            False: 'N',
            1: 'Y',
            0: 'N'
        }
        
        df['suppressed'] = df['suppressed'].map(suppression_mapping).fillna('N')
    else:
        # If no suppression column, infer from data
        df['suppressed'] = 'N'
        
        # Mark as suppressed if chronic_absenteeism_rate is missing but other data exists
        if 'chronic_absenteeism_rate' in df.columns and 'demographic' in df.columns:
            missing_rate = df['chronic_absenteeism_rate'].isna()
            has_demographic = df['demographic'].notna()
            df.loc[missing_rate & has_demographic, 'suppressed'] = 'Y'
    
    return df




class ChronicAbsenteeismETL(BaseETL):
    """ETL module for processing chronic absenteeism data."""
    
    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            'Chronically Absent Students': 'chronically_absent_count',
            'CHRONIC ABSENTEE COUNT': 'chronically_absent_count',
            'Students Enrolled 10 or More Days': 'enrollment_count',
            'ENROLLMENT COUNT OF STUDENTS WITH 10+ ENROLLED DAYS': 'enrollment_count',
            'Chronic Absenteeism Rate': 'chronic_absenteeism_rate',
            'PERCENT CHRONICALLY ABSENT': 'chronic_absenteeism_rate',
        }
    
    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        metrics = {}
        
        # Get grade for metric naming - normalize grade names
        grade = row.get('grade', 'all_grades')
        if pd.isna(grade) or grade == '':
            grade = 'all_grades'
        elif str(grade).lower() == 'all grades':
            grade = 'all_grades'
        else:
            # Convert "Grade X" to "grade_X" format
            grade = str(grade).lower().replace(' ', '_')
        
        # Extract chronic absenteeism rate
        if 'chronic_absenteeism_rate' in row and pd.notna(row['chronic_absenteeism_rate']):
            metrics[f'chronic_absenteeism_rate_{grade}'] = row['chronic_absenteeism_rate']
        
        # Extract chronic absenteeism count
        if 'chronically_absent_count' in row and pd.notna(row['chronically_absent_count']):
            metrics[f'chronic_absenteeism_count_{grade}'] = row['chronically_absent_count']
        
        # Extract enrollment count
        if 'enrollment_count' in row and pd.notna(row['enrollment_count']):
            metrics[f'chronic_absenteeism_enrollment_{grade}'] = row['enrollment_count']
        
        return metrics
    
    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get default metrics for suppressed chronic absenteeism records."""
        grade = row.get('grade', 'all_grades')
        if pd.isna(grade) or grade == '':
            grade = 'all_grades'
        
        return {
            f'chronic_absenteeism_rate_{grade}': pd.NA,
            f'chronic_absenteeism_count_{grade}': pd.NA,
            f'chronic_absenteeism_enrollment_{grade}': pd.NA
        }
    
    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Override to include chronic absenteeism specific missing value handling."""
        # Apply base missing value standardization
        df = super().standardize_missing_values(df)
        
        # Apply chronic absenteeism specific cleaning
        df = clean_numeric_values(df)
        df = standardize_suppression_field(df)
        
        return df


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Read chronic absenteeism files, normalize, and convert to KPI format with demographic standardization using BaseETL."""
    # Use BaseETL for consistent processing
    etl = ChronicAbsenteeismETL('chronic_absenteeism')
    etl.process(raw_dir, proc_dir, cfg)




if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    
    from pathlib import Path
    raw_dir = Path(__file__).parent.parent / "data" / "raw"
    proc_dir = Path(__file__).parent.parent / "data" / "processed"
    proc_dir.mkdir(exist_ok=True)
    
    test_config = Config(
        derive={"processing_date": "2025-07-19", "data_quality_flag": "reviewed"}
    ).dict()

    transform(raw_dir, proc_dir, test_config)