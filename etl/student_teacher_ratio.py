"""
Student-Teacher Ratio ETL Module

Processes Kentucky student-teacher ratio data showing class size indicators
by school/district. This is an institutional-level metric without demographic
breakdowns.

Data includes:
- Student-teacher ratios in format "15:01" (parsed to numeric 15.0)
- School-level and district-level aggregations
"""
from pathlib import Path
import pandas as pd
from typing import Dict, Any
import logging
import sys
import re

# Add etl directory to path for imports
etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from constants import KPI_COLUMNS
from base_etl import BaseETL, Config

logger = logging.getLogger(__name__)


def parse_ratio(ratio_str: str) -> float:
    """
    Parse ratio string like "15:01" into numeric value 15.0.
    
    Args:
        ratio_str: Ratio string in format "XX:YY"
        
    Returns:
        Numeric ratio value (first number before colon)
    """
    if pd.isna(ratio_str):
        return pd.NA
    
    # Convert to string and clean
    ratio_str = str(ratio_str).strip()
    
    # Match pattern like "15:01" or "15.5:01"
    match = re.match(r'^([\d.]+):(\d+)$', ratio_str)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            logger.warning(f"Could not convert ratio {ratio_str} to float")
            return pd.NA
    
    # Try to parse as plain number (in case format changes)
    try:
        return float(ratio_str)
    except (ValueError, TypeError):
        logger.warning(f"Invalid ratio format: {ratio_str}")
        return pd.NA


class StudentTeacherRatioETL(BaseETL):
    """ETL module for processing student-teacher ratio data."""
    
    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            'Student Teacher Ratio': 'ratio',
        }
    
    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        metrics = {}
        
        # Parse ratio string to numeric value
        ratio_str = row.get('ratio', pd.NA)
        ratio_numeric = parse_ratio(ratio_str)
        
        if pd.notna(ratio_numeric) and ratio_numeric > 0:
            metrics['student_teacher_ratio'] = ratio_numeric
        
        return metrics
    
    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get default metrics for suppressed student-teacher ratio records."""
        defaults = {}
        
        # Only create defaults for metrics that exist in the source data
        if 'ratio' in row.index:
            defaults['student_teacher_ratio'] = pd.NA
            
        return defaults
    
    def should_skip_row(self, row: pd.Series) -> bool:
        """Skip rows that don't have student-teacher ratio data."""
        # Check if we have ratio value
        ratio = row.get('ratio', pd.NA)
        if pd.isna(ratio):
            return True
            
        return super().should_skip_row(row)


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Read student-teacher ratio files, normalize, and convert to KPI format."""
    etl = StudentTeacherRatioETL('student_teacher_ratio')
    etl.process(raw_dir, proc_dir, cfg)


def main():
    """Run student-teacher ratio ETL process."""
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
