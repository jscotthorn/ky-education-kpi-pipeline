"""
Postsecondary Readiness ETL Module

Handles Kentucky postsecondary readiness data across years 2022-2024.
Normalizes column names, handles schema variations, standardizes missing values, and
applies demographic label standardization for consistent longitudinal reporting.

Data includes two rate metrics:
- Postsecondary Rate: Base readiness rate
- Postsecondary Rate With Bonus: Enhanced rate including bonus indicators
"""
from pathlib import Path
import pandas as pd
from pydantic import BaseModel
from typing import Dict, Any, Union
import logging

import sys
from pathlib import Path

# Add etl directory to path for imports
etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from base_etl import BaseETL

logger = logging.getLogger(__name__)


class Config(BaseModel):
    rename: Dict[str, str] = {}
    dtype: Dict[str, str] = {}
    derive: Dict[str, Union[str, int, float]] = {}




def clean_readiness_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate postsecondary readiness rate values."""
    rate_columns = [col for col in df.columns if 'postsecondary_rate' in col]
    
    for col in rate_columns:
        if col in df.columns:
            # Convert to numeric, errors='coerce' will make invalid values NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Validate rates are between 0 and 100
            invalid_mask = (df[col] < 0) | (df[col] > 100)
            if invalid_mask.any():
                logger.warning(f"Found {invalid_mask.sum()} invalid readiness rates in {col}")
                df.loc[invalid_mask, col] = pd.NA
    
    return df




class PostsecondaryReadinessETL(BaseETL):
    """ETL module for processing postsecondary readiness data."""
    
    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            'Postsecondary Rate': 'postsecondary_rate',
            'POSTSECONDARY RATE': 'postsecondary_rate',
            'Postsecondary Rate With Bonus': 'postsecondary_rate_with_bonus',
            'POSTSECONDARY RATE WITH BONUS': 'postsecondary_rate_with_bonus',
        }
    
    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        metrics = {}
        
        # Always extract both postsecondary readiness rates for consistency
        # Even if one or both are null/suppressed/invalid
        metrics['postsecondary_readiness_rate'] = row.get('postsecondary_rate', pd.NA)
        metrics['postsecondary_readiness_rate_with_bonus'] = row.get('postsecondary_rate_with_bonus', pd.NA)
        
        return metrics
    
    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get default metrics for suppressed postsecondary readiness records."""
        return {
            'postsecondary_readiness_rate': pd.NA,
            'postsecondary_readiness_rate_with_bonus': pd.NA
        }
    
    def convert_to_kpi_format(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """
        Override to ensure both postsecondary metrics are always created together.
        """
        kpi_rows = []
        
        for _, row in df.iterrows():
            if self.should_skip_row(row):
                continue
            
            kpi_template = self.create_kpi_template(row, source_file)
            metrics = self.extract_metrics(row)
            
            # Special handling for postsecondary readiness: always create both metrics
            base_value = metrics.get('postsecondary_readiness_rate')
            bonus_value = metrics.get('postsecondary_readiness_rate_with_bonus')
            
            # Create base rate record
            base_record = kpi_template.copy()
            base_record['metric'] = 'postsecondary_readiness_rate'
            if kpi_template['suppressed'] == 'Y' or pd.isna(base_value):
                base_record['value'] = pd.NA
            else:
                try:
                    base_record['value'] = float(base_value)
                except (ValueError, TypeError):
                    base_record['value'] = pd.NA
            kpi_rows.append(base_record)
            
            # Create bonus rate record  
            bonus_record = kpi_template.copy()
            bonus_record['metric'] = 'postsecondary_readiness_rate_with_bonus'
            if kpi_template['suppressed'] == 'Y' or pd.isna(bonus_value):
                bonus_record['value'] = pd.NA
            else:
                try:
                    bonus_record['value'] = float(bonus_value)
                except (ValueError, TypeError):
                    bonus_record['value'] = pd.NA
            kpi_rows.append(bonus_record)
        
        if not kpi_rows:
            logger.warning("No valid KPI rows created")
            return pd.DataFrame()
        
        # Create KPI DataFrame with consistent column order
        kpi_df = pd.DataFrame(kpi_rows)
        kpi_columns = ['district', 'school_id', 'school_name', 'year', 'student_group', 
                       'metric', 'value', 'suppressed', 'source_file', 'last_updated']
        
        # Only include columns that exist
        available_columns = [col for col in kpi_columns if col in kpi_df.columns]
        kpi_df = kpi_df[available_columns]
        
        return kpi_df
    
    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Override to include postsecondary readiness specific missing value handling."""
        # Apply base missing value standardization
        df = super().standardize_missing_values(df)
        
        # Apply postsecondary-specific cleaning
        df = clean_readiness_data(df)
        
        return df


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Read newest postsecondary readiness files, normalize, and convert to KPI format with demographic standardization using BaseETL."""
    # Use BaseETL for consistent processing
    etl = PostsecondaryReadinessETL('postsecondary_readiness')
    etl.transform(raw_dir, proc_dir, cfg)


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    
    from pathlib import Path
    raw_dir = Path(__file__).parent.parent / "data" / "raw"
    proc_dir = Path(__file__).parent.parent / "data" / "processed"
    proc_dir.mkdir(exist_ok=True)
    
    test_config = {
        "derive": {
            "processing_date": "2025-07-19",
            "data_quality_flag": "reviewed"
        }
    }
    
    transform(raw_dir, proc_dir, test_config)