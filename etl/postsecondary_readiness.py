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
from typing import Dict, Any, Optional, Union
import logging
from datetime import datetime

import sys
from pathlib import Path

# Add etl directory to path for imports
etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from demographic_mapper import DemographicMapper

logger = logging.getLogger(__name__)


class Config(BaseModel):
    rename: Dict[str, str] = {}
    dtype: Dict[str, str] = {}
    derive: Dict[str, Union[str, int, float]] = {}


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names using BaseETL common mappings plus postsecondary-specific columns."""
    from base_etl import BaseETL
    
    # Get common mappings from BaseETL
    column_mapping = BaseETL.COMMON_COLUMN_MAPPINGS.copy()
    
    # Add postsecondary-specific column mappings
    postsecondary_mappings = {
        'Postsecondary Rate': 'postsecondary_rate',
        'POSTSECONDARY RATE': 'postsecondary_rate',
        'Postsecondary Rate With Bonus': 'postsecondary_rate_with_bonus',
        'POSTSECONDARY RATE WITH BONUS': 'postsecondary_rate_with_bonus',
    }
    column_mapping.update(postsecondary_mappings)
    
    # Apply mapping for columns that exist
    rename_dict = {col: column_mapping[col] for col in df.columns if col in column_mapping}
    return df.rename(columns=rename_dict)


def standardize_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Convert empty strings and suppression markers to NaN."""
    # Replace empty strings with NaN
    df = df.replace('', pd.NA)
    df = df.replace('""', pd.NA)
    
    # Replace suppression markers with NaN in rate columns
    rate_columns = [col for col in df.columns if 'postsecondary_rate' in col]
    for col in rate_columns:
        if col in df.columns:
            df[col] = df[col].replace('*', pd.NA)
    
    return df


def add_derived_fields(df: pd.DataFrame, derive_config: Dict[str, Any]) -> pd.DataFrame:
    """Add derived fields based on configuration."""
    for field, value in derive_config.items():
        df[field] = value
    
    # Add data_source field based on columns present and format
    if 'postsecondary_rate' in df.columns:
        df['data_source'] = 'postsecondary_rates'
    else:
        df['data_source'] = 'unknown_format'
    
    return df


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


# Backward compatibility wrapper for tests
def convert_to_kpi_format(df: pd.DataFrame, demographic_mapper: Optional[DemographicMapper] = None) -> pd.DataFrame:
    """Backward compatibility wrapper for convert_to_kpi_format function."""
    from base_etl import BaseETL
    from typing import Dict, Any
    
    class PostsecondaryReadinessETL(BaseETL):
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
            
            # Extract postsecondary readiness rates
            if 'postsecondary_rate' in row and pd.notna(row['postsecondary_rate']):
                metrics['postsecondary_readiness_rate'] = row['postsecondary_rate']
            
            if 'postsecondary_rate_with_bonus' in row and pd.notna(row['postsecondary_rate_with_bonus']):
                metrics['postsecondary_readiness_rate_with_bonus'] = row['postsecondary_rate_with_bonus']
            
            return metrics
    
    # Create ETL instance  
    etl = PostsecondaryReadinessETL()
    
    # Set demographic mapper if provided
    if demographic_mapper is not None:
        etl.demographic_mapper = demographic_mapper
    
    # Extract source_file from dataframe if available
    source_file = df['source_file'].iloc[0] if 'source_file' in df.columns else 'postsecondary_readiness.csv'
    
    # CRITICAL FIX: Normalize data before calling BaseETL convert_to_kpi_format
    df_normalized = etl.normalize_column_names(df.copy())
    df_normalized = etl.standardize_missing_values(df_normalized)
    
    # Use BaseETL convert_to_kpi_format method with pre-normalized data
    return etl.convert_to_kpi_format(df_normalized, source_file)


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Read newest postsecondary readiness files, normalize, and convert to KPI format with demographic standardization using BaseETL."""
    from base_etl import BaseETL
    from typing import Dict, Any
    
    class PostsecondaryReadinessETL(BaseETL):
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
            
            # Extract postsecondary readiness rates
            if 'postsecondary_rate' in row and pd.notna(row['postsecondary_rate']):
                metrics['postsecondary_readiness_rate'] = row['postsecondary_rate']
            
            if 'postsecondary_rate_with_bonus' in row and pd.notna(row['postsecondary_rate_with_bonus']):
                metrics['postsecondary_readiness_rate_with_bonus'] = row['postsecondary_rate_with_bonus']
            
            return metrics
    
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