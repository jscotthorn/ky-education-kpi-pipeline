# Copyright 2025 Kentucky Open Government Coalition
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Graduation Rates ETL Module

Handles multiple file formats from Kentucky graduation rate data across years 2021-2024.
Normalizes column names, handles schema variations, standardizes missing values, and
applies demographic label standardization for consistent longitudinal reporting.
"""
from pathlib import Path
import pandas as pd
from typing import Dict, Any, Optional, Union
import logging
import sys
from pathlib import Path

# Add etl directory to path for imports
etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from base_etl import BaseETL, Config

logger = logging.getLogger(__name__)








def clean_graduation_rates(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate graduation rate values."""
    rate_columns = [col for col in df.columns if 'graduation_rate' in col]
    
    for col in rate_columns:
        if col in df.columns:
            # Convert to numeric, errors='coerce' will make invalid values NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Validate rates are between 0 and 100
            invalid_mask = (df[col] < 0) | (df[col] > 100)
            if invalid_mask.any():
                logger.warning(f"Found {invalid_mask.sum()} invalid graduation rates in {col}")
                df.loc[invalid_mask, col] = pd.NA
    
    return df


def handle_suppression_fields(df: pd.DataFrame) -> pd.DataFrame:
    """Handle graduation-specific suppression fields."""
    # Map suppression fields to standard 'suppressed' field
    suppressed_4_year = df.get('suppressed_4_year', 'N')
    suppressed_5_year = df.get('suppressed_5_year', 'N')
    
    # For now, use 4-year suppression as default (could be enhanced)
    df['suppressed'] = suppressed_4_year
    
    return df


class GraduationRatesETL(BaseETL):
    """ETL module for processing graduation rates data."""
    
    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            # Suppression indicators
            'Suppressed': 'suppressed_4_year',
            'SUPPRESSED 4 YEAR': 'suppressed_4_year',
            'Suppressed 4 Year': 'suppressed_4_year',
            'SUPPRESSED 5 YEAR': 'suppressed_5_year',
            
            # Graduation rate metrics
            '4 Year Cohort Graduation Rate': 'graduation_rate_4_year',
            '4-YEAR GRADUATION RATE': 'graduation_rate_4_year',
            '5-YEAR GRADUATION RATE': 'graduation_rate_5_year',
            
            # Count metrics
            'NUMBER OF GRADS IN 4-YEAR COHORT': 'grads_4_year_cohort',
            'NUMBER OF STUDENTS IN 4-YEAR COHORT': 'students_4_year_cohort',
            'NUMBER OF GRADS IN 5-YEAR COHORT': 'grads_5_year_cohort',
            'NUMBER OF STUDENTS IN 5-YEAR COHORT': 'students_5_year_cohort',
        }
    
    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        metrics = {}
        
        # Process 4-year graduation metrics
        if 'graduation_rate_4_year' in row and pd.notna(row['graduation_rate_4_year']):
            metrics['graduation_rate_4_year'] = row['graduation_rate_4_year']
        
        if 'grads_4_year_cohort' in row and pd.notna(row['grads_4_year_cohort']):
            metrics['graduation_count_4_year'] = row['grads_4_year_cohort']
        
        if 'students_4_year_cohort' in row and pd.notna(row['students_4_year_cohort']):
            metrics['graduation_total_4_year'] = row['students_4_year_cohort']
        
        # Process 5-year graduation metrics
        if 'graduation_rate_5_year' in row and pd.notna(row['graduation_rate_5_year']):
            metrics['graduation_rate_5_year'] = row['graduation_rate_5_year']
        
        if 'grads_5_year_cohort' in row and pd.notna(row['grads_5_year_cohort']):
            metrics['graduation_count_5_year'] = row['grads_5_year_cohort']
        
        if 'students_5_year_cohort' in row and pd.notna(row['students_5_year_cohort']):
            metrics['graduation_total_5_year'] = row['students_5_year_cohort']
        
        return metrics
    
    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get default metrics for suppressed graduation records."""
        defaults = {}
        
        # Only create defaults for metrics that exist in the source data
        if 'graduation_rate_4_year' in row.index:
            defaults['graduation_rate_4_year'] = pd.NA
        if 'grads_4_year_cohort' in row.index:
            defaults['graduation_count_4_year'] = pd.NA
        if 'students_4_year_cohort' in row.index:
            defaults['graduation_total_4_year'] = pd.NA
        if 'graduation_rate_5_year' in row.index:
            defaults['graduation_rate_5_year'] = pd.NA
        if 'grads_5_year_cohort' in row.index:
            defaults['graduation_count_5_year'] = pd.NA
        if 'students_5_year_cohort' in row.index:
            defaults['graduation_total_5_year'] = pd.NA
            
        return defaults
    
    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Override to include graduation-specific missing value handling."""
        # Apply base missing value standardization
        df = super().standardize_missing_values(df)
        
        # Apply graduation-specific cleaning
        df = clean_graduation_rates(df)
        df = handle_suppression_fields(df)
        
        return df

    def add_derived_fields(self, df: pd.DataFrame, derive_config: Dict[str, Any], source_file: str) -> pd.DataFrame:
        """Override to add graduation-specific data source identification."""
        # Apply base derived fields
        df = super().add_derived_fields(df, derive_config, source_file)
        
        # Add graduation-specific data source field based on columns present
        if 'grads_4_year_cohort' in df.columns:
            df['data_source'] = '2021_detailed'
        elif 'graduation_rate_5_year' in df.columns:
            df['data_source'] = '2022_2023_standard'
        else:
            df['data_source'] = '2024_simplified'
        
        return df


# Legacy function for backward compatibility
def convert_to_kpi_format(df: pd.DataFrame, demographic_mapper: Optional = None) -> pd.DataFrame:
    """Legacy function - now uses BaseETL internally."""
    etl = GraduationRatesETL('graduation_rates')
    return etl.convert_to_kpi_format(df, 'legacy_call.csv')


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Read graduation rates files, normalize, and convert to KPI format with demographic standardization using BaseETL."""
    # Use BaseETL for consistent processing
    etl = GraduationRatesETL('graduation_rates')
    etl.process(raw_dir, proc_dir, cfg)


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    
    from pathlib import Path
    raw_dir = Path(__file__).parent.parent / "data" / "raw"
    proc_dir = Path(__file__).parent.parent / "data" / "processed"
    proc_dir.mkdir(exist_ok=True)
    
    test_config = Config(
        derive={"processing_date": "2025-07-18", "data_quality_flag": "reviewed"}
    ).dict()

    transform(raw_dir, proc_dir, test_config)