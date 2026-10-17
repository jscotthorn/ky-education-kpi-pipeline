"""
Postsecondary Readiness ETL Module

Handles Kentucky postsecondary readiness data across years 2022-2026.
Normalizes column names, handles schema variations, standardizes missing values, and
applies demographic label standardization for consistent longitudinal reporting.

Data includes two rate metrics:
- Postsecondary Rate: Base readiness rate
- Postsecondary Rate With Bonus: Enhanced rate including bonus indicators

KYRC26 is ``KYRC26_ACCT_Postsecondary_Readiness.csv``. Its headers match
the KYRC24 and KYRC25 files this module already maps.
``Postsecondary_Readiness_2025.CSV`` is 404. The permanent KYRC25 file is
the 2024-25 source. A leftover temporary extract for 2024, 2025, or 2026
would emit a second copy of that year because those rate headers are
already mapped.
"""
from pathlib import Path
import pandas as pd
from typing import Dict, Any, Union
import logging
import sys

# Add etl directory to path for imports
etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from constants import KPI_COLUMNS

from base_etl import BaseETL, Config

logger = logging.getLogger(__name__)




def clean_readiness_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate postsecondary readiness rate values."""
    rate_columns = [col for col in df.columns if 'postsecondary_rate' in col]
    
    for col in rate_columns:
        if col in df.columns:
            # Convert to numeric, errors='coerce' will make invalid values NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Different validation rules for different rate types
            if 'with_bonus' in col.lower():
                # Bonus rates can exceed 100% but should be reasonable (0-150%)
                invalid_mask = (df[col] < 0) | (df[col] > 150)
                if invalid_mask.any():
                    logger.warning(f"Found {invalid_mask.sum()} invalid bonus readiness rates in {col} (outside 0-150%)")
                    df.loc[invalid_mask, col] = pd.NA
            else:
                # Standard rates should be between 0 and 100%
                invalid_mask = (df[col] < 0) | (df[col] > 100)
                if invalid_mask.any():
                    logger.warning(f"Found {invalid_mask.sum()} invalid readiness rates in {col} (outside 0-100%)")
                    df.loc[invalid_mask, col] = pd.NA
    
    return df


def _rate_is_empty(value: Any) -> bool:
    """True when a readiness rate was blank, not a published number."""
    if isinstance(value, str):
        return value.strip() == ''
    return bool(pd.isna(value))


class PostsecondaryReadinessETL(BaseETL):
    """ETL module for processing postsecondary readiness data.

    KYRC26 uses the same rate headers as KYRC24 and KYRC25.
    ``Postsecondary_Readiness_2025.CSV`` is 404. The permanent KYRC25
    file is the 2024-25 source. A leftover temporary extract for 2024,
    2025, or 2026 would emit a second copy of that year because
    ``Postsecondary Rate`` and ``Postsecondary Rate With Bonus`` are
    already mapped.
    """

    # Opt in here, in the same change that accepts the file. BaseETL skips
    # every KYRC26_* CSV until this is True.
    include_kyrc26 = True

    def accepts_data_file(self, file_path: Path) -> bool:
        """Skip a leftover temporary OAA readiness extract.

        ``Postsecondary_Readiness_2024.CSV``,
        ``Postsecondary_Readiness_2025.CSV``, and
        ``Postsecondary_Readiness_2026.CSV`` use the rate headers this
        module already maps. Permanent KYRC files are the source for
        those years. The 2025 file is also 404 upstream. A leftover copy
        would emit a second set of rows beside the KYRC file, under
        3-digit district codes and district ``State``. The match is
        case-insensitive. ``postsecondary_readiness_2023.csv`` and
        ``postsecondary_readiness_2022.csv`` stay.
        """
        if file_path.name.lower() in {
            "postsecondary_readiness_2024.csv",
            "postsecondary_readiness_2025.csv",
            "postsecondary_readiness_2026.csv",
        }:
            logger.info(
                "Skipping %s; the permanent KYRC readiness file is the source",
                file_path.name,
            )
            return False
        return super().accepts_data_file(file_path)

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            'Postsecondary Rate': 'postsecondary_rate',
            'POSTSECONDARY RATE': 'postsecondary_rate',
            'Postsecondary Rate With Bonus': 'postsecondary_rate_with_bonus',
            'POSTSECONDARY RATE WITH BONUS': 'postsecondary_rate_with_bonus',
            # Historical xlsx format (2018-19) - TRANSITION_READINESS_ACCOUNTABILITY
            'SCH_YEAR': 'school_year',
            'CNTYNO': 'county_number',
            'CNTYNAME': 'county_name',
            'DIST_NUMBER': 'district_number',
            'DIST_NAME': 'district_name',
            'SCH_NUMBER': 'school_number',
            'SCH_NAME': 'school_name',
            'SCH_CD': 'school_code',
            'STATE_SCH_ID': 'state_school_id',
            'NCESID': 'nces_id',
            'COOP': 'co_op',
            'COOP_CODE': 'co_op_code',
            'DEMOGRAPHIC': 'demographic',
            'SUPPRESSED': 'suppressed',
            'TRANSITIONRATE': 'postsecondary_rate',
            'TRANRATEWBONUS': 'postsecondary_rate_with_bonus',
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
        defaults = {}
        
        # Only create defaults for metrics that exist in the source data
        if 'postsecondary_rate' in row.index:
            defaults['postsecondary_readiness_rate'] = pd.NA
        if 'postsecondary_rate_with_bonus' in row.index:
            defaults['postsecondary_readiness_rate_with_bonus'] = pd.NA
            
        return defaults
    
    def convert_to_kpi_format(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """
        Override to keep the two postsecondary metrics together.

        A ``Y`` row is a privacy suppression and emits both metrics empty.
        A row that is not ``Y`` and has both rates empty was not reported.
        notes/8 treats that as the absence of a row, the same rule chronic
        absenteeism, graduation, and English learner progress use. Journal 75
        emitted those rows as ``suppressed`` ``N`` with an empty value.
        """
        kpi_rows = []
        
        for _, row in df.iterrows():
            if self.should_skip_row(row):
                continue

            kpi_template = self.create_kpi_template(row, source_file)
            if kpi_template is None:
                # Row was filtered (e.g., demographic not recognized or filtered out)
                continue

            metrics = self.extract_metrics(row)
            
            # Special handling for postsecondary readiness: always create both metrics
            base_value = metrics.get('postsecondary_readiness_rate')
            bonus_value = metrics.get('postsecondary_readiness_rate_with_bonus')

            # Not offered / not reported. Do not store it as an unsuppressed NaN.
            if (
                kpi_template['suppressed'] != 'Y'
                and _rate_is_empty(base_value)
                and _rate_is_empty(bonus_value)
            ):
                continue
            
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

        # Only include columns that exist
        available_columns = [col for col in KPI_COLUMNS if col in kpi_df.columns]
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