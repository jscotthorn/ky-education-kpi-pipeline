"""
Spending Per Student ETL Module

Handles Kentucky per-pupil spending data across years 2020-2025.
Normalizes column names, handles schema variations, standardizes missing values,
and processes spending metrics across different funding sources.

Data includes spending metrics for:
- Federal funds (personnel, non-personnel, total)
- State/local funds (personnel, non-personnel, total)
- All funds combined (total)

Note: This dataset contains school-level spending only, with no demographic breakdowns.
All records are assigned "All Students" as the student group for consistency with KPI format.
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


def clean_spending_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and validate spending values.

    Handles:
    - Comma-separated values (e.g., "9,827" -> 9827.0)
    - Missing/suppressed values
    - Negative values (should be very rare, log warnings)
    """
    spending_columns = [col for col in df.columns if 'spending' in col.lower() or 'expenditure' in col.lower()]

    for col in spending_columns:
        if col in df.columns:
            # Remove commas from string values
            if df[col].dtype == 'object':
                df[col] = df[col].str.replace(',', '', regex=False)

            # Convert to numeric
            df[col] = pd.to_numeric(df[col], errors='coerce')

            # Validate: spending should be non-negative
            negative_mask = df[col] < 0
            if negative_mask.any():
                logger.warning(f"Found {negative_mask.sum()} negative spending values in {col}")
                df.loc[negative_mask, col] = pd.NA

    return df


class SpendingPerStudentETL(BaseETL):
    """ETL module for processing per-pupil spending data."""

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        """
        Map spending-specific columns to standardized names.

        Handles column name variations across years:
        - 2020: "Personnel Spending per Student - State/Local Funds"
        - 2024+: "Personnel Expenditures State Local Per Student"
        """
        return {
            # Federal funding - Personnel
            'Personnel Spending per Student - Federal Funds': 'personnel_spending_federal',
            'PERSONNEL SPENDING PER STUDENT - FEDERAL FUNDS': 'personnel_spending_federal',

            # Federal funding - Non-personnel
            'Non-Personnel Spending per Student - Federal Funds': 'non_personnel_spending_federal',
            'NON-PERSONNEL SPENDING PER STUDENT - FEDERAL FUNDS': 'non_personnel_spending_federal',

            # Federal funding - Total
            'Total Spending per Student - Federal Funds': 'total_spending_federal',
            'TOTAL SPENDING PER STUDENT - FEDERAL FUNDS': 'total_spending_federal',
            'Total Expenditures Federal Per Student': 'total_spending_federal',
            'TOTAL EXPENDITURES FEDERAL PER STUDENT': 'total_spending_federal',

            # State/Local funding - Personnel
            'Personnel Spending per Student - State/Local Funds': 'personnel_spending_state_local',
            'PERSONNEL SPENDING PER STUDENT - STATE/LOCAL FUNDS': 'personnel_spending_state_local',
            'Personnel Expenditures State Local Per Student': 'personnel_spending_state_local',
            'PERSONNEL EXPENDITURES STATE LOCAL PER STUDENT': 'personnel_spending_state_local',

            # State/Local funding - Non-personnel
            'Non-Personnel Spending per Student - State/Local Funds': 'non_personnel_spending_state_local',
            'NON-PERSONNEL SPENDING PER STUDENT - STATE/LOCAL FUNDS': 'non_personnel_spending_state_local',
            'Non-Personnel Expenditures State Local Per Student': 'non_personnel_spending_state_local',
            'NON-PERSONNEL EXPENDITURES STATE LOCAL PER STUDENT': 'non_personnel_spending_state_local',

            # State/Local funding - Total
            'Total Spending per Student - State/Local Funds': 'total_spending_state_local',
            'TOTAL SPENDING PER STUDENT - STATE/LOCAL FUNDS': 'total_spending_state_local',

            # All funds - Total
            'Total Spending per Student - All Fund Sources': 'total_spending_all_funds',
            'TOTAL SPENDING PER STUDENT - ALL FUND SOURCES': 'total_spending_all_funds',
            'Total Expenditures Per Student': 'total_spending_all_funds',
            'TOTAL EXPENDITURES PER STUDENT': 'total_spending_all_funds',

            # Additional fields (2020 only)
            'MEMBERSHIP': 'membership',
            'Membership': 'membership',

            # Historical xlsx format (2018-19)
            # Note: 2018-19 file lacks SCH_CD, so use STATE_SCH_ID as school_code
            'SCH_YEAR': 'school_year',
            'CNTYNO': 'county_number',
            'CNTYNAME': 'county_name',
            'DIST_NUMBER': 'district_number',
            'DIST_NAME': 'district_name',
            'SCH_NUMBER': 'school_number',
            'SCH_NAME': 'school_name',
            'STATE_SCH_ID': 'school_code',  # Map to school_code for consistency
            'NCESID': 'nces_id',
            'COOP': 'co_op',
            'COOP_CODE': 'co_op_code',
            'PERSON_PER_STU_FED': 'personnel_spending_federal',
            'NONPERSON_PER_STU_FED': 'non_personnel_spending_federal',
            'TOTAL_PER_STU_FED': 'total_spending_federal',
            'PERSON_PER_STU_STATELOCAL': 'personnel_spending_state_local',
            'NONPERSON_PER_STU_STATELOCAL': 'non_personnel_spending_state_local',
            'TOTAL_PER_STU_STATELOCAL': 'total_spending_state_local',
            'TOTAL_PER_STU_ALLFUNDS': 'total_spending_all_funds',
        }

    def should_skip_row(self, row: pd.Series) -> bool:
        """
        Override to NOT skip rows without demographics.

        Spending data has no demographic dimension, so we process all rows
        and assign "All Students" as the student group.

        We still skip rows that have invalid/missing school identifiers.
        """
        # Don't skip based on demographics (this data has none)
        # Only skip if we can't identify the school
        school_code = row.get('school_code', '')
        if pd.isna(school_code) or school_code == '':
            return True

        return False

    def create_kpi_template(self, row: pd.Series, source_file: str) -> Dict[str, Any]:
        """
        Override to assign "All Students" demographic for all records.

        Spending data has no demographic breakdowns, so all records represent
        spending for all students at the school level.
        """
        # Get base template from parent
        template = super().create_kpi_template(row, source_file)

        # Override student_group to always be "All Students"
        template['student_group'] = 'All Students'

        return template

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        """
        Extract spending metrics from a data row.

        Returns all 7 spending metrics:
        - Federal: personnel, non-personnel, total
        - State/Local: personnel, non-personnel, total
        - All Funds: total
        """
        metrics = {}

        # Federal funding
        if pd.notna(row.get('personnel_spending_federal')):
            metrics['personnel_spending_per_student_federal'] = row['personnel_spending_federal']

        if pd.notna(row.get('non_personnel_spending_federal')):
            metrics['non_personnel_spending_per_student_federal'] = row['non_personnel_spending_federal']

        if pd.notna(row.get('total_spending_federal')):
            metrics['total_spending_per_student_federal'] = row['total_spending_federal']

        # State/Local funding
        if pd.notna(row.get('personnel_spending_state_local')):
            metrics['personnel_spending_per_student_state_local'] = row['personnel_spending_state_local']

        if pd.notna(row.get('non_personnel_spending_state_local')):
            metrics['non_personnel_spending_per_student_state_local'] = row['non_personnel_spending_state_local']

        if pd.notna(row.get('total_spending_state_local')):
            metrics['total_spending_per_student_state_local'] = row['total_spending_state_local']

        # All funds
        if pd.notna(row.get('total_spending_all_funds')):
            metrics['total_spending_per_student_all_funds'] = row['total_spending_all_funds']

        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """
        Get default metrics for suppressed spending records.

        For spending data, suppressed records should include NA values for all metrics.
        """
        return {
            'personnel_spending_per_student_federal': pd.NA,
            'non_personnel_spending_per_student_federal': pd.NA,
            'total_spending_per_student_federal': pd.NA,
            'personnel_spending_per_student_state_local': pd.NA,
            'non_personnel_spending_per_student_state_local': pd.NA,
            'total_spending_per_student_state_local': pd.NA,
            'total_spending_per_student_all_funds': pd.NA,
        }

    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Override to include spending-specific data cleaning."""
        # Apply base missing value standardization
        df = super().standardize_missing_values(df)

        # Apply spending-specific cleaning
        df = clean_spending_values(df)

        return df


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Read spending per student files, normalize, and convert to KPI format using BaseETL."""
    etl = SpendingPerStudentETL('spending_per_student')
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
