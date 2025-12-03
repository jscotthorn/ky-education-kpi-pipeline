"""
Financial Summary ETL Module

Processes Kentucky district-level financial summary data from 2020-2025.
Extracts KPIs for student membership, fund balance, staff FTE counts, and staff ratios.

Data includes:
- End-of-Year Student Membership (district totals)
- Fund Balance (absolute and percentage)
- Certified Staff FTE (total and teachers only)
- Classified Staff FTE
- Students per Certified Staff ratio
- Students per Classified Staff ratio
- Students per Non-Teacher Certified Staff ratio

Note: This dataset contains district-level aggregates only, with no demographic breakdowns
or school-level data. All records are assigned "All Students" as the student group.
"""
from pathlib import Path
import pandas as pd
from typing import Dict, Any
import logging
import sys

# Add etl directory to path for imports
etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from constants import KPI_COLUMNS
from base_etl import BaseETL, Config

logger = logging.getLogger(__name__)


def clean_numeric_values(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """
    Clean and validate numeric values with comma formatting.

    Handles:
    - Comma-separated values (e.g., "636,427" -> 636427.0)
    - Missing/suppressed values
    - Negative values (log warnings for fund balance which can be negative)
    """
    for col in columns:
        if col in df.columns:
            # Remove commas from string values
            if df[col].dtype == 'object':
                df[col] = df[col].str.replace(',', '', regex=False)

            # Convert to numeric
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


class FinancialSummaryETL(BaseETL):
    """ETL module for processing district financial summary data."""

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        """
        Map financial summary columns to standardized names.

        Handles column name variations across years:
        - KYRC24: Title case (e.g., "End-of-Year Student Membership")
        - 2020-2023: Uppercase (e.g., "MEMBERSHIP")
        - 2018-19: FINANCE xlsx format
        """
        return {
            # Student Membership
            'End-of-Year Student Membership': 'membership',
            'MEMBERSHIP': 'membership',
            'Membership': 'membership',

            # Fund Balance
            'Fund Balance': 'fund_balance',
            'FUND BALANCE': 'fund_balance',

            # Fund Balance Percentage
            'Fund Balance %': 'fund_balance_pct',
            'FUND BALANCE %': 'fund_balance_pct',

            # Certified Staff (Total)
            'Certified Staff': 'certified_staff',
            'FTE CERTIFIED STAFF': 'certified_staff',
            'FTE Certified Staff': 'certified_staff',

            # Certified Staff - Teachers Only
            'Certified Staff Teachers': 'certified_staff_teachers',
            'FTE CERTIFIED STAFF - TEACHERS': 'certified_staff_teachers',
            'FTE Certified Staff - Teachers': 'certified_staff_teachers',

            # Classified Staff
            'Classified Staff': 'classified_staff',
            'FTE CLASSIFIED STAFF': 'classified_staff',
            'FTE Classified Staff': 'classified_staff',

            # Historical xlsx format (2018-19) - FINANCE
            # Note: 2018-19 is district-level only, so DIST_NUMBER becomes school_code
            'SCH_YEAR': 'school_year',
            'CNTYNO': 'county_number',
            'CNTYNAME': 'county_name',
            'DIST_NUMBER': 'school_code',  # District-level data uses district number as identifier
            'DIST_NAME': 'district_name',
            'SCH_NUMBER': 'school_number',
            'SCH_NAME': 'school_name',
            'STATE_SCH_ID': 'state_school_id',
            'NCESID': 'nces_id',
            'COOP': 'co_op',
            'COOP_CODE': 'co_op_code',
            'GENERALFUNDBALANCE': 'fund_balance',
            'GENERALFUNDBALANCE_PCT': 'fund_balance_pct',
            'FTE_CERTIFIEDSTAFF': 'certified_staff',
            'FTE_CERTIFIEDSTAFF_TEACHERS': 'certified_staff_teachers',
            'FTE_CLASSIFIEDSTAFF': 'classified_staff',
        }

    def should_skip_row(self, row: pd.Series) -> bool:
        """
        Override to NOT skip rows without demographics.

        Financial summary data has no demographic dimension, so we process all rows
        and assign "All Students" as the student group.

        We skip:
        - Rows that have invalid/missing school identifiers
        - State total rows (school_code == '999' or '999000')
        """
        school_code = row.get('school_code', '')
        if pd.isna(school_code) or school_code == '':
            return True

        # Skip state-level aggregates
        school_code_str = str(school_code).strip()
        if school_code_str in ['999', '999000']:
            return True

        return False

    def create_kpi_template(self, row: pd.Series, source_file: str) -> Dict[str, Any]:
        """
        Override to assign "All Students" demographic for all records.

        Financial summary data has no demographic breakdowns, so all records represent
        district-level totals for all students.
        """
        # Get base template from parent
        template = super().create_kpi_template(row, source_file)

        # Override student_group to always be "All Students"
        template['student_group'] = 'All Students'

        return template

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        """
        Extract all financial summary metrics from a data row.

        Returns metrics for:
        - Student membership
        - Fund balance (absolute and percentage)
        - Certified staff FTE (total and teachers)
        - Classified staff FTE
        - Derived: non-teacher certified staff
        """
        metrics = {}

        # Student Membership
        if pd.notna(row.get('membership')):
            metrics['eoy_student_membership'] = row['membership']

        # Fund Balance
        if pd.notna(row.get('fund_balance')):
            metrics['fund_balance'] = row['fund_balance']

        if pd.notna(row.get('fund_balance_pct')):
            metrics['fund_balance_pct'] = row['fund_balance_pct']

        # Certified Staff
        certified_total = row.get('certified_staff')
        certified_teachers = row.get('certified_staff_teachers')

        if pd.notna(certified_total):
            metrics['certified_staff_fte'] = certified_total

        if pd.notna(certified_teachers):
            metrics['certified_staff_teachers_fte'] = certified_teachers

        # Derived: Non-teacher certified staff (administrators, counselors, etc.)
        if pd.notna(certified_total) and pd.notna(certified_teachers):
            try:
                non_teacher = float(certified_total) - float(certified_teachers)
                if non_teacher >= 0:
                    metrics['certified_staff_non_teachers_fte'] = non_teacher
            except (ValueError, TypeError):
                pass

        # Classified Staff
        if pd.notna(row.get('classified_staff')):
            metrics['classified_staff_fte'] = row['classified_staff']

        # Derived: Total Staff
        if pd.notna(certified_total) and pd.notna(row.get('classified_staff')):
            try:
                total_staff = float(certified_total) + float(row['classified_staff'])
                metrics['total_staff_fte'] = total_staff
            except (ValueError, TypeError):
                pass

        # Staff Ratio Calculations (students per staff)
        membership = row.get('membership')
        if pd.notna(membership) and float(membership) > 0:
            membership_val = float(membership)

            # Students per Certified Staff
            if pd.notna(certified_total) and float(certified_total) > 0:
                try:
                    metrics['students_per_certified_staff'] = round(
                        membership_val / float(certified_total), 2
                    )
                except (ValueError, TypeError, ZeroDivisionError):
                    pass

            # Students per Classified Staff
            if pd.notna(row.get('classified_staff')) and float(row['classified_staff']) > 0:
                try:
                    metrics['students_per_classified_staff'] = round(
                        membership_val / float(row['classified_staff']), 2
                    )
                except (ValueError, TypeError, ZeroDivisionError):
                    pass

            # Students per Non-Teacher Certified Staff
            if pd.notna(certified_total) and pd.notna(certified_teachers):
                try:
                    non_teacher = float(certified_total) - float(certified_teachers)
                    if non_teacher > 0:
                        metrics['students_per_non_teacher_certified_staff'] = round(
                            membership_val / non_teacher, 2
                        )
                except (ValueError, TypeError, ZeroDivisionError):
                    pass

        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """
        Get default metrics for suppressed financial summary records.

        For financial data, suppressed records should include NA values for all metrics.
        """
        return {
            'eoy_student_membership': pd.NA,
            'fund_balance': pd.NA,
            'fund_balance_pct': pd.NA,
            'certified_staff_fte': pd.NA,
            'certified_staff_teachers_fte': pd.NA,
            'certified_staff_non_teachers_fte': pd.NA,
            'classified_staff_fte': pd.NA,
            'total_staff_fte': pd.NA,
            'students_per_certified_staff': pd.NA,
            'students_per_classified_staff': pd.NA,
            'students_per_non_teacher_certified_staff': pd.NA,
        }

    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Override to include financial-specific data cleaning."""
        # Apply base missing value standardization
        df = super().standardize_missing_values(df)

        # Apply numeric cleaning for columns with comma formatting
        numeric_columns = [
            'membership', 'fund_balance', 'fund_balance_pct',
            'certified_staff', 'certified_staff_teachers', 'classified_staff'
        ]
        df = clean_numeric_values(df, numeric_columns)

        return df


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Read financial summary files, normalize, and convert to KPI format using BaseETL."""
    etl = FinancialSummaryETL('financial_summary')
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
