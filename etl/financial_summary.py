"""
Financial Summary ETL Module

Processes Kentucky district-level financial summary data from 2019-2025.
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
from typing import Any, Dict, List
import logging
import sys

# Add etl directory to path for imports
etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from constants import KPI_COLUMNS
from base_etl import BaseETL, Config

logger = logging.getLogger(__name__)

# Source measure columns after the column rename. A comma in ``41,013`` or
# ``(1,344,948)`` has to be gone before ``to_numeric``. The derived staffing
# ratios are not in this list. They are still membership divided by staff.
_MEASURE_COLUMNS = (
    "membership",
    "fund_balance",
    "fund_balance_pct",
    "certified_staff",
    "certified_staff_teachers",
    "classified_staff",
)

_SOURCE_METRICS = (
    ("membership", "eoy_student_membership"),
    ("fund_balance", "fund_balance"),
    ("fund_balance_pct", "fund_balance_pct"),
    ("certified_staff", "certified_staff_fte"),
    ("certified_staff_teachers", "certified_staff_teachers_fte"),
    ("classified_staff", "classified_staff_fte"),
)


def _star_column_name(column: str) -> str:
    """Boolean column that remembers a ``*`` after the cell becomes NA."""
    return f"_star_{column}"


def _is_string_like(series: pd.Series) -> bool:
    """True for object text and for pandas StringDtype.

    ``read_csv(..., dtype=str)`` on pandas 3 returns StringDtype, not object.
    A comma in ``41,013`` has to be removed before ``to_numeric``. pandas
    2.3.3 still returns object, so a ``dtype == object`` strip already keeps
    that count. pandas is unpinned.
    """
    return series.dtype == object or bool(pd.api.types.is_string_dtype(series))


def _is_star_text(value: Any) -> bool:
    """True when the cell text is a privacy star.

    A blank is not a star. ``bool(pd.NA)`` is not used.
    """
    if isinstance(value, str):
        return value.strip() in {"*", "**"}
    return False


def _star_mask(series: pd.Series) -> pd.Series:
    """Boolean mask of ``*`` cells, false for blanks and numbers.

    pandas 3 ``dtype=str`` is StringDtype. pandas 2.3.3 is object. Both go
    through ``astype("string")``. A missing cell is false, not NA, so later
    code never calls ``bool`` on ``pd.NA``.
    """
    if not _is_string_like(series):
        return pd.Series(False, index=series.index)
    stripped = series.astype("string").str.strip()
    return stripped.isin(["*", "**"]).fillna(False).astype(bool)


def _flag_is_set(value: Any) -> bool:
    """True for a recorded star flag. Missing and false stay false."""
    try:
        if bool(pd.isna(value)):
            return False
    except (TypeError, ValueError):
        return False
    if isinstance(value, str):
        return False
    return bool(value)


def _metric_was_starred(row: pd.Series, column: str) -> bool:
    """True when this measure cell was a ``*``.

    ``standardize_missing_values`` records the position before it turns the
    star into NA. A caller that still has the raw ``*`` is recognized from
    the cell text. A blank is not a star.
    """
    flag_name = _star_column_name(column)
    if flag_name in row.index:
        return _flag_is_set(row.get(flag_name))
    return _is_star_text(row.get(column))


def _is_missing(value: Any) -> bool:
    """True for None and for pandas missing values.

    ``bool(pd.NA)`` raises. A blank string is missing. A published 0 is not.
    """
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return True


def _normalize_numeric_text(series: pd.Series) -> pd.Series:
    """Remove thousands separators and accounting parentheses.

    ``41,013`` becomes ``41013``. ``(1,344,948)`` is the published negative
    fund balance and becomes ``-1344948``. ``astype("string")`` keeps a
    blank as ``<NA>``. ``astype(str)`` would turn it into the literal
    ``"nan"``. A ``*`` is left for ``to_numeric`` to drop after the star
    flag has been recorded.
    """
    text = series.astype("string").str.strip()
    text = text.str.replace(",", "", regex=False)
    text = text.str.replace('"', "", regex=False)
    return text.str.replace(r"^\((.*)\)$", r"-\1", regex=True)


def clean_numeric_values(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """
    Clean and validate numeric values with comma formatting.

    Handles:
    - Comma-separated values (e.g., "636,427" -> 636427.0) on object and StringDtype
    - Accounting parentheses (e.g., "(58,197)" -> -58197.0), a published negative fund balance
    - Missing values, including a privacy star, become NA
    - A published 0 stays 0
    """
    for col in columns:
        if col not in df.columns:
            continue
        if _is_string_like(df[col]):
            df[col] = _normalize_numeric_text(df[col])
        df[col] = pd.to_numeric(df[col], errors="coerce")

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

        # A starred source cell is stored as NA. A blank is omitted. A
        # published 0 is kept. Derived ratios below still use the numeric
        # cells, so a star or a blank does not invent a ratio.
        for column, metric_name in _SOURCE_METRICS:
            if _metric_was_starred(row, column):
                metrics[metric_name] = pd.NA
                continue
            value = row.get(column)
            if not _is_missing(value):
                metrics[metric_name] = value

        # Certified Staff
        certified_total = row.get('certified_staff')
        certified_teachers = row.get('certified_staff_teachers')

        # Derived: Non-teacher certified staff (administrators, counselors, etc.)
        if pd.notna(certified_total) and pd.notna(certified_teachers):
            try:
                non_teacher = float(certified_total) - float(certified_teachers)
                if non_teacher >= 0:
                    metrics['certified_staff_non_teachers_fte'] = non_teacher
            except (ValueError, TypeError):
                pass

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
        """Record ``*`` positions, then strip commas and accounting parentheses.

        The base method only rewrites ``dtype == object``. pandas 3
        ``dtype=str`` is StringDtype, so a comma in ``41,013`` survived
        that check and ``to_numeric`` dropped it. The cleaner here runs
        on object and StringDtype. ``BaseETL.standardize_missing_values``
        is not changed. A ``*`` is recorded before it becomes NA. A blank
        is not recorded. A published 0 is left as 0. ``(58,197)`` is a
        published negative fund balance.
        """
        star_columns = {
            column: _star_mask(df[column])
            for column in _MEASURE_COLUMNS
            if column in df.columns
        }
        df = super().standardize_missing_values(df)
        df = clean_numeric_values(df, list(_MEASURE_COLUMNS))
        for column, mask in star_columns.items():
            df[_star_column_name(column)] = mask
        return df

    def convert_to_kpi_format(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """Write a published number as ``N`` and a ``*`` cell as ``Y``.

        This override must track ``BaseETL.convert_to_kpi_format``.

        A blank measure is not a row. A published 0 is kept. A starred
        cell is value NA with ``suppressed`` Y. The other measures on
        that district stay ``N``. Derived ratios are omitted when an
        input is a star or a blank, because KDE did not publish a ratio
        to suppress. The staffing-ratio formulas are unchanged.
        """
        kpi_rows: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            if self.should_skip_row(row):
                continue
            kpi_template = self.create_kpi_template(row, source_file)
            if kpi_template is None:
                continue
            metrics = self.extract_metrics(row)
            row_suppressed = kpi_template.get("suppressed") == "Y"
            if not metrics and row_suppressed:
                metrics = self.get_suppressed_metric_defaults(row)
            for metric_name, value in metrics.items():
                kpi_record = kpi_template.copy()
                kpi_record["metric"] = metric_name
                if row_suppressed or _is_missing(value):
                    kpi_record["value"] = pd.NA
                    kpi_record["suppressed"] = "Y"
                else:
                    kpi_record["value"] = float(value)
                    kpi_record["suppressed"] = "N"
                kpi_rows.append(kpi_record)

        if not kpi_rows:
            logger.warning("No valid KPI rows created")
            return pd.DataFrame()
        kpi_df = pd.DataFrame(kpi_rows)
        available_columns = [col for col in KPI_COLUMNS if col in kpi_df.columns]
        return kpi_df[available_columns]


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
