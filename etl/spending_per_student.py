"""
Spending Per Student ETL Module

Handles Kentucky per-pupil spending data across years 2019-2025.
Normalizes column names, handles schema variations, standardizes missing values,
and processes spending metrics across different funding sources.

Data includes spending metrics for:
- Federal funds (personnel, non-personnel, total)
- State/local funds (personnel, non-personnel, total)
- All funds combined (total)

School rows for 2024 and 2025 come from the wide spending file. District and
state totals for those years come from the five long School / District / State
files. Those totals are stored under the same labels as the 2020-2023 spending
rows: district number as the school id (Fayette ``165``), ``---District Total---``,
and the district name. The state row is ``999``, district ``State``, and
``---State Total---``. There is no demographic breakdown. Every record uses
"All Students".
"""
from pathlib import Path
import pandas as pd
from typing import Dict, Any, Optional
import logging
import sys

# Add etl directory to path for imports
etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from constants import KPI_COLUMNS
from base_etl import BaseETL, Config

logger = logging.getLogger(__name__)

# Source measure columns after the column rename. A comma in ``20,164``
# has to be gone before ``to_numeric``. These are published dollars per
# student. Nothing in this list is calculated from the others.
_MEASURE_COLUMNS = (
    'personnel_spending_federal',
    'non_personnel_spending_federal',
    'total_spending_federal',
    'personnel_spending_state_local',
    'non_personnel_spending_state_local',
    'total_spending_state_local',
    'total_spending_all_funds',
)

_SOURCE_METRICS = (
    ('personnel_spending_federal', 'personnel_spending_per_student_federal'),
    ('non_personnel_spending_federal', 'non_personnel_spending_per_student_federal'),
    ('total_spending_federal', 'total_spending_per_student_federal'),
    ('personnel_spending_state_local', 'personnel_spending_per_student_state_local'),
    ('non_personnel_spending_state_local', 'non_personnel_spending_per_student_state_local'),
    ('total_spending_state_local', 'total_spending_per_student_state_local'),
    ('total_spending_all_funds', 'total_spending_per_student_all_funds'),
)


def _star_column_name(column: str) -> str:
    """Boolean column that remembers a ``*`` after the cell becomes NA."""
    return f'_star_{column}'


def _is_string_like(series: pd.Series) -> bool:
    """True for object text and for pandas StringDtype.

    ``read_csv(..., dtype=str)`` on pandas 3 returns StringDtype, not object.
    A comma in ``20,164`` has to be removed before ``to_numeric``. pandas
    2.3.3 still returns object, so a ``dtype == object`` strip already keeps
    that amount. pandas is unpinned.
    """
    return series.dtype == object or bool(pd.api.types.is_string_dtype(series))


def _is_star_text(value: Any) -> bool:
    """True when the cell text is a privacy star.

    A blank is not a star. ``bool(pd.NA)`` is not used.
    """
    if isinstance(value, str):
        return value.strip() in {'*', '**'}
    return False


def _star_mask(series: pd.Series) -> pd.Series:
    """Boolean mask of ``*`` cells, false for blanks and numbers.

    pandas 3 ``dtype=str`` is StringDtype. pandas 2.3.3 is object. Both go
    through ``astype("string")``. A missing cell is false, not NA, so later
    code never calls ``bool`` on ``pd.NA``.
    """
    if not _is_string_like(series):
        return pd.Series(False, index=series.index)
    stripped = series.astype('string').str.strip()
    return stripped.isin(['*', '**']).fillna(False).astype(bool)


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
        return value.strip() == ''
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return True


def _normalize_numeric_text(series: pd.Series) -> pd.Series:
    """Remove thousands separators.

    ``20,164`` becomes ``20164``. ``astype("string")`` keeps a blank as
    ``<NA>``. ``astype(str)`` would turn it into the literal ``"nan"``.
    A ``*`` is left for ``to_numeric`` to drop after the star flag has
    been recorded. A published 0 is left as 0.
    """
    text = series.astype('string').str.strip()
    text = text.str.replace(',', '', regex=False)
    text = text.str.replace('"', '', regex=False)
    return text


# Long files. The wide KYRC24/KYRC25 spending files are schools only.
# A row named "All Schools" whose code ends in 000 is a district or the
# state. The amount for that row is the District column, or the State /
# Statewide column on district number 999. The School column on those
# rows is 0. That 0 is not the district or state amount.
_LEVEL_FILENAME_MARKERS = (
    "FT_TOTAL_SPENDING_PER_STUDENT_",
    "FT_PERSONNEL_SPENDING_PER_STUDENT_",
)

# Category / Metric text to the wide column the rest of this module already
# maps. The published total is not the sum of the other columns.
_LEVEL_MEASURE_COLUMNS = {
    "Personnel Spending per Student - Federal Funds Personnel":
        "Personnel Spending per Student - Federal Funds",
    "Personnel Spending per Student - Federal Funds Non Personnel":
        "Non-Personnel Spending per Student - Federal Funds",
    "Personnel Expenditures State Local Per Student":
        "Personnel Expenditures State Local Per Student",
    "Non-Personnel Expenditures State Local Per Student":
        "Non-Personnel Expenditures State Local Per Student",
    "Total Expenditures Federal Per Student":
        "Total Expenditures Federal Per Student",
    "Total Spending per Student - State/Local Funds":
        "Total Spending per Student - State/Local Funds",
    "Total Expenditures Per Student":
        "Total Expenditures Per Student",
}

_LEVEL_IDENTITY_COLUMNS = (
    "School Year",
    "County Number",
    "County Name",
    "District Number",
    "District Name",
    "School Number",
    "School Name",
    "School Code",
    "State School Id",
    "NCES ID",
    "CO-OP",
    "CO-OP Code",
    "CO-OP CODE",
    "School Type",
)


def is_level_spending_file(filename: str) -> bool:
    """True for a district/state total file, false for the wide school file."""
    upper = filename.upper()
    return any(marker in upper for marker in _LEVEL_FILENAME_MARKERS)


def _text_series(series: pd.Series) -> pd.Series:
    """Strip cell text. A missing cell becomes an empty string.

    ``astype("string")`` covers object and pandas StringDtype. A published
    0 stays ``"0"``. A ``*`` stays ``"*"``. A comma in ``22,322`` stays.
    """
    text = series.astype("string").str.strip()
    return text.fillna("")


def select_district_state_spending(df: pd.DataFrame) -> pd.DataFrame:
    """Keep published district and state amounts from a long spending file.

    School rows are left out. Their per-pupil amounts are already on the
    wide file, and the District column on a school row repeats the district
    total. A blank level cell is left out. A ``*`` and a published 0 are
    kept for the usual suppressed / zero rules.

    Identifiers match the 2020-2023 spending rows. Fayette stays district
    ``Fayette County``, school ``---District Total---``, school code ``165``.
    The state stays district ``State``, school ``---State Total---``, school
    code ``999``. The 6-digit codes ``165000`` and ``999000`` are not written.
    """
    if df.empty:
        return df
    if "School Name" not in df.columns or "School Code" not in df.columns or "District" not in df.columns:
        logger.error("Level spending file is missing School Name, School Code, or District")
        return pd.DataFrame()
    if "Category" in df.columns:
        label_column = "Category"
    elif "Metric" in df.columns:
        label_column = "Metric"
    else:
        logger.error("Level spending file is missing Category and Metric")
        return pd.DataFrame()
    if "Statewide" in df.columns:
        state_column = "Statewide"
    elif "State" in df.columns:
        state_column = "State"
    else:
        logger.error("Level spending file is missing State and Statewide")
        return pd.DataFrame()

    school_name = _text_series(df["School Name"])
    school_code = _text_series(df["School Code"])
    district_number = _text_series(df["District Number"]) if "District Number" in df.columns else pd.Series("", index=df.index)
    is_aggregate = school_name.eq("All Schools") & school_code.str.endswith("000").fillna(False)
    is_state = (
        district_number.eq("999") | school_code.eq("999000")
    ).fillna(False).astype(bool)
    district_value = _text_series(df["District"])
    state_value = _text_series(df[state_column])
    chosen = state_value.where(is_state, district_value)
    labels = _text_series(df[label_column])
    identity = {
        column: _text_series(df[column])
        for column in _LEVEL_IDENTITY_COLUMNS
        if column in df.columns
    }

    records = []
    unknown_labels = set()
    for idx in df.index[is_aggregate.fillna(False).to_numpy()]:
        label = labels.at[idx]
        measure = _LEVEL_MEASURE_COLUMNS.get(label)
        if measure is None:
            if label not in unknown_labels:
                logger.warning("Unmapped spending category %r", label)
                unknown_labels.add(label)
            continue
        value = chosen.at[idx]
        if value == "":
            continue
        number = district_number.at[idx]
        if number == "":
            logger.warning("District aggregate spending row has no district number")
            continue
        record = {column: text.at[idx] for column, text in identity.items()}
        record["District Number"] = number
        record["School Code"] = number
        if bool(is_state.at[idx]):
            record["District Name"] = "State"
            record["School Name"] = "---State Total---"
        else:
            record["School Name"] = "---District Total---"
        record[measure] = value
        records.append(record)

    if not records:
        return pd.DataFrame()
    # StringDtype keeps a district number such as ``001``. A plain
    # DataFrame would read that as the integer 1.
    result = pd.DataFrame(records)
    for column in result.columns:
        result[column] = result[column].astype("string")
    return result


def clean_spending_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and validate spending values.

    Handles:
    - Comma-separated values (e.g., "20,164" -> 20164.0) on object and StringDtype
    - Missing values, including a privacy star, become NA
    - A published 0 stays 0
    - Negative values (should be very rare, log warnings)
    """
    spending_columns = [
        col for col in df.columns
        if not str(col).startswith('_star_')
        and ('spending' in col.lower() or 'expenditure' in col.lower())
    ]

    for col in spending_columns:
        if _is_string_like(df[col]):
            df[col] = _normalize_numeric_text(df[col])

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

    def _read_data_file(self, file_path: Path, sheet: Optional[str] = None) -> pd.DataFrame:
        """Read a wide school file, or the district and state rows of a long file.

        ``KYRC2{4,5}_FT_Spending_per_Student.csv`` is schools only. The
        Total and Personnel files publish district and state amounts in
        School / District / State columns. Those rows are reshaped onto
        the same measure columns as the wide file before the usual
        comma and suppression handling.
        """
        frame = super()._read_data_file(file_path, sheet)
        if is_level_spending_file(file_path.name):
            return select_district_state_spending(frame)
        return frame

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

        Returns the published spending amounts:
        - Federal: personnel, non-personnel, total
        - State/Local: personnel, non-personnel, total
        - All Funds: total

        A starred source cell is stored as NA. A blank is omitted. A
        published 0 is kept. No metric is calculated from the others.
        """
        metrics = {}

        for column, metric_name in _SOURCE_METRICS:
            if _metric_was_starred(row, column):
                metrics[metric_name] = pd.NA
                continue
            value = row.get(column)
            if not _is_missing(value):
                metrics[metric_name] = value

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
        """Record ``*`` positions, then strip thousands separators.

        ``clean_spending_values`` used to remove commas only when
        ``dtype == object``. pandas 3 ``dtype=str`` is StringDtype, so a
        comma in ``20,164`` survived that check and ``to_numeric`` dropped
        it. The cleaner runs on object and StringDtype. A ``*`` is recorded
        before it becomes NA. A blank is not recorded. A published 0 is
        left as 0.
        """
        star_columns = {
            column: _star_mask(df[column])
            for column in _MEASURE_COLUMNS
            if column in df.columns
        }
        df = super().standardize_missing_values(df)
        df = clean_spending_values(df)
        for column, mask in star_columns.items():
            df[_star_column_name(column)] = mask
        return df

    def convert_to_kpi_format(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """Write a published number as ``N`` and a ``*`` cell as ``Y``.

        This override must track ``BaseETL.convert_to_kpi_format``.

        A blank measure is not a row. A published 0 is kept. A starred
        cell is value NA with ``suppressed`` Y. The other measures on
        that school stay ``N``. No spending amount is calculated.
        """
        kpi_rows = []
        for _, row in df.iterrows():
            if self.should_skip_row(row):
                continue
            kpi_template = self.create_kpi_template(row, source_file)
            if kpi_template is None:
                continue
            metrics = self.extract_metrics(row)
            row_suppressed = kpi_template.get('suppressed') == 'Y'
            if not metrics and row_suppressed:
                metrics = self.get_suppressed_metric_defaults(row)
            for metric_name, value in metrics.items():
                kpi_record = kpi_template.copy()
                kpi_record['metric'] = metric_name
                if row_suppressed or _is_missing(value):
                    kpi_record['value'] = pd.NA
                    kpi_record['suppressed'] = 'Y'
                else:
                    kpi_record['value'] = float(value)
                    kpi_record['suppressed'] = 'N'
                kpi_rows.append(kpi_record)

        if not kpi_rows:
            logger.warning('No valid KPI rows created')
            return pd.DataFrame()
        kpi_df = pd.DataFrame(kpi_rows)
        available_columns = [col for col in KPI_COLUMNS if col in kpi_df.columns]
        return kpi_df[available_columns]


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
