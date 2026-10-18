"""
Teacher Turnover ETL Module

Processes Kentucky teacher turnover. The 2018-19 workbook cover sheet
defines the measure as the percentage of teacher turnover experienced by
the school or district. The turnover count is the number of teaching
positions which experienced turnover. The teacher count is the total
number of teachers.

The file is one row per school or district total. There is no demographic
column and no Title I split, so there is no within-group rate and no share
of all students. ``student_group`` is ``All Students``. This module does
not compute a gap.

The stored rate is the published Turnover Percent. It is not rebuilt from
the counts. In 2021, 14 schools publish 100 where the turnover count is
larger than the teacher count. Other registered years publish percents
above 100, including 1,700 at Brooklawn in 2022. A published 0 is kept.
A blank or ``N/A`` percent is missing. No registered year has a blank
measure cell. The 2023 file has 57 Kentucky Tech System rows where the
count, the turnover count, and the percent are all ``N/A``. The count is
not zero, so those rows stay missing.

KYRC26 adds Instructional Staff Count, Instructional Staff Turnover Count,
and Instructional Staff Turnover Percent. Those columns are a different
measure. State instructional-staff turnover is 11.7 against teacher
turnover 10.5. They are not required and are not emitted.

``KYRC23_OVW_Teacher_Turnover.csv`` returns 200 and republishes 2022-23.
It turns the 57 ``N/A`` rows into 0. It is not registered, and
``accepts_data_file`` skips that name. ``teacher_turnover_2020.csv`` is
the 2019-20 report card and is registered. No temporary OAA name was
registered, and the guessed temporary names return 404.
"""
from pathlib import Path
import pandas as pd
from typing import Any, Dict
import logging
import sys

# Add etl directory to path for imports
etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from base_etl import BaseETL, Config

logger = logging.getLogger(__name__)

# KYRC23 is the 2022-23 extract again. School year is 20222023 on every row.
# It stores the 57 historical N/A Kentucky Tech rows as 0.
_KYRC23_REPUBLISH = "KYRC23_OVW_TEACHER_TURNOVER.CSV"

# Text that is not a count or a percent. A published 0 is not in this set.
_NOT_A_NUMBER = {"", "N/A", "NA", "n/a", "#N/A", "*", "**", "---", "--", "<10"}

_MEASURE_COLUMNS = (
    "teacher_count",
    "teacher_turnover_count",
    "turnover_percent",
)


def _is_string_like(series: pd.Series) -> bool:
    """True for object text and for pandas StringDtype.

    ``read_csv(..., dtype=str)`` on pandas 3 returns StringDtype, not object.
    A comma in ``40,557`` has to be removed before ``to_numeric`` or an
    Int64 cast. pandas 2.3.3 still returns object. pandas is unpinned.
    """
    return series.dtype == object or bool(pd.api.types.is_string_dtype(series))


def _is_missing(value: Any) -> bool:
    """True for None and for pandas missing values.

    ``bool(pd.NA)`` raises. ``pd.isna`` on a scalar returns a real bool.
    A blank string is missing. The text ``N/A`` is handled by the parser.
    """
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return True


def _parse_number(value: Any) -> Any:
    """Parse a count or a percent that may be text.

    ``N/A`` and a blank are missing. These files publish a real 0, and a
    blank percent is not turned into 0. No registered year has a blank
    measure cell. A comma is removed so pandas 3 StringDtype does not drop
    ``40,557``. ``bool(pd.NA)`` is not used.
    """
    if isinstance(value, str):
        cleaned = value.replace(",", "").replace("%", "").strip()
        if cleaned in _NOT_A_NUMBER or cleaned.upper() in {"N/A", "#N/A", "NA"}:
            return pd.NA
        try:
            return float(cleaned)
        except ValueError:
            return pd.NA
    if isinstance(value, bool):
        return pd.NA
    if _is_missing(value):
        return pd.NA
    try:
        return float(value)
    except (TypeError, ValueError):
        return pd.NA


def _clean_measure_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Strip commas on the measure columns before a numeric cast.

    ``process`` casts ``config/mappings.yaml`` dtypes after
    ``standardize_missing_values``. This source has no dtype cast. The
    cleaner still runs so an Int64 cast of ``40,557`` does not become
    missing. Object and StringDtype are both cleaned. A published 0 is
    left as 0. ``N/A`` becomes missing.
    """
    for col in _MEASURE_COLUMNS:
        if col not in df.columns:
            continue
        series = df[col]
        if not _is_string_like(series):
            continue
        cleaned = series.astype("string").str.replace(",", "", regex=False).str.strip()
        missing = cleaned.isna() | cleaned.isin(list(_NOT_A_NUMBER))
        df[col] = cleaned.mask(missing, pd.NA)
    return df


class TeacherTurnoverETL(BaseETL):
    """ETL module for processing teacher turnover data.

    KYRC26 uses the KYRC25 turnover headers and adds three instructional
    staff columns. Those columns are not mapped. The 2020-2023 files use
    the uppercase names, which are already mapped. No temporary OAA
    extract is registered for this directory.
    """

    # Opt in here, in the same change that accepts the file. BaseETL skips
    # every KYRC26_* CSV until this is True.
    include_kyrc26 = True

    def accepts_data_file(self, file_path: Path) -> bool:
        """Skip the KYRC23 republish of the 2022-23 extract.

        ``KYRC23_OVW_Teacher_Turnover.csv`` returns 200 on the datasets
        host. School year is ``20222023`` on every row. After the district
        codes are padded to six digits, the school-code set matches
        ``teacher_turnover_2023.csv``. The historical file stores 57
        Kentucky Tech System rows as ``N/A``. KYRC23 stores those rows as
        0. Loading both files would write 2023 twice, and the second copy
        would turn missing rows into zeros. The guessed OAA names are 404
        and were never registered, so they are not skipped here.

        ``TEACHER_TURNOVER_17-18.xlsx`` returns 200 and is school year
        ``20172018``. It is earlier than the registered 2018-19 start, not
        a missing year between registered files, so it is not skipped and
        not registered.
        """
        if file_path.name.upper() == _KYRC23_REPUBLISH:
            logger.info(
                "Skipping %s; it republishes 2022-23 already loaded from "
                "teacher_turnover_2023.csv",
                file_path.name,
            )
            return False
        return super().accepts_data_file(file_path)

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        """
        Map teacher turnover columns to standardized names.

        Handles column name variations across years:
        - 2021-2023: Uppercase column names
        - 2024+: Title case column names
        """
        return {
            # Teacher count
            'Teacher Count': 'teacher_count',
            'TEACHER COUNT': 'teacher_count',

            # Turnover count
            'Teacher Turnover Count': 'teacher_turnover_count',
            'TEACHER TURNOVER COUNT': 'teacher_turnover_count',

            # Turnover percentage
            'Turnover Percent': 'turnover_percent',
            'TURNOVER PERCENT': 'turnover_percent',

            # Historical xlsx format (2018-19)
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
            'TCH_TURNOVER_CNT': 'teacher_turnover_count',
            'TCH_TOTAL': 'teacher_count',
            'TURNOVER_PCT': 'turnover_percent',
        }

    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Replace missing tokens, then strip commas on the measure columns.

        The base method only rewrites ``dtype == object``. pandas 3
        ``dtype=str`` is StringDtype, so the comma strip here is
        dtype-agnostic. ``BaseETL.standardize_missing_values`` is not
        changed.
        """
        df = super().standardize_missing_values(df)
        return _clean_measure_columns(df)

    def should_skip_row(self, row: pd.Series) -> bool:
        """
        Override to NOT skip rows without demographics.

        Teacher turnover data has no demographic dimension, so we process all rows
        and assign "All Students" as the student group.

        We still skip rows that have invalid/missing school identifiers.
        A row with a school code and an ``N/A`` percent is not skipped here.
        ``extract_metrics`` emits nothing for that missing value. A published
        0 is not skipped.
        """
        # Don't skip based on demographics (this data has none)
        # Only skip if we can't identify the school
        school_code = row.get('school_code', '')
        if _is_missing(school_code) or school_code == '':
            return True

        return False

    def create_kpi_template(self, row: pd.Series, source_file: str) -> Dict[str, Any]:
        """
        Override to assign "All Students" demographic for all records.

        Teacher turnover data has no demographic breakdowns, so all records represent
        data for all students at the school level.
        """
        # Get base template from parent
        template = super().create_kpi_template(row, source_file)

        # Override student_group to always be "All Students"
        template['student_group'] = 'All Students'

        return template

    def _metric_value(self, value: Any) -> Any:
        """Return a non-negative number, or missing.

        Zero is kept. A negative is excluded. The published percent is not
        capped at 100.
        """
        number = _parse_number(value)
        if _is_missing(number):
            return pd.NA
        if number < 0:
            return pd.NA
        return number

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        """
        Extract teacher turnover metrics from a data row.

        Returns metrics:
        - teacher_turnover_rate: Published turnover percent
        - teacher_turnover_count: Number of teaching positions that turned over
        - teacher_count: Total teacher count (denominator)

        The rate is the published percent. It is not
        ``turnover_count / teacher_count``. Instructional-staff columns are
        not read.
        """
        metrics: Dict[str, Any] = {}

        turnover_percent = self._metric_value(row.get('turnover_percent', pd.NA))
        if not _is_missing(turnover_percent):
            metrics['teacher_turnover_rate'] = turnover_percent

        turnover_count = self._metric_value(row.get('teacher_turnover_count', pd.NA))
        if not _is_missing(turnover_count):
            metrics['teacher_turnover_count'] = turnover_count

        teacher_count = self._metric_value(row.get('teacher_count', pd.NA))
        if not _is_missing(teacher_count):
            metrics['teacher_count'] = teacher_count

        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get default metrics for suppressed teacher turnover records."""
        defaults = {}

        # Only create defaults for metrics that exist in the source data
        if 'turnover_percent' in row.index:
            defaults['teacher_turnover_rate'] = pd.NA
        if 'teacher_turnover_count' in row.index:
            defaults['teacher_turnover_count'] = pd.NA
        if 'teacher_count' in row.index:
            defaults['teacher_count'] = pd.NA

        return defaults


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Read teacher turnover files, normalize, and convert to KPI format."""
    etl = TeacherTurnoverETL('teacher_turnover')
    etl.process(raw_dir, proc_dir, cfg)


def main():
    """Run teacher turnover ETL process."""
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
