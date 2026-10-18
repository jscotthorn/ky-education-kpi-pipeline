"""
Teacher Experience ETL Module

Processes Kentucky teacher experience data showing average years of teaching
experience by school and district. This is an institutional metric. The file
has no demographic column.

KDE's report-card glossary calls the measure Average School Experience: the
total years of teaching experience divided by the number of teachers in the
school. The 2018-19 cover sheet calls the same field "Average number of years'
experience by teachers" and the count "Total number of teachers contributing
to the Total Years of Experience". 703 KAR 5:140 defines average years of
experience as the average professional experience of classroom teachers,
excluding administrators, guidance counselors, and media specialists. The
published average is stored as ``teacher_average_years_experience``. The
educator count is the denominator used to recognize an empty institution. It
is not a separate KPI.

KYRC26 uses the same headers as KYRC24 and KYRC25. No temporary OAA extract
is registered, and the guessed temporary names return 404, so there is no
leftover temporary filename to skip. ``KYRC23_OVW_Average_Years_School_Experience.csv``
returns 200 and republishes 2022-23. It is not registered, and
``accepts_data_file`` skips that name.
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

# KYRC23 is the 2022-23 extract again. The registered historical file already
# covers that year. Measure cells match after district codes are aligned.
_KYRC23_REPUBLISH = "KYRC23_OVW_AVERAGE_YEARS_SCHOOL_EXPERIENCE.CSV"

_COUNT_COLUMNS = ("educator_count", "average_years_experience")


def _is_string_like(series: pd.Series) -> bool:
    """True for object text and for pandas StringDtype.

    ``read_csv(..., dtype=str)`` on pandas 3 returns StringDtype, not object.
    A comma in ``39,242`` has to be removed before ``to_numeric``.
    pandas 2.3.3 still returns object. pandas is unpinned. ``bool(pd.NA)``
    is not used to detect the dtype.
    """
    return series.dtype == object or bool(pd.api.types.is_string_dtype(series))


def _strip_thousands(series: pd.Series) -> pd.Series:
    """Remove thousands separators from a text column.

    ``astype("string")`` keeps missing values as ``<NA>``. ``astype(str)``
    turns them into the literal ``"nan"``.
    """
    if not _is_string_like(series):
        return series
    return series.astype("string").str.replace(",", "", regex=False).str.strip()


class TeacherExperienceETL(BaseETL):
    """ETL module for processing teacher experience data.

    KYRC26 uses the same headers as KYRC24 and KYRC25. The 2020-2023 files
    use the uppercase names, which are already mapped. No temporary OAA
    extract is registered for this directory.
    """

    # Opt in here, in the same change that accepts the file. BaseETL skips
    # every KYRC26_* CSV until this is True.
    include_kyrc26 = True

    def accepts_data_file(self, file_path: Path) -> bool:
        """Skip the KYRC23 republish of the 2022-23 extract.

        ``KYRC23_OVW_Average_Years_School_Experience.csv`` returns 200 on
        the datasets host. Educator counts and averages match
        ``average_years_school_experience_2023.csv`` after the 3-digit
        district codes are written as ``NNN000``. School codes match, so
        loading both files would write each school twice for 2023.
        ``School Year`` on the historical file is ``20222023`` except one
        Breathitt County total typed ``20022023``. KYRC23 corrects that
        cell to ``20222023``. The guessed OAA names are 404 and were never
        registered, so they are not skipped here.
        """
        if file_path.name.upper() == _KYRC23_REPUBLISH:
            logger.info(
                "Skipping %s; it republishes 2022-23 already loaded from "
                "average_years_school_experience_2023.csv",
                file_path.name,
            )
            return False
        return super().accepts_data_file(file_path)

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            'Educator Count': 'educator_count',
            'EDUCATOR COUNT': 'educator_count',
            'Average Years of Experience': 'average_years_experience',
            'AVERAGE YEARS OF EXPERIENCE': 'average_years_experience',
            # Historical xlsx format (2018-19) - SCHOOL_EXPERIENCE
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
            'TOTEXP': 'total_experience',  # Total years * teachers
            'CNTEXP': 'educator_count',  # Count of teachers
            'AVGEXPERIENCEYEARS': 'average_years_experience',
        }

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        metrics = {}

        # Helper function to safely convert to numeric
        def safe_numeric(value):
            if pd.isna(value):
                return pd.NA
            try:
                return pd.to_numeric(value, errors='coerce')
            except (TypeError, ValueError):
                return pd.NA

        # Average years of experience (primary metric).
        # A published 0.00 is omitted by this comparison. Those rows have an
        # educator count of 1 and no star. Whether a published zero should be
        # a KPI row is a pre-existing design question, not suppression.
        avg_years = safe_numeric(row.get('average_years_experience', pd.NA))
        if pd.notna(avg_years) and avg_years > 0:
            metrics['teacher_average_years_experience'] = avg_years

        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get default metrics for suppressed teacher experience records."""
        defaults = {}

        # Only create defaults for metrics that exist in the source data
        if 'average_years_experience' in row.index:
            defaults['teacher_average_years_experience'] = pd.NA

        return defaults

    def _parse_numeric_with_commas(self, value: Any) -> Any:
        """Parse a count that may contain a thousands separator.

        ``pd.NA`` is not used as a boolean. A comma in ``2,984`` is removed
        before ``float``. Numeric values, including numpy integers left by
        an Int64 cast, are returned as floats.
        """
        if value is None:
            return pd.NA
        if isinstance(value, str):
            cleaned = value.replace(',', '').strip()
            if cleaned == '':
                return pd.NA
            try:
                return float(cleaned)
            except ValueError:
                return pd.NA
        try:
            if bool(pd.isna(value)):
                return pd.NA
        except (TypeError, ValueError):
            return pd.NA
        if isinstance(value, bool):
            return pd.NA
        try:
            return float(value)
        except (TypeError, ValueError):
            return pd.NA

    def should_skip_row(self, row: pd.Series) -> bool:
        """Skip rows that don't have teacher experience data.

        An empty educator count or an empty average is not offered. notes/8:
        a row that is not suppressed and has every measure empty emits
        nothing. These files have no Suppressed column. A count of 0 is
        not in the registered files. ``pd.isna`` short-circuits before the
        comparison, so ``pd.NA`` from the Int64 cast is not evaluated as a
        boolean.
        """
        educator_count_raw = row.get('educator_count', pd.NA)
        educator_count = self._parse_numeric_with_commas(educator_count_raw)
        if pd.isna(educator_count) or educator_count == 0:
            return True

        avg_years = row.get('average_years_experience', pd.NA)
        if pd.isna(avg_years):
            return True

        return False  # Don't call super() - institutional data has no demographics

    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Strip thousands separators before the configured Int64 cast.

        ``config/mappings.yaml`` casts ``educator_count`` to Int64 inside
        ``BaseETL.process``, which runs ``to_numeric`` before
        ``should_skip_row``. A comma in ``39,242`` becomes missing at that
        cast, and the state row is then skipped. The strip has to happen
        here, on object and on StringDtype. pandas is unpinned.
        """
        df = super().standardize_missing_values(df)
        for column in _COUNT_COLUMNS:
            if column not in df.columns:
                continue
            df[column] = pd.to_numeric(_strip_thousands(df[column]), errors="coerce")
        return df


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Read teacher experience files, normalize, and convert to KPI format."""
    etl = TeacherExperienceETL('teacher_experience')
    etl.process(raw_dir, proc_dir, cfg)


def main():
    """Run teacher experience ETL process."""
    import logging
    logging.basicConfig(level=logging.INFO)

    from pathlib import Path
    raw_dir = Path(__file__).parent.parent / "data" / "raw"
    proc_dir = Path(__file__).parent.parent / "data" / "processed"
    proc_dir.mkdir(exist_ok=True)

    test_config = Config(
        derive={"processing_date": "2025-11-24", "data_quality_flag": "reviewed"}
    ).model_dump()

    transform(raw_dir, proc_dir, test_config)


if __name__ == "__main__":
    main()
