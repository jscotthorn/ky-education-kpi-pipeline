"""
Teacher Certification ETL Module

Processes Kentucky teacher certification. KDE's report-card glossary
defines Teachers with Emergency/Provisional Credentials as the percentage
of teachers granted an emergency or provisional certificate under
16 KAR 2:120 and 16 KAR 2:010. The count is the number of those teachers.
National Board certification is the number of teachers certified by the
National Board for Professional Teaching Standards, as reported by EPSB.
The file is one row per school or district total. There is no demographic
column, so ``student_group`` is ``All Students``. This module does not
compute a gap. The group columns are not shares and not within-group rates.

The stored rate is the published percent. It is not rebuilt from the
counts. A published 0 is kept. A 0% with a teacher count of 0 is a
placeholder, not a real zero, and it is still stored. A blank percent is
not turned into 0. A blank count is not a zero: the count column has to
be 0 before a 0% is read as a measured zero. In 2021, Virtual Academy
``042020`` publishes teacher count 0, emergency count 2, and percent 0.
That 0% is the placeholder. Adair County Middle ``001014`` publishes
40 / 0 / 0, and that 0% is a real zero.

KYRC26 emergency/provisional certification is not a continuation of the
2020-2025 series. The state cell is 14.3. That is not the next point
after the 2025 state percent of 1.9. National Board certification does
not make that jump. The state National Board percent is 2.7 after 2.9.

KYRC26 adds Rank I and Rank II count and percent. Those columns are a
different measure. State Rank I is 27.0 and state Rank II is 27.7.
Journal 103 question 5 is still open. The columns are not required and
are not emitted.

``KYRC23_OVW_Teacher_Certification_Data.csv`` returns 200 and is school
year 20222023 again. It rounds the two-decimal historical percents and
replaces most published counts with ``*``. It is not registered, and
``accepts_data_file`` skips that name. ``teacher_certifications_2020.csv``
is the 2019-20 report card and is registered. It publishes counts and
no percents. No temporary OAA name was registered, and the guessed
temporary names return 404.

These files have no Suppressed column. A ``*`` on a certification cell
is stored as value NA with ``suppressed`` ``Y``. A blank cell is not a
star and is not stored. ``teacher_certifications_2020.csv``,
``teacher_certifications_2021.csv``, and ``teacher_certifications_2022.csv``
repeat school codes. The copies agree on the teacher count and the
emergency/provisional cells and disagree on National Board. The copy
with the larger National Board count is kept.
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
try:
    from constants import KPI_COLUMNS
except ImportError:  # pragma: no cover - allow running as a script
    from etl.constants import KPI_COLUMNS

logger = logging.getLogger(__name__)

# KYRC23 is the 2022-23 extract again. School year is 20222023 on every
# row. The historical file keeps two-decimal percents. KYRC23 rounds
# those percents and stars most of the counts. Loading both writes 2023
# twice.
_KYRC23_REPUBLISH = "KYRC23_OVW_TEACHER_CERTIFICATION_DATA.CSV"

# Text that is not a count or a percent. A published 0 is not in this set.
_NOT_A_NUMBER = {"", "N/A", "NA", "n/a", "#N/A", "*", "**", "---", "--", "<10"}

# Columns that can become a KPI. Teacher count is the denominator. It is
# not itself a metric. Rank I and Rank II are not in this list.
_MEASURE_COLUMNS = (
    "teacher_count",
    "emergency_provisional_count",
    "emergency_provisional_percent",
    "national_board_certified_count",
    "national_board_certified_percent",
)

# A row with none of these parsed, and no star, is not a certification record.
_KPI_SOURCE_COLUMNS = (
    "emergency_provisional_count",
    "emergency_provisional_percent",
    "national_board_certified_count",
    "national_board_certified_percent",
)

# KPI name for each source column. Teacher count is the denominator and
# is not in this map. Rank I and Rank II are not in this map.
_METRIC_FOR_COLUMN = {
    "emergency_provisional_percent": "emergency_provisional_teacher_rate",
    "emergency_provisional_count": "emergency_provisional_teacher_count",
    "national_board_certified_percent": "national_board_certified_rate",
    "national_board_certified_count": "national_board_certified_count",
}

# 2020-2022 publish two or three rows for the same school code. The
# copies agree on teacher count and emergency/provisional and disagree
# on National Board. Later files do not repeat codes, so the rule is
# limited to these filenames.
_DUPLICATE_NB_FILES = frozenset({
    "teacher_certifications_2020.csv",
    "teacher_certifications_2021.csv",
    "teacher_certifications_2022.csv",
})


def _star_column_name(column: str) -> str:
    """Boolean column that remembers a ``*`` after the cell becomes NA."""
    return f"_star_{column}"


def _is_star_text(value: Any) -> bool:
    """True when the cell text is a privacy star.

    A blank is not a star. ``bool(pd.NA)`` is not used.
    """
    if isinstance(value, str):
        return value.strip() in {"*", "**"}
    return False


def _star_mask(series: pd.Series) -> pd.Series:
    """Boolean mask of ``*`` cells, false for blanks and numbers.

    pandas 3 ``dtype=str`` is StringDtype. pandas 2.3.3 is object. Both
    go through ``astype("string")``. A missing cell is false, not NA,
    so later code never calls ``bool`` on ``pd.NA``.
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
    """True when this certification cell was a ``*``.

    ``standardize_missing_values`` records the position before it turns
    the star into NA. A caller that still has the raw ``*`` is recognized
    from the cell text.
    """
    flag_name = _star_column_name(column)
    if flag_name in row.index:
        return _flag_is_set(row.get(flag_name))
    return _is_star_text(row.get(column))


def _is_string_like(series: pd.Series) -> bool:
    """True for object text and for pandas StringDtype.

    ``read_csv(..., dtype=str)`` on pandas 3 returns StringDtype, not object.
    A comma in ``43,037`` or a percent sign in ``3.0%`` has to be removed
    before ``to_numeric``. pandas 2.3.3 still returns object. pandas is
    unpinned.
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

    ``N/A``, ``*``, and a blank are missing. A published 0 is parsed as 0,
    including a placeholder 0% on a row with no teachers. A blank percent
    is not turned into 0. A comma is removed so ``6,133`` survives. A
    trailing percent sign is removed so the historical KYRC24 cell ``3.0%``
    survives. ``bool(pd.NA)`` is not used.
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
    """Strip commas and percent signs before a numeric cast.

    ``process`` casts ``config/mappings.yaml`` dtypes after
    ``standardize_missing_values``. This source has no dtype cast. The
    cleaner still runs so an Int64 cast of ``43,037`` or ``3.0%`` does not
    become missing. Object and StringDtype are both cleaned. A published 0
    is left as 0. ``*`` and ``N/A`` become missing.
    """
    for col in _MEASURE_COLUMNS:
        if col not in df.columns:
            continue
        series = df[col]
        if not _is_string_like(series):
            continue
        cleaned = (
            series.astype("string")
            .str.replace(",", "", regex=False)
            .str.replace("%", "", regex=False)
            .str.strip()
        )
        missing = cleaned.isna() | cleaned.isin(list(_NOT_A_NUMBER))
        df[col] = cleaned.mask(missing, pd.NA)
    return df


class TeacherCertificationETL(BaseETL):
    """ETL module for processing teacher certification data.

    KYRC26 uses the KYRC25 certification headers and adds Rank I and Rank
    II. Those columns are not mapped. The 2020-2023 files use the
    uppercase names, which are already mapped. The registered KYRC24 file
    has trailing spaces on three measure headers and a percent sign on the
    rate cells. Both are mapped here. No temporary OAA extract is
    registered for this directory.
    """

    # Opt in here, in the same change that accepts the file. BaseETL skips
    # every KYRC26_* CSV until this is True.
    include_kyrc26 = True

    def accepts_data_file(self, file_path: Path) -> bool:
        """Skip the KYRC23 republish of the 2022-23 extract.

        ``KYRC23_OVW_Teacher_Certification_Data.csv`` returns 200 on the
        datasets host. School year is ``20222023`` on every row. The school
        codes match ``teacher_certifications_2023.csv``. Where both files
        publish a count, the counts match, except six National Board
        counts, including the state row (2,086 historical, 2,076 on KYRC23).
        KYRC23 rounds the percent to one decimal and stars the rest.
        Loading both files would write 2023 twice. The guessed OAA names
        are 404 and were never registered, so they are not skipped here.
        """
        if file_path.name.upper() == _KYRC23_REPUBLISH:
            logger.info(
                "Skipping %s; it republishes 2022-23 already loaded from "
                "teacher_certifications_2023.csv",
                file_path.name,
            )
            return False
        return super().accepts_data_file(file_path)

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        """
        Map teacher certification columns to standardized names.

        Handles column name variations across years:
        - 2021-2023: Uppercase column names with EMERGENCY/PROVISIONAL
        - 2024 historical file: title case, with a trailing space on three
          measure headers
        - 2025-2026: title case without the trailing space
        """
        return {
            # Teacher count
            'Teacher Count': 'teacher_count',
            'TEACHER COUNT': 'teacher_count',

            # Emergency/Provisional - Count
            'Emergency Provisional Teacher Count': 'emergency_provisional_count',
            'Emergency Provisional Teacher Count ': 'emergency_provisional_count',
            'EMERGENCY/PROVISIONAL TEACHER COUNT': 'emergency_provisional_count',

            # Emergency/Provisional - Percent
            'Percent Emergency Provisional Teachers': 'emergency_provisional_percent',
            'Percent Emergency Provisional Teachers ': 'emergency_provisional_percent',
            'PERCENT EMERGENCY/PROVISIONAL TEACHERS': 'emergency_provisional_percent',

            # National Board Certified - Count
            'National Board Certified Count': 'national_board_certified_count',
            'NATIONAL BOARD CERTIFIED COUNT': 'national_board_certified_count',

            # National Board Certified - Percent
            'Percent National Board Certified Teacher': 'national_board_certified_percent',
            'Percent National Board Certified Teacher ': 'national_board_certified_percent',
            'PERCENT NATIONAL BOARD CERTIFIED TEACHERS': 'national_board_certified_percent',
        }

    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Record ``*`` positions, then strip commas and percent signs.

        The base method only rewrites ``dtype == object``. pandas 3
        ``dtype=str`` is StringDtype, so the cleaner here is
        dtype-agnostic. ``BaseETL.standardize_missing_values`` is not
        changed. A ``*`` is recorded before it becomes NA, and that cell
        is later written with ``suppressed`` ``Y``. A blank is not recorded.
        """
        star_columns = {
            column: _star_mask(df[column])
            for column in _KPI_SOURCE_COLUMNS
            if column in df.columns
        }
        df = super().standardize_missing_values(df)
        df = _clean_measure_columns(df)
        for column, mask in star_columns.items():
            df[_star_column_name(column)] = mask.to_numpy()
        return df

    def _keep_larger_national_board_copy(
        self, df: pd.DataFrame, source_file: str
    ) -> pd.DataFrame:
        """Keep one row per school code in the 2020-2022 files.

        Those files repeat school codes. The copies agree on the teacher
        count and on both emergency/provisional cells. They disagree on
        the National Board count and percent. The larger National Board
        count is the copy that sums to the state (3,168 in 2020, 3,147
        in 2021, 3,108 in 2022). A missing count sorts below a published
        number, including 0. 2023-2026 do not repeat codes and are left
        as published.
        """
        if Path(source_file).name.lower() not in _DUPLICATE_NB_FILES:
            return df
        if "school_code" not in df.columns or df.empty:
            return df
        code = df["school_code"].astype("string").str.strip()
        coded = code.notna() & code.ne("")
        if not bool(coded.any()):
            return df
        counts = code[coded].value_counts()
        repeated = set(counts[counts > 1].index)
        if not repeated:
            return df

        work = df.copy()
        work["_school_code_key"] = code
        nbc = work.get("national_board_certified_count")
        if nbc is None:
            work["_nbc_sort"] = pd.Series(pd.NA, index=work.index, dtype="Float64")
        else:
            work["_nbc_sort"] = pd.to_numeric(nbc, errors="coerce")
        percent = work.get("national_board_certified_percent")
        if percent is None:
            work["_nbp_sort"] = pd.Series(pd.NA, index=work.index, dtype="Float64")
        else:
            work["_nbp_sort"] = pd.to_numeric(percent, errors="coerce")
        work["_orig"] = range(len(work))
        repeated_rows = work["_school_code_key"].isin(repeated)
        kept_unique = work.loc[~repeated_rows]
        candidates = work.loc[repeated_rows].sort_values(
            ["_school_code_key", "_nbc_sort", "_nbp_sort", "_orig"],
            ascending=[True, False, False, True],
            na_position="last",
        )
        kept_repeated = candidates.drop_duplicates("_school_code_key", keep="first")
        dropped = int(repeated_rows.sum() - len(kept_repeated))
        logger.info(
            "Kept the larger National Board copy in %s; dropped %s extra rows",
            Path(source_file).name,
            dropped,
        )
        combined = pd.concat([kept_unique, kept_repeated], ignore_index=False)
        combined = combined.sort_values("_orig")
        return combined.drop(columns=["_school_code_key", "_nbc_sort", "_nbp_sort", "_orig"])

    def should_skip_row(self, row: pd.Series) -> bool:
        """
        Skip a row that has no school code or no certification measure.

        Teacher certification has no demographic column. A row is not
        skipped for that. A published 0 is not blank, so a 0% is not
        skipped. A ``*`` is a measure and is not skipped: it is stored
        as suppressed. A row whose certification cells are all blank
        has nothing to store. The check looks at the count as well as
        the percent: a blank percent with a positive count is not skipped.
        """
        school_code = row.get('school_code', '')
        if _is_missing(school_code) or school_code == '':
            return True

        present = [name for name in _KPI_SOURCE_COLUMNS if name in row.index]
        starred = [name for name in _KPI_SOURCE_COLUMNS if _metric_was_starred(row, name)]
        if not present and not starred:
            return True
        for name in starred:
            return False
        for name in present:
            if not _is_missing(_parse_number(row.get(name, pd.NA))):
                return False
        return True

    def create_kpi_template(self, row: pd.Series, source_file: str) -> Dict[str, Any]:
        """
        Override to assign "All Students" demographic for all records.

        Teacher certification data has no demographic breakdowns, so all records represent
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
        capped at 100 and is not rebuilt from the count.
        """
        number = _parse_number(value)
        if _is_missing(number):
            return pd.NA
        if number < 0:
            return pd.NA
        return number

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        """
        Extract teacher certification metrics from a data row.

        Returns metrics:
        - emergency_provisional_teacher_rate: Published emergency/provisional percent
        - emergency_provisional_teacher_count: Count on emergency/provisional
        - national_board_certified_rate: Published National Board percent
        - national_board_certified_count: Count with National Board certification

        The rate is the published percent. It is not
        ``count / teacher_count``. Rank I and Rank II are not read.
        A ``*`` cell is included as NA. ``convert_to_kpi_format`` writes
        that metric with ``suppressed`` ``Y``. A blank cell is omitted.
        """
        metrics: Dict[str, Any] = {}
        for column, metric_name in _METRIC_FOR_COLUMN.items():
            if column not in row.index and not _metric_was_starred(row, column):
                continue
            if _metric_was_starred(row, column):
                metrics[metric_name] = pd.NA
                continue
            value = self._metric_value(row.get(column, pd.NA))
            if not _is_missing(value):
                metrics[metric_name] = value
        return metrics

    def convert_to_kpi_format(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """Write a published cell as ``N`` and a ``*`` cell as ``Y``.

        The 2020-2022 files are reduced to the larger National Board copy
        before the rows are written. A blank certification cell is not a
        row. A fully starred row writes one suppressed metric per star.
        """
        df = self._keep_larger_national_board_copy(df, source_file)
        kpi_rows = []
        for _, row in df.iterrows():
            if self.should_skip_row(row):
                continue
            kpi_template = self.create_kpi_template(row, source_file)
            if kpi_template is None:
                continue
            metrics = self.extract_metrics(row)
            for metric_name, value in metrics.items():
                kpi_record = kpi_template.copy()
                kpi_record["metric"] = metric_name
                if _is_missing(value):
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

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get default metrics for suppressed teacher certification records."""
        defaults = {}

        # Only create defaults for metrics that exist in the source data
        if 'emergency_provisional_percent' in row.index:
            defaults['emergency_provisional_teacher_rate'] = pd.NA
        if 'emergency_provisional_count' in row.index:
            defaults['emergency_provisional_teacher_count'] = pd.NA
        if 'national_board_certified_percent' in row.index:
            defaults['national_board_certified_rate'] = pd.NA
        if 'national_board_certified_count' in row.index:
            defaults['national_board_certified_count'] = pd.NA

        return defaults


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Read teacher certification files, normalize, and convert to KPI format."""
    etl = TeacherCertificationETL('teacher_certification')
    etl.process(raw_dir, proc_dir, cfg)


def main():
    """Run teacher certification ETL process."""
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
