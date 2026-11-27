"""
Postsecondary Enrollment ETL Module

Refactored to use BaseETL for standardized processing.

KYRC26 is ``KYRC26_ADLF_Transition_to_In_State_Postsecondary_Education.csv``.
Its headers match the KYRC24 and KYRC25 files this module already maps.
There is no Suppressed column.

A published ``*`` on a measure cell is value NA with ``suppressed`` Y when
the same row's cohort count (``total_in_group``) is a published number
greater than 0. That star is padding, and no row is stored, when the
cohort count is 0 or blank. A row whose cohort count is itself ``*`` stays
unclassified: the star is not stored, and the row is listed in
``notes/flags/A-flagged-keys.csv`` with reason ``starred cohort count``
and label ``kde-recheck``. A published 0 is kept as 0 with ``suppressed``
N. A blank cell is omitted. ``**`` (2022 and 2023 rates) and ``<10`` (2021
rates) are not ``*`` and stay omitted, which is the prior behavior for
those tokens.

The in-file cohort count is the test. The graduates file is not read.
None of the registered 2021-2026 files contain a cohort of 0 or a blank
cohort cell, and none contain a blank measure cell. KYRC25 still includes
fully starred groups such as Collins Lane ``181025``; those cohort cells
are ``*``, so they stay unclassified rather than confirmed padding.
"""
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
import logging
import sys

etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from base_etl import BaseETL
from constants import KPI_COLUMNS

logger = logging.getLogger(__name__)

# Internal column name, then the KPI metric. The first entry is the cohort.
_MEASURES: Tuple[Tuple[str, str], ...] = (
    ("total_in_group", "postsecondary_enrollment_total_in_cohort"),
    ("public_college_enrolled", "postsecondary_enrollment_public_ky_college_count"),
    ("private_college_enrolled", "postsecondary_enrollment_private_ky_college_count"),
    ("college_enrolled_total", "postsecondary_enrollment_total_ky_college_count"),
    ("public_college_rate", "postsecondary_enrollment_public_ky_college_rate"),
    ("private_college_rate", "postsecondary_enrollment_private_ky_college_rate"),
    ("college_enrollment_rate", "postsecondary_enrollment_total_ky_college_rate"),
)

_COHORT_COLUMN = "total_in_group"
_BLANK_TEXT = {"", "nan", "none", "<na>", "n/a", "--", "---", '""'}


def _raw_column(column: str) -> str:
    """Column that keeps the source text after missing-value cleanup."""
    return f"_raw_{column}"


def _classify_cell(value: Any) -> Tuple[str, Optional[float]]:
    """Classify one published cell.

    Returns ``("star", None)`` for a single asterisk, ``("number", n)``
    for a published number including 0, ``("blank", None)`` for an empty
    cell, and ``("other", None)`` for ``**``, ``<10``, and any other
    token. ``**`` and ``<10`` are not stored.
    """
    if value is None:
        return "blank", None
    try:
        if pd.isna(value):
            return "blank", None
    except (TypeError, ValueError):
        pass
    text = str(value).strip().strip('"')
    if text.lower() in _BLANK_TEXT:
        return "blank", None
    if text == "*":
        return "star", None
    numeric = text.replace(",", "").replace("%", "").strip()
    try:
        return "number", float(numeric)
    except ValueError:
        return "other", None


def clean_percentage_values(df: pd.DataFrame) -> pd.DataFrame:
    """Remove percent signs and convert to numeric.

    Columns whose names start with ``_`` are raw copies. A ``*`` in that
    copy has to survive this step, so those columns are left alone.
    """
    percentage_columns = [
        col
        for col in df.columns
        if "rate" in str(col).lower() and not str(col).startswith("_")
    ]
    for col in percentage_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace("%", "", regex=False)
            df[col] = df[col].str.replace(",", "", regex=False)
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def clean_numeric_values(df: pd.DataFrame) -> pd.DataFrame:
    """Remove commas and quotes from numeric columns.

    A ``*`` is read from the raw copy before this coercion. The cleaned
    column is not the suppression record.
    """
    numeric_columns = [
        "total_in_group",
        "public_college_enrolled",
        "private_college_enrolled",
        "college_enrolled_total",
    ]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(",", "", regex=False)
            df[col] = df[col].str.replace("\"", "", regex=False)
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


class PostsecondaryEnrollmentETL(BaseETL):
    """ETL module for postsecondary enrollment data.

    KYRC26 uses the same measure headers as KYRC24 and KYRC25, including
    ``Percentage College Enrolled In State Table``. The 2021-2023 files
    use the uppercase names, which are already mapped. No temporary OAA
    extract is registered for this directory, and the guessed temporary
    names return 404, so there is no leftover filename to skip.
    """

    # Opt in here, in the same change that accepts the file. BaseETL skips
    # every KYRC26_* CSV until this is True.
    include_kyrc26 = True

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        """Extend base mappings with postsecondary enrollment specific fields."""
        return {
            "Total In Group": "total_in_group",
            "TOTAL IN GROUP": "total_in_group",
            "Public College Enrolled In State": "public_college_enrolled",
            "PUBLIC COLLEGE ENROLLED IN STATE": "public_college_enrolled",
            "Private College Enrolled In State": "private_college_enrolled",
            "PRIVATE COLLEGE ENROLLED IN STATE": "private_college_enrolled",
            "College Enrolled In State": "college_enrolled_total",
            "COLLEGE ENROLLED IN STATE": "college_enrolled_total",
            "Percentage Public College Enrolled In State": "public_college_rate",
            "PERCENTAGE PUBLIC COLLEGE ENROLLED IN STATE": "public_college_rate",
            "Percentage Private College Enrolled In State": "private_college_rate",
            "PERCENTAGE PRIVATE COLLEGE ENROLLED IN STATE": "private_college_rate",
            "Percentage College Enrolled In State": "college_enrollment_rate",
            "PERCENTAGE COLLEGE ENROLLED IN STATE": "college_enrollment_rate",
            "Percentage College Enrolled In State Table": "college_enrollment_rate",
        }

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        """Unused by ``convert_to_kpi_format``.

        One source row can mix a published number and a star. The base
        converter has one suppressed flag for the whole row, so the
        per-cell decision lives in ``convert_to_kpi_format``.
        """
        metrics = {}
        metrics["postsecondary_enrollment_total_in_cohort"] = row.get("total_in_group")
        metrics["postsecondary_enrollment_public_ky_college_count"] = row.get(
            "public_college_enrolled"
        )
        metrics["postsecondary_enrollment_private_ky_college_count"] = row.get(
            "private_college_enrolled"
        )
        metrics["postsecondary_enrollment_total_ky_college_count"] = row.get(
            "college_enrolled_total"
        )
        metrics["postsecondary_enrollment_public_ky_college_rate"] = row.get("public_college_rate")
        metrics["postsecondary_enrollment_private_ky_college_rate"] = row.get(
            "private_college_rate"
        )
        metrics["postsecondary_enrollment_total_ky_college_rate"] = row.get(
            "college_enrollment_rate"
        )
        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Placeholders if a row-level Suppressed flag is ever Y.

        These files have no Suppressed column. A starred measure is written
        from ``convert_to_kpi_format`` only when the cohort count is a
        published number greater than 0.
        """
        defaults = {}

        # Only create defaults for metrics that exist in the source data
        if "total_in_group" in row.index or _raw_column("total_in_group") in row.index:
            defaults["postsecondary_enrollment_total_in_cohort"] = pd.NA
        if "public_college_enrolled" in row.index or _raw_column("public_college_enrolled") in row.index:
            defaults["postsecondary_enrollment_public_ky_college_count"] = pd.NA
        if "private_college_enrolled" in row.index or _raw_column("private_college_enrolled") in row.index:
            defaults["postsecondary_enrollment_private_ky_college_count"] = pd.NA
        if "college_enrolled_total" in row.index or _raw_column("college_enrolled_total") in row.index:
            defaults["postsecondary_enrollment_total_ky_college_count"] = pd.NA
        if "public_college_rate" in row.index or _raw_column("public_college_rate") in row.index:
            defaults["postsecondary_enrollment_public_ky_college_rate"] = pd.NA
        if "private_college_rate" in row.index or _raw_column("private_college_rate") in row.index:
            defaults["postsecondary_enrollment_private_ky_college_rate"] = pd.NA
        if "college_enrollment_rate" in row.index or _raw_column("college_enrollment_rate") in row.index:
            defaults["postsecondary_enrollment_total_ky_college_rate"] = pd.NA

        return defaults

    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Keep the source text, then apply the usual numeric cleanup.

        The base method rewrites ``*`` to missing. The copy is taken first
        so a star can still be stored. ``BaseETL.standardize_missing_values``
        leaves a ``_`` column alone. pandas 2 and pandas 3 both accept
        ``astype("string")``.
        """
        copied = df.copy()
        for column, _metric in _MEASURES:
            if column in copied.columns:
                copied[_raw_column(column)] = copied[column].astype("string")
        copied = super().standardize_missing_values(copied)
        copied = clean_percentage_values(copied)
        copied = clean_numeric_values(copied)
        return copied

    def _cell_value(self, row: pd.Series, column: str) -> Any:
        """Source text for one measure, from the copy when it exists."""
        raw = _raw_column(column)
        if raw in row.index:
            return row.get(raw)
        if column in row.index:
            return row.get(column)
        return pd.NA

    def _column_present(self, row: pd.Series, column: str) -> bool:
        return _raw_column(column) in row.index or column in row.index

    def convert_to_kpi_format(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """One KPI row per published cell, with a per-cell suppressed flag.

        A star is stored only when the cohort count on that row is a
        published number greater than 0. A cohort of 0 or blank drops the
        star. A starred cohort drops the star and keeps any published
        number on the same row. ``**`` and ``<10`` are omitted.
        """
        kpi_rows: List[Dict[str, Any]] = []
        real_stars = 0
        padding_stars = 0
        starred_cohort_rows = 0

        for _, row in df.iterrows():
            if self.should_skip_row(row):
                continue
            kpi_template = self.create_kpi_template(row, source_file)
            if kpi_template is None:
                continue

            cohort_kind, cohort_number = _classify_cell(
                self._cell_value(row, _COHORT_COLUMN) if self._column_present(row, _COHORT_COLUMN) else pd.NA
            )
            real_cohort = (
                cohort_kind == "number"
                and cohort_number is not None
                and cohort_number > 0
            )
            if cohort_kind == "star":
                starred_cohort_rows += 1

            for column, metric_name in _MEASURES:
                if not self._column_present(row, column):
                    continue
                kind, number = _classify_cell(self._cell_value(row, column))
                if column == _COHORT_COLUMN:
                    if kind == "number" and number is not None:
                        record = kpi_template.copy()
                        record["metric"] = metric_name
                        record["value"] = float(number)
                        record["suppressed"] = "N"
                        kpi_rows.append(record)
                    continue
                if kind == "number" and number is not None:
                    record = kpi_template.copy()
                    record["metric"] = metric_name
                    record["value"] = float(number)
                    record["suppressed"] = "N"
                    kpi_rows.append(record)
                elif kind == "star":
                    if real_cohort:
                        record = kpi_template.copy()
                        record["metric"] = metric_name
                        record["value"] = pd.NA
                        record["suppressed"] = "Y"
                        kpi_rows.append(record)
                        real_stars += 1
                    else:
                        padding_stars += 1

        if real_stars or padding_stars or starred_cohort_rows:
            logger.info(
                "%s stored %s suppressed cells; %s stars left unstored; %s starred-cohort rows",
                source_file,
                real_stars,
                padding_stars,
                starred_cohort_rows,
            )

        if not kpi_rows:
            logger.warning("No valid KPI rows created from %s", source_file)
            return pd.DataFrame()

        kpi_df = pd.DataFrame(kpi_rows)
        available_columns = [col for col in KPI_COLUMNS if col in kpi_df.columns]
        return kpi_df[available_columns]


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Entry point for running the ETL."""
    etl = PostsecondaryEnrollmentETL("postsecondary_enrollment")
    etl.process(raw_dir, proc_dir, cfg)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    project_root = Path(__file__).parent.parent
    raw = project_root / "data" / "raw"
    proc = project_root / "data" / "processed"
    proc.mkdir(exist_ok=True)
    transform(raw, proc, {"derive": {"processing_date": "2025-07-19"}})
