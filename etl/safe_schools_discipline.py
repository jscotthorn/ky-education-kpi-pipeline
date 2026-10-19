"""
Safe Schools Discipline ETL Module

Processes Kentucky safe schools discipline resolutions and legal sanctions.

Scott's rule (2026-10-19), clarified the same day: it guides new metrics
and new design choices. This module already stores the published count.
It does not compute a rate, a share, a cross-level sum, or a lower-bound
total, and none of those is added. A count of KPI rows is a count of
stored published cells, not a figure KDE publishes. Issue #52 still
applies: a published 0 is kept, a partial star is suppressed, and a blank
is skipped.

The published cells are counts. There are no decimals, and the smallest
positive number in every registered file is 3. A published 0 is a real zero.
A ``*`` on a row that also has a number is a real suppression (1 or 2, or a
cell that does not apply) and is stored as value NA with ``suppressed`` ``Y``.
A row whose every count cell is ``*`` is the all-star padding KDE later
publishes as 0. That row is not stored. A blank cell is not stored.

``All Students`` is the unduplicated student count. The other demographic
rows are resolution or sanction events. Female plus Male equals the
published ``Total Events`` row, not ``All Students``. That row is skipped
by the existing aggregate filter. This module does not add Female and Male
into a replacement. The published ``Total`` column, and the historical
``TOTAL DISCIPLINE RESOLUTIONS`` column, are not stored. The old parser
did not emit them and did not sum the resolution columns. ``Total`` is
not that sum. This module does not start summing those columns, and it
does not invent a total when a component is ``*``.

KYRC26 uses the title-case resolution headers. KYRC25 spells the expulsion
column ``(Ssp2)``. KYRC24 spells it ``(SSP2)`` and uses ``Out-Of-School``,
``Removal By``, and ``Or``. All three are mapped. ``include_kyrc26`` is set
in this module.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from base_etl import BaseETL
from constants import KPI_COLUMNS

logger = logging.getLogger(__name__)

# Canonical count columns. ``Total`` and ``TOTAL DISCIPLINE RESOLUTIONS``
# are not in this list. They are not the sum of these columns.
COUNT_COLUMNS: List[str] = [
    "corporal_punishment_count",
    "restraint_count",
    "seclusion_count",
    "expelled_not_receiving_services_count",
    "expelled_receiving_services_count",
    "in_school_removal_count",
    "out_of_school_suspension_count",
    "removal_by_hearing_officer_count",
    "unilateral_removal_count",
    "arrest_count",
    "charges_count",
    "civil_proceedings_count",
    "court_designated_worker_count",
    "school_resource_officer_count",
]

# KYRC23 republishes 2022-23. The historical annual file is the registered
# copy. A leftover KYRC23 file would write a second 2023.
_KYRC23_REPUBLISH = {
    "KYRC23_SAFE_DISCIPLINE_RESOLUTIONS.CSV",
    "KYRC23_SAFE_LEGAL_SANCTIONS.CSV",
}

_NOT_A_NUMBER = {"", "*", "**", "---", "--", "<10", '""'}


def _star_column_name(column: str) -> str:
    """Boolean column that remembers a ``*`` after the cell becomes NA."""
    return f"_star_{column}"


def _is_string_like(series: pd.Series) -> bool:
    """True for object text and for pandas StringDtype.

    ``read_csv(..., dtype=str)`` on pandas 3 returns StringDtype, not object.
    A comma in ``35,818`` has to be removed before ``to_numeric``. pandas
    2.3.3 still returns object. pandas is unpinned.
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

    pandas 3 ``dtype=str`` is StringDtype. pandas 2.3.3 is object. Both
    go through ``astype("string")``. A missing cell is false, not NA.
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
    """True when this count cell was a ``*``.

    The star is recorded before the cell becomes NA. A caller that still
    has the raw ``*`` is recognized from the cell text.
    """
    flag_name = _star_column_name(column)
    if flag_name in row.index:
        return _flag_is_set(row.get(flag_name))
    return _is_star_text(row.get(column))


def _is_missing(value: Any) -> bool:
    """True for None, blanks, and pandas missing values.

    ``bool(pd.NA)`` raises. ``pd.isna`` on a scalar returns a real bool.
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
    """Parse a count that may contain a comma.

    ``*`` and a blank are missing. A published 0 is parsed as 0. A blank
    is not turned into 0.
    """
    if isinstance(value, str):
        cleaned = value.replace(",", "").replace('"', "").strip()
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


def _clean_count_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Strip commas on the count columns before a numeric cast.

    ``process`` casts ``config/mappings.yaml`` dtypes after
    ``standardize_missing_values``. This source has no dtype cast. The
    cleaner still runs so an Int64 cast of ``35,818`` does not become
    missing. Object and StringDtype are both cleaned. A published 0 is
    left as 0. ``*`` becomes missing after the star flag is recorded.
    """
    for column in COUNT_COLUMNS:
        if column not in df.columns:
            continue
        series = df[column]
        if not _is_string_like(series):
            continue
        cleaned = (
            series.astype("string")
            .str.replace(",", "", regex=False)
            .str.replace('"', "", regex=False)
            .str.strip()
        )
        missing = cleaned.isna() | cleaned.isin(list(_NOT_A_NUMBER))
        upper = cleaned.str.upper()
        missing = missing | upper.isin(["N/A", "#N/A", "NA", "NAN"])
        df[column] = cleaned.mask(missing, pd.NA)
    return df


class SafeSchoolsDisciplineETL(BaseETL):
    """ETL processor for safe schools discipline resolutions and sanctions.

    KYRC26 headers match KYRC25 except ``(Ssp2)`` became ``(SSP2)``. The
    KYRC24 spellings stay mapped. No temporary OAA extract was ever
    registered. The guessed temporary names return 404.
    """

    # Opt in here, in the same change that accepts the file. BaseETL skips
    # every KYRC26_* CSV until this is True.
    include_kyrc26 = True

    def __init__(self) -> None:
        super().__init__("safe_schools_discipline")

    def accepts_data_file(self, file_path: Path) -> bool:
        """Skip a republish and the wide 2018-19 workbook.

        ``KYRC23_SAFE_Discipline_Resolutions.csv`` and the legal twin are
        school year ``20222023``. They are not registered. The headers
        match the KYRC25 aliases, so a leftover copy would write a second
        2023 next to ``safe_schools_discipline_2023.csv``.

        ``SAFE_SCHOOLS_18-19.xlsx`` is the wide multi-table workbook. It
        has no ``DEMO`` column and no demographic row. The current parser
        produces no rows from it. Skipping the name keeps that result
        without reading the workbook.
        """
        upper = file_path.name.upper()
        if upper in _KYRC23_REPUBLISH:
            logger.info(
                "Skipping %s; it republishes 2022-23 and is not registered",
                file_path.name,
            )
            return False
        if upper.startswith("SAFE_SCHOOLS_18-19"):
            logger.info(
                "Skipping %s; the 2018-19 workbook is wide race and gender columns",
                file_path.name,
            )
            return False
        return super().accepts_data_file(file_path)

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        """Module-specific column mappings."""
        return {
            # KYRC24 discipline resolutions. ``Or``, ``Out-Of-School``, ``By``.
            "Corporal Punishment (SSP5)": "corporal_punishment_count",
            "Restraint (SSP7)": "restraint_count",
            "Seclusion (SSP8)": "seclusion_count",
            "Expelled, Not Receiving Services (SSP2)": "expelled_not_receiving_services_count",
            # KYRC25 spelling. KYRC26 uses (SSP2), mapped above.
            "Expelled, Not Receiving Services (Ssp2)": "expelled_not_receiving_services_count",
            "Expelled, Receiving Services (SSP1)": "expelled_receiving_services_count",
            "In-School Removal (INSR) Or In-District Removal (INDR) >=.5": "in_school_removal_count",
            # KYRC25 and KYRC26 spelling of the same column.
            "In-School Removal (INSR) or In-District Removal (INDR) >=.5": "in_school_removal_count",
            "Out-Of-School Suspensions (SSP3)": "out_of_school_suspension_count",
            "Out-of-School Suspensions (SSP3)": "out_of_school_suspension_count",
            "Removal By Hearing Officer (IAES2)": "removal_by_hearing_officer_count",
            "Removal by Hearing Officer (IAES2)": "removal_by_hearing_officer_count",
            "Unilateral Removal By School Personnel (IAES1)": "unilateral_removal_count",
            "Unilateral Removal by School Personnel (IAES1)": "unilateral_removal_count",
            # Legal sanctions. The headers match in KYRC24, KYRC25, and KYRC26.
            "Arrests": "arrest_count",
            "Charges": "charges_count",
            "Civil Proceedings": "civil_proceedings_count",
            "Court Designated Worker Involvement": "court_designated_worker_count",
            "School Resource Officer Involvement": "school_resource_officer_count",
            # 2020-2023 annual files.
            "EXPELLED RECEIVING SERVICES SSP1": "expelled_receiving_services_count",
            "EXPELLED NOT RECEIVING SERVICES SSP2": "expelled_not_receiving_services_count",
            "OUT OF SCHOOL SUSPENSION SSP3": "out_of_school_suspension_count",
            "CORPORAL PUNISHMENT SSP5": "corporal_punishment_count",
            "IN-SCHOOL REMOVAL INSR": "in_school_removal_count",
            "RESTRAINT SSP7": "restraint_count",
            "SECLUSION SSP8": "seclusion_count",
            "UNILATERAL REMOVAL BY SCHOOL PERSONNEL IAES1": "unilateral_removal_count",
            "REMOVAL BY HEARING OFFICER IAES2": "removal_by_hearing_officer_count",
            # Kept so the column name is stable. Not emitted.
            "TOTAL DISCIPLINE RESOLUTIONS": "total_discipline_resolutions",
            "Total": "total",
        }

    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Record ``*`` positions, then strip commas.

        The base method only rewrites ``dtype == object``. pandas 3
        ``dtype=str`` is StringDtype, so the cleaner here is
        dtype-agnostic. ``BaseETL.standardize_missing_values`` is not
        changed. A ``*`` is recorded before it becomes NA, and that cell
        is later written with ``suppressed`` ``Y``. A blank is not recorded.
        """
        star_columns = {
            column: _star_mask(df[column])
            for column in COUNT_COLUMNS
            if column in df.columns
        }
        df = super().standardize_missing_values(df)
        df = _clean_count_columns(df)
        for column, mask in star_columns.items():
            df[_star_column_name(column)] = mask.to_numpy()
        return df

    def _row_measure_state(self, row: pd.Series) -> tuple[bool, bool, bool, bool]:
        """Return present, saw_number, saw_star, saw_blank for this row."""
        present = False
        saw_number = False
        saw_star = False
        saw_blank = False
        for column in COUNT_COLUMNS:
            flagged = _metric_was_starred(row, column)
            if column not in row.index and not flagged:
                continue
            present = True
            if flagged:
                saw_star = True
                continue
            parsed = _parse_number(row.get(column, pd.NA))
            if _is_missing(parsed):
                saw_blank = True
            else:
                saw_number = True
        return present, saw_number, saw_star, saw_blank

    def should_skip_row(self, row: pd.Series) -> bool:
        """Skip ``Total Events``, an all-star row, and an all-blank row.

        ``Total Events`` is the event total. Female plus Male already
        equals it, and it is not a student group. An all-star row is
        padding: every count cell is ``*`` and there is no number and no
        blank. A blank is not a star. A row that has a number, including
        a published 0, is kept. A row of blanks is not offered and is
        skipped. These registered files have no blank count cell.
        """
        if super().should_skip_row(row):
            return True
        present, saw_number, saw_star, saw_blank = self._row_measure_state(row)
        if not present or saw_number:
            return not present
        if saw_star and not saw_blank:
            return True
        if not saw_star and saw_blank:
            return True
        return False

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        """Extract count metrics from a data row.

        A published 0 is kept. A ``*`` is returned as missing so the KPI
        row can be written with ``suppressed`` ``Y``. A blank is omitted.
        """
        metrics: Dict[str, Any] = {}
        for column in COUNT_COLUMNS:
            flagged = _metric_was_starred(row, column)
            if column not in row.index and not flagged:
                continue
            if flagged:
                metrics[column] = pd.NA
                continue
            parsed = _parse_number(row.get(column, pd.NA))
            if _is_missing(parsed):
                continue
            if parsed < 0:
                logger.warning("Negative count in %s: %s", column, parsed)
                continue
            metrics[column] = int(parsed) if parsed == int(parsed) else parsed
        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """NA for each count column present on a row-level suppressed record.

        These files have no ``Suppressed`` column. A ``*`` is handled per
        cell in ``convert_to_kpi_format``. This default is the base-class
        path for a row flagged ``suppressed`` ``Y`` with no extracted number.
        """
        defaults: Dict[str, Any] = {}
        for column in COUNT_COLUMNS:
            if column in row.index or _metric_was_starred(row, column):
                defaults[column] = pd.NA
        return defaults

    def convert_to_kpi_format(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """Write one KPI row per count cell.

        A published number, including 0, is ``suppressed`` ``N``. A ``*``
        is value NA and ``suppressed`` ``Y``. A blank cell is not a row.
        An all-star row is skipped before this loop.
        """
        kpi_rows: List[Dict[str, Any]] = []
        for _, row in df.iterrows():
            if self.should_skip_row(row):
                continue
            kpi_template = self.create_kpi_template(row, source_file)
            if kpi_template is None:
                continue
            metrics = self.extract_metrics(row)
            if not metrics and kpi_template["suppressed"] == "Y":
                metrics = self.get_suppressed_metric_defaults(row)
            for metric_name, value in metrics.items():
                kpi_record = kpi_template.copy()
                kpi_record["metric"] = metric_name
                starred = _metric_was_starred(row, metric_name)
                if starred or kpi_template["suppressed"] == "Y":
                    kpi_record["value"] = pd.NA
                    kpi_record["suppressed"] = "Y"
                    kpi_rows.append(kpi_record)
                    continue
                if _is_missing(value):
                    continue
                try:
                    kpi_record["value"] = float(value)
                except (TypeError, ValueError):
                    continue
                kpi_record["suppressed"] = "N"
                kpi_rows.append(kpi_record)

        if not kpi_rows:
            logger.warning("No valid KPI rows created")
            return pd.DataFrame()

        kpi_df = pd.DataFrame(kpi_rows)
        available_columns = [column for column in KPI_COLUMNS if column in kpi_df.columns]
        return kpi_df[available_columns]


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Read discipline files and write KPI rows."""
    etl = SafeSchoolsDisciplineETL()
    etl.process(raw_dir, proc_dir, cfg)


def main() -> None:
    """Run the discipline pipeline against the local raw directory."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    try:
        raw_dir = Path(__file__).parent.parent / "data" / "raw"
        proc_dir = Path(__file__).parent.parent / "data" / "processed"
        proc_dir.mkdir(exist_ok=True)
        test_config = {
            "derive": {
                "processing_date": "2025-07-20",
                "data_quality_flag": "reviewed",
            }
        }
        transform(raw_dir, proc_dir, test_config)
    except Exception as exc:
        logger.error("Pipeline execution failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
