"""
CTE Participation ETL Module

Handles Kentucky Career and Technical Education (CTE) participation data.
Normalizes column names, handles schema variations, standardizes missing values, and
applies demographic label standardization for consistent longitudinal reporting.

KYRC26 is ``KYRC26_CTE_Participation.csv``. Its headers match the KYRC25 file.
Those two files publish counts. The participant count, the high-school
enrollment count, the grade-12 completer count, and the grade-12 participant
count are stored. Neither rate is calculated. Participants are counted where
the course is taught and enrollment where the student is enrolled. KYRC25
and KYRC26 do not publish eligible completers.

The 2022 and 2023 files use the same four headers as KYRC24, in uppercase, and
every measure cell is a count or a ``*``. None of them is a rate. Counts are
stored on count metrics. The rate metrics are left absent for those years.
Every district and state enrollment cell in those two files is ``*``.

KYRC24 publishes the participation rate in ``CTE Participants in All Grades``
and the completion rate in ``Grade 12 CTE Completers``. ``Total Number of
Student`` is the participant count. ``Grade 12 CTE Eligible Completer`` is
the completer count: the header does not match the cells, same as ``Total
Number of Student``. KYRC23's column of that name equals the 2023 annual
``GRADE 12 CTE COMPLETERS`` count wherever both are numbers. A published rate
above 100 is kept. A published 0.0 is kept, including the 208 KYRC24
participation rates on a positive participant count (Refs #112). Those
published percents are the only ``cte_participation_rate`` and
``cte_completion_rate_grade_12`` values this module stores. KYRC23's
published 2023 rates are not loaded.

There is no Suppressed column. A ``*`` or ``<10`` cell is value NA with
``suppressed`` Y. A blank, ``--``, or ``N/A`` is not stored. ``N/A`` on a
career-center enrollment means those students are counted at the home school,
so the enrollment count is absent for that row. A published 0 is kept. A
later star on the same key does not replace a published number.
"""
from pathlib import Path
import pandas as pd
from typing import Any, Dict, List, Optional, Tuple
import logging

import sys

# Add etl directory to path for imports
etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from constants import KPI_COLUMNS
from base_etl import BaseETL, Config

logger = logging.getLogger(__name__)

# Internal column after the header mapping, then the KPI name.
# These are published counts. A star on the cell is suppressed Y.
_COUNT_COLUMNS: Tuple[Tuple[str, str], ...] = (
    ("cte_participant_count", "cte_participant_count"),
    ("total_student_count", "cte_high_school_enrollment_count"),
    ("cte_eligible_completer_count", "cte_eligible_completer_count_grade_12"),
    ("cte_completer_count_grade_12", "cte_completer_count_grade_12"),
    ("cte_participant_count_grade_12", "cte_participant_count_grade_12"),
)

# Published rate columns. KYRC24 only. Values above 100 stay as published.
_RATE_COLUMNS: Tuple[Tuple[str, str], ...] = (
    ("cte_participation_rate", "cte_participation_rate"),
    ("cte_completion_rate", "cte_completion_rate_grade_12"),
)

# ``<10`` is suppressed, not blank. No registered CTE file uses it.
_SUPPRESSED_TEXT = {"*", "**", "<10"}
_BLANK_TEXT = {"", "N/A", "n/a", "#N/A", "---", "--", '""', "nan", "None", "<NA>"}

Cell = Tuple[str, Optional[float]]


def _raw_column(column: str) -> str:
    """Column that keeps the source text after missing-value cleanup."""
    return f"_raw_{column}"


def _cell_text(value: Any) -> str:
    """Source text, or empty when the cell is missing.

    ``bool(pd.NA)`` raises. A missing cell is blank, not a number and not a star.
    """
    if value is None:
        return ""
    try:
        if bool(pd.isna(value)):
            return ""
    except (TypeError, ValueError):
        return ""
    return str(value).strip()


def _classify_cell(value: Any) -> Cell:
    """Classify one measure cell.

    ``star`` is a privacy star or ``<10``. ``blank`` is empty, not applicable,
    or not a number. ``number`` is a parsed value, including a published 0 and
    a published rate above 100. Commas are removed for object text and for
    pandas StringDtype. A negative is not stored.
    """
    text = _cell_text(value)
    if text in _SUPPRESSED_TEXT:
        return "star", None
    if text in _BLANK_TEXT:
        return "blank", None
    cleaned = text.replace(",", "").replace('"', "").replace("%", "")
    try:
        number = float(cleaned)
    except ValueError:
        return "blank", None
    if number < 0:
        logger.warning("Ignoring negative CTE value %r", text)
        return "blank", None
    return "number", number


def clean_numeric_with_commas(series: pd.Series) -> pd.Series:
    """Convert strings with commas to numeric values.

    ``read_csv(..., dtype=str)`` on pandas 3 returns StringDtype, not
    object. ``astype(str)`` would turn a missing cell into the literal
    ``nan``. ``astype("string")`` keeps it missing, and the comma strip
    does not check ``dtype == object``. pandas is unpinned. A star is not
    a number; callers that need the star must classify the source text
    first.
    """
    cleaned = (
        series.astype("string")
        .str.replace(",", "", regex=False)
        .str.replace('"', "", regex=False)
    )
    return pd.to_numeric(cleaned, errors="coerce")


def clean_cte_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean CTE participation values.

    A published rate above 100 is kept. KYRC24 has participation rates
    from 100.4 through 400.0, and those cells are the rate KDE published.
    A negative rate or count is set to missing. Counts are not capped.
    """
    for column in ("cte_participation_rate", "cte_completion_rate"):
        if column not in df.columns:
            continue
        df[column] = clean_numeric_with_commas(df[column])
        invalid_mask = df[column] < 0
        if invalid_mask.any():
            logger.warning("Found %s negative CTE rates in %s", int(invalid_mask.sum()), column)
            df.loc[invalid_mask, column] = pd.NA

    for column in (
        "cte_eligible_completer_count",
        "total_student_count",
        "cte_participant_count",
        "cte_completer_count_grade_12",
        "cte_participant_count_grade_12",
    ):
        if column not in df.columns:
            continue
        df[column] = clean_numeric_with_commas(df[column])
        invalid_mask = df[column] < 0
        if invalid_mask.any():
            logger.warning("Found %s negative CTE counts in %s", int(invalid_mask.sum()), column)
            df.loc[invalid_mask, column] = pd.NA

    return df


class CTEParticipationETL(BaseETL):
    """ETL module for processing CTE participation data.

    KYRC26 uses the same count headers as KYRC25. No temporary OAA extract
    is registered for this directory, and the guessed temporary names return
    404, so there is no leftover filename to skip. The 2022 and 2023
    historical names stay.
    """

    # Opt in here, in the same change that accepts the file. BaseETL skips
    # every KYRC26_* CSV until this is True.
    include_kyrc26 = True

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            # KYRC24 published participation rate. The header says participants.
            # The cells are percents, including values above 100.
            'CTE Participants in All Grades': 'cte_participation_rate',

            # 2022 and 2023. The same words are a count in every row.
            'CTE PARTICIPANTS IN ALL GRADES': 'cte_participant_count',

            # KYRC25 and KYRC26 participant count.
            'All High School CTE Participants': 'cte_participant_count',
            'ALL HIGH SCHOOL CTE PARTICIPANTS': 'cte_participant_count',

            # KYRC25 and KYRC26 high-school enrollment.
            'All High School Enrollments': 'total_student_count',
            'ALL HIGH SCHOOL ENROLLMENTS': 'total_student_count',

            # KYRC25 and KYRC26 grade-12 counts.
            '12th Grade Completers': 'cte_completer_count_grade_12',
            '12TH GRADE COMPLETERS': 'cte_completer_count_grade_12',
            '12th Grade Participants': 'cte_participant_count_grade_12',
            '12TH GRADE PARTICIPANTS': 'cte_participant_count_grade_12',

            # KYRC24. The header says eligible completers. The cells are the
            # completer count (state All Students 17,597). Same
            # header-versus-content problem as Total Number of Student.
            'Grade 12 CTE Eligible Completer': 'cte_completer_count_grade_12',

            # 2022 and 2023 eligible-completer count. The uppercase header
            # matches the cells. KYRC24 does not use this metric.
            'GRADE 12 CTE ELIGIBLE COMPLETER': 'cte_eligible_completer_count',

            # KYRC24 published completion rate.
            'Grade 12 CTE Completers': 'cte_completion_rate',

            # 2022 and 2023 completer count. Not a rate.
            'GRADE 12 CTE COMPLETERS': 'cte_completer_count_grade_12',

            # KYRC24. The header says students. The state cell is 143,415,
            # the participant count. It is not high-school enrollment.
            'Total Number of Student': 'cte_participant_count',

            # 2022 and 2023 high-school enrollment. Every district and state
            # cell is *, in every demographic.
            'TOTAL NUMBER OF STUDENT': 'total_student_count',
        }

    def _cell(self, row: pd.Series, column: str) -> Optional[Cell]:
        """One measure cell, or None when this file has no such column.

        The raw copy is read first. ``standardize_missing_values`` turns a
        ``*`` into missing on pandas 2, where ``dtype=str`` is still object.
        pandas 3 leaves the star in a StringDtype column. Either way the
        copy still has the source text.
        """
        raw_name = _raw_column(column)
        if raw_name in row.index:
            return _classify_cell(row.get(raw_name))
        if column not in row.index:
            return None
        return _classify_cell(row.get(column))

    def _iter_metric_cells(self, row: pd.Series) -> List[Tuple[str, str, Optional[float]]]:
        """Published cells on this row.

        A blank cell is omitted. A star is returned so the converter can
        store value NA with ``suppressed`` Y. A number is returned as
        published, including a rate above 100 and a count above 100.
        ``cte_participation_rate`` and ``cte_completion_rate_grade_12`` are
        only the percents KYRC24 published. KYRC25 and KYRC26 keep the
        counts and do not divide them.
        """
        found: List[Tuple[str, str, Optional[float]]] = []
        for column, metric_name in _COUNT_COLUMNS + _RATE_COLUMNS:
            cell = self._cell(row, column)
            if cell is None:
                continue
            kind, number = cell
            if kind == "blank":
                continue
            found.append((metric_name, kind, number))
        return found

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        """Numeric metrics for a row that is already a single suppressed flag.

        Stars are stored from ``convert_to_kpi_format``, which can mark one
        metric suppressed and keep another. This method returns numbers only,
        so a direct caller does not turn a count of 77 into a rate.
        """
        metrics: Dict[str, Any] = {}
        for metric_name, kind, number in self._iter_metric_cells(row):
            if kind == "number":
                metrics[metric_name] = number
        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get default metrics for suppressed CTE participation records."""
        return {
            'cte_participation_rate': pd.NA,
            'cte_participant_count': pd.NA,
            'cte_high_school_enrollment_count': pd.NA,
            'cte_eligible_completer_count_grade_12': pd.NA,
            'cte_completion_rate_grade_12': pd.NA,
            'cte_completer_count_grade_12': pd.NA,
            'cte_participant_count_grade_12': pd.NA,
        }

    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Keep a copy of each measure cell, including stars and commas.

        The base method only rewrites ``dtype == object``. pandas 3
        ``dtype=str`` is StringDtype, so a star would survive there and a
        comma would not. The copy is taken first, on both dtypes. A ``*``
        stays in the copy and is later written with ``suppressed`` Y.
        ``BaseETL.standardize_missing_values`` is not changed.
        """
        measure_columns = [column for column, _metric in _COUNT_COLUMNS + _RATE_COLUMNS]
        for column in measure_columns:
            if column in df.columns:
                df[_raw_column(column)] = df[column].astype("string")

        df = super().standardize_missing_values(df)

        for column in measure_columns:
            if column in df.columns and df[column].dtype == "object":
                df[column] = df[column].astype(str).str.replace(",", "", regex=False)

        return clean_cte_data(df)

    def convert_to_kpi_format(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """One row per school, year, student group, and published measure.

        A star or ``<10`` suppresses that measure only. Another measure on
        the same row stays as published. A blank cell is not a row. A
        published 0 is kept. A published rate above 100 is kept. The same
        key is not written twice. A star does not replace a number already
        stored on that key.
        """
        kpi_rows: List[Dict[str, Any]] = []
        seen: Dict[Tuple[str, str, str, str], int] = {}
        # Both rates are published KYRC24 percents. A value above 100 is
        # the rate KDE printed.
        rates_above_100 = 0

        for _, row in df.iterrows():
            if self.should_skip_row(row):
                continue
            kpi_template = self.create_kpi_template(row, source_file)
            if kpi_template is None:
                continue

            for metric_name, kind, number in self._iter_metric_cells(row):
                kpi_record = kpi_template.copy()
                kpi_record["metric"] = metric_name
                if kind == "star":
                    kpi_record["value"] = pd.NA
                    kpi_record["suppressed"] = "Y"
                else:
                    kpi_record["value"] = float(number) if number is not None else pd.NA
                    kpi_record["suppressed"] = "N"
                    if (
                        metric_name in ("cte_participation_rate", "cte_completion_rate_grade_12")
                        and number is not None
                        and number > 100
                    ):
                        rates_above_100 += 1

                key = (
                    str(kpi_record["school_id"]),
                    str(kpi_record["year"]),
                    str(kpi_record["student_group"]),
                    metric_name,
                )
                if key in seen:
                    previous = kpi_rows[seen[key]]
                    same_value = previous["suppressed"] == kpi_record["suppressed"] and (
                        previous["suppressed"] == "Y" or previous["value"] == kpi_record["value"]
                    )
                    if same_value:
                        continue
                    logger.warning(
                        "%s has two %s values for school %s %s %s",
                        source_file,
                        metric_name,
                        key[0],
                        key[1],
                        key[2],
                    )
                    # Keep a published number. A star must not replace it,
                    # whichever row came first.
                    previous_is_number = previous["suppressed"] != "Y"
                    new_is_number = kpi_record["suppressed"] != "Y"
                    if new_is_number and not previous_is_number:
                        kpi_rows[seen[key]] = kpi_record
                    continue

                seen[key] = len(kpi_rows)
                kpi_rows.append(kpi_record)

        if rates_above_100:
            logger.info(
                "%s kept %s published CTE rates above 100",
                source_file,
                rates_above_100,
            )

        if not kpi_rows:
            logger.warning("No valid KPI rows created from %s", source_file)
            return pd.DataFrame()

        kpi_df = pd.DataFrame(kpi_rows)
        available_columns = [col for col in KPI_COLUMNS if col in kpi_df.columns]
        return kpi_df[available_columns]


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Read newest CTE participation files, normalize, and convert to KPI format with demographic standardization using BaseETL."""
    # Use BaseETL for consistent processing
    etl = CTEParticipationETL('cte_participation')
    etl.process(raw_dir, proc_dir, cfg)


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    from pathlib import Path
    raw_dir = Path(__file__).parent.parent / "data" / "raw"
    proc_dir = Path(__file__).parent.parent / "data" / "processed"
    proc_dir.mkdir(exist_ok=True)

    test_config = Config(
        derive={"processing_date": "2025-07-21", "data_quality_flag": "reviewed"}
    ).model_dump()

    transform(raw_dir, proc_dir, test_config)
