"""
Student-Teacher Ratio ETL Module

Processes Kentucky student-teacher ratio data. This is an institutional
metric. The file has no demographic column and no Suppressed column.

KDE's report-card glossary defines Student to Teacher Ratio as the school's
6/30 SAAR membership divided by full-time-equivalent teachers, excluding
administrators, guidance counselors, media specialists, and resource
teachers. 703 KAR 5:140 calls the same measure the average student/teacher
ratio: end-of-year membership divided by FTE teachers, excluding
administrators, guidance counselors, and media specialists. The 2018-19
cover sheet describes ``STDNT_TCH_RATIO`` as the number of students enrolled
compared with the number of teachers on staff.

The published figure is students per one teacher, the number before the
colon in ``15:01``, ``15:1``, and ``28:01:00``. The parser stores that
number. It does not recompute membership divided by FTE. These files do
not contain those two components.

KYRC26 uses the same headers as KYRC25. It has school rows only. The state
row and the district ``All Schools`` rows are not in the file. This module
does not rebuild them. The glossary excludes the Kentucky School for the
Blind, the Kentucky School for the Deaf, and the Model Laboratory schools
from the state total, so a sum of school rows is not that total.

No temporary OAA extract is registered, and the guessed temporary names
return 404, so there is no leftover temporary filename to skip.
``KYRC23_OVW_Student_to_Teacher_Ratio.csv`` returns 200 and republishes
2022-23. It is not registered, and ``accepts_data_file`` skips that name.
"""
from pathlib import Path
import pandas as pd
from typing import Any, Dict, Optional
import logging
import sys
import re

# Add etl directory to path for imports
etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from base_etl import BaseETL, Config

logger = logging.getLogger(__name__)

# KYRC23 is the 2022-23 extract again. The registered historical file already
# covers that year. School codes match after 3-digit district codes are
# written as NNN000, and the ratio numerators match.
_KYRC23_REPUBLISH = "KYRC23_OVW_STUDENT_TO_TEACHER_RATIO.CSV"

# Students per one teacher. ``28:01:00`` is the same ratio with a trailing
# seconds field of zeros. A denominator of ``00`` is a published zero only
# when the numerator is 0 (``0:00`` or ``0:00:00``). ``15:00`` is not 15.
_RATIO_PART = re.compile(r"^[\d.]+$")
_ZERO_SECONDS = re.compile(r"^0+$")


def _ratio_text(value: Any) -> Optional[str]:
    """Return cleaned ratio text, or None when the cell is missing.

    ``pd.NA`` is not used as a boolean. ``read_csv(..., dtype=str)`` is
    StringDtype on pandas 3 and object on pandas 2.3.3. A comma in a
    numerator is removed before the match. pandas is unpinned. The
    registered files have no comma-formatted ratios.
    """
    if value is None:
        return None
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        return None
    text = str(value).strip().replace(",", "")
    if text == "" or text.lower() in {"nan", "<na>", "none"}:
        return None
    return text


def parse_ratio(ratio_str: Any) -> Any:
    """
    Parse a published student-teacher ratio into students per one teacher.

    ``15:01``, ``15:1``, and ``15.5:01`` all mean 15 or 15.5 students per
    teacher. ``28:01:00`` is that same shape with a trailing ``:00`` of
    zeros. ``0:01``, ``0:1``, ``00:01``, and ``0:00`` are a published zero.
    A ``:00`` or ``:00:00`` denominator is accepted only when the numerator
    is 0. ``15:00``, ``15:2``, and ``15:01:30`` are not this measure and
    return ``pd.NA``. A plain number is accepted for a file that drops the
    colon. A missing cell returns ``pd.NA``.

    Args:
        ratio_str: Ratio text, or a missing value.

    Returns:
        The numeric ratio, or ``pd.NA`` when the cell is missing or invalid.
    """
    text = _ratio_text(ratio_str)
    if text is None:
        return pd.NA

    if _RATIO_PART.match(text):
        try:
            return float(text)
        except ValueError:
            logger.warning(f"Could not convert ratio {ratio_str} to float")
            return pd.NA

    parts = text.split(":")
    if len(parts) in (2, 3) and all(_RATIO_PART.match(part) for part in parts):
        numerator, denominator = parts[0], parts[1]
        seconds = parts[2] if len(parts) == 3 else None
        seconds_are_zero = seconds is None or _ZERO_SECONDS.match(seconds) is not None
        try:
            numerator_value = float(numerator)
        except ValueError:
            logger.warning(f"Could not convert ratio {ratio_str} to float")
            return pd.NA
        # One teacher, optionally with a zero seconds field: ``15:01``,
        # ``15:1``, ``00:01``, ``28:01:00``.
        if denominator in {"1", "01"} and seconds_are_zero:
            return numerator_value
        # ``0:00`` and ``0:00:00`` are a published zero. ``15:00`` is not 15.
        if (
            denominator == "00"
            and numerator_value == 0.0
            and seconds_are_zero
        ):
            return 0.0

    logger.warning(f"Invalid ratio format: {ratio_str}")
    return pd.NA


class StudentTeacherRatioETL(BaseETL):
    """ETL module for processing student-teacher ratio data.

    KYRC26 uses the same ``Student Teacher Ratio`` header as KYRC24 and
    KYRC25. The 2020-2023 files use the doubled historical header, which is
    already mapped. No temporary OAA extract is registered for this directory.
    """

    # Opt in here, in the same change that accepts the file. BaseETL skips
    # every KYRC26_* CSV until this is True.
    include_kyrc26 = True

    def accepts_data_file(self, file_path: Path) -> bool:
        """Skip the KYRC23 republish of the 2022-23 extract.

        ``KYRC23_OVW_Student_to_Teacher_Ratio.csv`` returns 200 on the
        datasets host. After 3-digit district codes are written as
        ``NNN000``, the school codes match ``student_teacher_ratio_2023.csv``
        and the ratio numerators match. Loading both files would write each
        school twice for 2023. The guessed OAA names are 404 and were never
        registered, so they are not skipped here.
        """
        if file_path.name.upper() == _KYRC23_REPUBLISH:
            logger.info(
                "Skipping %s; it republishes 2022-23 already loaded from "
                "student_teacher_ratio_2023.csv",
                file_path.name,
            )
            return False
        return super().accepts_data_file(file_path)

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            'Student Teacher Ratio': 'ratio',
            'STUDENT TEACHER RATIO': 'ratio',
            'Student-Teacher Ratio': 'ratio',
            'Ratio': 'ratio',
            'STUDENT TO TEACHER RATIOSTUDENT TO TEACHER RATIO': 'ratio',  # Historical file anomaly

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
            'STDNT_TCH_RATIO': 'ratio',
        }

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        metrics = {}

        # Students per one teacher. A published zero (``0:01``, ``0:1``,
        # ``00:01``, or ``0:00``) is omitted by this comparison. Those rows
        # are not applicable: zero membership, or teachers counted outside
        # normal reporting. They are preschools, career and technical
        # centers, alternative schools, and KSB, KSD, and Model, not regular
        # A1 schools. Omitting them is correct under notes/8.
        ratio_numeric = parse_ratio(row.get('ratio', pd.NA))
        if pd.notna(ratio_numeric) and ratio_numeric > 0:
            metrics['student_teacher_ratio'] = ratio_numeric

        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get default metrics for suppressed student-teacher ratio records."""
        defaults = {}

        # Only create defaults for metrics that exist in the source data
        if 'ratio' in row.index:
            defaults['student_teacher_ratio'] = pd.NA

        return defaults

    def should_skip_row(self, row: pd.Series) -> bool:
        """Skip a row whose ratio cell is empty.

        notes/8: a row that is not suppressed and has every measure empty
        is not offered and emits nothing. These files have no Suppressed
        column. A published zero is not empty; ``extract_metrics`` decides
        whether that zero becomes a KPI row. ``pd.NA`` from a missing cell
        is not evaluated as a boolean.
        """
        ratio = row.get('ratio', pd.NA)
        if _ratio_text(ratio) is None:
            return True

        return False  # Don't call super() - institutional data has no demographics


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Read student-teacher ratio files, normalize, and convert to KPI format."""
    etl = StudentTeacherRatioETL('student_teacher_ratio')
    etl.process(raw_dir, proc_dir, cfg)


def main():
    """Run student-teacher ratio ETL process."""
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
