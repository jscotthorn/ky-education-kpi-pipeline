"""
Students Taught by Out-of-Field Teachers ETL Module

Processes Kentucky data showing the percentage of students taught by
out-of-field teachers, with equity breakdowns by Title I status and demographics.

KRS 161.1221 requires the Education Professional Standards Board to define
out-of-field teaching and to identify every teacher assigned out of field.
16 KAR 2:120 classifies that work as an emergency, probationary, or temporary
provisional certificate, or as a teacher with no certificate, an expired
certificate, or a certificate that does not cover the subject or the grade.
KDE's School Report Card quality-assurance catalogue describes this measure
as the comparison of students taught by out-of-field teachers in Title I
schools and in schools that are not Title I. The counts come from EPSB
assignment data through the Teacher Equity Report, which publishes the
percentage of the school population or of the subgroup population.

The group columns are within-group rates in every registered year, including
the historical KYRC24 file. Non-White plus White does not equal All Students.
A rate near the All Students figure is the percent of that group, not a
share of all students. These files publish the percent and not the counts,
so this module stores the published number and does not rebuild it.

KYRC26 uses the same headers as KYRC25. No temporary OAA extract is
registered, and the guessed temporary names return 404, so there is no
leftover temporary filename to skip.
``KYRC23_OVW_Students_Taught_by_Out_of_Field_Teachers.csv`` returns 200 and
republishes 2022-23. It is not registered, and ``accepts_data_file`` skips
that name. ``students_taught_by_out_of_field_teachers_2020.csv`` returns
200 and is school year 20192020. It is not registered. 1,680 of 1,831 Title I
and Not Title I rows are all zero, and 122 of 177 districts are entirely
zero, so state Title I 1.41 against 8.88 in 2021 is not a comparable series.
``accepts_data_file`` skips that name. Issue #66 holds whether to bring the
file back. The 2020 file is largely zero-fill. That is a different shape
from the ineffective-teacher placeholder, where the only number is students
with disabilities.

The registered KYRC24 file is the historical extract. Its headers are
``Unnamed: 13`` and ``Inexperienced Value`` through ``Inexperienced Value.8``.
The datasets copy's Title I and Not Title I cells match the UAT copy, and
the historical file's gap rows match the datasets gaps rather than the
historical group cells. This module does not swap that copy. Issue #66
holds the choice.

There is no count column and no blank measure cell in any registered year.
A blank percent is missing, not a zero. A published 0 is kept. A negative
cell is kept. The stored equity gap is signed for 2021-2023 and for KYRC26
(Title I minus Not Title I; KYRC26 has 32 cells off by 0.1) and is the
absolute value for 2024 and 2025. Issue #59 tracks the signed gap. This
module stores the published cell.
"""
from pathlib import Path
import pandas as pd
from typing import Any, Dict, Optional
import logging
import sys

# Add etl directory to path for imports
etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from constants import KPI_COLUMNS
from base_etl import BaseETL, Config

logger = logging.getLogger(__name__)

# KYRC23 is the 2022-23 extract again. School year is 20222023 on every row.
_KYRC23_REPUBLISH = "KYRC23_OVW_STUDENTS_TAUGHT_BY_OUT_OF_FIELD_TEACHERS.CSV"

# 2019-20 is a real file, and its coverage is not comparable. 1,680 of 1,831
# Title I and Not Title I rows are all zero. Issue #66 holds whether to
# register it later. A leftover download must not write year 2020.
_UNREGISTERED_2020 = "STUDENTS_TAUGHT_BY_OUT_OF_FIELD_TEACHERS_2020.CSV"

# Text that is not a percent. These registered files do not use it. A later
# file, or a StringDtype cell that kept the text, is still missing. There
# is no student-count column, so the text is not a zero. A published 0 is
# a real percent.
_NOT_A_PERCENT = {"", "N/A", "NA", "#N/A", "*", "**", "---", "--", "<10"}

# Title I label to the metric suffix. No registered year uses a schoolwide
# label. A label that is not in this map is skipped.
_TITLE_I_SUFFIX = {
    "Title 1": "title_1",
    "Title I": "title_1",
    "Not Title 1": "not_title_1",
    "Not Title I": "not_title_1",
    "Equity Gap": "equity_gap",
}

_DEMOGRAPHIC_KEYS = (
    "all_students",
    "non_white",
    "white",
    "economically_disadvantaged",
    "non_economically_disadvantaged",
    "students_with_disabilities",
    "student_without_disabilities",
    "english_learner",
    "non_english_learner",
)


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


def _text_or_blank(value: Any) -> str:
    """Return a stripped string. Missing values, including ``pd.NA``, are blank."""
    if _is_missing(value):
        return ""
    return str(value).strip()


def _parse_percent(value: Any) -> Any:
    """Parse one published percent.

    A comma is removed so pandas 3 StringDtype does not drop the cell.
    ``read_csv(..., dtype=str)`` is StringDtype on pandas 3 and object on
    pandas 2. No registered percent contains a comma. ``N/A``, ``*``, and a
    blank are missing. These files have no count column, so a blank percent
    is not 0. A published ``0`` stays 0. A negative number is returned and
    stored. ``bool(pd.NA)`` is not used.
    """
    if isinstance(value, str):
        cleaned = value.replace("%", "").replace(",", "").strip()
        if cleaned == "" or cleaned.upper() in _NOT_A_PERCENT or cleaned == "#N/A":
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


class StudentsTaughtByOutOfFieldTeachersETL(BaseETL):
    """ETL module for students taught by out-of-field teachers.

    KYRC26 uses the same headers as KYRC25. The 2021-2023 files use the
    uppercase percent names, which are already mapped. The registered KYRC24
    file uses the corrupted ``Inexperienced Value`` headers, which are mapped
    here. No temporary OAA extract is registered for this directory.
    """

    # Opt in here, in the same change that accepts the file. BaseETL skips
    # every KYRC26_* CSV until this is True.
    include_kyrc26 = True

    # Columns that need to be renamed for the registered KYRC24 file.
    # The header row literally says Unnamed: 13 and Inexperienced Value.
    # Position matches the later demographic order: All Students, Non-White,
    # White, and the six other groups. The datasets KYRC24 object repairs
    # the headers and changes school cells. It is not the file read here.
    KYRC24_COLUMN_FIXES = {
        'Unnamed: 13': 'title_i_status',
        'Inexperienced Value': 'all_students',
        'Inexperienced Value.1': 'non_white',
        'Inexperienced Value.2': 'white',
        'Inexperienced Value.3': 'economically_disadvantaged',
        'Inexperienced Value.4': 'non_economically_disadvantaged',
        'Inexperienced Value.5': 'students_with_disabilities',
        'Inexperienced Value.6': 'student_without_disabilities',
        'Inexperienced Value.7': 'english_learner',
        'Inexperienced Value.8': 'non_english_learner',
    }

    def accepts_data_file(self, file_path: Path) -> bool:
        """Skip the KYRC23 republish and the unregistered 2019-20 file.

        ``KYRC23_OVW_Students_Taught_by_Out_of_Field_Teachers.csv`` returns
        200 on the datasets host. School year is ``20222023`` on every row,
        and the school codes match ``students_taught_by_out_of_field_teachers_2023.csv``
        once a district code is padded to six digits. Many cells differ, so
        it is not a rounded copy of the historical file. Loading both files
        would write 2023 twice.

        ``students_taught_by_out_of_field_teachers_2020.csv`` returns 200
        and is school year 20192020. 1,680 of 1,831 Title I and Not Title I
        rows are all zero, and 122 of 177 districts are entirely zero.
        Loading it would show state Title I 1.41 against 8.88 in 2021.
        Issue #66 holds whether to bring it back. The guessed OAA names are
        404 and were never registered, so they are not skipped here.
        """
        upper_name = file_path.name.upper()
        if upper_name == _KYRC23_REPUBLISH:
            logger.info(
                "Skipping %s; it republishes 2022-23 already loaded from "
                "students_taught_by_out_of_field_teachers_2023.csv",
                file_path.name,
            )
            return False
        if upper_name == _UNREGISTERED_2020:
            logger.info(
                "Skipping %s; the 2019-20 file is largely zero-fill and is "
                "not registered. Issue #66 holds whether to bring it back.",
                file_path.name,
            )
            return False
        return super().accepts_data_file(file_path)

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        """
        Map columns to standardized names.

        Handles column name variations across years:
        - 2021-2023: Uppercase with % prefix and TCHRS abbreviation
        - KYRC24: Corrupted headers (handled separately)
        - KYRC25 and KYRC26: Title case demographic column names
        """
        mappings = {
            # Title I status
            'Title I Status': 'title_i_status',
            'Title_I_Status': 'title_i_status',
            'TITLE I STATUS': 'title_i_status',

            # Demographics - Standard (KYRC25 and KYRC26)
            'All Students': 'all_students',
            'Non-White': 'non_white',
            'White (non-Hispanic)': 'white',
            'White': 'white',
            'Economically Disadvantaged': 'economically_disadvantaged',
            'Non Economically Disadvantaged': 'non_economically_disadvantaged',
            'Non-Economically Disadvantaged': 'non_economically_disadvantaged',
            'Students with Disabilities (IEP)': 'students_with_disabilities',
            'Student without Disabilities (IEP)': 'student_without_disabilities',
            'English Learner': 'english_learner',
            'Non English Learner': 'non_english_learner',
            'Non-English Learner': 'non_english_learner',

            # Demographics - Historical (2021-2023). The 2019-20 file uses
            # the same names and is not loaded. Issue #66.
            '% STUDENTS TAUGHT BY OUT OF FIELD TCHRS': 'all_students',
            '% NON-WHITE STUDENTS TAUGHT BY OUT OF FIELD TCHRS': 'non_white',
            '% WHITE STUDENTS TAUGHT  BY OUT OF FIELD TCHRS': 'white',  # Note: double space in original
            '% WHITE STUDENTS TAUGHT BY OUT OF FIELD TCHRS': 'white',
            '% ECONOMICALLY DISADVANTAGED TAUGHT BY OUT OF FIELD TCHRS': 'economically_disadvantaged',
            '% NON-ECONOMICALLY DISADVANTAGED TAUGHT BY OUT OF FIELD TCHRS': 'non_economically_disadvantaged',
            '% STUDENTS WITH DISABILITIES TAUGHT BY OUT OF FIELD TCHRS': 'students_with_disabilities',
            '% NON-STUDENTS WITH DISABILITIES TAUGHT BY OUT OF FIELD TCHRS': 'student_without_disabilities',
            '% ENGLISH LEARNER STUDENTS TAUGHT BY OUT OF FIELD TCHRS': 'english_learner',
            '% NON-ENGLISH LEARNER STUDENTS TAUGHT BY OUT OF FIELD TCHRS': 'non_english_learner',

            # Gap columns (historical). The equity-gap row already stores the
            # gap in the demographic columns above. These extra columns are
            # not a second metric.
            'OUT OF FIELD GAP % NON-WHITE': 'gap_non_white',
            'OUT OF FIELD GAP % NON-WHITE ': 'gap_non_white',  # Note: trailing space in original
            'OUT OF FIELD GAP % ECONOMICALLY DISADVANTAGED': 'gap_economically_disadvantaged',
            'OUT OF FIELD GAP % STUDENTS WITH DISABILITIES': 'gap_students_with_disabilities',
            'OUT OF FIELD GAP % ENGLISH LEARNERS': 'gap_english_learners',
        }

        # Add KYRC24 corrupted column fixes
        mappings.update(self.KYRC24_COLUMN_FIXES)

        return mappings

    # Mapping from internal demographic keys to standard display names
    DEMOGRAPHIC_DISPLAY_MAP = {
        'all_students': 'All Students',
        'white': 'White (non-Hispanic)',
        'non_white': 'Non-White',
        'economically_disadvantaged': 'Economically Disadvantaged',
        'non_economically_disadvantaged': 'Non-Economically Disadvantaged',
        'students_with_disabilities': 'Students with Disabilities (IEP)',
        'student_without_disabilities': 'Student without Disabilities (IEP)',
        'english_learner': 'English Learner',
        'non_english_learner': 'Non-English Learner',
    }

    def _title_suffix(self, row: pd.Series) -> Optional[str]:
        """Metric suffix for a Title I label, or None when the label is not one we store."""
        return _TITLE_I_SUFFIX.get(_text_or_blank(row.get("title_i_status", "")))

    def _row_is_suppressed(self, row: pd.Series) -> bool:
        """True only for an explicit ``Y``. A missing flag is not suppressed."""
        return _text_or_blank(row.get("suppressed", "N")) == "Y"

    def _all_measures_blank(self, row: pd.Series) -> bool:
        """True when every mapped percent is missing.

        A published 0 is not blank. A negative cell is not blank. There is
        no count column to show that a blank percent is a true zero.
        """
        for key in _DEMOGRAPHIC_KEYS:
            if key not in row.index:
                continue
            if not _is_missing(_parse_percent(row.get(key, pd.NA))):
                return False
        return True

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        """
        Extract metrics from a data row.

        For each row with a Title I status, extract demographic values and create
        metrics with a double-underscore separator so the KPI writer can put the
        demographic on ``student_group``.

        A percent of 0 is kept. A blank is omitted. A negative cell is kept.
        The equity gap is a published difference. In 2021-2023 and in KYRC26
        it is Title I minus Not Title I, so a negative cell is one where
        Title I is lower. KYRC26 includes the state English learner gap of
        -6.0, and 32 cells are off by 0.1. The 2024 and 2025 files publish
        the absolute value and have no negatives. Issue #59 tracks the
        signed gap. The published cell is stored either way.
        """
        metrics: Dict[str, Any] = {}

        title_suffix = self._title_suffix(row)
        if title_suffix is None:
            return metrics

        for demo_key in _DEMOGRAPHIC_KEYS:
            if demo_key not in row.index:
                continue
            value = _parse_percent(row.get(demo_key, pd.NA))
            if _is_missing(value):
                continue
            metric_name = (
                f"students_taught_by_out_of_field_teachers_rate_{title_suffix}__{demo_key}"
            )
            metrics[metric_name] = value

        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get default metrics for suppressed records."""
        defaults: Dict[str, Any] = {}

        title_suffix = self._title_suffix(row)
        if title_suffix is None:
            return defaults

        for demo_key in _DEMOGRAPHIC_KEYS:
            if demo_key in row.index:
                metric_name = (
                    f"students_taught_by_out_of_field_teachers_rate_{title_suffix}__{demo_key}"
                )
                defaults[metric_name] = pd.NA

        return defaults

    def convert_to_kpi_format(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """
        Override base method to handle dynamic student_group assignment for equity metrics.
        """
        kpi_rows = []

        for _, row in df.iterrows():
            # Skip rows that shouldn't be processed
            if self.should_skip_row(row):
                continue

            # Create base KPI template
            kpi_template = self.create_kpi_template(row, source_file)

            # Extract metrics using module-specific logic
            metrics = self.extract_metrics(row)

            # Special handling for suppressed records
            if not metrics and kpi_template["suppressed"] == "Y":
                metrics = self.get_suppressed_metric_defaults(row)

            # Create KPI rows for each metric
            for metric_key, value in metrics.items():
                kpi_record = kpi_template.copy()

                # Check if this is an equity metric (has double underscore separator)
                if "__" in metric_key:
                    base_metric, demo_key = metric_key.split("__")
                    # Update metric name and student_group
                    kpi_record["metric"] = base_metric
                    kpi_record["student_group"] = self.DEMOGRAPHIC_DISPLAY_MAP.get(
                        demo_key, "All Students"
                    )
                else:
                    # Standard metric
                    kpi_record["metric"] = metric_key
                    # student_group remains as set in template

                # Handle suppression and value assignment
                if kpi_template["suppressed"] == "Y":
                    kpi_record["value"] = pd.NA
                    kpi_rows.append(kpi_record)
                else:
                    try:
                        if not _is_missing(value) and value != "":
                            kpi_record["value"] = float(value)
                            kpi_rows.append(kpi_record)
                    except (ValueError, TypeError):
                        continue

        if not kpi_rows:
            logger.warning("No valid KPI rows created")
            return pd.DataFrame()

        # Create KPI DataFrame with consistent column order
        kpi_df = pd.DataFrame(kpi_rows)
        available_columns = [col for col in KPI_COLUMNS if col in kpi_df.columns]
        return kpi_df[available_columns]

    def should_skip_row(self, row: pd.Series) -> bool:
        """Skip a row that is not a stored Title I label, or that has no percent.

        notes/8: a row that is not suppressed and whose values are empty
        emits nothing. These files have no count column, so that empty
        percent is not a zero. A row marked suppressed is kept so the empty
        value can be stored with ``suppressed`` ``Y``. A published 0 is not
        skipped. A negative equity-gap cell is not skipped. There is no
        demographic column on the row, so the base demographic check is not
        used.
        """
        if self._title_suffix(row) is None:
            return True

        if self._row_is_suppressed(row):
            return False

        if self._all_measures_blank(row):
            return True

        return False


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Read students taught by out-of-field teachers files, normalize, and convert to KPI format."""
    etl = StudentsTaughtByOutOfFieldTeachersETL('students_taught_by_out_of_field_teachers')
    etl.process(raw_dir, proc_dir, cfg)


def main():
    """Run students taught by out-of-field teachers ETL process."""
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
