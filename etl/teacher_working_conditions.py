"""
Teacher Working Conditions ETL Module

Processes Kentucky Impact survey results. KDE's report-card glossary calls
the measure Teacher Working Conditions: three constructs from the Impact
Kentucky working conditions survey that are connected to teacher retention
and student achievement. The Impact Value is the percent favorable, the
share of respondents who selected a favorable answer. Most often that is the
top two of five choices. The glossary's older "Teaching Working Conditions"
entry uses the same definition for the 2020 survey.

The file is one row per school and construct. There is no demographic
column. ``student_group`` is ``All Students``.

2020-2023 publish Managing Student Behavior, School Leadership, and Teaching
Environment, with a "Composite" suffix. KYRC24 and KYRC26 publish Managing
Student Behavior, School Climate, and School Leadership. This module keeps
those labels on separate metrics and does not bridge Teaching Environment
into School Climate.

Impact Kentucky is biennial. Each administration is published on two report
cards. The KPI year is the ending year of the school year in which the
survey was administered, the same rule ``BaseETL.extract_year`` uses for
every other module (an 8-digit ``YYYYYYYY`` school year keeps the last four
digits). Years with no administration are absent.

- January 13-February 21, 2020, school year 2019-20, KPI year 2020.
  Kept file: ``teacher_working_conditions_2020.csv`` (``20192020``).
- November 1-December 17, 2021, school year 2021-22, KPI year 2022.
  Kept file: ``teacher_working_conditions_2022.csv`` (``20212022``).
- November 1-December 15, 2023, school year 2023-24, KPI year 2024.
  Kept file: ``KYRC24_OVW_Teacher_Working_Conditions.csv`` (``20232024``).
- November 10-December 19, 2025, school year 2025-26, KPI year 2026.
  Kept file: ``KYRC26_OVW_Teacher_Working_Conditions.csv`` (``20252026``).

``accepts_data_file`` skips the other report-card copy by exact filename:
``teacher_working_conditions_2021.csv``, ``teacher_working_conditions_2023.csv``,
``KYRC25_OVW_Teacher_Working_Conditions.csv``, and
``KYRC23_OVW_Teacher_Working_Conditions.csv``. KYRC23 also turns 945 ``N/A``
cells into 0. None of those four names is registered.

KYRC26 uses the same headers as KYRC24. No temporary OAA extract is
registered, and the guessed temporary names return 404, so there is no
leftover temporary filename to skip.
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

# The second report-card copy of an Impact Kentucky administration.
# The value is the file whose school year is the administration year.
# KYRC23 is that 2021-22 survey again, with Teaching Environment relabeled
# School Climate and 945 N/A cells stored as 0.
_REPUBLISHED_SURVEY_FILES = {
    "TEACHER_WORKING_CONDITIONS_2021.CSV": "teacher_working_conditions_2020.csv",
    "TEACHER_WORKING_CONDITIONS_2023.CSV": "teacher_working_conditions_2022.csv",
    "KYRC25_OVW_TEACHER_WORKING_CONDITIONS.CSV": "KYRC24_OVW_Teacher_Working_Conditions.csv",
    "KYRC23_OVW_TEACHER_WORKING_CONDITIONS.CSV": "teacher_working_conditions_2022.csv",
}

# Text that is not a percent. These files have no count column, so N/A is
# not a zero. A published 0 is a real percent and is not in this set.
# A star is a suppressed percent, handled separately from this set.
_NOT_A_PERCENT = {"", "N/A", "NA", "n/a", "*", "**", "---", "--", "<10"}
_STAR_TEXT = {"*", "**"}


def _is_star_text(value: Any) -> bool:
    """True when the cell is a privacy star. A blank is not a star."""
    if not isinstance(value, str):
        return False
    return value.strip() in _STAR_TEXT


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


class TeacherWorkingConditionsETL(BaseETL):
    """ETL module for Impact Kentucky percent-favorable scores.

    KYRC26 uses the same headers as KYRC24. The 2020 and 2022 files use the
    uppercase names, which are already mapped. Each administration is loaded
    from the file whose school year is the survey year. The later report-card
    copy is skipped by filename.
    """

    # Opt in here, in the same change that accepts the file. BaseETL skips
    # every KYRC26_* CSV until this is True.
    include_kyrc26 = True

    def accepts_data_file(self, file_path: Path) -> bool:
        """Skip the second report-card copy of an Impact Kentucky survey.

        Each administration is stored once, under the ending year of the
        school year in which it was administered. The skipped names are the
        later report card for that same survey:

        - ``teacher_working_conditions_2021.csv`` is school year ``20202021``.
          It republishes the January-February 2020 survey already loaded
          from ``teacher_working_conditions_2020.csv``.
        - ``teacher_working_conditions_2023.csv`` is school year ``20222023``.
          It republishes the November-December 2021 survey already loaded
          from ``teacher_working_conditions_2022.csv``.
        - ``KYRC25_OVW_Teacher_Working_Conditions.csv`` is school year
          ``20242025``. It republishes the November-December 2023 survey
          already loaded from ``KYRC24_OVW_Teacher_Working_Conditions.csv``.
        - ``KYRC23_OVW_Teacher_Working_Conditions.csv`` is that 2021-22
          survey again. It relabels Teaching Environment as School Climate
          and turns 945 ``N/A`` cells into 0.

        The guessed OAA names are 404 and were never registered, so they
        are not skipped here. ``TELL_EQUITY_17-18.xlsx`` and
        ``TELL_EQUITY_18-19.xlsx`` return 200. Both are school year
        ``20172018``, and their measure column is ``EQUITY_MEASURE``, not
        Impact Measure. Without an alias the parser emits nothing, so those
        names are not skipped here.
        """
        kept = _REPUBLISHED_SURVEY_FILES.get(file_path.name.upper())
        if kept is not None:
            logger.info(
                "Skipping %s; it republishes an Impact Kentucky survey already loaded from %s",
                file_path.name,
                kept,
            )
            return False
        return super().accepts_data_file(file_path)

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        """
        Map columns to standardized names.

        Handles column name variations across years:
        - 2020 and 2022: Uppercase (IMPACT MEASURE, IMPACT VALUE)
        - 2024 and 2026: Title case (Impact Measure, Impact Value)
        """
        return {
            'Impact Measure': 'impact_measure',
            'IMPACT MEASURE': 'impact_measure',
            'Impact Value': 'impact_value',
            'IMPACT VALUE': 'impact_value',
        }

    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Record a ``*`` on the percent, then let the base cleaner blank it.

        pandas 2 stores ``dtype=str`` as object, and the base cleaner turns
        ``*`` into missing before this module sees the cell. pandas 3 stores
        it as StringDtype, which that cleaner leaves alone. The mask is
        taken first, on either dtype. A blank is not a star. A published 0
        is not a star.
        """
        star = None
        if "impact_value" in df.columns:
            star = df["impact_value"].map(_is_star_text).astype(bool)
        cleaned = super().standardize_missing_values(df)
        if star is not None and bool(star.any()):
            if "suppressed" not in cleaned.columns:
                cleaned["suppressed"] = "N"
            cleaned.loc[star.to_numpy(), "suppressed"] = "Y"
        return cleaned

    # Mapping from impact measure names to metric suffixes
    IMPACT_MEASURE_MAP = {
        # KYRC24/25/26 format
        'Managing Student Behavior': 'managing_student_behavior',
        'School Climate': 'school_climate',
        'School Leadership': 'school_leadership',
        # Historical format (2020-2023) with "Composite" suffix
        'Managing Student Behavior Composite': 'managing_student_behavior',
        'School Climate Composite': 'school_climate',
        'School Leadership Composite': 'school_leadership',
        'Teaching Environment Composite': 'teaching_environment',
    }

    def _parse_impact_value(self, value: Any) -> Any:
        """Parse a percent favorable that may be text.

        ``N/A`` and a blank are missing. These files have no response count,
        so a blank percent is not turned into 0. A published ``0`` stays 0.
        A comma is removed so pandas 3 StringDtype does not drop the cell.
        No registered value contains a comma. ``bool(pd.NA)`` is not used.
        """
        if isinstance(value, str):
            cleaned = value.replace("%", "").replace(",", "").strip()
            if cleaned in _NOT_A_PERCENT or cleaned.upper() == "N/A":
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

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        """
        Extract metrics from a data row.

        Each row represents one impact measure for a school. A percent of 0
        is kept. ``N/A`` contributes no metric.
        """
        metrics: Dict[str, Any] = {}

        impact_measure = row.get('impact_measure', '')
        if not isinstance(impact_measure, str):
            if _is_missing(impact_measure):
                return metrics
            impact_measure = str(impact_measure)

        metric_suffix = self.IMPACT_MEASURE_MAP.get(impact_measure)
        if not metric_suffix:
            return metrics

        impact_value = self._parse_impact_value(row.get('impact_value', pd.NA))
        if _is_missing(impact_value):
            return metrics

        metric_name = f'teacher_working_conditions_{metric_suffix}'
        metrics[metric_name] = impact_value
        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get default metrics for suppressed records."""
        defaults: Dict[str, Any] = {}

        impact_measure = row.get('impact_measure', '')
        if not isinstance(impact_measure, str):
            return defaults
        metric_suffix = self.IMPACT_MEASURE_MAP.get(impact_measure)

        if metric_suffix:
            metric_name = f'teacher_working_conditions_{metric_suffix}'
            defaults[metric_name] = pd.NA

        return defaults

    def should_skip_row(self, row: pd.Series) -> bool:
        """Skip a construct that is not one of the published measures.

        notes/8: a row that is not suppressed and has an empty value emits
        nothing. ``N/A`` is that empty value. There is no count column to
        show that the blank is a true zero. A row marked suppressed is kept
        so the empty value can be stored with ``suppressed`` ``Y``. A
        published 0 is not skipped. These files have no demographic column,
        so the base demographic check is not used.
        """
        impact_measure = row.get('impact_measure', '')
        if not isinstance(impact_measure, str) or impact_measure not in self.IMPACT_MEASURE_MAP:
            return True

        suppressed = row.get('suppressed', 'N')
        if isinstance(suppressed, str) and suppressed == 'Y':
            return False
        # A star is stored as NA with suppressed Y. It is not a blank.
        if _is_star_text(row.get('impact_value', pd.NA)):
            return False

        impact_value = self._parse_impact_value(row.get('impact_value', pd.NA))
        if _is_missing(impact_value):
            return True
        return False

    def create_kpi_template(self, row: pd.Series, source_file: str) -> Dict[str, Any]:
        """
        Override to set student_group to 'All Students' since this is school-level data.
        """
        from datetime import datetime

        # Extract school identification
        school_id = self.extract_school_id(row)
        year = self.extract_year(row, source_file)

        # Check if record is suppressed. A missing flag is N, not a boolean test of NA.
        # A privacy star is suppressed even when the flag column is absent.
        suppressed_flag = row.get('suppressed', 'N')
        is_suppressed = (
            (isinstance(suppressed_flag, str) and suppressed_flag == 'Y')
            or _is_star_text(row.get('impact_value', pd.NA))
        )

        return {
            'district': row.get('district_name', 'Unknown District'),
            'school_id': school_id,
            'school_name': self.standardize_school_name(row.get('school_name', 'Unknown School')),
            'year': year,
            'student_group': 'All Students',  # School-level data only
            'county_number': row.get('county_number', pd.NA),
            'county_name': row.get('county_name', pd.NA),
            'district_number': row.get('district_number', pd.NA),
            'school_code': row.get('school_code', pd.NA),
            'state_school_id': row.get('state_school_id', pd.NA),
            'nces_id': row.get('nces_id', pd.NA),
            'co_op': row.get('co_op', pd.NA),
            'co_op_code': row.get('co_op_code', pd.NA),
            'school_type': row.get('school_type', pd.NA),
            'suppressed': 'Y' if is_suppressed else 'N',
            'source_file': source_file,
            'last_updated': datetime.now().isoformat()
        }

    def convert_to_kpi_format(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """Convert one file, then leave one row per KPI key.

        Glasgow Independent ``197`` publishes two district-total rows for the
        same construct. When those percents agree, one row is kept. When
        they disagree, neither is kept: both numbers are published, and this
        parser does not choose between them.
        """
        kpi_df = super().convert_to_kpi_format(df, source_file)
        return self._collapse_duplicate_keys(kpi_df, source_file)

    def _collapse_duplicate_keys(self, kpi_df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """Keep one row when a key is repeated with the same percent.

        A conflicting pair is dropped and logged. The key is school, year,
        student group, and metric.
        """
        if kpi_df.empty:
            return kpi_df
        key = ["school_id", "year", "student_group", "metric"]
        repeated = kpi_df.duplicated(subset=key, keep=False)
        if not bool(repeated.any()):
            return kpi_df

        unique = kpi_df.loc[~repeated]
        kept_same: list[pd.DataFrame] = []
        for _, group in kpi_df.loc[repeated].groupby(key, dropna=False, sort=False):
            distinct: set[float | None] = set()
            for raw_value in group["value"].tolist():
                if _is_missing(raw_value):
                    distinct.add(None)
                    continue
                try:
                    distinct.add(round(float(raw_value), 6))
                except (TypeError, ValueError):
                    distinct.add(None)
            first = group.iloc[0]
            if len(distinct) == 1:
                kept_same.append(group.iloc[[0]])
                continue
            logger.warning(
                "Dropping conflicting duplicate key in %s: school %s year %s metric %s values %s",
                source_file,
                first["school_id"],
                first["year"],
                first["metric"],
                sorted(distinct, key=lambda item: (item is None, item if item is not None else 0)),
            )
        if not kept_same:
            return unique.reset_index(drop=True)
        return pd.concat([unique, *kept_same], ignore_index=True)


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Read teacher working conditions files, normalize, and convert to KPI format."""
    etl = TeacherWorkingConditionsETL('teacher_working_conditions')
    etl.process(raw_dir, proc_dir, cfg)


def main():
    """Run teacher working conditions ETL process."""
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
