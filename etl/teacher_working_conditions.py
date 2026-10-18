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
Environment, with a "Composite" suffix. KYRC24, KYRC25, and KYRC26 publish
Managing Student Behavior, School Climate, and School Leadership. Those
labels stay on separate metrics. The files do not say that School Climate
replaced Teaching Environment.

KYRC26 uses the same headers as KYRC25. No temporary OAA extract is
registered, and the guessed temporary names return 404, so there is no
leftover temporary filename to skip. ``KYRC23_OVW_Teacher_Working_Conditions.csv``
returns 200 and republishes 2022-23. It is not registered, and
``accepts_data_file`` skips that name. ``teacher_working_conditions_2020.csv``
is the 2019-20 report card and is registered.
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
# Teaching Environment is labeled School Climate, and N/A cells are stored as 0.
_KYRC23_REPUBLISH = "KYRC23_OVW_TEACHER_WORKING_CONDITIONS.CSV"

# Text that is not a percent. These files have no count column, so N/A is
# not a zero. A published 0 is a real percent and is not in this set.
_NOT_A_PERCENT = {"", "N/A", "NA", "n/a", "*", "**", "---", "--", "<10"}


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

    KYRC26 uses the same headers as KYRC24 and KYRC25. The 2020-2023 files
    use the uppercase names, which are already mapped. No temporary OAA
    extract is registered for this directory.
    """

    # Opt in here, in the same change that accepts the file. BaseETL skips
    # every KYRC26_* CSV until this is True.
    include_kyrc26 = True

    def accepts_data_file(self, file_path: Path) -> bool:
        """Skip the KYRC23 republish of the 2022-23 extract.

        ``KYRC23_OVW_Teacher_Working_Conditions.csv`` returns 200 on the
        datasets host. School year is ``20222023`` on every row, and the
        school-code set matches ``teacher_working_conditions_2023.csv``.
        The historical file labels the third construct Teaching Environment
        and stores an insufficient result as ``N/A``. KYRC23 labels that
        construct School Climate and stores those cells as 0. Loading both
        files would write 2023 twice. The guessed OAA names are 404 and
        were never registered, so they are not skipped here.

        ``TELL_EQUITY_17-18.xlsx`` and ``TELL_EQUITY_18-19.xlsx`` return
        200. Both are school year ``20172018``, and their measure column
        is ``EQUITY_MEASURE``, not Impact Measure. Without an alias the
        parser emits nothing, so those names are not skipped here.
        """
        if file_path.name.upper() == _KYRC23_REPUBLISH:
            logger.info(
                "Skipping %s; it republishes 2022-23 already loaded from "
                "teacher_working_conditions_2023.csv",
                file_path.name,
            )
            return False
        return super().accepts_data_file(file_path)

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        """
        Map columns to standardized names.

        Handles column name variations across years:
        - 2020-2023: Uppercase (IMPACT MEASURE, IMPACT VALUE)
        - 2024+: Title case (Impact Measure, Impact Value)
        """
        return {
            'Impact Measure': 'impact_measure',
            'IMPACT MEASURE': 'impact_measure',
            'Impact Value': 'impact_value',
            'IMPACT VALUE': 'impact_value',
        }

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
        suppressed_flag = row.get('suppressed', 'N')
        is_suppressed = isinstance(suppressed_flag, str) and suppressed_flag == 'Y'

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
    ).dict()

    transform(raw_dir, proc_dir, test_config)


if __name__ == "__main__":
    main()
