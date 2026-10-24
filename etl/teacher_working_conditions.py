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
  Kept file: the datasets copy of
  ``KYRC24_OVW_Teacher_Working_Conditions.csv`` (``20232024``). The
  historical file of that name starts at district 101. It is downloaded
  under ``KYRC24_OVW_Teacher_Working_Conditions_historical.csv`` and
  contributes only Craft Academy ``610000`` and ``610300``, the codes the
  datasets file does not publish. ``KYRC25_OVW_Teacher_Working_Conditions.csv``
  is the same survey with school year ``20242025``. It contributes only
  Mason County Career Magnet ``777135``, stored as KPI year 2024.
- November 10-December 19, 2025, school year 2025-26, KPI year 2026.
  Kept file: ``KYRC26_OVW_Teacher_Working_Conditions.csv`` (``20252026``).

``accepts_data_file`` skips the other report-card copy by exact filename:
``teacher_working_conditions_2021.csv``, ``teacher_working_conditions_2023.csv``,
and ``KYRC23_OVW_Teacher_Working_Conditions.csv``. KYRC23 also turns 945
``N/A`` cells into 0. None of those three names is registered. KYRC25 is
registered and is not a second KPI year: only keys the datasets KYRC24 file
does not already supply are stored, under 2024.

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
    "KYRC23_OVW_TEACHER_WORKING_CONDITIONS.CSV": "teacher_working_conditions_2022.csv",
}

# Datasets KYRC24 is the 2024 source. Its school year is 20232024, and its
# percents match KYRC25 on every shared key. The historical file of the same
# remote name is truncated and disagrees on nine district totals. It is
# stored locally under another name so the download does not overwrite the
# datasets file. A supplement row is kept only when that school-and-metric
# key is absent from the datasets file (issue #95).
_PRIMARY_2024_FILE = "KYRC24_OVW_Teacher_Working_Conditions.csv"
_HISTORICAL_2024_SUPPLEMENT = "KYRC24_OVW_Teacher_Working_Conditions_historical.csv"
_KYRC25_SUPPLEMENT = "KYRC25_OVW_Teacher_Working_Conditions.csv"
# KYRC25's school-year cell is 20242025. The three 777135 cells are the
# 2023-24 administration, so they are stored as 2024.
_ADMINISTRATION_YEAR_2024 = "20232024"
_SUPPLEMENT_2024_FILES = {
    _HISTORICAL_2024_SUPPLEMENT.upper(),
    _KYRC25_SUPPLEMENT.upper(),
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
    from the file whose school year is the survey year. The 2021, 2023, and
    KYRC23 copies are skipped by filename. KYRC25 and the historical KYRC24
    file add only keys the datasets KYRC24 file does not already supply.
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
        - ``KYRC23_OVW_Teacher_Working_Conditions.csv`` is that 2021-22
          survey again. It relabels Teaching Environment as School Climate
          and turns 945 ``N/A`` cells into 0.

        ``KYRC25_OVW_Teacher_Working_Conditions.csv`` is accepted. It is the
        2023-24 survey again. ``convert_to_kpi_format`` keeps a row only when
        the datasets KYRC24 file does not already supply that school and
        metric, and stores the kept rows as year 2024. The historical KYRC24
        download uses the same rule.

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

    def process(self, raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
        """Index the datasets KYRC24 keys, then run the shared writer.

        The index is the set of school-and-metric keys that file already
        supplies. Supplement files add a row only when its key is absent.
        """
        self._primary_2024_loaded = False
        self._primary_2024_keys = set()
        source_dir = raw_dir / self.source_name
        primary = source_dir / _PRIMARY_2024_FILE
        if (
            source_dir.exists()
            and primary.is_file()
            and primary.stat().st_size > 0
            and self.accepts_data_file(primary)
        ):
            try:
                self._primary_2024_keys = self._index_primary_2024(primary)
                self._primary_2024_loaded = True
            except Exception as exc:
                logger.error("Could not index datasets KYRC24 keys from %s: %s", primary.name, exc)
        super().process(raw_dir, proc_dir, cfg)

    def _index_primary_2024(self, path: Path) -> set[tuple[str, str]]:
        """Return (school id, metric) keys the datasets KYRC24 file supplies."""
        frame = self._read_data_file(path, None)
        frame = self.normalize_column_names(frame)
        frame = self.standardize_missing_values(frame)
        keys: set[tuple[str, str]] = set()
        for _, row in frame.iterrows():
            if self.should_skip_row(row):
                continue
            try:
                school_id = self.extract_school_id(row)
            except ValueError:
                continue
            for metric in self._key_metrics(row):
                keys.add((school_id, metric))
        return keys

    def _key_metrics(self, row: pd.Series) -> list[str]:
        """Metric names this row would write, including a suppressed star."""
        names = list(self.extract_metrics(row))
        if names:
            return names
        suppressed = row.get("suppressed", "N")
        starred = _is_star_text(row.get("impact_value", pd.NA))
        if starred or (isinstance(suppressed, str) and suppressed == "Y"):
            return list(self.get_suppressed_metric_defaults(row))
        return []

    def _rows_absent_from_primary(self, df: pd.DataFrame) -> pd.DataFrame:
        """Keep source rows whose school-and-metric key the datasets file lacks."""
        keep: list[bool] = []
        for _, row in df.iterrows():
            if self.should_skip_row(row):
                keep.append(False)
                continue
            try:
                school_id = self.extract_school_id(row)
            except ValueError:
                keep.append(False)
                continue
            metrics = self._key_metrics(row)
            if not metrics:
                keep.append(False)
                continue
            keep.append(any((school_id, metric) not in self._primary_2024_keys for metric in metrics))
        if not keep:
            return df.iloc[0:0].copy()
        return df.loc[pd.Series(keep, index=df.index)].copy()

    def convert_to_kpi_format(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """Convert one file, then leave one row per KPI key.

        Glasgow Independent ``197`` publishes two district-total rows for the
        same construct. When those percents agree, one row is kept. When
        they disagree, one row is stored with value NA and ``suppressed``
        ``Y``. Both published numbers stay in notes/105.md. Issue #108.

        The historical KYRC24 file and KYRC25 add only keys the datasets
        KYRC24 file does not already supply. A KYRC25 row that is kept is
        the 2023-24 administration, so its school year is stored as 2024.
        The source filename stays on the row.
        """
        name = Path(source_file).name
        frame = df
        if name.upper() in _SUPPLEMENT_2024_FILES:
            if not getattr(self, "_primary_2024_loaded", False):
                logger.info(
                    "Skipping %s; %s is not in this batch, so its keys are not supplemented",
                    name,
                    _PRIMARY_2024_FILE,
                )
                return pd.DataFrame()
            frame = self._rows_absent_from_primary(df)
            if frame.empty:
                logger.info(
                    "%s adds no school-and-metric key that %s does not already supply",
                    name,
                    _PRIMARY_2024_FILE,
                )
                return pd.DataFrame()
            if name.upper() == _KYRC25_SUPPLEMENT.upper():
                frame["school_year"] = _ADMINISTRATION_YEAR_2024
            added = sorted({self.extract_school_id(row) for _, row in frame.iterrows()})
            logger.info(
                "%s supplies %s rows for codes absent from %s: %s",
                name,
                len(frame),
                _PRIMARY_2024_FILE,
                ", ".join(added),
            )
        kpi_df = super().convert_to_kpi_format(frame, source_file)
        return self._collapse_duplicate_keys(kpi_df, source_file)

    def _collapse_duplicate_keys(self, kpi_df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """Keep one row when a key is repeated.

        Agreeing percents stay as published. A conflicting pair is stored
        once, with value NA and ``suppressed`` ``Y``, because both numbers
        are published and this parser does not choose. The key is school,
        year, student group, and metric. Issue #108.
        """
        if kpi_df.empty:
            return kpi_df
        key = ["school_id", "year", "student_group", "metric"]
        repeated = kpi_df.duplicated(subset=key, keep=False)
        if not bool(repeated.any()):
            return kpi_df

        keep_index: list[Any] = list(kpi_df.index[~repeated])
        conflict_index: list[Any] = []
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
            first_index = group.index[0]
            keep_index.append(first_index)
            if len(distinct) == 1:
                continue
            conflict_index.append(first_index)
            logger.warning(
                "Storing conflicting duplicate key in %s as suppressed: school %s year %s metric %s values %s",
                source_file,
                group.iloc[0]["school_id"],
                group.iloc[0]["year"],
                group.iloc[0]["metric"],
                sorted(distinct, key=lambda item: (item is None, item if item is not None else 0)),
            )
        collapsed = kpi_df.loc[keep_index].copy()
        if conflict_index:
            # A float column cannot hold pd.NA. Object does, on pandas 2 and 3.
            collapsed["value"] = collapsed["value"].astype(object)
            collapsed.loc[conflict_index, "value"] = pd.NA
            collapsed.loc[conflict_index, "suppressed"] = "Y"
        return collapsed.reset_index(drop=True)


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
