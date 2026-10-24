"""
Dual Credit ETL Module

Handles Kentucky dual credit program data.
Normalizes column names, standardizes missing values, and transforms to KPI format.

KDE publishes two different measures, and this module keeps them apart:

- Participation and performance is an unduplicated student count by
  demographic. A student who takes more than one dual credit course counts
  once. Completers finished at least one course. Qualifying grades are a
  grade of C or better in at least one course.
- Courses offered is a duplicated count of course enrollments by subject.
  A student in two courses counts twice. Those rows use
  ``dual_credit_course_*`` metric names so they do not collide with the
  unduplicated student metrics.

There is no Suppressed column. A ``*`` on a measure cell is stored as
value NA with suppressed Y. A blank cell is not a star and is not stored.

The pipeline does not emit ``has_dual_credit_program``. That flag was 1
whenever enrollment was positive and suppressed Y when enrollment was
starred. No published enrollment is 0, and a school without a program has
no row, so the flag was never 0. Scott Horn dropped it on 2026-10-20. Do
not restore it and do not add an alias. Enrollment, completion,
qualifying-score rates, and the course-level rates stay.
"""
from pathlib import Path
import re
import pandas as pd
from typing import Any, Dict, List, Optional
import logging

import sys

# Add etl directory to path for imports
etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from constants import KPI_COLUMNS
from base_etl import BaseETL, Config

logger = logging.getLogger(__name__)

_MEASURE_COLUMNS = (
    "course_enrollment",
    "course_completers",
    "qualifying_score",
)

_NOT_A_NUMBER = {"", "*", "**", "---", "--", "<10", '""', "nan", "None", "<NA>"}

# Participation file: unduplicated students. The qualifying column is a
# grade of C or better. The rate is derived here; KDE does not publish it.
_STUDENT_METRICS = {
    "course_enrollment": "dual_credit_enrollment",
    "course_completers": "dual_credit_completion_count",
    "qualifying_score": "dual_credit_qualifying_score_count",
}

# Courses-offered file: duplicated course enrollments. The subject is part
# of the metric name. ``{slug}`` is filled in per row.
_COURSE_METRIC_TEMPLATES = {
    "course_enrollment": "dual_credit_course_enrollment_{slug}",
    "course_completers": "dual_credit_course_completion_count_{slug}",
    "qualifying_score": "dual_credit_course_qualifying_score_count_{slug}",
}


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


def _is_string_like(series: pd.Series) -> bool:
    """True for object text and for pandas StringDtype.

    ``read_csv(..., dtype=str)`` on pandas 3 returns StringDtype, not object.
    A comma in ``55,553`` has to be removed before ``to_numeric``. pandas
    2.3.3 still returns object. pandas is unpinned.
    """
    return series.dtype == object or bool(pd.api.types.is_string_dtype(series))


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
    """True when this measure cell was a ``*``.

    ``standardize_missing_values`` records the position before it turns
    the star into NA. A caller that still has the raw ``*`` is recognized
    from the cell text.
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
    is not turned into 0. ``bool(pd.NA)`` is not used.
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


def clean_numeric_with_commas(series: pd.Series) -> pd.Series:
    """Convert strings with commas to numeric values.

    Object and StringDtype both go through ``astype(str)``. ``*`` and a
    blank become missing. A published 0 stays 0.
    """
    cleaned = series.astype(str).str.replace(",", "", regex=False).str.replace('"', "", regex=False)
    cleaned = cleaned.str.strip()
    missing = cleaned.isin(list(_NOT_A_NUMBER)) | cleaned.str.upper().isin(["N/A", "#N/A", "NA", "NAN"])
    cleaned = cleaned.mask(missing, pd.NA)
    return pd.to_numeric(cleaned, errors="coerce")


def subject_slug(subject: Any) -> str:
    """Turn a courses-offered subject into a metric suffix.

    ``Arts & Humanities`` becomes ``arts_and_humanities``. ``English/Language
    Arts`` becomes ``english_language_arts``. The slug is lowercase and uses
    underscores, which is the metric naming convention.
    """
    text = "" if _is_missing(subject) else str(subject).strip().lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def _clean_measure_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Strip commas before a numeric cast.

    ``process`` casts ``config/mappings.yaml`` dtypes after
    ``standardize_missing_values``. This source has no dtype cast. The
    cleaner still runs so an Int64 cast of ``55,553`` does not become
    missing. Object and StringDtype are both cleaned. A published 0 is
    left as 0. ``*`` becomes missing after the star flag is recorded.
    """
    for col in _MEASURE_COLUMNS:
        if col not in df.columns:
            continue
        series = df[col]
        if not _is_string_like(series):
            continue
        cleaned = series.astype("string").str.replace(",", "", regex=False).str.replace('"', "", regex=False).str.strip()
        missing = cleaned.isna() | cleaned.isin(list(_NOT_A_NUMBER))
        upper = cleaned.str.upper()
        missing = missing | upper.isin(["N/A", "#N/A", "NA", "NAN"])
        df[col] = cleaned.mask(missing, pd.NA)
    return df


class DualCreditETL(BaseETL):
    """ETL module for processing dual credit program data.

    KYRC26 participation headers match KYRC25. The courses-offered header
    ``Couse Completers`` (KYRC23 and KYRC25, and the datasets KYRC24 copy)
    and ``Couse Completers `` (the registered historical KYRC24 file, with
    a trailing space) both map to course completers. KYRC26 spells the
    header ``Course Completers``. No temporary OAA extract is registered
    for this directory.
    """

    # Opt in here, in the same change that accepts the file. BaseETL skips
    # every KYRC26_* CSV until this is True.
    include_kyrc26 = True

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            # Course offering fields
            'Advanced Course Type': 'course_type',
            'ADVANCED COURSE TYPE': 'course_type',
            'Subject': 'subject',
            'SUBJECT': 'subject',

            # Enrollment and performance metrics
            'Course Enrollment': 'course_enrollment',
            'COURSE ENROLLMENT': 'course_enrollment',
            'Course Completers': 'course_completers',
            'COURSE COMPLETERS': 'course_completers',
            # KYRC23 and KYRC25 courses offered, and the datasets KYRC24
            # copy. No trailing space.
            'Couse Completers': 'course_completers',
            # Registered historical KYRC24 courses offered. Trailing space.
            'Couse Completers ': 'course_completers',
            'COUSE COMPLETERS ': 'course_completers',
            'Students With Qualifying Score': 'qualifying_score',
            'STUDENTS WITH QUALIFYING SCORE': 'qualifying_score',
            'Students With Qualifying Score ': 'qualifying_score',
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
            for column in _MEASURE_COLUMNS
            if column in df.columns
        }
        df = super().standardize_missing_values(df)
        df = _clean_measure_columns(df)
        for column, mask in star_columns.items():
            df[_star_column_name(column)] = mask.to_numpy()
        return df

    def _row_has_measure(self, row: pd.Series) -> bool:
        """True when any measure cell is a number or a star.

        A blank is neither. A row of blanks is not offered and is skipped.
        """
        for name in _MEASURE_COLUMNS:
            if _metric_was_starred(row, name):
                return True
            if name in row.index and not _is_missing(_parse_number(row.get(name, pd.NA))):
                return True
        return False

    def should_skip_row(self, row: pd.Series) -> bool:
        """
        Skip a row that has no measure to store.

        Participation rows have a demographic. A missing demographic is
        skipped. Courses-offered rows have a subject and no demographic.
        Those rows are not skipped for a missing demographic. A blank
        measure cell is not a star. A row whose enrollment, completers,
        and qualifying cells are all blank is not offered and is skipped
        in every year. These files have no such row. A ``*`` is a measure
        and is not skipped.
        """
        if "demographic" in row.index:
            if _is_missing(row.get("demographic")):
                return True
            if super().should_skip_row(row):
                return True
        else:
            # Courses offered. A missing subject has no metric name.
            if "subject" not in row.index or _is_missing(row.get("subject")):
                return True
            if subject_slug(row.get("subject")) == "":
                return True

        return not self._row_has_measure(row)

    def _collapse_repeated_keys(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """Keep one row when a file repeats a school key.

        Participation keys are school code plus demographic. Courses-offered
        keys are school code plus subject. 2023-2026 do not repeat either
        key, so this rule does not drop a registered row. A number beats a
        ``*``, and a ``*`` beats a blank. When the enrollment counts tie,
        file order keeps the first row. The rule exists so a repeated key
        cannot be written twice.
        """
        if df.empty or "school_code" not in df.columns:
            return df
        # Participation rows are distinguished by demographic. Courses
        # offered has no demographic column, so the subject is the key.
        # A file that carries both columns is participation: the subject
        # does not vary, and keying on it would collapse the groups.
        if "demographic" in df.columns:
            key_name = "demographic"
        elif "subject" in df.columns:
            key_name = "subject"
        else:
            key_name = ""
        if key_name == "":
            return df
        code = df["school_code"].astype("string").str.strip()
        group = df[key_name].astype("string").str.strip()
        key = code + "|" + group
        counts = key.value_counts(dropna=False)
        repeated = set(counts[counts > 1].index)
        if not repeated:
            return df

        work = df.copy()
        work["_key"] = key
        work["_orig"] = range(len(work))
        enrollment = work["course_enrollment"] if "course_enrollment" in work.columns else pd.Series(pd.NA, index=work.index)
        work["_enroll_sort"] = pd.to_numeric(enrollment, errors="coerce")
        work["_enroll_star"] = work.apply(
            lambda row: 1 if _metric_was_starred(row, "course_enrollment") else 0,
            axis=1,
        ).astype(int)
        repeated_rows = work["_key"].isin(repeated)
        kept_unique = work.loc[~repeated_rows]
        candidates = work.loc[repeated_rows].sort_values(
            ["_key", "_enroll_sort", "_enroll_star", "_orig"],
            ascending=[True, False, False, True],
            na_position="last",
        )
        kept_repeated = candidates.drop_duplicates("_key", keep="first")
        dropped = int(repeated_rows.sum() - len(kept_repeated))
        logger.info(
            "Kept the larger enrollment copy in %s; dropped %s extra rows",
            Path(source_file).name,
            dropped,
        )
        combined = pd.concat([kept_unique, kept_repeated], ignore_index=False)
        combined = combined.sort_values("_orig")
        return combined.drop(columns=["_key", "_orig", "_enroll_sort", "_enroll_star"])

    def _metric_names(self, row: pd.Series) -> Optional[Dict[str, str]]:
        """Map the three source columns to KPI names for this row.

        A subject row is the courses-offered file. The subject is part of
        the metric name. A demographic row is the participation file and
        keeps the unduplicated student metric names.
        """
        if self._is_course_row(row):
            slug = subject_slug(row.get("subject"))
            if slug == "":
                return None
            return {column: template.format(slug=slug) for column, template in _COURSE_METRIC_TEMPLATES.items()}
        return dict(_STUDENT_METRICS)

    def _is_course_row(self, row: pd.Series) -> bool:
        """True for a courses-offered row.

        Those rows have a subject and no demographic. A participation row
        has a demographic. A test row that carries both is participation.
        """
        if "demographic" in row.index and not _is_missing(row.get("demographic")):
            return False
        return "subject" in row.index and not _is_missing(row.get("subject"))

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        """
        Extract dual credit metrics from a data row.

        Participation rows (no subject) return the unduplicated student
        metrics. Courses-offered rows return duplicated course counts with
        the subject in the metric name.

        A ``*`` cell is included as NA. ``convert_to_kpi_format`` writes
        that metric with ``suppressed`` ``Y``. A blank cell is omitted.
        A completion rate is completers divided by enrollment. A qualifying
        rate is qualifying grades divided by completers. Both rates are
        derived. A rate is omitted when either count is a star or a blank,
        because the source did not publish a rate to suppress. A published
        0 enrollment does not produce a rate.
        """
        names = self._metric_names(row)
        if names is None:
            return {}
        metrics: Dict[str, Any] = {}
        parsed: Dict[str, Any] = {}
        for column, metric_name in names.items():
            if column not in row.index and not _metric_was_starred(row, column):
                parsed[column] = pd.NA
                continue
            if _metric_was_starred(row, column):
                metrics[metric_name] = pd.NA
                parsed[column] = pd.NA
                continue
            number = _parse_number(row.get(column, pd.NA))
            parsed[column] = number
            if _is_missing(number) or number < 0:
                continue
            metrics[metric_name] = int(number)

        enrollment = parsed.get("course_enrollment", pd.NA)
        completers = parsed.get("course_completers", pd.NA)
        qualifying = parsed.get("qualifying_score", pd.NA)
        if self._is_course_row(row):
            rate_completion = "dual_credit_course_completion_rate_" + subject_slug(row.get("subject"))
            rate_qualifying = "dual_credit_course_qualifying_score_rate_" + subject_slug(row.get("subject"))
        else:
            rate_completion = "dual_credit_completion_rate"
            rate_qualifying = "dual_credit_qualifying_score_rate"

        if not _is_missing(enrollment) and not _is_missing(completers) and enrollment > 0 and completers >= 0:
            metrics[rate_completion] = round((completers / enrollment) * 100, 1)
        if not _is_missing(completers) and not _is_missing(qualifying) and completers > 0 and qualifying >= 0:
            metrics[rate_qualifying] = round((qualifying / completers) * 100, 1)
        return metrics

    def convert_to_kpi_format(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """Write a published cell as ``N`` and a ``*`` cell as ``Y``.

        A blank cell is not a row. A fully starred participation row writes
        the three counts as suppressed. ``has_dual_credit_program`` is not
        emitted.
        Courses-offered subjects do not share a metric with the
        participation file, so All Enrollments (143,848 in 2026) is not
        written on top of All Students (55,553 in 2026).
        """
        df = self._collapse_repeated_keys(df, source_file)
        kpi_rows: List[Dict[str, Any]] = []
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
        """Get default metrics for a fully suppressed participation row."""
        names = self._metric_names(row) or dict(_STUDENT_METRICS)
        defaults = {metric_name: pd.NA for metric_name in names.values()}
        if not self._is_course_row(row):
            defaults["dual_credit_completion_rate"] = pd.NA
            defaults["dual_credit_qualifying_score_rate"] = pd.NA
        return defaults


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Process dual credit data and convert to KPI format using BaseETL."""
    etl = DualCreditETL('dual_credit')
    etl.process(raw_dir, proc_dir, cfg)


if __name__ == "__main__":
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
