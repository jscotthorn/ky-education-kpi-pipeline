"""
School Courses ETL Module

KDE's school-courses summary is specialized course enrollment, not the full
catalog. The 2018-19 coversheet defines the categories as career and
technical education, health and physical education, visual and performing
arts, and world languages. ``Total_Students`` is the number of students
enrolled in that course. There is no demographic column and no state row.

One row is stored per school, year, and student group. ``courses_offered_count``
is the number of course rows at that school. A starred enrollment total
counts. A blank total is not stored and is not counted. A published 0 is a
course row, so it counts. No registered file has a 0 or a blank total.
Every count metric is stored with ``suppressed`` N, including a suppressed
count of 0. Grade cells are not summed into a replacement total.

The category comes from KDE's own column (``Course Category`` /
``COURSECATEGORY``). The 2019 coversheet and that column use ``CTE``,
``HPE``, ``VPA``, and ``WL``. Files from 2020 through 2026 spell health
and physical education ``HEPE``. Both labels map to ``_hpe``. A label that
is not one of those, including a blank, is ``courses_offered_count_other``.
The state course code is not used: visual arts and health/PE both use
prefix 70. The four named categories plus ``_other`` sum to
``courses_offered_count``.

In 2020-2026 the smallest published enrollment is 3, so a starred total in
those years can also mean 0-2 or not applicable. 2019 publishes 1s and has
no stars. The 2019, 2021, and 2023 values used to be 1 per course. They are
now the school counts.

``algebra_1_grade_8_count`` and ``has_algebra_1_access`` are still emitted
per course, and only when the course name contains ``Algebra I`` or
``Algebra 1``. No registered file has such a course.
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

_COUNT_COLUMNS = (
    "student_count",
    "preschool_count",
    "kindergarten_count",
    "grade1_count",
    "grade2_count",
    "grade3_count",
    "grade4_count",
    "grade5_count",
    "grade6_count",
    "grade7_count",
    "grade8_count",
    "grade9_count",
    "grade10_count",
    "grade11_count",
    "grade12_count",
    "grade14_count",
)

_NOT_A_NUMBER = {"", "*", "**", "---", "--", "<10", '""', "nan", "None", "<NA>"}

# KYRC23 repeats school year 20222023. The registered copy is
# school_courses_summary_2023.csv. A leftover KYRC23 file would write
# that year twice.
_REPUBLISH_FILENAMES = {
    "KYRC23_EDOP_SCHOOL_COURSES_SUMMARY.CSV",
}

# KDE publishes the category. 2019 uses HPE. 2020-2026 use HEPE for the
# same health and physical education group. Anything else is other.
_CATEGORY_TO_BUCKET = {
    "CTE": "cte",
    "HPE": "hpe",
    "HEPE": "hpe",
    "VPA": "vpa",
    "WL": "wl",
}

_OFFERED_METRICS = (
    "courses_offered_count",
    "courses_offered_suppressed_count",
    "courses_offered_count_cte",
    "courses_offered_count_hpe",
    "courses_offered_count_vpa",
    "courses_offered_count_wl",
    "courses_offered_count_other",
)

_IDENTITY_FIELDS = (
    "district",
    "school_name",
    "county_number",
    "county_name",
    "district_number",
    "school_code",
    "state_school_id",
    "nces_id",
    "co_op",
    "co_op_code",
    "school_type",
)


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

    ``read_csv(..., dtype=str)`` on pandas 3 returns StringDtype, not
    object. A comma in ``1,284`` has to be removed before ``to_numeric``.
    pandas 2.3.3 still returns object. pandas is unpinned.
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
    """True when this count cell was a ``*``.

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


def _clean_count_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Strip commas before a numeric cast.

    ``process`` casts ``config/mappings.yaml`` dtypes after
    ``standardize_missing_values``. This source has no dtype cast. The
    cleaner still runs so an Int64 cast of ``1,284`` does not become
    missing. Object and StringDtype are both cleaned. A published 0 is
    left as 0. ``*`` becomes missing after the star flag is recorded.
    """
    for col in _COUNT_COLUMNS:
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


def _is_algebra_1(course_name: Any) -> bool:
    """True when the state course name is Algebra 1."""
    if _is_missing(course_name):
        return False
    text = str(course_name).upper()
    return "ALGEBRA I" in text or "ALGEBRA 1" in text


def _category_bucket(value: Any) -> Tuple[str, Optional[str]]:
    """Map KDE's category label to a metric suffix.

    Returns ``(bucket, unknown_label)``. A blank is ``other`` with no
    label to report. A label outside CTE / HPE / HEPE / VPA / WL is
    ``other`` and the label is returned so the caller can report it.
    The course code is not consulted.
    """
    if _is_missing(value):
        return "other", None
    label = str(value).strip().upper()
    if label == "":
        return "other", None
    bucket = _CATEGORY_TO_BUCKET.get(label)
    if bucket is None:
        return "other", label
    return bucket, None


def _identity_text(value: Any) -> str:
    """Comparable text for a school identifier. Missing is empty."""
    if _is_missing(value):
        return ""
    return str(value).strip()


def _merge_identity(
    kept: Dict[str, Any],
    incoming: Dict[str, Any],
    key: Tuple[str, str, str],
    seen_conflicts: set,
) -> None:
    """Fill a blank identifier and keep the first value when they differ."""
    for field in _IDENTITY_FIELDS:
        current = _identity_text(kept.get(field))
        nxt = _identity_text(incoming.get(field))
        if not current and nxt:
            kept[field] = incoming.get(field)
            continue
        if current and nxt and current != nxt:
            token = (key, field)
            if token in seen_conflicts:
                continue
            seen_conflicts.add(token)
            logger.warning(
                "School %s year %s group %s has disagreeing %s values %r and %r; keeping the first",
                key[0],
                key[1],
                key[2],
                field,
                current,
                nxt,
            )


def _empty_tally() -> Dict[str, int]:
    """Zero counts for the seven offered-course metrics."""
    return {metric: 0 for metric in _OFFERED_METRICS}


class SchoolCoursesETL(BaseETL):
    """ETL module for specialized school course offerings.

    KYRC25 and KYRC26 headers match each other. They name the enrollment
    total ``All Students`` and the grades ``Grade 8``, not the historical
    ``STUDENT COUNT`` and ``GRADE8 COUNT``. Those aliases are mapped here.
    There is no Suppressed column. No temporary OAA extract was registered.
    """

    # Opt in here, in the same change that accepts the file. BaseETL skips
    # every KYRC26_* CSV until this is True.
    include_kyrc26 = True

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            # Course identification
            'Course Category': 'course_category',
            'COURSE CATEGORY': 'course_category',
            'COURSECATEGORY': 'course_category',
            'State Course Code': 'course_code',
            'STATE COURSE CODE': 'course_code',
            'STATECOURSECODE': 'course_code',
            'State Course Name': 'course_name',
            'STATE COURSE NAME': 'course_name',
            'STATECOURSENAME': 'course_name',

            # Enrollment total. KYRC23-26 call this All Students. Historical
            # CSVs call it STUDENT COUNT. The 2018-19 workbook calls it
            # TOTAL_STUDENTS.
            'Student Count': 'student_count',
            'STUDENT COUNT': 'student_count',
            'All Students': 'student_count',
            'TOTAL_STUDENTS': 'student_count',

            # Grade counts. KYRC files use "Grade 8". Historical CSVs use
            # "GRADE8 COUNT". The 2018-19 workbook uses G8.
            'Preschool Count': 'preschool_count',
            'PRESCHOOL COUNT': 'preschool_count',
            'Preschool': 'preschool_count',
            'Kindergarten Count': 'kindergarten_count',
            'KINDERGARTEN COUNT': 'kindergarten_count',
            'Kindergarten': 'kindergarten_count',
            'Grade1 Count': 'grade1_count',
            'GRADE1 COUNT': 'grade1_count',
            'Grade 1': 'grade1_count',
            'Grade2 Count': 'grade2_count',
            'GRADE2 COUNT': 'grade2_count',
            'Grade 2': 'grade2_count',
            'Grade3 Count': 'grade3_count',
            'GRADE3 COUNT': 'grade3_count',
            'Grade 3': 'grade3_count',
            'Grade4 Count': 'grade4_count',
            'GRADE4 COUNT': 'grade4_count',
            'Grade 4': 'grade4_count',
            'Grade5 Count': 'grade5_count',
            'GRADE5 COUNT': 'grade5_count',
            'Grade 5': 'grade5_count',
            'Grade6 Count': 'grade6_count',
            'GRADE6 COUNT': 'grade6_count',
            'Grade 6': 'grade6_count',
            'Grade7 Count': 'grade7_count',
            'GRADE7 COUNT': 'grade7_count',
            'Grade 7': 'grade7_count',
            'Grade8 Count': 'grade8_count',
            'GRADE8 COUNT': 'grade8_count',
            'Grade 8': 'grade8_count',
            'Grade9 Count': 'grade9_count',
            'GRADE9 COUNT': 'grade9_count',
            'Grade 9': 'grade9_count',
            'Grade10 Count': 'grade10_count',
            'GRADE10 COUNT': 'grade10_count',
            'Grade 10': 'grade10_count',
            'Grade11 Count': 'grade11_count',
            'GRADE11 COUNT': 'grade11_count',
            'Grade 11': 'grade11_count',
            'Grade12 Count': 'grade12_count',
            'GRADE12 COUNT': 'grade12_count',
            'Grade 12': 'grade12_count',
            'Grade14 Count': 'grade14_count',
            'GRADE14 COUNT': 'grade14_count',
            'Grade 14': 'grade14_count',

            # Historical xlsx format (2018-19). No DEMO or DGROUP column.
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
            'P': 'preschool_count',
            'K': 'kindergarten_count',
            'G1': 'grade1_count',
            'G2': 'grade2_count',
            'G3': 'grade3_count',
            'G4': 'grade4_count',
            'G5': 'grade5_count',
            'G6': 'grade6_count',
            'G7': 'grade7_count',
            'G8': 'grade8_count',
            'G9': 'grade9_count',
            'G10': 'grade10_count',
            'G11': 'grade11_count',
            'G12': 'grade12_count',
            'G14': 'grade14_count',
        }

    def accepts_data_file(self, file_path: Path) -> bool:
        """Skip the KYRC23 republish of 2022-23.

        ``school_courses_summary_2023.csv`` is the registered 2022-23 file.
        KYRC23 is the same school year. 11 course keys differ on each side.
        52 shared totals differ: 46 are a published count becoming ``*``,
        and 6 differ only by a thousands comma. Loading both would write
        2023 twice.
        """
        if file_path.name.upper() in _REPUBLISH_FILENAMES:
            logger.info(
                "Skipping %s; it republishes 2022-23 already loaded from school_courses_summary_2023.csv",
                file_path.name,
            )
            return False
        return super().accepts_data_file(file_path)

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
            for column in _COUNT_COLUMNS
            if column in df.columns
        }
        df = super().standardize_missing_values(df)
        df = _clean_count_columns(df)
        for column, mask in star_columns.items():
            df[_star_column_name(column)] = mask.to_numpy()
        return df

    def _enrollment_kind(self, row: pd.Series) -> str:
        """Classify the course enrollment total.

        ``star`` is a ``*``. ``blank`` is empty. ``number`` is a parsed
        count, including a published 0. Anything else is ``blank``.
        """
        if "student_count" not in row.index and not _metric_was_starred(row, "student_count"):
            return "blank"
        if _metric_was_starred(row, "student_count"):
            return "star"
        number = _parse_number(row.get("student_count", pd.NA))
        if _is_missing(number):
            return "blank"
        return "number"

    def should_skip_row(self, row: pd.Series) -> bool:
        """Skip a course whose enrollment total is blank.

        These files have no demographic column, so a missing demographic
        is not a reason to skip. A ``*`` is an enrollment total and is
        not skipped. A blank total means the course count was not
        published. No registered file has a blank total. A published 0
        is kept.
        """
        if self._enrollment_kind(row) != "blank":
            return False
        # An Algebra 1 row can still carry a grade-8 count when the
        # total is blank. No registered file has that name.
        if _is_algebra_1(row.get("course_name")):
            if _metric_was_starred(row, "grade8_count"):
                return False
            grade8 = _parse_number(row.get("grade8_count", pd.NA))
            if not _is_missing(grade8):
                return False
        return True

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        """Algebra 1 grade 8, when the course is named.

        Course counts are not emitted here. ``convert_to_kpi_format``
        collapses them to one row per school, year, and student group.
        A starred total with a numeric grade is still a starred total:
        the grade cells are not a substitute count. That row is included
        in ``courses_offered_count`` and ``courses_offered_suppressed_count``.

        Algebra metrics follow the grade-8 cell with the same star and
        blank rules. No registered course name contains Algebra.
        """
        metrics: Dict[str, Any] = {}
        if _is_algebra_1(row.get("course_name")):
            if _metric_was_starred(row, "grade8_count"):
                metrics["algebra_1_grade_8_count"] = pd.NA
                metrics["has_algebra_1_access"] = pd.NA
            else:
                grade8 = _parse_number(row.get("grade8_count", pd.NA))
                if not _is_missing(grade8) and grade8 >= 0:
                    metrics["algebra_1_grade_8_count"] = int(grade8)
                    metrics["has_algebra_1_access"] = 1 if grade8 > 0 else 0
        return metrics

    def convert_to_kpi_format(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """One count row per school, year, and student group.

        ``courses_offered_count`` is the number of course rows whose
        enrollment total is a number or a ``*``. A blank total is not
        counted. ``courses_offered_suppressed_count`` is how many of those
        totals are ``*``, and is 0 when none are. Category counts use the
        same rows. All seven metrics are stored with ``suppressed`` N.

        Algebra metrics stay one row per course. These files have no
        demographic, so ``student_group`` stays ``All Students``.
        """
        if "course_category" not in df.columns:
            logger.warning(
                "%s has no course category column; every counted course is courses_offered_count_other",
                source_file,
            )

        groups: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        order: List[Tuple[str, str, str]] = []
        seen_conflicts: set = set()
        unknown_labels: Dict[str, int] = {}
        blank_categories = 0
        algebra_rows: List[Dict[str, Any]] = []

        for _, row in df.iterrows():
            if self.should_skip_row(row):
                continue
            kpi_template = self.create_kpi_template(row, source_file)
            if kpi_template is None:
                continue

            kind = self._enrollment_kind(row)
            if kind in {"star", "number"}:
                key = (
                    str(kpi_template["school_id"]),
                    str(kpi_template["year"]),
                    str(kpi_template["student_group"]),
                )
                if key not in groups:
                    groups[key] = {"template": kpi_template, "tally": _empty_tally()}
                    order.append(key)
                else:
                    _merge_identity(groups[key]["template"], kpi_template, key, seen_conflicts)
                bucket, unknown = _category_bucket(row.get("course_category"))
                if unknown is not None:
                    unknown_labels[unknown] = unknown_labels.get(unknown, 0) + 1
                elif bucket == "other":
                    blank_categories += 1
                tally = groups[key]["tally"]
                tally["courses_offered_count"] += 1
                tally[f"courses_offered_count_{bucket}"] += 1
                if kind == "star":
                    tally["courses_offered_suppressed_count"] += 1

            metrics = self.extract_metrics(row)
            if not metrics and kpi_template.get("suppressed") == "Y":
                metrics = self.get_suppressed_metric_defaults(row)
            for metric_name, value in metrics.items():
                kpi_record = kpi_template.copy()
                kpi_record["metric"] = metric_name
                if _is_missing(value):
                    kpi_record["value"] = pd.NA
                    kpi_record["suppressed"] = "Y"
                else:
                    kpi_record["value"] = float(value)
                    kpi_record["suppressed"] = "N"
                algebra_rows.append(kpi_record)

        if blank_categories or unknown_labels:
            logger.warning(
                "%s unclassified courses: %s blank category, unrecognized labels %s",
                source_file,
                blank_categories,
                sorted(unknown_labels.items()),
            )

        kpi_rows: List[Dict[str, Any]] = []
        for key in order:
            group = groups[key]
            template = group["template"]
            tally = group["tally"]
            category_sum = (
                tally["courses_offered_count_cte"]
                + tally["courses_offered_count_hpe"]
                + tally["courses_offered_count_vpa"]
                + tally["courses_offered_count_wl"]
                + tally["courses_offered_count_other"]
            )
            if category_sum != tally["courses_offered_count"]:
                raise ValueError(
                    f"{source_file} school {key[0]} year {key[1]} category sum "
                    f"{category_sum} does not equal courses_offered_count "
                    f"{tally['courses_offered_count']}"
                )
            for metric_name in _OFFERED_METRICS:
                kpi_record = template.copy()
                kpi_record["metric"] = metric_name
                kpi_record["value"] = float(tally[metric_name])
                kpi_record["suppressed"] = "N"
                kpi_rows.append(kpi_record)
        kpi_rows.extend(algebra_rows)

        if not kpi_rows:
            logger.warning("No valid KPI rows created")
            return pd.DataFrame()
        kpi_df = pd.DataFrame(kpi_rows)
        available_columns = [col for col in KPI_COLUMNS if col in kpi_df.columns]
        return kpi_df[available_columns]

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Algebra placeholders when a suppressed row has no grade-8 metric.

        Course counts are school aggregates and are not filled in here.
        """
        defaults: Dict[str, Any] = {}
        if _is_algebra_1(row.get("course_name")):
            defaults["algebra_1_grade_8_count"] = pd.NA
            defaults["has_algebra_1_access"] = pd.NA
        return defaults


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Process school courses data and convert to KPI format using BaseETL."""
    etl = SchoolCoursesETL("school_courses")
    etl.process(raw_dir, proc_dir, cfg)


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    raw_dir = Path(__file__).parent.parent / "data" / "raw"
    proc_dir = Path(__file__).parent.parent / "data" / "processed"
    proc_dir.mkdir(exist_ok=True)

    test_config = Config(
        derive={"processing_date": "2025-11-24", "data_quality_flag": "reviewed"}
    ).model_dump()

    transform(raw_dir, proc_dir, test_config)
