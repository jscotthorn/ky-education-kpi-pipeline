"""
Advanced Coursework ETL Module

Kentucky publishes three different advanced-coursework measures. This module
keeps them on different metric names.

- Participation and performance is an unduplicated student count by
  demographic and course type. A student in two AP courses counts once.
  A completer finished at least one course. A qualifying score is a
  qualifying exam score on at least one AP, Cambridge, or IB exam.
  The derived rate is qualifying scores divided by students tested.
- Courses offered is a duplicated count of course enrollments. A student
  in two courses counts twice. KDE's glossary calls that total Advanced
  Courses Offered. The course type ``ALL`` rows are the subject totals,
  including ``All Enrollments``. An AP, IB, or Cambridge row is one
  course, or that type's own ``All Enrollments`` total. The subject, or
  the state course code plus the course name, is part of the metric name.
  Two course names can share one state code. The name keeps those rows
  from landing on one metric.
- The overview file is the CRDC advanced-coursework count, in total and
  by gender, for AP, IB, and dual credit. The percent columns are the
  published rates. The KPI year is the report-card year. ``Collected
  School Year`` is the CRDC collection and is not a KPI column.

Dual credit is not loaded here. ``dual_credit`` owns that measure.
Course type ``DC`` and ``Dual Credit`` are skipped in every file,
including the 2021-2023 historical files that label it ``DC`` and the
overview file that labels it ``Dual Credit``.

Course type ``CAI`` and ``Cambridge Advanced International`` are
Cambridge. Course type ``ALL`` is the unduplicated participation total
across course types, or the courses-offered subject total. Nothing else
uses the ``advanced_*`` prefix. ``ALL`` and ``CAI`` do not share a name.

There is no Suppressed column on the EDOP files. A ``*`` on a measure
cell is stored as value NA with suppressed Y. A blank cell is not a
star and is not stored. A published 0 is kept.
"""
from pathlib import Path
import re
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

# Participation and courses-offered measure columns. Overview uses the
# count and rate columns below. A star is recorded on every one of them.
_PARTICIPATION_COLUMNS = (
    "course_enrollment",
    "course_completers",
    "number_tested",
    "qualifying_score",
)

_OVERVIEW_COLUMNS = (
    "total_count",
    "female_count",
    "male_count",
    "total_rate",
    "female_rate",
    "male_rate",
)

_MEASURE_COLUMNS = _PARTICIPATION_COLUMNS + _OVERVIEW_COLUMNS

_NOT_A_NUMBER = {"", "*", "**", "---", "--", "<10", '""', "nan", "None", "<NA>"}

# The 2018-19 workbook labels groups in DISAGGGROUP. DEMO_ABBREV is not
# read. On that sheet ACD is "Students with Disability with IEP" and CSG
# is "Consolidated Student Group". The shared map sends the code CSG to
# Students with Disabilities and drops the code ACD. Using the label
# avoids that swap. "Two More Races" is the overview file's label for
# Two or More Races.
_LOCAL_DEMOGRAPHICS = {
    "Two More Races": "Two or More Races",
    # KYRC24 overview spells the Pacific Islander group this way, with a
    # trailing space on the header cell. strip() runs before this lookup.
    "Hawaiian Pacific": "Native Hawaiian or Pacific Islander",
    "Students with Disability with IEP": "Students with Disabilities (IEP)",
    "EL": "English Learner",
    "Free/Reduced": "Economically Disadvantaged",
    "Gifted & Talented": "Gifted and Talented",
    "Hawaiian/PI": "Native Hawaiian or Pacific Islander",
    "Hispanic": "Hispanic or Latino",
    "Two or More": "Two or More Races",
    "White": "White (non-Hispanic)",
    "Foster": "Foster Care",
    "Military Connected": "Military Dependent",
    "Indian/Alaska": "American Indian or Alaska Native",
}

# KYRC23 participation is the AP/IB/Cambridge slice of the historical
# 2023 file, with more cells starred. KYRC23 courses offered repeats
# those course rows and adds subject totals. Loading either file would
# write 2023 twice. KYRC23 overview is the CRDC 2020-21 extract already
# registered as KYRC25: 36,612 shared keys, 0 measure differences.
# KYRC23 adds 15 schools. It is not registered. These three names are
# skipped so a copy in the raw directory does not write the year twice.
_SKIP_EXACT_NAMES = {
    "KYRC23_EDOP_Advanced_Courses_Offered.csv",
    "KYRC23_EDOP_Advanced_Courses_Participation_and_Performance.csv",
    "KYRC23_OVW_Advanced_Coursework.csv",
}

_PARTICIPATION_METRICS = {
    "course_enrollment": "{prefix}_course_enrollment",
    "course_completers": "{prefix}_completion_count",
    "number_tested": "{prefix}_tested_count",
    "qualifying_score": "{prefix}_qualifying_score_count",
}

_OFFERED_METRICS = {
    "course_enrollment": "{prefix}_offered_enrollment_{slug}",
    "course_completers": "{prefix}_offered_completion_count_{slug}",
    "number_tested": "{prefix}_offered_tested_count_{slug}",
    "qualifying_score": "{prefix}_offered_qualifying_score_count_{slug}",
}

_OVERVIEW_METRICS = {
    "total_count": "{prefix}_participation_count",
    "female_count": "{prefix}_participation_count_female",
    "male_count": "{prefix}_participation_count_male",
    "total_rate": "{prefix}_participation_rate",
    "female_rate": "{prefix}_participation_rate_female",
    "male_rate": "{prefix}_participation_rate_male",
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

    ``read_csv(..., dtype=str)`` on pandas 3 returns StringDtype, not
    object. A comma in ``37,212`` has to be removed before a numeric
    cast. pandas 2.3.3 still returns object. pandas is unpinned.
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
    """Parse a count or a percent that may contain a comma.

    ``*`` and a blank are missing. A published 0 is parsed as 0. A blank
    is not turned into 0. ``bool(pd.NA)`` is not used.
    """
    if isinstance(value, str):
        cleaned = value.replace(",", "").replace("%", "").replace('"', "").strip()
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
    cleaned = series.astype(str).str.replace(",", "", regex=False).str.replace("%", "", regex=False)
    cleaned = cleaned.str.replace('"', "", regex=False).str.strip()
    missing = cleaned.isin(list(_NOT_A_NUMBER)) | cleaned.str.upper().isin(
        ["N/A", "#N/A", "NA", "NAN"]
    )
    cleaned = cleaned.mask(missing, pd.NA)
    return pd.to_numeric(cleaned, errors="coerce")


def subject_slug(subject: Any) -> str:
    """Turn a subject or course name into a metric suffix.

    ``English/Language Arts`` becomes ``english_language_arts``.
    ``All Enrollments`` becomes ``all_enrollments``.
    """
    text = "" if _is_missing(subject) else str(subject).strip().lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def _course_type_text(row: pd.Series) -> str:
    value = row.get("course_type", "")
    if _is_missing(value):
        return ""
    return str(value).strip().upper()


def _is_dual_credit_type(text: str) -> bool:
    """True for ``DC`` and for ``Dual Credit``.

    The historical files use the code ``DC``. KYRC24 and later use the
    words ``Dual Credit``. A substring check for ``DUAL CREDIT`` misses
    ``DC``. Matching ``DC`` as a substring would also be wrong, so the
    code is an exact token.
    """
    if text == "DC":
        return True
    return "DUAL CREDIT" in text


def _prefix_for_type(text: str) -> Optional[str]:
    """Map a course type to a metric prefix.

    ``ALL`` is the only type that uses ``advanced``. ``CAI`` is
    Cambridge, not ``advanced``. ``DC`` returns None so the row is
    skipped. An unrecognized type also returns None. It is not folded
    into ``advanced``.
    """
    if text == "" or _is_dual_credit_type(text):
        return None
    if text in {"AP", "ADVANCED PLACEMENT"} or "ADVANCED PLACEMENT" in text:
        return "ap"
    if text in {"IB", "INTERNATIONAL BACCALAUREATE"} or "INTERNATIONAL BACCALAUREATE" in text:
        return "ib"
    if text == "CAI" or "CAMBRIDGE" in text:
        return "cambridge"
    if text == "ALL":
        return "advanced"
    return None


def _clean_measure_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Strip commas before a numeric cast.

    ``process`` casts ``config/mappings.yaml`` dtypes after
    ``standardize_missing_values``. This source has no dtype cast. The
    cleaner still runs so an Int64 cast of ``37,212`` does not become
    missing. Object and StringDtype are both cleaned. A published 0 is
    left as 0. ``*`` becomes missing after the star flag is recorded.
    """
    for col in _MEASURE_COLUMNS:
        if col not in df.columns:
            continue
        series = df[col]
        if not _is_string_like(series):
            continue
        cleaned = (
            series.astype("string")
            .str.replace(",", "", regex=False)
            .str.replace("%", "", regex=False)
            .str.replace('"', "", regex=False)
            .str.strip()
        )
        missing = cleaned.isna() | cleaned.isin(list(_NOT_A_NUMBER))
        upper = cleaned.str.upper()
        missing = missing | upper.isin(["N/A", "#N/A", "NA", "NAN"])
        df[col] = cleaned.mask(missing, pd.NA)
    return df


class AdvancedCourseworkETL(BaseETL):
    """ETL module for AP, IB, Cambridge, and the advanced-course total.

    KYRC26 headers match KYRC25 on all three files. The historical
    KYRC24 courses-offered file spells ``Number Tested `` and
    ``Students With Qualifying Score `` with a trailing space. Later
    files drop the space. Both aliases are mapped. No temporary OAA
    extract is registered for this directory.
    """

    # Opt in here, in the same change that accepts the file. BaseETL
    # skips every KYRC26_* CSV until this is True.
    include_kyrc26 = True

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            # Course identification. 2018-19 uses the short names.
            "State Course Code": "course_code",
            "STATE COURSE CODE": "course_code",
            "STATE_COURSE_CODE": "course_code",
            "State Course Name": "course_name",
            "STATE COURSE NAME": "course_name",
            "STATE_COURSE_NAME": "course_name",
            "Advanced Course Type": "course_type",
            "ADVANCED COURSE TYPE": "course_type",
            "Course Type": "course_type",
            "COURSE TYPE": "course_type",
            "COURSE_TYPE": "course_type",
            "Subject": "subject",
            "SUBJECT": "subject",

            # Enrollment and performance. The KYRC24 historical offered
            # file has a trailing space on the last two headers.
            "Course Enrollment": "course_enrollment",
            "COURSE ENROLLMENT": "course_enrollment",
            "Course Completers": "course_completers",
            "COURSE COMPLETERS": "course_completers",
            "Number Tested": "number_tested",
            "NUMBER TESTED": "number_tested",
            "Number Tested ": "number_tested",
            "Students With Qualifying Score": "qualifying_score",
            "STUDENTS WITH QUALIFYING SCORE": "qualifying_score",
            "Students With Qualifying Score ": "qualifying_score",
            "Earned Qualifying Score": "qualifying_score",
            "EARNED QUALIFYING SCORE": "qualifying_score",
            # 2018-19 DATA sheet.
            "ENROLLED_CNT": "course_enrollment",
            "TOTAL_ENROLLMENT_CNT": "course_enrollment",
            "COMPLETERS_CNT": "course_completers",
            "TEST_TAKERS_CNT": "number_tested",
            "EARNED_QUALIFYING_SCORE_CNT": "qualifying_score",

            # 2018-19 identity columns. DISAGGGROUP is the group label.
            # DEMO_ABBREV is left unmapped so the code ACD is not sent
            # through the shared filter.
            "SCH_YEAR": "school_year",
            "CNTYNO": "county_number",
            "CNTYNAME": "county_name",
            "DIST_NUMBER": "district_number",
            "DIST_NAME": "district_name",
            "SCH_NUMBER": "school_number",
            "SCH_NAME": "school_name",
            "SCH_CD": "school_code",
            "STATE_SCH_ID": "state_school_id",
            "NCESID": "nces_id",
            "COOP": "co_op",
            "COOP_CODE": "co_op_code",
            "DISAGGGROUP": "demographic",

            # Overview file. Counts and the published percents.
            "Total": "total_count",
            "TOTAL": "total_count",
            "Female": "female_count",
            "FEMALE": "female_count",
            "Male": "male_count",
            "MALE": "male_count",
            "Total %": "total_rate",
            "TOTAL %": "total_rate",
            "Female %": "female_rate",
            "FEMALE %": "female_rate",
            "Male %": "male_rate",
            "MALE %": "male_rate",
        }

    def accepts_data_file(self, file_path: Path) -> bool:
        """Skip the two KYRC23 EDOP files that repeat 2022-23.

        ``advanced_courses_participation_and_performance_2023.csv`` is
        the registered 2022-23 participation file. The KYRC23
        participation name returns 200 on datasets and on UAT and 404
        on the historical host. After the 3-digit district codes are
        padded, all 6,444 AP, IB, and Cambridge keys match. KYRC23
        stars cells the historical file publishes (34 enrollment cells,
        including Asian AP counts of 3, 4, and 6). The courses-offered
        twin repeats the course rows and adds the ``ALL`` subject
        totals. The overview name is the CRDC 2020-21 extract already
        registered as ``KYRC25_OVW_Advanced_Coursework.csv``: every
        shared measure cell matches, and KYRC23 has 15 schools KYRC25
        does not. The guessed OAA names are 404 and were never
        registered, so they are not listed here.
        """
        if file_path.name in _SKIP_EXACT_NAMES:
            logger.info(
                "Skipping %s; the historical 2023 advanced files already hold 2022-23",
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
        is later written with ``suppressed`` ``Y``. A blank is not
        recorded. A published 0 is left as 0.
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
        if "demographic" in df.columns:
            df["demographic"] = df["demographic"].map(self._local_demographic)
        return df

    @staticmethod
    def _local_demographic(value: Any) -> Any:
        """Apply the labels that belong to this module only."""
        if not isinstance(value, str):
            return value
        return _LOCAL_DEMOGRAPHICS.get(value.strip(), value)

    def _kind(self, row: pd.Series) -> str:
        """Return ``overview``, ``offered``, or ``participation``.

        The overview file has the count and percent columns and no
        course enrollment. Courses offered has a subject or a course
        code and no demographic. Participation has a demographic.
        """
        has_overview = any(column in row.index for column in _OVERVIEW_COLUMNS)
        has_participation = any(column in row.index for column in _PARTICIPATION_COLUMNS)
        if has_overview and not has_participation:
            return "overview"
        if "demographic" not in row.index or _is_missing(row.get("demographic")):
            if "subject" in row.index or "course_code" in row.index or "course_name" in row.index:
                return "offered"
        return "participation"

    def _row_has_measure(self, row: pd.Series, columns: Tuple[str, ...]) -> bool:
        """True when any listed cell is a number or a star.

        A blank is neither. A row of blanks is not offered and is skipped.
        """
        for name in columns:
            if name not in row.index and not _metric_was_starred(row, name):
                continue
            if _metric_was_starred(row, name):
                return True
            if name in row.index and not _is_missing(_parse_number(row.get(name, pd.NA))):
                return True
        return False

    def should_skip_row(self, row: pd.Series) -> bool:
        """Skip dual credit, a blank measure row, and an empty group.

        ``DC`` and ``Dual Credit`` are skipped in every year. A blank
        overview row, or a participation row whose four measure cells
        are blank, is not offered and is skipped in every year. A ``*``
        is a measure and is not skipped. Courses offered have no
        demographic. A missing demographic does not skip those rows.
        A participation row with no demographic is skipped.
        """
        if _is_dual_credit_type(_course_type_text(row)):
            return True
        if _prefix_for_type(_course_type_text(row)) is None and _course_type_text(row) != "":
            return True
        kind = self._kind(row)
        if kind == "overview":
            return not self._row_has_measure(row, _OVERVIEW_COLUMNS)
        if kind == "offered":
            if _prefix_for_type(_course_type_text(row)) is None:
                return True
            return not self._row_has_measure(row, _PARTICIPATION_COLUMNS)
        if "demographic" not in row.index or _is_missing(row.get("demographic")):
            return True
        if super().should_skip_row(row):
            return True
        return not self._row_has_measure(row, _PARTICIPATION_COLUMNS)

    def _collapse_repeated_keys(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """Keep one row when a file repeats a school key.

        Participation keys are school code, demographic, and course
        type. Courses-offered keys add the subject, the course code,
        and the course name, so two subjects are not collapsed.
        2020-2026 do not repeat a participation key. A number beats a
        ``*``, and a ``*`` beats a blank. When the enrollment counts
        tie, file order keeps the first row.
        """
        if df.empty or "school_code" not in df.columns:
            return df
        def _part(column: str) -> pd.Series:
            # A missing piece is an empty token. String addition with
            # pd.NA would make the whole key NA, and every subject total
            # (course code ``N/A``) would collapse into one row.
            if column not in df.columns:
                return pd.Series("", index=df.index, dtype="string")
            return df[column].astype("string").str.strip().fillna("")

        code = _part("school_code")
        group = _part("demographic")
        course_type = _part("course_type")
        subject = _part("subject")
        course_code = _part("course_code")
        course_name = _part("course_name")
        key = code + "|" + group + "|" + course_type + "|" + subject + "|" + course_code + "|" + course_name
        counts = key.value_counts(dropna=False)
        repeated = set(counts[counts > 1].index)
        if not repeated:
            return df

        work = df.copy()
        work["_key"] = key
        work["_orig"] = range(len(work))
        enrollment = (
            work["course_enrollment"]
            if "course_enrollment" in work.columns
            else pd.Series(pd.NA, index=work.index)
        )
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

    def _offered_slug(self, row: pd.Series) -> str:
        """Suffix for a courses-offered metric.

        Course type ``ALL`` is a subject total, so the subject is the
        suffix. An AP, IB, or Cambridge ``All Enrollments`` row is that
        type's total and uses the subject as well. A course row uses
        the state course code and the course name. In 2022, 10 IB
        school-and-code pairs have two names and the same enrollment.
        In 2023, 58 pairs have two names and different counts, including
        state AP Computer Science A ``110701`` at 525 and 55. The name
        is part of the suffix so those rows do not share a metric. A
        course with no code uses the course name, then the subject.
        """
        text = _course_type_text(row)
        subject = subject_slug(row.get("subject"))
        if text == "ALL":
            return subject
        subject_text = "" if _is_missing(row.get("subject")) else str(row.get("subject")).strip().lower()
        if subject_text == "all enrollments":
            return subject
        name_slug = subject_slug(row.get("course_name"))
        code = row.get("course_code")
        code_text = "" if _is_missing(code) else str(code).strip()
        code_slug = ""
        if code_text.upper() not in {"", "N/A", "NA", "NAN"}:
            code_slug = subject_slug(code_text)
            if code_slug in {"", "n_a", "na"}:
                code_slug = ""
        if code_slug and name_slug not in {"", "all"}:
            return f"{code_slug}_{name_slug}"
        if code_slug:
            return code_slug
        if name_slug not in {"", "all"}:
            return name_slug
        return subject

    def _counts_as_course(self, row: pd.Series) -> bool:
        """True for one offered course, not a subject or type total.

        ``num_{prefix}_courses_offered`` counts these rows. The
        ``All Enrollments`` row and course type ``ALL`` are totals.
        """
        if _course_type_text(row) == "ALL":
            return False
        subject_text = "" if _is_missing(row.get("subject")) else str(row.get("subject")).strip().lower()
        if subject_text == "all enrollments":
            return False
        return self._offered_slug(row) != ""

    def _names_for_row(self, row: pd.Series) -> Optional[Dict[str, str]]:
        """Map source columns to KPI names for this row."""
        prefix = _prefix_for_type(_course_type_text(row))
        if prefix is None:
            return None
        kind = self._kind(row)
        if kind == "overview":
            return {
                column: template.format(prefix=prefix)
                for column, template in _OVERVIEW_METRICS.items()
            }
        if kind == "offered":
            slug = self._offered_slug(row)
            if slug == "":
                return None
            # Course type ALL always uses the advanced prefix, even if
            # a future label is added. The prefix function already
            # returns advanced only for ALL.
            return {
                column: template.format(prefix=prefix, slug=slug)
                for column, template in _OFFERED_METRICS.items()
            }
        return {
            column: template.format(prefix=prefix)
            for column, template in _PARTICIPATION_METRICS.items()
        }

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        """Extract one row's metrics.

        A ``*`` cell is included as NA. ``convert_to_kpi_format`` writes
        that metric with ``suppressed`` ``Y``. A blank cell is omitted.
        A published 0 is kept. Dual credit returns nothing.

        The participation qualifying rate is qualifying scores divided
        by students tested. The courses-offered completion rate is
        completers divided by enrollment, and its qualifying rate is
        qualifying scores divided by students tested. A rate is omitted
        when either input is a star, a blank, or a zero denominator.
        Overview rates are the published percents, not a quotient.
        """
        names = self._names_for_row(row)
        if names is None:
            return {}
        kind = self._kind(row)
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
            if kind == "overview" and column.endswith("_rate"):
                metrics[metric_name] = float(number)
            else:
                metrics[metric_name] = int(number) if float(number).is_integer() else float(number)

        if kind == "participation":
            # ALL qualifying counts include dual-credit grades, and ALL
            # tested counts are AP, Cambridge, and IB exams. The quotient
            # is not a rate KDE publishes. AP, IB, and Cambridge use
            # qualifying exams divided by students tested.
            tested = parsed.get("number_tested", pd.NA)
            qualifying = parsed.get("qualifying_score", pd.NA)
            prefix = _prefix_for_type(_course_type_text(row))
            if (
                prefix is not None
                and prefix != "advanced"
                and not _is_missing(tested)
                and not _is_missing(qualifying)
                and tested > 0
                and qualifying >= 0
            ):
                metrics[f"{prefix}_qualifying_score_rate"] = round((qualifying / tested) * 100, 1)
        elif kind == "offered":
            prefix = _prefix_for_type(_course_type_text(row))
            slug = self._offered_slug(row)
            enrollment = parsed.get("course_enrollment", pd.NA)
            completers = parsed.get("course_completers", pd.NA)
            tested = parsed.get("number_tested", pd.NA)
            qualifying = parsed.get("qualifying_score", pd.NA)
            if prefix is not None and slug != "":
                if (
                    not _is_missing(enrollment)
                    and not _is_missing(completers)
                    and enrollment > 0
                    and completers >= 0
                ):
                    metrics[f"{prefix}_offered_completion_rate_{slug}"] = round(
                        (completers / enrollment) * 100, 1
                    )
                if (
                    not _is_missing(tested)
                    and not _is_missing(qualifying)
                    and tested > 0
                    and qualifying >= 0
                ):
                    metrics[f"{prefix}_offered_qualifying_score_rate_{slug}"] = round(
                        (qualifying / tested) * 100, 1
                    )
        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Default NA metrics for a fully suppressed row."""
        names = self._names_for_row(row)
        if not names:
            return {}
        defaults = {metric_name: pd.NA for metric_name in names.values()}
        prefix = _prefix_for_type(_course_type_text(row))
        if prefix is not None and self._kind(row) == "participation":
            defaults[f"{prefix}_qualifying_score_rate"] = pd.NA
        return defaults

    def convert_to_kpi_format(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """Write a published cell as ``N`` and a ``*`` cell as ``Y``.

        A blank cell is not a row. Dual credit is not written. Course
        type ``ALL`` and course type ``CAI`` do not share a metric.
        Courses offered do not use the participation metric names.
        ``num_{prefix}_courses_offered`` is the count of course rows at
        the school, one row per school and course type.
        """
        df = self._collapse_repeated_keys(df, source_file)
        kpi_rows: List[Dict[str, Any]] = []
        course_counts: Dict[Tuple[str, str], Dict[str, Any]] = {}
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
            if self._kind(row) == "offered" and self._counts_as_course(row):
                prefix = _prefix_for_type(_course_type_text(row))
                if prefix is not None:
                    school_id = str(kpi_template.get("school_id", ""))
                    key = (school_id, prefix)
                    if key not in course_counts:
                        course_counts[key] = {"template": kpi_template, "count": 0}
                    course_counts[key]["count"] += 1

        for (school_id, prefix), payload in course_counts.items():
            kpi_record = payload["template"].copy()
            kpi_record["metric"] = f"num_{prefix}_courses_offered"
            kpi_record["value"] = float(payload["count"])
            kpi_record["suppressed"] = "N"
            kpi_record["student_group"] = "All Students"
            kpi_rows.append(kpi_record)

        if not kpi_rows:
            logger.warning("No valid KPI rows created")
            return pd.DataFrame()
        kpi_df = pd.DataFrame(kpi_rows)
        available_columns = [col for col in KPI_COLUMNS if col in kpi_df.columns]
        return kpi_df[available_columns]


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Process advanced coursework data and convert to KPI format using BaseETL."""
    etl = AdvancedCourseworkETL("advanced_coursework")
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
