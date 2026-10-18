"""
Gifted and Talented Participation ETL Module

Handles Kentucky gifted and talented program participation data.
Normalizes column names, standardizes missing values, and transforms to KPI format.

KDE publishes two measures:

- Grade-level participation is an unduplicated count of gifted students by
  demographic and grade. The 2018-19 coversheet calls ``GT_CNT`` the number
  of gifted and talented students enrolled. Gender counts partition that
  total. The seven race counts also partition it on the state row.
- Category participation is the count of students identified in one gifted
  category (general intellectual ability, a specific academic aptitude,
  creative or divergent thinking, psychosocial or leadership, a visual or
  performing arts area, or the primary talent pool). A student may be
  identified in more than one category, so the categories are not shares
  of the grade total and they do not sum to it.

The category file's ``All Students`` cell is that same unduplicated total.
It is stored as ``gifted_participation_count_all_grades`` and is not given
a second metric name. A school that appears on both files keeps the grade
file's row. That category row is left out before the CSV is written.

There is no Suppressed column. A ``*`` on a measure cell is stored as
value NA with suppressed Y. A blank cell is not a star and is not stored.

A published 0 is kept where the file actually publishes 0. The 2018-19
workbook publishes 0, including most grade 14 cells. KYRC25 and KYRC26
schools that appear only on the category file publish 0. The 2020-2023
historical files publish no 0 and no blank. The smallest number in those
files is 3, so a Y covers a count of 0, 1, or 2 and a not-applicable
cell, not only a privacy suppression. Preschool, grade 14, and
Consolidated Student Group are starred on every 2020-2023 row, including
the state row. Those three slices are not written for 2020-2023. Every
other star in those years is still stored. 2019 and 2024 onward are
unchanged: 2019 publishes real consolidated and grade 14 counts, and a
later preschool or grade 14 star is still stored. The KYRC23 republication
of 2022-23 leaves 16,277 of the 20,036 fully starred 2023 rows out,
turns 135,421 starred grade cells into blanks, and turns 8,765 starred
category cells into 0, so the omitted slices are not hidden counts.

The 2018-19 workbook's disability count is ``DGROUP`` ``Studw/disab.``
(DEMO code ``ACD``). DEMO code ``CSG`` is Consolidated Student Group.
This module reads that workbook's group from ``DGROUP``. The shared
demographic map is not changed.
"""
from pathlib import Path
import pandas as pd
from typing import Any, Dict, List, Optional, Set, Tuple
import logging

import sys

# Add etl directory to path for imports
etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from constants import KPI_COLUMNS
from base_etl import BaseETL, Config

logger = logging.getLogger(__name__)

# Grade column (normalized) -> KPI metric. Absent columns are not emitted.
# KYRC26 has no Grade 14. KYRC25 and later grade files have no Preschool.
_GRADE_METRICS: Tuple[Tuple[str, str], ...] = (
    ("all_grades", "gifted_participation_count_all_grades"),
    ("preschool", "gifted_participation_count_preschool"),
    ("kindergarten", "gifted_participation_count_kindergarten"),
    ("grade_1", "gifted_participation_count_grade_1"),
    ("grade_2", "gifted_participation_count_grade_2"),
    ("grade_3", "gifted_participation_count_grade_3"),
    ("grade_4", "gifted_participation_count_grade_4"),
    ("grade_5", "gifted_participation_count_grade_5"),
    ("grade_6", "gifted_participation_count_grade_6"),
    ("grade_7", "gifted_participation_count_grade_7"),
    ("grade_8", "gifted_participation_count_grade_8"),
    ("grade_9", "gifted_participation_count_grade_9"),
    ("grade_10", "gifted_participation_count_grade_10"),
    ("grade_11", "gifted_participation_count_grade_11"),
    ("grade_12", "gifted_participation_count_grade_12"),
    ("grade_14", "gifted_participation_count_grade_14"),
)

# Category column (normalized slug) -> metric suffix. The 2018-19 workbook
# names language arts ``ACADEMICARTS_CNT``. Its state total lines up with
# later ``Specific Academic Aptitude Language Arts`` (about 20,000), and
# ``VISPERFART_CNT`` lines up with visual art (about 5,000). The coversheet
# text "Art Students" on AcademicArts is the language-arts column.
_CATEGORY_SLUGS: Tuple[str, ...] = (
    "creative_or_divergent_thinking",
    "general_intellectual_ability",
    "primary_talent_pool",
    "psychosocial_or_leadership_ability",
    "specific_academic_aptitude_language_arts",
    "specific_academic_aptitude_math",
    "specific_academic_aptitude_science",
    "specific_academic_aptitude_social_studies",
    "visual_and_performing_arts_art",
    "visual_and_performing_arts_dance",
    "visual_and_performing_arts_drama",
    "visual_and_performing_arts_music",
)

# Long-format Category labels, after underscores become spaces.
_CATEGORY_LABELS = {
    "creative or divergent thinking": "creative_or_divergent_thinking",
    "divergent thinking": "creative_or_divergent_thinking",
    "general intellectual ability": "general_intellectual_ability",
    "primary talent pool": "primary_talent_pool",
    "psychosocial or leadership ability": "psychosocial_or_leadership_ability",
    "psycho social leadership": "psychosocial_or_leadership_ability",
    "psychosocial leadership": "psychosocial_or_leadership_ability",
    "specific academic aptitude language arts": "specific_academic_aptitude_language_arts",
    "specific academic aptitude math": "specific_academic_aptitude_math",
    "specific academic aptitude science": "specific_academic_aptitude_science",
    "specific academic aptitude social studies": "specific_academic_aptitude_social_studies",
    "visual and performing arts art": "visual_and_performing_arts_art",
    "visual and performing arts dance": "visual_and_performing_arts_dance",
    "visual and performing arts drama": "visual_and_performing_arts_drama",
    "visual and performing arts music": "visual_and_performing_arts_music",
}

_MEASURE_COLUMNS: Tuple[str, ...] = tuple(
    column for column, _metric in _GRADE_METRICS
) + _CATEGORY_SLUGS + ("students",)

_NOT_A_NUMBER = {"", "*", "**", "---", "--", "<10", '""', "nan", "None", "<NA>"}

# Same school year as gifted_and_talented_2023.csv. Shared All Students
# cells match. Loading both would write 2023 twice. The historical file
# keeps preschool, the consolidated group, 3-digit district codes, and
# category counts by demographic.
_SKIP_EXACT_NAMES = {
    "KYRC23_EDOP_Gifted_Participation_by_Grade_Level.csv",
    "KYRC23_EDOP_Gifted_Participation_by_Category.csv",
}

# 2018-19 DATA sheet. DEMO ``CSG`` is Consolidated Student Group.
# DEMO ``ACD`` is the disability count, labeled ``Studw/disab.``.
_WORKBOOK_2019 = "gifted_and_talented_18-19.xlsx"
_ALL_STUDENTS_LABELS = {"all students", "tst"}
_ALL_GRADES_METRIC = "gifted_participation_count_all_grades"

# Named exception to notes/8. These four files star preschool, grade 14,
# and Consolidated Student Group on every row, state included. The skip
# is this year set and these slices. It does not look at whether a
# column is all stars, and it does not apply to any other year.
_OMIT_UNINFORMATIVE_YEARS = {"2020", "2021", "2022", "2023"}
_OMIT_UNINFORMATIVE_METRICS = {
    "gifted_participation_count_preschool",
    "gifted_participation_count_grade_14",
}
_OMIT_UNINFORMATIVE_GROUP = "Consolidated Student Group"


def _star_column_name(column: str) -> str:
    """Boolean column that remembers a ``*`` after the cell becomes NA."""
    return f"_star_{column}"


def _is_star_text(value: Any) -> bool:
    """True when the cell text is a privacy star.

    A blank is not a star. ``bool(pd.NA)`` is not used. This is the
    path for a row that has no ``_star_`` column. ``*`` and ``**`` are
    stars. An empty string is not.
    """
    if isinstance(value, str):
        return value.strip() in {"*", "**"}
    return False


def _is_string_like(series: pd.Series) -> bool:
    """True for object text and for pandas StringDtype.

    ``read_csv(..., dtype=str)`` on pandas 3 returns StringDtype, not object.
    A comma in ``90,464`` has to be removed before a numeric cast. pandas
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
    """True for a recorded star flag. Missing and false stay false.

    A merged row can carry the flag as a real bool or as the text
    ``True`` after a frame is rebuilt. ``bool(pd.NA)`` is not used.
    """
    try:
        if bool(pd.isna(value)):
            return False
    except (TypeError, ValueError):
        return False
    if isinstance(value, str):
        return value.strip().lower() == "true"
    return bool(value)


def _metric_was_starred(row: pd.Series, column: str) -> bool:
    """True when this measure cell was a ``*``.

    ``standardize_missing_values`` records the position before it turns
    the star into NA. A row that has no ``_star_`` column is recognized
    from the cell text. A blank on that path is not a star.
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


def category_label_slug(label: Any) -> Optional[str]:
    """Map a Category cell to a metric suffix.

    ``All Students`` is the unduplicated total, not a talent category, so
    this returns None for that label. ``SPECIFIC ACADEMIC APTITUDE_LANGUAGE
    ARTS`` and ``Specific Academic Aptitude Language Arts`` are the same
    category. A text search for ``ART`` would also match language arts.
    """
    if _is_missing(label):
        return None
    text = " ".join(str(label).strip().lower().replace("_", " ").split())
    if text == "all students":
        return None
    return _CATEGORY_LABELS.get(text)


def _cell_rank(value: Any, starred: bool) -> Tuple[int, float]:
    """Order cells so a number beats a star and a star beats a blank.

    Two numbers compare by size. The only registered pair that differs
    is Covington Independent ``134`` in 2022, All Students total 105
    against 3.
    """
    number = _parse_number(value)
    if not _is_missing(number):
        return (2, float(number))
    if starred:
        return (1, 0.0)
    return (0, 0.0)


def _clean_measure_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Strip commas before a numeric cast.

    ``process`` casts ``config/mappings.yaml`` dtypes after
    ``standardize_missing_values``. This source has no dtype cast. The
    cleaner still runs so an Int64 cast of ``90,464`` does not become
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


class GiftedTalentedETL(BaseETL):
    """ETL module for gifted and talented participation.

    KYRC26 grade file drops Grade 14. The category file matches KYRC25:
    one row per category and a ``Students`` count. ``TOTAL COUNT`` on the
    2020-2023 historical files is the all-grades total. ``include_kyrc26``
    opts the parser into the registered KYRC26 files.

    A Y on those 2020-2023 files is not evidence the group exists. Those
    files publish no 0 and no blank, and the smallest number is 3.
    Preschool, grade 14, and Consolidated Student Group are starred on
    every row, including the state row. Those three slices are not
    written for 2020-2023. Every other star in those years is still
    stored. 2019 and 2024 onward are unchanged. The 2018-19 disability
    group is read from ``DGROUP``.
    """

    # Opt in here, in the same change that accepts the file. BaseETL skips
    # every KYRC26_* CSV until this is True.
    include_kyrc26 = True

    def __init__(self, source_name: Optional[str] = None) -> None:
        super().__init__(source_name)
        # (year, school_id) whose grade file already emits All Students
        # all-grades. Category-file copies of that cell are not written.
        self._grade_all_students_keys: Set[Tuple[str, str]] = set()

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            "All Grades": "all_grades",
            "ALL GRADES": "all_grades",
            "TOTAL COUNT": "all_grades",
            "Preschool": "preschool",
            "PRESCHOOL": "preschool",
            "PRESCHOOL COUNT": "preschool",
            "K": "kindergarten",
            "KINDERGARTEN": "kindergarten",
            "KINDERGARTEN COUNT": "kindergarten",
            "Grade 1": "grade_1",
            "GRADE 1": "grade_1",
            "GRADE1 COUNT": "grade_1",
            "Grade 2": "grade_2",
            "GRADE 2": "grade_2",
            "GRADE2 COUNT": "grade_2",
            "Grade 3": "grade_3",
            "GRADE 3": "grade_3",
            "GRADE3 COUNT": "grade_3",
            "Grade 4": "grade_4",
            "GRADE 4": "grade_4",
            "GRADE4 COUNT": "grade_4",
            "Grade 5": "grade_5",
            "GRADE 5": "grade_5",
            "GRADE5 COUNT": "grade_5",
            "Grade 6": "grade_6",
            "GRADE 6": "grade_6",
            "GRADE6 COUNT": "grade_6",
            "Grade 7": "grade_7",
            "GRADE 7": "grade_7",
            "GRADE7 COUNT": "grade_7",
            "Grade 8": "grade_8",
            "GRADE 8": "grade_8",
            "GRADE8 COUNT": "grade_8",
            "Grade 9": "grade_9",
            "GRADE 9": "grade_9",
            "GRADE9 COUNT": "grade_9",
            "Grade 10": "grade_10",
            "GRADE 10": "grade_10",
            "GRADE10 COUNT": "grade_10",
            "Grade 11": "grade_11",
            "GRADE 11": "grade_11",
            "GRADE11 COUNT": "grade_11",
            "Grade 12": "grade_12",
            "GRADE 12": "grade_12",
            "GRADE12 COUNT": "grade_12",
            "Grade 14": "grade_14",
            "GRADE 14": "grade_14",
            "GRADE14 COUNT": "grade_14",
            # 2018-19 workbook. DATA sheet, not the coversheet.
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
            "DEMO": "demographic",
            # Human label. CSG's label is Consolidated Student Group.
            # ACD's label is Studw/disab., the disability count.
            "DGROUP": "dgroup",
            "GT_CNT": "all_grades",
            "P_CNT": "preschool",
            "K_CNT": "kindergarten",
            "G1_CNT": "grade_1",
            "G2_CNT": "grade_2",
            "G3_CNT": "grade_3",
            "G4_CNT": "grade_4",
            "G5_CNT": "grade_5",
            "G6_CNT": "grade_6",
            "G7_CNT": "grade_7",
            "G8_CNT": "grade_8",
            "G9_CNT": "grade_9",
            "G10_CNT": "grade_10",
            "G11_CNT": "grade_11",
            "G12_CNT": "grade_12",
            "G14_CNT": "grade_14",
            "DIVERGENTTHINKING_CNT": "creative_or_divergent_thinking",
            "GENERALINTELLECTUAL_CNT": "general_intellectual_ability",
            "PRIMARYTALENTPOOL_CNT": "primary_talent_pool",
            "PSYCHOSOCIALLEADERSHIP_CNT": "psychosocial_or_leadership_ability",
            "ACADEMICARTS_CNT": "specific_academic_aptitude_language_arts",
            "ACADEMICMATH_CNT": "specific_academic_aptitude_math",
            "ACADEMICSCIENCE_CNT": "specific_academic_aptitude_science",
            "ACADEMICSOCIALSTUDIES_CNT": "specific_academic_aptitude_social_studies",
            "VISPERFART_CNT": "visual_and_performing_arts_art",
            "VISPERFDANCE_CNT": "visual_and_performing_arts_dance",
            "VISPERFDRAMA_CNT": "visual_and_performing_arts_drama",
            "VISPERFMUSIC_CNT": "visual_and_performing_arts_music",
            # 2021-2023 historical files. Category columns sit on the
            # demographic row. The underscore is part of the header.
            "CREATIVE OR DIVERGENT THINKING": "creative_or_divergent_thinking",
            "GENERAL INTELLECTUAL ABILITY": "general_intellectual_ability",
            "PRIMARY TALENT POOL": "primary_talent_pool",
            "PSYCHO SOCIAL LEADERSHIP": "psychosocial_or_leadership_ability",
            "SPECIFIC ACADEMIC APTITUDE_LANGUAGE ARTS": "specific_academic_aptitude_language_arts",
            "SPECIFIC ACADEMIC APTITUDE_MATH": "specific_academic_aptitude_math",
            "SPECIFIC ACADEMIC APTITUDE_SCIENCE": "specific_academic_aptitude_science",
            "SPECIFIC ACADEMIC APTITUDE_SOCIAL STUDIES": "specific_academic_aptitude_social_studies",
            "VISUAL AND PERFORMING ARTS_ART": "visual_and_performing_arts_art",
            "VISUAL AND PERFORMING ARTS_DANCE": "visual_and_performing_arts_dance",
            "VISUAL AND PERFORMING ARTS_DRAMA": "visual_and_performing_arts_drama",
            "VISUAL AND PERFORMING ARTS_MUSIC": "visual_and_performing_arts_music",
            # KYRC24 category file is wide and diagonal: the count sits in
            # the column that matches the Category cell. KYRC25 and KYRC26
            # are long: Category + Students.
            "Category": "category",
            "CATEGORY": "category",
            "Creative Or Divergent Thinking": "creative_or_divergent_thinking",
            "General Intellectual Ability": "general_intellectual_ability",
            "Primary Talent Pool": "primary_talent_pool",
            "Psychosocial or Leadership Ability": "psychosocial_or_leadership_ability",
            "Specific Academic Aptitude Language Arts": "specific_academic_aptitude_language_arts",
            "Specific Academic Aptitude Math": "specific_academic_aptitude_math",
            "Specific Academic Aptitude Science": "specific_academic_aptitude_science",
            "Specific Academic Aptitude Social Studies": "specific_academic_aptitude_social_studies",
            "Visual And Performing Arts Art": "visual_and_performing_arts_art",
            "Visual And Performing Arts Dance": "visual_and_performing_arts_dance",
            "Visual And Performing Arts Drama": "visual_and_performing_arts_drama",
            "Visual And Performing Arts Music": "visual_and_performing_arts_music",
            "Students": "students",
            "STUDENTS": "students",
        }

    def accepts_data_file(self, file_path: Path) -> bool:
        """Skip the KYRC23 extracts that repeat 2022-23.

        ``gifted_and_talented_2023.csv`` is the registered 2022-23 file.
        The KYRC23 names return 200 on datasets and on UAT and 404 on the
        historical host. School year is ``20222023``. On shared school
        codes the All Students total matches, including the state total
        87,613. The guessed OAA names are 404 and were never registered,
        so they are not listed here.
        """
        if file_path.name in _SKIP_EXACT_NAMES:
            logger.info(
                "Skipping %s; gifted_and_talented_2023.csv already holds 2022-23",
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

        A blank is neither. A row of blanks is not stored.
        """
        for name in _MEASURE_COLUMNS:
            if _metric_was_starred(row, name):
                return True
            if name in row.index and not _is_missing(_parse_number(row.get(name, pd.NA))):
                return True
        return False

    def _is_long_category_row(self, row: pd.Series) -> bool:
        """True for a KYRC25/KYRC26 category row.

        Those rows have ``Category`` and ``Students`` and no demographic.
        The KYRC24 category file is wide: it has ``Category`` and no
        ``Students`` column. A demographic row is the grade file, even
        when category columns share the row.
        """
        if "students" not in row.index or "category" not in row.index:
            return False
        if _is_missing(row.get("category")):
            return False
        if "demographic" in row.index and not _is_missing(row.get("demographic")):
            return False
        return True

    def should_skip_row(self, row: pd.Series) -> bool:
        """Skip a row that has no demographic and no category, or no measure.

        A blank measure cell is not a star. A row whose grade and category
        cells are all blank is skipped in every year. A ``*`` is a measure
        and is not skipped. Category-file rows have no demographic. They
        are not skipped for that.
        """
        has_demo = "demographic" in row.index and not _is_missing(row.get("demographic"))
        has_category = "category" in row.index and not _is_missing(row.get("category"))
        if has_demo:
            if super().should_skip_row(row):
                return True
        elif not has_category:
            return True
        return not self._row_has_measure(row)

    def _collapse_repeated_keys(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """Keep one row when a file repeats a school key.

        Grade files key on school code plus demographic. Category files
        key on school code plus category. 2022 repeats Covington
        Independent ``134`` once per demographic. The copies agree except
        All Students ``TOTAL COUNT``, which is 105 and 3. A number beats
        a star. A larger number beats a smaller one. That keeps 105, and
        it keeps the grade cells the previous parser already stored from
        the 105 row. Other years do not repeat a key.
        """
        if df.empty or "school_code" not in df.columns:
            return df
        code = df["school_code"].astype("string").str.strip()
        if "demographic" in df.columns and df["demographic"].notna().any():
            group = df["demographic"].astype("string").str.strip()
            rank_column = "all_grades" if "all_grades" in df.columns else ""
        elif "category" in df.columns:
            group = df["category"].astype("string").str.strip()
            rank_column = "students" if "students" in df.columns else "all_grades"
        else:
            return df
        key = code + "|" + group
        counts = key.value_counts(dropna=False)
        repeated = set(counts[counts > 1].index)
        if not repeated:
            return df

        measure_columns = [column for column in _MEASURE_COLUMNS if column in df.columns]
        work = df.copy()
        work["_key"] = key.to_numpy()
        repeated_mask = work["_key"].isin(repeated)
        drop_indexes: List[Any] = []
        dropped = 0
        for _key_value, group_rows in work.loc[repeated_mask].groupby("_key", sort=False):
            merged = self._merge_duplicate_rows(group_rows, measure_columns, rank_column)
            work.loc[merged.name] = merged
            drop_indexes.extend(index for index in group_rows.index if index != merged.name)
            dropped += len(group_rows) - 1
        logger.info(
            "Merged repeated keys in %s; dropped %s extra rows",
            Path(source_file).name,
            dropped,
        )
        if drop_indexes:
            work = work.drop(index=drop_indexes)
        return work.drop(columns=["_key"])

    def _merge_duplicate_rows(
        self,
        rows: pd.DataFrame,
        measure_columns: List[str],
        rank_column: str,
    ) -> pd.Series:
        """Overlay duplicate rows. A number beats a star. The larger number wins."""
        best_index = rows.index[0]
        best_rank = (-1, -1.0)
        for position, (index, row) in enumerate(rows.iterrows()):
            if rank_column:
                rank = _cell_rank(row.get(rank_column), _metric_was_starred(row, rank_column))
            else:
                rank = (0, 0.0)
            # Earlier file order wins a tie.
            if rank > best_rank or (rank == best_rank and position == 0):
                best_rank = rank
                best_index = index
        base = rows.loc[best_index].copy()
        for index, row in rows.iterrows():
            if index == best_index:
                continue
            for column in measure_columns:
                current = _cell_rank(base.get(column), _metric_was_starred(base, column))
                incoming = _cell_rank(row.get(column), _metric_was_starred(row, column))
                if incoming > current:
                    base[column] = row.get(column)
                    flag = _star_column_name(column)
                    if flag in row.index:
                        base[flag] = row.get(flag)
                    elif flag in base.index:
                        base[flag] = False
        return base

    def _put_metric(self, metrics: Dict[str, Any], metric_name: str, column: str, row: pd.Series) -> None:
        """Store one cell. A star is NA. A blank is omitted. A 0 is kept."""
        if column not in row.index and not _metric_was_starred(row, column):
            return
        if _metric_was_starred(row, column):
            metrics[metric_name] = pd.NA
            return
        number = _parse_number(row.get(column, pd.NA))
        if _is_missing(number) or number < 0:
            return
        metrics[metric_name] = int(number)

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        """Extract grade counts and category counts.

        A long category row writes one metric. ``All Students`` on that
        row is ``gifted_participation_count_all_grades``. Any other
        category is ``gifted_category_count_{slug}``, so it does not land
        on a grade metric. A wide row writes each populated grade or
        category column. A ``*`` is included as NA. A blank is omitted.
        A published 0 is included.
        """
        metrics: Dict[str, Any] = {}
        if self._is_long_category_row(row):
            label = row.get("category")
            slug = category_label_slug(label)
            text = "" if _is_missing(label) else " ".join(str(label).strip().lower().replace("_", " ").split())
            if text == "all students":
                self._put_metric(metrics, "gifted_participation_count_all_grades", "students", row)
            elif slug:
                self._put_metric(metrics, f"gifted_category_count_{slug}", "students", row)
            else:
                logger.warning("Unrecognized gifted category label %r", label)
            return metrics

        for column, metric_name in _GRADE_METRICS:
            if column in row.index or _metric_was_starred(row, column):
                self._put_metric(metrics, metric_name, column, row)
        for slug in _CATEGORY_SLUGS:
            if slug in row.index or _metric_was_starred(row, slug):
                self._put_metric(metrics, f"gifted_category_count_{slug}", slug, row)
        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Default metrics for a row the base class treats as fully suppressed."""
        defaults = {metric_name: pd.NA for _column, metric_name in _GRADE_METRICS}
        for slug in _CATEGORY_SLUGS:
            defaults[f"gifted_category_count_{slug}"] = pd.NA
        return defaults

    def _apply_2019_workbook_labels(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """Read the 2018-19 group from ``DGROUP``, not the DEMO code.

        The shared map sends code ``CSG`` to Students with Disabilities
        and drops code ``ACD``. In this workbook ``DGROUP`` says ``CSG``
        is Consolidated Student Group and ``ACD`` is ``Studw/disab.``,
        the disability count (state 3,136; the 2020 disability count is
        3,357). The year-2019 map already turns ``Studw/disab.`` into
        Students with Disabilities and leaves Consolidated Student Group
        as itself. Other ``DGROUP`` labels land on the same groups the
        codes do. Other modules keep the shared yaml.
        """
        if Path(source_file).name.lower() != _WORKBOOK_2019:
            return df
        if "dgroup" not in df.columns:
            return df
        text = df["dgroup"].astype("string").str.strip()
        missing_token = text.str.lower().isin(["<na>", "nan", "none"]).fillna(False)
        present = (df["dgroup"].notna() & text.ne("") & ~missing_token).fillna(False)
        if not bool(present.any()):
            return df
        out = df.copy()
        out.loc[present, "demographic"] = text.loc[present]
        return out

    def _collect_grade_all_students_keys(self, raw_dir: Path) -> Set[Tuple[str, str]]:
        """Schools whose grade file already emits All Students all-grades.

        The category file repeats that cell. Where the grade file has
        the school, the two numbers match, and neither file is a star
        against a number. The category copy is not written. A school
        that is only on the category file, including a published 0, is
        not in this set and is kept. The output CSV is not read back.
        """
        keys: Set[Tuple[str, str]] = set()
        source_dir = raw_dir / self.source_name
        if not source_dir.exists():
            return keys
        for data_file, sheet in self._find_data_files(source_dir):
            if "by_category" in data_file.name.lower():
                continue
            if data_file.stat().st_size == 0:
                continue
            frame = self.normalize_column_names(self._read_data_file(data_file, sheet))
            keys.update(self._all_students_keys_in_frame(frame, data_file.name))
        logger.info(
            "Grade files already hold All Students all-grades for %s schools",
            len(keys),
        )
        return keys

    def _all_students_keys_in_frame(self, df: pd.DataFrame, source_file: str) -> Set[Tuple[str, str]]:
        """(year, school id) where All Students all-grades is a number or a star."""
        if df.empty or "school_code" not in df.columns or "all_grades" not in df.columns:
            return set()
        if "demographic" not in df.columns:
            return set()
        demo = df["demographic"].astype("string").str.strip().str.lower()
        subset = df.loc[demo.isin(_ALL_STUDENTS_LABELS)]
        if subset.empty:
            return set()
        text = subset["all_grades"].astype("string").str.replace(",", "", regex=False).str.strip()
        starred = text.isin(["*", "**"])
        numeric = pd.to_numeric(text.mask(starred), errors="coerce").notna()
        keep = subset.loc[starred.to_numpy() | numeric.to_numpy()]
        if keep.empty:
            return set()
        if "school_year" in keep.columns:
            year_text = keep["school_year"].astype("string").str.strip()
        else:
            year_text = pd.Series("", index=keep.index, dtype="string")
        fallback = self.extract_year(pd.Series({"school_year": ""}), source_file)
        length = year_text.str.len()
        year = year_text.where(length != 8, year_text.str[-4:])
        year = year.where(length.isin([4, 8]), fallback)
        code = keep["school_code"].astype("string").str.strip()
        code = code.str.replace(r"\.0$", "", regex=True)
        usable = (code.ne("") & ~code.str.lower().isin(["nan", "<na>", "none"])).fillna(False)
        pairs = zip(year.loc[usable].astype(str), code.loc[usable].astype(str))
        return set(pairs)

    def _category_total_already_on_grade_file(self, source_file: str, template: Dict[str, Any]) -> bool:
        """True when this category All Students total is already on the grade file."""
        if "by_category" not in Path(source_file).name.lower():
            return False
        key = (str(template.get("year", "")), str(template.get("school_id", "")))
        return key in self._grade_all_students_keys

    def _omit_2020_2023_uninformative_slice(
        self, year: Any, student_group: Any, metric_name: str
    ) -> bool:
        """Skip preschool, grade 14, and Consolidated Student Group for 2020-2023.

        The decision is the year set and these slices. The cell is not
        inspected, so a number on Consolidated Student Group in those
        years is omitted with the stars. A column that happens to be all
        stars is not enough: kindergarten stars in the same files are
        written. 2019 and 2024 onward return False, including a preschool
        or grade 14 star.
        """
        if str(year) not in _OMIT_UNINFORMATIVE_YEARS:
            return False
        if student_group == _OMIT_UNINFORMATIVE_GROUP:
            return True
        return metric_name in _OMIT_UNINFORMATIVE_METRICS

    def convert_to_kpi_format(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """Write a published cell as ``N`` and a ``*`` cell as ``Y``.

        A blank cell is not a row. Repeated school keys are merged first.
        The 2018-19 group is the ``DGROUP`` label. The category total
        uses the grade metric name, and the talent categories use
        ``gifted_category_count_*``. A category All Students total that
        the grade file already holds is not written. Preschool, grade 14,
        and Consolidated Student Group are not written for 2020-2023.
        """
        df = self._apply_2019_workbook_labels(df, source_file)
        df = self._collapse_repeated_keys(df, source_file)
        kpi_rows: List[Dict[str, Any]] = []
        skipped_category_totals = 0
        omitted_uninformative = 0
        for _, row in df.iterrows():
            if self.should_skip_row(row):
                continue
            kpi_template = self.create_kpi_template(row, source_file)
            if kpi_template is None:
                continue
            metrics = self.extract_metrics(row)
            for metric_name, value in metrics.items():
                if (
                    metric_name == _ALL_GRADES_METRIC
                    and self._category_total_already_on_grade_file(source_file, kpi_template)
                ):
                    skipped_category_totals += 1
                    continue
                if self._omit_2020_2023_uninformative_slice(
                    kpi_template.get("year"),
                    kpi_template.get("student_group"),
                    metric_name,
                ):
                    omitted_uninformative += 1
                    continue
                kpi_record = kpi_template.copy()
                kpi_record["metric"] = metric_name
                if _is_missing(value):
                    kpi_record["value"] = pd.NA
                    kpi_record["suppressed"] = "Y"
                else:
                    kpi_record["value"] = float(value)
                    kpi_record["suppressed"] = "N"
                kpi_rows.append(kpi_record)
        if skipped_category_totals:
            logger.info(
                "Left %s category All Students totals off %s; the grade file already has them",
                skipped_category_totals,
                Path(source_file).name,
            )
        if omitted_uninformative:
            logger.info(
                "Left %s preschool, grade 14, and Consolidated Student Group cells off %s; "
                "those slices are starred on every 2020-2023 row",
                omitted_uninformative,
                Path(source_file).name,
            )

        if not kpi_rows:
            logger.warning("No valid KPI rows created")
            return pd.DataFrame()
        kpi_df = pd.DataFrame(kpi_rows)
        available_columns = [col for col in KPI_COLUMNS if col in kpi_df.columns]
        return kpi_df[available_columns]

    def process(self, raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
        """Write KPI rows, leaving out category totals the grade file already has.

        The category file's All Students count is the same total as the
        grade file's All Grades. Both use ``gifted_participation_count_all_grades``.
        Where the two files share a school, the values match and the grade
        file's row is kept. A school that is only on the category file,
        including a published 0, is kept. The grade-file schools are
        collected first, so file order does not matter and the output
        CSV is not read back.
        """
        self._grade_all_students_keys = self._collect_grade_all_students_keys(raw_dir)
        super().process(raw_dir, proc_dir, cfg)


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Process gifted and talented participation data using BaseETL."""
    etl = GiftedTalentedETL("gifted_talented")
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
