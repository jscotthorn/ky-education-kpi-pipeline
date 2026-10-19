"""
Career Readiness Indicators ETL Module

KDE publishes one row per school, district, or state total. There is no
pathway-name column and no program-area column. ``Pathways Available`` is
not in this file in any year, including the 2018-19 workbook. The
indicator headers are counts, not names, so nothing is summed across items
and a school is not aggregated from pathway rows.

Kentucky's accountability page defines postsecondary readiness as a diploma
(or a grade-12 non-graduate) plus one academic or career indicator. This
file is the career-readiness slice of that count:

- Industry certification is an approved industry certificate, license, or
  credential.
- CTE end-of-program assessment is the state assessment tied to articulated
  credit. The 2018-19 workbook does not have this column.
- Dual credit here is a KDE-approved CTE dual-credit course with a grade of
  C or better. It is not the unduplicated dual-credit enrollment in
  ``dual_credit``. The stored name stays
  ``career_readiness_dual_credit_cte_count``.
- Apprenticeship is the TRACK youth apprenticeship.
- Work-based learning, from 2023 Senate Bill 25, is one count of a
  KBE-approved co-op or internship of at least 300 hours aligned to a
  credential or associate degree. The file does not split co-op from
  internship.
- Exceptional work experience is the older column on the 2018-19 workbook
  and on the 2022 and 2023 historical files. Those two historical columns
  are blank on every row. It is stored on its own metric. It is not filled
  in as work-based learning.
- More than one indicator, no indicator, and total graduates are the counts
  KDE published under those headers.

In 2022 the six columns industry, end-of-program, dual credit,
apprenticeship, more than one, and no indicator add to total graduates on
every row when a blank is read as 0. In 2023 and later the published
indicator cells overlap, so they are not a partition. This module does not
add them and does not turn a blank into 0. The published total-graduates
cell is stored on its own. KYRC24 does not have that column.

A ``*`` is stored as value NA with ``suppressed`` Y. The star is read
before the cell is coerced. A blank cell is not stored. A published 0 is
kept. KYRC24 publishes 0. The 2019-2023 files publish no 0 and no star.
The smallest published count is 1, and a blank is an empty cell. KYRC25
and KYRC26 publish no 0 and no blank. On the indicator columns the
smallest published count is 3, so a star there can also mean 0-2 or not
applicable. Total graduates is published down to 13 in KYRC25, with one
starred district, and down to 10 in KYRC26, with no star.

There is no demographic column. ``student_group`` is ``All Students``.
The 2018-19 workbook has no DEMO or DGROUP column. State rows are district
``All Districts`` and school id ``999000`` on the KYRC files, and district
``State`` and school id ``999`` on the 2019-2023 files. Those ids are not
padded.

Each level keeps the cells KDE published. Where every district cell is a
number, the state total still does not equal the sum of the districts.
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

# Internal column after the module mapping, then the KPI name.
# These are published counts. They are not added together.
_MEASURES: Tuple[Tuple[str, str], ...] = (
    ("total_graduates", "career_readiness_total_graduates_count"),
    ("industry_certification", "career_readiness_industry_certification_count"),
    ("cte_end_of_program_assessment", "career_readiness_cte_end_of_program_assessment_count"),
    ("apprenticeship", "career_readiness_apprenticeship_count"),
    ("dual_credit_cte", "career_readiness_dual_credit_cte_count"),
    ("exceptional_work_experience", "career_readiness_exceptional_work_experience_count"),
    ("work_based_learning", "career_readiness_work_based_learning_count"),
    ("more_than_one_indicator", "career_readiness_more_than_one_indicator_count"),
    ("no_indicator", "career_readiness_no_indicator_count"),
)

# A blank is not a star. ``*`` is recorded before the cell becomes NA.
_STAR_TEXT = {"*", "**"}
_BLANK_TEXT = {"", "N/A", "n/a", "#N/A", "---", "--", "<10", '""', "nan", "None", "<NA>"}

# KYRC23 is school year 20222023. The registered copy is
# career_readiness_indicators_2023.csv. Padding a 3-digit district code
# with 000 makes the keys match. Every shared measure cell matches.
# KYRC23 drops total graduates, renames the all-blank exceptional-work
# column to work-based learning, and writes school id 999000. Loading
# both would write 2023 twice.
# CTE_TRANSITION_READINESS_17-18.xlsx is school year 20172018. It is
# earlier than this series. Its industry-certification column would
# otherwise write 2018 rows. It is not registered.
# The guessed OAA names were never on this module. They are 404. A leftover
# copy must not write 2024, 2025, or 2026 a second time.
_SKIP_FILENAMES = {
    "KYRC23_CTE_CAREER_READINESS_INDICATORS.CSV",
    "CTE_TRANSITION_READINESS_17-18.XLSX",
    "CAREER_READINESS_INDICATORS_2024.CSV",
    "CAREER_READINESS_INDICATORS_2025.CSV",
    "CAREER_READINESS_INDICATORS_2026.CSV",
    "CTE_CAREER_READINESS_INDICATORS_2024.CSV",
    "CTE_CAREER_READINESS_INDICATORS_2025.CSV",
    "CTE_CAREER_READINESS_INDICATORS_2026.CSV",
}

Cell = Tuple[str, Optional[float]]


def _raw_column(column: str) -> str:
    """Column that keeps the source text after missing-value cleanup."""
    return f"_raw_{column}"


def _cell_text(value: Any) -> str:
    """Source text, or empty when the cell is missing.

    ``bool(pd.NA)`` raises. A missing cell is blank, not a number.
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

    ``star`` is a privacy star. ``blank`` is empty or not a number.
    ``number`` is a parsed count, including a published 0. Commas are
    removed for object text and for pandas StringDtype.
    """
    text = _cell_text(value)
    if text in _STAR_TEXT:
        return "star", None
    if text in _BLANK_TEXT:
        return "blank", None
    cleaned = text.replace(",", "").replace('"', "")
    try:
        number = float(cleaned)
    except ValueError:
        return "blank", None
    if number < 0:
        logger.warning("Ignoring negative career readiness count %r", text)
        return "blank", None
    return "number", number


def _merge_cell(current: Cell, new: Cell) -> Tuple[Cell, bool]:
    """Combine two listings of the same school and measure.

    No registered file repeats a school code. Identical listings stay as
    they are. A blank beside a real cell keeps the real cell. A number
    that disagrees, or a star beside a number, becomes a star so the
    stored cell is not a sum of the two listings.
    """
    current_kind, current_number = current
    new_kind, new_number = new
    if current_kind == "blank":
        return new, False
    if new_kind == "blank":
        return current, False
    if current_kind == new_kind and (current_kind == "star" or current_number == new_number):
        return current, False
    return ("star", None), True


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


class CareerReadinessIndicatorsETL(BaseETL):
    """ETL module for CTE career readiness indicator counts.

    KYRC26 uses the same headers as KYRC25. The guessed temporary OAA
    names return 404. ``accepts_data_file`` still skips them, and skips
    the KYRC23 republish of 2022-23.
    """

    # Opt in here, in the same change that accepts the file. BaseETL skips
    # every KYRC26_* CSV until this is True.
    include_kyrc26 = True

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            # 2018-19 workbook identity. No DEMO or DGROUP column.
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

            # Total graduates. Absent on KYRC23 and KYRC24. The 2017-18
            # workbook's TOTALGRAD_ENROLL_CNT is enrollment, not graduates,
            # and is not mapped.
            'Total Graduates': 'total_graduates',
            'TOTAL GRADUATES': 'total_graduates',
            'TOTAL_GRADUATES_CNT': 'total_graduates',

            # Industry certification. The 2022 and 2023 files are plural.
            'Industry Certification': 'industry_certification',
            'INDUSTRY CERTIFICATION': 'industry_certification',
            'INDUSTRY CERTIFICATIONS': 'industry_certification',
            'IND_CERTS_CNT': 'industry_certification',

            # End-of-program assessment. Not on the 2018-19 workbook.
            'CTE End-of-Program Assessment': 'cte_end_of_program_assessment',
            'CTE END-OF-PROGRAM ASSESSMENT': 'cte_end_of_program_assessment',
            'CTE END OF PROGRAM ASSESSMENT': 'cte_end_of_program_assessment',

            'Apprenticeship': 'apprenticeship',
            'APPRENTICESHIP': 'apprenticeship',
            'APPRENTICESHIP_CNT': 'apprenticeship',

            # CTE dual credit. Not dual_credit enrollment.
            'Dual Credit': 'dual_credit_cte',
            'DUAL CREDIT': 'dual_credit_cte',
            'DUAL_CREDIT_CNT': 'dual_credit_cte',

            # Older column. Blank on every 2022 and 2023 row.
            'Exceptional Work Experience': 'exceptional_work_experience',
            'EXCEPTIONAL WORK EXPERIENCE': 'exceptional_work_experience',
            'EXP_WORK_EXPERIENCE_CNT': 'exceptional_work_experience',

            # One published count. Not split into co-op and internship.
            'Work-Based Learning': 'work_based_learning',
            'WORK-BASED LEARNING': 'work_based_learning',
            'WORK BASED LEARNING': 'work_based_learning',

            'More Than One Indicator': 'more_than_one_indicator',
            'MORE THAN ONE INDICATOR': 'more_than_one_indicator',
            'MORE_THAN_ONE_IND_CNT': 'more_than_one_indicator',

            'No Indicator': 'no_indicator',
            'NO INDICATOR': 'no_indicator',
            'NO_INDICATOR_CNT': 'no_indicator',
        }

    def accepts_data_file(self, file_path: Path) -> bool:
        """Skip a republish, an earlier workbook, and leftover temp names.

        ``career_readiness_indicators_2023.csv`` is the registered 2022-23
        file. KYRC23 is the same school year and the same measure cells.
        The 2017-18 workbook is earlier than this series. The guessed OAA
        names are 404 and were never registered; a leftover copy would
        write a year the permanent file already writes.
        """
        if file_path.name.upper() in _SKIP_FILENAMES:
            logger.info(
                "Skipping %s; it republishes a year already loaded or is outside this series",
                file_path.name,
            )
            return False
        return super().accepts_data_file(file_path)

    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Keep the source text, including stars and thousands commas.

        The base method only rewrites ``dtype == object``. pandas 3
        ``dtype=str`` is StringDtype, so a comma in a count would survive
        here only because the parser reads the copied text. A ``*`` stays
        in the copy and is later written with ``suppressed`` Y.
        ``BaseETL.standardize_missing_values`` is not changed.
        """
        for column, _metric in _MEASURES:
            if column in df.columns:
                df[_raw_column(column)] = df[column].astype("string")
        return super().standardize_missing_values(df)

    def _measure_value(self, row: pd.Series, column: str) -> Any:
        """Source text for one measure, from the copy when it exists."""
        raw = _raw_column(column)
        if raw in row.index:
            return row.get(raw)
        if column in row.index:
            return row.get(column)
        return pd.NA

    def _measure_is_present(self, row: pd.Series, column: str) -> bool:
        return _raw_column(column) in row.index or column in row.index

    def _row_is_blank(self, row: pd.Series) -> bool:
        """True when every published measure cell is empty.

        A ``*`` is a published cell. A 0 is a published cell. A file with
        no mapped measure columns is blank. Those rows are not indicators.
        """
        for column, _metric in _MEASURES:
            if not self._measure_is_present(row, column):
                continue
            kind, _number = _classify_cell(self._measure_value(row, column))
            if kind != "blank":
                return False
        # No mapped measure column, or every measure cell is empty.
        return True

    def should_skip_row(self, row: pd.Series) -> bool:
        """Skip a school that published no indicator cells.

        These files have no demographic column, so a missing demographic
        is not a reason to skip. District and state rows are kept. A row
        whose measure cells are all blank was not offered. A ``*`` is not
        blank. A published 0 is not blank.
        """
        return self._row_is_blank(row)

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        """Per-metric values are written in ``convert_to_kpi_format``.

        One source row can mix a published count and a star. The base
        converter has one suppressed flag for the whole row, so it cannot
        store both.
        """
        return {}

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Placeholders for a suppressed record.

        These files have no Suppressed column. A starred cell is written
        from ``convert_to_kpi_format`` with ``suppressed`` Y.
        """
        return {metric: pd.NA for _column, metric in _MEASURES}

    def convert_to_kpi_format(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """One row per school, year, student group, and published measure.

        A star suppresses that measure only. Another measure on the same
        school stays as published. Two listings of the same school are not
        added. No registered file has that repeat.
        """
        groups: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        order: List[Tuple[str, str, str]] = []
        conflicts = 0
        blank_rows = 0

        for _, row in df.iterrows():
            if self.should_skip_row(row):
                blank_rows += 1
                continue
            kpi_template = self.create_kpi_template(row, source_file)
            if kpi_template is None:
                continue

            key = (
                str(kpi_template["school_id"]),
                str(kpi_template["year"]),
                str(kpi_template["student_group"]),
            )
            if key not in groups:
                groups[key] = {"template": kpi_template, "cells": {}}
                order.append(key)
            stored: Dict[str, Cell] = groups[key]["cells"]
            for column, _metric in _MEASURES:
                if not self._measure_is_present(row, column):
                    continue
                cell = _classify_cell(self._measure_value(row, column))
                if column not in stored:
                    stored[column] = cell
                    continue
                merged, conflict = _merge_cell(stored[column], cell)
                stored[column] = merged
                if conflict:
                    conflicts += 1
                    logger.warning(
                        "%s school %s has disagreeing %s cells",
                        source_file,
                        key[0],
                        column,
                    )

        if blank_rows:
            logger.info("%s skipped %s blank career-readiness rows", source_file, blank_rows)
        if conflicts:
            logger.warning("%s career-readiness cells disagreed in %s listings", source_file, conflicts)

        kpi_rows: List[Dict[str, Any]] = []
        for key in order:
            group = groups[key]
            template = group["template"]
            for column, metric_name in _MEASURES:
                if column not in group["cells"]:
                    continue
                kind, number = group["cells"][column]
                if kind == "blank":
                    continue
                kpi_record = template.copy()
                kpi_record["metric"] = metric_name
                if kind == "star":
                    kpi_record["value"] = pd.NA
                    kpi_record["suppressed"] = "Y"
                else:
                    kpi_record["value"] = float(number) if number is not None else pd.NA
                    kpi_record["suppressed"] = "N"
                kpi_rows.append(kpi_record)

        if not kpi_rows:
            logger.warning("No valid KPI rows created from %s", source_file)
            return pd.DataFrame()
        kpi_df = pd.DataFrame(kpi_rows)
        available_columns = [col for col in KPI_COLUMNS if col in kpi_df.columns]
        return kpi_df[available_columns]


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Process career readiness indicators and convert to KPI format."""
    etl = CareerReadinessIndicatorsETL('career_readiness_indicators')
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
    ).model_dump()

    transform(raw_dir, proc_dir, test_config)
