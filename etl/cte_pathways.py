"""
CTE Pathways ETL Module

KDE publishes one row per career pathway. ``Pathways Available`` is the
pathway name in every registered year, including the 2018-19 workbook.
It is not a count. The report-card glossary defines a career pathway as a
coherent sequence of academic and CTE courses. Active enrollment is the
number of students enrolled during the school year in at least one valid
course in that pathway. Concentrators are 12th-grade students with at
least two and fewer than four credits in that pathway. Completers are
12th-grade students with four or more credits in that pathway. Those two
statuses are different groups. A student in two pathways is counted on
both rows.

The output is one row per school, year, and student group. There is no
demographic column, so the group is ``All Students``. ``num_cte_pathways``
is the number of pathway names at that school. A starred count still
counts, because the pathway name is published. A published 0 counts.
A row whose enrollment, concentrator, and completer cells are all blank
is not offered and is not counted.

``cte_pathway_enrollment``, ``cte_concentrator_count``, and
``cte_pathway_completer_count`` are the sums of those cells. A total is
stored as value NA with ``suppressed`` Y when any component is ``*``,
because a partial sum is only a lower bound. Blanks are not components.
In 2020-2026 the smallest published count is 3, so a star in those years
can also mean 0-2 or not applicable. 2019 publishes 0 and has no stars.
Those zeros stay. 386 school pathway rows are enrollment 0 and completers
0, so the 2019 school pathway-count mean is 12.04. Without those rows it
is 10.75. Later years sit between 10.4 and 11.4.

``cte_pathway_completer_count`` sums completers on each pathway. It is a
different measure from ``cte_completer_count_grade_12`` in CTE
participation, which counts each 12th-grade completer once. In 2026 the
published state pathway-completer cells sum to at least 36,436. The
participation state count is 19,990.

State and district rows keep the cells KDE published at that level. On
KYRC25 and KYRC26 some state pathway cells do not equal the sum of the
district cells, and the district sum is higher. That difference is
flagged kde-recheck in issue #86. This module does not replace a level
with the sum of the level below.

``cte_pathway_completion_rate`` is completers divided by concentrators,
times 100, rounded to one decimal place. The pipeline calculates it.
KDE does not publish it. Scott approved it on 2026-10-19 as an
existing-practice exception. It is not a share of concentrators, because
the concentrator group (at least two and fewer than four credits) and
the completer group (four or more credits) do not overlap. Any starred
pathway suppresses the rate. 2019 has none because that file has no
concentrator column. Concentrators appear to be counted across grades.
A partial sum is not stored when a component is starred.
``num_cte_pathways`` is a count of published pathway rows. The enrollment,
concentrator, and completer totals are sums across pathways, so a
student in two pathways counts twice.

2020-2023 list some pathways twice, under two program areas, with the
same counts. That pathway is counted once. The 2018-19 workbook uses
``PREPARATORY_STUDENTS_CNT`` instead of concentrators. Those cells are
not concentrators and are not stored. The workbook has no DEMO or DGROUP
column.
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

# Source column after the module mapping, then the KPI it sums into.
_MEASURES: Tuple[Tuple[str, str], ...] = (
    ("active_enrollment", "cte_pathway_enrollment"),
    ("concentrator_students", "cte_concentrator_count"),
    ("pathway_completers", "cte_pathway_completer_count"),
)

# A blank is not a star. ``*`` is recorded before the cell becomes NA.
_STAR_TEXT = {"*", "**"}
_BLANK_TEXT = {"", "N/A", "n/a", "#N/A", "---", "--", "<10", '""', "nan", "None", "<NA>"}

# KYRC23 is school year 20222023. The registered copy is
# career_pathways_2023.csv. A leftover KYRC23 file would write that year
# twice. 29 completer cells are a published count in KYRC23 and ``*`` in
# the historical file.
_REPUBLISH_FILENAMES = {
    "KYRC23_CTE_CAREER_PATHWAYS.CSV",
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
        logger.warning("Ignoring negative CTE pathway count %r", text)
        return "blank", None
    return "number", number


def _merge_cell(current: Cell, new: Cell) -> Tuple[Cell, bool]:
    """Combine two listings of the same pathway.

    Identical listings stay as they are. A blank beside a real cell keeps
    the real cell. A number that disagrees with the other listing, or a
    star beside a number, is a conflict and becomes a star so the school
    total is not a partial sum.
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
    does not check ``dtype == object``. pandas is unpinned.
    """
    cleaned = (
        series.astype("string")
        .str.replace(",", "", regex=False)
        .str.replace('"', "", regex=False)
    )
    return pd.to_numeric(cleaned, errors="coerce")


class CTEPathwaysETL(BaseETL):
    """ETL module for CTE pathway availability.

    KYRC26 uses the same headers as KYRC25. ``Pathways Available`` is the
    pathway name. No temporary OAA extract was registered, and the guessed
    temporary names return 404, so there is no leftover filename to skip.
    """

    # Opt in here, in the same change that accepts the file. BaseETL skips
    # every KYRC26_* CSV until this is True.
    include_kyrc26 = True

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            # Program identification. The name is not a measure.
            'Program Area': 'program_area',
            'PROGRAM AREA': 'program_area',
            'PROGRAM_AREA': 'program_area',
            'Pathways Available': 'pathways_available',
            'PATHWAYS AVAILABLE': 'pathways_available',
            'PATHWAYS_AVAILABLE': 'pathways_available',

            # Active enrollment. KYRC files drop the word Count.
            'Active Enrollment': 'active_enrollment',
            'ACTIVE ENROLLMENT': 'active_enrollment',
            'Active Enrollments Count': 'active_enrollment',
            'ACTIVE ENROLLMENTS COUNT': 'active_enrollment',
            'ACTIVE_ENROLLMENTS_CNT': 'active_enrollment',

            # Concentrators. The 2018-19 workbook has preparatory students
            # instead, and that column is not mapped here.
            'Concentrator Students': 'concentrator_students',
            'CONCENTRATOR STUDENTS': 'concentrator_students',
            'Concentrator Students Count': 'concentrator_students',
            'CONCENTRATOR STUDENTS COUNT': 'concentrator_students',

            # Completers.
            'Pathway Completers': 'pathway_completers',
            'PATHWAY COMPLETERS': 'pathway_completers',
            'Pathway Completers Count': 'pathway_completers',
            'PATHWAY COMPLETERS COUNT': 'pathway_completers',
            'PATHWAY_COMPLETERS_CNT': 'pathway_completers',

            # 2018-19 workbook identity columns. No DEMO or DGROUP column.
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
        }

    def accepts_data_file(self, file_path: Path) -> bool:
        """Skip the KYRC23 republish of 2022-23.

        ``career_pathways_2023.csv`` is the registered 2022-23 file.
        KYRC23 is the same school year and the same pathway keys. 40
        completer cells are a number in KYRC23 and ``*`` in the historical
        file: 29 at schools and 11 on district or state rows. Loading
        both would write 2023 twice.
        """
        if file_path.name.upper() in _REPUBLISH_FILENAMES:
            logger.info(
                "Skipping %s; it republishes 2022-23 already loaded from career_pathways_2023.csv",
                file_path.name,
            )
            return False
        return super().accepts_data_file(file_path)

    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Keep the source text, including stars and thousands commas.

        The base method only rewrites ``dtype == object``. pandas 3
        ``dtype=str`` is StringDtype, so a comma in ``2,546`` would
        survive here only because the parser reads the copied text.
        ``BaseETL.standardize_missing_values`` is not changed. A ``*``
        stays in the copy and is later written with ``suppressed`` Y.
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
        no mapped measure columns, such as a leftover
        ``CAREER_PATHWAYS_17-18.xlsx``, is blank. Those rows are not
        pathways.
        """
        for column, _metric in _MEASURES:
            if not self._measure_is_present(row, column):
                continue
            kind, _number = _classify_cell(self._measure_value(row, column))
            if kind != "blank":
                return False
        return True

    def should_skip_row(self, row: pd.Series) -> bool:
        """Skip a pathway that was not offered.

        These files have no demographic column, so a missing demographic
        is not a reason to skip. District and state rows are kept. A
        blank enrollment, concentrator, and completer means the pathway
        was not offered. A ``*`` is not blank. A published 0 is not blank.
        """
        return self._row_is_blank(row)

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        """Pathway totals are school aggregates.

        ``convert_to_kpi_format`` sums the pathway rows. A single row is
        not a KPI record, because the pathway name is not the metric.
        """
        return {}

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Placeholders for a suppressed pathway record.

        School totals are built in ``convert_to_kpi_format``. These files
        have no Suppressed column.
        """
        return {
            'num_cte_pathways': pd.NA,
            'cte_pathway_enrollment': pd.NA,
            'cte_concentrator_count': pd.NA,
            'cte_pathway_completer_count': pd.NA,
            'cte_pathway_completion_rate': pd.NA,
        }

    def convert_to_kpi_format(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """One row per school, year, student group, and metric.

        Pathway names that are listed twice under two program areas count
        once when the cells agree. A starred component suppresses that
        measure's total. The pathway count itself is stored with
        ``suppressed`` N.
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

            pathway_name = _cell_text(row.get("pathways_available"))
            key = (
                str(kpi_template["school_id"]),
                str(kpi_template["year"]),
                str(kpi_template["student_group"]),
            )
            if key not in groups:
                groups[key] = {"template": kpi_template, "pathways": {}}
                order.append(key)
            pathways: Dict[str, Dict[str, Cell]] = groups[key]["pathways"]
            if pathway_name not in pathways:
                pathways[pathway_name] = {}
            stored = pathways[pathway_name]
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
                        "%s school %s pathway %r has disagreeing %s cells",
                        source_file,
                        key[0],
                        pathway_name,
                        column,
                    )

        if blank_rows:
            logger.info("%s skipped %s blank pathway rows", source_file, blank_rows)
        if conflicts:
            logger.warning("%s pathway cells disagreed in %s listings", source_file, conflicts)

        kpi_rows: List[Dict[str, Any]] = []
        for key in order:
            group = groups[key]
            template = group["template"]
            pathways = group["pathways"]
            count_record = template.copy()
            count_record["metric"] = "num_cte_pathways"
            count_record["value"] = float(len(pathways))
            count_record["suppressed"] = "N"
            kpi_rows.append(count_record)

            totals: Dict[str, Cell] = {}
            for column, metric_name in _MEASURES:
                cells = [item[column] for item in pathways.values() if column in item]
                if not cells:
                    continue
                if any(kind == "star" for kind, _number in cells):
                    totals[column] = ("star", None)
                    kpi_record = template.copy()
                    kpi_record["metric"] = metric_name
                    kpi_record["value"] = pd.NA
                    kpi_record["suppressed"] = "Y"
                    kpi_rows.append(kpi_record)
                    continue
                numbers = [number for kind, number in cells if kind == "number" and number is not None]
                if not numbers:
                    continue
                total = float(sum(numbers))
                totals[column] = ("number", total)
                kpi_record = template.copy()
                kpi_record["metric"] = metric_name
                kpi_record["value"] = total
                kpi_record["suppressed"] = "N"
                kpi_rows.append(kpi_record)

            rate_record = self._completion_rate_record(template, totals)
            if rate_record is not None:
                kpi_rows.append(rate_record)

        if not kpi_rows:
            logger.warning("No valid KPI rows created from %s", source_file)
            return pd.DataFrame()
        kpi_df = pd.DataFrame(kpi_rows)
        available_columns = [col for col in KPI_COLUMNS if col in kpi_df.columns]
        return kpi_df[available_columns]

    def _completion_rate_record(
        self,
        template: Dict[str, Any],
        totals: Dict[str, Cell],
    ) -> Optional[Dict[str, Any]]:
        """Completers divided by concentrators, times 100, rounded to 1 place.

        The pipeline calculates this rate. KDE does not publish it. Scott
        approved it on 2026-10-19 as an existing-practice exception. Any
        starred pathway suppresses it. It is omitted when the concentrator
        total is 0 or was not published, including 2019.
        """
        concentrators = totals.get("concentrator_students")
        completers = totals.get("pathway_completers")
        if concentrators is None or completers is None:
            return None
        record = template.copy()
        record["metric"] = "cte_pathway_completion_rate"
        if concentrators[0] == "star" or completers[0] == "star":
            record["value"] = pd.NA
            record["suppressed"] = "Y"
            return record
        concentrator_total = concentrators[1]
        completer_total = completers[1]
        if concentrator_total is None or completer_total is None or concentrator_total <= 0:
            return None
        record["value"] = round((completer_total / concentrator_total) * 100, 1)
        record["suppressed"] = "N"
        return record


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Process CTE pathway data and convert to KPI format using BaseETL."""
    etl = CTEPathwaysETL('cte_pathways')
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
