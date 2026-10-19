"""
Kindergarten Readiness ETL Module

Refactored to use BaseETL for standardized processing and KPI generation.
Handles kindergarten readiness data in both percentage and count formats
across multiple years.

KYRC24, KYRC25, and KYRC26 composites name the measure columns like counts
(``Ready With Interventions``, ``Ready``, ``Ready With Enrichments``,
``Total Ready``). The cells are percents. Each rate is that published
percent. ``kindergarten_readiness_rate`` is ``Total Ready``. The component
rates and the prior-setting rates are the published cells too. Those files
do not emit ``_count`` or ``_total`` metrics. There is no tested count on
the file. Do not divide a percent by the sum of the components, and do not
divide it by a tested count.

The older percent files and the workbooks publish a percent and, in some
years, ``NUMBER TESTED``. The rate is that percent. The total is the
published tested count. The existing count is ``round(percent / 100 *
tested)``. That headcount is an estimate of a figure KDE did not publish:
exact when the tested count is below about 1,000, and about +/-24 students
at state level, because the percent is rounded to one decimal.

``Kindergarten_Screen_2025.CSV`` is 404. The permanent KYRC25 composite is
the 2024-25 source. A leftover temporary extract for 2024, 2025, or 2026
would write that year a second time.
"""
from pathlib import Path
import pandas as pd
from typing import Dict, Any, Optional
import logging

import sys

# Add etl directory to path for imports
etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from base_etl import BaseETL, Config


def _slugify(value: Any) -> str:
    """Convert a string to a metric-friendly slug."""
    if pd.isna(value):
        return "unknown"
    slug = (
        str(value)
        .lower()
        .strip()
        .replace(" ", "_")
        .replace("/", "_")
        .replace("-", "_")
    )
    slug = slug.replace("__", "_")
    return "".join(ch for ch in slug if ch.isalnum() or ch == "_")


def _is_string_like(series: pd.Series) -> bool:
    """True for object text and for pandas StringDtype.

    ``read_csv(..., dtype=str)`` and ``read_excel(..., dtype=str)`` on
    pandas 3 return StringDtype, not object. A comma in ``6,104`` has to
    be removed before ``to_numeric``. pandas 2.3.3 still returns object.
    pandas is unpinned.
    """
    return series.dtype == object or bool(pd.api.types.is_string_dtype(series))


def _strip_commas(series: pd.Series) -> pd.Series:
    """Remove thousands separators from a text column.

    ``astype("string")`` keeps missing values as ``<NA>``. ``astype(str)``
    turns them into the literal ``"nan"``.
    """
    if not _is_string_like(series):
        return series
    return series.astype("string").str.replace(",", "", regex=False)


logger = logging.getLogger(__name__)

# Temporary OAA extracts. The 2025 file is 404. The 2026 file is live and
# uses the percent headers this module already maps, so a leftover copy
# would write year 2026 beside the KYRC26 composite. The match is
# case-insensitive. ``kindergarten_screen_2023.csv`` and earlier stay.
_LEFTOVER_TEMPORARY = {
    "kindergarten_screen_2024.csv",
    "kindergarten_screen_2025.csv",
    "kindergarten_screen_2026.csv",
}

# Datasets KYRC23 is school year 20222023, the same year as
# ``kindergarten_screen_2023.csv``. Where both files publish a number, the
# total-ready cells match. Loading both would write ``kindergarten_readiness_rate``
# twice.
_REPUBLISHED_COMPOSITE = {
    "kyrc23_asmt_kindergarten_screen_composite.csv",
}

# Coversheet of KSCREEN_18-19.xlsx and KSCREEN_17-18.xlsx.
# ACD is Disabilities (IEP). CSG is Consolidated Student Group.
# ACO is No disabilities. The shared map sends CSG to Students with
# Disabilities and drops ACD and ACO. These workbooks have no DGROUP
# column, so the coversheet is the label. The shared yaml is unchanged.
_WORKBOOK_FILES = {
    "kscreen_18-19.xlsx",
    "kscreen_17-18.xlsx",
}
_WORKBOOK_DEMO_LABELS = {
    "ACD": "Students with Disabilities (IEP)",
    "CSG": "Consolidated Student Group",
    "ACO": "Students without IEP",
}

# 2018-19 publishes these names on ``---COOP Total---`` rows. 2017-18
# publishes the same nine totals as ``---District Total---`` with a blank
# district name and school code 901-909. 909 has no abbreviation in 2019.
_COOP_TOTALS = {
    "901": ("Central Kentucky Educational Cooperative", "CKEC"),
    "902": ("Green River Regional Educational Cooperative", "GRREC"),
    "903": ("Kentucky Educational Development Corporation", "KEDC"),
    "904": ("Kentucky Valley Educational Corporation", "KVEC"),
    "905": ("Northern Kentucky Cooperative for Educational Services", "NKCES"),
    "906": ("Ohio Valley Educational Cooperative", "OVEC"),
    "907": ("Southeast/Southcentral Education Cooperative", "SESC"),
    "908": ("West Kentucky Educational Cooperative", "WKEC"),
    "909": ("Greater Louisville Educational Cooperative", None),
}

_MISSING_TEXT = {"", "nan", "none", "<na>", "nat"}

_COUNT_COLUMNS = (
    "ready_with_interventions_count",
    "ready_count",
    "ready_with_enrichments_count",
)

# KYRC24-26 composites. The rate is the published percent. The old _count
# and _total metrics held those same percents, so they are not emitted.
_COMPOSITE_RATE_METRICS = (
    "kindergarten_readiness_rate",
    "kindergarten_ready_with_interventions_rate",
    "kindergarten_ready_rate",
    "kindergarten_ready_with_enrichments_rate",
)

_MEASURE_COLUMNS = _COUNT_COLUMNS + (
    "total_ready_count",
    "total_percent_ready",
    "ready_with_interventions_percent",
    "ready_percent",
    "ready_with_enrichments_percent",
    "number_tested",
)


def _published_percent(value: Any) -> float:
    """Return the percent KDE published, with no further arithmetic.

    Dividing by the sum of the rounded components turned a published 28
    into ``28.000000000000004`` and a published 50 into ``49.50495...``.
    ``float`` of a whole-number percent is the published value.
    """
    return float(value)


def _prior_is_all(row: pd.Series) -> bool:
    """True when the row is the all-settings total."""
    prior_setting = row.get("prior_setting")
    if pd.isna(prior_setting):
        return True
    return str(prior_setting).strip().lower() == "all students"


def _demographic_is_all(row: pd.Series) -> bool:
    return str(row.get("demographic", "")).strip().lower() == "all students"


def _text_or_blank(value: Any) -> str:
    """Return stripped text, or ``""`` when the cell is missing.

    pandas 2 ``astype(str)`` turns a missing county into the literal
    ``"nan"``. pandas 3 leaves it blank. Both are empty.
    """
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    if text.lower() in _MISSING_TEXT:
        return ""
    return text


def _has_count_shape(row: pd.Series) -> bool:
    return all(column in row.index for column in _COUNT_COLUMNS)


def clean_readiness_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate readiness percentage and count fields.

    Thousands separators are removed before ``to_numeric``. The 2018-19
    and 2017-18 workbooks store tested counts as ``6,104``. ``to_numeric``
    would otherwise turn that cell into missing on pandas 2 and pandas 3,
    and the production Int64 cast would then drop the count.
    """
    percent_cols = [c for c in df.columns if "percent" in c.lower() and "ready" in c.lower()] + [
        "total_ready_count"
    ]
    for col in percent_cols:
        if col in df.columns:
            df[col] = _strip_commas(df[col])
            df[col] = pd.to_numeric(df[col], errors="coerce")
            invalid = (df[col] < 0) | (df[col] > 100)
            if invalid.any():
                df.loc[invalid, col] = pd.NA

    count_cols = [
        "ready_with_interventions_count",
        "ready_count",
        "ready_with_enrichments_count",
        "number_tested",
        "enrollment",
    ]
    for col in count_cols:
        if col in df.columns:
            df[col] = _strip_commas(df[col])
            df[col] = pd.to_numeric(df[col], errors="coerce")
            invalid = df[col] < 0
            if invalid.any():
                df.loc[invalid, col] = pd.NA

    return df


class KindergartenReadinessETL(BaseETL):
    """ETL module for kindergarten readiness.

    KYRC26 uses the same composite headers as KYRC24 and KYRC25.
    ``include_kyrc26`` opts this module into that file.
    """

    # Opt in here, in the same change that accepts the file. BaseETL skips
    # every KYRC26_* CSV until this is True.
    include_kyrc26 = True

    def extract_school_id(self, row: pd.Series) -> str:
        """Keep the nine co-op totals distinct.

        ``KSCREEN_18-19.xlsx`` publishes a ``---COOP Total---`` row for each
        co-op with school code ``000``. The shared extractor would store all
        nine under that one code. The published co-op code (901-909) is the
        identifier on the row. 2017-18 publishes the same nine totals with
        school code 901-909 already, and ``_label_2017_coop_total`` renames
        those rows to ``---COOP Total---`` before this runs. A school row
        still uses its school code.
        """
        school_name = row.get("school_name")
        if pd.notna(school_name) and str(school_name).strip() == "---COOP Total---":
            code = row.get("co_op_code")
            if pd.isna(code) or str(code).strip() == "":
                code = row.get("district_number")
            if pd.notna(code) and str(code).strip() != "":
                return self._clean_school_id(code)
        return super().extract_school_id(row)

    def accepts_data_file(self, file_path: Path) -> bool:
        """Skip a leftover temporary extract and the KYRC23 republish.

        ``Kindergarten_Screen_2024.CSV``, ``Kindergarten_Screen_2025.CSV``,
        and ``Kindergarten_Screen_2026.CSV`` use ``Total Percent Ready``
        and ``PRIOR_SETTING``, which this module already maps. The 2025
        file is 404. The 2026 file is still on the temporary host. A
        leftover copy would emit a second year beside the KYRC composite,
        under 3-digit codes and district ``State``.

        ``KYRC23_ASMT_Kindergarten_Screen_Composite.csv`` is school year
        20222023. ``kindergarten_screen_2023.csv`` already loads that year,
        and the published totals match. Historical names through 2023 stay.
        """
        name = file_path.name.lower()
        if name in _LEFTOVER_TEMPORARY:
            logger.info(
                "Skipping %s; the permanent KYRC composite is the source",
                file_path.name,
            )
            return False
        if name in _REPUBLISHED_COMPOSITE:
            logger.info(
                "Skipping %s; kindergarten_screen_2023.csv is the 2022-23 source",
                file_path.name,
            )
            return False
        return super().accepts_data_file(file_path)

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            # Readiness metrics (percentage format)
            "TOTAL PERCENT READY": "total_percent_ready",
            "Total Percent Ready": "total_percent_ready",

            # Readiness metrics (count format - older files)
            "Ready With Interventions": "ready_with_interventions_count",
            "Ready": "ready_count",
            "Ready With Enrichments": "ready_with_enrichments_count",
            "Total Ready": "total_ready_count",

            # 2025 file uses "Percent" prefix for these
            "Percent Ready with Interventions": "ready_with_interventions_percent",
            "Percent Ready": "ready_percent",
            "Percent Ready With Enrichments": "ready_with_enrichments_percent",

            # Test counts
            "NUMBER TESTED": "number_tested",
            "Number Tested": "number_tested",

            # Suppression
            "Suppressed": "suppressed",
            "SUPPRESSED": "suppressed",

            # Prior setting (structure changed in 2025)
            "Prior Setting": "prior_setting",
            "PRIOR SETTING": "prior_setting",
            "PRIOR_SETTING": "prior_setting",  # 2025 uses uppercase with underscore

            # Historical xlsx format (2018-19) - KSCREEN
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
            'DEMOGRAPHIC': 'demographic',
            'ENROLLMENT': 'enrollment',
            'NUMTESTED': 'number_tested',
            'PARTICIPATIONRATE': 'participation_rate',
            'NOTREADY': 'not_ready_percent',  # % not ready
            'READY': 'ready_percent',  # % ready (plain)
            'READYWITHENRICHMENTS': 'ready_with_enrichments_percent',
            'TOTALREADY': 'total_percent_ready',  # % total ready
        }

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        metrics: Dict[str, Any] = {}

        prior_setting = row.get("prior_setting")
        prior_slug = _slugify(prior_setting) if pd.notna(prior_setting) else None
        is_prior_all = _prior_is_all(row)

        # KYRC24-26 composites. The three component columns are present and
        # at least one published percent is on the row. Store those percents
        # as rates. A fully blank row falls through so a Y row still gets
        # the rate placeholders.
        if _has_count_shape(row) and any(
            pd.notna(row.get(column))
            for column in (*_COUNT_COLUMNS, "total_ready_count")
        ):
            if is_prior_all:
                if pd.notna(row.get("ready_with_interventions_count")):
                    metrics["kindergarten_ready_with_interventions_rate"] = (
                        _published_percent(row["ready_with_interventions_count"])
                    )
                if pd.notna(row.get("ready_count")):
                    metrics["kindergarten_ready_rate"] = _published_percent(
                        row["ready_count"]
                    )
                if pd.notna(row.get("ready_with_enrichments_count")):
                    metrics["kindergarten_ready_with_enrichments_rate"] = (
                        _published_percent(row["ready_with_enrichments_count"])
                    )
                if pd.notna(row.get("total_ready_count")):
                    metrics["kindergarten_readiness_rate"] = _published_percent(
                        row["total_ready_count"]
                    )
            elif (
                prior_slug
                and _demographic_is_all(row)
                and pd.notna(row.get("total_ready_count"))
            ):
                metrics[f"kindergarten_{prior_slug}_rate"] = _published_percent(
                    row["total_ready_count"]
                )
            return metrics

        # Percentage formats (check 2025 format first, then fallback to older formats)
        rate = row.get("total_percent_ready")
        if pd.isna(rate):
            rate = row.get("total_ready_count")  # some 2024 files label rate this way

        # 2025 file has additional percentage columns we can extract
        rwi_percent = row.get("ready_with_interventions_percent")
        r_percent = row.get("ready_percent")
        rwe_percent = row.get("ready_with_enrichments_percent")

        total = row.get("number_tested")
        if pd.notna(total):
            total = float(total)

        if is_prior_all and pd.notna(rate):
            metrics["kindergarten_readiness_rate"] = float(rate)
        if is_prior_all and pd.notna(total):
            metrics["kindergarten_readiness_total"] = total
            if pd.notna(rate):
                # Existing headcount. KDE publishes the percent and the
                # tested count. ``round(percent / 100 * tested)`` is exact
                # below about 1,000 tested and about +/-24 at state level.
                metrics["kindergarten_readiness_count"] = round(
                    (float(rate) / 100) * total
                )
            elif str(row.get("suppressed", "N")).strip() == "Y":
                # All Students prior, suppressed Y, a published NUMBER TESTED,
                # and a blank percent. The total makes this dict non-empty,
                # so get_suppressed_metric_defaults does not run and the rate
                # and count would be dropped. Name them here. The converter
                # stores every metric on a Y row as NA, including the tested
                # count. Storing NUMBER TESTED as NA predates this change.
                metrics["kindergarten_readiness_rate"] = pd.NA
                metrics["kindergarten_readiness_count"] = pd.NA

        if (
            prior_slug
            and not is_prior_all
            and _demographic_is_all(row)
            and pd.notna(total)
            and pd.notna(rate)
        ):
            metrics[f"kindergarten_{prior_slug}_rate"] = float(rate)
            metrics[f"kindergarten_{prior_slug}_count"] = round(
                (float(rate) / 100) * total
            )

        # 2025 file breakdown percentages (optional - extract if available)
        if is_prior_all and pd.notna(rwi_percent):
            metrics["kindergarten_ready_with_interventions_rate"] = float(rwi_percent)
        if is_prior_all and pd.notna(r_percent):
            metrics["kindergarten_ready_rate"] = float(r_percent)
        if is_prior_all and pd.notna(rwe_percent):
            metrics["kindergarten_ready_with_enrichments_rate"] = float(rwe_percent)

        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Placeholders for the metrics a published row of this shape would emit.

        A suppressed prior-setting row used to emit the overall readiness
        metrics. Every prior setting then wrote the same key, so one school
        and group had several ``kindergarten_readiness_rate`` rows. The
        overall metrics belong to prior setting ``All Students``. Another
        prior setting, for demographic ``All Students`` only, gets the
        prior-setting metric. A starred row for any other demographic emits
        nothing here, which is what ``extract_metrics`` already does when
        the cell is a number.
        """
        metrics: Dict[str, Any] = {}
        if _prior_is_all(row):
            if _has_count_shape(row):
                for name in _COMPOSITE_RATE_METRICS:
                    metrics[name] = pd.NA
                return metrics
            if "total_percent_ready" in row.index or "total_ready_count" in row.index:
                metrics["kindergarten_readiness_rate"] = pd.NA
            if "number_tested" in row.index:
                metrics["kindergarten_readiness_total"] = pd.NA
                metrics["kindergarten_readiness_count"] = pd.NA
            if "ready_with_interventions_percent" in row.index:
                metrics["kindergarten_ready_with_interventions_rate"] = pd.NA
            if "ready_percent" in row.index:
                metrics["kindergarten_ready_rate"] = pd.NA
            if "ready_with_enrichments_percent" in row.index:
                metrics["kindergarten_ready_with_enrichments_rate"] = pd.NA
            return metrics

        if not _demographic_is_all(row):
            return metrics

        # Composite files store the prior-setting percent as a rate and do
        # not emit a count. Percent files need ``number_tested`` before
        # extract_metrics will emit the prior-setting rate, so a suppressed
        # row follows that and still emits the rounded-count placeholder.
        count_shape = _has_count_shape(row)
        percent_with_tested = "number_tested" in row.index and (
            "total_percent_ready" in row.index or "total_ready_count" in row.index
        )
        if count_shape or percent_with_tested:
            slug = _slugify(row.get("prior_setting"))
            metrics[f"kindergarten_{slug}_rate"] = pd.NA
            if percent_with_tested and not count_shape:
                metrics[f"kindergarten_{slug}_count"] = pd.NA
        return metrics

    def _measures_are_blank(self, row: pd.Series) -> bool:
        """True when every readiness measure on the row is empty.

        A published 0 is not blank. A ``*`` has already been coerced to
        missing by ``clean_readiness_data``.
        """
        present = [column for column in _MEASURE_COLUMNS if column in row.index]
        if not present:
            return False
        for column in present:
            value = row.get(column)
            if pd.isna(value):
                continue
            if str(value).strip() == "":
                continue
            return False
        return True

    def should_skip_row(self, row: pd.Series) -> bool:
        """Skip a row that is not suppressed and has no readiness values.

        notes/8: a blank (not ``Y``, every value empty) is not offered and
        is not stored. A ``Y`` row still emits placeholders. A published 0
        is kept.
        """
        if super().should_skip_row(row):
            return True
        suppressed = row.get("suppressed", "N")
        if suppressed == "Y":
            return False
        return self._measures_are_blank(row)

    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        df = super().standardize_missing_values(df)
        df = clean_readiness_data(df)
        if "suppressed" in df.columns:
            df["suppressed"] = df["suppressed"].replace({"Yes": "Y", "No": "N"}).fillna("N")
        else:
            df["suppressed"] = "N"
        return df

    def _apply_workbook_labels(self, row: pd.Series, source_file: str) -> pd.Series:
        """Read ACD, CSG, and ACO from the workbook coversheet.

        The 2018-19 and 2017-18 coversheets say ``ACD`` is Disabilities
        (IEP), ``CSG`` is Consolidated Student Group, and ``ACO`` is No
        disabilities. The shared map does the reverse for ``CSG`` and
        drops ``ACD`` and ``ACO``. The data files contain ``ACD`` and do
        not contain ``CSG``. The 2019 state disability count is 6,104
        tested, total ready 34.2. The 2020 disability count is 7,173
        tested, total ready 35.0. Issue #76. The shared yaml is unchanged.
        """
        if Path(source_file).name.lower() not in _WORKBOOK_FILES:
            return row
        code = row.get("demographic")
        if pd.isna(code):
            return row
        label = _WORKBOOK_DEMO_LABELS.get(str(code).strip())
        if not label:
            return row
        updated = row.copy()
        updated["demographic"] = label
        return updated

    def _label_2017_coop_total(self, row: pd.Series, source_file: str) -> pd.Series:
        """Label 2017-18 co-op totals the way 2019 publishes them.

        Districts 901-909 in ``KSCREEN_17-18.xlsx`` are ``---District Total---``
        with a blank district name. Stored that way they look like nine
        unnamed districts. 2019 already publishes the long co-op name,
        ``---COOP Total---``, and the abbreviation (blank for 909). A 2019
        row has a district name, so this does not rewrite it.
        """
        if Path(source_file).name.lower() not in _WORKBOOK_FILES:
            return row
        code = _text_or_blank(row.get("district_number"))
        if code.endswith(".0"):
            code = code[:-2]
        if code not in _COOP_TOTALS:
            return row
        if _text_or_blank(row.get("district_name")):
            return row
        if _text_or_blank(row.get("school_name")) != "---District Total---":
            return row
        long_name, abbreviation = _COOP_TOTALS[code]
        updated = row.copy()
        updated["district_name"] = long_name
        updated["school_name"] = "---COOP Total---"
        updated["co_op_code"] = code
        updated["co_op"] = abbreviation if abbreviation else pd.NA
        return updated

    def create_kpi_template(self, row: pd.Series, source_file: str) -> Optional[Dict[str, Any]]:
        """
        Override to handle kindergarten-specific suppression logic.

        The workbook ``Y`` flag means some domain cell is suppressed. A
        published composite percent on that row is kept and stored as
        suppressed ``N``. A star with no composite percent stays ``Y``.
        """
        row = self._apply_workbook_labels(row, source_file)
        row = self._label_2017_coop_total(row, source_file)
        template = super().create_kpi_template(row, source_file)

        # If template is None (e.g., filtered demographic), return None
        if template is None:
            return None

        # pandas 2 casts a missing county_number to the text "nan". pandas 3
        # leaves it blank. Workbook state and co-op rows have no county.
        if not _text_or_blank(template.get("county_number")):
            template["county_number"] = pd.NA

        # Override suppression if we have actual readiness data
        # Check if total_percent_ready or component readiness values exist
        has_readiness_data = (
            pd.notna(row.get("total_percent_ready")) or
            pd.notna(row.get("total_ready_count")) or
            (pd.notna(row.get("ready_with_interventions_count")) and
             pd.notna(row.get("ready_count")) and
             pd.notna(row.get("ready_with_enrichments_count")))
        )

        if has_readiness_data and template['suppressed'] == 'Y':
            template['suppressed'] = 'N'

        return template


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Entry point for pipeline."""
    etl = KindergartenReadinessETL("kindergarten_readiness")
    etl.process(raw_dir, proc_dir, cfg)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    root = Path(__file__).parent.parent
    conf = Config(derive={})
    transform(root / "data" / "raw", root / "data" / "processed", conf.dict())
