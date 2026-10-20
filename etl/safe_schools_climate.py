"""
Safe Schools Climate ETL Module

Handles Kentucky safe schools climate and safety data:
- Climate and safety index scores for KYRC23 through KYRC26.
  ``KYRC23_ACCT_Index_Scores.csv`` is the 2022-23 climate and safety
  index, the same series as KYRC24-26. It is loaded.
- Precautionary measures (2020-2026)
- Accountability profiles. 2022 stores ``QUALITY OF SCHOOL CLIMATE AND
  SAFETY INDICATOR RATE`` as ``climate_safety_indicator_rate_{level}``.
  2023 stores ``QUALITY OF SCHOOL CLIMATE AND SAFETY COMBINED INDICATOR
  RATE`` (status plus change), not STATUS, as
  ``climate_safety_combined_indicator_rate_{level}``. Those two names
  are not the climate index and they are not one series. Each published
  number is stored once.

``include_kyrc26`` does not apply. ``get_files_to_process`` names the files.

Index files publish elementary, middle, and high on the same school code.
The level is part of the metric name (``climate_index_score_elementary`` and
the middle and high siblings, and the same three for safety) so those rows
do not collapse. A row with no level is not stored under those names. There
is no unsuffixed ``climate_index_score`` or ``safety_index_score``.

A ``*`` is value NA with ``suppressed`` Y. A blank cell is not stored. A
published 0 is kept. The compliance rate is the existing percentage of Yes
answers among the eight policy questions. KYRC26 dropped classroom doors,
so 2026 is 7 of 7 rather than 8 of 8. Outside doors are stored on their
own and are not part of that rate. Co-op ``GREC`` (2025) and ``GLEC`` (2026)
are stored as published. ``GRREC`` is a different co-op.

Survey item files are not loaded. Scott decided on 2026-10-20 to remove
``climate_index_score_calculated`` and ``safety_index_score_calculated``.
The average ignored school level when it dropped duplicate keys and kept
the elementary file, so district and multi-level school values were
elementary-only. The published per-level indexes replace them. 2021-22 is
not in this series.
"""
from pathlib import Path
import gc
import pandas as pd
from pandas.api.types import union_categoricals
import re
from pydantic import BaseModel
from typing import Dict, Any, Optional, Union, List
import logging

import sys
from pathlib import Path

# Add etl directory to path for imports
etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from base_etl import BaseETL, Config
from constants import KPI_COLUMNS
from demographic_mapper import standardize_demographics

logger = logging.getLogger(__name__)

# Sentinel for a starred cell. ``convert_to_kpi_format`` stores it as NA / Y.
STAR_TOKEN = "*"

OUTSIDE_DOORS_METRIC = "outside_doors_locked_during_school_hours"

# The compliance rate still uses these eight questions. Outside doors are a
# separate published question. Classroom doors stay on this list; KYRC26 does
# not publish that column, so that year the rate uses the questions that remain.
POLICY_COLUMNS = [
    "visitors_sign_in",
    "classroom_doors_lock",
    "classroom_phones",
    "annual_climate_survey",
    "student_survey_data",
    "resource_officer",
    "mental_health_referrals",
    "discipline_code_distributed",
]

INDEX_MEASURE_COLUMNS = (
    "climate_index",
    "safety_index",
    "climate_safety_indicator_rate",
    "climate_safety_combined_indicator_rate",
)

LEVEL_SUFFIXES = {
    "elementary school": "elementary",
    "middle school": "middle",
    "high school": "high",
    "elementary": "elementary",
    "middle": "middle",
    "high": "high",
    "es": "elementary",
    "ms": "middle",
    "hs": "high",
}

# Temporary index files use 3-digit school codes and would write 2025 or 2026
# again beside the permanent KYRC file. KYRC23 precautionary republishes the
# answered rows of precautionary_measures_2023.csv. Those names are not loaded.
# KYRC23_ACCT_Index_Scores.csv is the 2023 climate and safety index and is loaded.
REJECTED_CLIMATE_FILENAMES = frozenset({
    "Quality_of_School_Climate_and_Safety_Survey_Index_Scores_2025.CSV",
    "Quality_of_School_Climate_and_Safety_Survey_Index_Scores_2026.CSV",
    "KYRC23_SAFE_Precautionary_Measures.csv",
})


def _is_string_like(series: pd.Series) -> bool:
    """True for object text and for pandas StringDtype.

    ``read_csv(..., dtype=str)`` on pandas 3 returns StringDtype, not object.
    pandas 2.3.3 still returns object. pandas is unpinned.
    """
    return series.dtype == object or bool(pd.api.types.is_string_dtype(series))


def _is_blank(value: Any) -> bool:
    """True for a missing cell. Does not evaluate ``bool(pd.NA)``."""
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    try:
        missing = pd.isna(value)
    except (TypeError, ValueError):
        return False
    if isinstance(missing, bool):
        return missing
    return False


def _cell_text(value: Any) -> str:
    if _is_blank(value):
        return ""
    return str(value).strip()


def _is_star_cell(value: Any) -> bool:
    return _cell_text(value) == "*"


def level_suffix(level: Any) -> str:
    """Map a published level onto the metric-name suffix."""
    text = _cell_text(level).lower()
    if text == "":
        return ""
    mapped = LEVEL_SUFFIXES.get(text)
    if mapped:
        return mapped
    return re.sub(r"[^a-z0-9]+", "_", text).strip("_")


def index_metric_name(base: str, level: Any) -> Optional[str]:
    """``climate_index_score_elementary`` when KDE published a level.

    A blank level returns None. The unsuffixed names ``climate_index_score``
    and ``safety_index_score`` are not emitted.
    """
    suffix = level_suffix(level)
    if not suffix:
        return None
    return f"{base}_{suffix}"


def _star_flag_name(column: str) -> str:
    return f"{column}__star"


def flag_starred_measures(df: pd.DataFrame) -> pd.DataFrame:
    """Record a ``*`` before missing-value cleanup can drop it.

    The check is dtype-agnostic. On pandas 2, ``standardize_missing_values``
    turns ``*`` into NA when the column is object. On pandas 3 the same
    ``dtype=str`` column is StringDtype and that replacement does not run.
    """
    columns = [
        col for col in df.columns
        if col in INDEX_MEASURE_COLUMNS or col in POLICY_COLUMNS or col == "outside_doors_locked"
    ]
    for col in columns:
        if not _is_string_like(df[col]):
            continue
        text = df[col].astype("string").str.strip()
        starred = text.eq("*").fillna(False)
        df[_star_flag_name(col)] = starred.map({True: "Y", False: "N"})
    return df


def _row_measure_is_star(row: pd.Series, column: str) -> bool:
    if _cell_text(row.get(_star_flag_name(column), "N")).upper() == "Y":
        return True
    return _is_star_cell(row.get(column, pd.NA))


def _parse_published_number(value: Any) -> float:
    """Parse a published index. A comma is removed for StringDtype and object."""
    if isinstance(value, str):
        value = value.replace(",", "").strip()
    return float(value)


def _yes_no_flag(value: Any) -> Optional[float]:
    """1 for a published Yes and 0 for a published No."""
    text = _cell_text(value).upper()
    if text == "YES":
        return 1.0
    if text == "NO":
        return 0.0
    return None


def _cast_string_columns_to_category(df: pd.DataFrame) -> pd.DataFrame:
    """Store repeated labels once per file before the frames are concatenated.

    An empty id column is inferred as float. Casting every column except
    ``value`` makes the category codes line up across files.
    """
    for col in df.columns:
        if col == "value":
            continue
        df[col] = df[col].astype("string").astype("category")
    return df


def _align_categories(frames: List[pd.DataFrame]) -> None:
    """Give each file the same category codes so concat does not expand them."""
    if len(frames) < 2:
        return
    columns = [
        col for col in frames[0].columns
        if isinstance(frames[0][col].dtype, pd.CategoricalDtype)
    ]
    for col in columns:
        unified = union_categoricals(
            [frame[col] for frame in frames],
            sort_categories=False,
        )
        categories = unified.categories
        codes = unified.codes
        start = 0
        for frame in frames:
            count = len(frame)
            frame[col] = pd.Categorical.from_codes(
                codes[start:start + count],
                categories=categories,
            )
            start += count
        del unified


def _assert_unique_keys(combined_df: pd.DataFrame) -> None:
    """Raise when year, school, student group, and metric repeat."""
    if combined_df.empty:
        return
    key = ["year", "school_id", "student_group", "metric"]
    duplicate_rows = combined_df.duplicated(key, keep=False)
    if not bool(duplicate_rows.any()):
        return
    sample = combined_df.loc[duplicate_rows, key].head(5)
    raise ValueError(
        f"safe_schools_climate has {int(duplicate_rows.sum())} duplicate-key rows. "
        f"Sample: {sample.to_dict('records')}"
    )


def calculate_policy_compliance_rate(row: pd.Series, policy_columns: List[str]) -> float:
    """Percentage of published Yes answers.

    A blank is not an answer. A ``*`` is not an answer and is not counted as
    No. Outside doors are not in ``policy_columns``.
    """
    yes_count = 0
    total_count = 0

    for col in policy_columns:
        if col not in row.index:
            continue
        if _row_measure_is_star(row, col) or _is_blank(row.get(col, pd.NA)):
            continue
        total_count += 1
        if _cell_text(row.get(col)).upper() == "YES":
            yes_count += 1

    if total_count == 0:
        return pd.NA

    return round((yes_count / total_count) * 100, 1)


def clean_index_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate climate/safety index scores."""
    index_columns = ['climate_index', 'safety_index']
    
    for col in index_columns:
        if col in df.columns:
            # Convert to numeric
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Validate scores are between 0 and 100
            invalid_mask = (df[col] < 0) | (df[col] > 100)
            if invalid_mask.any():
                logger.warning(f"Found {invalid_mask.sum()} invalid scores in {col}")
                df.loc[invalid_mask, col] = pd.NA
    
    return df


class SafeSchoolsClimateETL(BaseETL):
    """ETL module for processing safe schools climate and safety data."""

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            # NOTE: Map to 'district_name' to match base_etl.py's create_kpi_template()
            # which uses row.get('district_name', 'Unknown District')
            'District Name': 'district_name',
            'DISTRICT NAME': 'district_name',
            'School Name': 'school_name',
            'SCHOOL NAME': 'school_name',
            'County Number': 'county_number',
            'COUNTY NUMBER': 'county_number',
            'County Name': 'county_name',
            'COUNTY NAME': 'county_name',
            'District Number': 'district_number',
            'DISTRICT NUMBER': 'district_number',
            'School Number': 'school_number',
            'SCHOOL NUMBER': 'school_number',
            'School Code': 'school_code',
            'SCHOOL CODE': 'school_code',
            'State School Id': 'state_school_id',
            'STATE SCHOOL ID': 'state_school_id',
            'NCES ID': 'nces_id',
            'CO-OP': 'co_op',
            'CO-OP Code': 'co_op_code',
            'CO-OP CODE': 'co_op_code',
            'School Type': 'school_type',
            'SCHOOL TYPE': 'school_type',
            'Level': 'level',
            'LEVEL': 'level',
            'Demographic': 'demographic',
            'DEMOGRAPHIC': 'demographic',
            'School Year': 'school_year',
            'SCHOOL YEAR': 'school_year',
            # 2025 files use quoted column names
            '"School Year"': 'school_year',
            '"School Code"': 'school_code',
            '"District Name"': 'district_name',
            '"School Name"': 'school_name',
            '"Level"': 'level',
            '"Demographic"': 'demographic',
            '"Climate Index"': 'climate_index',
            '"Safety Index"': 'safety_index',
            '"Suppressed"': 'suppressed',
            # Precautionary measures columns.
            # KYRC24 asks questions. KYRC25 and KYRC26 state them. The shortened
            # historical headers stay mapped. Outside doors are stored alone.
            'Are visitors to the building required to sign-in?': 'visitors_sign_in',
            'Visitors required to sign-in': 'visitors_sign_in',
            'Visitors are required to sign-in to the building': 'visitors_sign_in',
            'Visitors are required to sign in to the buildings.': 'visitors_sign_in',
            'Do all classroom doors lock from the inside?': 'classroom_doors_lock',
            'All classroom doors lock from the inside.': 'classroom_doors_lock',
            'All classroom doors lock from the inside': 'classroom_doors_lock',
            'Do all classrooms have access to a telephone accessing outside lines?': 'classroom_phones',
            'All classrooms have access to telephone': 'classroom_phones',
            'All classrooms have access to a telephone accessing outside lines': 'classroom_phones',
            'All classrooms have access to a telephone that can reach outside lines.': 'classroom_phones',
            'Does your school administer a school climate survey annually?': 'annual_climate_survey',
            'School climate survey administered annually.': 'annual_climate_survey',
            'The school administers a school climate survey annually': 'annual_climate_survey',
            'Schools administer a school climate survey annually.': 'annual_climate_survey',
            'Does your school collect and use student survey data?': 'student_survey_data',
            'Student survey data collected and used': 'student_survey_data',
            'The school collects and uses student survey data': 'student_survey_data',
            'Schools collect and use student survey data.': 'student_survey_data',
            'Does your school have a full-time School Resource Officer?': 'resource_officer',
            'Full-time resource officer': 'resource_officer',
            'The school has a full-time School Resource Officer': 'resource_officer',
            'Schools have a full-time school resource officer.': 'resource_officer',
            'Does your school have a process in place to provide mental health referrals for students?': 'mental_health_referrals',
            'Mental health referral process in place': 'mental_health_referrals',
            'The school has a process in place to provide mental health referrals for students': 'mental_health_referrals',
            'Schools have a process in place to provide mental health referrals for students.': 'mental_health_referrals',
            'Is the district discipline code distributed to parents?': 'discipline_code_distributed',
            'District discipline code distributed to parents': 'discipline_code_distributed',
            'The district discipline code is distributed to parents': 'discipline_code_distributed',
            'The district discipline code is distributed to all parents.': 'discipline_code_distributed',
            'All outside doors are locked during school hours': 'outside_doors_locked',
            'All outside doors are locked during school hours.': 'outside_doors_locked',
            'Outside doors locked during school hours': 'outside_doors_locked',
            # Index score columns (multiple variations)
            'CLIMATE INDEX': 'climate_index',
            'SAFETY INDEX': 'safety_index',
            'Climate Index': 'climate_index',
            'Safety Index': 'safety_index',
            # Accountability profile columns.
            # 2022 publishes INDICATOR RATE. 2023 publishes COMBINED INDICATOR
            # RATE (status plus change), not STATUS. The headers stay on
            # different columns so the two years are not one series. State
            # 2023 combined vs status is elementary 77.0 vs 76.8, high 62.0
            # vs 61.7, and middle 65.3 vs 65.6. STATUS is not stored.
            'QUALITY OF SCHOOL CLIMATE AND SAFETY STATUS': 'climate_safety_status',
            'QUALITY OF SCHOOL CLIMATE AND SAFETY STATUS RATING': 'climate_safety_rating',
            'QUALITY OF SCHOOL CLIMATE AND SAFETY COMBINED INDICATOR RATE': 'climate_safety_combined_indicator_rate',
            'QUALITY OF SCHOOL CLIMATE AND SAFETY INDICATOR RATE': 'climate_safety_indicator_rate',
            'QUALITY OF SCHOOL CLIMATE AND SAFETY INDICATOR RATING': 'climate_safety_rating',
        }
    
    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get default metrics for suppressed records based on available data."""
        metrics = {}
        level = row.get("level", pd.NA)

        # The level suffix matches extract_metrics so a suppressed row does
        # not land on a different metric than the published number.
        if any(col in row.index for col in ("climate_index", "safety_index")):
            climate_name = index_metric_name("climate_index_score", level)
            safety_name = index_metric_name("safety_index_score", level)
            if climate_name and safety_name:
                metrics[climate_name] = pd.NA
                metrics[safety_name] = pd.NA
        for column, base_name in (
            ("climate_safety_indicator_rate", "climate_safety_indicator_rate"),
            ("climate_safety_combined_indicator_rate", "climate_safety_combined_indicator_rate"),
        ):
            if column in row.index:
                name = index_metric_name(base_name, level)
                if name:
                    metrics[name] = pd.NA

        if any(col in row.index for col in POLICY_COLUMNS):
            metrics["safety_policy_compliance_rate"] = pd.NA
        if "outside_doors_locked" in row.index:
            metrics[OUTSIDE_DOORS_METRIC] = pd.NA

        return metrics

    def _store_index_measure(
        self,
        metrics: Dict[str, Any],
        row: pd.Series,
        column: str,
        base_name: str,
        row_suppressed: bool,
    ) -> None:
        """Store one published index cell, or a star, and skip a blank."""
        if column not in row.index:
            return
        name = index_metric_name(base_name, row.get("level", pd.NA))
        if not name:
            return
        starred = _row_measure_is_star(row, column)
        blank = _is_blank(row.get(column, pd.NA))
        if starred or (row_suppressed and blank):
            metrics[name] = STAR_TOKEN
            return
        if blank:
            return
        metrics[name] = row.get(column)

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        """Extract metrics based on the data type."""
        metrics: Dict[str, Any] = {}
        row_suppressed = _cell_text(row.get("suppressed", "N")).upper() in {"Y", "YES"}

        # Direct index scores. Level is part of the metric name.
        if "climate_index" in row.index or "safety_index" in row.index:
            self._store_index_measure(metrics, row, "climate_index", "climate_index_score", row_suppressed)
            self._store_index_measure(metrics, row, "safety_index", "safety_index_score", row_suppressed)
            if row_suppressed:
                # An all-star or blank-and-suppressed row stores both indexes.
                for column, base_name in (
                    ("climate_index", "climate_index_score"),
                    ("safety_index", "safety_index_score"),
                ):
                    if column in row.index:
                        name = index_metric_name(base_name, row.get("level", pd.NA))
                        if name and name not in metrics:
                            metrics[name] = STAR_TOKEN

        # A survey question row is not a published index and is not stored.

        # 2022 stores the indicator rate. 2023 stores the combined indicator
        # rate (status plus change), not STATUS. Each published number is
        # stored once, under its own level-suffixed name. Neither name is
        # the climate or safety index, and the two years do not share a name.
        for column, base_name in (
            ("climate_safety_indicator_rate", "climate_safety_indicator_rate"),
            ("climate_safety_combined_indicator_rate", "climate_safety_combined_indicator_rate"),
        ):
            if column in row.index:
                self._store_index_measure(metrics, row, column, base_name, row_suppressed)

        answered = [
            col for col in POLICY_COLUMNS
            if col in row.index and not _is_blank(row.get(col, pd.NA)) and not _row_measure_is_star(row, col)
        ]
        if answered:
            compliance_rate = calculate_policy_compliance_rate(row, POLICY_COLUMNS)
            if not _is_blank(compliance_rate):
                metrics["safety_policy_compliance_rate"] = compliance_rate
        elif row_suppressed and any(col in row.index for col in POLICY_COLUMNS):
            metrics["safety_policy_compliance_rate"] = STAR_TOKEN

        if "outside_doors_locked" in row.index:
            if _row_measure_is_star(row, "outside_doors_locked"):
                metrics[OUTSIDE_DOORS_METRIC] = STAR_TOKEN
            else:
                flag = _yes_no_flag(row.get("outside_doors_locked"))
                if flag is not None:
                    metrics[OUTSIDE_DOORS_METRIC] = flag

        return metrics

    def convert_to_kpi_format(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """Convert rows to KPI records, with a star stored per metric.

        A row-level Suppressed Y stores every emitted metric as NA / Y.
        A single starred cell on an otherwise published row is NA / Y, and
        the published number beside it is kept.
        """
        if df is None or df.empty:
            return pd.DataFrame()

        kpi_rows = []
        for _, row in df.iterrows():
            if self.should_skip_row(row):
                continue
            kpi_template = self.create_kpi_template(row, source_file)
            if kpi_template is None:
                continue
            metrics = self.extract_metrics(row)
            if not metrics and kpi_template["suppressed"] == "Y":
                metrics = self.get_suppressed_metric_defaults(row)
            for metric_name, value in metrics.items():
                kpi_record = kpi_template.copy()
                cell_star = _is_star_cell(value)
                if kpi_template["suppressed"] == "Y" or cell_star:
                    kpi_record["value"] = pd.NA
                    kpi_record["suppressed"] = "Y"
                    kpi_record["metric"] = metric_name
                    kpi_rows.append(kpi_record)
                    continue
                if _is_blank(value):
                    continue
                try:
                    kpi_record["value"] = _parse_published_number(value)
                except (ValueError, TypeError):
                    continue
                kpi_record["metric"] = metric_name
                kpi_rows.append(kpi_record)

        if not kpi_rows:
            logger.warning("No valid KPI rows created")
            return pd.DataFrame()

        kpi_df = pd.DataFrame(kpi_rows)
        available_columns = [col for col in KPI_COLUMNS if col in kpi_df.columns]
        return kpi_df[available_columns]
    
    
    def process_file(self, file_path: Path) -> pd.DataFrame:
        """Process one index, precautionary, or accountability file."""
        logger.info(f"Processing file: {file_path.name}")

        if file_path.stat().st_size == 0:
            logger.warning(f"Empty file (0 bytes): {file_path.name}")
            return pd.DataFrame()

        try:
            df = pd.read_csv(file_path, encoding='utf-8-sig', low_memory=False, dtype=str)
        except pd.errors.EmptyDataError:
            logger.warning(f"No data found in file: {file_path.name}")
            return pd.DataFrame()
        
        # Skip if empty DataFrame
        if df.empty:
            logger.warning(f"Empty DataFrame: {file_path.name}")
            return pd.DataFrame()
        
        # Apply column mappings
        df = self.normalize_column_names(df)

        # Standardize school names (empty/null → "---District Total---", "All Schools" → "---District Total---")
        if 'school_name' in df.columns:
            df['school_name'] = df['school_name'].apply(lambda x: self.standardize_school_name(x))

        # Extract year based on file type
        if 'school_year' in df.columns:
            df['year'] = df['school_year'].astype(str).str[-4:]
        else:
            # Try to extract year from filename
            year_match = re.search(r'(20\d{2})', file_path.name)
            if year_match:
                df['year'] = year_match.group(1)
            else:
                df['year'] = '2024'
        
        # Set demographic if not present
        if 'demographic' not in df.columns:
            df['demographic'] = 'All Students'
        
        # Apply demographic mapping
        if 'demographic' in df.columns and 'year' in df.columns:
            df['student_group'] = standardize_demographics(
                df['demographic'],
                df['year'].iloc[0] if len(df) > 0 else '2024',
                self.source_name
            )
        else:
            df['student_group'] = 'All Students'

        # Yes/No suppression labels are Y/N before the row template is built.
        # Index files already publish Y/N. This does not change those cells.
        if "suppressed" in df.columns and _is_string_like(df["suppressed"]):
            df["suppressed"] = (
                df["suppressed"].astype("string").str.strip().replace(
                    {"Yes": "Y", "No": "N", "yes": "Y", "no": "N", "YES": "Y", "NO": "N"}
                )
            )

        # Record stars before BaseETL turns an object-dtype '*' into NA.
        df = flag_starred_measures(df)

        # Process missing values
        df = self.standardize_missing_values(df)

        # Add source file info
        df['source_file'] = file_path.name

        return df
    
    def process_files(self, files: List[Path]) -> pd.DataFrame:
        """Override to handle different file types appropriately."""
        all_data = []

        for file in files:
            if file.name in REJECTED_CLIMATE_FILENAMES:
                logger.info(f"Skipping rejected climate file: {file.name}")
                continue
            logger.info(f"Processing file: {file}")

            try:
                df = self.process_file(file)
                kpi_df = self.convert_to_kpi_format(df, file.name)
                del df
                if not kpi_df.empty:
                    _cast_string_columns_to_category(kpi_df)
                    all_data.append(kpi_df)

            except Exception as e:
                logger.error(f"Error processing {file}: {e}")
                continue
        
        if not all_data:
            logger.warning("No data processed from any files")
            return pd.DataFrame()

        # One reindex: align category codes, then drop the per-file frames.
        _align_categories(all_data)
        combined_df = pd.concat(all_data, ignore_index=True)
        del all_data
        gc.collect()

        # DEBUG: Check combined_df before deduplication
        logger.info(f"Combined dataframe before deduplication: {len(combined_df)} records")
        if 'year' in combined_df.columns:
            year_2025_before = combined_df[combined_df['year'] == '2025']
            logger.info(f"Year 2025 records before deduplication: {len(year_2025_before)}")
            if 'district' in combined_df.columns:
                logger.info(f"Year 2025 Fayette County records before dedup: {len(year_2025_before[year_2025_before['district'] == 'Fayette County'])}")
                logger.info(f"Year 2025 'Unknown District' records before dedup: {len(year_2025_before[year_2025_before['district'] == 'Unknown District'])}")
                # Show source file breakdown for 2025
                if 'source_file' in combined_df.columns:
                    logger.info(f"Year 2025 records by source file before dedup:\n{year_2025_before.groupby('source_file').size().to_dict()}")

        # Remove duplicates keeping the most recent
        combined_df = combined_df.sort_values(['year', 'source_file'], ascending=[False, True])
        combined_df = combined_df.drop_duplicates(
            subset=['district', 'school_id', 'school_name', 'year', 'student_group', 'metric'],
            keep='first'
        )

        # DEBUG: Check combined_df after deduplication
        logger.info(f"Combined dataframe after deduplication: {len(combined_df)} records")
        if 'year' in combined_df.columns:
            year_2025_after = combined_df[combined_df['year'] == '2025']
            logger.info(f"Year 2025 records after deduplication: {len(year_2025_after)}")
            if 'district' in combined_df.columns:
                logger.info(f"Year 2025 Fayette County records after dedup: {len(year_2025_after[year_2025_after['district'] == 'Fayette County'])}")
                logger.info(f"Year 2025 'Unknown District' records after dedup: {len(year_2025_after[year_2025_after['district'] == 'Unknown District'])}")
                # Show source file breakdown for 2025
                if 'source_file' in combined_df.columns:
                    logger.info(f"Year 2025 records by source file after dedup:\n{year_2025_after.groupby('source_file').size().to_dict()}")

        combined_df["value"] = pd.to_numeric(combined_df["value"], errors="coerce")
        _assert_unique_keys(combined_df)
        return combined_df
    
    def get_files_to_process(self, raw_dir: Path) -> List[Path]:
        """Get all files to process based on file patterns."""
        module_dir = raw_dir / self.source_name
        files_to_process = []

        # Define file patterns to process
        patterns = [
            'KYRC23_ACCT_Index_Scores.csv',
            'KYRC24_ACCT_Index_Scores.csv',
            'KYRC25_ACCT_Index_Scores.csv',
            'KYRC26_ACCT_Index_Scores.csv',
            'KYRC24_SAFE_Precautionary_Measures.csv',
            'KYRC25_SAFE_Precautionary_Measures.csv',
            'KYRC26_SAFE_Precautionary_Measures.csv',
            'accountability_profile_2022.csv',  # Climate indicator starts in 2022
            'accountability_profile_2023.csv',
            'precautionary_measures_*.csv',
            # Survey item files are not selected. They only fed the removed
            # calculated indexes. The temporary index-score names stay rejected.
        ]

        # Find all matching files
        for pattern in patterns:
            matched_files = list(module_dir.glob(pattern))
            if matched_files:
                logger.info(f"Pattern '{pattern}' matched {len(matched_files)} files: {[f.name for f in matched_files]}")
                for file_path in matched_files:
                    if file_path.name in REJECTED_CLIMATE_FILENAMES:
                        logger.info(f"Skipping rejected climate file: {file_path.name}")
                        continue
                    if file_path.is_file():
                        files_to_process.append(file_path)
            else:
                logger.debug(f"Pattern '{pattern}' matched no files")

        logger.info(f"Found {len(files_to_process)} files to process for safe schools climate")
        logger.info(f"Files to process: {[f.name for f in sorted(files_to_process)]}")
        return sorted(files_to_process)


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Process safe schools climate data using BaseETL."""
    etl = SafeSchoolsClimateETL('safe_schools_climate')
    
    # Use custom file selection instead of BaseETL's automatic CSV discovery
    source_dir = raw_dir / etl.source_name
    if not source_dir.exists():
        logger.info(f"No raw data directory for {etl.source_name}; skipping.")
        return
    
    # Get files using our custom method
    csv_files = etl.get_files_to_process(raw_dir)
    if not csv_files:
        logger.info(f"No relevant files found for {etl.source_name}; skipping.")
        return
    
    # Process files using the SafeSchoolsClimateETL logic
    result_df = etl.process_files(csv_files)
    
    if not result_df.empty:
        # Save results
        output_file = proc_dir / f"{etl.source_name}.csv"
        result_df.to_csv(output_file, index=False)
        logger.info(f"KPI data written to {output_file}")
        
        # Generate demographic report
        report_file = proc_dir / f"{etl.source_name}_demographic_report.md"
        etl.demographic_mapper.save_audit_report(report_file)
        logger.info(f"Demographic report written to {report_file}")
        
        # Log summary statistics
        logger.info(f"Total KPI rows: {len(result_df)}, Total columns: {len(result_df.columns)}")
        
        if 'value' in result_df.columns:
            numeric_values = pd.to_numeric(result_df['value'], errors='coerce')
            valid_values = numeric_values.dropna()
            if len(valid_values) > 0:
                logger.info(f"KPI value range: {valid_values.min()} - {valid_values.max()}")
        
        if 'metric' in result_df.columns:
            metric_counts = result_df['metric'].value_counts()
            logger.info(f"Metrics created: {metric_counts.to_dict()}")
        
        if 'student_group' in result_df.columns:
            demo_counts = result_df['student_group'].value_counts().head(10)
            logger.info(f"Top 10 demographics: {demo_counts.to_dict()}")
        
        logger.info(f"Completed processing for {etl.source_name}")
        
        print(f"Wrote {output_file}")
        print(f"Demographic report: {report_file}")
    else:
        logger.warning(f"No KPI data generated for {etl.source_name}")


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    
    from pathlib import Path
    raw_dir = Path(__file__).parent.parent / "data" / "raw"
    proc_dir = Path(__file__).parent.parent / "data" / "processed"
    proc_dir.mkdir(exist_ok=True)
    
    test_config = Config(
        derive={"processing_date": "2025-07-21", "data_quality_flag": "reviewed"}
    ).dict()

    transform(raw_dir, proc_dir, test_config)