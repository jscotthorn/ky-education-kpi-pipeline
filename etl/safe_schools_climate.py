"""
Safe Schools Climate ETL Module

Handles Kentucky safe schools climate and safety data:
- Climate and safety index scores for KYRC24 through KYRC26.
  ``KYRC23_ACCT_Index_Scores.csv`` is the separate 2022-23 climate and
  safety pair behind accountability. ``(climate + safety) / 2`` matches
  the 2023 status rate within 0.05. That file is not loaded.
- Precautionary measures (2020-2026)
- Accountability profiles. 2022 stores ``QUALITY OF SCHOOL CLIMATE AND
  SAFETY INDICATOR RATE``. 2023 stores ``QUALITY OF SCHOOL CLIMATE AND
  SAFETY COMBINED INDICATOR RATE`` (status plus change), not STATUS.

``include_kyrc26`` does not apply. ``get_files_to_process`` names the files.

Index files publish elementary, middle, and high on the same school code.
The level is part of the metric name (``climate_index_score_elementary`` and
the middle and high siblings, and the same three for safety) so those rows
do not collapse. A row with no level keeps the unsuffixed name.

A ``*`` is value NA with ``suppressed`` Y. A blank cell is not stored. A
published 0 is kept. The compliance rate is the existing percentage of Yes
answers among the eight policy questions. KYRC26 dropped classroom doors,
so 2026 is 7 of 7 rather than 8 of 8. Outside doors are stored on their
own and are not part of that rate. Co-op ``GREC`` (2025) and ``GLEC`` (2026)
are stored as published. ``GRREC`` is a different co-op.
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
    "climate_safety_combined_rate",
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
# again beside the permanent KYRC file. KYRC23 index is the separate 2022-23
# climate and safety pair behind accountability. (climate + safety) / 2
# matches the 2023 status rate within 0.05. This module stores the 2023
# combined indicator rate instead, and does not load the KYRC23 file.
# KYRC23 precautionary republishes the answered rows of
# precautionary_measures_2023.csv. None of these names are loaded.
REJECTED_CLIMATE_FILENAMES = frozenset({
    "Quality_of_School_Climate_and_Safety_Survey_Index_Scores_2025.CSV",
    "Quality_of_School_Climate_and_Safety_Survey_Index_Scores_2026.CSV",
    "KYRC23_ACCT_Index_Scores.csv",
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


def index_metric_name(base: str, level: Any) -> str:
    """``climate_index_score_elementary`` when KDE published a level."""
    suffix = level_suffix(level)
    if suffix:
        return f"{base}_{suffix}"
    return base


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


def calculate_aggregate_index_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate aggregate climate and safety index scores from survey question data.
    
    Returns DataFrame with calculated scores for each district/school/demographic combination.
    """
    # Filter to survey question data with question_index scores
    survey_data = df[
        (df['question_type'].notna()) & 
        (df['question_index'].notna()) & 
        (df['question_index'] != '') &
        (df['question_index'] != '*')  # Remove suppressed values
    ].copy()
    
    if survey_data.empty:
        logger.info("No survey data found for aggregation")
        return pd.DataFrame()
    
    
    logger.info(f"Processing {len(survey_data)} survey question records for aggregation")
    
    
    # Convert question_index to numeric
    survey_data['question_index'] = pd.to_numeric(survey_data['question_index'], errors='coerce')
    
    # Remove rows with invalid scores
    survey_data = survey_data[survey_data['question_index'].notna()]
    
    if survey_data.empty:
        logger.info("No valid numeric question index scores found")
        return pd.DataFrame()
    
    # Normalize question types
    survey_data['question_type_clean'] = survey_data['question_type'].str.upper().str.strip()
    
    # Group by key fields and question type, calculate mean scores
    # NOTE: Use 'district_name' to match base_etl.py column naming convention
    groupby_cols = [
        'district_name', 'school_name', 'year', 'student_group',
        'county_number', 'county_name', 'district_number',
        'school_number', 'school_code', 'state_school_id', 'nces_id',
        'co_op', 'co_op_code', 'school_type', 'level', 'source_file'
    ]
    
    # Filter columns that actually exist
    existing_cols = [col for col in groupby_cols if col in survey_data.columns]
    logger.info(f"Grouping by columns: {existing_cols}")
    
    # Handle NaN values in groupby columns for state-level records
    # Fill NaN values with placeholder strings to ensure state/district aggregations are included
    for col in existing_cols:
        if col in ['county_number', 'county_name', 'state_school_id', 'nces_id', 'co_op', 'co_op_code']:
            survey_data[col] = survey_data[col].fillna('999')
        elif col in ['school_type']:
            survey_data[col] = survey_data[col].fillna('State')
    
    aggregated_scores = []
    
    # Calculate climate scores
    climate_data = survey_data[survey_data['question_type_clean'].isin(['C', 'CLIMATE'])]
    if not climate_data.empty:
        logger.info(f"Calculating climate scores for {len(climate_data)} records")
        climate_agg = climate_data.groupby(existing_cols, dropna=False)['question_index'].agg([
            ('mean_score', 'mean'),
            ('question_count', 'count')
        ]).reset_index()
        climate_agg['metric'] = 'climate_index_score_calculated'
        climate_agg['value'] = climate_agg['mean_score'].round(1)
        aggregated_scores.append(climate_agg)
        logger.info(f"Generated {len(climate_agg)} climate score records")
    
    # Calculate safety scores  
    safety_data = survey_data[survey_data['question_type_clean'].isin(['S', 'SAFETY'])]
    if not safety_data.empty:
        logger.info(f"Calculating safety scores for {len(safety_data)} records")
        safety_agg = safety_data.groupby(existing_cols, dropna=False)['question_index'].agg([
            ('mean_score', 'mean'),
            ('question_count', 'count')
        ]).reset_index()
        safety_agg['metric'] = 'safety_index_score_calculated'
        safety_agg['value'] = safety_agg['mean_score'].round(1)
        aggregated_scores.append(safety_agg)
        logger.info(f"Generated {len(safety_agg)} safety score records")
    
    if not aggregated_scores:
        logger.info("No aggregated scores generated")
        return pd.DataFrame()
    
    # Combine climate and safety scores
    result_df = pd.concat(aggregated_scores, ignore_index=True)
    
    # Add required columns for KPI format
    result_df['suppressed'] = 'N'  # Calculated scores are not suppressed
    
    # Select and rename columns to match KPI format
    final_cols = existing_cols + ['metric', 'value', 'suppressed']
    result_df = result_df[final_cols]
    
    logger.info(f"Final aggregated result: {len(result_df)} records")
    return result_df


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

    def __init__(self, source_name: str):
        super().__init__(source_name)
        self._question_metadata = None

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            # Standard KPI fields from survey files
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
            '"Question Number"': 'question_number',
            '"Suppressed"': 'suppressed',
            '"Strongly Disagree"': 'strongly_disagree',
            '"Disagree"': 'disagree',
            '"Agree"': 'agree',
            '"Strongly Agree"': 'strongly_agree',
            '"Agree and Strongly Agree"': 'agree_strongly_agree_pct',
            '"Question Index"': 'question_index',
            '"Question Type"': 'question_type',
            '"Question"': 'question_text',
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
            # Survey results columns
            'Question Type': 'question_type',
            'QUESTION TYPE': 'question_type', 
            'Question': 'question_text',
            'QUESTION': 'question_text',
            'Question Index': 'question_index',
            'QUESTION INDEX': 'question_index',
            'Question Number': 'question_number',
            'QUESTION NUMBER': 'question_number',
            'Agree / Strongly Agree': 'agree_strongly_agree_pct',
            'Agree and Strongly Agree': 'agree_strongly_agree_pct',
            # Accountability profile columns.
            # 2022 publishes INDICATOR RATE. 2023 publishes COMBINED INDICATOR
            # RATE (status plus change), not STATUS. Both headers feed the
            # same stored cell. State 2023 combined vs status is elementary
            # 77.0 vs 76.8, high 62.0 vs 61.7, and middle 65.3 vs 65.6.
            'QUALITY OF SCHOOL CLIMATE AND SAFETY STATUS': 'climate_safety_status',
            'QUALITY OF SCHOOL CLIMATE AND SAFETY STATUS RATING': 'climate_safety_rating',
            'QUALITY OF SCHOOL CLIMATE AND SAFETY COMBINED INDICATOR RATE': 'climate_safety_combined_rate',
            'QUALITY OF SCHOOL CLIMATE AND SAFETY INDICATOR RATE': 'climate_safety_combined_rate',
            'QUALITY OF SCHOOL CLIMATE AND SAFETY INDICATOR RATING': 'climate_safety_rating',
        }
    
    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get default metrics for suppressed records based on available data."""
        metrics = {}
        level = row.get("level", pd.NA)

        # Only include index scores if we have index columns.
        # The level suffix matches extract_metrics so a suppressed row does
        # not land on a different metric than the published number.
        if any(col in row.index for col in ("climate_index", "safety_index", "climate_safety_combined_rate")):
            metrics[index_metric_name("climate_index_score", level)] = pd.NA
            metrics[index_metric_name("safety_index_score", level)] = pd.NA

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
                        if name not in metrics:
                            metrics[name] = STAR_TOKEN

        # Survey question rows. These are not the index-score file. The metric
        # name stays unsuffixed, which is what the calculated-score path uses.
        if "question_type" in row.index and "question_index" in row.index:
            question_type = _cell_text(row.get("question_type", "")).upper()
            question_index = row.get("question_index", pd.NA)
            if question_type in ["C", "CLIMATE"] and not _is_blank(question_index) and not _is_star_cell(question_index):
                metrics["climate_index_score"] = question_index
            elif question_type in ["S", "SAFETY"] and not _is_blank(question_index) and not _is_star_cell(question_index):
                metrics["safety_index_score"] = question_index

        # 2022 stores the indicator rate. 2023 stores the combined indicator
        # rate (status plus change), not STATUS. The existing calculation
        # stores that published number as both the climate score and the
        # safety score. It is not a second index KDE calculated.
        if "climate_safety_combined_rate" in row.index:
            self._store_index_measure(
                metrics, row, "climate_safety_combined_rate", "climate_index_score", row_suppressed
            )
            # Copy the same cell onto the safety metric. A blank stays absent.
            climate_name = index_metric_name("climate_index_score", row.get("level", pd.NA))
            safety_name = index_metric_name("safety_index_score", row.get("level", pd.NA))
            if climate_name in metrics:
                metrics[safety_name] = metrics[climate_name]

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
    
    def format_calculated_scores_as_kpi(self, calculated_df: pd.DataFrame) -> pd.DataFrame:
        """Format calculated index scores to match KPI output format."""
        if calculated_df.empty:
            return pd.DataFrame()
        
        # Ensure we have the required columns
        required_cols = ['year', 'metric', 'value', 'suppressed']
        for col in required_cols:
            if col not in calculated_df.columns:
                if col == 'year':
                    calculated_df['year'] = '2024'  # Default for missing year
                elif col == 'suppressed':
                    calculated_df['suppressed'] = 'N'
                else:
                    calculated_df[col] = pd.NA
        
        # Map to standard KPI column names
        kpi_df = calculated_df.copy()

        # Rename district_name -> district to match KPI output format
        # (Internal processing uses district_name to match base_etl.py convention)
        if 'district_name' in kpi_df.columns:
            kpi_df = kpi_df.rename(columns={'district_name': 'district'})

        # Generate school_id from school_code if not present
        if 'school_id' not in kpi_df.columns and 'school_code' in kpi_df.columns:
            kpi_df['school_id'] = kpi_df['school_code']

        # Ensure required KPI columns exist (all 19 standard columns)
        kpi_columns = [
            'year', 'metric', 'district', 'school_name', 'student_group', 'value', 'suppressed',
            'county_number', 'county_name', 'district_number', 'school_id', 'school_code',
            'state_school_id', 'nces_id', 'co_op', 'co_op_code', 'school_type', 'source_file',
            'last_updated'
        ]
        
        for col in kpi_columns:
            if col not in kpi_df.columns:
                if col == 'last_updated':
                    # Add timestamp for last_updated column
                    from datetime import datetime
                    kpi_df[col] = datetime.now().isoformat()
                else:
                    kpi_df[col] = pd.NA
        
        return kpi_df[kpi_columns]
    
    def load_question_metadata(self, raw_dir: Path) -> pd.DataFrame:
        """Load question metadata from the 2025 questions file."""
        if self._question_metadata is not None:
            return self._question_metadata

        module_dir = raw_dir / self.source_name
        questions_file = module_dir / 'Quality_of_School_Climate_and_Safety_Survey_Questions_2025.CSV'

        if not questions_file.exists():
            logger.info("No 2025 questions metadata file found")
            return pd.DataFrame()

        try:
            # Try latin1 encoding first (2025 files have special characters)
            try:
                df = pd.read_csv(questions_file, encoding='latin1', dtype=str)
            except Exception:
                df = pd.read_csv(questions_file, encoding='utf-8-sig', dtype=str)

            df = self.normalize_column_names(df)
            logger.info(f"Successfully loaded questions metadata file with {len(df)} rows")
            logger.info(f"Metadata columns: {list(df.columns)}")

            # Create a mapping from (level, question_number) -> (question_type, question_text)
            if 'level' in df.columns and 'question_number' in df.columns:
                # Normalize level codes: ES, MS, HS
                if 'level' in df.columns:
                    df['level'] = df['level'].str.upper().str.strip()

                self._question_metadata = df[['level', 'question_number', 'question_type', 'question_text']].copy()
                logger.info(f"Loaded {len(self._question_metadata)} question metadata records")
                logger.info(f"Question types found: {self._question_metadata['question_type'].value_counts().to_dict()}")
            else:
                logger.warning(f"Question metadata file missing required columns. Has: {list(df.columns)}")
                self._question_metadata = pd.DataFrame()

        except Exception as e:
            logger.error(f"Error loading question metadata: {e}")
            import traceback
            logger.error(traceback.format_exc())
            self._question_metadata = pd.DataFrame()

        return self._question_metadata

    def identify_file_type(self, file_path: Path) -> str:
        """Identify the type of file based on name and content."""
        filename = file_path.name.lower()

        if 'index_scores' in filename:
            return 'index_scores'
        elif 'survey_results' in filename:
            return 'survey_results'
        elif 'precautionary' in filename:
            return 'precautionary_measures'
        elif 'accountability_profile' in filename:
            return 'accountability_profile'
        elif 'quality_of_school_climate' in filename:
            if 'index' in filename:
                return 'survey_index_scores'
            elif 'questions' in filename:
                return 'survey_questions_metadata'
            elif 'elementary' in filename or 'middle' in filename or 'high' in filename:
                return 'survey_responses'
            else:
                return 'survey_responses'
        else:
            return 'unknown'
    
    def process_file(self, file_path: Path, raw_dir: Path = None) -> pd.DataFrame:
        """Process a single file based on its type."""
        file_type = self.identify_file_type(file_path)
        logger.info(f"Processing {file_type} file: {file_path.name}")

        # Skip metadata files - they're only used for reference
        if file_type == 'survey_questions_metadata':
            logger.info(f"Skipping metadata file: {file_path.name}")
            return pd.DataFrame()

        # Check for empty file
        if file_path.stat().st_size == 0:
            logger.warning(f"Empty file (0 bytes): {file_path.name}")
            return pd.DataFrame()
        
        # Read the file with proper options to avoid warnings and handle large files
        try:
            # For very large files (>100MB), use chunking
            if file_path.stat().st_size > 100 * 1024 * 1024:  # 100MB
                logger.info(f"Large file detected ({file_path.stat().st_size / (1024*1024):.1f}MB), using chunked reading")
                chunks = []
                chunk_size = 50000  # Process 50k rows at a time
                for chunk in pd.read_csv(file_path, encoding='utf-8-sig', dtype=str, chunksize=chunk_size):
                    # Only keep rows with survey question data to reduce memory
                    if file_type == 'survey_results':
                        # Handle different column name formats
                        question_type_col = 'QUESTION TYPE' if 'QUESTION TYPE' in chunk.columns else 'Question Type'
                        question_index_col = 'QUESTION INDEX' if 'QUESTION INDEX' in chunk.columns else 'Question Index'
                        if question_type_col in chunk.columns and question_index_col in chunk.columns:
                            chunk = chunk[chunk[question_type_col].notna() & chunk[question_index_col].notna()]
                    chunks.append(chunk)
                    if len(chunks) % 10 == 0:  # Log progress every 10 chunks
                        logger.info(f"Processed {len(chunks) * chunk_size} rows...")
                df = pd.concat(chunks, ignore_index=True)
                logger.info(f"Loaded {len(df)} rows from chunked file")
            else:
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

        # For 2025 survey files without question_type, merge from metadata
        if file_type == 'survey_responses' and 'question_type' not in df.columns and raw_dir:
            logger.info(f"Survey file needs question_type - attempting metadata merge for {file_path.name}")
            metadata = self.load_question_metadata(raw_dir)
            logger.info(f"Metadata loaded: {len(metadata)} records, empty={metadata.empty}")

            if not metadata.empty and 'level' in df.columns and 'question_number' in df.columns:
                # Normalize level codes to match metadata (ES, MS, HS)
                df['level'] = df['level'].str.upper().str.strip()
                # Normalize question_number to ensure consistent format
                df['question_number'] = df['question_number'].astype(str).str.zfill(2)
                metadata['question_number'] = metadata['question_number'].astype(str).str.zfill(2)

                logger.info(f"Before merge - Survey rows: {len(df)}, unique levels: {df['level'].unique()}, question_numbers: {df['question_number'].nunique()}")
                logger.info(f"Metadata has levels: {metadata['level'].unique()}, question_numbers: {metadata['question_number'].nunique()}")

                # Merge question_type and question_text from metadata
                df_before = len(df)
                df = df.merge(
                    metadata[['level', 'question_number', 'question_type', 'question_text']],
                    on=['level', 'question_number'],
                    how='left'
                )
                logger.info(f"After merge - Survey rows: {len(df)} (before: {df_before})")
                logger.info(f"question_type column added: {'question_type' in df.columns}")
                if 'question_type' in df.columns:
                    qt_values = df['question_type'].value_counts().to_dict()
                    qt_nulls = df['question_type'].isna().sum()
                    logger.info(f"question_type values: {qt_values}, nulls: {qt_nulls}")
            else:
                logger.warning(f"Cannot merge metadata - metadata empty: {metadata.empty}, has level: {'level' in df.columns}, has question_number: {'question_number' in df.columns}")

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
    
    def process_files(self, files: List[Path], raw_dir: Path = None) -> pd.DataFrame:
        """Override to handle different file types appropriately."""
        all_data = []

        # Load question metadata first if we have 2025 files
        if raw_dir:
            self.load_question_metadata(raw_dir)

        raw_survey_data = []
        for file in files:
            if file.name in REJECTED_CLIMATE_FILENAMES:
                logger.info(f"Skipping rejected climate file: {file.name}")
                continue
            logger.info(f"Processing file: {file}")

            try:
                # Use the unified process_file method
                df = self.process_file(file, raw_dir=raw_dir)
                if (
                    not df.empty
                    and "question_type" in df.columns
                    and "question_index" in df.columns
                ):
                    raw_survey_data.append(df)

                # Convert to KPI format
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
        
        # Calculate aggregate index scores from survey data
        logger.info("Calculating aggregate climate and safety index scores from survey data")
        
        if raw_survey_data:
            # Combine all raw survey data
            all_survey_df = pd.concat(raw_survey_data, ignore_index=True)

            # DEBUG: Check district values in survey data (uses district_name internally)
            if 'district_name' in all_survey_df.columns:
                logger.info(f"Survey data has district_name column. Sample values: {all_survey_df['district_name'].value_counts().head(10).to_dict()}")
                logger.info(f"Survey data district_name null count: {all_survey_df['district_name'].isna().sum()} out of {len(all_survey_df)}")
            else:
                logger.warning("Survey data MISSING district_name column!")

            # CRITICAL FIX: Normalize year column for 2025 survey files
            # The 2025 files have school_year="20242025" but we need year="2025"
            if 'year' in all_survey_df.columns:
                # Extract last 4 digits from year column (handles both "2025" and "20242025")
                all_survey_df['year'] = all_survey_df['year'].astype(str).str[-4:]
                logger.info(f"Normalized year values in survey data: {all_survey_df['year'].value_counts().to_dict()}")

            # Calculate aggregated scores
            calculated_scores_df = calculate_aggregate_index_scores(all_survey_df)

            if not calculated_scores_df.empty:
                # DEBUG: Check district values in calculated scores (uses district_name internally)
                if 'district_name' in calculated_scores_df.columns:
                    logger.info(f"Calculated scores have district_name column. Sample values: {calculated_scores_df['district_name'].value_counts().head(10).to_dict()}")
                    logger.info(f"Calculated scores district_name null count: {calculated_scores_df['district_name'].isna().sum()} out of {len(calculated_scores_df)}")
                    # Check for 2025 data specifically
                    if 'year' in calculated_scores_df.columns:
                        logger.info(f"Calculated scores year values: {calculated_scores_df['year'].value_counts().to_dict()}")
                        year_2025_df = calculated_scores_df[calculated_scores_df['year'] == '2025']
                        if not year_2025_df.empty:
                            logger.info(f"Year 2025 calculated scores: {len(year_2025_df)} records")
                            logger.info(f"Year 2025 Fayette County calculated scores: {len(year_2025_df[year_2025_df['district_name'] == 'Fayette County'])} records")
                        else:
                            logger.warning("No year 2025 calculated scores found!")
                    else:
                        logger.warning("Calculated scores MISSING year column!")
                else:
                    logger.warning("Calculated scores MISSING district_name column!")

                # Format calculated scores as KPI data
                calculated_kpi_df = self.format_calculated_scores_as_kpi(calculated_scores_df)
                if not calculated_kpi_df.empty:
                    logger.info(f"Adding {len(calculated_kpi_df)} calculated index score records")

                    # DEBUG: Check district values in formatted KPI data
                    if 'district' in calculated_kpi_df.columns:
                        logger.info(f"Formatted KPI data district sample: {calculated_kpi_df['district'].value_counts().head(5).to_dict()}")
                        if 'year' in calculated_kpi_df.columns:
                            year_2025_kpi = calculated_kpi_df[calculated_kpi_df['year'] == '2025']
                            if not year_2025_kpi.empty:
                                logger.info(f"Year 2025 formatted KPI records: {len(year_2025_kpi)}")
                                logger.info(f"Year 2025 Fayette County formatted KPI records: {len(year_2025_kpi[year_2025_kpi['district'] == 'Fayette County'])}")

                    combined_df = pd.concat([combined_df, calculated_kpi_df], ignore_index=True)

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
            'KYRC24_ACCT_Index_Scores.csv',
            'KYRC25_ACCT_Index_Scores.csv',
            'KYRC26_ACCT_Index_Scores.csv',
            'KYRC24_ACCT_Survey_Results.csv',  # 2024 survey questions - now with chunked processing
            'KYRC24_SAFE_Precautionary_Measures.csv',
            'KYRC25_SAFE_Precautionary_Measures.csv',
            'KYRC26_SAFE_Precautionary_Measures.csv',
            'accountability_profile_2022.csv',  # Climate indicator starts in 2022
            'accountability_profile_2023.csv',
            'precautionary_measures_*.csv',
            'quality_of_school_climate_and_safety_survey_*.csv',  # Historical survey data (lowercase)
            'quality_of_school_climate_and_safety_survey_*.CSV',  # Historical survey data (uppercase)
            # 2025 response files. The temporary index-scores name is rejected.
            'Quality_of_School_Climate_and_Safety_Survey_Elementary_School_2025.CSV',
            'Quality_of_School_Climate_and_Safety_Survey_Middle_School_2025.CSV',
            'Quality_of_School_Climate_and_Safety_Survey_High_School_2025.CSV',
            'Quality_of_School_Climate_and_Safety_Survey_Questions_2025.CSV',  # Metadata file
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
    result_df = etl.process_files(csv_files, raw_dir=raw_dir)
    
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