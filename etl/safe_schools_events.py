# Copyright 2025 Kentucky Open Government Coalition
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Safe Schools Behavioral Events ETL Module

Handles behavioral event counts from the Kentucky SAFE files for 2019-20
through 2025-26. KYRC24, KYRC25, and KYRC26 are separate type, grade,
location, and context files. The 2019-20 through 2022-23 type counts come
from the combined event-details files, and the grade counts come from the
events-by-grade files.

Each stored count is the cell KDE published. A ``*`` is value NA with
``suppressed`` Y. A blank cell is not stored. A published 0 is kept.
``safe_incident_rate_*`` is an incident rate: events per involved student.
The pipeline calculates it. KDE does not publish it.

The module reads an allow-list of filenames under ``data/raw/safe_schools``.
``include_kyrc26`` does not apply. ``KYRC23_SAFE_Behavior_Events_by_*.csv``
republishes 2022-23 and is not on the list.
"""
from pathlib import Path
import gc
import pandas as pd
from pandas.api.types import union_categoricals
from typing import Dict, Any, Optional, Union
import logging
try:
    from .constants import KPI_COLUMNS
except ImportError:
    from constants import KPI_COLUMNS
try:
    from .base_etl import Config
except ImportError:
    try:
        from base_etl import Config
    except ImportError:
        import sys
        from pathlib import Path
        sys.path.append(str(Path(__file__).parent))
        from base_etl import Config
try:
    from .demographic_mapper import DemographicMapper
except ImportError:
    try:
        from etl.demographic_mapper import DemographicMapper
    except ImportError:
        import sys
        from pathlib import Path
        sys.path.append(str(Path(__file__).parent))
        from demographic_mapper import DemographicMapper
try:
    from .kyrc import normalize_kyrc_grade_token
except ImportError:
    try:
        from kyrc import normalize_kyrc_grade_token
    except ImportError:
        from etl.kyrc import normalize_kyrc_grade_token

logger = logging.getLogger(__name__)

# KYRC25 and KYRC26 grade headers are PS / 1st / 14th. The helper maps
# those onto Preschool / Grade 1 / Grade 14 before the column map below.
BEHAVIOR_KINDS = ("Type", "Grade_Level", "Location", "Context")
KYRC_RELEASES = (24, 25, 26)
CELL_FLAG_PREFIX = "_cell_"

# The Total column is the same measure on the type, location, and context
# files. Location and context publish a number on some rows where the type
# file publishes ``*``. Emitting all three under ``safe_*_total`` duplicates
# the key. The type file keeps the metric. Location and context keep their
# own breakdown columns.
_TOTAL_METRIC = "total_events"

HISTORICAL_EVENT_FILES = [
    "safe_schools_event_details_2020.csv",
    "safe_schools_event_details_2021.csv",
    "safe_schools_event_details_2022.csv",
    "safe_schools_event_details_2023.csv",
    "safe_schools_events_by_grade_2020.csv",
    "safe_schools_events_by_grade_2021.csv",
    "safe_schools_events_by_grade_2022.csv",
    "safe_schools_events_by_grade_2023.csv",
]


def kyrc_behavior_filenames() -> list[str]:
    """KYRC24, KYRC25, and KYRC26 behavior files this module reads."""
    return [
        f"KYRC{release}_SAFE_Behavior_Events_by_{kind}.csv"
        for release in KYRC_RELEASES
        for kind in BEHAVIOR_KINDS
    ]


def rejected_event_filenames() -> frozenset[str]:
    """Names that must not be parsed, even if a copy is left in the raw directory.

    ``KYRC23_SAFE_Behavior_Events_by_*.csv`` republishes 2022-23. The guessed
    temporary OAA names returned 404 and were never registered. The allow-list
    is what ``transform`` reads. This set is the explicit reject list so a
    later directory scan cannot write the year twice.
    """
    rejected = [
        f"KYRC23_SAFE_Behavior_Events_by_{kind}.csv" for kind in BEHAVIOR_KINDS
    ]
    for year in (2024, 2025, 2026):
        for kind in BEHAVIOR_KINDS:
            rejected.append(f"Behavior_Events_by_{kind}_{year}.CSV")
            rejected.append(f"SAFE_Behavior_Events_by_{kind}_{year}.CSV")
    rejected.append("SAFE_SCHOOLS_18-19.xlsx")
    rejected.append("SAFE_SCHOOLS_17-18.xlsx")
    return frozenset(rejected)


def files_to_process() -> list[str]:
    """Allow-list of behavior files. Rejected names are not included."""
    allowed = kyrc_behavior_filenames() + list(HISTORICAL_EVENT_FILES)
    rejected = rejected_event_filenames()
    return [name for name in allowed if name not in rejected]


def _is_string_like(series: pd.Series) -> bool:
    """True for object text and for pandas StringDtype.

    ``read_csv(..., dtype=str)`` on pandas 3 returns StringDtype, not object.
    A comma in ``86,009`` has to be removed before ``to_numeric``. pandas
    2.3.3 still returns object. pandas is unpinned.
    """
    return series.dtype == object or bool(pd.api.types.is_string_dtype(series))


def _missing_mask(series: pd.Series) -> pd.Series:
    """True for None, blanks, and pandas missing values.

    ``bool(pd.NA)`` is not used. A numeric 0 is present.
    """
    if _is_string_like(series):
        text = series.astype("string").str.strip()
        return text.isna() | text.eq("") | text.str.lower().isin(["nan", "<na>", "none"])
    return series.isna()


def _classify_measure(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Return numeric values and a flag of number, star, blank, or invalid.

    The star is recorded before the cell is coerced. A blank is not a star.
    A published 0 stays 0. Commas are stripped on object and on StringDtype.
    """
    if not _is_string_like(series):
        values = pd.to_numeric(series, errors="coerce")
        flag = pd.Series("number", index=series.index, dtype="object")
        neg = values.notna() & (values < 0)
        flag.loc[values.isna()] = "blank"
        flag.loc[neg] = "invalid"
        values = values.mask(values.isna() | neg)
        return values, flag

    text = series.astype("string").str.strip()
    star = text.eq("*").fillna(False)
    blank = (text.isna() | text.eq("")).fillna(False)
    stripped = text.str.replace(",", "", regex=False)
    values = pd.to_numeric(stripped.mask(star | blank), errors="coerce")
    invalid = ~(star | blank) & values.isna()
    neg = values.notna() & (values < 0)
    values = values.mask(star | blank | invalid | neg)
    flag = pd.Series("number", index=series.index, dtype="object")
    flag.loc[blank] = "blank"
    flag.loc[star] = "star"
    flag.loc[invalid | neg] = "invalid"
    return values, flag


def extract_year_from_school_year(year_value):
    """Extract year using same logic as base_etl.py - take last 4 digits for ending year."""
    year = str(year_value)
    if len(year) == 8:  # Format: YYYYYYYY (e.g., "20232024")
        return year[-4:]  # Take last 4 digits (ending year)
    elif len(year) == 4:  # Already 4 digits
        return year
    else:
        return '2024'  # Default


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names to lowercase with underscores.

    KYRC25 and KYRC26 grade headers are ``PS``, ``K``, ``1st`` through
    ``12th``, and ``14th``. There is no ``13th``. ``normalize_kyrc_grade_token``
    maps those onto the KYRC24 names (``Preschool``, ``Grade 1``) before the
    dictionary below. Labels that are already historical are unchanged.
    """
    renamed_grades = {}
    for col in df.columns:
        mapped = normalize_kyrc_grade_token(col)
        if mapped != col:
            renamed_grades[col] = mapped
    if renamed_grades:
        df = df.rename(columns=renamed_grades)

    column_mapping = {
        # KYRC24 format columns
        'School Year': 'school_year',
        'County Number': 'county_number', 
        'County Name': 'county_name',
        'District Number': 'district_number',
        'District Name': 'district_name',
        'School Number': 'school_number',
        'School Name': 'school_name',
        'School Code': 'school_code',
        'State School Id': 'state_school_id',
        'NCES ID': 'nces_id',
        'CO-OP': 'co_op',
        'CO-OP Code': 'co_op_code',
        'School Type': 'school_type',
        'Demographic': 'demographic',
        
        # Historical format columns (uppercase)
        'SCHOOL YEAR': 'school_year',
        'COUNTY NUMBER': 'county_number',
        'COUNTY NAME': 'county_name', 
        'DISTRICT NUMBER': 'district_number',
        'DISTRICT NAME': 'district_name',
        'SCHOOL NUMBER': 'school_number',
        'SCHOOL NAME': 'school_name',
        'SCHOOL CODE': 'school_code',
        'STATE SCHOOL ID': 'state_school_id',
        'NCES ID': 'nces_id',
        'CO-OP': 'co_op',
        'CO-OP CODE': 'co_op_code',
        'SCHOOL TYPE': 'school_type',
        'DEMOGRAPHIC': 'demographic',
        
        # Event type columns (KYRC24)
        'Total': 'total_events',
        'Alcohol': 'alcohol_events',
        'Assault, 1st Degree': 'assault_1st_degree',
        'Drugs': 'drug_events',
        'Harassment (Includes Bullying)': 'harassment_events',
        'Other Assault or Violence': 'other_assault_events',
        'Other Events Resulting in State Resolutions': 'other_state_events',
        'Tobacco': 'tobacco_events',
        'Weapons': 'weapon_events',
        
        # Event type columns (Historical)
        'TOTAL EVENTS BY TYPE': 'total_events',
        'ASSAULT 1ST DEGREE': 'assault_1st_degree',
        'OTHER ASSAULT OR VIOLENCE': 'other_assault_events',
        'WEAPONS': 'weapon_events',
        'HARRASSMENT (INCLUDES BULLYING)': 'harassment_events',
        'DRUGS': 'drug_events',
        'ALCOHOL': 'alcohol_events',
        'TOBACCO': 'tobacco_events',
        'OTHER EVENTS W_STATE RESOLUTION': 'other_state_events',
        
        # Grade level columns (KYRC24)
        'All Grades': 'all_grades',
        'Preschool': 'preschool',
        'K': 'kindergarten',
        'Grade 1': 'grade_1',
        'Grade 2': 'grade_2',
        'Grade 3': 'grade_3',
        'Grade 4': 'grade_4',
        'Grade 5': 'grade_5',
        'Grade 6': 'grade_6',
        'Grade 7': 'grade_7',
        'Grade 8': 'grade_8',
        'Grade 9': 'grade_9',
        'Grade 10': 'grade_10',
        'Grade 11': 'grade_11',
        'Grade 12': 'grade_12',
        'Grade 14': 'grade_14',
        
        # Grade level columns (Historical)
        'TOTAL STUDENT COUNT': 'all_grades',
        'PRESCHOOL COUNT': 'preschool',
        'KINDERGARTEN COUNT': 'kindergarten',
        'GRADE1 COUNT': 'grade_1',
        'GRADE2 COUNT': 'grade_2',
        'GRADE3 COUNT': 'grade_3',
        'GRADE4 COUNT': 'grade_4',
        'GRADE5 COUNT': 'grade_5',
        'GRADE6 COUNT': 'grade_6',
        'GRADE7 COUNT': 'grade_7',
        'GRADE8 COUNT': 'grade_8',
        'GRADE9 COUNT': 'grade_9',
        'GRADE10 COUNT': 'grade_10',
        'GRADE11 COUNT': 'grade_11',
        'GRADE12 COUNT': 'grade_12',
        'GRADE14 COUNT': 'grade_14',
        
        # Location columns (KYRC24)
        'Classroom': 'classroom',
        'Bus': 'bus',
        'Hallway/Stairwell': 'hallway_stairwell',
        'Cafeteria': 'cafeteria',
        'Restroom': 'restroom',
        'Gymnasium': 'gymnasium',
        'Playground': 'playground',
        'Other': 'other_location',
        'Campus Grounds': 'campus_grounds',
        
        # Location columns (Historical - from event details)
        'LOCATION - CLASSROOM': 'classroom',
        'LOCATION - BUS': 'bus',
        'LOCATION - HALLWAY/STAIRWAY': 'hallway_stairwell',
        'LOCATION - CAFETERIA': 'cafeteria',
        'LOCATION - CAMPUS GROUNDS': 'campus_grounds',
        'LOCATION - RESTROOM': 'restroom',
        'LOCATION - GYMNASIUM': 'gymnasium',
        'LOCATION - PLAYGROUND': 'playground',
        'LOCATION - OTHER': 'other_location',
        
        # Context columns (KYRC24)
        'School Sponsored Event; During School Hours': 'school_sponsored_during',
        'School Sponsored Event; Not During School Hours': 'school_sponsored_not_during',
        'Non School Sponsored Event; During School Hours': 'non_school_sponsored_during',
        'Non School Sponsored Event; Not During School Hours': 'non_school_sponsored_not_during',
        
        # Context columns (Historical)
        'SCHOOL SPONSORED SCHOOL HOURS': 'school_sponsored_during',
        'SCHOOL SPONSORED NOT SCHOOL HOURS': 'school_sponsored_not_during',
        'NON-SCHOOL SPONSORED SCHOOL HOURS': 'non_school_sponsored_during',
        'NON-SCHOOL SPONSORED NOT SCHOOL HOURS': 'non_school_sponsored_not_during',
    }
    
    # Apply column mapping
    df = df.rename(columns=column_mapping)
    
    # Convert any remaining columns to lowercase with underscores
    def to_snake_case(name):
        import re
        name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()
        return name.replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '')
    
    columns_to_rename = {}
    for col in df.columns:
        if col not in column_mapping.values():
            new_name = to_snake_case(col)
            if new_name != col:
                columns_to_rename[col] = new_name
    
    if columns_to_rename:
        df = df.rename(columns=columns_to_rename)
    
    return df


def add_derived_fields(df: pd.DataFrame, derive_config: Dict[str, Any]) -> pd.DataFrame:
    """Add derived fields based on configuration and detect data source."""
    for field, value in derive_config.items():
        df[field] = value
    
    # Detect data source based on file structure
    # First check for historical patterns (more specific)
    if 'total_events' in df.columns and 'assault_1st_degree' in df.columns and 'classroom' in df.columns:
        # Historical format - comprehensive event details (has all: events, types, locations, context)
        df['data_source'] = 'historical_event_details'
    elif 'all_grades' in df.columns and 'preschool' in df.columns and 'total_student_count' not in df.columns:
        # Historical format - events by grade (grade columns without total_student_count)
        df['data_source'] = 'historical_events_by_grade'
    elif 'total_events' in df.columns:
        # KYRC24 format detection - check specific patterns
        if 'all_grades' in df.columns:
            df['data_source'] = 'kyrc24_events_by_grade'
        elif 'classroom' in df.columns and 'bus' in df.columns and 'alcohol_events' not in df.columns:
            df['data_source'] = 'kyrc24_events_by_location'
        elif 'school_sponsored_during' in df.columns and 'alcohol_events' not in df.columns:
            df['data_source'] = 'kyrc24_events_by_context'
        elif 'alcohol_events' in df.columns:
            df['data_source'] = 'kyrc24_events_by_type'
        else:
            df['data_source'] = 'unknown_format'
    else:
        df['data_source'] = 'unknown_format'
    
    return df


_IDENTIFIER_COLUMNS = {
    "school_year",
    "county_number",
    "county_name",
    "district_number",
    "district_name",
    "school_number",
    "school_name",
    "school_code",
    "state_school_id",
    "nces_id",
    "co_op",
    "co_op_code",
    "school_type",
    "demographic",
    "data_source",
    "year",
}


def _metric_columns_present(df: pd.DataFrame) -> list[str]:
    """Every non-identifier column. Those are the published count cells.

    A keyword list misses ``assault_1st_degree`` and ``other_location``.
    ``county_name`` contains ``count`` and is an identifier, so it is excluded
    by name rather than by substring.
    """
    columns = []
    for col in df.columns:
        if str(col).startswith(CELL_FLAG_PREFIX) or col in _IDENTIFIER_COLUMNS:
            continue
        columns.append(col)
    return columns


def clean_event_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean event counts and record whether each cell was a star or a blank.

    A ``*`` is stored as missing and flagged ``star``, so the KPI row can be
    ``suppressed`` Y. A blank is flagged ``blank`` and is not emitted. A
    published 0 stays 0 and is flagged ``number``. Commas are stripped before
    ``to_numeric`` on both object and StringDtype columns.
    """
    for col in _metric_columns_present(df):
        values, flag = _classify_measure(df[col])
        invalid_mask = flag.eq("invalid")
        if invalid_mask.any():
            logger.warning(f"Found {int(invalid_mask.sum())} non-numeric event counts in {col}")
        df[col] = values
        df[f"{CELL_FLAG_PREFIX}{col}"] = flag
    return df


def convert_to_kpi_format(df: pd.DataFrame, demographic_mapper: Optional[DemographicMapper] = None) -> pd.DataFrame:
    """Convert wide format safe schools events data to long KPI format with three-tier structure."""
    from datetime import datetime
    
    # Initialize demographic mapper if not provided
    if demographic_mapper is None:
        demographic_mapper = DemographicMapper()
    
    # Separate rows into three tiers based on data interpretation discovery
    students_affected_rows = pd.DataFrame()
    demographic_rows = pd.DataFrame()
    total_events_rows = pd.DataFrame()
    
    if 'demographic' in df.columns:
        # Tier 1: Students Affected (scope metrics) - "All Students" rows
        all_students_values = ['All Students', 'all students', 'ALL STUDENTS']
        students_affected_mask = df['demographic'].isin(all_students_values)
        
        # Tier 3: Total Events (intensity metrics) - "Total Events" rows
        total_events_values = ['Total Events', 'total events', 'TOTAL EVENTS']
        total_events_mask = df['demographic'].isin(total_events_values)
        
        # Tier 2: Demographic breakdowns (equity metrics) - all other rows
        demographic_mask = ~(students_affected_mask | total_events_mask)
        
        if students_affected_mask.any():
            students_affected_rows = df[students_affected_mask].copy()
        if total_events_mask.any():
            total_events_rows = df[total_events_mask].copy()
        if demographic_mask.any():
            demographic_rows = df[demographic_mask].copy()
    else:
        # If no demographic column, treat all as demographic rows
        demographic_rows = df.copy()
    
    all_kpi_data = []
    
    # Process Tier 1: Students Affected (scope metrics)
    if len(students_affected_rows) > 0:
        kpi_students_affected = _process_students_affected_rows(students_affected_rows)
        if not kpi_students_affected.empty:
            all_kpi_data.append(kpi_students_affected)
    
    # Process Tier 2: Demographic breakdowns (equity metrics)
    if len(demographic_rows) > 0:
        kpi_demographic = _process_demographic_rows(demographic_rows, demographic_mapper)
        if not kpi_demographic.empty:
            all_kpi_data.append(kpi_demographic)
    
    # Process Tier 3: Total Events (intensity metrics)
    if len(total_events_rows) > 0:
        kpi_total_events = _process_total_events_rows(total_events_rows)
        if not kpi_total_events.empty:
            all_kpi_data.append(kpi_total_events)
    
    # Combine all results
    if all_kpi_data:
        combined_df = pd.concat(all_kpi_data, ignore_index=True)
        
        # Add Tier 4: incident rate, events per involved student
        logger.info(f"Calculating derived rates for {len(combined_df)} KPI records")
        derived_rates = _calculate_derived_rates(combined_df)
        if not derived_rates.empty:
            logger.info(f"Generated {len(derived_rates)} derived rate records")
            combined_df = pd.concat([combined_df, derived_rates], ignore_index=True)
        else:
            logger.warning("No derived rates were generated")
        
        combined_df = combined_df.reindex(columns=KPI_COLUMNS)
        return combined_df
    else:
        logger.warning("No valid data found for KPI conversion")
        return pd.DataFrame()


def _school_id_series(kpi_df: pd.DataFrame) -> pd.Series:
    """State school id when KDE published one, otherwise the school code.

    District and state aggregate rows leave State School Id blank. Using
    that blank as ``school_id`` collides every district on the same key.
    The published school code is already 999 or 999000. It is not padded.
    School rows that have a state school id keep that id.
    """
    if "state_school_id" in kpi_df.columns:
        sid = kpi_df["state_school_id"]
    else:
        sid = pd.Series(pd.NA, index=kpi_df.index)
    if "school_code" in kpi_df.columns:
        code = kpi_df["school_code"]
    else:
        code = pd.Series(pd.NA, index=kpi_df.index)
    missing = _missing_mask(sid)
    school_id = sid.astype("object")
    return school_id.where(~missing, code.astype("object"))


def _process_rows_helper(
    df: pd.DataFrame,
    data_source: str,
    prefix: str,
    student_group: Union[str, pd.Series],
    suffix: str = "",
) -> pd.DataFrame:
    """Shared logic for melting and metric mapping across tiers."""
    from datetime import datetime

    if "events_by_type" in data_source or "event_details" in data_source:
        metric_columns = [
            "total_events",
            "alcohol_events",
            "assault_1st_degree",
            "drug_events",
            "harassment_events",
            "other_assault_events",
            "other_state_events",
            "tobacco_events",
            "weapon_events",
        ]
    elif "events_by_grade" in data_source:
        metric_columns = [
            "all_grades",
            "preschool",
            "kindergarten",
            "grade_1",
            "grade_2",
            "grade_3",
            "grade_4",
            "grade_5",
            "grade_6",
            "grade_7",
            "grade_8",
            "grade_9",
            "grade_10",
            "grade_11",
            "grade_12",
            "grade_14",
        ]
    elif "events_by_location" in data_source:
        # total_events is omitted. The type file owns safe_*_total. The
        # location Total matches that cell except where the type file is
        # starred and this file publishes a number. Those cells are not a
        # second measure and are not stored under the same key.
        metric_columns = [
            "classroom",
            "bus",
            "hallway_stairwell",
            "cafeteria",
            "restroom",
            "gymnasium",
            "playground",
            "other_location",
            "campus_grounds",
        ]
    elif "events_by_context" in data_source:
        metric_columns = [
            "school_sponsored_during",
            "school_sponsored_not_during",
            "non_school_sponsored_during",
            "non_school_sponsored_not_during",
        ]
    else:
        logger.warning(f"Unknown data source: {data_source}")
        return pd.DataFrame()

    metric_columns = [col for col in metric_columns if col in df.columns]
    if not metric_columns:
        logger.warning("No metric columns found for KPI conversion")
        return pd.DataFrame()

    # Extract year using standardized helper function
    df["year"] = df["school_year"].apply(extract_year_from_school_year).astype(int)

    id_columns = [
        "district_name",
        "state_school_id",
        "school_code",
        "school_name",
        "year",
        "demographic",
        "data_source",
    ]
    id_columns = [col for col in id_columns if col in df.columns]

    kpi_df = df.melt(
        id_vars=id_columns,
        value_vars=metric_columns,
        var_name="metric_raw",
        value_name="value",
    )

    flag_source = df[id_columns].copy()
    for col in metric_columns:
        flag_name = f"{CELL_FLAG_PREFIX}{col}"
        if flag_name in df.columns:
            flag_source[col] = df[flag_name]
        else:
            flag_source[col] = "number"
    flag_long = flag_source.melt(
        id_vars=id_columns,
        value_vars=metric_columns,
        var_name="metric_raw",
        value_name="cell_flag",
    )
    kpi_df["cell_flag"] = flag_long["cell_flag"].to_numpy()
    # A blank cell is not an event and is not stored. A star stays, as NA.
    kpi_df = kpi_df.loc[kpi_df["cell_flag"] != "blank"].copy()

    kpi_df["value"] = pd.to_numeric(kpi_df["value"], errors="coerce")

    base_map = {
        "total_events": "total",
        "alcohol_events": "alcohol",
        "assault_1st_degree": "assault_1st",
        "drug_events": "drugs",
        "harassment_events": "harassment",
        "other_assault_events": "assault_other",
        "other_state_events": "other_state",
        "tobacco_events": "tobacco",
        "weapon_events": "weapons",
        "hallway_stairwell": "hallway",
        "campus_grounds": "campus",
        "school_sponsored_not_during": "school_sponsored_after",
        "non_school_sponsored_not_during": "non_school_after",
    }

    metric_mapping = {
        col: f"{prefix}_{base_map.get(col, col)}{suffix}"
        for col in metric_columns
    }
    kpi_df["metric"] = kpi_df["metric_raw"].map(metric_mapping)

    # The star was recorded before coercion. Invalid text is missing too.
    # A published number, including 0, stays suppressed N.
    kpi_df["suppressed"] = "N"
    starred = kpi_df["cell_flag"].isin(["star", "invalid"])
    kpi_df.loc[starred, "suppressed"] = "Y"
    kpi_df.loc[starred, "value"] = pd.NA

    if isinstance(student_group, pd.Series):
        repeated = pd.concat([
            student_group.reset_index(drop=True)
            for _ in range(len(metric_columns))
        ], ignore_index=True)
        kpi_df["student_group"] = repeated
    else:
        kpi_df["student_group"] = student_group

    final_df = pd.DataFrame(
        {
            "district": kpi_df.get("district_name", "Unknown"),
            "school_id": _school_id_series(kpi_df),
            "school_name": kpi_df.get("school_name", "Unknown"),
            "year": kpi_df["year"],
            "student_group": kpi_df["student_group"],
            "county_number": kpi_df.get("county_number", pd.NA),
            "county_name": kpi_df.get("county_name", pd.NA),
            "district_number": kpi_df.get("district_number", pd.NA),
            "school_code": kpi_df.get("school_code", pd.NA),
            "state_school_id": kpi_df.get("state_school_id", pd.NA),
            "nces_id": kpi_df.get("nces_id", pd.NA),
            "co_op": kpi_df.get("co_op", pd.NA),
            "co_op_code": kpi_df.get("co_op_code", pd.NA),
            "school_type": kpi_df.get("school_type", pd.NA),
            "metric": kpi_df["metric"],
            "value": kpi_df["value"],
            "suppressed": kpi_df["suppressed"],
        }
    )

    final_df = final_df[final_df["metric"].notna()]
    # Ensure standard column ordering
    final_df = final_df.reindex(columns=KPI_COLUMNS[:-2])
    return final_df


def _process_demographic_rows(
    df: pd.DataFrame, demographic_mapper: DemographicMapper
) -> pd.DataFrame:
    """Process rows with demographic breakdowns."""
    from datetime import datetime

    data_source = df["data_source"].iloc[0] if "data_source" in df.columns else "unknown"
    # Extract year using standardized helper function
    df["year"] = df["school_year"].apply(extract_year_from_school_year).astype(int)
    source_file = f"safe_schools_events_{data_source}"

    student_groups = df.apply(
        lambda row: demographic_mapper.map_demographic(
            row["demographic"], row["year"], source_file
        ),
        axis=1,
    )

    final_df = _process_rows_helper(
        df, data_source, "safe_event_count", student_groups, suffix="_by_demo"
    )
    if final_df.empty:
        return final_df
    final_df["source_file"] = source_file
    final_df["last_updated"] = datetime.now().isoformat()
    final_df = final_df.reindex(columns=KPI_COLUMNS)
    return final_df


def _process_students_affected_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Process 'All Students' rows as students affected (scope metrics)."""
    from datetime import datetime

    data_source = df["data_source"].iloc[0] if "data_source" in df.columns else "unknown"
    final_df = _process_rows_helper(
        df, data_source, "safe_students_affected", "All Students"
    )
    if final_df.empty:
        return final_df
    final_df["source_file"] = f"safe_schools_events_{data_source}_students_affected"
    final_df["last_updated"] = datetime.now().isoformat()
    final_df = final_df.reindex(columns=KPI_COLUMNS)
    return final_df


def _process_total_events_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Process 'Total Events' rows as intensity metrics."""
    from datetime import datetime

    data_source = df["data_source"].iloc[0] if "data_source" in df.columns else "unknown"
    final_df = _process_rows_helper(
        df, data_source, "safe_event_count", "All Students - Total Events"
    )
    if final_df.empty:
        return final_df
    final_df["source_file"] = f"safe_schools_events_{data_source}_total_events"
    final_df["last_updated"] = datetime.now().isoformat()
    final_df = final_df.reindex(columns=KPI_COLUMNS)
    return final_df


def _calculate_derived_rates(kpi_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate Tier 4 derived rates (events per affected student) - optimized version."""
    from datetime import datetime
    
    if kpi_df.empty:
        logger.warning("Empty KPI dataframe for derived rates calculation")
        return pd.DataFrame()
    
    # Separate students affected and total events data using vectorized operations
    students_affected_df = kpi_df[
        (kpi_df['metric'].str.startswith('safe_students_affected_')) & 
        (kpi_df['student_group'] == 'All Students')
    ].copy()
    
    total_events_df = kpi_df[
        (kpi_df['metric'].str.startswith('safe_event_count_')) & 
        (kpi_df['student_group'] == 'All Students - Total Events') &
        (~kpi_df['metric'].str.contains('_by_demo')) &
        (~kpi_df['metric'].str.contains('students_affected'))
    ].copy()
    
    if students_affected_df.empty or total_events_df.empty:
        logger.info("No matching students affected or total events data for rate calculation")
        return pd.DataFrame()
    
    # Extract base metric names
    students_affected_df['base_metric'] = students_affected_df['metric'].str.replace('safe_students_affected_', '')
    total_events_df['base_metric'] = total_events_df['metric'].str.replace('safe_event_count_', '')
    
    # Merge on key fields to calculate rates and retain location columns
    merge_cols = ['district', 'school_id', 'school_name', 'year', 'base_metric']
    location_cols = [
        'county_number', 'county_name', 'district_number', 'school_code',
        'state_school_id', 'nces_id', 'co_op', 'co_op_code', 'school_type'
    ]

    merged_df = pd.merge(
        students_affected_df[merge_cols + location_cols + ['value']].rename(columns={'value': 'students_affected'}),
        total_events_df[merge_cols + location_cols + ['value']].rename(columns={'value': 'total_events'}),
        on=merge_cols,
        how='inner',
        suffixes=('_sa', '_te')
    )

    for col in location_cols:
        merged_df[col] = merged_df[f"{col}_sa"].combine_first(merged_df[f"{col}_te"])
        merged_df.drop(columns=[f"{col}_sa", f"{col}_te"], inplace=True)
    
    if merged_df.empty:
        logger.info("No matching data for rate calculation after merge")
        return pd.DataFrame()
    
    # Calculate rates vectorized
    merged_df['students_affected'] = pd.to_numeric(merged_df['students_affected'], errors='coerce')
    merged_df['total_events'] = pd.to_numeric(merged_df['total_events'], errors='coerce')
    
    # Only calculate rates where both values are valid and students_affected > 0
    valid_mask = (
        merged_df['students_affected'].notna() & 
        merged_df['total_events'].notna() &
        (merged_df['students_affected'] > 0) &
        (merged_df['total_events'] >= 0)
    )
    
    valid_df = merged_df[valid_mask].copy()
    
    if valid_df.empty:
        logger.info("No valid data for rate calculation")
        return pd.DataFrame()
    
    # Calculate incident rates
    valid_df['incident_rate'] = (valid_df['total_events'] / valid_df['students_affected']).round(3)
    
    # Create final rate records
    rate_records = pd.DataFrame({
        'district': valid_df['district'],
        'school_id': valid_df['school_id'],
        'school_name': valid_df['school_name'],
        'year': valid_df['year'],
        'student_group': 'All Students',
        'county_number': valid_df['county_number'],
        'county_name': valid_df['county_name'],
        'district_number': valid_df['district_number'],
        'school_code': valid_df['school_code'],
        'state_school_id': valid_df['state_school_id'],
        'nces_id': valid_df['nces_id'],
        'co_op': valid_df['co_op'],
        'co_op_code': valid_df['co_op_code'],
        'school_type': valid_df['school_type'],
        'metric': 'safe_incident_rate_' + valid_df['base_metric'],
        'value': valid_df['incident_rate'],
        'suppressed': 'N',
        'source_file': 'derived_rates_' + valid_df['base_metric'],
        'last_updated': datetime.now().isoformat()
    })
    rate_records = rate_records.reindex(columns=KPI_COLUMNS)
    
    logger.info(f"Generated {len(rate_records)} derived rate records")
    return rate_records


def _process_aggregate_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Process aggregate rows (Total Events) as separate KPIs."""
    from datetime import datetime

    data_source = df["data_source"].iloc[0] if "data_source" in df.columns else "unknown"
    final_df = _process_rows_helper(
        df, data_source, "safe_event_count", "All Students - Aggregate", suffix="_aggregate"
    )
    if final_df.empty:
        return final_df
    final_df["source_file"] = f"safe_schools_events_{data_source}_aggregate"
    final_df["last_updated"] = datetime.now().isoformat()
    final_df = final_df.reindex(columns=KPI_COLUMNS)
    return final_df


def _read_events_csv(file_path: Path) -> pd.DataFrame:
    """Read one behavior file.

    School Code is read as text so a district code ``001000`` is not inferred
    as ``1000``. Other columns keep pandas' inference. State School Id on a
    school row stays the value the previous parser stored. Measure columns
    that contain ``*`` or a thousands comma stay text and are cleaned later.
    """
    header = pd.read_csv(file_path, nrows=0, encoding="utf-8-sig")
    dtype: Dict[str, str] = {}
    for name in ("School Code", "SCHOOL CODE"):
        if name in header.columns:
            dtype[name] = "string"
    return pd.read_csv(file_path, encoding="utf-8-sig", dtype=dtype or None)


def _string_columns(df: pd.DataFrame) -> list[str]:
    """Columns whose values are text. ``year`` and ``value`` stay numeric."""
    columns = []
    for col in df.columns:
        if col in ("year", "value"):
            continue
        series = df[col]
        if _is_string_like(series) or series.dtype == object:
            columns.append(col)
    return columns


def _cast_string_columns_to_category(df: pd.DataFrame) -> pd.DataFrame:
    """Store repeated labels once per file so the concat does not hold every string.

    A float school id such as ``1001010.0`` and a code such as ``001000`` become
    the same text ``to_csv`` already writes. Every file then has string
    categories, which ``union_categoricals`` can combine.
    """
    for col in _string_columns(df):
        df[col] = df[col].astype("string").astype("category")
    return df


def _align_categories(frames: list[pd.DataFrame]) -> None:
    """Give each file the same category codes so ``concat`` does not expand them.

    ``union_categoricals`` keeps the values. It only widens the code set.
    """
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
    key = ["year", "school_id", "student_group", "metric"]
    duplicate_rows = combined_df.duplicated(key, keep=False)
    if not bool(duplicate_rows.any()):
        return
    sample = combined_df.loc[duplicate_rows, key].head(5)
    raise ValueError(
        f"safe_schools_events has {int(duplicate_rows.sum())} duplicate-key rows. "
        f"Sample: {sample.to_dict('records')}"
    )


def transform(raw_dir: str, proc_dir: str, config: Dict[str, Any]) -> str:
    """Transform safe schools events data from raw to processed format."""
    raw_path = Path(raw_dir) / "safe_schools"
    proc_path = Path(proc_dir)
    
    # Create output directory
    proc_path.mkdir(parents=True, exist_ok=True)
    
    # Initialize demographic mapper for audit logging
    demographic_mapper = DemographicMapper()
    
    all_data = []
    rejected = rejected_event_filenames()
    
    for filename in files_to_process():
        if filename in rejected:
            logger.info(f"Skipping rejected behavior file: {filename}")
            continue
        file_path = raw_path / filename
        if not file_path.exists():
            continue
        try:
            logger.info(f"Processing behavior file: {filename}")
            df = _read_events_csv(file_path)
            df = normalize_column_names(df)
            df = add_derived_fields(df, config.get('derive', {}))
            df = clean_event_data(df)
            kpi_df = convert_to_kpi_format(df, demographic_mapper)
            del df
            if not kpi_df.empty:
                _cast_string_columns_to_category(kpi_df)
                all_data.append(kpi_df)
                logger.info(f"Processed {len(kpi_df)} KPI records from {filename}")
            else:
                logger.warning(f"No data produced from {filename}")
        except Exception as e:
            logger.error(f"Error processing {filename}: {str(e)}")
            raise
    
    # Combine all data
    if all_data:
        # Each file is already in KPI_COLUMNS order. A second reindex copies
        # the whole frame. Align the category codes, then drop the per-file frames.
        _align_categories(all_data)
        combined_df = pd.concat(all_data, ignore_index=True)
        del all_data
        gc.collect()

        # FIX: Ensure value column is numeric for etl_runner validation
        # Convert value column to numeric, this will properly handle suppressed records as NaN
        combined_df['value'] = pd.to_numeric(combined_df['value'], errors='coerce')
        _assert_unique_keys(combined_df)
        
        # Save main output
        output_file = proc_path / 'safe_schools_events.csv'
        combined_df.to_csv(output_file, index=False)
        
        # Save demographic report
        audit_file = proc_path / 'safe_schools_events_demographic_report.md'
        # Validate demographics for report
        validation_results = []
        for year in combined_df['year'].unique():
            year_demographics = combined_df[combined_df['year'] == year]['student_group'].unique().tolist()
            validation_results.append(demographic_mapper.validate_demographics(year_demographics, str(year)))
        demographic_mapper.save_audit_report(audit_file, validation_results)
        
        logger.info(f"Saved {len(combined_df)} total KPI records to {output_file}")
        logger.info(f"Saved demographic report to {audit_file}")
        
        return str(output_file)
    else:
        logger.error("No data was processed successfully")
        return ""


if __name__ == "__main__":
    # Test the module
    import sys
    from pathlib import Path
    
    # Add parent directory to path for imports
    sys.path.append(str(Path(__file__).parent.parent))
    
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
    # Test with sample configuration
    config = Config(derive={"data_version": "2024"}).model_dump()

    raw_dir = "data/raw"
    proc_dir = "data/processed"

    result = transform(raw_dir, proc_dir, config)
    if result:
        print(f"Successfully processed safe schools events data: {result}")
    else:
        print("Failed to process safe schools events data")