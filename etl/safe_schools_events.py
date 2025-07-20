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

Handles behavioral events data from Kentucky SAFE program across years 2020-2024.
Maps KYRC24 format files to historical equivalents for longitudinal analysis.
Covers event types, grade levels, locations, and contexts for school safety metrics.
"""
from pathlib import Path
import pandas as pd
from pydantic import BaseModel
from typing import Dict, Any, Optional, Union
import logging
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

logger = logging.getLogger(__name__)


class Config(BaseModel):
    rename: Dict[str, str] = {}
    dtype: Dict[str, str] = {}
    derive: Dict[str, Union[str, int, float]] = {}


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names to lowercase with underscores."""
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


def clean_event_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate event count values."""
    # Identify numeric columns (event counts)
    numeric_columns = []
    for col in df.columns:
        if any(keyword in col for keyword in ['events', 'count', 'total', 'grade_', 'classroom', 'bus', 'hallway', 'cafeteria', 'restroom', 'gymnasium', 'playground', 'campus', 'school_sponsored', 'non_school']):
            if col not in ['school_year', 'data_source']:
                numeric_columns.append(col)
    
    for col in numeric_columns:
        if col in df.columns:
            # Handle suppression markers
            df[col] = df[col].astype(str).str.replace('*', '', regex=False)
            df[col] = df[col].replace('', pd.NA)
            
            # Convert to numeric, errors='coerce' will make invalid values NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Validate counts are non-negative
            invalid_mask = df[col] < 0
            if invalid_mask.any():
                logger.warning(f"Found {invalid_mask.sum()} negative event counts in {col}")
                df.loc[invalid_mask, col] = pd.NA
    
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
        
        # Add Tier 4: Derived rate calculations (events per affected student)
        logger.info(f"Calculating derived rates for {len(combined_df)} KPI records")
        derived_rates = _calculate_derived_rates(combined_df)
        if not derived_rates.empty:
            logger.info(f"Generated {len(derived_rates)} derived rate records")
            combined_df = pd.concat([combined_df, derived_rates], ignore_index=True)
        else:
            logger.warning("No derived rates were generated")
        
        return combined_df
    else:
        logger.warning("No valid data found for KPI conversion")
        return pd.DataFrame()


def _process_demographic_rows(df: pd.DataFrame, demographic_mapper: DemographicMapper) -> pd.DataFrame:
    """Process rows with demographic breakdowns."""
    from datetime import datetime
    
    # Identify the metric columns based on data source
    data_source = df['data_source'].iloc[0] if 'data_source' in df.columns else 'unknown'
    
    if 'events_by_type' in data_source or 'event_details' in data_source:
        # Event type metrics
        metric_columns = [
            'total_events', 'alcohol_events', 'assault_1st_degree', 'drug_events',
            'harassment_events', 'other_assault_events', 'other_state_events',
            'tobacco_events', 'weapon_events'
        ]
    elif 'events_by_grade' in data_source:
        # Grade level metrics
        metric_columns = [
            'all_grades', 'preschool', 'kindergarten', 'grade_1', 'grade_2', 'grade_3',
            'grade_4', 'grade_5', 'grade_6', 'grade_7', 'grade_8', 'grade_9',
            'grade_10', 'grade_11', 'grade_12', 'grade_14'
        ]
    elif 'events_by_location' in data_source:
        # Location metrics
        metric_columns = [
            'total_events', 'classroom', 'bus', 'hallway_stairwell', 'cafeteria',
            'restroom', 'gymnasium', 'playground', 'other_location', 'campus_grounds'
        ]
    elif 'events_by_context' in data_source:
        # Context metrics
        metric_columns = [
            'total_events', 'school_sponsored_during', 'school_sponsored_not_during',
            'non_school_sponsored_during', 'non_school_sponsored_not_during'
        ]
    else:
        logger.warning(f"Unknown data source: {data_source}")
        return pd.DataFrame()
    
    # Filter to only existing columns
    metric_columns = [col for col in metric_columns if col in df.columns]
    
    if not metric_columns:
        logger.warning("No metric columns found for KPI conversion")
        return pd.DataFrame()
    
    # Extract year from school_year column
    df['year'] = df['school_year'].astype(str).str.extract(r'(\d{4})').astype(int)
    
    # Prepare base columns for melting
    id_columns = ['district_name', 'state_school_id', 'school_name', 'year', 'demographic', 'data_source']
    id_columns = [col for col in id_columns if col in df.columns]
    
    # Melt to long format
    kpi_df = df.melt(
        id_vars=id_columns,
        value_vars=metric_columns,
        var_name='metric_raw',
        value_name='value'
    )
    
    # Convert value column to numeric immediately after melt
    kpi_df['value'] = pd.to_numeric(kpi_df['value'], errors='coerce')
    
    # Create standardized metric names for demographic equity analysis
    metric_mapping = {
        # Event type metrics
        'total_events': 'safe_event_count_total_by_demo',
        'alcohol_events': 'safe_event_count_alcohol_by_demo',
        'assault_1st_degree': 'safe_event_count_assault_1st_by_demo',
        'drug_events': 'safe_event_count_drugs_by_demo',
        'harassment_events': 'safe_event_count_harassment_by_demo',
        'other_assault_events': 'safe_event_count_assault_other_by_demo',
        'other_state_events': 'safe_event_count_other_state_by_demo',
        'tobacco_events': 'safe_event_count_tobacco_by_demo',
        'weapon_events': 'safe_event_count_weapons_by_demo',
        
        # Grade level metrics
        'all_grades': 'safe_event_count_all_grades_by_demo',
        'preschool': 'safe_event_count_preschool_by_demo',
        'kindergarten': 'safe_event_count_kindergarten_by_demo',
        'grade_1': 'safe_event_count_grade_1_by_demo',
        'grade_2': 'safe_event_count_grade_2_by_demo',
        'grade_3': 'safe_event_count_grade_3_by_demo',
        'grade_4': 'safe_event_count_grade_4_by_demo',
        'grade_5': 'safe_event_count_grade_5_by_demo',
        'grade_6': 'safe_event_count_grade_6_by_demo',
        'grade_7': 'safe_event_count_grade_7_by_demo',
        'grade_8': 'safe_event_count_grade_8_by_demo',
        'grade_9': 'safe_event_count_grade_9_by_demo',
        'grade_10': 'safe_event_count_grade_10_by_demo',
        'grade_11': 'safe_event_count_grade_11_by_demo',
        'grade_12': 'safe_event_count_grade_12_by_demo',
        'grade_14': 'safe_event_count_grade_14_by_demo',
        
        # Location metrics
        'classroom': 'safe_event_count_classroom_by_demo',
        'bus': 'safe_event_count_bus_by_demo',
        'hallway_stairwell': 'safe_event_count_hallway_by_demo',
        'cafeteria': 'safe_event_count_cafeteria_by_demo',
        'restroom': 'safe_event_count_restroom_by_demo',
        'gymnasium': 'safe_event_count_gymnasium_by_demo',
        'playground': 'safe_event_count_playground_by_demo',
        'other_location': 'safe_event_count_other_location_by_demo',
        'campus_grounds': 'safe_event_count_campus_by_demo',
        
        # Context metrics
        'school_sponsored_during': 'safe_event_count_school_sponsored_during_by_demo',
        'school_sponsored_not_during': 'safe_event_count_school_sponsored_after_by_demo',
        'non_school_sponsored_during': 'safe_event_count_non_school_during_by_demo',
        'non_school_sponsored_not_during': 'safe_event_count_non_school_after_by_demo',
    }
    
    kpi_df['metric'] = kpi_df['metric_raw'].map(metric_mapping)
    
    # Handle suppression
    kpi_df['suppressed'] = 'N'
    kpi_df.loc[kpi_df['value'].isna(), 'suppressed'] = 'Y'
    
    # Standardize demographics using DemographicMapper
    source_file = f"safe_schools_events_{data_source}"
    kpi_df['student_group'] = kpi_df.apply(
        lambda row: demographic_mapper.map_demographic(
            row['demographic'], row['year'], source_file
        ), axis=1
    )
    
    # Create final KPI format
    final_df = pd.DataFrame({
        'district': kpi_df['district_name'] if 'district_name' in kpi_df.columns else 'Unknown',
        'school_id': kpi_df['state_school_id'] if 'state_school_id' in kpi_df.columns else kpi_df.get('nces_id', 'Unknown'),
        'school_name': kpi_df['school_name'] if 'school_name' in kpi_df.columns else 'Unknown',
        'year': kpi_df['year'],
        'student_group': kpi_df['student_group'],
        'metric': kpi_df['metric'],
        'value': kpi_df['value'],
        'suppressed': kpi_df['suppressed'],
        'source_file': source_file,
        'last_updated': datetime.now().isoformat()
    })
    
    # Filter out rows with unknown metrics
    final_df = final_df[final_df['metric'].notna()]
    
    return final_df


def _process_students_affected_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Process 'All Students' rows as students affected (scope metrics)."""
    from datetime import datetime
    
    # Identify the metric columns based on data source
    data_source = df['data_source'].iloc[0] if 'data_source' in df.columns else 'unknown'
    
    if 'events_by_type' in data_source or 'event_details' in data_source:
        # Event type metrics for students affected
        metric_columns = [
            'total_events', 'alcohol_events', 'assault_1st_degree', 'drug_events',
            'harassment_events', 'other_assault_events', 'other_state_events',
            'tobacco_events', 'weapon_events'
        ]
    elif 'events_by_grade' in data_source:
        # Grade level metrics for students affected
        metric_columns = [
            'all_grades', 'preschool', 'kindergarten', 'grade_1', 'grade_2', 'grade_3',
            'grade_4', 'grade_5', 'grade_6', 'grade_7', 'grade_8', 'grade_9',
            'grade_10', 'grade_11', 'grade_12', 'grade_14'
        ]
    elif 'events_by_location' in data_source:
        # Location metrics for students affected
        metric_columns = [
            'total_events', 'classroom', 'bus', 'hallway_stairwell', 'cafeteria',
            'restroom', 'gymnasium', 'playground', 'other_location', 'campus_grounds'
        ]
    elif 'events_by_context' in data_source:
        # Context metrics for students affected
        metric_columns = [
            'total_events', 'school_sponsored_during', 'school_sponsored_not_during',
            'non_school_sponsored_during', 'non_school_sponsored_not_during'
        ]
    else:
        logger.warning(f"Unknown data source for students affected: {data_source}")
        return pd.DataFrame()
    
    # Filter to only existing columns
    metric_columns = [col for col in metric_columns if col in df.columns]
    
    if not metric_columns:
        logger.warning("No metric columns found for students affected KPI conversion")
        return pd.DataFrame()
    
    # Extract year from school_year column
    df['year'] = df['school_year'].astype(str).str.extract(r'(\d{4})').astype(int)
    
    # Prepare base columns for melting
    id_columns = ['district_name', 'state_school_id', 'school_name', 'year', 'demographic', 'data_source']
    id_columns = [col for col in id_columns if col in df.columns]
    
    # Melt to long format
    kpi_df = df.melt(
        id_vars=id_columns,
        value_vars=metric_columns,
        var_name='metric_raw',
        value_name='value'
    )
    
    # Convert value column to numeric immediately after melt
    kpi_df['value'] = pd.to_numeric(kpi_df['value'], errors='coerce')
    
    # Create standardized metric names for students affected (scope metrics)
    metric_mapping = {
        # Event type metrics - students affected
        'total_events': 'safe_students_affected_total',
        'alcohol_events': 'safe_students_affected_alcohol',
        'assault_1st_degree': 'safe_students_affected_assault_1st',
        'drug_events': 'safe_students_affected_drugs',
        'harassment_events': 'safe_students_affected_harassment',
        'other_assault_events': 'safe_students_affected_assault_other',
        'other_state_events': 'safe_students_affected_other_state',
        'tobacco_events': 'safe_students_affected_tobacco',
        'weapon_events': 'safe_students_affected_weapons',
        
        # Grade level metrics - students affected
        'all_grades': 'safe_students_affected_all_grades',
        'preschool': 'safe_students_affected_preschool',
        'kindergarten': 'safe_students_affected_kindergarten',
        'grade_1': 'safe_students_affected_grade_1',
        'grade_2': 'safe_students_affected_grade_2',
        'grade_3': 'safe_students_affected_grade_3',
        'grade_4': 'safe_students_affected_grade_4',
        'grade_5': 'safe_students_affected_grade_5',
        'grade_6': 'safe_students_affected_grade_6',
        'grade_7': 'safe_students_affected_grade_7',
        'grade_8': 'safe_students_affected_grade_8',
        'grade_9': 'safe_students_affected_grade_9',
        'grade_10': 'safe_students_affected_grade_10',
        'grade_11': 'safe_students_affected_grade_11',
        'grade_12': 'safe_students_affected_grade_12',
        'grade_14': 'safe_students_affected_grade_14',
        
        # Location metrics - students affected
        'classroom': 'safe_students_affected_classroom',
        'bus': 'safe_students_affected_bus',
        'hallway_stairwell': 'safe_students_affected_hallway',
        'cafeteria': 'safe_students_affected_cafeteria',
        'restroom': 'safe_students_affected_restroom',
        'gymnasium': 'safe_students_affected_gymnasium',
        'playground': 'safe_students_affected_playground',
        'other_location': 'safe_students_affected_other_location',
        'campus_grounds': 'safe_students_affected_campus',
        
        # Context metrics - students affected
        'school_sponsored_during': 'safe_students_affected_school_sponsored_during',
        'school_sponsored_not_during': 'safe_students_affected_school_sponsored_after',
        'non_school_sponsored_during': 'safe_students_affected_non_school_during',
        'non_school_sponsored_not_during': 'safe_students_affected_non_school_after',
    }
    
    kpi_df['metric'] = kpi_df['metric_raw'].map(metric_mapping)
    
    # Handle suppression
    kpi_df['suppressed'] = 'N'
    kpi_df.loc[kpi_df['value'].isna(), 'suppressed'] = 'Y'
    
    # For students affected rows, use "All Students" as the student group
    kpi_df['student_group'] = 'All Students'
    
    # Create final KPI format
    source_file = f"safe_schools_events_{data_source}_students_affected"
    final_df = pd.DataFrame({
        'district': kpi_df['district_name'] if 'district_name' in kpi_df.columns else 'Unknown',
        'school_id': kpi_df['state_school_id'] if 'state_school_id' in kpi_df.columns else kpi_df.get('nces_id', 'Unknown'),
        'school_name': kpi_df['school_name'] if 'school_name' in kpi_df.columns else 'Unknown',
        'year': kpi_df['year'],
        'student_group': kpi_df['student_group'],
        'metric': kpi_df['metric'],
        'value': kpi_df['value'],
        'suppressed': kpi_df['suppressed'],
        'source_file': source_file,
        'last_updated': datetime.now().isoformat()
    })
    
    # Filter out rows with unknown metrics
    final_df = final_df[final_df['metric'].notna()]
    
    return final_df


def _process_total_events_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Process 'Total Events' rows as intensity metrics."""
    from datetime import datetime
    
    # Identify the metric columns based on data source
    data_source = df['data_source'].iloc[0] if 'data_source' in df.columns else 'unknown'
    
    if 'events_by_type' in data_source or 'event_details' in data_source:
        # Event type metrics for total events
        metric_columns = [
            'total_events', 'alcohol_events', 'assault_1st_degree', 'drug_events',
            'harassment_events', 'other_assault_events', 'other_state_events',
            'tobacco_events', 'weapon_events'
        ]
    elif 'events_by_grade' in data_source:
        # Grade level metrics for total events
        metric_columns = [
            'all_grades', 'preschool', 'kindergarten', 'grade_1', 'grade_2', 'grade_3',
            'grade_4', 'grade_5', 'grade_6', 'grade_7', 'grade_8', 'grade_9',
            'grade_10', 'grade_11', 'grade_12', 'grade_14'
        ]
    elif 'events_by_location' in data_source:
        # Location metrics for total events
        metric_columns = [
            'total_events', 'classroom', 'bus', 'hallway_stairwell', 'cafeteria',
            'restroom', 'gymnasium', 'playground', 'other_location', 'campus_grounds'
        ]
    elif 'events_by_context' in data_source:
        # Context metrics for total events
        metric_columns = [
            'total_events', 'school_sponsored_during', 'school_sponsored_not_during',
            'non_school_sponsored_during', 'non_school_sponsored_not_during'
        ]
    else:
        logger.warning(f"Unknown data source for total events: {data_source}")
        return pd.DataFrame()
    
    # Filter to only existing columns
    metric_columns = [col for col in metric_columns if col in df.columns]
    
    if not metric_columns:
        logger.warning("No metric columns found for total events KPI conversion")
        return pd.DataFrame()
    
    # Extract year from school_year column
    df['year'] = df['school_year'].astype(str).str.extract(r'(\d{4})').astype(int)
    
    # Prepare base columns for melting
    id_columns = ['district_name', 'state_school_id', 'school_name', 'year', 'demographic', 'data_source']
    id_columns = [col for col in id_columns if col in df.columns]
    
    # Melt to long format
    kpi_df = df.melt(
        id_vars=id_columns,
        value_vars=metric_columns,
        var_name='metric_raw',
        value_name='value'
    )
    
    # Convert value column to numeric immediately after melt
    kpi_df['value'] = pd.to_numeric(kpi_df['value'], errors='coerce')
    
    # Create standardized metric names for total events (intensity metrics)
    metric_mapping = {
        # Event type metrics - total events
        'total_events': 'safe_event_count_total',
        'alcohol_events': 'safe_event_count_alcohol',
        'assault_1st_degree': 'safe_event_count_assault_1st',
        'drug_events': 'safe_event_count_drugs',
        'harassment_events': 'safe_event_count_harassment',
        'other_assault_events': 'safe_event_count_assault_other',
        'other_state_events': 'safe_event_count_other_state',
        'tobacco_events': 'safe_event_count_tobacco',
        'weapon_events': 'safe_event_count_weapons',
        
        # Grade level metrics - total events
        'all_grades': 'safe_event_count_all_grades',
        'preschool': 'safe_event_count_preschool',
        'kindergarten': 'safe_event_count_kindergarten',
        'grade_1': 'safe_event_count_grade_1',
        'grade_2': 'safe_event_count_grade_2',
        'grade_3': 'safe_event_count_grade_3',
        'grade_4': 'safe_event_count_grade_4',
        'grade_5': 'safe_event_count_grade_5',
        'grade_6': 'safe_event_count_grade_6',
        'grade_7': 'safe_event_count_grade_7',
        'grade_8': 'safe_event_count_grade_8',
        'grade_9': 'safe_event_count_grade_9',
        'grade_10': 'safe_event_count_grade_10',
        'grade_11': 'safe_event_count_grade_11',
        'grade_12': 'safe_event_count_grade_12',
        'grade_14': 'safe_event_count_grade_14',
        
        # Location metrics - total events
        'classroom': 'safe_event_count_classroom',
        'bus': 'safe_event_count_bus',
        'hallway_stairwell': 'safe_event_count_hallway',
        'cafeteria': 'safe_event_count_cafeteria',
        'restroom': 'safe_event_count_restroom',
        'gymnasium': 'safe_event_count_gymnasium',
        'playground': 'safe_event_count_playground',
        'other_location': 'safe_event_count_other_location',
        'campus_grounds': 'safe_event_count_campus',
        
        # Context metrics - total events
        'school_sponsored_during': 'safe_event_count_school_sponsored_during',
        'school_sponsored_not_during': 'safe_event_count_school_sponsored_after',
        'non_school_sponsored_during': 'safe_event_count_non_school_during',
        'non_school_sponsored_not_during': 'safe_event_count_non_school_after',
    }
    
    kpi_df['metric'] = kpi_df['metric_raw'].map(metric_mapping)
    
    # Handle suppression
    kpi_df['suppressed'] = 'N'
    kpi_df.loc[kpi_df['value'].isna(), 'suppressed'] = 'Y'
    
    # For total events rows, use "All Students - Total Events" as the student group
    kpi_df['student_group'] = 'All Students - Total Events'
    
    # Create final KPI format
    source_file = f"safe_schools_events_{data_source}_total_events"
    final_df = pd.DataFrame({
        'district': kpi_df['district_name'] if 'district_name' in kpi_df.columns else 'Unknown',
        'school_id': kpi_df['state_school_id'] if 'state_school_id' in kpi_df.columns else kpi_df.get('nces_id', 'Unknown'),
        'school_name': kpi_df['school_name'] if 'school_name' in kpi_df.columns else 'Unknown',
        'year': kpi_df['year'],
        'student_group': kpi_df['student_group'],
        'metric': kpi_df['metric'],
        'value': kpi_df['value'],
        'suppressed': kpi_df['suppressed'],
        'source_file': source_file,
        'last_updated': datetime.now().isoformat()
    })
    
    # Filter out rows with unknown metrics
    final_df = final_df[final_df['metric'].notna()]
    
    return final_df


def _calculate_derived_rates(kpi_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate Tier 4 derived rates (events per affected student)."""
    from datetime import datetime
    
    if kpi_df.empty:
        logger.warning("Empty KPI dataframe for derived rates calculation")
        return pd.DataFrame()
    
    derived_rates = []
    
    # Debug: Check what metrics are available
    available_students_affected = [m for m in kpi_df['metric'].unique() if m.startswith('safe_students_affected_')]
    available_total_events = [m for m in kpi_df['metric'].unique() if m.startswith('safe_event_count_') and not '_by_demo' in m and not 'students_affected' in m]
    logger.info(f"Available students affected metrics: {len(available_students_affected)}")
    logger.info(f"Available total events metrics: {len(available_total_events)}")
    
    # Group by school, year, and metric type to calculate rates
    school_groups = list(kpi_df.groupby(['district', 'school_id', 'school_name', 'year']))
    logger.info(f"Processing {len(school_groups)} school-year combinations for derived rates")
    
    for (district, school_id, school_name, year), group in school_groups:
        
        # Get available metric types (extract base metric names)
        available_metrics = set()
        for metric in group['metric'].unique():
            if metric.startswith('safe_students_affected_'):
                base_metric = metric.replace('safe_students_affected_', '')
                available_metrics.add(base_metric)
        
        # Calculate rates for each available metric type
        for base_metric in available_metrics:
            students_affected_metric = f'safe_students_affected_{base_metric}'
            total_events_metric = f'safe_event_count_{base_metric}'
            
            # Get students affected value (Tier 1)
            students_affected_row = group[
                (group['metric'] == students_affected_metric) & 
                (group['student_group'] == 'All Students')
            ]
            
            # Get total events value (Tier 3)
            total_events_row = group[
                (group['metric'] == total_events_metric) & 
                (group['student_group'] == 'All Students - Total Events')
            ]
            
            if len(students_affected_row) == 1 and len(total_events_row) == 1:
                students_affected = students_affected_row['value'].iloc[0]
                total_events = total_events_row['value'].iloc[0]
                
                # Only calculate rate if both values are valid and numeric
                try:
                    students_affected_num = float(students_affected) if pd.notna(students_affected) else 0
                    total_events_num = float(total_events) if pd.notna(total_events) else 0
                except (ValueError, TypeError):
                    # Skip if values can't be converted (suppressed data, etc.)
                    continue
                
                if students_affected_num > 0 and total_events_num >= 0:
                    incident_rate = total_events_num / students_affected_num
                    
                    # Create derived rate record
                    rate_record = {
                        'district': district,
                        'school_id': school_id,
                        'school_name': school_name,
                        'year': year,
                        'student_group': 'All Students',  # Rates apply to all students collectively
                        'metric': f'safe_incident_rate_{base_metric}',
                        'value': round(incident_rate, 3),  # Round to 3 decimal places
                        'suppressed': 'N',  # Derived rates are not suppressed if source data isn't
                        'source_file': f'derived_rates_{base_metric}',
                        'last_updated': datetime.now().isoformat()
                    }
                    
                    derived_rates.append(rate_record)
    
    if derived_rates:
        return pd.DataFrame(derived_rates)
    else:
        return pd.DataFrame()


def _process_aggregate_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Process aggregate rows (Total Events) as separate KPIs."""
    from datetime import datetime
    
    # Identify the metric columns based on data source
    data_source = df['data_source'].iloc[0] if 'data_source' in df.columns else 'unknown'
    
    if 'events_by_type' in data_source or 'event_details' in data_source:
        # Event type metrics for aggregates
        metric_columns = [
            'total_events', 'alcohol_events', 'assault_1st_degree', 'drug_events',
            'harassment_events', 'other_assault_events', 'other_state_events',
            'tobacco_events', 'weapon_events'
        ]
    elif 'events_by_grade' in data_source:
        # Grade level metrics for aggregates
        metric_columns = [
            'all_grades', 'preschool', 'kindergarten', 'grade_1', 'grade_2', 'grade_3',
            'grade_4', 'grade_5', 'grade_6', 'grade_7', 'grade_8', 'grade_9',
            'grade_10', 'grade_11', 'grade_12', 'grade_14'
        ]
    elif 'events_by_location' in data_source:
        # Location metrics for aggregates
        metric_columns = [
            'total_events', 'classroom', 'bus', 'hallway_stairwell', 'cafeteria',
            'restroom', 'gymnasium', 'playground', 'other_location', 'campus_grounds'
        ]
    elif 'events_by_context' in data_source:
        # Context metrics for aggregates
        metric_columns = [
            'total_events', 'school_sponsored_during', 'school_sponsored_not_during',
            'non_school_sponsored_during', 'non_school_sponsored_not_during'
        ]
    else:
        logger.warning(f"Unknown data source for aggregates: {data_source}")
        return pd.DataFrame()
    
    # Filter to only existing columns
    metric_columns = [col for col in metric_columns if col in df.columns]
    
    if not metric_columns:
        logger.warning("No metric columns found for aggregate KPI conversion")
        return pd.DataFrame()
    
    # Extract year from school_year column
    df['year'] = df['school_year'].astype(str).str.extract(r'(\d{4})').astype(int)
    
    # Prepare base columns for melting
    id_columns = ['district_name', 'state_school_id', 'school_name', 'year', 'demographic', 'data_source']
    id_columns = [col for col in id_columns if col in df.columns]
    
    # Melt to long format
    kpi_df = df.melt(
        id_vars=id_columns,
        value_vars=metric_columns,
        var_name='metric_raw',
        value_name='value'
    )
    
    # Convert value column to numeric immediately after melt
    kpi_df['value'] = pd.to_numeric(kpi_df['value'], errors='coerce')
    
    # Create standardized metric names for aggregates (no demographic suffix)
    metric_mapping = {
        # Event type metrics - aggregates
        'total_events': 'safe_event_count_total_aggregate',
        'alcohol_events': 'safe_event_count_alcohol_aggregate',
        'assault_1st_degree': 'safe_event_count_assault_1st_aggregate',
        'drug_events': 'safe_event_count_drugs_aggregate',
        'harassment_events': 'safe_event_count_harassment_aggregate',
        'other_assault_events': 'safe_event_count_assault_other_aggregate',
        'other_state_events': 'safe_event_count_other_state_aggregate',
        'tobacco_events': 'safe_event_count_tobacco_aggregate',
        'weapon_events': 'safe_event_count_weapons_aggregate',
        
        # Grade level metrics - aggregates
        'all_grades': 'safe_event_count_all_grades_aggregate',
        'preschool': 'safe_event_count_preschool_aggregate',
        'kindergarten': 'safe_event_count_kindergarten_aggregate',
        'grade_1': 'safe_event_count_grade_1_aggregate',
        'grade_2': 'safe_event_count_grade_2_aggregate',
        'grade_3': 'safe_event_count_grade_3_aggregate',
        'grade_4': 'safe_event_count_grade_4_aggregate',
        'grade_5': 'safe_event_count_grade_5_aggregate',
        'grade_6': 'safe_event_count_grade_6_aggregate',
        'grade_7': 'safe_event_count_grade_7_aggregate',
        'grade_8': 'safe_event_count_grade_8_aggregate',
        'grade_9': 'safe_event_count_grade_9_aggregate',
        'grade_10': 'safe_event_count_grade_10_aggregate',
        'grade_11': 'safe_event_count_grade_11_aggregate',
        'grade_12': 'safe_event_count_grade_12_aggregate',
        'grade_14': 'safe_event_count_grade_14_aggregate',
        
        # Location metrics - aggregates
        'classroom': 'safe_event_count_classroom_aggregate',
        'bus': 'safe_event_count_bus_aggregate',
        'hallway_stairwell': 'safe_event_count_hallway_aggregate',
        'cafeteria': 'safe_event_count_cafeteria_aggregate',
        'restroom': 'safe_event_count_restroom_aggregate',
        'gymnasium': 'safe_event_count_gymnasium_aggregate',
        'playground': 'safe_event_count_playground_aggregate',
        'other_location': 'safe_event_count_other_location_aggregate',
        'campus_grounds': 'safe_event_count_campus_aggregate',
        
        # Context metrics - aggregates
        'school_sponsored_during': 'safe_event_count_school_sponsored_during_aggregate',
        'school_sponsored_not_during': 'safe_event_count_school_sponsored_after_aggregate',
        'non_school_sponsored_during': 'safe_event_count_non_school_during_aggregate',
        'non_school_sponsored_not_during': 'safe_event_count_non_school_after_aggregate',
    }
    
    kpi_df['metric'] = kpi_df['metric_raw'].map(metric_mapping)
    
    # Handle suppression
    kpi_df['suppressed'] = 'N'
    kpi_df.loc[kpi_df['value'].isna(), 'suppressed'] = 'Y'
    
    # For aggregate rows, use the demographic value as the student_group (e.g., "Total Events")
    kpi_df['student_group'] = 'All Students - Aggregate'
    
    # Create final KPI format
    source_file = f"safe_schools_events_{data_source}_aggregate"
    final_df = pd.DataFrame({
        'district': kpi_df['district_name'] if 'district_name' in kpi_df.columns else 'Unknown',
        'school_id': kpi_df['state_school_id'] if 'state_school_id' in kpi_df.columns else kpi_df.get('nces_id', 'Unknown'),
        'school_name': kpi_df['school_name'] if 'school_name' in kpi_df.columns else 'Unknown',
        'year': kpi_df['year'],
        'student_group': kpi_df['student_group'],
        'metric': kpi_df['metric'],
        'value': kpi_df['value'],
        'suppressed': kpi_df['suppressed'],
        'source_file': source_file,
        'last_updated': datetime.now().isoformat()
    })
    
    # Filter out rows with unknown metrics
    final_df = final_df[final_df['metric'].notna()]
    
    return final_df


def transform(raw_dir: str, proc_dir: str, config: Dict[str, Any]) -> str:
    """Transform safe schools events data from raw to processed format."""
    raw_path = Path(raw_dir)
    proc_path = Path(proc_dir)
    
    # Create output directory
    proc_path.mkdir(parents=True, exist_ok=True)
    
    # Initialize demographic mapper for audit logging
    demographic_mapper = DemographicMapper()
    
    all_data = []
    
    # Process KYRC24 files
    kyrc24_files = [
        'KYRC24_SAFE_Behavior_Events_by_Type.csv',
        'KYRC24_SAFE_Behavior_Events_by_Grade_Level.csv',
        'KYRC24_SAFE_Behavior_Events_by_Location.csv',
        'KYRC24_SAFE_Behavior_Events_by_Context.csv'
    ]
    
    for filename in kyrc24_files:
        file_path = raw_path / filename
        if file_path.exists():
            try:
                logger.info(f"Processing KYRC24 file: {filename}")
                df = pd.read_csv(file_path, encoding='utf-8-sig')
                
                # Apply transformations
                df = normalize_column_names(df)
                df = add_derived_fields(df, config.get('derive', {}))
                df = clean_event_data(df)
                
                # Convert to KPI format
                kpi_df = convert_to_kpi_format(df, demographic_mapper)
                
                if not kpi_df.empty:
                    all_data.append(kpi_df)
                    logger.info(f"Processed {len(kpi_df)} KPI records from {filename}")
                else:
                    logger.warning(f"No data produced from {filename}")
                    
            except Exception as e:
                logger.error(f"Error processing {filename}: {str(e)}")
    
    # Process historical files
    historical_files = [
        ('safe_schools_event_details_2020.csv', 'historical_event_details'),
        ('safe_schools_event_details_2021.csv', 'historical_event_details'),
        ('safe_schools_event_details_2022.csv', 'historical_event_details'),
        ('safe_schools_event_details_2023.csv', 'historical_event_details'),
        ('safe_schools_events_by_grade_2020.csv', 'historical_events_by_grade'),
        ('safe_schools_events_by_grade_2021.csv', 'historical_events_by_grade'),
        ('safe_schools_events_by_grade_2022.csv', 'historical_events_by_grade'),
        ('safe_schools_events_by_grade_2023.csv', 'historical_events_by_grade')
    ]
    
    for filename, expected_source in historical_files:
        file_path = raw_path / filename
        if file_path.exists():
            try:
                logger.info(f"Processing historical file: {filename}")
                df = pd.read_csv(file_path, encoding='utf-8-sig')
                
                # Apply transformations
                df = normalize_column_names(df)
                df = add_derived_fields(df, config.get('derive', {}))
                df = clean_event_data(df)
                
                # Convert to KPI format
                kpi_df = convert_to_kpi_format(df, demographic_mapper)
                
                if not kpi_df.empty:
                    all_data.append(kpi_df)
                    logger.info(f"Processed {len(kpi_df)} KPI records from {filename}")
                else:
                    logger.warning(f"No data produced from {filename}")
                    
            except Exception as e:
                logger.error(f"Error processing {filename}: {str(e)}")
    
    # Combine all data
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # FIX: Ensure value column is numeric for etl_runner validation
        # Convert value column to numeric, this will properly handle suppressed records as NaN
        combined_df['value'] = pd.to_numeric(combined_df['value'], errors='coerce')
        
        # Save main output
        output_file = proc_path / 'safe_schools_events.csv'
        combined_df.to_csv(output_file, index=False)
        
        # Save demographic audit
        audit_file = proc_path / 'safe_schools_events_demographic_audit.csv'
        demographic_mapper.save_audit_log(audit_file)
        
        logger.info(f"Saved {len(combined_df)} total KPI records to {output_file}")
        logger.info(f"Saved demographic audit log to {audit_file}")
        
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
    config = {
        "derive": {
            "data_version": "2024"
        }
    }
    
    raw_dir = "data/raw/safe_schools"
    proc_dir = "data/processed"
    
    result = transform(raw_dir, proc_dir, config)
    if result:
        print(f"Successfully processed safe schools events data: {result}")
    else:
        print("Failed to process safe schools events data")