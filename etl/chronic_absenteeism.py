"""
Chronic Absenteeism ETL Module

Handles chronic absenteeism data from Kentucky across years 2023-2024.
Processes chronic absenteeism rates, counts, and enrollment data with grade-level
and demographic breakdowns following standardized KPI format.
"""
from pathlib import Path
import pandas as pd
from pydantic import BaseModel
from typing import Dict, Any, Optional, Union
import logging
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
        'School Year': 'school_year',
        'SCHOOL YEAR': 'school_year',
        'County Number': 'county_number',
        'COUNTY NUMBER': 'county_number',
        'County Name': 'county_name',
        'COUNTY NAME': 'county_name',
        'District Number': 'district_number',
        'DISTRICT NUMBER': 'district_number',
        'District Name': 'district_name',
        'DISTRICT NAME': 'district_name',
        'School Number': 'school_number',
        'SCHOOL NUMBER': 'school_number',
        'School Name': 'school_name',
        'SCHOOL NAME': 'school_name',
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
        'Grade': 'grade',
        'GRADE': 'grade',
        'Demographic': 'demographic',
        'DEMOGRAPHIC': 'demographic',
        'Suppressed': 'suppressed',
        'SUPPRESSED': 'suppressed',
        'Chronically Absent Students': 'chronically_absent_count',
        'CHRONIC ABSENTEE COUNT': 'chronically_absent_count',
        'Students Enrolled 10 or More Days': 'enrollment_count',
        'ENROLLMENT COUNT OF STUDENTS WITH 10+ ENROLLED DAYS': 'enrollment_count',
        'Chronic Absenteeism Rate': 'chronic_absenteeism_rate',
        'PERCENT CHRONICALLY ABSENT': 'chronic_absenteeism_rate',
    }
    
    # Apply mapping for columns that exist
    rename_dict = {col: column_mapping[col] for col in df.columns if col in column_mapping}
    return df.rename(columns=rename_dict)


def standardize_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Convert empty strings and suppression markers to NaN."""
    # Replace empty strings with NaN
    df = df.replace('', pd.NA)
    df = df.replace('""', pd.NA)
    
    # Replace suppression markers with NaN but preserve in separate columns
    numeric_columns = ['chronically_absent_count', 'enrollment_count', 'chronic_absenteeism_rate']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = df[col].replace('*', pd.NA)
    
    return df


def clean_numeric_values(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and convert numeric values, handling commas and invalid data."""
    numeric_columns = ['chronically_absent_count', 'enrollment_count', 'chronic_absenteeism_rate']
    
    for col in numeric_columns:
        if col in df.columns:
            # Remove commas from numbers
            df[col] = df[col].astype(str).str.replace(',', '').replace('nan', pd.NA)
            
            # Convert to numeric
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Validate ranges
            if col == 'chronic_absenteeism_rate':
                # Rates should be between 0 and 100
                invalid_mask = (df[col] < 0) | (df[col] > 100)
                if invalid_mask.any():
                    logger.warning(f"Found {invalid_mask.sum()} invalid chronic absenteeism rates in {col}")
                    df.loc[invalid_mask, col] = pd.NA
            elif col in ['chronically_absent_count', 'enrollment_count']:
                # Counts should be non-negative
                invalid_mask = df[col] < 0
                if invalid_mask.any():
                    logger.warning(f"Found {invalid_mask.sum()} negative counts in {col}")
                    df.loc[invalid_mask, col] = pd.NA
    
    return df


def add_derived_fields(df: pd.DataFrame, derive_config: Dict[str, Any]) -> pd.DataFrame:
    """Add derived fields based on configuration."""
    for field, value in derive_config.items():
        df[field] = value
    
    # Add data_source field based on year and format
    if 'school_year' in df.columns:
        # Extract year for source identification
        df['data_source'] = df['school_year'].apply(lambda x: f'chronic_absenteeism_{str(x)[-4:]}')
    else:
        df['data_source'] = 'chronic_absenteeism_unknown'
    
    return df


def standardize_suppression_field(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize suppression indicators to Y/N format."""
    if 'suppressed' in df.columns:
        # Map various suppression indicators to standard Y/N
        suppression_mapping = {
            'Yes': 'Y',
            'No': 'N',
            'Y': 'Y',
            'N': 'N',
            True: 'Y',
            False: 'N',
            1: 'Y',
            0: 'N'
        }
        
        df['suppressed'] = df['suppressed'].map(suppression_mapping).fillna('N')
    else:
        # If no suppression column, infer from data
        df['suppressed'] = 'N'
        
        # Mark as suppressed if chronic_absenteeism_rate is missing but other data exists
        if 'chronic_absenteeism_rate' in df.columns and 'demographic' in df.columns:
            missing_rate = df['chronic_absenteeism_rate'].isna()
            has_demographic = df['demographic'].notna()
            df.loc[missing_rate & has_demographic, 'suppressed'] = 'Y'
    
    return df


def normalize_grade_field(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize grade field values for consistent reporting."""
    if 'grade' not in df.columns:
        return df
    
    grade_mapping = {
        'All Grades': 'all_grades',
        'ALL GRADES': 'all_grades',
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
        'Kindergarten': 'kindergarten',
        'Pre-K': 'pre_k',
        'Preschool': 'preschool'
    }
    
    df['grade'] = df['grade'].map(grade_mapping).fillna(df['grade'].str.lower().str.replace(' ', '_'))
    
    return df


def convert_to_kpi_format(df: pd.DataFrame, demographic_mapper: Optional[DemographicMapper] = None) -> pd.DataFrame:
    """Convert wide format chronic absenteeism data to long KPI format with standardized demographics."""
    from datetime import datetime
    
    # Initialize demographic mapper if not provided
    if demographic_mapper is None:
        demographic_mapper = DemographicMapper()
    
    # Skip if no chronic absenteeism data
    if 'chronic_absenteeism_rate' not in df.columns and 'chronically_absent_count' not in df.columns:
        logger.warning("No chronic absenteeism data columns found for KPI conversion")
        return pd.DataFrame()
    
    kpi_rows = []
    
    for _, row in df.iterrows():
        # Extract school identification
        school_id = row.get('state_school_id', row.get('nces_id', row.get('school_code', '')))
        if pd.isna(school_id) or school_id == '':
            school_id = row.get('school_code', 'unknown')
        
        # Convert school_id to string without .0 suffix
        if pd.notna(school_id) and school_id != '':
            try:
                # If it's a numeric value, convert to int then string to remove .0
                school_id = str(int(float(school_id)))
            except (ValueError, TypeError):
                # If conversion fails, use as string
                school_id = str(school_id)
        
        # Extract year from school_year (e.g., "20232024" -> "2024")
        year = row.get('school_year', '')
        if len(str(year)) == 8:  # Format: YYYYYYYY
            year = str(year)[-4:]  # Take last 4 digits
        elif len(str(year)) == 4:  # Already 4 digits
            year = str(year)
        else:
            year = '2024'  # Default
        
        # Map demographic to standard student group names using demographic mapper
        original_demographic = row.get('demographic', 'All Students')
        source_file = row.get('source_file', 'chronic_absenteeism.csv')
        student_group = demographic_mapper.map_demographic(original_demographic, year, source_file)
        
        # Get grade for metric naming
        grade = row.get('grade', 'all_grades')
        if pd.isna(grade) or grade == '':
            grade = 'all_grades'
        
        # Check if this record is suppressed
        is_suppressed = row.get('suppressed', 'N') == 'Y'
        
        # Common KPI row template
        kpi_template = {
            'district': row.get('district_name', 'Fayette County'),
            'school_id': school_id,
            'school_name': row.get('school_name', 'Unknown School'),
            'year': year,
            'student_group': student_group,
            'source_file': row.get('source_file', 'chronic_absenteeism.csv'),
            'last_updated': datetime.now().isoformat(),
            'suppressed': 'Y' if is_suppressed else 'N'
        }
        
        # Create metrics based on available data
        metrics_created = []
        
        # Chronic absenteeism rate metric
        if 'chronic_absenteeism_rate' in df.columns:
            rate_kpi = kpi_template.copy()
            rate_kpi.update({
                'metric': f'chronic_absenteeism_rate_{grade}',
            })
            
            if is_suppressed:
                rate_kpi['value'] = pd.NA
            else:
                rate_value = row.get('chronic_absenteeism_rate')
                if pd.notna(rate_value):
                    rate_kpi['value'] = float(rate_value)
                    metrics_created.append(rate_kpi)
                else:
                    continue
            
            if is_suppressed or pd.notna(rate_value):
                kpi_rows.append(rate_kpi)
        
        # Chronic absenteeism count metric
        if 'chronically_absent_count' in df.columns:
            count_kpi = kpi_template.copy()
            count_kpi.update({
                'metric': f'chronic_absenteeism_count_{grade}',
            })
            
            if is_suppressed:
                count_kpi['value'] = pd.NA
            else:
                count_value = row.get('chronically_absent_count')
                if pd.notna(count_value):
                    count_kpi['value'] = int(float(count_value))
                else:
                    continue
            
            if is_suppressed or pd.notna(count_value):
                kpi_rows.append(count_kpi)
        
        # Enrollment count metric
        if 'enrollment_count' in df.columns:
            enrollment_kpi = kpi_template.copy()
            enrollment_kpi.update({
                'metric': f'chronic_absenteeism_enrollment_{grade}',
            })
            
            if is_suppressed:
                enrollment_kpi['value'] = pd.NA
            else:
                enrollment_value = row.get('enrollment_count')
                if pd.notna(enrollment_value):
                    enrollment_kpi['value'] = int(float(enrollment_value))
                else:
                    continue
            
            if is_suppressed or pd.notna(enrollment_value):
                kpi_rows.append(enrollment_kpi)
    
    if not kpi_rows:
        logger.warning("No valid KPI rows created")
        return pd.DataFrame()
    
    # Create KPI dataframe
    kpi_df = pd.DataFrame(kpi_rows)
    
    # Filter out grade-level metrics that are completely suppressed
    # Check each metric to see if it has any non-suppressed data
    if not kpi_df.empty:
        metrics_to_keep = []
        
        # Group by metric to analyze suppression
        for metric_name, metric_group in kpi_df.groupby('metric'):
            # Check if any records in this metric are non-suppressed
            has_data = (metric_group['suppressed'] == 'N').any()
            
            # Keep all_grades metrics regardless (these have data)
            # Only keep grade-level metrics if they have some non-suppressed data
            if 'all_grades' in metric_name or has_data:
                metrics_to_keep.append(metric_name)
        
        # Get count before filtering
        total_metrics_before = len(kpi_df['metric'].unique())
        
        # Filter dataframe to only include metrics with actual data
        kpi_df = kpi_df[kpi_df['metric'].isin(metrics_to_keep)]
        
        excluded_metrics = total_metrics_before - len(metrics_to_keep)
        logger.info(f"Filtered metrics: kept {len(metrics_to_keep)} metrics with data, "
                   f"excluded {excluded_metrics} completely suppressed grade-level metrics")
    
    # Ensure consistent column order
    kpi_columns = ['district', 'school_id', 'school_name', 'year', 'student_group', 
                   'metric', 'value', 'suppressed', 'source_file', 'last_updated']
    
    # Only include columns that exist
    available_columns = [col for col in kpi_columns if col in kpi_df.columns]
    kpi_df = kpi_df[available_columns]
    
    return kpi_df


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Read chronic absenteeism files, normalize, and convert to KPI format with demographic standardization."""
    source_name = Path(__file__).stem
    source_dir = raw_dir / source_name
    
    if not source_dir.exists():
        logger.info(f"No raw data directory for {source_name}; skipping.")
        return
    
    # Find all CSV files in the source directory
    csv_files = list(source_dir.glob("*.csv"))
    if not csv_files:
        logger.info(f"No CSV files found in {source_dir}; skipping.")
        return
    
    logger.info(f"Found {len(csv_files)} chronic absenteeism files to process")
    
    # Initialize demographic mapper
    demographic_mapper = DemographicMapper()
    
    all_kpi_dataframes = []
    conf = Config(**cfg)
    
    for csv_file in csv_files:
        logger.info(f"Processing {csv_file.name}")
        
        try:
            # Read CSV file
            df = pd.read_csv(csv_file)
            
            # Skip if empty
            if df.empty:
                logger.warning(f"Empty file: {csv_file.name}")
                continue
            
            # Apply transformations
            df = normalize_column_names(df)
            df = standardize_missing_values(df)
            df = clean_numeric_values(df)
            df = standardize_suppression_field(df)
            df = normalize_grade_field(df)
            df = add_derived_fields(df, conf.derive)
            
            # Apply configuration-based transformations
            if conf.rename:
                df = df.rename(columns=conf.rename)
            
            # Add file source for tracking
            df['source_file'] = csv_file.name
            
            # Convert to KPI format with demographic mapping
            kpi_df = convert_to_kpi_format(df, demographic_mapper)
            
            if not kpi_df.empty:
                all_kpi_dataframes.append(kpi_df)
                logger.info(f"Processed {len(df)} rows from {csv_file.name}, created {len(kpi_df)} KPI rows")
            else:
                logger.warning(f"No KPI data created from {csv_file.name}")
            
        except Exception as e:
            logger.error(f"Error processing {csv_file.name}: {e}")
            continue
    
    if not all_kpi_dataframes:
        logger.warning("No valid KPI data files processed")
        return
    
    # Combine all KPI dataframes
    combined_kpi_df = pd.concat(all_kpi_dataframes, ignore_index=True, sort=False)
    
    # Validate demographic coverage
    unique_demographics = combined_kpi_df['student_group'].unique().tolist()
    years_processed = combined_kpi_df['year'].unique().tolist()
    
    # Validate demographics for each year
    for year in years_processed:
        year_demographics = combined_kpi_df[combined_kpi_df['year'] == year]['student_group'].unique().tolist()
        validation_result = demographic_mapper.validate_demographics(year_demographics, year)
        
        if validation_result['missing_required']:
            logger.warning(f"Missing required demographics for {year}: {validation_result['missing_required']}")
        if validation_result['unexpected']:
            logger.warning(f"Unexpected demographics for {year}: {validation_result['unexpected']}")
        
        logger.info(f"Year {year}: {len(validation_result['valid'])} valid demographics, "
                   f"{len(validation_result['missing_optional'])} optional missing")
    
    # Save demographic mapping audit log
    audit_path = proc_dir / f"{source_name}_demographic_audit.csv"
    demographic_mapper.save_audit_log(audit_path)
    
    # Write processed KPI data
    output_path = proc_dir / f"{source_name}.csv"
    combined_kpi_df.to_csv(output_path, index=False)
    
    logger.info(f"Combined chronic absenteeism KPI data written to {output_path}")
    logger.info(f"Demographic audit log written to {audit_path}")
    logger.info(f"Total KPI rows: {len(combined_kpi_df)}, Total columns: {len(combined_kpi_df.columns)}")
    logger.info(f"Unique demographics standardized: {len(unique_demographics)}")
    
    # Log summary statistics
    if 'value' in combined_kpi_df.columns:
        valid_values = combined_kpi_df['value'].dropna()
        if len(valid_values) > 0:
            logger.info(f"KPI value range: {valid_values.min():.1f} - {valid_values.max():.1f}")
    
    # Log metric distribution
    if 'metric' in combined_kpi_df.columns:
        metric_counts = combined_kpi_df['metric'].value_counts()
        logger.info(f"Metrics created: {dict(metric_counts)}")
    
    # Log demographic distribution
    if 'student_group' in combined_kpi_df.columns:
        demo_counts = combined_kpi_df['student_group'].value_counts()
        logger.info(f"Top 10 demographics: {dict(demo_counts.head(10))}")
    
    print(f"Wrote {output_path}")
    print(f"Demographic audit: {audit_path}")


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    
    from pathlib import Path
    raw_dir = Path(__file__).parent.parent / "data" / "raw"
    proc_dir = Path(__file__).parent.parent / "data" / "processed"
    proc_dir.mkdir(exist_ok=True)
    
    test_config = {
        "derive": {
            "processing_date": "2025-07-19",
            "data_quality_flag": "reviewed"
        }
    }
    
    transform(raw_dir, proc_dir, test_config)