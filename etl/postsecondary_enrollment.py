"""
Postsecondary Enrollment ETL Module

Transforms Kentucky Department of Education postsecondary enrollment data 
into standardized KPI format for equity analysis.

Handles two different data formats:
- 2024: KYRC24 format with simplified structure and BOM encoding
- 2021-2023: Standard format with detailed county/district/school breakdowns

Processes enrollment counts and rates for:
- Public college enrollment in-state
- Private college enrollment in-state  
- Total college enrollment in-state

Follows project standards for demographic mapping, audit logging, and data validation.
"""

from pathlib import Path
import pandas as pd
from pydantic import BaseModel
from typing import Dict, Any, Optional, Union, List
import logging
import re

try:
    from .demographic_mapper import DemographicMapper
except ImportError:
    import sys
    from pathlib import Path
    sys.path.append(str(Path(__file__).parent.parent))
    from etl.demographic_mapper import DemographicMapper

logger = logging.getLogger(__name__)


class Config(BaseModel):
    rename: Dict[str, str] = {}
    dtype: Dict[str, str] = {}
    derive: Dict[str, Union[str, int, float]] = {}


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names to lowercase with underscores for consistent processing."""
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
        'Demographic': 'demographic',
        'DEMOGRAPHIC': 'demographic',
        
        # Enrollment metrics
        'Total In Group': 'total_in_group',
        'TOTAL IN GROUP': 'total_in_group',
        'Public College Enrolled In State': 'public_college_enrolled',
        'PUBLIC COLLEGE ENROLLED IN STATE': 'public_college_enrolled',
        'Private College Enrolled In State': 'private_college_enrolled',
        'PRIVATE COLLEGE ENROLLED IN STATE': 'private_college_enrolled',
        'College Enrolled In State': 'college_enrolled_total',
        'COLLEGE ENROLLED IN STATE': 'college_enrolled_total',
        'Percentage Public College Enrolled In State': 'public_college_rate',
        'PERCENTAGE PUBLIC COLLEGE ENROLLED IN STATE': 'public_college_rate',
        'Percentage Private College Enrolled In State': 'private_college_rate',
        'PERCENTAGE PRIVATE COLLEGE ENROLLED IN STATE': 'private_college_rate',
        'Percentage College Enrolled In State': 'college_enrollment_rate',
        'PERCENTAGE COLLEGE ENROLLED IN STATE': 'college_enrollment_rate',
        'Percentage College Enrolled In State Table': 'college_enrollment_rate'  # 2024 variation
    }
    
    # Handle BOM and encoding issues
    if df.columns[0].startswith('﻿'):
        df.columns.values[0] = df.columns[0].replace('﻿', '')
    
    # Apply normalization
    df = df.rename(columns=column_mapping)
    
    return df


def detect_data_format(df: pd.DataFrame, source_file: str) -> str:
    """
    Detect the data format based on available columns and filename.
    
    Returns:
        'kyrc24' for 2024 format or 'standard' for 2021-2023 format
    """
    if 'KYRC24' in source_file:
        return 'kyrc24'
    elif any(col in df.columns for col in ['county_number', 'county_name']) and '2024' not in source_file:
        return 'standard'
    else:
        # Default to standard if unclear
        return 'standard'


def add_derived_fields(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    """Add derived fields for data source tracking and year extraction."""
    df = df.copy()
    
    # Add source file tracking
    df['source_file'] = source_file
    
    # Extract year from school_year column
    if 'school_year' in df.columns:
        # Handle different year formats
        year_str = df['school_year'].astype(str).str.extract(r'(\d{4})')[0]
        df['year'] = pd.to_numeric(year_str, errors='coerce')
        
        # For 8-digit format, determine if it's start year or end year based on source
        # Check if original school_year was 8 digits
        mask_8digit = df['school_year'].astype(str).str.len() == 8
        if mask_8digit.any():
            # For KYRC24 files, use ending year; for others, use starting year  
            if 'KYRC24' in source_file:
                df.loc[mask_8digit, 'year'] = df.loc[mask_8digit, 'year'] + 1
            # else: use starting year (which is already extracted)
    
    return df


def standardize_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize missing value representations across different file formats."""
    # Standard missing value indicators for postsecondary enrollment
    missing_indicators = ['*', '**', '', 'N/A', 'n/a', '---', '--', '<10']
    
    # Replace missing indicators with pandas NA
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].replace(missing_indicators, pd.NA)
    
    return df


def clean_percentage_values(df: pd.DataFrame) -> pd.DataFrame:
    """Clean percentage columns by removing % signs and converting to numeric."""
    percentage_columns = [col for col in df.columns if 'rate' in col.lower()]
    
    for col in percentage_columns:
        if col in df.columns:
            # Remove % signs and convert to numeric
            df[col] = df[col].astype(str).str.replace('%', '').str.replace(',', '')
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df


def clean_numeric_values(df: pd.DataFrame) -> pd.DataFrame:
    """Clean numeric columns by removing commas and converting to numeric."""
    numeric_columns = ['total_in_group', 'public_college_enrolled', 'private_college_enrolled', 'college_enrolled_total']
    
    for col in numeric_columns:
        if col in df.columns:
            # Remove commas and convert to numeric
            df[col] = df[col].astype(str).str.replace(',', '').str.replace('"', '')
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df


def convert_to_kpi_format(df: pd.DataFrame, source_file: str, demographic_mapper: DemographicMapper) -> pd.DataFrame:
    """
    Convert postsecondary enrollment data to standardized KPI format.
    
    Returns:
        DataFrame in standardized KPI format with required columns:
        district, school_id, school_name, year, student_group, metric, value, suppressed, source_file, last_updated
    """
    format_type = detect_data_format(df, source_file)
    
    kpi_records = []
    
    for _, row in df.iterrows():
        # Skip non-data rows 
        if pd.isna(row.get('demographic')) or row.get('demographic') in ['Total Events', '---District Total---']:
            continue
            
        # Extract base information
        district = row.get('district_name', 'Unknown District')
        school_id = str(row.get('school_code', '')) if pd.notna(row.get('school_code')) else ''
        school_name = row.get('school_name', 'Unknown School')
        year = int(row.get('year', 0)) if pd.notna(row.get('year')) else 0
        
        # Map demographic using DemographicMapper
        original_demographic = str(row.get('demographic', ''))
        student_group = demographic_mapper.map_demographic(
            original_demographic, year, source_file
        )
        
        # Define metrics to extract
        metrics_data = [
            ('postsecondary_enrollment_total_cohort', row.get('total_in_group')),
            ('postsecondary_enrollment_public_count', row.get('public_college_enrolled')),
            ('postsecondary_enrollment_private_count', row.get('private_college_enrolled')),
            ('postsecondary_enrollment_total_count', row.get('college_enrolled_total')),
            ('postsecondary_enrollment_public_rate', row.get('public_college_rate')),
            ('postsecondary_enrollment_private_rate', row.get('private_college_rate')),
            ('postsecondary_enrollment_total_rate', row.get('college_enrollment_rate'))
        ]
        
        # Create KPI records for each metric
        for metric_name, value in metrics_data:
            # Handle suppression
            suppressed = 'N'
            if pd.isna(value) or value == '*' or str(value).strip() == '' or str(value) == '**':
                suppressed = 'Y'
                value = pd.NA
            else:
                try:
                    value = float(value)
                    # Ensure non-negative counts for count metrics
                    if 'count' in metric_name or 'cohort' in metric_name:
                        if value < 0:
                            suppressed = 'Y'
                            value = pd.NA
                    # Ensure valid rate ranges for rate metrics
                    elif 'rate' in metric_name:
                        if value < 0 or value > 100:
                            suppressed = 'Y'
                            value = pd.NA
                except (ValueError, TypeError):
                    suppressed = 'Y'
                    value = pd.NA
            
            kpi_record = {
                'district': district,
                'school_id': school_id,
                'school_name': school_name,
                'year': year,
                'student_group': student_group,
                'metric': metric_name,
                'value': value,
                'suppressed': suppressed,
                'source_file': source_file,
                'last_updated': pd.Timestamp.now().isoformat()
            }
            
            kpi_records.append(kpi_record)
    
    # Convert to DataFrame
    kpi_df = pd.DataFrame(kpi_records)
    
    # Validate output format
    expected_columns = [
        'district', 'school_id', 'school_name', 'year', 'student_group',
        'metric', 'value', 'suppressed', 'source_file', 'last_updated'
    ]
    
    if not all(col in kpi_df.columns for col in expected_columns):
        missing_cols = set(expected_columns) - set(kpi_df.columns)
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Ensure correct column order
    kpi_df = kpi_df[expected_columns]
    
    logger.info(f"Converted {len(df)} input rows to {len(kpi_df)} KPI records")
    
    return kpi_df


def transform(raw_dir: str, proc_dir: str, config: Optional[Config] = None) -> Dict[str, Any]:
    """
    Transform postsecondary enrollment data to KPI format.
    
    Args:
        raw_dir: Directory containing raw CSV files
        proc_dir: Directory to write processed CSV files 
        config: Optional configuration
        
    Returns:
        Dictionary containing transformation results and metadata
    """
    raw_path = Path(raw_dir)
    proc_path = Path(proc_dir)
    
    # Ensure output directory exists
    proc_path.mkdir(parents=True, exist_ok=True)
    
    # Initialize demographic mapper
    demographic_mapper = DemographicMapper()
    
    # Find CSV files
    csv_files = list(raw_path.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {raw_path}")
    
    logger.info(f"Found {len(csv_files)} CSV files to process")
    
    all_kpi_dataframes = []
    file_metadata = {}
    
    for csv_file in csv_files:
        logger.info(f"Processing {csv_file.name}")
        
        try:
            # Read raw data
            df = pd.read_csv(csv_file, encoding='utf-8-sig')
            original_rows = len(df)
            
            if df.empty:
                logger.warning(f"Empty file: {csv_file.name}")
                continue
            
            # Apply transformations
            df = normalize_column_names(df)
            df = add_derived_fields(df, csv_file.name)
            df = standardize_missing_values(df)
            df = clean_percentage_values(df)
            df = clean_numeric_values(df)
            
            # Convert to KPI format
            kpi_df = convert_to_kpi_format(df, csv_file.name, demographic_mapper)
            
            all_kpi_dataframes.append(kpi_df)
            
            # Track metadata
            file_metadata[csv_file.name] = {
                'original_rows': original_rows,
                'kpi_rows': len(kpi_df),
                'years': sorted(kpi_df['year'].unique().tolist()),
                'demographics': sorted(kpi_df['student_group'].unique().tolist()),
                'metrics': sorted(kpi_df['metric'].unique().tolist())
            }
            
            logger.info(f"Successfully processed {csv_file.name}: {original_rows} → {len(kpi_df)} KPI rows")
            
        except Exception as e:
            logger.error(f"Error processing {csv_file.name}: {str(e)}")
            continue
    
    if not all_kpi_dataframes:
        raise RuntimeError("No files were successfully processed")
    
    # Combine all data
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
    source_name = "postsecondary_enrollment"
    audit_path = proc_path / f"{source_name}_demographic_audit.csv"
    demographic_mapper.save_audit_log(audit_path)
    
    # Write processed KPI data
    output_path = proc_path / f"{source_name}.csv"
    combined_kpi_df.to_csv(output_path, index=False)
    
    logger.info(f"Combined postsecondary enrollment KPI data written to {output_path}")
    logger.info(f"Demographic audit log written to {audit_path}")
    logger.info(f"Total KPI rows: {len(combined_kpi_df)}, Total columns: {len(combined_kpi_df.columns)}")
    
    return {
        'success': True,
        'output_path': str(output_path),
        'audit_path': str(audit_path),
        'total_rows': len(combined_kpi_df),
        'files_processed': len(all_kpi_dataframes),
        'years_covered': sorted(years_processed),
        'demographics_found': len(unique_demographics),
        'metrics_generated': sorted(combined_kpi_df['metric'].unique().tolist()),
        'file_metadata': file_metadata
    }


if __name__ == "__main__":
    import sys
    import os
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Set up paths
    current_dir = Path(__file__).parent
    project_root = current_dir.parent
    raw_dir = project_root / 'data' / 'raw' / 'postsecondary_enrollment'
    processed_dir = project_root / 'data' / 'processed'
    
    try:
        # Run transformation
        result = transform(str(raw_dir), str(processed_dir))
        
        print("✅ Postsecondary enrollment data transformation completed successfully")
        print(f"📊 Processed {result['files_processed']} files")
        print(f"📈 Generated {result['total_rows']} KPI records")
        print(f"📅 Years covered: {result['years_covered']}")
        print(f"👥 Demographics found: {result['demographics_found']}")
        print(f"📏 Metrics generated: {len(result['metrics_generated'])}")
        print(f"💾 Output saved to: {result['output_path']}")
        print(f"📋 Audit log saved to: {result['audit_path']}")
        
    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}")
        print("❌ Postsecondary enrollment data transformation failed")
        print(f"Error: {str(e)}")
        sys.exit(1)