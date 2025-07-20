"""
Out-of-School Suspension ETL Module

Transforms Kentucky Department of Education out-of-school suspension data 
into standardized KPI format for equity analysis.

Handles two different data formats:
- 2024: KYRC24 format with separate single/multiple suspension columns by disability status
- 2021-2023: Safe Schools format with single "OUT OF SCHOOL SUSPENSION SSP3" column

Follows project standards for demographic mapping, audit logging, and data validation.
"""

from pathlib import Path
import pandas as pd
from pydantic import BaseModel
from typing import Dict, Any, Optional, Union, List
import logging

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
        'Collected School Year': 'collected_school_year',
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
        
        # 2024 KYRC24 format columns
        'In-School With Disabilities': 'in_school_with_disabilities',
        'In-School Without Disabilities': 'in_school_without_disabilities',
        'Single Out-of-School With Disabilities': 'single_out_of_school_with_disabilities',
        'Single Out-of-School Without Disabilities': 'single_out_of_school_without_disabilities',
        'Multiple Out-of-School With Disabilities': 'multiple_out_of_school_with_disabilities',
        'Multiple Out-of-School Without Disabilities': 'multiple_out_of_school_without_disabilities',
        
        # 2021-2023 Safe Schools format columns
        'Total Discipline Resolutions': 'total_discipline_resolutions',
        'TOTAL DISCIPLINE RESOLUTIONS': 'total_discipline_resolutions',
        'Expelled Receiving Services SSP1': 'expelled_receiving_services',
        'EXPELLED RECEIVING SERVICES SSP1': 'expelled_receiving_services',
        'Expelled Not Receiving Services SSP2': 'expelled_not_receiving_services',
        'EXPELLED NOT RECEIVING SERVICES SSP2': 'expelled_not_receiving_services',
        'Out of School Suspension SSP3': 'out_of_school_suspension',
        'OUT OF SCHOOL SUSPENSION SSP3': 'out_of_school_suspension',
        'Corporal Punishment SSP5': 'corporal_punishment',
        'CORPORAL PUNISHMENT SSP5': 'corporal_punishment',
        'In-School Removal INSR': 'in_school_removal',
        'IN-SCHOOL REMOVAL INSR': 'in_school_removal',
        'Restraint SSP7': 'restraint',
        'RESTRAINT SSP7': 'restraint',
        'Seclusion SSP8': 'seclusion',
        'SECLUSION SSP8': 'seclusion',
        'Unilateral Removal by School Personnel IAES1': 'unilateral_removal',
        'UNILATERAL REMOVAL BY SCHOOL PERSONNEL IAES1': 'unilateral_removal',
        'Removal by Hearing Officer IAES2': 'removal_by_hearing_officer',
        'REMOVAL BY HEARING OFFICER IAES2': 'removal_by_hearing_officer'
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
        'kyrc24' for 2024 format or 'safe_schools' for 2021-2023 format
    """
    if 'KYRC24' in source_file or 'single_out_of_school_with_disabilities' in df.columns:
        return 'kyrc24'
    elif 'out_of_school_suspension' in df.columns or 'safe_schools' in source_file.lower():
        return 'safe_schools'
    else:
        raise ValueError(f"Unable to detect data format for file: {source_file}")


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
        
        # For 2024 data with 8-digit format (20232024), use the ending year
        df.loc[df['year'] > 2025, 'year'] = (df.loc[df['year'] > 2025, 'year'] // 10000) + 1
    
    return df


def standardize_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize missing value representations across different file formats."""
    # Standard missing value indicators
    missing_indicators = ['*', '**', '', 'N/A', 'n/a', '---', '--']
    
    # Replace missing indicators with pandas NA
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].replace(missing_indicators, pd.NA)
    
    return df


def convert_to_kpi_format(df: pd.DataFrame, source_file: str, demographic_mapper: DemographicMapper) -> pd.DataFrame:
    """
    Convert out-of-school suspension data to standardized KPI format.
    
    Returns:
        DataFrame in standardized KPI format with required columns:
        district, school_id, school_name, year, student_group, metric, value, suppressed, source_file, last_updated
    """
    format_type = detect_data_format(df, source_file)
    
    kpi_records = []
    
    for _, row in df.iterrows():
        # Skip non-data rows 
        if pd.isna(row.get('demographic')) or row.get('demographic') in ['Total Events']:
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
        
        # Process suspension data based on format
        metrics_data = []
        
        if format_type == 'kyrc24':
            # 2024 format: separate single/multiple columns by disability status
            raw_metrics = {
                'out_of_school_suspension_single_with_disabilities_count': 
                    row.get('single_out_of_school_with_disabilities', 0),
                'out_of_school_suspension_single_without_disabilities_count': 
                    row.get('single_out_of_school_without_disabilities', 0),
                'out_of_school_suspension_multiple_with_disabilities_count': 
                    row.get('multiple_out_of_school_with_disabilities', 0),
                'out_of_school_suspension_multiple_without_disabilities_count': 
                    row.get('multiple_out_of_school_without_disabilities', 0),
            }
            
            # Calculate totals
            single_total = sum(pd.to_numeric(val, errors='coerce') for val in [
                row.get('single_out_of_school_with_disabilities', 0),
                row.get('single_out_of_school_without_disabilities', 0)
            ] if pd.notna(val) and str(val) != '*')
            
            multiple_total = sum(pd.to_numeric(val, errors='coerce') for val in [
                row.get('multiple_out_of_school_with_disabilities', 0),
                row.get('multiple_out_of_school_without_disabilities', 0)
            ] if pd.notna(val) and str(val) != '*')
            
            overall_total = single_total + multiple_total
            
            raw_metrics.update({
                'out_of_school_suspension_single_total_count': single_total,
                'out_of_school_suspension_multiple_total_count': multiple_total,
                'out_of_school_suspension_total_count': overall_total
            })
            
            metrics_data = list(raw_metrics.items())
            
        else:  # safe_schools format
            # 2021-2023 format: single out-of-school suspension column
            out_of_school_count = row.get('out_of_school_suspension', 0)
            metrics_data = [('out_of_school_suspension_count', out_of_school_count)]
        
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
                    # Ensure non-negative counts
                    if value < 0:
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
    Transform out-of-school suspension data to KPI format.
    
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
    source_name = "out_of_school_suspension"
    audit_path = proc_path / f"{source_name}_demographic_audit.csv"
    demographic_mapper.save_audit_log(audit_path)
    
    # Write processed KPI data
    output_path = proc_path / f"{source_name}.csv"
    # Ensure school_id is string type before writing
    if 'school_id' in combined_kpi_df.columns:
        combined_kpi_df['school_id'] = combined_kpi_df['school_id'].astype(str)
    combined_kpi_df.to_csv(output_path, index=False)
    
    logger.info(f"Combined out-of-school suspension KPI data written to {output_path}")
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
    raw_dir = project_root / 'data' / 'raw' / 'out_of_school_suspension'
    processed_dir = project_root / 'data' / 'processed'
    
    try:
        # Run transformation
        result = transform(str(raw_dir), str(processed_dir))
        
        print("✅ Out-of-school suspension data transformation completed successfully")
        print(f"📊 Processed {result['files_processed']} files")
        print(f"📈 Generated {result['total_rows']} KPI records")
        print(f"📅 Years covered: {result['years_covered']}")
        print(f"👥 Demographics found: {result['demographics_found']}")
        print(f"📏 Metrics generated: {len(result['metrics_generated'])}")
        print(f"💾 Output saved to: {result['output_path']}")
        print(f"📋 Audit log saved to: {result['audit_path']}")
        
    except Exception as e:
        logger.error(f"Transformation failed: {str(e)}")
        print("❌ Out-of-school suspension data transformation failed")
        print(f"Error: {str(e)}")
        sys.exit(1)