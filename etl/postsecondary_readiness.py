"""
Postsecondary Readiness ETL Module

Handles Kentucky postsecondary readiness data across years 2022-2024.
Normalizes column names, handles schema variations, standardizes missing values, and
applies demographic label standardization for consistent longitudinal reporting.

Data includes two rate metrics:
- Postsecondary Rate: Base readiness rate
- Postsecondary Rate With Bonus: Enhanced rate including bonus indicators
"""
from pathlib import Path
import pandas as pd
from pydantic import BaseModel
from typing import Dict, Any, Optional, Union
import logging
from datetime import datetime

try:
    from .demographic_mapper import DemographicMapper
except ImportError:
    from etl.demographic_mapper import DemographicMapper

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
        'CO-OP CODE': 'co_op_code',
        'School Type': 'school_type',
        'SCHOOL TYPE': 'school_type',
        'Demographic': 'demographic',
        'DEMOGRAPHIC': 'demographic',
        'Suppressed': 'suppressed',
        'SUPPRESSED': 'suppressed',
        'Postsecondary Rate': 'postsecondary_rate',
        'POSTSECONDARY RATE': 'postsecondary_rate',
        'Postsecondary Rate With Bonus': 'postsecondary_rate_with_bonus',
        'POSTSECONDARY RATE WITH BONUS': 'postsecondary_rate_with_bonus',
    }
    
    # Apply mapping for columns that exist
    rename_dict = {col: column_mapping[col] for col in df.columns if col in column_mapping}
    return df.rename(columns=rename_dict)


def standardize_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Convert empty strings and suppression markers to NaN."""
    # Replace empty strings with NaN
    df = df.replace('', pd.NA)
    df = df.replace('""', pd.NA)
    
    # Replace suppression markers with NaN in rate columns
    rate_columns = [col for col in df.columns if 'postsecondary_rate' in col]
    for col in rate_columns:
        if col in df.columns:
            df[col] = df[col].replace('*', pd.NA)
    
    return df


def add_derived_fields(df: pd.DataFrame, derive_config: Dict[str, Any]) -> pd.DataFrame:
    """Add derived fields based on configuration."""
    for field, value in derive_config.items():
        df[field] = value
    
    # Add data_source field based on columns present and format
    if 'postsecondary_rate' in df.columns:
        df['data_source'] = 'postsecondary_rates'
    else:
        df['data_source'] = 'unknown_format'
    
    return df


def clean_readiness_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate postsecondary readiness rate values."""
    rate_columns = [col for col in df.columns if 'postsecondary_rate' in col]
    
    for col in rate_columns:
        if col in df.columns:
            # Convert to numeric, errors='coerce' will make invalid values NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Validate rates are between 0 and 100
            invalid_mask = (df[col] < 0) | (df[col] > 100)
            if invalid_mask.any():
                logger.warning(f"Found {invalid_mask.sum()} invalid readiness rates in {col}")
                df.loc[invalid_mask, col] = pd.NA
    
    return df


def convert_to_kpi_format(df: pd.DataFrame, demographic_mapper: Optional[DemographicMapper] = None) -> pd.DataFrame:
    """Convert wide format postsecondary readiness data to long KPI format with standardized demographics."""
    
    # Initialize demographic mapper if not provided
    if demographic_mapper is None:
        demographic_mapper = DemographicMapper()
    
    # Identify rate columns
    rate_columns = [col for col in df.columns if 'postsecondary_rate' in col]
    
    if not rate_columns:
        logger.warning("No postsecondary rate columns found for KPI conversion")
        return pd.DataFrame()
    
    # Define standard KPI columns that should be preserved
    id_columns = ['school_year', 'county_name', 'district_name', 'school_name', 
                  'school_code', 'state_school_id', 'nces_id', 'demographic', 'source_file']
    
    # Keep only columns that exist in the dataframe
    available_id_columns = [col for col in id_columns if col in df.columns]
    
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
        source_file = row.get('source_file', 'postsecondary_readiness.csv')
        student_group = demographic_mapper.map_demographic(original_demographic, year, source_file)
        
        # Check if this record is suppressed
        is_suppressed = row.get('suppressed', 'N') == 'Y'
        
        # Common KPI row template
        kpi_template = {
            'district': row.get('district_name', 'Fayette County'),
            'school_id': school_id,
            'school_name': row.get('school_name', 'Unknown School'),
            'year': year,
            'student_group': student_group,
            'suppressed': 'Y' if is_suppressed else 'N',
            'source_file': row.get('source_file', 'postsecondary_readiness.csv'),
            'last_updated': datetime.now().isoformat()
        }
        
        # Process each rate metric
        for rate_col in rate_columns:
            # Determine metric name based on column
            if 'with_bonus' in rate_col:
                metric_name = 'postsecondary_readiness_rate_with_bonus'
            else:
                metric_name = 'postsecondary_readiness_rate'
            
            # Create rate KPI row
            rate_kpi = kpi_template.copy()
            rate_kpi.update({
                'metric': metric_name
            })
            
            if is_suppressed:
                # For suppressed records, set value to NaN
                rate_kpi['value'] = pd.NA
            else:
                # Process normal (non-suppressed) data
                rate_value = row.get(rate_col)
                
                # Skip if rate value is missing or invalid
                if pd.isna(rate_value) or rate_value == '':
                    continue
                
                # Convert to numeric if it's a string
                try:
                    numeric_rate = float(rate_value)
                    rate_kpi['value'] = numeric_rate
                except (ValueError, TypeError):
                    continue
            
            kpi_rows.append(rate_kpi)
    
    if not kpi_rows:
        logger.warning("No valid KPI rows created")
        return pd.DataFrame()
    
    # Create KPI dataframe
    kpi_df = pd.DataFrame(kpi_rows)
    
    # Ensure consistent column order
    kpi_columns = ['district', 'school_id', 'school_name', 'year', 'student_group', 
                   'metric', 'value', 'suppressed', 'source_file', 'last_updated']
    
    # Only include columns that exist
    available_columns = [col for col in kpi_columns if col in kpi_df.columns]
    kpi_df = kpi_df[available_columns]
    
    return kpi_df


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Read newest postsecondary readiness files, normalize, and convert to KPI format with demographic standardization."""
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
    
    logger.info(f"Found {len(csv_files)} postsecondary readiness files to process")
    
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
            df = add_derived_fields(df, conf.derive)
            df = clean_readiness_data(df)
            
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
    
    logger.info(f"Combined postsecondary readiness KPI data written to {output_path}")
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