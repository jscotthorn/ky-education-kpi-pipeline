"""
Graduation Rates ETL Module

Handles multiple file formats from Kentucky graduation rate data across years 2021-2024.
Normalizes column names, handles schema variations, and standardizes missing values.
"""
from pathlib import Path
import pandas as pd
from pydantic import BaseModel
from typing import Dict, Any, Optional, Union
import logging

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
        'Demographic': 'demographic',
        'DEMOGRAPHIC': 'demographic',
        'Suppressed': 'suppressed_4_year',
        'SUPPRESSED 4 YEAR': 'suppressed_4_year',
        'Suppressed 4 Year': 'suppressed_4_year',
        'SUPPRESSED 5 YEAR': 'suppressed_5_year',
        '4 Year Cohort Graduation Rate': 'graduation_rate_4_year',
        '4-YEAR GRADUATION RATE': 'graduation_rate_4_year',
        '5-YEAR GRADUATION RATE': 'graduation_rate_5_year',
        'NUMBER OF GRADS IN 4-YEAR COHORT': 'grads_4_year_cohort',
        'NUMBER OF STUDENTS IN 4-YEAR COHORT': 'students_4_year_cohort',
        'NUMBER OF GRADS IN 5-YEAR COHORT': 'grads_5_year_cohort',
        'NUMBER OF STUDENTS IN 5-YEAR COHORT': 'students_5_year_cohort',
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
    rate_columns = [col for col in df.columns if 'graduation_rate' in col]
    for col in rate_columns:
        if col in df.columns:
            df[col] = df[col].replace('*', pd.NA)
    
    return df


def add_derived_fields(df: pd.DataFrame, derive_config: Dict[str, Any]) -> pd.DataFrame:
    """Add derived fields based on configuration."""
    for field, value in derive_config.items():
        df[field] = value
    
    # Add data_source field based on columns present
    if 'grads_4_year_cohort' in df.columns:
        df['data_source'] = '2021_detailed'
    elif 'graduation_rate_5_year' in df.columns:
        df['data_source'] = '2022_2023_standard'
    else:
        df['data_source'] = '2024_simplified'
    
    return df


def clean_graduation_rates(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate graduation rate values."""
    rate_columns = [col for col in df.columns if 'graduation_rate' in col]
    
    for col in rate_columns:
        if col in df.columns:
            # Convert to numeric, errors='coerce' will make invalid values NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Validate rates are between 0 and 100
            invalid_mask = (df[col] < 0) | (df[col] > 100)
            if invalid_mask.any():
                logger.warning(f"Found {invalid_mask.sum()} invalid graduation rates in {col}")
                df.loc[invalid_mask, col] = pd.NA
    
    return df


def convert_to_kpi_format(df: pd.DataFrame) -> pd.DataFrame:
    """Convert wide format graduation data to long KPI format with expanded metrics."""
    from datetime import datetime
    
    # Identify metric columns
    metric_columns = [col for col in df.columns if 'graduation_rate' in col]
    
    if not metric_columns:
        logger.warning("No graduation rate columns found for KPI conversion")
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
        
        # Map demographic to standard student group names
        student_group = row.get('demographic', 'All Students')
        if pd.isna(student_group):
            student_group = 'All Students'
        
        # Common KPI row template
        kpi_template = {
            'district': row.get('district_name', 'Fayette County'),
            'school_id': school_id,
            'school_name': row.get('school_name', 'Unknown School'),
            'year': year,
            'student_group': student_group,
            'source_file': row.get('source_file', 'graduation_rates.csv'),
            'last_updated': datetime.now().isoformat()
        }
        
        # Process each graduation rate metric
        for metric_col in metric_columns:
            rate_value = row.get(metric_col)
            
            # Skip if rate value is missing or invalid
            if pd.isna(rate_value) or rate_value == '':
                continue
            
            # Convert to numeric if it's a string
            try:
                numeric_rate = float(rate_value)
            except (ValueError, TypeError):
                continue
            
            # Determine period (4-year or 5-year)
            period = '4_year' if '4' in metric_col else '5_year'
            
            # Create rate KPI row
            rate_kpi = kpi_template.copy()
            rate_kpi.update({
                'metric': f'graduation_rate_{period}',
                'value': numeric_rate
            })
            kpi_rows.append(rate_kpi)
            
            # Look for corresponding count columns
            count_columns = [
                f'grads_{period}_cohort',
                f'students_{period}_cohort',
                f'number_of_grads_in_{period.replace("_", "-")}_cohort',
                f'number_of_students_in_{period.replace("_", "-")}_cohort'
            ]
            
            grads_count = None
            total_count = None
            
            # Find graduation count and total count
            for count_col in count_columns:
                if count_col in df.columns:
                    if 'grads' in count_col:
                        grads_count = row.get(count_col)
                    elif 'students' in count_col:
                        total_count = row.get(count_col)
            
            # Create count KPI rows if data is available
            if grads_count is not None and not pd.isna(grads_count) and grads_count != '':
                try:
                    numeric_grads = int(float(grads_count))
                    count_kpi = kpi_template.copy()
                    count_kpi.update({
                        'metric': f'graduation_count_{period}',
                        'value': numeric_grads
                    })
                    kpi_rows.append(count_kpi)
                except (ValueError, TypeError):
                    logger.warning(f"Could not convert grads count to numeric: {grads_count}")
            
            if total_count is not None and not pd.isna(total_count) and total_count != '':
                try:
                    numeric_total = int(float(total_count))
                    total_kpi = kpi_template.copy()
                    total_kpi.update({
                        'metric': f'graduation_total_{period}',
                        'value': numeric_total
                    })
                    kpi_rows.append(total_kpi)
                except (ValueError, TypeError):
                    logger.warning(f"Could not convert total count to numeric: {total_count}")
            
            # If we have rate but no counts, try to calculate from rate and see if we can infer
            # For files that only have rates, we can't generate counts
            if grads_count is None and total_count is None:
                logger.debug(f"No count data available for {metric_col} in {row.get('source_file', 'unknown')}")
    
    if not kpi_rows:
        logger.warning("No valid KPI rows created")
        return pd.DataFrame()
    
    # Create KPI dataframe
    kpi_df = pd.DataFrame(kpi_rows)
    
    # Ensure consistent column order
    kpi_columns = ['district', 'school_id', 'school_name', 'year', 'student_group', 
                   'metric', 'value', 'source_file', 'last_updated']
    
    # Only include columns that exist
    available_columns = [col for col in kpi_columns if col in kpi_df.columns]
    kpi_df = kpi_df[available_columns]
    
    return kpi_df


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Read newest graduation rates files, normalize, and convert to KPI format."""
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
    
    logger.info(f"Found {len(csv_files)} graduation rate files to process")
    
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
            df = clean_graduation_rates(df)
            
            # Apply configuration-based transformations
            if conf.rename:
                df = df.rename(columns=conf.rename)
            
            # Add file source for tracking
            df['source_file'] = csv_file.name
            
            # Convert to KPI format
            kpi_df = convert_to_kpi_format(df)
            
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
    
    # Write processed KPI data
    output_path = proc_dir / f"{source_name}.csv"
    combined_kpi_df.to_csv(output_path, index=False)
    
    logger.info(f"Combined graduation rates KPI data written to {output_path}")
    logger.info(f"Total KPI rows: {len(combined_kpi_df)}, Total columns: {len(combined_kpi_df.columns)}")
    
    # Log summary statistics
    if 'value' in combined_kpi_df.columns:
        valid_values = combined_kpi_df['value'].dropna()
        if len(valid_values) > 0:
            logger.info(f"KPI value range: {valid_values.min():.1f} - {valid_values.max():.1f}")
    
    # Log metric distribution
    if 'metric' in combined_kpi_df.columns:
        metric_counts = combined_kpi_df['metric'].value_counts()
        logger.info(f"Metrics created: {dict(metric_counts)}")
    
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    
    from pathlib import Path
    raw_dir = Path(__file__).parent.parent / "data" / "raw"
    proc_dir = Path(__file__).parent.parent / "data" / "processed"
    proc_dir.mkdir(exist_ok=True)
    
    test_config = {
        "derive": {
            "processing_date": "2025-07-18",
            "data_quality_flag": "reviewed"
        }
    }
    
    transform(raw_dir, proc_dir, test_config)