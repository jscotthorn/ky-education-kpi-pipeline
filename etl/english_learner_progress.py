"""
English Learner Progress ETL Module

Handles English learner progress/proficiency data from Kentucky across years 2022-2024.
Processes percentage scores in 4 proficiency bands (0, 60-80, 100, 140) directly from
Kentucky Department of Education data following standardized KPI format.
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
        'Demographic': 'demographic',
        'DEMOGRAPHIC': 'demographic',
        'Level': 'level',
        'LEVEL': 'level',
        'Suppressed': 'suppressed',
        'SUPPRESSED': 'suppressed',
        'Percentage Of Value Table Score Of 0': 'percentage_score_0',
        'PERCENTAGE OF VALUE TABLE SCORE OF 0': 'percentage_score_0',
        'Percentage Of Value Table Score Of 60 And 80': 'percentage_score_60_80',
        'PERCENTAGE OF VALUE TABLE SCORE OF 60 AND 80': 'percentage_score_60_80',
        'Percentage Of Value Table Score Of 100': 'percentage_score_100',
        'PERCENTAGE OF VALUE TABLE SCORE OF 100': 'percentage_score_100',
        'Percentage Of Value Table Score Of 140': 'percentage_score_140',
        'PERCENTAGE OF VALUE TABLE SCORE OF 140': 'percentage_score_140',
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
    percentage_columns = [col for col in df.columns if 'percentage_score' in col]
    for col in percentage_columns:
        if col in df.columns:
            df[col] = df[col].replace('*', pd.NA)
    
    return df


def add_derived_fields(df: pd.DataFrame, derive_config: Dict[str, Any]) -> pd.DataFrame:
    """Add derived fields based on configuration."""
    for field, value in derive_config.items():
        df[field] = value
    
    # Add data_source field based on year and format
    if 'school_year' in df.columns:
        # Extract year for source identification
        df['data_source'] = df['school_year'].apply(lambda x: f'english_learner_{str(x)[-4:]}')
    else:
        df['data_source'] = 'english_learner_unknown'
    
    return df


def clean_percentage_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate percentage score values."""
    percentage_columns = [col for col in df.columns if 'percentage_score' in col]
    
    for col in percentage_columns:
        if col in df.columns:
            # Convert to numeric, errors='coerce' will make invalid values NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Validate percentages are between 0 and 100
            invalid_mask = (df[col] < 0) | (df[col] > 100)
            if invalid_mask.any():
                logger.warning(f"Found {invalid_mask.sum()} invalid percentage scores in {col}")
                df.loc[invalid_mask, col] = pd.NA
    
    return df


def calculate_progress_metrics(row: pd.Series) -> Dict[str, float]:
    """Extract English learner proficiency scores directly from percentage data."""
    metrics = {}
    
    # Get percentage values for each proficiency band
    score_0 = row.get('percentage_score_0', pd.NA)
    score_60_80 = row.get('percentage_score_60_80', pd.NA)
    score_100 = row.get('percentage_score_100', pd.NA)
    score_140 = row.get('percentage_score_140', pd.NA)
    
    # Direct score metrics reflecting actual data structure
    if pd.notna(score_0):
        metrics['english_learner_score_0'] = score_0
    if pd.notna(score_60_80):
        metrics['english_learner_score_60_80'] = score_60_80
    if pd.notna(score_100):
        metrics['english_learner_score_100'] = score_100
    if pd.notna(score_140):
        metrics['english_learner_score_140'] = score_140
    
    return metrics


def convert_to_kpi_format(df: pd.DataFrame, demographic_mapper: Optional[DemographicMapper] = None) -> pd.DataFrame:
    """Convert wide format English learner data to long KPI format with standardized demographics."""
    from datetime import datetime
    
    # Initialize demographic mapper if not provided
    if demographic_mapper is None:
        demographic_mapper = DemographicMapper()
    
    # Skip if no percentage columns
    percentage_columns = [col for col in df.columns if 'percentage_score' in col]
    if not percentage_columns:
        logger.warning("No percentage score columns found for KPI conversion")
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
        source_file = row.get('source_file', 'english_learner_progress.csv')
        student_group = demographic_mapper.map_demographic(original_demographic, year, source_file)
        
        # Get education level for metric naming
        level = row.get('level', 'All')
        if pd.isna(level) or level == '':
            level = 'All'
        
        # Normalize level names
        level_mapping = {
            'ES': 'elementary',
            'Elementary School': 'elementary',
            'Elementary': 'elementary',
            'MS': 'middle',
            'Middle School': 'middle',
            'Middle': 'middle',
            'HS': 'high',
            'High School': 'high',
            'High': 'high',
            'All': 'all'
        }
        normalized_level = level_mapping.get(level, 'all')
        
        # Check if this record is suppressed
        is_suppressed = row.get('suppressed', 'N') == 'Y'
        
        # Common KPI row template
        kpi_template = {
            'district': row.get('district_name', 'Fayette County'),
            'school_id': school_id,
            'school_name': row.get('school_name', 'Unknown School'),
            'year': year,
            'student_group': student_group,
            'source_file': row.get('source_file', 'english_learner_progress.csv'),
            'last_updated': datetime.now().isoformat(),
            'suppressed': 'Y' if is_suppressed else 'N'
        }
        
        if is_suppressed:
            # For suppressed records, create metrics with NaN values
            suppressed_metrics = [
                f'english_learner_score_0_{normalized_level}',
                f'english_learner_score_60_80_{normalized_level}',
                f'english_learner_score_100_{normalized_level}',
                f'english_learner_score_140_{normalized_level}'
            ]
            
            for metric in suppressed_metrics:
                metric_kpi = kpi_template.copy()
                metric_kpi.update({
                    'metric': metric,
                    'value': pd.NA
                })
                kpi_rows.append(metric_kpi)
        else:
            # Calculate metrics from percentage scores
            metrics = calculate_progress_metrics(row)
            
            # Create KPI rows for each calculated metric
            for metric_name, value in metrics.items():
                if pd.notna(value):
                    metric_kpi = kpi_template.copy()
                    metric_kpi.update({
                        'metric': f'{metric_name}_{normalized_level}',
                        'value': float(value)
                    })
                    kpi_rows.append(metric_kpi)
    
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
    """Read English learner progress files, normalize, and convert to KPI format with demographic standardization."""
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
    
    logger.info(f"Found {len(csv_files)} English learner progress files to process")
    
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
            df = clean_percentage_scores(df)
            
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
    
    logger.info(f"Combined English learner progress KPI data written to {output_path}")
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