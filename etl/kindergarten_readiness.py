"""
Kindergarten Readiness ETL Module

Handles multiple file formats from Kentucky kindergarten readiness data across years 2021-2024.
Normalizes column names, handles schema variations, standardizes missing values, and
applies demographic label standardization for consistent longitudinal reporting.

Key Data Differences by Year:
- 2024: Uses counts (Total Ready), has state-level data ("All Districts")
- 2021-2023: Uses percentages (TOTAL PERCENT READY), district-level only
- 2021: Has NUMBER TESTED field, others do not
- Demographic categories vary between years
"""
from pathlib import Path
import pandas as pd
from pydantic import BaseModel
from typing import Dict, Any, Optional, Union
import logging
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
        'Demographic': 'demographic',
        'DEMOGRAPHIC': 'demographic',
        'Prior Setting': 'prior_setting',
        'PRIOR SETTING': 'prior_setting',
        'TOTAL PERCENT READY': 'total_percent_ready',
        'Total Percent Ready': 'total_percent_ready',
        'PERCENT READY WITH INTERVENTIONS': 'percent_ready_with_interventions',
        'PERCENT READY': 'percent_ready',
        'PERCENT READY WITH ENRICHMENTS': 'percent_ready_with_enrichments',
        'Ready With Interventions': 'ready_with_interventions_count',
        'Ready': 'ready_count',
        'Ready With Enrichments': 'ready_with_enrichments_count',
        'Total Ready': 'total_ready_count',
        'NUMBER TESTED': 'number_tested',
        'Number Tested': 'number_tested',
        'ENROLLMENT': 'enrollment',
        'Enrollment': 'enrollment',
        'PARTICIPATION RATE': 'participation_rate',
        'Participation Rate': 'participation_rate',
        'Suppressed': 'suppressed',
        'SUPPRESSED': 'suppressed',
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
    readiness_columns = [col for col in df.columns if 'ready' in col.lower() or 'percent' in col.lower()]
    for col in readiness_columns:
        if col in df.columns:
            df[col] = df[col].replace('*', pd.NA)
    
    return df


def validate_demographic_column(df: pd.DataFrame) -> pd.DataFrame:
    """Validate demographic column exists and log demographic groups found."""
    if 'demographic' in df.columns:
        demographics = df['demographic'].unique()
        logger.info(f"Found {len(demographics)} demographic groups: {list(demographics)}")
        return df
    else:
        logger.warning("No demographic column found, returning all data")
        return df


def filter_target_prior_setting(df: pd.DataFrame) -> pd.DataFrame:
    """Filter to include 'All Students' prior setting for district/school totals."""
    if 'prior_setting' in df.columns:
        # Use "All Students" prior setting for overall readiness rates
        target_df = df[df['prior_setting'] == 'All Students'].copy()
        logger.info(f"Filtered to 'All Students' prior setting: {len(target_df)} rows from {len(df)} total")
        return target_df
    else:
        logger.warning("No prior_setting column found, returning all data")
        return df


def add_derived_fields(df: pd.DataFrame, derive_config: Dict[str, Any]) -> pd.DataFrame:
    """Add derived fields based on configuration."""
    for field, value in derive_config.items():
        df[field] = value
    
    # Add data_source field based on columns present and data format
    if 'total_ready_count' in df.columns:
        # Check if values are actually percentages (2024 format) vs counts
        sample_values = df['total_ready_count'].dropna()
        if len(sample_values) > 0:
            max_value = pd.to_numeric(sample_values, errors='coerce').max()
            if pd.notna(max_value) and max_value <= 100:
                # Values are 0-100 range, likely percentages not counts
                df['data_source'] = '2024_percentages'
                logger.info(f"Detected 2024 percentage format (max value: {max_value})")
            else:
                # Values exceed 100, likely actual counts
                df['data_source'] = '2024_counts'
                logger.info(f"Detected 2024 count format (max value: {max_value})")
        else:
            df['data_source'] = '2024_percentages'  # Default to safer assumption
    elif 'number_tested' in df.columns:
        df['data_source'] = '2021_with_testing_data'
    elif 'total_percent_ready' in df.columns:
        df['data_source'] = '2022_2023_percentages_only'
    else:
        df['data_source'] = 'unknown_format'
    
    return df


def clean_readiness_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate kindergarten readiness values."""
    # Clean percentage columns
    percent_columns = [col for col in df.columns if 'percent' in col.lower() and 'ready' in col.lower()]
    
    for col in percent_columns:
        if col in df.columns:
            # Convert to numeric, errors='coerce' will make invalid values NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Validate percentages are between 0 and 100
            invalid_mask = (df[col] < 0) | (df[col] > 100)
            if invalid_mask.any():
                logger.warning(f"Found {invalid_mask.sum()} invalid readiness percentages in {col}")
                df.loc[invalid_mask, col] = pd.NA
    
    # Clean count columns
    count_columns = [col for col in df.columns if 'count' in col.lower() or col == 'number_tested' or col == 'enrollment']
    
    for col in count_columns:
        if col in df.columns:
            # Convert to numeric
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Validate counts are non-negative
            invalid_mask = df[col] < 0
            if invalid_mask.any():
                logger.warning(f"Found {invalid_mask.sum()} invalid negative counts in {col}")
                df.loc[invalid_mask, col] = pd.NA
    
    return df


def mark_suppressed_data(df: pd.DataFrame) -> pd.DataFrame:
    """Mark suppressed data records instead of filtering them out."""
    if 'suppressed' in df.columns:
        suppressed_count = len(df[df['suppressed'] == 'Y'])
        if suppressed_count > 0:
            logger.info(f"Found {suppressed_count} suppressed records that will be included with NaN values")
        return df
    else:
        logger.info("No suppressed column found, treating all data as non-suppressed")
        df['suppressed'] = 'N'
        return df


def convert_to_kpi_format(df: pd.DataFrame, demographic_mapper: Optional[DemographicMapper] = None) -> pd.DataFrame:
    """Convert wide format kindergarten readiness data to long KPI format with standardized demographics."""
    from datetime import datetime
    
    # Initialize demographic mapper if not provided
    if demographic_mapper is None:
        demographic_mapper = DemographicMapper()
    
    if df.empty:
        logger.warning("Empty dataframe provided for KPI conversion")
        return pd.DataFrame()
    
    kpi_rows = []
    
    for _, row in df.iterrows():
        # Extract school/district identification
        district_number = row.get('district_number', '')
        district_name = row.get('district_name', 'Unknown District')
        school_number = row.get('school_number', '')
        school_name = row.get('school_name', district_name)  # Use district name if no school
        
        # Clean and format IDs
        if pd.notna(district_number) and district_number != '':
            try:
                district_id = str(int(float(district_number)))
            except (ValueError, TypeError):
                district_id = str(district_number)
        else:
            district_id = 'unknown'
        
        if pd.notna(school_number) and school_number != '':
            try:
                school_id = str(int(float(school_number)))
            except (ValueError, TypeError):
                school_id = str(school_number)
        else:
            # Use district ID if no school ID (district-level data)
            school_id = district_id
        
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
        source_file = row.get('source_file', 'kindergarten_readiness.csv')
        student_group = demographic_mapper.map_demographic(original_demographic, year, source_file)
        
        # Check if this record is suppressed
        is_suppressed = row.get('suppressed', 'N') == 'Y'
        
        # Common KPI row template
        kpi_template = {
            'district': district_name,
            'school_id': school_id,
            'school_name': school_name,
            'year': year,
            'student_group': student_group,
            'suppressed': 'Y' if is_suppressed else 'N',
            'source_file': source_file,
            'last_updated': datetime.now().isoformat()
        }
        
        # Determine data format and extract KPI values
        data_source = row.get('data_source', 'unknown')
        
        if data_source == '2024_counts':
            # 2024 data: counts format
            if is_suppressed:
                # For suppressed records, create KPIs with NaN values
                for metric in ['kindergarten_readiness_rate', 'kindergarten_readiness_count', 'kindergarten_readiness_total']:
                    suppressed_kpi = kpi_template.copy()
                    suppressed_kpi.update({
                        'metric': metric,
                        'value': pd.NA
                    })
                    kpi_rows.append(suppressed_kpi)
            else:
                total_ready_count = row.get('total_ready_count')
                ready_with_interventions = row.get('ready_with_interventions_count')
                ready_count = row.get('ready_count') 
                ready_with_enrichments = row.get('ready_with_enrichments_count')
                
                # Calculate total tested from component counts
                total_tested = None
                if all(pd.notna(x) and x != '' for x in [ready_with_interventions, ready_count, ready_with_enrichments]):
                    try:
                        total_tested = int(float(ready_with_interventions)) + int(float(ready_count)) + int(float(ready_with_enrichments))
                    except (ValueError, TypeError):
                        pass
                
                # Create rate KPI if we can calculate it
                if total_ready_count is not None and pd.notna(total_ready_count) and total_ready_count != '':
                    try:
                        ready_count_num = float(total_ready_count)
                        if total_tested and total_tested > 0:
                            ready_rate = (ready_count_num / total_tested) * 100
                            
                            rate_kpi = kpi_template.copy()
                            rate_kpi.update({
                                'metric': 'kindergarten_readiness_rate',
                                'value': ready_rate
                            })
                            kpi_rows.append(rate_kpi)
                        
                        # Create count KPI
                        count_kpi = kpi_template.copy()
                        count_kpi.update({
                            'metric': 'kindergarten_readiness_count',
                            'value': int(ready_count_num)
                        })
                        kpi_rows.append(count_kpi)
                        
                    except (ValueError, TypeError):
                        logger.warning(f"Could not convert readiness count to numeric: {total_ready_count}")
                
                # Create total KPI if available
                if total_tested is not None:
                    total_kpi = kpi_template.copy()
                    total_kpi.update({
                        'metric': 'kindergarten_readiness_total',
                        'value': total_tested
                    })
                    kpi_rows.append(total_kpi)
        
        elif data_source == '2024_percentages':
            # 2024 data: percentage format (no count/total data available)
            if is_suppressed:
                # For suppressed records, create only rate KPI with NaN value
                suppressed_kpi = kpi_template.copy()
                suppressed_kpi.update({
                    'metric': 'kindergarten_readiness_rate',
                    'value': pd.NA
                })
                kpi_rows.append(suppressed_kpi)
            else:
                total_ready_rate = row.get('total_ready_count')  # Actually contains percentage
                
                # Create rate KPI only (no count/total available in this format)
                if total_ready_rate is not None and pd.notna(total_ready_rate) and total_ready_rate != '':
                    try:
                        ready_rate = float(total_ready_rate)
                        rate_kpi = kpi_template.copy()
                        rate_kpi.update({
                            'metric': 'kindergarten_readiness_rate',
                            'value': ready_rate
                        })
                        kpi_rows.append(rate_kpi)
                        logger.debug(f"Created 2024 percentage rate KPI: {ready_rate}%")
                    except (ValueError, TypeError):
                        logger.warning(f"Could not convert 2024 readiness rate to numeric: {total_ready_rate}")
        
        else:
            # 2021-2023 data: percentage format
            if is_suppressed:
                # For suppressed records, create KPIs with NaN values
                for metric in ['kindergarten_readiness_rate', 'kindergarten_readiness_count', 'kindergarten_readiness_total']:
                    suppressed_kpi = kpi_template.copy()
                    suppressed_kpi.update({
                        'metric': metric,
                        'value': pd.NA
                    })
                    kpi_rows.append(suppressed_kpi)
            else:
                total_percent_ready = row.get('total_percent_ready')
                number_tested = row.get('number_tested')
                
                # Create rate KPI
                if total_percent_ready is not None and pd.notna(total_percent_ready) and total_percent_ready != '':
                    try:
                        ready_rate = float(total_percent_ready)
                        
                        rate_kpi = kpi_template.copy()
                        rate_kpi.update({
                            'metric': 'kindergarten_readiness_rate', 
                            'value': ready_rate
                        })
                        kpi_rows.append(rate_kpi)
                        
                        # Calculate count if we have both rate and total tested
                        if number_tested is not None and pd.notna(number_tested) and number_tested != '':
                            try:
                                tested_num = int(float(number_tested))
                                ready_count = int((ready_rate / 100) * tested_num)
                                
                                count_kpi = kpi_template.copy()
                                count_kpi.update({
                                    'metric': 'kindergarten_readiness_count',
                                    'value': ready_count
                                })
                                kpi_rows.append(count_kpi)
                                
                            except (ValueError, TypeError):
                                logger.warning(f"Could not calculate readiness count from rate and total tested")
                        
                    except (ValueError, TypeError):
                        logger.warning(f"Could not convert readiness rate to numeric: {total_percent_ready}")
                
                # Create total KPI if available
                if number_tested is not None and pd.notna(number_tested) and number_tested != '':
                    try:
                        tested_num = int(float(number_tested))
                        
                        total_kpi = kpi_template.copy()
                        total_kpi.update({
                            'metric': 'kindergarten_readiness_total',
                            'value': tested_num
                        })
                        kpi_rows.append(total_kpi)
                        
                    except (ValueError, TypeError):
                        logger.warning(f"Could not convert number tested to numeric: {number_tested}")
    
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
    """Read kindergarten readiness files, normalize, and convert to KPI format with demographic standardization."""
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
    
    logger.info(f"Found {len(csv_files)} kindergarten readiness files to process")
    
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
            df = mark_suppressed_data(df)
            df = validate_demographic_column(df)
            df = filter_target_prior_setting(df)
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
    
    logger.info(f"Combined kindergarten readiness KPI data written to {output_path}")
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
        logger.info(f"Demographics: {dict(demo_counts)}")
    
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
            "processing_date": "2025-07-18",
            "data_quality_flag": "reviewed"
        }
    }
    
    transform(raw_dir, proc_dir, test_config)