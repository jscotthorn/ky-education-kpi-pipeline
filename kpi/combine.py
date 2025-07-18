"""
Combine processed ETL outputs into master KPI file.
All processed files should already be in KPI format (long layout).
Handles expanded KPI metrics including rates, counts, and totals.
"""
from pathlib import Path
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def validate_kpi_format(df: pd.DataFrame, source_name: str) -> bool:
    """Validate that dataframe is in correct KPI format."""
    required_columns = ['district', 'school_id', 'school_name', 'year', 
                       'student_group', 'metric', 'value', 'source_file', 'last_updated']
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logger.warning(f"File {source_name} missing required KPI columns: {missing_columns}")
        return False
    
    # Check if 'value' column is numeric
    if not pd.api.types.is_numeric_dtype(df['value']):
        logger.warning(f"File {source_name} has non-numeric 'value' column")
        return False
    
    return True


def combine_all(proc_dir: Path, output_path: Path) -> None:
    """Combine all processed KPI CSV files into kpi_master.csv."""
    kpi_dfs = []
    
    for csv_file in proc_dir.glob("*.csv"):
        logger.info(f"Processing {csv_file.name}")
        
        try:
            df = pd.read_csv(csv_file)
            
            if df.empty:
                logger.warning(f"Empty file: {csv_file.name}")
                continue
            
            # Validate KPI format
            if not validate_kpi_format(df, csv_file.name):
                logger.error(f"Skipping {csv_file.name} - invalid KPI format")
                continue
            
            # Ensure source_file is populated
            if 'source_file' not in df.columns or df['source_file'].isna().all():
                df['source_file'] = csv_file.name
            
            kpi_dfs.append(df)
            logger.info(f"Added {len(df)} KPI rows from {csv_file.name}")
            
        except Exception as e:
            logger.error(f"Error processing {csv_file.name}: {e}")
            continue
    
    if not kpi_dfs:
        logger.warning("No valid KPI files found; creating empty master file.")
        # Create empty file with correct KPI schema
        empty_df = pd.DataFrame(columns=[
            'district', 'school_id', 'school_name', 'year', 'student_group', 
            'metric', 'value', 'source_file', 'last_updated'
        ])
        empty_df.to_csv(output_path, index=False)
        return
    
    # Combine all KPI dataframes
    master_df = pd.concat(kpi_dfs, ignore_index=True, sort=False)
    
    # Ensure consistent column order
    kpi_columns = ['district', 'school_id', 'school_name', 'year', 'student_group', 
                   'metric', 'value', 'source_file', 'last_updated']
    
    # Only include columns that exist
    available_columns = [col for col in kpi_columns if col in master_df.columns]
    master_df = master_df[available_columns]
    
    # Sort by key fields for consistent output
    sort_columns = ['district', 'school_name', 'year', 'student_group', 'metric']
    existing_sort_columns = [col for col in sort_columns if col in master_df.columns]
    if existing_sort_columns:
        master_df = master_df.sort_values(existing_sort_columns).reset_index(drop=True)
    
    # Write master KPI file
    master_df.to_csv(output_path, index=False)
    
    logger.info(f"Combined {len(kpi_dfs)} KPI sources into {output_path}")
    logger.info(f"Master KPI file has {len(master_df)} rows, {len(master_df.columns)} columns")
    
    # Log summary statistics
    if 'metric' in master_df.columns:
        metric_counts = master_df['metric'].value_counts()
        logger.info(f"Metrics in master file: {dict(metric_counts)}")
    
    if 'student_group' in master_df.columns:
        group_counts = master_df['student_group'].value_counts()
        logger.info(f"Student groups: {len(group_counts)} unique groups")
    
    print(f"Combined {len(kpi_dfs)} sources into {output_path}")
    print(f"Master file has {len(master_df)} rows, {len(master_df.columns)} columns")


if __name__ == "__main__":
    from pathlib import Path
    
    proc_dir = Path(__file__).parent.parent / "data" / "processed"
    output_path = Path(__file__).parent / "kpi_master.csv"
    combine_all(proc_dir, output_path)