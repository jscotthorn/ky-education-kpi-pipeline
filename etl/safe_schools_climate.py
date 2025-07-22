"""
Safe Schools Climate ETL Module

Handles Kentucky safe schools climate and safety data:
- Precautionary measures/safety policy compliance data (2024)
- Quality of school climate and safety survey index scores (2021-2023)

Generates metrics:
- climate_index_score: School climate perception index (0-100)
- safety_index_score: School safety perception index (0-100)
- safety_policy_compliance_rate: Percentage of implemented safety policies
"""
from pathlib import Path
import pandas as pd
from typing import Dict, Any, Union, List
import logging

import sys
from pathlib import Path

# Add etl directory to path for imports
etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from base_etl import BaseETL, Config
from demographic_mapper import standardize_demographics

logger = logging.getLogger(__name__)


def calculate_policy_compliance_rate(row: pd.Series, policy_columns: List[str]) -> float:
    """Calculate the percentage of 'Yes' responses across policy questions."""
    yes_count = 0
    total_count = 0
    
    for col in policy_columns:
        if col in row and pd.notna(row[col]):
            total_count += 1
            if str(row[col]).strip().upper() == 'YES':
                yes_count += 1
    
    if total_count == 0:
        return pd.NA
    
    return round((yes_count / total_count) * 100, 1)


def clean_index_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate climate/safety index scores."""
    index_columns = ['climate_index', 'safety_index']
    
    for col in index_columns:
        if col in df.columns:
            # Convert to numeric
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Validate scores are between 0 and 100
            invalid_mask = (df[col] < 0) | (df[col] > 100)
            if invalid_mask.any():
                logger.warning(f"Found {invalid_mask.sum()} invalid scores in {col}")
                df.loc[invalid_mask, col] = pd.NA
    
    return df


class SafeSchoolsClimateETL(BaseETL):
    """ETL module for processing safe schools climate and safety data."""
    
    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            # Precautionary measures columns
            'Are visitors to the building required to sign-in?': 'visitors_sign_in',
            'Do all classroom doors lock from the inside?': 'classroom_doors_lock',
            'Do all classrooms have access to a telephone accessing outside lines?': 'classroom_phones',
            'Does your school administer a school climate survey annually?': 'annual_climate_survey',
            'Does your school collect and use student survey data?': 'student_survey_data',
            'Does your school have a full-time School Resource Officer?': 'resource_officer',
            'Does your school have a process in place to provide mental health referrals for students?': 'mental_health_referrals',
            'Is the district discipline code distributed to parents?': 'discipline_code_distributed',
            # Index score columns
            'CLIMATE INDEX': 'climate_index',
            'SAFETY INDEX': 'safety_index',
        }
    
    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get default metrics for suppressed records."""
        return {
            'climate_index_score': pd.NA,
            'safety_index_score': pd.NA,
            'safety_policy_compliance_rate': pd.NA
        }
    
    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        """Extract metrics based on the data type (precautionary measures vs index scores)."""
        metrics = {}
        
        # Check if this is index score data
        if 'climate_index' in row or 'safety_index' in row:
            metrics['climate_index_score'] = row.get('climate_index', pd.NA)
            metrics['safety_index_score'] = row.get('safety_index', pd.NA)
        
        # Check if this is precautionary measures data
        policy_columns = [
            'visitors_sign_in', 'classroom_doors_lock', 'classroom_phones',
            'annual_climate_survey', 'student_survey_data', 'resource_officer',
            'mental_health_referrals', 'discipline_code_distributed'
        ]
        
        if any(col in row for col in policy_columns):
            compliance_rate = calculate_policy_compliance_rate(row, policy_columns)
            metrics['safety_policy_compliance_rate'] = compliance_rate
        
        return metrics
    
    def process_precautionary_measures(self, file_path: Path) -> pd.DataFrame:
        """Process precautionary measures file with special handling."""
        logger.info(f"Processing precautionary measures file: {file_path}")
        
        # Read the file
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        
        # Apply column mappings
        df = self.normalize_column_names(df)
        
        # Set demographic to 'All Students' for school-level data
        df['demographic'] = 'All Students'
        
        # Extract year from School Year column (e.g., "20232024" -> 2024)
        if 'school_year' in df.columns:
            df['year'] = df['school_year'].astype(str).str[-4:]
        else:
            df['year'] = '2024'
        
        # Process normally
        df = self.standardize_missing_values(df)
        
        # Apply demographic mapping if demographic column exists
        if 'demographic' in df.columns and 'year' in df.columns:
            df['student_group'] = standardize_demographics(
                df['demographic'], 
                df['year'].iloc[0] if len(df) > 0 else '2024',
                source_file=file_path.name
            )
        else:
            df['student_group'] = 'All Students'
        
        return df
    
    def process_files(self, files: List[Path]) -> pd.DataFrame:
        """Override to handle different file types appropriately."""
        all_data = []
        
        for file in files:
            logger.info(f"Processing file: {file}")
            
            try:
                # Special handling for precautionary measures
                if 'KYRC24_SAFE_Precautionary_Measures' in file.name:
                    df = self.process_precautionary_measures(file)
                else:
                    # Normal processing for index score files
                    df = pd.read_csv(file, encoding='utf-8-sig')
                    df = self.normalize_column_names(df)
                    
                    # Extract year from filename or school year column
                    if 'index_scores' in file.name:
                        year_match = file.name.split('_')[-1].replace('.csv', '')
                        df['year'] = year_match
                    elif 'school_year' in df.columns:
                        # Convert "20222023" to "2023"
                        df['year'] = df['school_year'].astype(str).str[-4:]
                    
                    df = self.standardize_missing_values(df)
                    df = clean_index_scores(df)
                    
                    # Apply demographic mapping
                    if 'demographic' in df.columns and 'year' in df.columns:
                        df['student_group'] = standardize_demographics(
                            df['demographic'], 
                            df['year'].iloc[0] if len(df) > 0 else '2023',
                            source_file=file.name
                        )
                    else:
                        df['student_group'] = 'All Students'
                
                # Convert to KPI format
                kpi_df = self.convert_to_kpi_format(df, file.name)
                if not kpi_df.empty:
                    all_data.append(kpi_df)
                    
            except Exception as e:
                logger.error(f"Error processing {file}: {e}")
                continue
        
        if not all_data:
            logger.warning("No data processed from any files")
            return pd.DataFrame()
        
        # Combine all data
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Remove duplicates keeping the most recent
        combined_df = combined_df.sort_values(['year', 'source_file'], ascending=[False, True])
        combined_df = combined_df.drop_duplicates(
            subset=['district', 'school_id', 'school_name', 'year', 'student_group', 'metric'],
            keep='first'
        )
        
        return combined_df
    
    def get_files_to_process(self, raw_dir: Path) -> List[Path]:
        """Override to only process specific files."""
        module_dir = raw_dir / self.source_name
        
        # Only process precautionary measures and index score files
        files_to_process = []
        
        # Add precautionary measures file
        precautionary_file = module_dir / 'KYRC24_SAFE_Precautionary_Measures.csv'
        if precautionary_file.exists():
            files_to_process.append(precautionary_file)
        
        # Add index score files
        for year in ['2021', '2022', '2023']:
            index_file = module_dir / f'quality_of_school_climate_and_safety_survey_index_scores_{year}.csv'
            if index_file.exists():
                files_to_process.append(index_file)
        
        logger.info(f"Found {len(files_to_process)} files to process for safe schools climate")
        return files_to_process


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Process safe schools climate data using BaseETL."""
    etl = SafeSchoolsClimateETL('safe_schools_climate')
    
    # Override the default transform to use only specific files
    source_dir = raw_dir / etl.source_name
    
    if not source_dir.exists():
        logger.info(f"No raw data directory for {etl.source_name}; skipping.")
        return
    
    # Get only the files we want to process
    csv_files = etl.get_files_to_process(raw_dir)
    
    if not csv_files:
        logger.info(f"No files to process for {etl.source_name}")
        return
    
    # Process the files
    combined_df = etl.process_files(csv_files)
    
    if combined_df.empty:
        logger.warning("No data processed")
        return
    
    # Save to processed directory
    output_file = proc_dir / f"{etl.source_name}_kpi.csv"
    combined_df.to_csv(output_file, index=False)
    logger.info(f"Saved {len(combined_df)} records to {output_file}")


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    
    from pathlib import Path
    raw_dir = Path(__file__).parent.parent / "data" / "raw"
    proc_dir = Path(__file__).parent.parent / "data" / "processed"
    proc_dir.mkdir(exist_ok=True)
    
    test_config = Config(
        derive={"processing_date": "2025-07-21", "data_quality_flag": "reviewed"}
    ).dict()

    transform(raw_dir, proc_dir, test_config)