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
import re
from pydantic import BaseModel
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
            # Index score columns (multiple variations)
            'CLIMATE INDEX': 'climate_index',
            'SAFETY INDEX': 'safety_index',
            'Climate Index': 'climate_index',
            'Safety Index': 'safety_index',
            # Survey results columns
            'Question Type': 'question_type',
            'Question': 'question_text',
            'Question Index': 'question_index',
            'Agree / Strongly Agree': 'agree_strongly_agree_pct',
            # Accountability profile columns
            'QUALITY OF SCHOOL CLIMATE AND SAFETY STATUS': 'climate_safety_status',
            'QUALITY OF SCHOOL CLIMATE AND SAFETY STATUS RATING': 'climate_safety_rating',
            'QUALITY OF SCHOOL CLIMATE AND SAFETY COMBINED INDICATOR RATE': 'climate_safety_combined_rate',
        }
    
    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get default metrics for suppressed records based on available data."""
        metrics = {}
        
        # Only include index scores if we have index columns
        if 'climate_index' in row or 'safety_index' in row or 'climate_safety_combined_rate' in row:
            metrics['climate_index_score'] = pd.NA
            metrics['safety_index_score'] = pd.NA
            
        # Only include policy compliance if we have policy columns  
        policy_columns = [
            'visitors_sign_in', 'classroom_doors_lock', 'classroom_phones',
            'annual_climate_survey', 'student_survey_data', 'resource_officer',
            'mental_health_referrals', 'discipline_code_distributed'
        ]
        if any(col in row for col in policy_columns):
            metrics['safety_policy_compliance_rate'] = pd.NA
            
        return metrics
    
    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        """Extract metrics based on the data type."""
        metrics = {}
        
        # Check if this is direct index score data (KYRC24_ACCT_Index_Scores.csv)
        if 'climate_index' in row and pd.notna(row.get('climate_index')):
            metrics['climate_index_score'] = row.get('climate_index', pd.NA)
        if 'safety_index' in row and pd.notna(row.get('safety_index')):
            metrics['safety_index_score'] = row.get('safety_index', pd.NA)
        
        # Check if this is survey results data with question index
        if 'question_type' in row and 'question_index' in row:
            question_type = str(row.get('question_type', '')).lower()
            if question_type == 'climate':
                metrics['climate_index_score'] = row.get('question_index', pd.NA)
            elif question_type == 'safety':
                metrics['safety_index_score'] = row.get('question_index', pd.NA)
        
        # Check if this is accountability profile data
        if 'climate_safety_combined_rate' in row and pd.notna(row.get('climate_safety_combined_rate')):
            # Use combined rate as both climate and safety score for historical data
            combined_rate = row.get('climate_safety_combined_rate', pd.NA)
            metrics['climate_index_score'] = combined_rate
            metrics['safety_index_score'] = combined_rate
        
        # Check if this is precautionary measures data
        policy_columns = [
            'visitors_sign_in', 'classroom_doors_lock', 'classroom_phones',
            'annual_climate_survey', 'student_survey_data', 'resource_officer',
            'mental_health_referrals', 'discipline_code_distributed'
        ]
        
        # Only calculate if we have at least one policy column with actual data
        if any(col in row and pd.notna(row.get(col)) for col in policy_columns):
            compliance_rate = calculate_policy_compliance_rate(row, policy_columns)
            if pd.notna(compliance_rate):  # Only add if we got a valid rate
                metrics['safety_policy_compliance_rate'] = compliance_rate
        
        return metrics
    
    def identify_file_type(self, file_path: Path) -> str:
        """Identify the type of file based on name and content."""
        filename = file_path.name.lower()
        
        if 'index_scores' in filename:
            return 'index_scores'
        elif 'survey_results' in filename:
            return 'survey_results'
        elif 'precautionary' in filename:
            return 'precautionary_measures'
        elif 'accountability_profile' in filename:
            return 'accountability_profile'
        elif 'quality_of_school_climate' in filename:
            if 'index' in filename:
                return 'survey_index_scores'
            else:
                return 'survey_responses'
        else:
            return 'unknown'
    
    def process_file(self, file_path: Path) -> pd.DataFrame:
        """Process a single file based on its type."""
        file_type = self.identify_file_type(file_path)
        logger.info(f"Processing {file_type} file: {file_path.name}")
        
        # Read the file
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        
        # Apply column mappings
        df = self.normalize_column_names(df)
        
        # Extract year based on file type
        if 'school_year' in df.columns:
            df['year'] = df['school_year'].astype(str).str[-4:]
        else:
            # Try to extract year from filename
            year_match = re.search(r'(20\d{2})', file_path.name)
            if year_match:
                df['year'] = year_match.group(1)
            else:
                df['year'] = '2024'
        
        # Set demographic if not present
        if 'demographic' not in df.columns:
            df['demographic'] = 'All Students'
        
        # Apply demographic mapping
        if 'demographic' in df.columns and 'year' in df.columns:
            df['student_group'] = standardize_demographics(
                df['demographic'], 
                df['year'].iloc[0] if len(df) > 0 else '2024',
                self.source_name
            )
        else:
            df['student_group'] = 'All Students'
        
        # Process missing values
        df = self.standardize_missing_values(df)
        
        # Add source file info
        df['source_file'] = file_path.name
        
        return df
    
    def process_files(self, files: List[Path]) -> pd.DataFrame:
        """Override to handle different file types appropriately."""
        all_data = []
        
        for file in files:
            logger.info(f"Processing file: {file}")
            
            try:
                # Use the unified process_file method
                df = self.process_file(file)
                
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
        """Get all files to process based on file patterns."""
        module_dir = raw_dir / self.source_name
        files_to_process = []
        
        # Define file patterns to process
        patterns = [
            'KYRC24_ACCT_Index_Scores.csv',
            'KYRC24_ACCT_Survey_Results.csv', 
            'KYRC24_SAFE_Precautionary_Measures.csv',
            'accountability_profile_*.csv',
            'precautionary_measures_*.csv',
            'quality_of_school_climate_and_safety_survey_*.csv'
        ]
        
        # Find all matching files
        for pattern in patterns:
            for file_path in module_dir.glob(pattern):
                if file_path.is_file():
                    files_to_process.append(file_path)
        
        logger.info(f"Found {len(files_to_process)} files to process for safe schools climate")
        return sorted(files_to_process)


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