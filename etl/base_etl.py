"""
Base ETL Module

Abstract base class for all ETL modules in the equity data pipeline.
Provides common functionality, column mappings, and template methods
to reduce code duplication and ensure consistency across modules.
"""

from abc import ABC, abstractmethod
from pathlib import Path
import pandas as pd
from pydantic import BaseModel
from typing import Dict, Any, Optional, Union, List
import logging
from datetime import datetime

import sys
from pathlib import Path

# Add etl directory to path for imports
etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from demographic_mapper import DemographicMapper

logger = logging.getLogger(__name__)


class Config(BaseModel):
    """Standard configuration model for all ETL modules."""
    rename: Dict[str, str] = {}
    dtype: Dict[str, str] = {}
    derive: Dict[str, Union[str, int, float]] = {}


class BaseETL(ABC):
    """
    Abstract base class for ETL modules.
    
    Provides common functionality for:
    - Column name normalization
    - Missing value standardization
    - KPI format conversion utilities
    - File processing workflow
    - Demographic mapping integration
    """
    
    # Common column mappings used across all modules
    COMMON_COLUMN_MAPPINGS = {
        # School year variations
        'School Year': 'school_year',
        'SCHOOL YEAR': 'school_year',
        
        # County identification
        'County Number': 'county_number',
        'COUNTY NUMBER': 'county_number',
        'County Name': 'county_name',
        'COUNTY NAME': 'county_name',
        
        # District identification
        'District Number': 'district_number',
        'DISTRICT NUMBER': 'district_number',
        'District Name': 'district_name',
        'DISTRICT NAME': 'district_name',
        
        # School identification
        'School Number': 'school_number',
        'SCHOOL NUMBER': 'school_number',
        'School Name': 'school_name',
        'SCHOOL NAME': 'school_name',
        'School Code': 'school_code',
        'SCHOOL CODE': 'school_code',
        'State School Id': 'state_school_id',
        'STATE SCHOOL ID': 'state_school_id',
        'NCES ID': 'nces_id',
        
        # Co-op identification
        'CO-OP': 'co_op',
        'CO-OP Code': 'co_op_code',
        'CO-OP CODE': 'co_op_code',
        
        # School type
        'School Type': 'school_type',
        'SCHOOL TYPE': 'school_type',
        
        # Demographics
        'Demographic': 'demographic',
        'DEMOGRAPHIC': 'demographic',
        
        # Grade field
        'Grade': 'grade',
        'GRADE': 'grade',
        
        # Suppression indicators
        'Suppressed': 'suppressed',
        'SUPPRESSED': 'suppressed',
    }
    
    # Common missing value indicators
    MISSING_VALUE_INDICATORS = ['*', '**', '', 'N/A', 'n/a', '---', '--', '<10', '""']
    
    # District aggregate row patterns to potentially filter
    DISTRICT_AGGREGATE_PATTERNS = ['Total Events', '---District Total---', 'District Total', 'All Schools']
    
    def __init__(self, source_name: Optional[str] = None):
        """
        Initialize the ETL module.
        
        Args:
            source_name: Name of the data source (defaults to module filename)
        """
        self.source_name = source_name or self.__class__.__module__.split('.')[-1]
        self.demographic_mapper = DemographicMapper()
    
    @property
    @abstractmethod
    def module_column_mappings(self) -> Dict[str, str]:
        """
        Module-specific column mappings.
        Should return a dictionary of column name mappings specific to this data source.
        """
        pass
    
    @abstractmethod
    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        """
        Extract metric values from a data row.
        
        Args:
            row: A pandas Series representing one row of data
            
        Returns:
            Dictionary mapping metric names to values
        """
        pass
    
    @abstractmethod
    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """
        Get detaults for suppressed metrics for a row.
        
        This method allows each ETL module to define its own default metrics
        when actual values cannot be extracted, particularly for suppressed
        records that need to be preserved in the output with NA values.
        
        Args:
            row: A pandas Series representing one row of data
            
        Returns:
            Dictionary mapping metric names to pd.NA or other default values
        """
        pass
    
    def get_column_mappings(self) -> Dict[str, str]:
        """
        Get combined column mappings (common + module-specific).
        
        Returns:
            Combined dictionary of column mappings
        """
        mappings = self.COMMON_COLUMN_MAPPINGS.copy()
        mappings.update(self.module_column_mappings)
        return mappings
    
    def normalize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize column names using combined mappings.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with normalized column names
        """
        # Handle BOM encoding issues
        if df.columns[0].startswith('﻿'):
            df.columns.values[0] = df.columns[0].replace('﻿', '')
        
        # Apply column mappings
        column_mappings = self.get_column_mappings()
        rename_dict = {col: column_mappings[col] for col in df.columns if col in column_mappings}
        return df.rename(columns=rename_dict)
    
    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert various missing value representations to pandas NA.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with standardized missing values
        """
        # Replace missing indicators with pandas NA
        pd.set_option("future.no_silent_downcasting", True)
        for col in df.columns:
            if df[col].dtype == "object":
                result = df[col].replace(self.MISSING_VALUE_INDICATORS, pd.NA)
                df[col] = result.infer_objects(copy=False)
        
        return df
    
    def add_derived_fields(self, df: pd.DataFrame, derive_config: Dict[str, Any], source_file: str) -> pd.DataFrame:
        """
        Add derived fields based on configuration and source tracking.
        
        Args:
            df: Input DataFrame
            derive_config: Configuration for derived fields
            source_file: Source filename for tracking
            
        Returns:
            DataFrame with added derived fields
        """
        # Add configuration-based derived fields
        for field, value in derive_config.items():
            df[field] = value
        
        # Add source file tracking
        df['source_file'] = source_file
        
        # Add data source identification
        df['data_source'] = self.source_name
        
        return df
    
    def extract_school_id(self, row: pd.Series) -> str:
        """
        Extract and clean school ID from row data.
        
        Uses a standardized identifier for maximum longitudinal consistency:
        School Code is required across all years.
        
        Args:
            row: Data row
            
        Returns:
            Cleaned school ID string
        """
        # Primary: School Code (universally available, hierarchical structure)
        school_id = row.get('school_code', '')
        if pd.notna(school_id) and school_id != '':
            return self._clean_school_id(school_id)

        # We should use a consistent identifier across years. Throwing an error
        # to monitor our use of other columns as we migrate ETLs to this
        # standard.
        logger.error("CRITICAL: No valid school ID found in row")
        raise ValueError("No valid school ID found in row")
    
    def _clean_school_id(self, school_id: Any) -> str:
        """
        Clean and standardize school ID format.
        
        Args:
            school_id: Raw school ID value
            
        Returns:
            Cleaned school ID string
        """
        if pd.isna(school_id) or school_id == '':
            return 'unknown'
        
        # Convert to string and clean up formatting
        cleaned_id = str(school_id).strip()
        
        # Remove .0 suffix if present (from float conversion) but preserve leading zeros
        if cleaned_id.endswith('.0'):
            cleaned_id = cleaned_id[:-2]
        
        return cleaned_id
    
    def extract_year(self, row: pd.Series) -> str:
        """
        Extract year from school_year field.
        
        Args:
            row: Data row
            
        Returns:
            4-digit year string
        """
        year = row.get('school_year', '')
        
        if len(str(year)) == 8:  # Format: YYYYYYYY (e.g., "20232024")
            year = str(year)[-4:]  # Take last 4 digits (ending year)
        elif len(str(year)) == 4:  # Already 4 digits
            year = str(year)
        else:
            year = '2024'  # Default
        
        return year
    
    def normalize_grade_field(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize grade field values for consistent reporting.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with normalized grade field
        """
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
        
        df['grade'] = df['grade'].astype(str)
        df['grade'] = df['grade'].map(grade_mapping).fillna(
            df['grade'].str.lower().str.replace(' ', '_')
        )
        
        return df
    
    def should_skip_row(self, row: pd.Series) -> bool:
        """
        Determine if a row should be skipped (e.g., district aggregates).
        
        Args:
            row: Data row
            
        Returns:
            True if row should be skipped
        """
        demographic = row.get('demographic')
        
        # Skip rows with missing demographics
        if pd.isna(demographic):
            return True
        
        # Skip district aggregate rows (can be overridden by subclasses)
        if str(demographic) in self.DISTRICT_AGGREGATE_PATTERNS:
            return True
        
        return False
    
    def create_kpi_template(self, row: pd.Series, source_file: str) -> Dict[str, Any]:
        """
        Create a base KPI record template from a data row.
        
        Args:
            row: Data row
            source_file: Source filename
            
        Returns:
            Base KPI record dictionary
        """
        # Extract school identification
        school_id = self.extract_school_id(row)
        year = self.extract_year(row)
        
        # Map demographic using DemographicMapper
        original_demographic = row.get('demographic', 'All Students')
        student_group = self.demographic_mapper.map_demographic(
            original_demographic, year, source_file
        )
        
        # Check if record is suppressed
        is_suppressed = row.get('suppressed', 'N') == 'Y'
        
        return {
            'district': row.get('district_name', 'Unknown District'),
            'school_id': school_id,
            'school_name': row.get('school_name', 'Unknown School'),
            'year': year,
            'student_group': student_group,
            'suppressed': 'Y' if is_suppressed else 'N',
            'source_file': source_file,
            'last_updated': datetime.now().isoformat()
        }
    
    def convert_to_kpi_format(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """
        Convert data to standardized KPI format.
        
        Args:
            df: Input DataFrame
            source_file: Source filename
            
        Returns:
            DataFrame in KPI format
        """
        kpi_rows = []
        
        for _, row in df.iterrows():
            # Skip rows that shouldn't be processed
            if self.should_skip_row(row):
                continue
            
            # Create base KPI template
            kpi_template = self.create_kpi_template(row, source_file)
            
            # Extract metrics using module-specific logic
            metrics = self.extract_metrics(row)
            
            # Special handling for suppressed records: if no metrics extracted but record is suppressed,
            # create default metrics to ensure suppressed records are never lost
            if not metrics and kpi_template['suppressed'] == 'Y':
                metrics = self.get_suppressed_metric_defaults(row)
            
            # Create KPI rows for each metric
            for metric_name, value in metrics.items():
                kpi_record = kpi_template.copy()
                
                # Handle suppression and value assignment
                if kpi_template['suppressed'] == 'Y':
                    # For suppressed records, always include with NA value
                    kpi_record['value'] = pd.NA
                    kpi_record['metric'] = metric_name
                    kpi_rows.append(kpi_record)
                else:
                    # For non-suppressed records, validate and clean the value
                    try:
                        if pd.notna(value) and value != '':
                            kpi_record['value'] = float(value)
                            kpi_record['metric'] = metric_name
                            kpi_rows.append(kpi_record)
                        else:
                            continue  # Skip metrics with no value
                    except (ValueError, TypeError):
                        continue  # Skip invalid values
        
        if not kpi_rows:
            logger.warning("No valid KPI rows created")
            return pd.DataFrame()
        
        # Create KPI DataFrame with consistent column order
        kpi_df = pd.DataFrame(kpi_rows)
        kpi_columns = ['district', 'school_id', 'school_name', 'year', 'student_group', 
                       'metric', 'value', 'suppressed', 'source_file', 'last_updated']
        
        # Only include columns that exist
        available_columns = [col for col in kpi_columns if col in kpi_df.columns]
        kpi_df = kpi_df[available_columns]
        
        return kpi_df
    
    def transform(self, raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
        """
        Main transformation method - template method pattern.
        
        Args:
            raw_dir: Path to raw data directory
            proc_dir: Path to processed data directory  
            cfg: Configuration dictionary
        """
        source_dir = raw_dir / self.source_name
        
        if not source_dir.exists():
            logger.info(f"No raw data directory for {self.source_name}; skipping.")
            return
        
        # Find all CSV files
        csv_files = list(source_dir.glob("*.csv"))
        if not csv_files:
            logger.info(f"No CSV files found in {source_dir}; skipping.")
            return
        
        logger.info(f"Found {len(csv_files)} files to process for {self.source_name}")
        
        all_kpi_dataframes = []
        conf = Config(**cfg)
        
        for csv_file in csv_files:
            logger.info(f"Processing {csv_file.name}")
            
            try:
                # Check if file is empty before attempting to read
                if csv_file.stat().st_size == 0:
                    logger.warning(f"Empty file (0 bytes): {csv_file.name}")
                    continue
                
                # Read CSV file as strings to avoid mixed-type warnings and handle large files
                df = pd.read_csv(csv_file, encoding='utf-8-sig', dtype=str, low_memory=False)
                
                # Skip if empty DataFrame
                if df.empty:
                    logger.warning(f"Empty DataFrame: {csv_file.name}")
                    continue
                
                # Apply standard transformations
                df = self.normalize_column_names(df)
                df = self.standardize_missing_values(df)
                df = self.normalize_grade_field(df)
                df = self.add_derived_fields(df, conf.derive, csv_file.name)

                # Apply configuration-based transformations
                if conf.rename:
                    df = df.rename(columns=conf.rename)

                # Enforce data types from configuration
                if conf.dtype:
                    for col, dtype in conf.dtype.items():
                        if col in df.columns:
                            try:
                                # Handle numeric types (float, int, and nullable Int64)
                                if (dtype.lower().startswith('float') or 
                                    dtype.lower().startswith('int') or 
                                    dtype in ['Int64', 'Int32', 'Float64']):
                                    df[col] = pd.to_numeric(df[col], errors='coerce')
                                df[col] = df[col].astype(dtype)
                            except Exception as e:
                                logger.warning(f"Failed to convert column {col} to {dtype}: {e}")
                
                # Convert to KPI format
                kpi_df = self.convert_to_kpi_format(df, csv_file.name)
                
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
        validation_results = self._validate_demographics(combined_kpi_df)

        # Save outputs
        self._save_outputs(combined_kpi_df, proc_dir, validation_results)
        
        logger.info(f"Completed processing for {self.source_name}")
    
    def _validate_demographics(self, kpi_df: pd.DataFrame) -> List[Dict[str, List[str]]]:
        """Validate demographic coverage for processed data.

        Returns a list of validation results for each year.
        """
        years_processed = kpi_df['year'].unique().tolist()
        results: List[Dict[str, List[str]]] = []
        
        # Validate demographics for each year
        for year in years_processed:
            year_demographics = kpi_df[kpi_df['year'] == year]['student_group'].unique().tolist()
            validation_result = self.demographic_mapper.validate_demographics(year_demographics, year)
            results.append(validation_result)
            
            if validation_result['missing_required']:
                logger.warning(f"Missing required demographics for {year}: {validation_result['missing_required']}")
            if validation_result['unexpected']:
                logger.warning(f"Unexpected demographics for {year}: {validation_result['unexpected']}")
            
            logger.info(
                f"Year {year}: {len(validation_result['valid'])} valid demographics, "
                f"{len(validation_result['missing_optional'])} optional missing"
            )
        return results

    def _save_outputs(self, kpi_df: pd.DataFrame, proc_dir: Path, validation_results: List[Dict[str, List[str]]]) -> None:
        """Save KPI data and demographic audit log."""
        # Save demographic mapping report
        audit_path = proc_dir / f"{self.source_name}_demographic_report.md"
        self.demographic_mapper.save_audit_report(audit_path, validation_results)
        
        # Write processed KPI data
        output_path = proc_dir / f"{self.source_name}.csv"
        # Ensure school_id is string type before writing to prevent integer inference on read
        if 'school_id' in kpi_df.columns:
            kpi_df = kpi_df.copy()
            kpi_df['school_id'] = kpi_df['school_id'].astype(str)
        kpi_df.to_csv(output_path, index=False)
        
        logger.info(f"KPI data written to {output_path}")
        logger.info(f"Demographic report written to {audit_path}")
        logger.info(f"Total KPI rows: {len(kpi_df)}, Total columns: {len(kpi_df.columns)}")
        
        # Log summary statistics
        if 'value' in kpi_df.columns:
            valid_values = kpi_df['value'].dropna()
            if len(valid_values) > 0:
                logger.info(f"KPI value range: {valid_values.min():.1f} - {valid_values.max():.1f}")
        
        # Log metric and demographic distribution
        if 'metric' in kpi_df.columns:
            metric_counts = kpi_df['metric'].value_counts()
            logger.info(f"Metrics created: {dict(metric_counts)}")
        
        if 'student_group' in kpi_df.columns:
            demo_counts = kpi_df['student_group'].value_counts()
            logger.info(f"Top 10 demographics: {dict(demo_counts.head(10))}")
        
        print(f"Wrote {output_path}")
        print(f"Demographic report: {audit_path}")