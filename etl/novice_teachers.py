"""
Novice Teachers ETL Module

Processes Kentucky novice teacher data from two complementary files:
1. Institutional data showing teacher experience levels by school
2. EQUITY data showing which STUDENTS are taught by inexperienced teachers (by demographics and Title I status)

The equity file is especially valuable for understanding teacher quality gaps:
- Title I vs Non-Title I schools
- By race/ethnicity, economic status, disability status, English learner status

Data includes:
- Teacher counts by experience level (<1 year, 1-3 years)
- Percentage of students taught by inexperienced teachers (EQUITY METRIC)
- Demographics and Title I status breakdowns
"""
from pathlib import Path
import pandas as pd
from typing import Dict, Any
import logging
import sys

# Add etl directory to path for imports
etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from constants import KPI_COLUMNS
from base_etl import BaseETL, Config

logger = logging.getLogger(__name__)


class NoviceTeachersETL(BaseETL):
    """ETL module for processing novice teacher and teacher quality equity data."""
    
    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            # Institutional file columns
            'Teacher Count': 'teacher_count',
            'TEACHER COUNT': 'teacher_count',
            'Total New Teachers With 1 3 Years Experience': 'new_teachers_1_to_3_years',
            'TOTAL NEW TEACHERS WITH 1-3 YEARS EXPERIENCE': 'new_teachers_1_to_3_years',
            'Total New Teachers With Less Than 1 Year Experience': 'new_teachers_less_than_1_year',
            'TOTAL NEW TEACHERS WITH LESS THAN 1 YEAR EXPERIENCE': 'new_teachers_less_than_1_year',
            'Percent Of Teachers With 1 3 Years Experience': 'percent_new_teachers_1_to_3_years',
            'PERCENT OF TEACHERS WITH 1-3 YEARS EXPERIENCE': 'percent_new_teachers_1_to_3_years',
            'Percent Of Teachers With Less Than 1 Year Experience': 'percent_new_teachers_less_than_1_year',
            'PERCENT OF TEACHERS WITH LESS THAN 1 YEAR EXPERIENCE': 'percent_new_teachers_less_than_1_year',
            
            # Equity file columns (Title I status is a separate field)
            'Title_I_Status': 'title_i_status',
            'TITLE I STATUS': 'title_i_status',
            
            # Demographics - Standard (KYRC24/25)
            'All Students': 'all_students',
            'Non-White': 'non_white',
            'White': 'white',
            'Economically Disadvantaged': 'economically_disadvantaged',
            'Non-Economically Disadvantaged': 'non_economically_disadvantaged',
            'Students with Disabilities (IEP)': 'students_with_disabilities',
            'Student without Disabilities (IEP)': 'student_without_disabilities',
            'English Learner': 'english_learner',
            'Non-English Learner': 'non_english_learner',
            
            # Demographics - Historical (2020-2023)
            '% STUDENTS TAUGHT BY INEXPERIENCED TCHERS': 'all_students',
            '% NON-WHITE STUDENTS TAUGHT BY INEXPERIENCED TCHERS': 'non_white',
            '% WHITE STUDENTS TAUGHT BY INEXPERIENCED TCHRS': 'white',
            '% ECONOMICALLY DISADVANTAGED TAUGHT BY INEXPERIENCED TCHRS': 'economically_disadvantaged',
            '% NON-ECONOMICALLY DISADVANTAGED TAUGHT BY INEXPERIENCED TCHRS': 'non_economically_disadvantaged',
            '% STUDENTS WITH DISABILITIES TAUGHT BY INEXPERIENCED TCHRS': 'students_with_disabilities',
            '% NON-STUDENTS WITH DISABILITIES TAUGHT BY INEXPERIENCED TCHRS': 'student_without_disabilities',
            '% ENGLISH LEARNER STUDENTS TAUGHT BY INEXPERIENCED TCHRS': 'english_learner',
            '% NON-ENGLISH LEARNER STUDENTS TAUGHT BY INEXPERIENCED TCHRS': 'non_english_learner',
        }
    
    # Mapping from internal demographic keys to standard display names
    DEMOGRAPHIC_DISPLAY_MAP = {
        'all_students': 'All Students',
        'white': 'White (non-Hispanic)',
        'non_white': 'Non-White',
        'economically_disadvantaged': 'Economically Disadvantaged',
        'non_economically_disadvantaged': 'Non-Economically Disadvantaged',
        'students_with_disabilities': 'Students with Disabilities (IEP)',
        'student_without_disabilities': 'Student without Disabilities (IEP)',
        'english_learner': 'English Learner',
        'non_english_learner': 'Non-English Learner',
    }

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        metrics = {}
        
        # Helper function to safely convert to numeric
        def safe_numeric(value):
            if pd.isna(value):
                return pd.NA
            try:
                return pd.to_numeric(value, errors='coerce')
            except:
                return pd.NA
        
        # FILE 1: Institutional novice teacher percentages (school-level)
        percent_less_than_1 = safe_numeric(row.get('percent_new_teachers_less_than_1_year', pd.NA))
        if pd.notna(percent_less_than_1) and percent_less_than_1 >= 0:
            metrics['novice_teacher_rate_less_than_1_year'] = percent_less_than_1
        
        percent_1_to_3 = safe_numeric(row.get('percent_new_teachers_1_to_3_years', pd.NA))
        if pd.notna(percent_1_to_3) and percent_1_to_3 >= 0:
            metrics['novice_teacher_rate_1_to_3_years'] = percent_1_to_3
        
        # FILE 2: Equity metrics - students taught by inexperienced teachers
        # This file has a special structure with Title I status AND demographics
        
        # Get Title I status to determine which demographic columns to use
        title_i_status = row.get('title_i_status', '')
        
        # Process equity data based on Title I status
        if title_i_status in ['Title 1', 'Not Title 1', 'Equity Gap']:
            # These are percentage values showing what % of students are taught by inexperienced teachers
            
            # Create metric name based on Title I status
            title_i_suffix = title_i_status.lower().replace(' ', '_')
            
            # Process each demographic group column
            demographic_columns = {
                'all_students': 'all_students',
                'non_white': 'non_white',
                'white': 'white',
                'economically_disadvantaged': 'economically_disadvantaged',
                'non_economically_disadvantaged': 'non_economically_disadvantaged',
                'students_with_disabilities': 'students_with_disabilities',
                'student_without_disabilities': 'student_without_disabilities',
                'english_learner': 'english_learner',
                'non_english_learner': 'non_english_learner',
            }
            
            for col_name, demo_key in demographic_columns.items():
                value = safe_numeric(row.get(col_name, pd.NA))
                if pd.notna(value) and value >= 0:
                    # Use double underscore separator to identify equity metrics and separate demographic key
                    metric_name = f'students_taught_by_inexperienced_teachers_rate_{title_i_suffix}__{demo_key}'
                    metrics[metric_name] = value
        
        return metrics
    
    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get default metrics for suppressed novice teacher records."""
        defaults = {}
        
        # Institutional file defaults
        if 'percent_new_teachers_less_than_1_year' in row.index:
            defaults['novice_teacher_rate_less_than_1_year'] = pd.NA
        if 'percent_new_teachers_1_to_3_years' in row.index:
            defaults['novice_teacher_rate_1_to_3_years'] = pd.NA
        
        # Equity file defaults - based on Title I status
        title_i_status = row.get('title_i_status', '')
        if title_i_status in ['Title 1', 'Not Title 1', 'Equity Gap']:
            title_i_suffix = title_i_status.lower().replace(' ', '_')
            
            demographic_columns = [
                'all_students', 'non_white', 'white', 
                'economically_disadvantaged', 'non_economically_disadvantaged',
                'students_with_disabilities', 'student_without_disabilities',
                'english_learner', 'non_english_learner',
            ]
            
            for demo_key in demographic_columns:
                if demo_key in row.index:
                    # Use double underscore separator
                    metric_name = f'students_taught_by_inexperienced_teachers_rate_{title_i_suffix}__{demo_key}'
                    defaults[metric_name] = pd.NA
            
        return defaults

    def convert_to_kpi_format(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """
        Override base method to handle dynamic student_group assignment for equity metrics.
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
            
            # Special handling for suppressed records
            if not metrics and kpi_template['suppressed'] == 'Y':
                metrics = self.get_suppressed_metric_defaults(row)
            
            # Create KPI rows for each metric
            for metric_key, value in metrics.items():
                kpi_record = kpi_template.copy()
                
                # Check if this is an equity metric (has double underscore separator)
                if '__' in metric_key:
                    base_metric, demo_key = metric_key.split('__')
                    # Update metric name and student_group
                    kpi_record['metric'] = base_metric
                    kpi_record['student_group'] = self.DEMOGRAPHIC_DISPLAY_MAP.get(demo_key, 'All Students')
                else:
                    # Standard metric
                    kpi_record['metric'] = metric_key
                    # student_group remains as set in template (usually 'All Students' for institutional data)
                
                # Handle suppression and value assignment
                if kpi_template['suppressed'] == 'Y':
                    kpi_record['value'] = pd.NA
                    kpi_rows.append(kpi_record)
                else:
                    try:
                        if pd.notna(value) and value != '':
                            kpi_record['value'] = float(value)
                            kpi_rows.append(kpi_record)
                    except (ValueError, TypeError):
                        continue
        
        if not kpi_rows:
            logger.warning("No valid KPI rows created")
            return pd.DataFrame()
        
        # Create KPI DataFrame with consistent column order
        kpi_df = pd.DataFrame(kpi_rows)
        available_columns = [col for col in KPI_COLUMNS if col in kpi_df.columns]
        return kpi_df[available_columns]
    
    def should_skip_row(self, row: pd.Series) -> bool:
        """Skip rows that don't have novice teacher data."""
        # Check institutional file
        has_institutional = pd.notna(row.get('teacher_count', pd.NA))
        
        # Check equity file
        has_equity = row.get('title_i_status', '') in ['Title 1', 'Not Title 1', 'Equity Gap']
        
        # If neither file type is present, skip
        if not has_institutional and not has_equity:
            return True
            
        return False  # Don't call super() - may not have demographics in institutional file


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Read novice teacher files, normalize, and convert to KPI format."""
    etl = NoviceTeachersETL('novice_teachers')
    etl.process(raw_dir, proc_dir, cfg)


def main():
    """Run novice teachers ETL process."""
    import logging
    logging.basicConfig(level=logging.INFO)
    
    from pathlib import Path
    raw_dir = Path(__file__).parent.parent / "data" / "raw"
    proc_dir = Path(__file__).parent.parent / "data" / "processed"
    proc_dir.mkdir(exist_ok=True)
    
    test_config = Config(
        derive={"processing_date": "2025-11-24", "data_quality_flag": "reviewed"}
    ).dict()

    transform(raw_dir, proc_dir, test_config)


if __name__ == "__main__":
    main()
