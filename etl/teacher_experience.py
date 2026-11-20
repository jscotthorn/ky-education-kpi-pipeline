"""
Teacher Experience ETL Module

Processes Kentucky teacher experience data showing average years of teaching
experience by school/district. This is an institutional-level metric without
demographic breakdowns.

Data includes:
- Educator count per institution
- Average years of teaching experience
- School-level and district-level aggregations
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


class TeacherExperienceETL(BaseETL):
    """ETL module for processing teacher experience data."""
    
    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            'Educator Count': 'educator_count',
            'EDUCATOR COUNT': 'educator_count',
            'Average Years of Experience': 'average_years_experience',
            'AVERAGE YEARS OF EXPERIENCE': 'average_years_experience',
            # Historical xlsx format (2018-19) - SCHOOL_EXPERIENCE
            'SCH_YEAR': 'school_year',
            'CNTYNO': 'county_number',
            'CNTYNAME': 'county_name',
            'DIST_NUMBER': 'district_number',
            'DIST_NAME': 'district_name',
            'SCH_NUMBER': 'school_number',
            'SCH_NAME': 'school_name',
            'SCH_CD': 'school_code',
            'STATE_SCH_ID': 'state_school_id',
            'NCESID': 'nces_id',
            'COOP': 'co_op',
            'COOP_CODE': 'co_op_code',
            'TOTEXP': 'total_experience',  # Total years * teachers
            'CNTEXP': 'educator_count',  # Count of teachers
            'AVGEXPERIENCEYEARS': 'average_years_experience',
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
        
        # Average years of experience (primary metric)
        avg_years = safe_numeric(row.get('average_years_experience', pd.NA))
        if pd.notna(avg_years) and avg_years > 0:
            metrics['teacher_average_years_experience'] = avg_years
        
        return metrics
    
    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get default metrics for suppressed teacher experience records."""
        defaults = {}
        
        # Only create defaults for metrics that exist in the source data
        if 'average_years_experience' in row.index:
            defaults['teacher_average_years_experience'] = pd.NA
            
        return defaults
    
    def _parse_numeric_with_commas(self, value) -> float:
        """Parse numeric values that may contain comma formatting (e.g., '2,984')."""
        if pd.isna(value):
            return pd.NA
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            # Remove commas and whitespace before converting
            cleaned = value.replace(',', '').strip()
            try:
                return float(cleaned)
            except ValueError:
                return pd.NA
        return pd.NA

    def should_skip_row(self, row: pd.Series) -> bool:
        """Skip rows that don't have teacher experience data."""
        # Check if we have educator count (denominator)
        # Handle comma-formatted numbers like '2,984' for large districts
        educator_count_raw = row.get('educator_count', pd.NA)
        educator_count = self._parse_numeric_with_commas(educator_count_raw)
        if pd.isna(educator_count) or educator_count == 0:
            return True

        # Check if we have average years
        avg_years = row.get('average_years_experience', pd.NA)
        if pd.isna(avg_years):
            return True

        return False  # Don't call super() - institutional data has no demographics


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Read teacher experience files, normalize, and convert to KPI format."""
    etl = TeacherExperienceETL('teacher_experience')
    etl.process(raw_dir, proc_dir, cfg)


def main():
    """Run teacher experience ETL process."""
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
