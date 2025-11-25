"""
Gifted and Talented Participation ETL Module

Handles Kentucky gifted and talented program participation data.
Normalizes column names, standardizes missing values, and transforms to KPI format.

Data includes participation counts by:
- Demographics (All Students, race/ethnicity, gender)
- Grade level (Preschool through Grade 12, Grade 14)
"""
from pathlib import Path
import pandas as pd
from typing import Dict, Any
import logging

import sys
from pathlib import Path

# Add etl directory to path for imports
etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from base_etl import BaseETL, Config

logger = logging.getLogger(__name__)


def clean_numeric_with_commas(series: pd.Series) -> pd.Series:
    """Convert strings with commas to numeric values."""
    cleaned = series.astype(str).str.replace(',', '').str.replace('"', '')
    return pd.to_numeric(cleaned, errors='coerce')


class GiftedTalentedETL(BaseETL):
    """ETL module for processing gifted and talented participation data."""

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            # Grade-level participation counts
            'All Grades': 'all_grades',
            'ALL GRADES': 'all_grades',
            'Preschool': 'preschool',
            'PRESCHOOL': 'preschool',
            'K': 'kindergarten',
            'KINDERGARTEN': 'kindergarten',
            'Grade 1': 'grade_1',
            'GRADE 1': 'grade_1',
            'GRADE1 COUNT': 'grade_1',
            'Grade 2': 'grade_2',
            'GRADE 2': 'grade_2',
            'GRADE2 COUNT': 'grade_2',
            'Grade 3': 'grade_3',
            'GRADE 3': 'grade_3',
            'GRADE3 COUNT': 'grade_3',
            'Grade 4': 'grade_4',
            'GRADE 4': 'grade_4',
            'GRADE4 COUNT': 'grade_4',
            'Grade 5': 'grade_5',
            'GRADE 5': 'grade_5',
            'GRADE5 COUNT': 'grade_5',
            'Grade 6': 'grade_6',
            'GRADE 6': 'grade_6',
            'GRADE6 COUNT': 'grade_6',
            'Grade 7': 'grade_7',
            'GRADE 7': 'grade_7',
            'GRADE7 COUNT': 'grade_7',
            'Grade 8': 'grade_8',
            'GRADE 8': 'grade_8',
            'GRADE8 COUNT': 'grade_8',
            'Grade 9': 'grade_9',
            'GRADE 9': 'grade_9',
            'GRADE9 COUNT': 'grade_9',
            'Grade 10': 'grade_10',
            'GRADE 10': 'grade_10',
            'GRADE10 COUNT': 'grade_10',
            'Grade 11': 'grade_11',
            'GRADE 11': 'grade_11',
            'GRADE11 COUNT': 'grade_11',
            'Grade 12': 'grade_12',
            'GRADE 12': 'grade_12',
            'GRADE12 COUNT': 'grade_12',
            'Grade 14': 'grade_14',
            'GRADE 14': 'grade_14',
            'GRADE14 COUNT': 'grade_14',
        }

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        metrics = {}

        # Grade levels to process
        grade_levels = [
            'all_grades', 'preschool', 'kindergarten',
            'grade_1', 'grade_2', 'grade_3', 'grade_4', 'grade_5', 'grade_6',
            'grade_7', 'grade_8', 'grade_9', 'grade_10', 'grade_11', 'grade_12',
            'grade_14'
        ]

        # Extract participation counts for each grade level
        for grade in grade_levels:
            value = row.get(grade, pd.NA)
            if pd.notna(value):
                # Clean comma-separated numbers
                value_clean = clean_numeric_with_commas(pd.Series([value])).iloc[0]
                if pd.notna(value_clean) and value_clean >= 0:
                    metric_name = f'gifted_participation_count_{grade}'
                    metrics[metric_name] = int(value_clean)

        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get default metrics for suppressed gifted participation records."""
        # Create default metrics for all grade levels
        defaults = {}
        grade_levels = [
            'all_grades', 'preschool', 'kindergarten',
            'grade_1', 'grade_2', 'grade_3', 'grade_4', 'grade_5', 'grade_6',
            'grade_7', 'grade_8', 'grade_9', 'grade_10', 'grade_11', 'grade_12',
            'grade_14'
        ]
        for grade in grade_levels:
            defaults[f'gifted_participation_count_{grade}'] = pd.NA
        return defaults

    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Override to include gifted/talented specific missing value handling."""
        # Apply base missing value standardization
        df = super().standardize_missing_values(df)

        # Handle comma-separated numbers in grade columns
        grade_columns = [
            'all_grades', 'preschool', 'kindergarten',
            'grade_1', 'grade_2', 'grade_3', 'grade_4', 'grade_5', 'grade_6',
            'grade_7', 'grade_8', 'grade_9', 'grade_10', 'grade_11', 'grade_12',
            'grade_14'
        ]
        for col in grade_columns:
            if col in df.columns and df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.replace(',', '')

        return df


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Process gifted/talented participation data and convert to KPI format using BaseETL."""
    etl = GiftedTalentedETL('gifted_talented')
    etl.process(raw_dir, proc_dir, cfg)


if __name__ == "__main__":
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
