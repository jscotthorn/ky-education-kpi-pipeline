"""Kentucky Summative Assessment ETL module."""
from pathlib import Path
from typing import Dict, Any, Union
import pandas as pd
import logging
import sys
from pydantic import BaseModel

etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from base_etl import BaseETL

logger = logging.getLogger(__name__)


class Config(BaseModel):
    rename: Dict[str, str] = {}
    dtype: Dict[str, str] = {}
    derive: Dict[str, Union[str, int, float]] = {}


class KentuckySummativeAssessmentETL(BaseETL):
    """ETL for processing Kentucky Summative Assessment data."""
    
    def convert_to_kpi_format(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """Optimized KPI conversion for large KSA datasets."""
        if df.empty:
            return pd.DataFrame()
        
        logger.info(f"Converting {len(df)} rows to KPI format using chunked processing")
        
        # Process in chunks to manage memory usage
        chunk_size = 50000  # Process 50K rows at a time for better performance
        kpi_chunks = []
        
        import time
        start_time = time.time()
        
        for chunk_idx, start_idx in enumerate(range(0, len(df), chunk_size)):
            end_idx = min(start_idx + chunk_size, len(df))
            chunk_df = df.iloc[start_idx:end_idx].copy()
            
            chunk_start = time.time()
            logger.info(f"Processing chunk {chunk_idx + 1}: rows {start_idx:,} to {end_idx:,}")
            
            # Use parent class method for this chunk
            chunk_kpi = super().convert_to_kpi_format(chunk_df, source_file)
            
            if not chunk_kpi.empty:
                kpi_chunks.append(chunk_kpi)
            
            chunk_time = time.time() - chunk_start
            logger.info(f"Chunk {chunk_idx + 1} completed in {chunk_time:.1f}s, created {len(chunk_kpi):,} KPI rows")
        
        if kpi_chunks:
            result = pd.concat(kpi_chunks, ignore_index=True)
            logger.info(f"Generated {len(result):,} KPI rows from {len(df):,} input rows")
            return result
        else:
            return pd.DataFrame()

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            'Level': 'level',
            'LEVEL': 'level',
            'Subject': 'subject',
            'SUBJECT': 'subject',
            'Novice': 'novice',
            'NOVICE': 'novice',
            'Apprentice': 'apprentice',
            'APPRENTICE': 'apprentice',
            'Proficient': 'proficient',
            'PROFICIENT': 'proficient',
            'Distinguished': 'distinguished',
            'DISTINGUISHED': 'distinguished',
            'Proficient / Distinguished': 'proficient_distinguished',
            'PROFICIENT/DISTINGUISHED': 'proficient_distinguished',
            'Content Index': 'content_index',
            'CONTENT INDEX': 'content_index',
        }

    def _normalize_subject(self, value: Any) -> str:
        mapping = {
            'MA': 'math',
            'Mathematics': 'math',
            'RD': 'reading',
            'Reading': 'reading',
            'SC': 'science',
            'Science': 'science',
            'SS': 'social_studies',
            'Social Studies': 'social_studies',
            'WR': 'writing',
            'Writing': 'writing',
        }
        if value is None or pd.isna(value) or value == '':
            return 'unknown_subject'
        val = str(value).strip()
        return mapping.get(val, val.lower().replace(' ', '_'))

    def _normalize_grade(self, grade: Any) -> str:
        if grade is None or pd.isna(grade) or str(grade).strip() == '':
            return 'all_grades'
        g = str(grade).strip()
        if g.isdigit():
            return f'grade_{int(g)}'
        if g.lower().startswith('grade '):
            return g.lower().replace('grade ', 'grade_')
        return g.lower().replace(' ', '_')

    def _normalize_level(self, level: Any) -> str:
        mapping = {
            'ES': 'elementary',
            'MS': 'middle',
            'HS': 'high',
            'Elementary School': 'elementary',
            'Middle School': 'middle',
            'High School': 'high',
            'Elementary': 'elementary',
            'Middle': 'middle',
            'High': 'high',
        }
        if level is None or pd.isna(level) or str(level).strip() == '':
            return 'all'
        lv = str(level).strip()
        return mapping.get(lv, lv.lower().replace(' ', '_'))

    def _grade_to_level(self, grade: str) -> str:
        if grade.startswith('grade_'):
            try:
                num = int(grade.split('_')[1])
                if num >= 9:
                    return 'high'
                if num >= 6:
                    return 'middle'
                return 'elementary'
            except ValueError:
                return 'all'
        if grade in {'kindergarten', 'pre_k', 'preschool'}:
            return 'elementary'
        return 'all'

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        subject = self._normalize_subject(row.get('subject'))
        grade_period = self._normalize_grade(row.get('grade'))
        level_period = self._normalize_level(row.get('level'))
        if level_period == 'all' and grade_period != 'all_grades':
            level_period = self._grade_to_level(grade_period)

        metrics: Dict[str, Any] = {}

        def add(name: str, value: Any) -> None:
            if pd.notna(value):
                # Always include grade-level metrics if we have grade data
                if grade_period != 'all_grades':
                    try:
                        metrics[f'kentucky_summative_assessment_{subject}_{name}_{grade_period}'] = float(value)
                    except (ValueError, TypeError):
                        metrics[f'kentucky_summative_assessment_{subject}_{name}_{grade_period}'] = pd.NA
                
                # Include level-based metrics for backwards compatibility and aggregation
                if level_period != 'all':
                    try:
                        metrics[f'kentucky_summative_assessment_{subject}_{name}_{level_period}'] = float(value)
                    except (ValueError, TypeError):
                        metrics[f'kentucky_summative_assessment_{subject}_{name}_{level_period}'] = pd.NA

        add('novice_rate', row.get('novice'))
        add('apprentice_rate', row.get('apprentice'))
        add('proficient_rate', row.get('proficient'))
        add('distinguished_rate', row.get('distinguished'))
        add('proficient_distinguished_rate', row.get('proficient_distinguished'))
        if 'content_index' in row:
            add('content_index_score', row.get('content_index'))

        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        subject = self._normalize_subject(row.get('subject'))
        grade_period = self._normalize_grade(row.get('grade'))
        level_period = self._normalize_level(row.get('level'))
        if level_period == 'all' and grade_period != 'all_grades':
            level_period = self._grade_to_level(grade_period)

        metrics: Dict[str, Any] = {}

        # Always include grade-level metrics if we have grade data
        if grade_period != 'all_grades':
            metrics[f'kentucky_summative_assessment_{subject}_novice_rate_{grade_period}'] = pd.NA
            metrics[f'kentucky_summative_assessment_{subject}_apprentice_rate_{grade_period}'] = pd.NA
            metrics[f'kentucky_summative_assessment_{subject}_proficient_rate_{grade_period}'] = pd.NA
            metrics[f'kentucky_summative_assessment_{subject}_distinguished_rate_{grade_period}'] = pd.NA
            metrics[f'kentucky_summative_assessment_{subject}_proficient_distinguished_rate_{grade_period}'] = pd.NA
            if 'content_index' in row:
                metrics[f'kentucky_summative_assessment_{subject}_content_index_score_{grade_period}'] = pd.NA

        # Include level-based metrics for backwards compatibility and aggregation
        if level_period != 'all':
            metrics[f'kentucky_summative_assessment_{subject}_novice_rate_{level_period}'] = pd.NA
            metrics[f'kentucky_summative_assessment_{subject}_apprentice_rate_{level_period}'] = pd.NA
            metrics[f'kentucky_summative_assessment_{subject}_proficient_rate_{level_period}'] = pd.NA
            metrics[f'kentucky_summative_assessment_{subject}_distinguished_rate_{level_period}'] = pd.NA
            metrics[f'kentucky_summative_assessment_{subject}_proficient_distinguished_rate_{level_period}'] = pd.NA
            if 'content_index' in row:
                metrics[f'kentucky_summative_assessment_{subject}_content_index_score_{level_period}'] = pd.NA

        return metrics

    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        df = super().standardize_missing_values(df)
        # Percentage columns (0-100 range)
        percentage_cols = [
            'novice',
            'apprentice',
            'proficient',
            'distinguished',
            'proficient_distinguished',
        ]
        for col in percentage_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                invalid = (df[col] < 0) | (df[col] > 100)
                if invalid.any():
                    logger.warning(
                        f'Found {invalid.sum()} invalid percentage values in {col}'
                    )
                    df.loc[invalid, col] = pd.NA
        
        # Content index column (scale score, not percentage - can exceed 100)
        if 'content_index' in df.columns:
            df['content_index'] = pd.to_numeric(df['content_index'], errors='coerce')
            # Only validate for negative values (scale scores should be positive)
            invalid = df['content_index'] < 0
            if invalid.any():
                logger.warning(
                    f'Found {invalid.sum()} invalid content index values (negative scores)'
                )
                df.loc[invalid, 'content_index'] = pd.NA
        return df

def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Entry point for pipeline."""
    etl = KentuckySummativeAssessmentETL('kentucky_summative_assessment')
    etl.process(raw_dir, proc_dir, cfg)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    root = Path(__file__).parent.parent
    raw_dir = root / 'data' / 'raw'
    proc_dir = root / 'data' / 'processed'
    proc_dir.mkdir(exist_ok=True)
    transform(raw_dir, proc_dir, {'derive': {}})
