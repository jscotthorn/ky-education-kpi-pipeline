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
                for period in {grade_period, level_period}:
                    if period == 'all' or period == 'all_grades':
                        continue
                    try:
                        metrics[f'{subject}_{name}_{period}'] = float(value)
                    except (ValueError, TypeError):
                        metrics[f'{subject}_{name}_{period}'] = pd.NA

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

        for period in {grade_period, level_period}:
            if period == 'all' or period == 'all_grades':
                continue
            metrics[f'{subject}_novice_rate_{period}'] = pd.NA
            metrics[f'{subject}_apprentice_rate_{period}'] = pd.NA
            metrics[f'{subject}_proficient_rate_{period}'] = pd.NA
            metrics[f'{subject}_distinguished_rate_{period}'] = pd.NA
            metrics[f'{subject}_proficient_distinguished_rate_{period}'] = pd.NA
            if 'content_index' in row:
                metrics[f'{subject}_content_index_score_{period}'] = pd.NA

        return metrics

    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        df = super().standardize_missing_values(df)
        numeric_cols = [
            'novice',
            'apprentice',
            'proficient',
            'distinguished',
            'proficient_distinguished',
            'content_index',
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                invalid = (df[col] < 0) | (df[col] > 100)
                if invalid.any():
                    logger.warning(
                        f'Found {invalid.sum()} invalid values in {col}'
                    )
                    df.loc[invalid, col] = pd.NA
        return df

def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Entry point for pipeline."""
    etl = KentuckySummativeAssessmentETL('kentucky_summative_assessment')
    etl.transform(raw_dir, proc_dir, cfg)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    root = Path(__file__).parent.parent
    raw_dir = root / 'data' / 'raw'
    proc_dir = root / 'data' / 'processed'
    proc_dir.mkdir(exist_ok=True)
    transform(raw_dir, proc_dir, {'derive': {}})
