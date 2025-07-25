"""Out-of-School Suspension ETL Module

Refactored to leverage :class:`BaseETL` for common processing.
Handles both the newer KYRC24 format and historical Safe Schools format
and outputs standardized KPI metrics.
"""
from pathlib import Path
from typing import Dict, Any
import pandas as pd
import logging
import sys

# Ensure local imports work when running as script
etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from base_etl import BaseETL

logger = logging.getLogger(__name__)


class OutOfSchoolSuspensionETL(BaseETL):
    """ETL module for out-of-school suspension data."""

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            'Collected School Year': 'collected_school_year',
            'In-School With Disabilities': 'in_school_with_disabilities',
            'In-School Without Disabilities': 'in_school_without_disabilities',
            'Single Out-of-School With Disabilities': 'single_out_of_school_with_disabilities',
            'Single Out-of-School Without Disabilities': 'single_out_of_school_without_disabilities',
            'Multiple Out-of-School With Disabilities': 'multiple_out_of_school_with_disabilities',
            'Multiple Out-of-School Without Disabilities': 'multiple_out_of_school_without_disabilities',
            'Total Discipline Resolutions': 'total_discipline_resolutions',
            'Expelled Receiving Services SSP1': 'expelled_receiving_services',
            'Expelled Not Receiving Services SSP2': 'expelled_not_receiving_services',
            'Out of School Suspension SSP3': 'out_of_school_suspension',
            'OUT OF SCHOOL SUSPENSION SSP3': 'out_of_school_suspension',
            'Corporal Punishment SSP5': 'corporal_punishment',
            'In-School Removal INSR': 'in_school_removal',
            'Restraint SSP7': 'restraint',
            'Seclusion SSP8': 'seclusion',
            'Unilateral Removal by School Personnel IAES1': 'unilateral_removal',
            'Removal by Hearing Officer IAES2': 'removal_by_hearing_officer',
        }

    def _numeric_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove commas and convert numeric columns to numeric types."""
        numeric_keywords = [
            'out_of_school',
            'in_school',
            'expelled',
            'corporal_punishment',
            'restraint',
            'seclusion',
            'unilateral_removal',
            'removal_by_hearing_officer',
            'total_discipline_resolutions',
        ]
        numeric_cols = [
            c for c in df.columns if any(k in c.lower() for k in numeric_keywords)
        ]
        for col in numeric_cols:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(',', '')
                .replace('nan', pd.NA)
            )
            df[col] = pd.to_numeric(df[col], errors='coerce')
        return df

    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize missing values and create suppression indicator."""
        df = super().standardize_missing_values(df)
        df = self._numeric_clean(df)

        if 'suppressed' not in df.columns:
            suspension_cols = [c for c in df.columns if 'out_of_school' in c.lower()]
            df['suppressed'] = df[suspension_cols].isna().all(axis=1).map({True: 'Y', False: 'N'})
        else:
            mapping = {
                'Yes': 'Y', 'Y': 'Y', '1': 'Y', True: 'Y',
                'No': 'N', 'N': 'N', '0': 'N', False: 'N'
            }
            df['suppressed'] = df['suppressed'].map(mapping).fillna('N')
        return df

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        metrics: Dict[str, Any] = {}
        if 'single_out_of_school_with_disabilities' in row.index:
            # KYRC24 format
            swd = row.get('single_out_of_school_with_disabilities')
            swod = row.get('single_out_of_school_without_disabilities')
            mwd = row.get('multiple_out_of_school_with_disabilities')
            mwod = row.get('multiple_out_of_school_without_disabilities')

            metrics = {
                'out_of_school_suspension_single_with_disabilities_count': swd,
                'out_of_school_suspension_single_without_disabilities_count': swod,
                'out_of_school_suspension_multiple_with_disabilities_count': mwd,
                'out_of_school_suspension_multiple_without_disabilities_count': mwod,
            }

            single_total = sum(v for v in [swd, swod] if pd.notna(v))
            multiple_total = sum(v for v in [mwd, mwod] if pd.notna(v))
            metrics.update({
                'out_of_school_suspension_single_total_count': single_total,
                'out_of_school_suspension_multiple_total_count': multiple_total,
                'out_of_school_suspension_total_count': single_total + multiple_total,
            })
        elif 'out_of_school_suspension' in row.index:
            metrics = {
                'out_of_school_suspension_count': row.get('out_of_school_suspension')
            }

        # Additional counts available in both formats
        in_swd = row.get('in_school_with_disabilities') if 'in_school_with_disabilities' in row.index else None
        in_swod = row.get('in_school_without_disabilities') if 'in_school_without_disabilities' in row.index else None
        if 'in_school_with_disabilities' in row.index or 'in_school_without_disabilities' in row.index:
            metrics['in_school_suspension_with_disabilities_count'] = in_swd
            metrics['in_school_suspension_without_disabilities_count'] = in_swod
            ins_total = sum(v for v in [in_swd, in_swod] if pd.notna(v))
            metrics['in_school_suspension_total_count'] = ins_total

        count_map = {
            'expelled_receiving_services': 'expelled_receiving_services_count',
            'expelled_not_receiving_services': 'expelled_not_receiving_services_count',
            'corporal_punishment': 'corporal_punishment_count',
            'in_school_removal': 'in_school_removal_count',
            'restraint': 'restraint_count',
            'seclusion': 'seclusion_count',
            'unilateral_removal': 'unilateral_removal_count',
            'removal_by_hearing_officer': 'removal_by_hearing_officer_count',
            'total_discipline_resolutions': 'discipline_resolution_total_count',
        }
        for src, metric_name in count_map.items():
            if src in row.index:
                metrics[metric_name] = row.get(src)

        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        if 'single_out_of_school_with_disabilities' in row.index:
            defaults = {
                'out_of_school_suspension_single_with_disabilities_count': pd.NA,
                'out_of_school_suspension_single_without_disabilities_count': pd.NA,
                'out_of_school_suspension_multiple_with_disabilities_count': pd.NA,
                'out_of_school_suspension_multiple_without_disabilities_count': pd.NA,
                'out_of_school_suspension_single_total_count': pd.NA,
                'out_of_school_suspension_multiple_total_count': pd.NA,
                'out_of_school_suspension_total_count': pd.NA,
            }
        else:
            defaults = {'out_of_school_suspension_count': pd.NA}

        extra_defaults = [
            'in_school_suspension_with_disabilities_count',
            'in_school_suspension_without_disabilities_count',
            'in_school_suspension_total_count',
            'expelled_receiving_services_count',
            'expelled_not_receiving_services_count',
            'corporal_punishment_count',
            'in_school_removal_count',
            'restraint_count',
            'seclusion_count',
            'unilateral_removal_count',
            'removal_by_hearing_officer_count',
            'discipline_resolution_total_count',
        ]

        for metric in extra_defaults:
            defaults[metric] = pd.NA

        return defaults

    def convert_to_kpi_format(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """Override to apply metric-level suppression handling."""
        kpi_records = []
        for _, row in df.iterrows():
            if self.should_skip_row(row):
                continue
            kpi_template = self.create_kpi_template(row, source_file)
            metrics = self.extract_metrics(row)
            if not metrics and row.get('suppressed') == 'Y':
                metrics = self.get_suppressed_metric_defaults(row)
            for metric, value in metrics.items():
                record = kpi_template.copy()
                try:
                    if pd.isna(value):
                        raise ValueError('missing')
                    numeric_val = float(value)
                    if numeric_val < 0:
                        raise ValueError('negative')
                    record['value'] = numeric_val
                    record['suppressed'] = 'N'
                except Exception:
                    record['value'] = pd.NA
                    record['suppressed'] = 'Y'
                record['metric'] = metric
                kpi_records.append(record)
        if not kpi_records:
            return pd.DataFrame()
        kpi_df = pd.DataFrame(kpi_records)
        columns = [
            'district', 'school_id', 'school_name', 'year', 'student_group',
            'metric', 'value', 'suppressed', 'source_file', 'last_updated'
        ]
        return kpi_df[columns]


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Entry point for pipeline execution."""
    etl = OutOfSchoolSuspensionETL('out_of_school_suspension')
    etl.transform(raw_dir, proc_dir, cfg)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    project_root = Path(__file__).parent.parent
    raw_dir = project_root / 'data' / 'raw'
    proc_dir = project_root / 'data' / 'processed'
    proc_dir.mkdir(exist_ok=True)
    transform(raw_dir, proc_dir, {'derive': {}})
