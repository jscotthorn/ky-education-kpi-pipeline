"""Out-of-School Suspension ETL Module

Refactored to leverage :class:`BaseETL` for common processing.
Handles two different measures that share this directory:

- KYRC24, KYRC25, and KYRC26 ``OVW_Student_Suspensions`` files are the
  biennial Civil Rights Data Collection (CRDC) counts: in-school and
  out-of-school suspensions for students with and without disabilities.
  KDE's report-card glossary calls this "Student Suspensions (CRDC)".
  ``School Year`` is the report-card year. ``Collected School Year`` is
  the CRDC year and is not the KPI year.
- ``safe_schools_discipline_YYYY`` files are the annual Safe Schools
  resolution ``OUT OF SCHOOL SUSPENSION SSP3``. ``All Students`` on that
  file is a student count. ``Total Events`` is the incident count and is
  skipped because it is not a demographic.
"""
from pathlib import Path
from typing import Any, Dict
import pandas as pd
import logging
import sys

# Ensure local imports work when running as script
etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from base_etl import BaseETL
try:
    from constants import KPI_COLUMNS
except ImportError:
    KPI_COLUMNS = []

logger = logging.getLogger(__name__)

# Tokens that mean the cell was empty, not a privacy star.
_BLANK_CELL_TOKENS = {"", "nan", "none", "<na>", "nat"}


def _cell_is_blank(value: Any) -> bool:
    """True when a measure cell is empty.

    A ``*`` is suppression, not a blank. ``bool(pd.NA)`` raises, so missing
    values go through ``pd.isna`` and this function never evaluates a NA as
    a boolean.
    """
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().lower() in _BLANK_CELL_TOKENS
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


class OutOfSchoolSuspensionETL(BaseETL):
    """ETL module for out-of-school suspension data."""

    # KYRC26 headers match KYRC25. Opt in in the pull request that accepts
    # the file. The KPI year stays on School Year.
    include_kyrc26 = True

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

    def _measure_columns(self, df: pd.DataFrame) -> list:
        """Return count columns this module knows how to read."""
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
        return [
            column for column in df.columns
            if any(keyword in column.lower() for keyword in numeric_keywords)
        ]

    def _drop_blank_measure_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop a row whose measure cells are all empty.

        notes/8: a row that is not suppressed and has every value empty is
        not offered. These files have no Suppressed column. A ``*`` is a
        privacy suppression and stays. KYRC24, KYRC25, KYRC26, and the
        2020-2023 files have no all-blank measure row, so this does not
        change their counts.
        """
        measure_columns = self._measure_columns(df)
        if not measure_columns or df.empty:
            return df
        blank_row = pd.Series(True, index=df.index)
        for column in measure_columns:
            blank_row = blank_row & df[column].map(_cell_is_blank)
        if not bool(blank_row.any()):
            return df
        return df.loc[~blank_row].copy()

    def _numeric_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove commas and convert numeric columns to numeric types.

        ``read_csv(..., dtype=str)`` is StringDtype on pandas 3 and object
        on pandas 2.3.3. This cleaner does not check ``dtype == object``.
        ``astype(str)`` then ``str.replace`` keeps a count such as
        ``1,300`` on both. The registered files have no thousands
        separators. pandas is unpinned.
        """
        for col in self._measure_columns(df):
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(',', '', regex=False)
                .replace('nan', pd.NA)
            )
            df[col] = pd.to_numeric(df[col], errors='coerce')
        return df

    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize missing values and create suppression indicator."""
        df = self._drop_blank_measure_rows(df)
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
            # Annual Safe Schools SSP3 column. A fully starred CRDC row does
            # not take this branch: those files have the single/multiple
            # columns and stay in the branch above.
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
        # Use standard KPI columns, only including those that exist
        available_columns = [col for col in KPI_COLUMNS if col in kpi_df.columns]
        return kpi_df[available_columns]


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Entry point for pipeline execution."""
    etl = OutOfSchoolSuspensionETL('out_of_school_suspension')
    etl.process(raw_dir, proc_dir, cfg)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    project_root = Path(__file__).parent.parent
    raw_dir = project_root / 'data' / 'raw'
    proc_dir = project_root / 'data' / 'processed'
    proc_dir.mkdir(exist_ok=True)
    transform(raw_dir, proc_dir, {'derive': {}})
