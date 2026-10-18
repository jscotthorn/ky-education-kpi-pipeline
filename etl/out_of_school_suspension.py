"""Out-of-School Suspension ETL Module

Refactored to leverage :class:`BaseETL` for common processing.

This module reads the biennial Civil Rights Data Collection (CRDC) file
``OVW_Student_Suspensions``. KDE's report-card glossary calls it Student
Suspensions (CRDC): in-school and out-of-school suspension counts for
students with and without disabilities.

The KPI year is the report-card year in ``School Year``. It is not the
CRDC collection year.

- Report-card year 2024 (``KYRC24``, ``School Year`` ``20232024``) is the
  2020-21 collection. ``Collected School Year`` is ``20202021`` on every
  row. That column is not in the KPI output.
- Report-card year 2026 (``KYRC26``, ``School Year`` ``20252026``) is the
  2021-22 collection. ``Collected School Year`` is ``20212022`` on every
  row. That column is not in the KPI output.

``KYRC25_OVW_Student_Suspensions.csv`` is the same 2020-21 collection
with ``School Year`` set to ``20242025``. It is not registered, and
:meth:`accepts_data_file` skips it.

The annual Safe Schools files ``safe_schools_discipline_YYYY.csv`` are
not read here. ``safe_schools_discipline`` has published
``out_of_school_suspension_count`` from ``OUT OF SCHOOL SUSPENSION SSP3``
since commit ``e7d815a`` (4 Aug 2025).
"""
from pathlib import Path
from typing import Any, Dict, Sequence
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

# The six CRDC count columns. Totals are computed from these.
_CRDC_COUNT_COLUMNS = (
    'in_school_with_disabilities',
    'in_school_without_disabilities',
    'single_out_of_school_with_disabilities',
    'single_out_of_school_without_disabilities',
    'multiple_out_of_school_with_disabilities',
    'multiple_out_of_school_without_disabilities',
)

# KYRC25 republishes the KYRC24 CRDC 2020-21 extract under a later
# report-card year. The filename guard rejects this name.
_KYRC25_REPUBLISH = "KYRC25_OVW_STUDENT_SUSPENSIONS.CSV"


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


def _sum_unless_suppressed(parts: Sequence[Any]) -> Any:
    """Sum components. Any missing component makes the total missing.

    A ``*`` is NA after missing-value standardization. Summing only the
    published parts is not the file's count: when every part is starred the
    sum of an empty set is 0, and when one part is starred the sum is the
    other part with a withheld student missing. notes/8: that total is
    suppressed. ``pd.isna`` is used so ``pd.NA`` is never treated as a
    boolean.
    """
    for part in parts:
        if pd.isna(part):
            return pd.NA
    return sum(parts)


class OutOfSchoolSuspensionETL(BaseETL):
    """ETL module for CRDC out-of-school suspension data."""

    # KYRC26 headers match the CRDC extract this module already maps.
    # The KPI year stays on School Year.
    include_kyrc26 = True

    def accepts_data_file(self, file_path: Path) -> bool:
        """Skip a republished CRDC extract and the annual Safe Schools files.

        ``KYRC25_OVW_Student_Suspensions.csv`` republishes CRDC 2020-21.
        It matches the datasets copy of KYRC24 apart from ``School Year``
        (collected year stays ``20202021``, and the same cells are starred).
        Registering it would publish those counts again as 2025.

        ``safe_schools_discipline_YYYY.csv`` is the annual Safe Schools
        file. ``safe_schools_discipline`` owns ``OUT OF SCHOOL SUSPENSION
        SSP3`` since ``e7d815a``. ``kpi_master`` concatenates without
        deduping, so reading the file here duplicated that count.
        """
        name = file_path.name
        upper = name.upper()
        if upper == _KYRC25_REPUBLISH:
            logger.info(
                "Skipping %s; it republishes CRDC 2020-21 under School Year 20242025",
                name,
            )
            return False
        if upper.startswith("SAFE_SCHOOLS_DISCIPLINE_"):
            logger.info(
                "Skipping %s; safe_schools_discipline owns SSP3 since e7d815a",
                name,
            )
            return False
        return super().accepts_data_file(file_path)

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
        }

    def _measure_columns(self, df: pd.DataFrame) -> list:
        """Return the CRDC count columns present on this frame."""
        return [column for column in _CRDC_COUNT_COLUMNS if column in df.columns]

    def _drop_blank_measure_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop a row whose measure cells are all empty.

        notes/8: a row that is not suppressed and has every value empty is
        not offered. These files have no Suppressed column. A ``*`` is a
        privacy suppression and stays. KYRC24 and KYRC26 have no all-blank
        measure row, so this does not change their counts.
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
        separators. A ``*`` becomes NA here. pandas is unpinned.
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
            suspension_cols = self._measure_columns(df)
            if suspension_cols:
                df['suppressed'] = df[suspension_cols].isna().all(axis=1).map({True: 'Y', False: 'N'})
            else:
                df['suppressed'] = 'N'
        else:
            mapping = {
                'Yes': 'Y', 'Y': 'Y', '1': 'Y', True: 'Y',
                'No': 'N', 'N': 'N', '0': 'N', False: 'N'
            }
            df['suppressed'] = df['suppressed'].map(mapping).fillna('N')
        return df

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        metrics: Dict[str, Any] = {}
        if 'single_out_of_school_with_disabilities' not in row.index:
            return metrics

        swd = row.get('single_out_of_school_with_disabilities')
        swod = row.get('single_out_of_school_without_disabilities')
        mwd = row.get('multiple_out_of_school_with_disabilities')
        mwod = row.get('multiple_out_of_school_without_disabilities')

        single_total = _sum_unless_suppressed([swd, swod])
        multiple_total = _sum_unless_suppressed([mwd, mwod])
        metrics = {
            'out_of_school_suspension_single_with_disabilities_count': swd,
            'out_of_school_suspension_single_without_disabilities_count': swod,
            'out_of_school_suspension_multiple_with_disabilities_count': mwd,
            'out_of_school_suspension_multiple_without_disabilities_count': mwod,
            'out_of_school_suspension_single_total_count': single_total,
            'out_of_school_suspension_multiple_total_count': multiple_total,
            'out_of_school_suspension_total_count': _sum_unless_suppressed(
                [single_total, multiple_total]
            ),
        }

        if 'in_school_with_disabilities' in row.index or 'in_school_without_disabilities' in row.index:
            in_swd = row.get('in_school_with_disabilities') if 'in_school_with_disabilities' in row.index else pd.NA
            in_swod = row.get('in_school_without_disabilities') if 'in_school_without_disabilities' in row.index else pd.NA
            metrics['in_school_suspension_with_disabilities_count'] = in_swd
            metrics['in_school_suspension_without_disabilities_count'] = in_swod
            metrics['in_school_suspension_total_count'] = _sum_unless_suppressed([in_swd, in_swod])

        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        if 'single_out_of_school_with_disabilities' not in row.index:
            return {}
        return {
            'out_of_school_suspension_single_with_disabilities_count': pd.NA,
            'out_of_school_suspension_single_without_disabilities_count': pd.NA,
            'out_of_school_suspension_multiple_with_disabilities_count': pd.NA,
            'out_of_school_suspension_multiple_without_disabilities_count': pd.NA,
            'out_of_school_suspension_single_total_count': pd.NA,
            'out_of_school_suspension_multiple_total_count': pd.NA,
            'out_of_school_suspension_total_count': pd.NA,
            'in_school_suspension_with_disabilities_count': pd.NA,
            'in_school_suspension_without_disabilities_count': pd.NA,
            'in_school_suspension_total_count': pd.NA,
        }

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
