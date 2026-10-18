"""
Secondary Enrollment ETL Module

Students with a secondary (concurrent) enrollment at this school, by grade,
for 2019-2026. A secondary enrollment is a concurrent second enrollment at a
school other than the student's main school, mostly at career and technical
centers and regional academies. KDE pairs this series with primary enrollment.

All Grades counts unique students in every grade. KYRC files publish only
grades 6-12 and 14, so All Grades can exceed those cells. Older files had
preschool through grade 5 columns (always 0), and their totals equal the
grade sums for 2019-2024. KYRC25 has 37 rows where the total is higher
(state gap 3). KYRC26 has 47 (state gap 63: East Jessamine Middle 61,
Tates Creek Middle 1, Camden Station Elementary 1). Those students are
most likely below grade 6, which is unproven. On those rows middle, high
school, and grade 14 will not sum to the total. The parser stores All Grades.
"""
from pathlib import Path
import pandas as pd
from typing import Any, Dict
import logging
import sys

# Add etl directory to path for imports
etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from constants import KPI_COLUMNS
from base_etl import BaseETL, Config

logger = logging.getLogger(__name__)


def _is_string_like(series: pd.Series) -> bool:
    """True for object text and for pandas StringDtype.

    ``read_csv(..., dtype=str)`` on pandas 3 returns StringDtype, not object.
    A comma in ``29,360`` has to be removed before ``to_numeric``.
    pandas 2.3.3 still returns object, so the older ``dtype == object``
    strip already keeps that count. pandas is unpinned.
    """
    return series.dtype == object or bool(pd.api.types.is_string_dtype(series))


def _count_is_blank(value: Any) -> bool:
    """True when a count is missing or zero.

    ``value or fallback`` raises ``boolean value of NA is ambiguous`` on
    pandas 2 when ``value`` is ``pd.NA``. ``np.nan`` is truthy, so the same
    expression does not fall through on pandas 3. Treat both as blank, and
    keep the old falsy fallback for a numeric zero.
    """
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    try:
        if bool(pd.isna(value)):
            return True
    except (TypeError, ValueError):
        return False
    if isinstance(value, bool):
        return False
    try:
        return float(value) == 0.0
    except (TypeError, ValueError):
        return False


def clean_enrollment_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and validate secondary enrollment count values."""
    grade_columns = [
        'grade_6', 'grade_7', 'grade_8', 'grade_9', 'grade_10', 'grade_11', 'grade_12',
        'grade_14', 'all_grades', 'total_student_count'
    ]

    for col in grade_columns:
        if col in df.columns:
            # Convert to numeric, handling commas in numbers.
            # astype("string") keeps missing values as <NA>. astype(str) turns
            # them into the literal "nan", and to_numeric then yields NaN.
            if _is_string_like(df[col]):
                df[col] = (
                    df[col]
                    .astype("string")
                    .str.replace(",", "", regex=False)
                    .str.replace('"', "", regex=False)
                )

            # Convert to numeric, errors='coerce' will make invalid values NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')

            # Enrollment counts should be non-negative integers
            invalid_mask = df[col] < 0
            if invalid_mask.any():
                logger.warning(f"Found {invalid_mask.sum()} negative enrollment counts in {col}")
                df.loc[invalid_mask, col] = pd.NA

    return df


class SecondaryEnrollmentETL(BaseETL):
    """ETL module for secondary (concurrent) enrollment by grade.

    KYRC24, KYRC25, and KYRC26 share the same 23-column grade headers.
    KYRC24 is registered on the datasets host. The 2020-2023 files use
    the uppercase count names, which are already mapped. No temporary OAA
    extract is registered for this directory, and the guessed temporary
    names return 404, so there is no leftover filename to skip.
    """

    # Opt in here, in the same change that accepts the file. BaseETL skips
    # every KYRC26_* CSV until this is True.
    include_kyrc26 = True

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            # KYRC25 format
            'All Grades': 'all_grades',
            'Grade 6': 'grade_6',
            'Grade 7': 'grade_7',
            'Grade 8': 'grade_8',
            'Grade 9': 'grade_9',
            'Grade 10': 'grade_10',
            'Grade 11': 'grade_11',
            'Grade 12': 'grade_12',
            'Grade 14': 'grade_14',

            # Historical format (uppercase from 2020-2023 CSVs)
            'TOTAL STUDENT COUNT': 'total_student_count',
            'GRADE6 COUNT': 'grade_6',
            'GRADE7 COUNT': 'grade_7',
            'GRADE8 COUNT': 'grade_8',
            'GRADE9 COUNT': 'grade_9',
            'GRADE10 COUNT': 'grade_10',
            'GRADE11 COUNT': 'grade_11',
            'GRADE12 COUNT': 'grade_12',
            'GRADE14 COUNT': 'grade_14',

            # Older files include preschool through grade 5. Those cells are 0
            # and are not emitted. All Grades still counts every grade.
            'PRESCHOOL COUNT': 'preschool',
            'KINDERGARTEN COUNT': 'k',
            'GRADE1 COUNT': 'grade_1',
            'GRADE2 COUNT': 'grade_2',
            'GRADE3 COUNT': 'grade_3',
            'GRADE4 COUNT': 'grade_4',
            'GRADE5 COUNT': 'grade_5',

            # Historical xlsx format (2018-19)
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
            'STUDENTGROUP': 'demographic',
            'TOTALSTUDENTS_CNT': 'total_student_count',
            'P_CNT': 'preschool',
            'K_CNT': 'k',
            'G1_CNT': 'grade_1',
            'G2_CNT': 'grade_2',
            'G3_CNT': 'grade_3',
            'G4_CNT': 'grade_4',
            'G5_CNT': 'grade_5',
            'G6_CNT': 'grade_6',
            'G7_CNT': 'grade_7',
            'G8_CNT': 'grade_8',
            'G9_CNT': 'grade_9',
            'G10_CNT': 'grade_10',
            'G11_CNT': 'grade_11',
            'G12_CNT': 'grade_12',
            'G14_CNT': 'grade_14',
        }

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        metrics = {}

        # Helper function to safely convert to numeric
        def safe_numeric(value: Any) -> Any:
            if pd.isna(value):
                return pd.NA
            try:
                return pd.to_numeric(value, errors='coerce')
            except Exception:
                return pd.NA

        # Total secondary enrollment. A blank All Grades cell is pd.NA after
        # the Int64 cast in process(). ``or`` cannot test that value.
        all_grades = row.get('all_grades', pd.NA)
        total_count = row.get('total_student_count', pd.NA)
        total_val = total_count if _count_is_blank(all_grades) else all_grades
        total_numeric = safe_numeric(total_val)
        if pd.notna(total_numeric) and total_numeric > 0:
            metrics['secondary_enrollment_total'] = total_numeric

        # Published grade columns. Grade 14 is handled below.
        grade_metrics = {
            'secondary_enrollment_grade_6': safe_numeric(row.get('grade_6', pd.NA)),
            'secondary_enrollment_grade_7': safe_numeric(row.get('grade_7', pd.NA)),
            'secondary_enrollment_grade_8': safe_numeric(row.get('grade_8', pd.NA)),
            'secondary_enrollment_grade_9': safe_numeric(row.get('grade_9', pd.NA)),
            'secondary_enrollment_grade_10': safe_numeric(row.get('grade_10', pd.NA)),
            'secondary_enrollment_grade_11': safe_numeric(row.get('grade_11', pd.NA)),
            'secondary_enrollment_grade_12': safe_numeric(row.get('grade_12', pd.NA)),
        }

        # A zero is an offered grade with a count of 0. It is omitted here
        # (value > 0) and again in convert_to_kpi_format (value != 0). A
        # blank grade never reaches this check. Whether a published zero
        # should be a KPI row is a pre-existing design question, not
        # suppression. Counts of 1 and 2 stay.
        for metric_name, value in grade_metrics.items():
            if pd.notna(value) and value > 0:
                metrics[metric_name] = value

        # Grade 14 (rare but exists in some data)
        grade_14_val = safe_numeric(row.get('grade_14', pd.NA))
        if pd.notna(grade_14_val) and grade_14_val > 0:
            metrics['secondary_enrollment_grade_14'] = grade_14_val

        # Calculate aggregated metrics for school levels
        middle_grades = ['grade_6', 'grade_7', 'grade_8']
        high_school_grades = ['grade_9', 'grade_10', 'grade_11', 'grade_12']

        # Middle school enrollment (6-8)
        middle_counts = []
        for grade in middle_grades:
            value = row.get(grade, pd.NA)
            if pd.notna(value):
                try:
                    numeric_value = pd.to_numeric(value, errors='coerce')
                    if pd.notna(numeric_value) and numeric_value > 0:
                        middle_counts.append(numeric_value)
                except Exception:
                    pass
        if middle_counts:
            metrics['secondary_enrollment_middle'] = sum(middle_counts)

        # High school enrollment (9-12)
        high_school_counts = []
        for grade in high_school_grades:
            value = row.get(grade, pd.NA)
            if pd.notna(value):
                try:
                    numeric_value = pd.to_numeric(value, errors='coerce')
                    if pd.notna(numeric_value) and numeric_value > 0:
                        high_school_counts.append(numeric_value)
                except Exception:
                    pass
        if high_school_counts:
            metrics['secondary_enrollment_high_school'] = sum(high_school_counts)

        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get default metrics for suppressed secondary enrollment records."""
        defaults = {}

        # Only create defaults for metrics that exist in the source data
        if 'all_grades' in row.index or 'total_student_count' in row.index:
            defaults['secondary_enrollment_total'] = pd.NA

        # Check for individual grade level columns
        if 'grade_6' in row.index:
            defaults['secondary_enrollment_grade_6'] = pd.NA
        if 'grade_7' in row.index:
            defaults['secondary_enrollment_grade_7'] = pd.NA
        if 'grade_8' in row.index:
            defaults['secondary_enrollment_grade_8'] = pd.NA
        if 'grade_9' in row.index:
            defaults['secondary_enrollment_grade_9'] = pd.NA
        if 'grade_10' in row.index:
            defaults['secondary_enrollment_grade_10'] = pd.NA
        if 'grade_11' in row.index:
            defaults['secondary_enrollment_grade_11'] = pd.NA
        if 'grade_12' in row.index:
            defaults['secondary_enrollment_grade_12'] = pd.NA

        # Check for aggregate level columns
        grade_columns = [col for col in row.index if col.startswith('grade_')]
        if any(col in ['grade_6', 'grade_7', 'grade_8'] for col in grade_columns):
            defaults['secondary_enrollment_middle'] = pd.NA
        if any(col in ['grade_9', 'grade_10', 'grade_11', 'grade_12'] for col in grade_columns):
            defaults['secondary_enrollment_high_school'] = pd.NA

        return defaults

    def convert_to_kpi_format(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """Override to ensure all secondary enrollment metrics are created together."""
        kpi_rows = []

        for _, row in df.iterrows():
            if self.should_skip_row(row):
                continue

            kpi_template = self.create_kpi_template(row, source_file)
            metrics = self.extract_metrics(row)

            # Create separate KPI rows for each metric
            for metric_name, metric_value in metrics.items():
                if pd.notna(metric_value) and metric_value != 0:  # Skip zero enrollments
                    kpi_row = kpi_template.copy()
                    kpi_row['metric'] = metric_name
                    kpi_row['value'] = metric_value
                    kpi_rows.append(kpi_row)

        return pd.DataFrame(kpi_rows, columns=KPI_COLUMNS)

    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Override to include secondary enrollment specific missing value handling."""
        # Apply base missing value standardization
        df = super().standardize_missing_values(df)

        # Apply enrollment-specific cleaning
        df = clean_enrollment_data(df)

        return df


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Read secondary enrollment files, normalize, and convert to KPI format."""
    etl = SecondaryEnrollmentETL('secondary_enrollment')
    etl.process(raw_dir, proc_dir, cfg)


def main() -> None:
    """Run secondary enrollment ETL process."""
    logging.basicConfig(level=logging.INFO)

    raw_dir = Path(__file__).parent.parent / "data" / "raw"
    proc_dir = Path(__file__).parent.parent / "data" / "processed"
    proc_dir.mkdir(exist_ok=True)

    test_config = Config(
        derive={"processing_date": "2025-11-25", "data_quality_flag": "reviewed"}
    ).model_dump()

    transform(raw_dir, proc_dir, test_config)


if __name__ == "__main__":
    main()
