"""
Student Enrollment ETL Module

Handles Kentucky student enrollment data (Preschool-Grade 12) across years 2019-2026.
Combines historical primary enrollment files and the KYRC overview files.
Normalizes column names, handles schema variations, standardizes missing values,
and applies demographic label standardization for consistent longitudinal reporting.

Data includes enrollment counts by grade level:
- Preschool through Grade 12 enrollment counts
- Total student counts by demographic group
- School-level and district-level aggregations
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
    A comma in ``669,865`` has to be removed before ``to_numeric``.
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
    """Clean and validate student enrollment count values."""
    # Grade-level columns to validate
    grade_columns = [
        'preschool', 'k', 'grade_1', 'grade_2', 'grade_3', 'grade_4', 'grade_5',
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


class StudentEnrollmentETL(BaseETL):
    """ETL module for processing student enrollment data.

    KYRC26 uses the same grade headers as KYRC24 and KYRC25. The 2020-2023
    files use the uppercase count names, which are already mapped. No
    temporary OAA extract is registered for this directory, and the guessed
    temporary names return 404, so there is no leftover filename to skip.
    """

    # Opt in here, in the same change that accepts the file. BaseETL skips
    # every KYRC26_* CSV until this is True.
    include_kyrc26 = True

    def accepts_data_file(self, file_path: Path) -> bool:
        """Skip a student-membership extract dropped into this directory.

        ``OVW_Student_Membership`` is a different count, not preschool
        membership. KYRC26 has no Preschool column on that file, and its
        All Grades total is not this file's total. The grade headers it
        still has are mapped, so a parse would emit ``student_enrollment_*``
        rows. Membership is not registered here and has no ETL. Preschool
        enrollment stays on ``OVW_Student_Enrollment``.
        """
        if "student_membership" in file_path.name.lower():
            logger.info(
                "Skipping %s; student membership is not the enrollment source",
                file_path.name,
            )
            return False
        return super().accepts_data_file(file_path)

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            # KYRC24 format
            'All Grades': 'all_grades',
            'Preschool': 'preschool',
            'K': 'k',
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
            'Grade 14': 'grade_14',

            # Historical format (uppercase from 2020-2023 CSVs)
            'TOTAL STUDENT COUNT': 'total_student_count',
            'PRESCHOOL COUNT': 'preschool',
            'KINDERGARTEN COUNT': 'k',
            'GRADE1 COUNT': 'grade_1',
            'GRADE2 COUNT': 'grade_2',
            'GRADE3 COUNT': 'grade_3',
            'GRADE4 COUNT': 'grade_4',
            'GRADE5 COUNT': 'grade_5',
            'GRADE6 COUNT': 'grade_6',
            'GRADE7 COUNT': 'grade_7',
            'GRADE8 COUNT': 'grade_8',
            'GRADE9 COUNT': 'grade_9',
            'GRADE10 COUNT': 'grade_10',
            'GRADE11 COUNT': 'grade_11',
            'GRADE12 COUNT': 'grade_12',
            'GRADE14 COUNT': 'grade_14',

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
        def safe_numeric(value):
            if pd.isna(value):
                return pd.NA
            try:
                return pd.to_numeric(value, errors='coerce')
            except:
                return pd.NA
        
        # Total enrollment. A blank All Grades cell is pd.NA after cleaning.
        # ``or`` cannot test that value on pandas 2.
        all_grades = row.get('all_grades', pd.NA)
        total_count = row.get('total_student_count', pd.NA)
        total_val = total_count if _count_is_blank(all_grades) else all_grades
        total_numeric = safe_numeric(total_val)
        if pd.notna(total_numeric) and total_numeric > 0:
            metrics['student_enrollment_total'] = total_numeric
        
        # Individual grade levels
        grade_metrics = {
            'student_enrollment_preschool': safe_numeric(row.get('preschool', pd.NA)),
            'student_enrollment_kindergarten': safe_numeric(row.get('k', pd.NA)),
            'student_enrollment_grade_1': safe_numeric(row.get('grade_1', pd.NA)),
            'student_enrollment_grade_2': safe_numeric(row.get('grade_2', pd.NA)),
            'student_enrollment_grade_3': safe_numeric(row.get('grade_3', pd.NA)),
            'student_enrollment_grade_4': safe_numeric(row.get('grade_4', pd.NA)),
            'student_enrollment_grade_5': safe_numeric(row.get('grade_5', pd.NA)),
            'student_enrollment_grade_6': safe_numeric(row.get('grade_6', pd.NA)),
            'student_enrollment_grade_7': safe_numeric(row.get('grade_7', pd.NA)),
            'student_enrollment_grade_8': safe_numeric(row.get('grade_8', pd.NA)),
            'student_enrollment_grade_9': safe_numeric(row.get('grade_9', pd.NA)),
            'student_enrollment_grade_10': safe_numeric(row.get('grade_10', pd.NA)),
            'student_enrollment_grade_11': safe_numeric(row.get('grade_11', pd.NA)),
            'student_enrollment_grade_12': safe_numeric(row.get('grade_12', pd.NA)),
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
            metrics['student_enrollment_grade_14'] = grade_14_val
        
        # Calculate aggregated metrics for school levels
        primary_grades = ['preschool', 'k', 'grade_1', 'grade_2', 'grade_3', 'grade_4', 'grade_5']
        middle_grades = ['grade_6', 'grade_7', 'grade_8']
        secondary_grades = ['grade_9', 'grade_10', 'grade_11', 'grade_12']
        
        # Primary enrollment (PreK-5)
        primary_counts = []
        for grade in primary_grades:
            value = row.get(grade, pd.NA)
            if pd.notna(value):
                try:
                    numeric_value = pd.to_numeric(value, errors='coerce')
                    if pd.notna(numeric_value) and numeric_value > 0:
                        primary_counts.append(numeric_value)
                except:
                    pass
        if primary_counts:
            metrics['student_enrollment_primary'] = sum(primary_counts)
        
        # Middle enrollment (6-8)
        middle_counts = []
        for grade in middle_grades:
            value = row.get(grade, pd.NA)
            if pd.notna(value):
                try:
                    numeric_value = pd.to_numeric(value, errors='coerce')
                    if pd.notna(numeric_value) and numeric_value > 0:
                        middle_counts.append(numeric_value)
                except:
                    pass
        if middle_counts:
            metrics['student_enrollment_middle'] = sum(middle_counts)
        
        # Secondary enrollment (9-12)
        secondary_counts = []
        for grade in secondary_grades:
            value = row.get(grade, pd.NA)
            if pd.notna(value):
                try:
                    numeric_value = pd.to_numeric(value, errors='coerce')
                    if pd.notna(numeric_value) and numeric_value > 0:
                        secondary_counts.append(numeric_value)
                except:
                    pass
        if secondary_counts:
            metrics['student_enrollment_secondary'] = sum(secondary_counts)
        
        return metrics
    
    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get default metrics for suppressed student enrollment records."""
        defaults = {}
        
        # Only create defaults for metrics that exist in the source data
        # Check for total enrollment columns
        if 'all_grades' in row.index or 'total_student_count' in row.index:
            defaults['student_enrollment_total'] = pd.NA
            
        # Check for individual grade level columns
        if 'preschool' in row.index:
            defaults['student_enrollment_preschool'] = pd.NA
        if 'k' in row.index:
            defaults['student_enrollment_kindergarten'] = pd.NA
        if 'grade_1' in row.index:
            defaults['student_enrollment_grade_1'] = pd.NA
        if 'grade_2' in row.index:
            defaults['student_enrollment_grade_2'] = pd.NA
        if 'grade_3' in row.index:
            defaults['student_enrollment_grade_3'] = pd.NA
        if 'grade_4' in row.index:
            defaults['student_enrollment_grade_4'] = pd.NA
        if 'grade_5' in row.index:
            defaults['student_enrollment_grade_5'] = pd.NA
        if 'grade_6' in row.index:
            defaults['student_enrollment_grade_6'] = pd.NA
        if 'grade_7' in row.index:
            defaults['student_enrollment_grade_7'] = pd.NA
        if 'grade_8' in row.index:
            defaults['student_enrollment_grade_8'] = pd.NA
        if 'grade_9' in row.index:
            defaults['student_enrollment_grade_9'] = pd.NA
        if 'grade_10' in row.index:
            defaults['student_enrollment_grade_10'] = pd.NA
        if 'grade_11' in row.index:
            defaults['student_enrollment_grade_11'] = pd.NA
        if 'grade_12' in row.index:
            defaults['student_enrollment_grade_12'] = pd.NA
            
        # Check for aggregate level columns (these are calculated, not from source)
        # Only add them if we have the underlying grade data to calculate from
        grade_columns = [col for col in row.index if col.startswith('grade_') and col[6:].isdigit()]
        if any(col in ['grade_1', 'grade_2', 'grade_3', 'grade_4', 'grade_5'] for col in grade_columns):
            defaults['student_enrollment_primary'] = pd.NA
        if any(col in ['grade_6', 'grade_7', 'grade_8'] for col in grade_columns):
            defaults['student_enrollment_middle'] = pd.NA
        if any(col in ['grade_9', 'grade_10', 'grade_11', 'grade_12'] for col in grade_columns):
            defaults['student_enrollment_secondary'] = pd.NA
            
        return defaults
    
    def convert_to_kpi_format(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """
        Override to ensure all student enrollment metrics are created together.
        """
        kpi_rows = []
        
        for _, row in df.iterrows():
            if self.should_skip_row(row):
                continue
            
            kpi_template = self.create_kpi_template(row, source_file)
            metrics = self.extract_metrics(row)
            
            # Create separate KPI rows for each metric
            for metric_name, metric_value in metrics.items():
                # Second zero skip. extract_metrics already drops value <= 0.
                if pd.notna(metric_value) and metric_value != 0:
                    kpi_row = kpi_template.copy()
                    kpi_row['metric'] = metric_name
                    kpi_row['value'] = metric_value
                    kpi_rows.append(kpi_row)
        
        return pd.DataFrame(kpi_rows, columns=KPI_COLUMNS)
    
    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Override to include student enrollment specific missing value handling."""
        # Apply base missing value standardization
        df = super().standardize_missing_values(df)

        # Apply enrollment-specific cleaning
        df = clean_enrollment_data(df)

        return df


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Read newest student enrollment files, normalize, and convert to KPI format with demographic standardization using BaseETL."""
    # Use BaseETL for consistent processing
    etl = StudentEnrollmentETL('student_enrollment')
    etl.process(raw_dir, proc_dir, cfg)


def main():
    """Run student enrollment ETL process."""
    import logging
    logging.basicConfig(level=logging.INFO)
    
    from pathlib import Path
    raw_dir = Path(__file__).parent.parent / "data" / "raw"
    proc_dir = Path(__file__).parent.parent / "data" / "processed"
    proc_dir.mkdir(exist_ok=True)
    
    test_config = Config(
        derive={"processing_date": "2025-07-25", "data_quality_flag": "reviewed"}
    ).dict()

    transform(raw_dir, proc_dir, test_config)


if __name__ == "__main__":
    main()