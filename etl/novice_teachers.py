"""
Novice Teachers ETL Module

Processes Kentucky novice teacher data from two complementary files:

1. Institutional data: the share of teachers who are inexperienced, split into
   less than 1 year and 1 to 3 years.
2. Equity data: the share of students taught by inexperienced teachers, by
   Title I status and demographic group.

KDE's report-card glossary defines inexperienced teachers as teachers with
0-3 years of teaching experience. A teacher in the first year (less than 1
year) is a New Teacher. The 2021-22 report-card change note says the
inexperienced-teacher breakdown is "less than one year" and "between one to
three years." The quality-assurance worksheet says the counts come from the
Kentucky Educator Credentialing System. The students-taught file is the
comparison of students taught by those teachers.

The two published rates are stored separately. This module does not add them
into one total.

KYRC26 uses the same headers as KYRC24 and KYRC25. No temporary OAA extract
is registered, and the guessed temporary names return 404, so there is no
leftover temporary filename to skip. ``KYRC23_OVW_Inexperienced_Teachers.csv``
and ``KYRC23_OVW_Students_Taught_by_Inexperienced_Teachers.csv`` return 200
and republish 2022-23. They are not registered, and ``accepts_data_file``
skips those names.

The 2018-19 workbook is a gap in the series. Its cover sheet calls the rate
"Percentage of New Teachers." That is the first-year rate, not the 1-3 year
rate, so only ``novice_teacher_rate_less_than_1_year`` is emitted. On that
file a blank ``NEWPCT`` with ``NEWTEACHERS`` 0 and ``TEACHERS`` above 0 is
0 percent: every other row matches new teachers divided by teachers.
Those rows are stored as 0.0 with ``suppressed`` ``N``. A blank percent
without that zero count stays skipped. The 2020 file has a new-teacher
count and no rate. The 2021 file has one combined ``PERCENT INEXPIENCED
TEACHERS`` rate. Those columns are not the two split rates, so they are
not mapped onto either KPI. The analysis dataset requires both split
rates before it builds a combined series, so 2019 stays the first-year
metric only.
"""
from pathlib import Path
import pandas as pd
from typing import Any, Dict, List
import logging
import sys

# Add etl directory to path for imports
etl_dir = Path(__file__).parent
sys.path.insert(0, str(etl_dir))

from constants import KPI_COLUMNS
from base_etl import BaseETL, Config

logger = logging.getLogger(__name__)

# KYRC23 is the 2022-23 extract again. The registered historical files
# already cover that year. School codes match after the 3-digit district
# codes are written as NNN000, so loading both would write 2023 twice.
_KYRC23_REPUBLISHES = {
    "KYRC23_OVW_INEXPERIENCED_TEACHERS.CSV",
    "KYRC23_OVW_STUDENTS_TAUGHT_BY_INEXPERIENCED_TEACHERS.CSV",
}

_TITLE_STATUSES = ("Title 1", "Not Title 1", "Equity Gap")

_INSTITUTIONAL_RATES = (
    "percent_new_teachers_less_than_1_year",
    "percent_new_teachers_1_to_3_years",
)

_COUNT_COLUMNS = (
    "teacher_count",
    "new_teachers_1_to_3_years",
    "new_teachers_less_than_1_year",
)

# Columns the production mappings.yaml casts to Int64 or float. A comma has
# to be gone before that cast. The equity rate columns have no commas.
_COMMA_COLUMNS = _COUNT_COLUMNS + _INSTITUTIONAL_RATES

_DEMOGRAPHIC_KEYS = (
    "all_students",
    "non_white",
    "white",
    "economically_disadvantaged",
    "non_economically_disadvantaged",
    "students_with_disabilities",
    "student_without_disabilities",
    "english_learner",
    "non_english_learner",
)


def _is_string_like(series: pd.Series) -> bool:
    """True for object text and for pandas StringDtype.

    ``read_csv(..., dtype=str)`` on pandas 3 returns StringDtype, not object.
    A comma in ``6,016`` has to be removed before ``to_numeric``.
    pandas 2.3.3 still returns object. pandas is unpinned. ``bool(pd.NA)``
    is not used to detect the dtype.
    """
    return series.dtype == object or bool(pd.api.types.is_string_dtype(series))


def _strip_thousands(series: pd.Series) -> pd.Series:
    """Remove thousands separators from a text column.

    ``astype("string")`` keeps missing values as ``<NA>``. ``astype(str)``
    turns them into the literal ``"nan"``.
    """
    if not _is_string_like(series):
        return series
    return series.astype("string").str.replace(",", "", regex=False).str.strip()


def _parse_number(value: Any) -> Any:
    """Parse one rate or count. ``pd.NA`` is not used as a boolean.

    A comma in ``6,016`` is removed before ``float``. Numeric values,
    including numpy integers left by an Int64 cast, are returned as floats.
    """
    if value is None:
        return pd.NA
    if isinstance(value, str):
        cleaned = value.replace(",", "").strip()
        if cleaned == "":
            return pd.NA
        try:
            return float(cleaned)
        except ValueError:
            return pd.NA
    try:
        if bool(pd.isna(value)):
            return pd.NA
    except (TypeError, ValueError):
        return pd.NA
    if isinstance(value, bool):
        return pd.NA
    try:
        return float(value)
    except (TypeError, ValueError):
        return pd.NA


def _text_or_blank(value: Any) -> str:
    """Return a stripped string. Missing values, including ``pd.NA``, are blank."""
    if value is None:
        return ""
    try:
        if bool(pd.isna(value)):
            return ""
    except (TypeError, ValueError):
        return ""
    return str(value).strip()


class NoviceTeachersETL(BaseETL):
    """ETL module for novice teacher rates and the student-equity rates.

    KYRC26 uses the same headers as KYRC24 and KYRC25. The 2022-2023 files
    use the uppercase split-rate names, which are already mapped. The
    2018-19 workbook uses ``NEWPCT`` for the first-year rate. No temporary
    OAA extract is registered for this directory.
    """

    # Opt in here, in the same change that accepts the file. BaseETL skips
    # every KYRC26_* CSV until this is True.
    include_kyrc26 = True

    def accepts_data_file(self, file_path: Path) -> bool:
        """Skip the KYRC23 republish of the 2022-23 extracts.

        Both KYRC23 files return 200 on the datasets host. School codes
        match the registered 2022-23 files after 3-digit district codes are
        written as ``NNN000``. Loading both would write each school twice
        for 2023. The new-teacher counts match. Teacher counts and the
        published rates do not all match, because KYRC23 revised some
        denominators and rounded rates to one decimal. It is still school
        year ``20222023`` on every row. The guessed OAA names are 404 and
        were never registered, so they are not skipped here.
        """
        if file_path.name.upper() in _KYRC23_REPUBLISHES:
            logger.info(
                "Skipping %s; it republishes 2022-23 already loaded from "
                "the historical inexperienced-teacher files",
                file_path.name,
            )
            return False
        return super().accepts_data_file(file_path)

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

            # 2018-19 workbook. NEWPCT is the percentage of new teachers.
            # The glossary calls a first-year teacher a New Teacher, so this
            # is the less-than-1-year rate. There is no 1-3 year column.
            # 2020 TOTAL NEW TEACHERS is a count with no rate. 2021
            # PERCENT INEXPIENCED TEACHERS is one combined rate. Neither
            # column is one of the two split rates, so neither is mapped.
            'SCH_YEAR': 'school_year',
            'CNTYNO': 'county_number',
            'CNTYNAME': 'county_name',
            'DIST_NUMBER': 'district_number',
            'DIST_NAME': 'district_name',
            'SCH_NUMBER': 'school_number',
            'SCH_NAME': 'school_name',
            'STATE_SCH_ID': 'state_school_id',
            'NCESID': 'nces_id',
            'COOP': 'co_op',
            'COOP_CODE': 'co_op_code',
            'TEACHERS': 'teacher_count',
            'NEWTEACHERS': 'new_teachers_less_than_1_year',
            'NEWPCT': 'percent_new_teachers_less_than_1_year',

            # Equity file columns (Title I status is a separate field)
            'Title_I_Status': 'title_i_status',
            'TITLE I STATUS': 'title_i_status',

            # Demographics - Standard (KYRC24/25/26)
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

    def _title_status(self, row: pd.Series) -> str:
        """Title I status, or blank when the cell is missing.

        ``pd.NA in (...)`` raises ``boolean value of NA is ambiguous``.
        Compare the string after missing values are turned into a blank.
        """
        return _text_or_blank(row.get("title_i_status", ""))

    def _is_suppressed(self, row: pd.Series) -> bool:
        """True only for an explicit ``Y``. A missing flag is not suppressed."""
        return _text_or_blank(row.get("suppressed", "N")) == "Y"

    def _columns_present(self, row: pd.Series, names: tuple) -> List[str]:
        return [name for name in names if name in row.index]

    def _all_measures_blank(self, row: pd.Series, names: tuple) -> bool:
        """True when every listed measure column is present and empty.

        A column that is not on the row is not an empty measure. The 2020
        institutional file has a teacher count and no rate columns. That
        row is not a blank-rate row.
        """
        present = self._columns_present(row, names)
        if not present:
            return False
        return all(pd.isna(_parse_number(row.get(name))) for name in present)

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        metrics = {}

        # Institutional rates. A published 0 is kept. The 2018-19 workbook
        # leaves NEWPCT blank when the new-teacher count is 0. That blank
        # is 0 percent when the teacher count is positive. Any other blank
        # percent is not a rate. The workbook has no 1-3 year column.
        percent_less_than_1 = _parse_number(row.get('percent_new_teachers_less_than_1_year', pd.NA))
        if pd.isna(percent_less_than_1) and self._blank_percent_is_zero_new_teachers(row):
            percent_less_than_1 = 0.0
        if pd.notna(percent_less_than_1) and percent_less_than_1 >= 0:
            metrics['novice_teacher_rate_less_than_1_year'] = percent_less_than_1

        percent_1_to_3 = _parse_number(row.get('percent_new_teachers_1_to_3_years', pd.NA))
        if pd.notna(percent_1_to_3) and percent_1_to_3 >= 0:
            metrics['novice_teacher_rate_1_to_3_years'] = percent_1_to_3

        # Equity rates. Title I, Not Title I, and Equity Gap are separate
        # rows, and each demographic is its own KPI. 2020-2023 and KYRC26
        # publish a signed gap (Title I minus Not Title I). KYRC24 and
        # KYRC25 publish the absolute value. The ``>= 0`` check drops the
        # signed negatives. That predates KYRC26. Issue #59 tracks storing
        # a signed gap. This pull request does not change the drop.
        title_i_status = self._title_status(row)
        if title_i_status in _TITLE_STATUSES:
            title_i_suffix = title_i_status.lower().replace(' ', '_')
            for demo_key in _DEMOGRAPHIC_KEYS:
                value = _parse_number(row.get(demo_key, pd.NA))
                if pd.notna(value) and value >= 0:
                    metric_name = (
                        f'students_taught_by_inexperienced_teachers_rate_{title_i_suffix}__{demo_key}'
                    )
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
        title_i_status = self._title_status(row)
        if title_i_status in _TITLE_STATUSES:
            title_i_suffix = title_i_status.lower().replace(' ', '_')
            for demo_key in _DEMOGRAPHIC_KEYS:
                if demo_key in row.index:
                    metric_name = (
                        f'students_taught_by_inexperienced_teachers_rate_{title_i_suffix}__{demo_key}'
                    )
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
                        parsed = _parse_number(value)
                        if pd.notna(parsed):
                            kpi_record['value'] = float(parsed)
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
        """Skip rows that are not an inexperienced-teacher record.

        notes/8: a row that is not suppressed and has every measure empty
        is not offered, and it emits nothing. A suppressed row is kept so
        the empty value can be stored with ``suppressed`` ``Y``. These
        files have no Suppressed column. The 2022 Kentucky School for the
        Blind, Kentucky School for the Deaf, and Model Laboratory district
        totals have a teacher count and blank rate cells. Those rates are
        empty, so the row is skipped. A published rate of 0 is kept.

        ``pd.isna`` short-circuits before a comparison, so ``pd.NA`` from
        the Int64 cast is not evaluated as a boolean.
        """
        has_institutional = pd.notna(_parse_number(row.get('teacher_count', pd.NA)))
        has_equity = self._title_status(row) in _TITLE_STATUSES
        if not has_institutional and not has_equity:
            return True

        if self._is_suppressed(row):
            return False

        if has_equity and self._all_measures_blank(row, _DEMOGRAPHIC_KEYS):
            return True
        if has_institutional and not has_equity and self._all_measures_blank(row, _INSTITUTIONAL_RATES):
            # A blank 2018-19 percent with zero new teachers is 0%, not a
            # missing rate. A blank percent with no zero count stays skipped.
            return not self._blank_percent_is_zero_new_teachers(row)
        return False  # Don't call super() - may not have demographics in institutional file

    def _blank_percent_is_zero_new_teachers(self, row: pd.Series) -> bool:
        """True when a blank first-year percent is a published zero.

        The 2018-19 workbook leaves ``NEWPCT`` blank on the rows where
        ``NEWTEACHERS`` is 0 and ``TEACHERS`` is above 0. On every other
        row the percent equals that count divided by the teacher count.
        The 2022 blank district totals have empty counts, so they stay
        skipped.
        """
        percent = _parse_number(row.get("percent_new_teachers_less_than_1_year", pd.NA))
        if pd.notna(percent):
            return False
        new_teachers = _parse_number(row.get("new_teachers_less_than_1_year", pd.NA))
        teachers = _parse_number(row.get("teacher_count", pd.NA))
        return (
            pd.notna(new_teachers)
            and new_teachers == 0
            and pd.notna(teachers)
            and teachers > 0
        )

    def _fill_workbook_school_code(self, df: pd.DataFrame) -> pd.DataFrame:
        """Build a 6-digit school code for the 2018-19 workbook.

        That file has district and school numbers and no school code.
        A school is the 3-digit district number plus the 3-digit school
        number. A blank school number is a district total, ``NNN000``.
        The state row is district ``999`` and becomes ``999000``. Later
        historical files publish their own school code, including the
        3-digit state code ``999``, and those codes are left as written.
        """
        if "school_code" in df.columns:
            return df
        if "district_number" not in df.columns or "school_number" not in df.columns:
            return df

        districts = df["district_number"].astype("string").str.strip().str.zfill(3)
        schools = df["school_number"].astype("string").str.strip()
        missing_school = schools.isna() | schools.eq("") | schools.str.lower().isin(["nan", "<na>", "none"])
        school_codes = districts + schools.str.zfill(3)
        district_totals = districts + "000"
        df = df.copy()
        df["school_code"] = school_codes.mask(missing_school, district_totals)
        return df

    def standardize_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Strip thousands separators before the configured Int64 cast.

        ``config/mappings.yaml`` casts ``teacher_count`` and the two
        new-teacher counts to Int64 inside ``BaseETL.process``, which runs
        ``to_numeric`` before ``should_skip_row``. A comma in ``6,016``
        becomes missing at that cast. The strip has to happen here, on
        object and on StringDtype. pandas is unpinned. The only commas in
        the registered files are the state new-teacher counts. Teacher
        counts and the rate columns have no commas, so the strip does not
        change a published rate.
        """
        df = super().standardize_missing_values(df)
        df = self._fill_workbook_school_code(df)
        for column in _COMMA_COLUMNS:
            if column not in df.columns:
                continue
            df[column] = pd.to_numeric(_strip_thousands(df[column]), errors="coerce")
        return df


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
    ).model_dump()

    transform(raw_dir, proc_dir, test_config)


if __name__ == "__main__":
    main()
