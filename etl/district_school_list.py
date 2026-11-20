"""
District School List ETL Module

Processes Kentucky school directory data to extract school location coordinates
and institutional characteristics as KPI metrics.

Data includes:
- School latitude (decimal degrees)
- School longitude (decimal degrees)
- School Type (dummy-encoded for Bayesian models)
- Title I Status (dummy-encoded for Bayesian models)

School Type codes (KDE classification):
- A1: Standard public school (reference category)
- A2: Career/Technical Education center
- A3: Special education program
- A4: Preschool program
- A5: Alternative program (remediation)
- A6: Alternative program (state agency children)
- A8, B1, B2, C2, D1: Other classifications

Title I Status categories:
- Not a Title 1 School (reference category)
- Title 1 Eligible - Schoolwide School
- Title 1 Eligible - Targeted Assistance School
- Title 1 Eligible - No Program
- Title 1 - Schoolwide School
- Title 1 Eligible - Schoolwide Program

Note: This is institutional-level data without demographic breakdowns.
All records have student_group='All Students'.
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


class DistrictSchoolListETL(BaseETL):
    """ETL module for processing district school list data to extract coordinates."""

    @property
    def module_column_mappings(self) -> Dict[str, str]:
        return {
            # Coordinate columns
            'Latitude': 'latitude',
            'LATITUDE': 'latitude',
            'Longitude': 'longitude',
            'LONGITUDE': 'longitude',
            # Additional school info columns that might be useful
            'Low Grade': 'low_grade',
            'LOW GRADE': 'low_grade',
            'LOW_GRADE': 'low_grade',
            'High Grade': 'high_grade',
            'HIGH GRADE': 'high_grade',
            'HIGH_GRADE': 'high_grade',
            'Title I Status': 'title_i_status',
            'TITLE I STATUS': 'title_i_status',
            'TITLE1_STATUS': 'title_i_status',
            'School Type': 'school_type',
            'SCHOOL TYPE': 'school_type',
            'SCH_TYPE': 'school_type',
            'Address': 'address',
            'ADDRESS': 'address',
            'City': 'city',
            'CITY': 'city',
            'Zipcode': 'zipcode',
            'ZIPCODE': 'zipcode',
            # NCES ID variations
            'NCES Id': 'nces_id',
            'Co-Op': 'co_op',
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
        }

    # School Type codes to create dummy variables for (A1 is reference category)
    SCHOOL_TYPE_DUMMIES = ['A2', 'A3', 'A4', 'A5', 'A6', 'A8', 'B1', 'B2', 'C2', 'D1']

    # Title I Status categories (reference: "Not a Title 1 School" / "Not a Title I School")
    # We create a simplified binary indicator plus a "schoolwide" indicator
    # Note: Some years use "Title 1" and others use "Title I" (capital I)
    TITLE_I_SCHOOLWIDE_PATTERNS = [
        'Title 1 Eligible - Schoolwide School',
        'Title 1 - Schoolwide School',
        'Title 1 Eligible - Schoolwide Program',
        'Title I Eligible - Schoolwide School',
        'Title I - Schoolwide School',
        'Title I Eligible - Schoolwide Program',
    ]
    TITLE_I_TARGETED_PATTERNS = [
        'Title 1 Eligible - Targeted Assistance School',
        'Title I Eligible - Targeted Assistance School',
    ]
    TITLE_I_ELIGIBLE_NO_PROGRAM_PATTERNS = [
        'Title 1 Eligible - No Program',
        'Title I Eligible - No Program',
    ]

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        """Extract latitude, longitude, grade range, school type, and Title I status.

        Both coordinates must be valid for either to be included. This ensures
        geographic data integrity - a school with only one valid coordinate
        would be unusable for mapping purposes.

        Grade range (low_grade, high_grade) is extracted for school type classification.

        School Type and Title I Status are dummy-encoded for use in Bayesian models:
        - School Type: A1 is reference category (1150 schools, 77% of data)
        - Title I: "Not a Title 1 School" is reference category (368 schools, 25%)
        """
        metrics = {}
        lat_value = None
        lon_value = None

        # Extract and validate latitude
        lat = row.get('latitude', pd.NA)
        if pd.notna(lat):
            try:
                lat_value = float(lat)
                # Validate reasonable latitude range for Kentucky (~36 to ~39 degrees)
                if not (35.0 <= lat_value <= 40.0):
                    logger.warning(f"Latitude {lat_value} outside expected Kentucky range")
                    lat_value = None
            except (ValueError, TypeError):
                lat_value = None

        # Extract and validate longitude
        lon = row.get('longitude', pd.NA)
        if pd.notna(lon):
            try:
                lon_value = float(lon)
                # Validate reasonable longitude range for Kentucky (~-89 to ~-82 degrees)
                if not (-90.0 <= lon_value <= -80.0):
                    logger.warning(f"Longitude {lon_value} outside expected Kentucky range")
                    lon_value = None
            except (ValueError, TypeError):
                lon_value = None

        # Only include both coordinates if both are valid
        if lat_value is not None and lon_value is not None:
            metrics['school_latitude'] = lat_value
            metrics['school_longitude'] = lon_value

        # Extract grade range for school type classification
        low_grade = row.get('low_grade', pd.NA)
        high_grade = row.get('high_grade', pd.NA)

        if pd.notna(low_grade):
            metrics['school_low_grade'] = self._normalize_grade(low_grade)

        if pd.notna(high_grade):
            metrics['school_high_grade'] = self._normalize_grade(high_grade)

        # Extract School Type dummy variables
        school_type = row.get('school_type', pd.NA)
        metrics.update(self._encode_school_type(school_type))

        # Extract Title I Status dummy variables
        title_i_status = row.get('title_i_status', pd.NA)
        metrics.update(self._encode_title_i_status(title_i_status))

        return metrics

    def _encode_school_type(self, school_type: Any) -> Dict[str, int]:
        """Encode School Type as dummy variables.

        A1 (standard public school) is the reference category and is omitted.
        Returns binary indicators for other school types.
        """
        metrics = {}

        # Initialize all dummies to 0
        for code in self.SCHOOL_TYPE_DUMMIES:
            metrics[f'school_type_{code.lower()}'] = 0

        if pd.notna(school_type):
            school_type_str = str(school_type).strip().upper()
            # Set the appropriate dummy to 1 if it matches
            if school_type_str in self.SCHOOL_TYPE_DUMMIES:
                metrics[f'school_type_{school_type_str.lower()}'] = 1

        return metrics

    def _encode_title_i_status(self, title_i_status: Any) -> Dict[str, int]:
        """Encode Title I Status as dummy variables.

        Creates three binary indicators:
        - title_i_schoolwide: 1 if school has schoolwide Title I program
        - title_i_targeted: 1 if school has targeted assistance Title I program
        - title_i_eligible_no_program: 1 if eligible but no program implemented

        Reference category: "Not a Title 1 School" (all dummies = 0)
        """
        metrics = {
            'title_i_schoolwide': 0,
            'title_i_targeted': 0,
            'title_i_eligible_no_program': 0,
        }

        if pd.notna(title_i_status):
            status_str = str(title_i_status).strip()

            if status_str in self.TITLE_I_SCHOOLWIDE_PATTERNS:
                metrics['title_i_schoolwide'] = 1
            elif status_str in self.TITLE_I_TARGETED_PATTERNS:
                metrics['title_i_targeted'] = 1
            elif status_str in self.TITLE_I_ELIGIBLE_NO_PROGRAM_PATTERNS:
                metrics['title_i_eligible_no_program'] = 1
            # "Not a Title 1 School" -> all zeros (reference category)

        return metrics

    def _normalize_grade(self, grade: Any) -> float:
        """Normalize grade value to numeric.

        Handles special cases:
        - 'P', 'PK', 'Preschool' -> -1 (Pre-K)
        - 'K' -> 0 (Kindergarten)
        - Ordinal suffixes ('1st', '2nd', '3rd', '4th', '5th', etc.) -> numeric
        - Numeric grades -> as-is
        """
        if pd.isna(grade):
            return pd.NA

        grade_str = str(grade).strip().upper()

        # Handle pre-kindergarten
        if grade_str in ['P', 'PK', 'PRE-K', 'PREK', 'PRESCHOOL']:
            return -1.0

        # Handle kindergarten
        if grade_str in ['K', 'KG', 'KINDERGARTEN']:
            return 0.0

        # Strip ordinal suffixes (ST, ND, RD, TH)
        import re
        grade_cleaned = re.sub(r'(ST|ND|RD|TH)$', '', grade_str)

        # Try numeric conversion
        try:
            return float(grade_cleaned)
        except (ValueError, TypeError):
            logger.warning(f"Could not convert grade '{grade}' to numeric")
            return pd.NA

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get default metrics for suppressed records (unlikely for directory data)."""
        defaults = {
            'school_latitude': pd.NA,
            'school_longitude': pd.NA,
            'school_low_grade': pd.NA,
            'school_high_grade': pd.NA,
        }
        # Add School Type dummies (all 0 = A1 reference category)
        for code in self.SCHOOL_TYPE_DUMMIES:
            defaults[f'school_type_{code.lower()}'] = 0
        # Add Title I dummies (all 0 = Not a Title 1 School)
        defaults['title_i_schoolwide'] = 0
        defaults['title_i_targeted'] = 0
        defaults['title_i_eligible_no_program'] = 0
        return defaults

    def should_skip_row(self, row: pd.Series) -> bool:
        """
        Skip rows that are district totals or don't have valid coordinates.

        District school list files contain both school-level and district aggregate rows.
        We only want individual school records with valid coordinates.
        """
        school_name = row.get('school_name', '')

        # Skip district aggregate rows
        if pd.isna(school_name) or school_name == '':
            return True
        if str(school_name) in self.DISTRICT_AGGREGATE_PATTERNS:
            return True
        if '---District Total---' in str(school_name):
            return True
        if 'All Schools' in str(school_name):
            return True

        # Skip rows without valid coordinates
        lat = row.get('latitude', pd.NA)
        lon = row.get('longitude', pd.NA)
        if pd.isna(lat) or pd.isna(lon):
            return True

        # Validate coordinates are parseable
        try:
            float(lat)
            float(lon)
        except (ValueError, TypeError):
            return True

        return False

    def create_kpi_template(self, row: pd.Series, source_file: str) -> Dict[str, Any]:
        """
        Create base KPI record for school directory data.

        Override to set student_group='All Students' since this is institutional data.
        """
        # Extract school identification
        school_id = self.extract_school_id(row)
        year = self.extract_year(row, source_file)

        return {
            'district': row.get('district_name', 'Unknown District'),
            'school_id': school_id,
            'school_name': self.standardize_school_name(row.get('school_name', 'Unknown School')),
            'year': year,
            'student_group': 'All Students',  # Institutional data, no demographics
            'county_number': row.get('county_number', pd.NA),
            'county_name': row.get('county_name', pd.NA),
            'district_number': row.get('district_number', pd.NA),
            'school_code': row.get('school_code', pd.NA),
            'state_school_id': row.get('state_school_id', pd.NA),
            'nces_id': row.get('nces_id', pd.NA),
            'co_op': row.get('co_op', pd.NA),
            'co_op_code': row.get('co_op_code', pd.NA),
            'school_type': row.get('school_type', pd.NA),
            'suppressed': 'N',  # Directory data is not suppressed
            'source_file': source_file,
            'last_updated': pd.Timestamp.now().isoformat()
        }


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Read district school list files, normalize, and convert to KPI format."""
    etl = DistrictSchoolListETL('district_school_list')
    etl.process(raw_dir, proc_dir, cfg)


def main():
    """Run district school list ETL process."""
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
