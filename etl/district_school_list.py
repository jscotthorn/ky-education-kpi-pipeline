"""
District School List ETL Module

Processes Kentucky school directory data to extract school location coordinates
as KPI metrics. This provides latitude and longitude for geographic analysis
and mapping of school-level metrics.

Data includes:
- School latitude (decimal degrees)
- School longitude (decimal degrees)

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
            'High Grade': 'high_grade',
            'HIGH GRADE': 'high_grade',
            'Title I Status': 'title_i_status',
            'TITLE I STATUS': 'title_i_status',
            'Address': 'address',
            'ADDRESS': 'address',
            'City': 'city',
            'CITY': 'city',
            'Zipcode': 'zipcode',
            'ZIPCODE': 'zipcode',
            # NCES ID variations
            'NCES Id': 'nces_id',
            'Co-Op': 'co_op',
        }

    def extract_metrics(self, row: pd.Series) -> Dict[str, Any]:
        """Extract latitude and longitude as KPI metrics.

        Both coordinates must be valid for either to be included. This ensures
        geographic data integrity - a school with only one valid coordinate
        would be unusable for mapping purposes.
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

        return metrics

    def get_suppressed_metric_defaults(self, row: pd.Series) -> Dict[str, Any]:
        """Get default metrics for suppressed records (unlikely for directory data)."""
        return {
            'school_latitude': pd.NA,
            'school_longitude': pd.NA,
        }

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
