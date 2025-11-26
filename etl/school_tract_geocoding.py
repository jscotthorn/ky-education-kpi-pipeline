# Copyright 2025 Kentucky Open Government Coalition
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
School to Census Tract Geocoding Module

Maps Kentucky schools to Census tracts using their latitude/longitude coordinates.
This enables joining tract-level Census ACS data to individual schools for
more granular neighborhood context than county-level data alone.

The Census Geocoding API provides tract FIPS codes for any lat/lon within the US.

Usage:
    python etl/school_tract_geocoding.py
    python etl/school_tract_geocoding.py --county FAYETTE  # Fayette County only
    python etl/school_tract_geocoding.py --year 2022
"""

import logging
import os
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# Paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
EXTERNAL_DIR = DATA_DIR / "external"
OUTPUT_DIR = EXTERNAL_DIR / "school_tracts"


class SchoolTractGeocoder:
    """
    Maps schools to Census tracts using the Census Geocoding API.

    The Census Geocoding API accepts latitude/longitude and returns
    geographic identifiers including state, county, and tract FIPS codes.
    """

    GEOCODE_URL = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"

    def __init__(self, benchmark: str = "Public_AR_Current", vintage: str = "Current_Current"):
        """
        Initialize the geocoder.

        Args:
            benchmark: Census benchmark (e.g., "Public_AR_Current")
            vintage: Census vintage (e.g., "Current_Current", "Census2020_Current")
        """
        self.benchmark = benchmark
        self.vintage = vintage
        self._cache: Dict[Tuple[float, float], dict] = {}

    def geocode_point(
        self,
        latitude: float,
        longitude: float,
        retry_count: int = 3,
        retry_delay: float = 1.0
    ) -> Optional[dict]:
        """
        Geocode a single lat/lon point to get Census geography.

        Args:
            latitude: Latitude coordinate
            longitude: Longitude coordinate
            retry_count: Number of retries on failure
            retry_delay: Delay between retries in seconds

        Returns:
            Dictionary with tract_fips, county_fips, state_fips, or None if failed
        """
        # Check cache first
        cache_key = (round(latitude, 6), round(longitude, 6))
        if cache_key in self._cache:
            return self._cache[cache_key]

        params = {
            "x": longitude,
            "y": latitude,
            "benchmark": self.benchmark,
            "vintage": self.vintage,
            "format": "json"
        }

        for attempt in range(retry_count):
            try:
                response = requests.get(self.GEOCODE_URL, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()

                # Parse response
                result = self._parse_geocode_response(data)
                if result:
                    self._cache[cache_key] = result
                    return result

            except requests.RequestException as e:
                logger.warning(f"Geocode attempt {attempt + 1} failed for ({latitude}, {longitude}): {e}")
                if attempt < retry_count - 1:
                    time.sleep(retry_delay)

        return None

    def _parse_geocode_response(self, data: dict) -> Optional[dict]:
        """Parse Census Geocoding API response."""
        try:
            result = data.get("result", {})
            geographies = result.get("geographies", {})

            # Get Census Tracts
            tracts = geographies.get("Census Tracts", [])
            if not tracts:
                tracts = geographies.get("2020 Census Tracts", [])

            if not tracts:
                return None

            tract = tracts[0]

            state_fips = tract.get("STATE", "")
            county_fips = tract.get("COUNTY", "")
            tract_code = tract.get("TRACT", "")

            if not all([state_fips, county_fips, tract_code]):
                return None

            return {
                "state_fips": state_fips,
                "county_fips": state_fips + county_fips,
                "tract_fips": state_fips + county_fips + tract_code,
                "tract_code": tract_code,
                "geoid": tract.get("GEOID", ""),
                "name": tract.get("NAME", "")
            }

        except (KeyError, IndexError) as e:
            logger.warning(f"Failed to parse geocode response: {e}")
            return None

    def geocode_schools(
        self,
        schools_df: pd.DataFrame,
        lat_col: str = "latitude",
        lon_col: str = "longitude",
        school_id_col: str = "school_id",
        rate_limit_delay: float = 0.1
    ) -> pd.DataFrame:
        """
        Geocode multiple schools to Census tracts.

        Args:
            schools_df: DataFrame with school locations
            lat_col: Name of latitude column
            lon_col: Name of longitude column
            school_id_col: Name of school ID column
            rate_limit_delay: Delay between API calls (seconds)

        Returns:
            DataFrame with school_id and tract information
        """
        results = []
        total = len(schools_df)

        for idx, row in schools_df.iterrows():
            school_id = row[school_id_col]
            lat = row[lat_col]
            lon = row[lon_col]

            if pd.isna(lat) or pd.isna(lon):
                logger.warning(f"Missing coordinates for school {school_id}")
                continue

            # Progress logging
            if (idx + 1) % 50 == 0:
                logger.info(f"Geocoded {idx + 1}/{total} schools")

            geo_result = self.geocode_point(lat, lon)

            if geo_result:
                results.append({
                    school_id_col: school_id,
                    "latitude": lat,
                    "longitude": lon,
                    **geo_result
                })
            else:
                logger.warning(f"Failed to geocode school {school_id} at ({lat}, {lon})")
                results.append({
                    school_id_col: school_id,
                    "latitude": lat,
                    "longitude": lon,
                    "state_fips": None,
                    "county_fips": None,
                    "tract_fips": None,
                    "tract_code": None,
                    "geoid": None,
                    "name": None
                })

            # Rate limiting
            time.sleep(rate_limit_delay)

        logger.info(f"Geocoded {len(results)} schools")
        return pd.DataFrame(results)


def load_school_locations(
    location_file: Path,
    county_filter: Optional[str] = None,
    year: int = 2021
) -> pd.DataFrame:
    """
    Load school locations from the district_school_list.csv file.

    Args:
        location_file: Path to the school location CSV
        county_filter: Optional county name to filter (e.g., "FAYETTE")
        year: Year to filter locations for

    Returns:
        DataFrame with school_id, latitude, longitude
    """
    logger.info(f"Loading school locations from {location_file}")

    # Preserve school_id as string to maintain leading zeros (e.g., "001010")
    df = pd.read_csv(location_file, dtype={'school_id': str}, low_memory=False)

    # Filter to specified year
    df = df[df['year'] == year].copy()

    # Filter to lat/lon metrics
    lat_df = df[df['metric'] == 'school_latitude'][['school_id', 'school_name', 'district', 'county_name', 'value']].copy()
    lat_df = lat_df.rename(columns={'value': 'latitude'})

    lon_df = df[df['metric'] == 'school_longitude'][['school_id', 'value']].copy()
    lon_df = lon_df.rename(columns={'value': 'longitude'})

    # Merge lat/lon
    schools = lat_df.merge(lon_df, on='school_id', how='inner')

    # Convert to numeric
    schools['latitude'] = pd.to_numeric(schools['latitude'], errors='coerce')
    schools['longitude'] = pd.to_numeric(schools['longitude'], errors='coerce')

    # Filter by county if specified
    if county_filter:
        county_upper = county_filter.upper()
        schools = schools[schools['county_name'].str.upper() == county_upper].copy()
        logger.info(f"Filtered to {len(schools)} schools in {county_filter} county")
    else:
        logger.info(f"Loaded {len(schools)} schools statewide")

    return schools


def join_tract_acs_data(
    school_tracts: pd.DataFrame,
    acs_file: Path
) -> pd.DataFrame:
    """
    Join tract-level ACS data to geocoded schools.

    Args:
        school_tracts: DataFrame with school_id and tract_fips
        acs_file: Path to Census ACS tract data

    Returns:
        DataFrame with school-level tract characteristics
    """
    logger.info(f"Joining ACS data from {acs_file}")

    acs_df = pd.read_csv(acs_file)

    # Select relevant tract-level variables
    tract_vars = [
        'tract_fips',
        'median_household_income',
        'poverty_rate',
        'unemployment_rate',
        'pct_bachelors_plus',
        'pct_single_parent',
        'pct_owner_occupied',
        'pct_broadband',
        'pct_housing_cost_burden_30_plus'
    ]

    # Filter to available columns
    available_vars = [v for v in tract_vars if v in acs_df.columns]
    acs_subset = acs_df[available_vars].copy()

    # Rename for clarity (prefix with tract_)
    rename_map = {col: f"tract_{col}" for col in available_vars if col != 'tract_fips'}
    acs_subset = acs_subset.rename(columns=rename_map)

    # Ensure tract_fips is string type in both DataFrames for merge
    school_tracts['tract_fips'] = school_tracts['tract_fips'].astype(str)
    acs_subset['tract_fips'] = acs_subset['tract_fips'].astype(str)

    # Join to schools
    result = school_tracts.merge(
        acs_subset,
        on='tract_fips',
        how='left'
    )

    matched = result['tract_median_household_income'].notna().sum()
    logger.info(f"Matched {matched}/{len(result)} schools to ACS tract data")

    return result


def run_geocoding(
    location_file: Path,
    output_dir: Path,
    county_filter: Optional[str] = None,
    year: int = 2021,
    acs_file: Optional[Path] = None
) -> pd.DataFrame:
    """
    Run full geocoding pipeline.

    Args:
        location_file: Path to school locations CSV
        output_dir: Directory for output files
        county_filter: Optional county to filter
        year: Year for school locations
        acs_file: Optional path to ACS tract data for joining

    Returns:
        DataFrame with geocoded schools
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load school locations
    schools = load_school_locations(location_file, county_filter, year)

    if schools.empty:
        logger.error("No schools found to geocode")
        return pd.DataFrame()

    # Geocode to tracts
    geocoder = SchoolTractGeocoder()
    school_tracts = geocoder.geocode_schools(
        schools,
        lat_col='latitude',
        lon_col='longitude',
        school_id_col='school_id'
    )

    # Merge back school metadata
    school_tracts = school_tracts.merge(
        schools[['school_id', 'school_name', 'district', 'county_name']],
        on='school_id',
        how='left'
    )

    # Join ACS data if available
    if acs_file and acs_file.exists():
        school_tracts = join_tract_acs_data(school_tracts, acs_file)

    # Save results
    county_suffix = f"_{county_filter.lower()}" if county_filter else "_statewide"
    output_file = output_dir / f"school_tracts{county_suffix}_{year}.csv"
    school_tracts.to_csv(output_file, index=False)
    logger.info(f"Saved geocoded schools to {output_file}")

    return school_tracts


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    parser = argparse.ArgumentParser(
        description="Geocode Kentucky schools to Census tracts"
    )
    parser.add_argument(
        "--location-file",
        type=Path,
        default=Path(__file__).parent.parent.parent / "ky-education-portal" / "src" / "data" / "processed" / "district_school_list.csv",
        help="Path to school locations CSV"
    )
    parser.add_argument(
        "--county",
        type=str,
        default=None,
        help="County name to filter (e.g., FAYETTE)"
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2021,
        help="Year for school locations"
    )
    parser.add_argument(
        "--acs-file",
        type=Path,
        default=None,
        help="Path to ACS tract data for joining"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT_DIR,
        help="Output directory"
    )

    args = parser.parse_args()

    # Default ACS file for Fayette County
    if args.acs_file is None and args.county and args.county.upper() == "FAYETTE":
        args.acs_file = EXTERNAL_DIR / "census_acs" / "census_acs_tracts_fayette_2022.csv"

    run_geocoding(
        location_file=args.location_file,
        output_dir=args.output,
        county_filter=args.county,
        year=args.year,
        acs_file=args.acs_file
    )
