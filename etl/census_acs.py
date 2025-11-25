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
Census ACS 5-Year Estimates ETL Module

Fetches tract-level demographic and economic data from the American Community
Survey (ACS) 5-Year Estimates for within-county variation analysis.

Data: Tract-level median income, housing burden, educational attainment, demographics
Release Schedule: December each year for 5-year period ending 2 years prior
Lag: 24 months (Dec 2024 release has 2018-2022 data)

Source: https://www.census.gov/data/developers/data-sets/acs-5year.html
API: https://api.census.gov/data/YYYY/acs/acs5

Use Case: Capturing within-Fayette-County variation in economic conditions
that affects school performance. Schools in different census tracts serve
communities with different economic characteristics.
"""

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)


# ACS 5-Year variable codes
# Reference: https://api.census.gov/data/2022/acs/acs5/variables.html
ACS_VARIABLES = {
    # Income
    "B19013_001E": "median_household_income",
    # Housing
    "B25070_010E": "pct_housing_cost_burden_30_plus",  # Gross rent 30%+ of income
    "B25003_002E": "housing_units_owner_occupied",
    "B25003_003E": "housing_units_renter_occupied",
    # Educational Attainment (population 25+)
    "B15003_001E": "pop_25_plus_total",
    "B15003_017E": "pop_25_plus_hs_diploma",
    "B15003_022E": "pop_25_plus_bachelors",
    "B15003_023E": "pop_25_plus_masters",
    "B15003_024E": "pop_25_plus_professional",
    "B15003_025E": "pop_25_plus_doctorate",
    # Employment
    "B23025_003E": "labor_force_total",
    "B23025_005E": "unemployed",
    # Poverty
    "B17001_001E": "poverty_status_total",
    "B17001_002E": "poverty_status_below",
    # Children in poverty (under 18)
    "B17006_001E": "families_total",
    "B17006_002E": "families_below_poverty",
    # Single parent households
    "B11003_010E": "single_mother_families",
    "B11003_016E": "single_father_families",
    # Internet access
    "B28002_004E": "households_with_broadband",
    "B28002_001E": "households_total_internet",
}

# Kentucky county FIPS codes for reference
KY_MAJOR_COUNTIES = {
    "067": "Fayette",
    "111": "Jefferson",
    "015": "Boone",
    "117": "Kenton",
    "037": "Campbell",
    "227": "Warren",
    "059": "Daviess",
    "093": "Hardin",
    "151": "Madison",
    "199": "Pulaski",
}


class CensusACS5ETL:
    """
    ETL module for Census ACS 5-Year Estimates.

    Fetches tract-level data for detailed within-county analysis:
    - Median household income by tract
    - Housing cost burden (rent/mortgage as % of income)
    - Educational attainment
    - Employment/unemployment
    - Poverty rates
    - Single parent households
    - Internet access (digital divide)

    This granular data helps capture economic variation within a county
    that affects school performance. For example, two Fayette County
    schools may serve very different communities.
    """

    BASE_URL = "https://api.census.gov/data"
    STATE_FIPS = "21"  # Kentucky

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the Census ACS ETL.

        Args:
            api_key: Census API key. If not provided, looks for CENSUS_API_KEY
                    environment variable. Get a key at:
                    https://api.census.gov/data/key_signup.html
        """
        self.api_key = api_key or os.getenv("CENSUS_API_KEY")
        if not self.api_key:
            logger.warning(
                "No Census API key provided. Set CENSUS_API_KEY environment variable "
                "or pass api_key parameter. Get a key at: "
                "https://api.census.gov/data/key_signup.html"
            )

    def extract_tracts(
        self,
        year: int,
        county_fips: str = "067",
        variables: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Extract tract-level ACS data for a Kentucky county.

        Args:
            year: ACS 5-year ending year (e.g., 2022 for 2018-2022 data)
            county_fips: County FIPS code (default: 067 = Fayette)
            variables: List of ACS variable codes to fetch (default: all)

        Returns:
            DataFrame with one row per tract

        Raises:
            requests.RequestException: If API request fails
            ValueError: If API returns invalid data
        """
        if variables is None:
            variables = list(ACS_VARIABLES.keys())

        # Build variable list for API request
        var_list = ",".join(["NAME"] + variables)

        # Build API URL
        url = f"{self.BASE_URL}/{year}/acs/acs5"
        params = {
            "get": var_list,
            "for": "tract:*",
            "in": f"state:{self.STATE_FIPS} county:{county_fips}",
        }

        if self.api_key:
            params["key"] = self.api_key

        logger.info(
            f"Fetching ACS 5-year data for year {year}, "
            f"county {county_fips} from Census API"
        )

        try:
            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Census API request failed: {e}")
            raise

        try:
            data = response.json()
        except ValueError as e:
            logger.error(f"Failed to parse Census API response: {e}")
            raise ValueError(f"Invalid JSON response from Census API: {e}")

        if not data or len(data) < 2:
            raise ValueError(
                f"No data returned from Census API for year {year}, county {county_fips}"
            )

        # Convert to DataFrame
        df = pd.DataFrame(data[1:], columns=data[0])

        logger.info(f"Extracted {len(df)} tract records for county {county_fips}")

        return df

    def extract_county(
        self,
        year: int,
        variables: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Extract county-level ACS data for all Kentucky counties.

        Args:
            year: ACS 5-year ending year
            variables: List of ACS variable codes to fetch

        Returns:
            DataFrame with one row per county
        """
        if variables is None:
            variables = list(ACS_VARIABLES.keys())

        var_list = ",".join(["NAME"] + variables)

        url = f"{self.BASE_URL}/{year}/acs/acs5"
        params = {
            "get": var_list,
            "for": "county:*",
            "in": f"state:{self.STATE_FIPS}",
        }

        if self.api_key:
            params["key"] = self.api_key

        logger.info(f"Fetching ACS 5-year county data for year {year}")

        try:
            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Census API request failed: {e}")
            raise

        data = response.json()

        if not data or len(data) < 2:
            raise ValueError(f"No data returned from Census API for year {year}")

        df = pd.DataFrame(data[1:], columns=data[0])
        logger.info(f"Extracted {len(df)} county records")

        return df

    def transform(
        self,
        df: pd.DataFrame,
        year: int,
        geography_level: str = "tract",
    ) -> pd.DataFrame:
        """
        Transform Census API data to standardized format.

        Args:
            df: Raw DataFrame from extract()
            year: Data year for metadata
            geography_level: 'tract' or 'county'

        Returns:
            DataFrame with renamed columns and derived metrics
        """
        # Rename ACS variable codes to readable names
        rename_map = {code: name for code, name in ACS_VARIABLES.items() if code in df.columns}
        df = df.rename(columns=rename_map)

        # Create geography identifiers
        if geography_level == "tract":
            df["tract_fips"] = df["state"] + df["county"] + df["tract"]
            df["county_fips"] = df["state"] + df["county"]
        else:
            df["county_fips"] = df["state"] + df["county"]

        # Convert numeric columns and handle Census special values
        # Census uses negative values like -666666666 to indicate missing/unavailable data
        numeric_cols = [col for col in df.columns if col in ACS_VARIABLES.values()]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
                # Replace Census missing value indicators with NaN
                df.loc[df[col] < 0, col] = pd.NA

        # Calculate derived metrics
        df = self._calculate_derived_metrics(df)

        # Add metadata
        df["year"] = year
        df["data_vintage"] = f"{year-4}-{year}"  # ACS 5-year range
        df["geography_level"] = geography_level
        df["source"] = "Census ACS 5-Year"
        df["last_updated"] = datetime.now().isoformat()

        return df

    def _calculate_derived_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate derived metrics from raw ACS variables."""

        # Poverty rate
        if "poverty_status_total" in df.columns and "poverty_status_below" in df.columns:
            df["poverty_rate"] = (
                df["poverty_status_below"] / df["poverty_status_total"] * 100
            ).round(1)

        # Unemployment rate
        if "labor_force_total" in df.columns and "unemployed" in df.columns:
            df["unemployment_rate"] = (
                df["unemployed"] / df["labor_force_total"] * 100
            ).round(1)

        # Percent with bachelor's degree or higher
        if "pop_25_plus_total" in df.columns:
            bachelors_plus = 0
            for col in ["pop_25_plus_bachelors", "pop_25_plus_masters",
                       "pop_25_plus_professional", "pop_25_plus_doctorate"]:
                if col in df.columns:
                    bachelors_plus += df[col].fillna(0)
            df["pct_bachelors_plus"] = (bachelors_plus / df["pop_25_plus_total"] * 100).round(1)

        # Percent single parent households
        if "families_total" in df.columns:
            single_parent = 0
            if "single_mother_families" in df.columns:
                single_parent += df["single_mother_families"].fillna(0)
            if "single_father_families" in df.columns:
                single_parent += df["single_father_families"].fillna(0)
            df["pct_single_parent"] = (single_parent / df["families_total"] * 100).round(1)

        # Percent owner-occupied housing
        if "housing_units_owner_occupied" in df.columns and "housing_units_renter_occupied" in df.columns:
            total_housing = df["housing_units_owner_occupied"] + df["housing_units_renter_occupied"]
            df["pct_owner_occupied"] = (
                df["housing_units_owner_occupied"] / total_housing * 100
            ).round(1)

        # Percent with broadband
        if "households_with_broadband" in df.columns and "households_total_internet" in df.columns:
            df["pct_broadband"] = (
                df["households_with_broadband"] / df["households_total_internet"] * 100
            ).round(1)

        return df

    def transform_long(
        self,
        df: pd.DataFrame,
        year: int,
        geography_level: str = "tract",
        metrics: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Transform to long format (one row per geography per metric).

        Args:
            df: Raw DataFrame from extract()
            year: Data year
            geography_level: 'tract' or 'county'
            metrics: Specific metrics to include (default: all derived metrics)

        Returns:
            DataFrame in long format
        """
        # First do wide transformation
        df_wide = self.transform(df, year, geography_level)

        # Define metrics to melt
        if metrics is None:
            metrics = [
                "median_household_income",
                "poverty_rate",
                "unemployment_rate",
                "pct_bachelors_plus",
                "pct_single_parent",
                "pct_owner_occupied",
                "pct_broadband",
                "pct_housing_cost_burden_30_plus",
            ]

        # Filter to metrics that exist
        metrics = [m for m in metrics if m in df_wide.columns]

        # Determine ID columns
        if geography_level == "tract":
            id_vars = ["tract_fips", "county_fips", "NAME", "year", "data_vintage",
                      "geography_level", "source", "last_updated"]
        else:
            id_vars = ["county_fips", "NAME", "year", "data_vintage",
                      "geography_level", "source", "last_updated"]

        id_vars = [col for col in id_vars if col in df_wide.columns]

        # Melt to long format
        df_long = df_wide.melt(
            id_vars=id_vars,
            value_vars=metrics,
            var_name="metric",
            value_name="value",
        )

        return df_long

    def load(self, df: pd.DataFrame, output_path: Path) -> None:
        """Save transformed data to CSV."""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)
        logger.info(f"Saved {len(df)} records to {output_path}")

    def run_tracts(
        self,
        year: int,
        county_fips: str = "067",
        output_dir: Optional[Path] = None,
        long_format: bool = False,
    ) -> pd.DataFrame:
        """
        Run full ETL for tract-level data.

        Args:
            year: ACS 5-year ending year
            county_fips: County FIPS code
            output_dir: Directory to save output
            long_format: If True, output in long format (one row per metric)

        Returns:
            Transformed DataFrame
        """
        county_name = KY_MAJOR_COUNTIES.get(county_fips, county_fips)

        # Extract
        df_raw = self.extract_tracts(year, county_fips)

        # Transform
        if long_format:
            df_transformed = self.transform_long(df_raw, year, "tract")
        else:
            df_transformed = self.transform(df_raw, year, "tract")

        # Load
        if output_dir:
            suffix = "_long" if long_format else ""
            filename = f"census_acs_tracts_{county_name.lower()}_{year}{suffix}.csv"
            output_path = output_dir / filename
            self.load(df_transformed, output_path)
            print(f"Wrote {output_path}")

        return df_transformed

    def run_counties(
        self,
        year: int,
        output_dir: Optional[Path] = None,
        long_format: bool = False,
    ) -> pd.DataFrame:
        """
        Run full ETL for county-level data.

        Args:
            year: ACS 5-year ending year
            output_dir: Directory to save output
            long_format: If True, output in long format

        Returns:
            Transformed DataFrame
        """
        # Extract
        df_raw = self.extract_county(year)

        # Transform
        if long_format:
            df_transformed = self.transform_long(df_raw, year, "county")
        else:
            df_transformed = self.transform(df_raw, year, "county")

        # Load
        if output_dir:
            suffix = "_long" if long_format else ""
            filename = f"census_acs_counties_{year}{suffix}.csv"
            output_path = output_dir / filename
            self.load(df_transformed, output_path)
            print(f"Wrote {output_path}")

        return df_transformed

    def run_multiple_counties(
        self,
        year: int,
        county_fips_list: List[str],
        output_dir: Optional[Path] = None,
    ) -> pd.DataFrame:
        """
        Run ETL for tracts in multiple counties.

        Args:
            year: ACS 5-year ending year
            county_fips_list: List of county FIPS codes
            output_dir: Directory to save output

        Returns:
            Combined DataFrame with all counties
        """
        all_data = []

        for county_fips in county_fips_list:
            try:
                df = self.run_tracts(year, county_fips, output_dir=None)
                all_data.append(df)
                county_name = KY_MAJOR_COUNTIES.get(county_fips, county_fips)
                logger.info(f"Successfully processed {county_name} County")
            except Exception as e:
                logger.error(f"Failed to process county {county_fips}: {e}")
                continue

        if not all_data:
            raise ValueError("No data was successfully fetched for any county")

        combined = pd.concat(all_data, ignore_index=True)

        if output_dir:
            output_path = output_dir / f"census_acs_tracts_combined_{year}.csv"
            self.load(combined, output_path)
            print(f"Wrote combined file: {output_path}")

        return combined

    def get_available_years(self) -> List[int]:
        """
        Get list of years with available ACS 5-year data.

        ACS 5-year estimates are available from 2009 onwards (2005-2009 data).
        Most recent is typically (current year - 2).

        Returns:
            List of available ending years
        """
        current_year = datetime.now().year
        current_month = datetime.now().month

        # ACS 5-year is released in December
        if current_month >= 12:
            latest_year = current_year - 1
        else:
            latest_year = current_year - 2

        return list(range(2009, latest_year + 1))


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """
    ETL runner compatible function for Census ACS data.

    Args:
        raw_dir: Not used (data fetched from API)
        proc_dir: Output directory for processed data
        cfg: Configuration dictionary
    """
    api_key = cfg.get("api_key") or os.getenv("CENSUS_API_KEY")
    year = cfg.get("year", 2022)
    county_fips = cfg.get("county_fips", "067")  # Fayette by default

    output_dir = proc_dir.parent / "external" / "census_acs"
    output_dir.mkdir(parents=True, exist_ok=True)

    etl = CensusACS5ETL(api_key=api_key)
    etl.run_tracts(year, county_fips, output_dir)

    logger.info(f"Census ACS ETL complete. Output in {output_dir}")


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Fetch Census ACS 5-Year tract-level data for Kentucky"
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2022,
        help="ACS 5-year ending year (default: 2022)",
    )
    parser.add_argument(
        "--county",
        type=str,
        default="067",
        help="County FIPS code (default: 067 = Fayette)",
    )
    parser.add_argument(
        "--all-major",
        action="store_true",
        help="Fetch data for all major Kentucky counties",
    )
    parser.add_argument(
        "--counties-only",
        action="store_true",
        help="Fetch county-level data instead of tract-level",
    )
    parser.add_argument(
        "--long-format",
        action="store_true",
        help="Output in long format (one row per metric)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).parent.parent / "data" / "external" / "census_acs",
        help="Output directory",
    )
    parser.add_argument(
        "--api-key",
        type=str,
        default=None,
        help="Census API key (or set CENSUS_API_KEY env var)",
    )

    args = parser.parse_args()

    etl = CensusACS5ETL(api_key=args.api_key)

    if args.counties_only:
        etl.run_counties(args.year, args.output, args.long_format)
    elif args.all_major:
        etl.run_multiple_counties(
            args.year, list(KY_MAJOR_COUNTIES.keys()), args.output
        )
    else:
        etl.run_tracts(args.year, args.county, args.output, args.long_format)
