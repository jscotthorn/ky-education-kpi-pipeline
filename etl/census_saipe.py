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
Census SAIPE (Small Area Income and Poverty Estimates) ETL Module

Fetches county-level income and poverty data from the Census Bureau API
for use in Hierarchical Bayesian bright spots modeling.

Data: County-level median household income, poverty rates, child poverty
Release Schedule: December each year for previous year's estimates
Lag: 12 months (Dec 2024 release has 2023 data)

Source: https://www.census.gov/programs-surveys/saipe.html
API: https://api.census.gov/data/timeseries/poverty/saipe

Kentucky FIPS code: 21
"""

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)


# Kentucky county FIPS codes with names for reference
KY_COUNTIES = {
    "001": "Adair",
    "003": "Allen",
    "005": "Anderson",
    "007": "Ballard",
    "009": "Barren",
    "011": "Bath",
    "013": "Bell",
    "015": "Boone",
    "017": "Bourbon",
    "019": "Boyd",
    "021": "Boyle",
    "023": "Bracken",
    "025": "Breathitt",
    "027": "Breckinridge",
    "029": "Bullitt",
    "031": "Butler",
    "033": "Caldwell",
    "035": "Calloway",
    "037": "Campbell",
    "039": "Carlisle",
    "041": "Carroll",
    "043": "Carter",
    "045": "Casey",
    "047": "Christian",
    "049": "Clark",
    "051": "Clay",
    "053": "Clinton",
    "055": "Crittenden",
    "057": "Cumberland",
    "059": "Daviess",
    "061": "Edmonson",
    "063": "Elliott",
    "065": "Estill",
    "067": "Fayette",
    "069": "Fleming",
    "071": "Floyd",
    "073": "Franklin",
    "075": "Fulton",
    "077": "Gallatin",
    "079": "Garrard",
    "081": "Grant",
    "083": "Graves",
    "085": "Grayson",
    "087": "Green",
    "089": "Greenup",
    "091": "Hancock",
    "093": "Hardin",
    "095": "Harlan",
    "097": "Harrison",
    "099": "Hart",
    "101": "Henderson",
    "103": "Henry",
    "105": "Hickman",
    "107": "Hopkins",
    "109": "Jackson",
    "111": "Jefferson",
    "113": "Jessamine",
    "115": "Johnson",
    "117": "Kenton",
    "119": "Knott",
    "121": "Knox",
    "123": "Larue",
    "125": "Laurel",
    "127": "Lawrence",
    "129": "Lee",
    "131": "Leslie",
    "133": "Letcher",
    "135": "Lewis",
    "137": "Lincoln",
    "139": "Livingston",
    "141": "Logan",
    "143": "Lyon",
    "145": "McCracken",
    "147": "McCreary",
    "149": "McLean",
    "151": "Madison",
    "153": "Magoffin",
    "155": "Marion",
    "157": "Marshall",
    "159": "Martin",
    "161": "Mason",
    "163": "Meade",
    "165": "Menifee",
    "167": "Mercer",
    "169": "Metcalfe",
    "171": "Monroe",
    "173": "Montgomery",
    "175": "Morgan",
    "177": "Muhlenberg",
    "179": "Nelson",
    "181": "Nicholas",
    "183": "Ohio",
    "185": "Oldham",
    "187": "Owen",
    "189": "Owsley",
    "191": "Pendleton",
    "193": "Perry",
    "195": "Pike",
    "197": "Powell",
    "199": "Pulaski",
    "201": "Robertson",
    "203": "Rockcastle",
    "205": "Rowan",
    "207": "Russell",
    "209": "Scott",
    "211": "Shelby",
    "213": "Simpson",
    "215": "Spencer",
    "217": "Taylor",
    "219": "Todd",
    "221": "Trigg",
    "223": "Trimble",
    "225": "Union",
    "227": "Warren",
    "229": "Washington",
    "231": "Wayne",
    "233": "Webster",
    "235": "Whitley",
    "237": "Wolfe",
    "239": "Woodford",
}


class CensusSAIPEETL:
    """
    ETL module for Census SAIPE (Small Area Income and Poverty Estimates).

    Fetches county-level economic indicators from the Census Bureau API:
    - Median household income
    - Poverty rate (all ages)
    - Child poverty rate (ages 0-17)
    - Poverty rate ages 5-17 (school-age children)

    These indicators are used as covariates in the Hierarchical Bayesian
    bright spots model to control for community economic context.
    """

    BASE_URL = "https://api.census.gov/data/timeseries/poverty/saipe"
    STATE_FIPS = "21"  # Kentucky

    # SAIPE variable codes
    # See: https://api.census.gov/data/timeseries/poverty/saipe/variables.html
    VARIABLES = {
        "SAEMHI_PT": "median_household_income",  # Median Household Income Estimate
        "SAEPOVRTALL_PT": "poverty_rate_all_ages",  # Poverty Rate All Ages
        "SAEPOVRT0_17_PT": "poverty_rate_0_17",  # Poverty Rate Ages 0-17
        "SAEPOVRT5_17R_PT": "poverty_rate_5_17",  # Poverty Rate Ages 5-17 (school-age)
    }

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the Census SAIPE ETL.

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

    def extract(self, year: int) -> pd.DataFrame:
        """
        Extract SAIPE data for all Kentucky counties for a given year.

        Args:
            year: Data year (e.g., 2023 for data released Dec 2024)

        Returns:
            DataFrame with county FIPS, name, and economic indicators

        Raises:
            requests.RequestException: If API request fails
            ValueError: If API returns invalid data
        """
        # Build variable list for API request
        var_list = ",".join(["NAME"] + list(self.VARIABLES.keys()))

        # Build API URL
        url = f"{self.BASE_URL}?get={var_list}&for=county:*&in=state:{self.STATE_FIPS}&time={year}"

        if self.api_key:
            url += f"&key={self.api_key}"

        logger.info(f"Fetching SAIPE data for year {year} from Census API")

        try:
            response = requests.get(url, timeout=30)
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
            raise ValueError(f"No data returned from Census API for year {year}")

        # Convert to DataFrame
        df = pd.DataFrame(data[1:], columns=data[0])

        logger.info(f"Extracted {len(df)} county records for year {year}")

        return df

    def transform(self, df: pd.DataFrame, year: int) -> pd.DataFrame:
        """
        Transform Census API data to standardized format.

        Args:
            df: Raw DataFrame from extract()
            year: Data year for metadata

        Returns:
            DataFrame in long format with one row per county per metric
        """
        # Create full FIPS code (state + county)
        df["county_fips"] = self.STATE_FIPS + df["county"]

        # Rename Census variables to our standard names
        df = df.rename(columns=self.VARIABLES)

        # Add county name from our reference (Census NAME includes state)
        df["county_name"] = df["county"].map(KY_COUNTIES)

        # Convert numeric columns
        numeric_cols = list(self.VARIABLES.values())
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Melt to long format (one row per metric)
        id_vars = ["county_fips", "county_name", "county", "state", "NAME"]
        value_vars = [col for col in numeric_cols if col in df.columns]

        df_long = df.melt(
            id_vars=[col for col in id_vars if col in df.columns],
            value_vars=value_vars,
            var_name="metric",
            value_name="value",
        )

        # Add metadata fields
        df_long["year"] = year
        df_long["data_vintage"] = str(year)
        df_long["geography_level"] = "county"
        df_long["source"] = "Census SAIPE"
        df_long["data_as_of_date"] = f"{year}-12-31"
        df_long["last_updated"] = datetime.now().isoformat()

        # Select and order final columns
        output_cols = [
            "year",
            "geography_level",
            "county_fips",
            "county_name",
            "metric",
            "value",
            "data_vintage",
            "source",
            "data_as_of_date",
            "last_updated",
        ]

        df_long = df_long[output_cols]

        logger.info(
            f"Transformed to {len(df_long)} metric records "
            f"({len(df_long['metric'].unique())} metrics x {len(df_long['county_fips'].unique())} counties)"
        )

        return df_long

    def load(self, df: pd.DataFrame, output_path: Path) -> None:
        """
        Save transformed data to CSV.

        Args:
            df: Transformed DataFrame
            output_path: Path to save CSV file
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)
        logger.info(f"Saved {len(df)} records to {output_path}")

    def run(self, year: int, output_dir: Optional[Path] = None) -> pd.DataFrame:
        """
        Run full ETL pipeline for a given year.

        Args:
            year: Data year to fetch
            output_dir: Directory to save output (default: data/external/census_saipe)

        Returns:
            Transformed DataFrame
        """
        # Extract
        df_raw = self.extract(year)

        # Transform
        df_transformed = self.transform(df_raw, year)

        # Load (if output directory specified)
        if output_dir:
            output_path = output_dir / f"census_saipe_{year}.csv"
            self.load(df_transformed, output_path)
            print(f"Wrote {output_path}")

        return df_transformed

    def run_multiple_years(
        self, years: List[int], output_dir: Optional[Path] = None
    ) -> pd.DataFrame:
        """
        Run ETL for multiple years and combine results.

        Args:
            years: List of years to fetch
            output_dir: Directory to save individual year files

        Returns:
            Combined DataFrame with all years
        """
        all_data = []

        for year in years:
            try:
                df = self.run(year, output_dir)
                all_data.append(df)
                logger.info(f"Successfully processed year {year}")
            except Exception as e:
                logger.error(f"Failed to process year {year}: {e}")
                continue

        if not all_data:
            raise ValueError("No data was successfully fetched for any year")

        combined = pd.concat(all_data, ignore_index=True)

        # Save combined file
        if output_dir:
            combined_path = output_dir / "census_saipe_combined.csv"
            self.load(combined, combined_path)
            print(f"Wrote combined file: {combined_path}")

        return combined

    def get_county_crosswalk(self) -> pd.DataFrame:
        """
        Get a crosswalk between county FIPS codes and names.

        Useful for joining SAIPE data with school-level data.

        Returns:
            DataFrame with county_fips and county_name columns
        """
        return pd.DataFrame(
            [
                {"county_fips": f"{self.STATE_FIPS}{code}", "county_name": name}
                for code, name in KY_COUNTIES.items()
            ]
        )

    def get_available_years(self) -> List[int]:
        """
        Get list of years with available SAIPE data.

        SAIPE data is typically available from 1989 onwards, with annual
        estimates starting in 2000.

        Returns:
            List of available years
        """
        # SAIPE annual estimates run from 2000 to present
        # Most recent year is typically (current year - 1) released in December
        current_year = datetime.now().year
        current_month = datetime.now().month

        # If we're past December, the previous year should be available
        # Otherwise, go back two years to be safe
        if current_month >= 12:
            latest_year = current_year - 1
        else:
            latest_year = current_year - 2

        return list(range(2000, latest_year + 1))


def transform(raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """
    ETL runner compatible function for Census SAIPE data.

    This function is called by etl_runner.py and follows the standard
    transform signature used by other ETL modules.

    Args:
        raw_dir: Not used (data is fetched from API)
        proc_dir: Output directory for processed data
        cfg: Configuration dictionary (may contain 'years' list and 'api_key')
    """
    # Get configuration
    api_key = cfg.get("api_key") or os.getenv("CENSUS_API_KEY")
    years = cfg.get("years", [2021, 2022, 2023])

    # Create output directory
    output_dir = proc_dir.parent / "external" / "census_saipe"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Run ETL
    etl = CensusSAIPEETL(api_key=api_key)
    etl.run_multiple_years(years, output_dir)

    logger.info(f"Census SAIPE ETL complete. Output in {output_dir}")


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Fetch Census SAIPE income and poverty data for Kentucky counties"
    )
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        default=[2021, 2022, 2023],
        help="Years to fetch (default: 2021 2022 2023)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).parent.parent / "data" / "external" / "census_saipe",
        help="Output directory (default: data/external/census_saipe)",
    )
    parser.add_argument(
        "--api-key",
        type=str,
        default=None,
        help="Census API key (or set CENSUS_API_KEY env var)",
    )

    args = parser.parse_args()

    etl = CensusSAIPEETL(api_key=args.api_key)
    etl.run_multiple_years(args.years, args.output)
