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
Unit tests for Census ACS 5-Year ETL module.

Tests the extraction, transformation, and loading of Census ACS
tract-level demographic and economic data for Kentucky.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from etl.census_acs import CensusACS5ETL, ACS_VARIABLES, KY_MAJOR_COUNTIES


class TestCensusACS5ETL:
    """Test suite for CensusACS5ETL class."""

    @pytest.fixture
    def etl(self):
        """Create ETL instance with test API key."""
        return CensusACS5ETL(api_key="test_key")

    @pytest.fixture
    def sample_tract_response(self):
        """Sample Census API response for tract data."""
        return [
            # Header row
            [
                "NAME",
                "B19013_001E",  # median income
                "B17001_001E",  # poverty total
                "B17001_002E",  # poverty below
                "B15003_001E",  # pop 25+ total
                "B15003_022E",  # bachelors
                "B28002_004E",  # broadband
                "B28002_001E",  # total internet
                "state",
                "county",
                "tract",
            ],
            # High-income tract
            [
                "Census Tract 6, Fayette County, Kentucky",
                "125000",
                "1500",
                "50",
                "2000",
                "1200",
                "800",
                "850",
                "21",
                "067",
                "000600",
            ],
            # Low-income tract
            [
                "Census Tract 4, Fayette County, Kentucky",
                "28000",
                "2000",
                "800",
                "1500",
                "200",
                "400",
                "500",
                "21",
                "067",
                "000400",
            ],
            # Tract with missing income (Census uses negative values)
            [
                "Census Tract 8, Fayette County, Kentucky",
                "-666666666",
                "500",
                "200",
                "800",
                "400",
                "200",
                "250",
                "21",
                "067",
                "000800",
            ],
        ]

    @pytest.fixture
    def sample_county_response(self):
        """Sample Census API response for county data."""
        return [
            [
                "NAME",
                "B19013_001E",
                "B17001_001E",
                "B17001_002E",
                "state",
                "county",
            ],
            ["Fayette County, Kentucky", "67000", "50000", "8000", "21", "067"],
            ["Jefferson County, Kentucky", "58000", "120000", "18000", "21", "111"],
        ]

    def test_init_with_api_key(self):
        """Test initialization with provided API key."""
        etl = CensusACS5ETL(api_key="my_key")
        assert etl.api_key == "my_key"

    def test_init_without_api_key(self):
        """Test initialization without API key logs warning."""
        with patch.dict("os.environ", {}, clear=True):
            with patch("etl.census_acs.logger") as mock_logger:
                etl = CensusACS5ETL()
                mock_logger.warning.assert_called()

    def test_acs_variables_defined(self):
        """Test that ACS variables are defined."""
        assert len(ACS_VARIABLES) > 0
        assert "B19013_001E" in ACS_VARIABLES  # Median income
        assert ACS_VARIABLES["B19013_001E"] == "median_household_income"

    def test_ky_major_counties_defined(self):
        """Test major Kentucky counties are defined."""
        assert "067" in KY_MAJOR_COUNTIES  # Fayette
        assert "111" in KY_MAJOR_COUNTIES  # Jefferson
        assert KY_MAJOR_COUNTIES["067"] == "Fayette"

    @patch("requests.get")
    def test_extract_tracts_success(self, mock_get, etl, sample_tract_response):
        """Test successful tract data extraction."""
        mock_response = MagicMock()
        mock_response.json.return_value = sample_tract_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        df = etl.extract_tracts(2022, "067")

        assert len(df) == 3
        assert "B19013_001E" in df.columns
        mock_get.assert_called_once()

    @patch("requests.get")
    def test_extract_tracts_api_error(self, mock_get, etl):
        """Test handling of API errors."""
        from requests.exceptions import RequestException

        mock_get.side_effect = RequestException("API Error")

        with pytest.raises(RequestException):
            etl.extract_tracts(2022, "067")

    @patch("requests.get")
    def test_extract_county_success(self, mock_get, etl, sample_county_response):
        """Test successful county data extraction."""
        mock_response = MagicMock()
        mock_response.json.return_value = sample_county_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        df = etl.extract_county(2022)

        assert len(df) == 2
        mock_get.assert_called_once()

    def test_transform_tract_data(self, etl, sample_tract_response):
        """Test transformation of tract data."""
        df_raw = pd.DataFrame(
            sample_tract_response[1:], columns=sample_tract_response[0]
        )

        df = etl.transform(df_raw, 2022, "tract")

        # Check required columns exist
        assert "tract_fips" in df.columns
        assert "county_fips" in df.columns
        assert "year" in df.columns
        assert "source" in df.columns

        # Check tract FIPS format
        assert df["tract_fips"].iloc[0] == "21067000600"

    def test_transform_handles_missing_values(self, etl, sample_tract_response):
        """Test that Census missing value indicators are converted to NaN."""
        df_raw = pd.DataFrame(
            sample_tract_response[1:], columns=sample_tract_response[0]
        )

        df = etl.transform(df_raw, 2022, "tract")

        # The -666666666 value should become NaN
        tract_8 = df[df["tract_fips"] == "21067000800"]
        assert pd.isna(tract_8["median_household_income"].values[0])

    def test_transform_calculates_poverty_rate(self, etl, sample_tract_response):
        """Test poverty rate calculation."""
        df_raw = pd.DataFrame(
            sample_tract_response[1:], columns=sample_tract_response[0]
        )

        df = etl.transform(df_raw, 2022, "tract")

        # Tract 6: 50/1500 = 3.33%
        tract_6 = df[df["tract_fips"] == "21067000600"]
        assert "poverty_rate" in df.columns
        assert abs(tract_6["poverty_rate"].values[0] - 3.3) < 0.1

        # Tract 4: 800/2000 = 40%
        tract_4 = df[df["tract_fips"] == "21067000400"]
        assert abs(tract_4["poverty_rate"].values[0] - 40.0) < 0.1

    def test_transform_calculates_bachelors_plus(self, etl, sample_tract_response):
        """Test bachelor's degree calculation."""
        df_raw = pd.DataFrame(
            sample_tract_response[1:], columns=sample_tract_response[0]
        )

        df = etl.transform(df_raw, 2022, "tract")

        # Tract 6: 1200/2000 = 60%
        tract_6 = df[df["tract_fips"] == "21067000600"]
        assert "pct_bachelors_plus" in df.columns
        assert abs(tract_6["pct_bachelors_plus"].values[0] - 60.0) < 0.1

    def test_transform_calculates_broadband(self, etl, sample_tract_response):
        """Test broadband percentage calculation."""
        df_raw = pd.DataFrame(
            sample_tract_response[1:], columns=sample_tract_response[0]
        )

        df = etl.transform(df_raw, 2022, "tract")

        # Tract 6: 800/850 = 94.1%
        tract_6 = df[df["tract_fips"] == "21067000600"]
        assert "pct_broadband" in df.columns
        assert abs(tract_6["pct_broadband"].values[0] - 94.1) < 0.2

    def test_transform_long_format(self, etl, sample_tract_response):
        """Test transformation to long format."""
        df_raw = pd.DataFrame(
            sample_tract_response[1:], columns=sample_tract_response[0]
        )

        df_long = etl.transform_long(df_raw, 2022, "tract")

        # Should have multiple rows per tract (one per metric)
        assert len(df_long) > len(sample_tract_response) - 1
        assert "metric" in df_long.columns
        assert "value" in df_long.columns

    def test_transform_adds_metadata(self, etl, sample_tract_response):
        """Test that metadata fields are added."""
        df_raw = pd.DataFrame(
            sample_tract_response[1:], columns=sample_tract_response[0]
        )

        df = etl.transform(df_raw, 2022, "tract")

        assert df["year"].unique()[0] == 2022
        assert df["data_vintage"].unique()[0] == "2018-2022"
        assert df["geography_level"].unique()[0] == "tract"
        assert df["source"].unique()[0] == "Census ACS 5-Year"

    def test_load_creates_directory(self, etl, sample_tract_response, tmp_path):
        """Test that load creates output directory."""
        df_raw = pd.DataFrame(
            sample_tract_response[1:], columns=sample_tract_response[0]
        )
        df = etl.transform(df_raw, 2022, "tract")

        output_path = tmp_path / "subdir" / "test.csv"
        etl.load(df, output_path)

        assert output_path.exists()

    def test_load_writes_csv(self, etl, sample_tract_response, tmp_path):
        """Test that load writes correct CSV content."""
        df_raw = pd.DataFrame(
            sample_tract_response[1:], columns=sample_tract_response[0]
        )
        df = etl.transform(df_raw, 2022, "tract")

        output_path = tmp_path / "test.csv"
        etl.load(df, output_path)

        df_loaded = pd.read_csv(output_path)
        assert len(df_loaded) == len(df)

    @patch("requests.get")
    def test_run_tracts(self, mock_get, etl, sample_tract_response, tmp_path):
        """Test full ETL pipeline for tracts."""
        mock_response = MagicMock()
        mock_response.json.return_value = sample_tract_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        df = etl.run_tracts(2022, "067", tmp_path)

        # Check output file exists
        output_file = tmp_path / "census_acs_tracts_fayette_2022.csv"
        assert output_file.exists()

        # Check data
        assert len(df) == 3

    @patch("requests.get")
    def test_run_tracts_long_format(self, mock_get, etl, sample_tract_response, tmp_path):
        """Test ETL with long format output."""
        mock_response = MagicMock()
        mock_response.json.return_value = sample_tract_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        df = etl.run_tracts(2022, "067", tmp_path, long_format=True)

        output_file = tmp_path / "census_acs_tracts_fayette_2022_long.csv"
        assert output_file.exists()
        assert "metric" in df.columns

    @patch("requests.get")
    def test_run_counties(self, mock_get, etl, sample_county_response, tmp_path):
        """Test county-level ETL."""
        mock_response = MagicMock()
        mock_response.json.return_value = sample_county_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        df = etl.run_counties(2022, tmp_path)

        output_file = tmp_path / "census_acs_counties_2022.csv"
        assert output_file.exists()
        assert len(df) == 2

    def test_get_available_years(self, etl):
        """Test available years calculation."""
        years = etl.get_available_years()

        assert 2009 in years  # First ACS 5-year
        assert 2020 in years
        assert 2008 not in years


class TestCensusACSIntegration:
    """Integration tests hitting real Census API."""

    @pytest.mark.integration
    def test_real_api_fetch_tracts(self):
        """Test fetching real tract data from Census API."""
        etl = CensusACS5ETL()
        df = etl.extract_tracts(2022, "067")  # Fayette County

        # Should have ~80 tracts in Fayette County
        assert len(df) > 50

    @pytest.mark.integration
    def test_real_api_transform(self):
        """Test full extraction and transformation with real data."""
        etl = CensusACS5ETL()
        df_raw = etl.extract_tracts(2022, "067")
        df = etl.transform(df_raw, 2022, "tract")

        # Check derived metrics exist
        assert "poverty_rate" in df.columns
        assert "pct_bachelors_plus" in df.columns

        # Verify reasonable ranges
        poverty_valid = df["poverty_rate"].dropna()
        assert poverty_valid.min() >= 0
        assert poverty_valid.max() <= 100


class TestCensusACSEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_tract_name(self):
        """Test handling of empty tract names."""
        etl = CensusACS5ETL(api_key="test")

        raw_data = [
            ["NAME", "B19013_001E", "state", "county", "tract"],
            ["", "50000", "21", "067", "000100"],
        ]
        df_raw = pd.DataFrame(raw_data[1:], columns=raw_data[0])

        df = etl.transform(df_raw, 2022, "tract")
        assert len(df) == 1

    def test_all_missing_values(self):
        """Test handling when all values are missing."""
        etl = CensusACS5ETL(api_key="test")

        raw_data = [
            ["NAME", "B19013_001E", "B17001_001E", "B17001_002E", "state", "county", "tract"],
            ["Test Tract", "-666666666", "-666666666", "-666666666", "21", "067", "000100"],
        ]
        df_raw = pd.DataFrame(raw_data[1:], columns=raw_data[0])

        df = etl.transform(df_raw, 2022, "tract")

        # Should handle gracefully, not crash
        assert len(df) == 1
        assert pd.isna(df["median_household_income"].values[0])

    def test_zero_denominator_handling(self):
        """Test that zero denominators don't cause errors."""
        etl = CensusACS5ETL(api_key="test")

        raw_data = [
            ["NAME", "B17001_001E", "B17001_002E", "B15003_001E", "B15003_022E", "state", "county", "tract"],
            ["Test Tract", "0", "0", "0", "0", "21", "067", "000100"],
        ]
        df_raw = pd.DataFrame(raw_data[1:], columns=raw_data[0])

        df = etl.transform(df_raw, 2022, "tract")

        # Should produce NaN, not error
        assert len(df) == 1
