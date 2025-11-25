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
Unit tests for Census SAIPE ETL module.

Tests the extraction, transformation, and loading of Census SAIPE
income and poverty data for Kentucky counties.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from etl.census_saipe import CensusSAIPEETL, KY_COUNTIES


class TestCensusSAIPEETL:
    """Test suite for CensusSAIPEETL class."""

    @pytest.fixture
    def etl(self):
        """Create ETL instance with test API key."""
        return CensusSAIPEETL(api_key="test_key")

    @pytest.fixture
    def sample_api_response(self):
        """Sample Census API response data."""
        return [
            # Header row
            [
                "NAME",
                "SAEMHI_PT",
                "SAEPOVRTALL_PT",
                "SAEPOVRT0_17_PT",
                "SAEPOVRT5_17R_PT",
                "state",
                "county",
                "time",
            ],
            # Fayette County
            [
                "Fayette County, Kentucky",
                "67320",
                "15.7",
                "18.2",
                "17.2",
                "21",
                "067",
                "2023",
            ],
            # Jefferson County
            [
                "Jefferson County, Kentucky",
                "58321",
                "14.2",
                "19.5",
                "18.1",
                "21",
                "111",
                "2023",
            ],
            # Bell County (low income, high poverty)
            [
                "Bell County, Kentucky",
                "35566",
                "29.8",
                "42.1",
                "40.5",
                "21",
                "013",
                "2023",
            ],
        ]

    def test_init_with_api_key(self):
        """Test initialization with provided API key."""
        etl = CensusSAIPEETL(api_key="my_key")
        assert etl.api_key == "my_key"

    def test_init_without_api_key(self):
        """Test initialization without API key logs warning."""
        with patch.dict("os.environ", {}, clear=True):
            with patch("etl.census_saipe.logger") as mock_logger:
                etl = CensusSAIPEETL()
                mock_logger.warning.assert_called()

    def test_ky_counties_complete(self):
        """Test that KY_COUNTIES has all 120 Kentucky counties."""
        assert len(KY_COUNTIES) == 120

    def test_ky_counties_fayette(self):
        """Test Fayette County is correctly mapped."""
        assert KY_COUNTIES["067"] == "Fayette"

    def test_ky_counties_jefferson(self):
        """Test Jefferson County is correctly mapped."""
        assert KY_COUNTIES["111"] == "Jefferson"

    @patch("requests.get")
    def test_extract_success(self, mock_get, etl, sample_api_response):
        """Test successful data extraction from Census API."""
        mock_response = MagicMock()
        mock_response.json.return_value = sample_api_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        df = etl.extract(2023)

        assert len(df) == 3  # 3 counties in sample
        assert "NAME" in df.columns
        assert "SAEMHI_PT" in df.columns
        mock_get.assert_called_once()

    @patch("requests.get")
    def test_extract_api_error(self, mock_get, etl):
        """Test handling of API request errors."""
        from requests.exceptions import RequestException

        mock_get.side_effect = RequestException("API Error")

        with pytest.raises(RequestException):
            etl.extract(2023)

    @patch("requests.get")
    def test_extract_invalid_json(self, mock_get, etl):
        """Test handling of invalid JSON response."""
        mock_response = MagicMock()
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        with pytest.raises(ValueError, match="Invalid JSON"):
            etl.extract(2023)

    @patch("requests.get")
    def test_extract_empty_response(self, mock_get, etl):
        """Test handling of empty API response."""
        mock_response = MagicMock()
        mock_response.json.return_value = []
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        with pytest.raises(ValueError, match="No data returned"):
            etl.extract(2023)

    def test_transform_basic(self, etl, sample_api_response):
        """Test basic data transformation."""
        # Create DataFrame from sample response
        df_raw = pd.DataFrame(sample_api_response[1:], columns=sample_api_response[0])

        df_transformed = etl.transform(df_raw, 2023)

        # Check we have 4 metrics x 3 counties = 12 rows
        assert len(df_transformed) == 12

        # Check required columns exist
        required_cols = [
            "year",
            "geography_level",
            "county_fips",
            "county_name",
            "metric",
            "value",
            "data_vintage",
            "source",
        ]
        for col in required_cols:
            assert col in df_transformed.columns

    def test_transform_county_fips(self, etl, sample_api_response):
        """Test that county FIPS codes are correctly formed."""
        df_raw = pd.DataFrame(sample_api_response[1:], columns=sample_api_response[0])
        df_transformed = etl.transform(df_raw, 2023)

        # All FIPS codes should start with 21 (Kentucky)
        fips_codes = df_transformed["county_fips"].unique()
        for fips in fips_codes:
            assert str(fips).startswith("21")

    def test_transform_county_names(self, etl, sample_api_response):
        """Test that county names are correctly mapped."""
        df_raw = pd.DataFrame(sample_api_response[1:], columns=sample_api_response[0])
        df_transformed = etl.transform(df_raw, 2023)

        county_names = df_transformed["county_name"].unique()
        assert "Fayette" in county_names
        assert "Jefferson" in county_names
        assert "Bell" in county_names

    def test_transform_metrics(self, etl, sample_api_response):
        """Test that all expected metrics are present."""
        df_raw = pd.DataFrame(sample_api_response[1:], columns=sample_api_response[0])
        df_transformed = etl.transform(df_raw, 2023)

        metrics = df_transformed["metric"].unique()
        expected_metrics = [
            "median_household_income",
            "poverty_rate_all_ages",
            "poverty_rate_0_17",
            "poverty_rate_5_17",
        ]

        for metric in expected_metrics:
            assert metric in metrics

    def test_transform_numeric_values(self, etl, sample_api_response):
        """Test that values are converted to numeric."""
        df_raw = pd.DataFrame(sample_api_response[1:], columns=sample_api_response[0])
        df_transformed = etl.transform(df_raw, 2023)

        # Check Fayette median income
        fayette_income = df_transformed[
            (df_transformed["county_name"] == "Fayette")
            & (df_transformed["metric"] == "median_household_income")
        ]["value"].values[0]

        assert fayette_income == 67320.0
        assert isinstance(fayette_income, float)

    def test_transform_metadata(self, etl, sample_api_response):
        """Test that metadata fields are correctly set."""
        df_raw = pd.DataFrame(sample_api_response[1:], columns=sample_api_response[0])
        df_transformed = etl.transform(df_raw, 2023)

        assert df_transformed["year"].unique()[0] == 2023
        assert df_transformed["data_vintage"].unique()[0] == "2023"
        assert df_transformed["geography_level"].unique()[0] == "county"
        assert df_transformed["source"].unique()[0] == "Census SAIPE"

    def test_load_creates_directory(self, etl, sample_api_response, tmp_path):
        """Test that load creates output directory if needed."""
        df_raw = pd.DataFrame(sample_api_response[1:], columns=sample_api_response[0])
        df_transformed = etl.transform(df_raw, 2023)

        output_path = tmp_path / "subdir" / "test.csv"
        etl.load(df_transformed, output_path)

        assert output_path.exists()
        assert output_path.parent.exists()

    def test_load_writes_csv(self, etl, sample_api_response, tmp_path):
        """Test that load writes correct CSV content."""
        df_raw = pd.DataFrame(sample_api_response[1:], columns=sample_api_response[0])
        df_transformed = etl.transform(df_raw, 2023)

        output_path = tmp_path / "test.csv"
        etl.load(df_transformed, output_path)

        # Read back and verify
        df_loaded = pd.read_csv(output_path)
        assert len(df_loaded) == len(df_transformed)
        assert list(df_loaded.columns) == list(df_transformed.columns)

    @patch("requests.get")
    def test_run_full_pipeline(self, mock_get, etl, sample_api_response, tmp_path):
        """Test full ETL pipeline."""
        mock_response = MagicMock()
        mock_response.json.return_value = sample_api_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        df = etl.run(2023, tmp_path)

        # Check output file was created
        output_file = tmp_path / "census_saipe_2023.csv"
        assert output_file.exists()

        # Check returned DataFrame
        assert len(df) == 12  # 4 metrics x 3 counties

    @patch("requests.get")
    def test_run_multiple_years(self, mock_get, etl, sample_api_response, tmp_path):
        """Test multi-year ETL pipeline."""
        mock_response = MagicMock()
        mock_response.json.return_value = sample_api_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        df = etl.run_multiple_years([2022, 2023], tmp_path)

        # Check individual year files
        assert (tmp_path / "census_saipe_2022.csv").exists()
        assert (tmp_path / "census_saipe_2023.csv").exists()

        # Check combined file
        assert (tmp_path / "census_saipe_combined.csv").exists()

        # Check combined DataFrame has both years
        assert len(df) == 24  # 12 records x 2 years

    def test_get_county_crosswalk(self, etl):
        """Test county crosswalk generation."""
        crosswalk = etl.get_county_crosswalk()

        assert len(crosswalk) == 120
        assert "county_fips" in crosswalk.columns
        assert "county_name" in crosswalk.columns

        # Check Fayette County
        fayette = crosswalk[crosswalk["county_name"] == "Fayette"]
        assert len(fayette) == 1
        assert fayette["county_fips"].values[0] == "21067"

    def test_get_available_years(self, etl):
        """Test available years calculation."""
        years = etl.get_available_years()

        assert 2000 in years
        assert 2020 in years
        assert 1999 not in years  # Before SAIPE annual estimates
        assert len(years) > 20  # Should have 20+ years of data


class TestCensusSAIPEIntegration:
    """Integration tests that hit the real Census API."""

    @pytest.mark.integration
    def test_real_api_fetch(self):
        """Test fetching real data from Census API (requires network)."""
        etl = CensusSAIPEETL()
        df = etl.extract(2022)

        # Should have 120 Kentucky counties
        assert len(df) == 120

        # Check expected columns
        assert "SAEMHI_PT" in df.columns
        assert "SAEPOVRTALL_PT" in df.columns

    @pytest.mark.integration
    def test_real_api_transform(self):
        """Test full extraction and transformation with real data."""
        etl = CensusSAIPEETL()
        df_raw = etl.extract(2022)
        df = etl.transform(df_raw, 2022)

        # Should have 4 metrics x 120 counties = 480 rows
        assert len(df) == 480

        # Check Fayette County exists
        fayette = df[df["county_name"] == "Fayette"]
        assert len(fayette) == 4  # 4 metrics

        # Check median income is reasonable
        fayette_income = fayette[fayette["metric"] == "median_household_income"][
            "value"
        ].values[0]
        assert 40000 < fayette_income < 150000  # Reasonable range


class TestCensusSAIPEEdgeCases:
    """Test edge cases and error handling."""

    def test_missing_values_handled(self):
        """Test that missing values in API response are handled."""
        etl = CensusSAIPEETL(api_key="test")

        # Response with some missing values
        raw_data = [
            [
                "NAME",
                "SAEMHI_PT",
                "SAEPOVRTALL_PT",
                "SAEPOVRT0_17_PT",
                "SAEPOVRT5_17R_PT",
                "state",
                "county",
                "time",
            ],
            ["Test County, KY", "", "15.0", "N/A", None, "21", "001", "2023"],
        ]
        df_raw = pd.DataFrame(raw_data[1:], columns=raw_data[0])
        df = etl.transform(df_raw, 2023)

        # Should have 4 rows (one per metric)
        assert len(df) == 4

        # Missing income should be NaN
        income = df[df["metric"] == "median_household_income"]["value"].values[0]
        assert pd.isna(income)

    def test_special_characters_in_county_name(self):
        """Test handling of county names with special characters."""
        etl = CensusSAIPEETL(api_key="test")

        raw_data = [
            [
                "NAME",
                "SAEMHI_PT",
                "SAEPOVRTALL_PT",
                "SAEPOVRT0_17_PT",
                "SAEPOVRT5_17R_PT",
                "state",
                "county",
                "time",
            ],
            [
                "O'Brien County, Kentucky",
                "50000",
                "15.0",
                "18.0",
                "17.0",
                "21",
                "999",
                "2023",
            ],
        ]
        df_raw = pd.DataFrame(raw_data[1:], columns=raw_data[0])

        # Should not raise an error
        df = etl.transform(df_raw, 2023)
        assert len(df) == 4
