"""
Tests for District School List ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
from etl.constants import KPI_COLUMNS
import shutil
from etl.district_school_list import transform, DistrictSchoolListETL


class TestDistrictSchoolListETL:

    def setup_method(self):
        """Setup test directories and sample data."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)

        # Create sample raw data directory
        self.sample_dir = self.raw_dir / "district_school_list"
        self.sample_dir.mkdir(parents=True)

        # Create ETL instance for testing
        self.etl = DistrictSchoolListETL('district_school_list')

    def teardown_method(self):
        """Clean up test directories."""
        shutil.rmtree(self.test_dir)

    def create_sample_2024_data(self):
        """Create sample KYRC24 format data."""
        data = pd.DataFrame({
            'School Year': ['20232024', '20232024', '20232024', '20232024'],
            'County Number': ['001', '001', '001', '001'],
            'County Name': ['Adair', 'Adair', 'Adair', 'Adair'],
            'District Number': ['001', '001', '001', '001'],
            'District Name': ['Adair County', 'Adair County', 'Adair County', 'Adair County'],
            'School Number': ['010', '011', '999', ''],
            'School Name': ['Adair County High School', 'Adair County Elementary', 'All Schools', '---District Total---'],
            'School Code': ['001010', '001011', '001000', '001'],
            'State School Id': ['001001010', '001001011', '', ''],
            'NCES Id': ['210003000001', '210003000002', '', ''],
            'Co-Op': ['GRREC', 'GRREC', 'GRREC', 'GRREC'],
            'Co-Op Code': ['902', '902', '902', '902'],
            'School Type': ['A1', 'A1', '', ''],
            'Latitude': ['37.107858', '37.111644', '', '37.105343'],
            'Longitude': ['-85.328527', '-85.329785', '', '-85.322045']
        })
        return data

    def create_sample_2025_data(self):
        """Create sample KYRC25 format data."""
        data = pd.DataFrame({
            'School Year': ['20242025', '20242025', '20242025'],
            'County Number': ['001', '001', '001'],
            'County Name': ['ADAIR', 'ADAIR', 'ADAIR'],
            'District Number': ['001', '001', '001'],
            'District Name': ['Adair County', 'Adair County', 'Adair County'],
            'School Number': ['010', '014', ''],
            'School Code': ['001010', '001014', '001'],
            'School Name': ['Adair County High School', 'Adair County Middle School', ''],
            'State School Id': ['001001010', '001001014', ''],
            'NCES Id': ['210003000001', '210003001919', ''],
            'Co-Op': ['GRREC', 'GRREC', 'GRREC'],
            'Co-Op Code': ['902', '902', '902'],
            'School Type': ['A1', 'A1', ''],
            'Latitude': ['37.107858', '37.103889', '37.105343'],
            'Longitude': ['-85.328527', '-85.324549', '-85.322045']
        })
        return data

    def test_column_mappings(self):
        """Test that column mappings include required fields."""
        mappings = self.etl.module_column_mappings

        assert 'Latitude' in mappings
        assert 'Longitude' in mappings
        assert mappings['Latitude'] == 'latitude'
        assert mappings['Longitude'] == 'longitude'

    def test_extract_metrics(self):
        """Test metric extraction from sample data."""
        sample_data = self.create_sample_2024_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        # Test first row (school with valid coordinates)
        row = sample_data.iloc[0]
        metrics = self.etl.extract_metrics(row)

        assert 'school_latitude' in metrics
        assert 'school_longitude' in metrics
        assert metrics['school_latitude'] == 37.107858
        assert metrics['school_longitude'] == -85.328527

        # Test second row
        row = sample_data.iloc[1]
        metrics = self.etl.extract_metrics(row)
        assert metrics['school_latitude'] == 37.111644
        assert metrics['school_longitude'] == -85.329785

    def test_extract_metrics_missing_coordinates(self):
        """Test extraction with missing coordinates."""
        data = pd.DataFrame({
            'School Year': ['20232024'],
            'County Number': ['001'],
            'District Number': ['001'],
            'School Number': ['010'],
            'School Code': ['001010'],
            'District Name': ['Test'],
            'School Name': ['Test School'],
            'Latitude': [pd.NA],
            'Longitude': [pd.NA]
        })

        data = self.etl.normalize_column_names(data)
        row = data.iloc[0]

        # Should skip row with missing coordinates
        assert self.etl.should_skip_row(row)

    def test_extract_metrics_invalid_coordinates(self):
        """Test that invalid coordinates outside Kentucky range are rejected."""
        data = pd.DataFrame({
            'School Year': ['20232024', '20232024'],
            'County Number': ['001', '001'],
            'District Number': ['001', '001'],
            'School Number': ['010', '011'],
            'School Code': ['001010', '001011'],
            'District Name': ['Test', 'Test'],
            'School Name': ['School A', 'School B'],
            # First row: valid Kentucky coords, second row: invalid (Florida)
            'Latitude': ['37.5', '25.0'],
            'Longitude': ['-85.5', '-80.0']
        })

        data = self.etl.normalize_column_names(data)

        # Valid Kentucky coordinates
        row1 = data.iloc[0]
        metrics1 = self.etl.extract_metrics(row1)
        assert 'school_latitude' in metrics1
        assert 'school_longitude' in metrics1

        # Invalid coordinates (outside Kentucky)
        row2 = data.iloc[1]
        metrics2 = self.etl.extract_metrics(row2)
        # Should not extract invalid coordinates
        assert 'school_latitude' not in metrics2
        assert 'school_longitude' not in metrics2

    def test_should_skip_district_totals(self):
        """Test that district total rows are skipped."""
        sample_data = self.create_sample_2024_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        # Row with "All Schools" should be skipped
        row_all_schools = sample_data.iloc[2]
        assert self.etl.should_skip_row(row_all_schools)

        # Row with "---District Total---" should be skipped
        row_district_total = sample_data.iloc[3]
        assert self.etl.should_skip_row(row_district_total)

        # Valid school row should not be skipped
        row_valid = sample_data.iloc[0]
        assert not self.etl.should_skip_row(row_valid)

    def test_should_skip_empty_school_name(self):
        """Test that rows with empty school names in 2025 format are skipped."""
        sample_data = self.create_sample_2025_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        # Row with empty school name (district total in 2025 format)
        row_empty_name = sample_data.iloc[2]
        assert self.etl.should_skip_row(row_empty_name)

    def test_convert_to_kpi_format(self):
        """Test conversion to KPI format."""
        sample_data = self.create_sample_2024_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        kpi_df = self.etl.convert_to_kpi_format(sample_data, "test_file.csv")

        # Check that DataFrame has correct columns
        assert set(kpi_df.columns).issubset(set(KPI_COLUMNS))

        # Check that we have the right number of KPI rows
        # 2 valid schools * 2 metrics (lat, long) = 4 rows
        assert len(kpi_df) == 4

        # Check specific metrics exist
        metrics = kpi_df['metric'].unique()
        assert 'school_latitude' in metrics
        assert 'school_longitude' in metrics

        # Check values are correct
        high_school_lat = kpi_df[
            (kpi_df['school_id'] == '001010') &
            (kpi_df['metric'] == 'school_latitude')
        ].iloc[0]
        assert high_school_lat['value'] == 37.107858
        assert high_school_lat['year'] == '2024'
        assert high_school_lat['student_group'] == 'All Students'

    def test_suppressed_metric_defaults(self):
        """Test suppressed metric defaults."""
        sample_row = pd.Series({'latitude': '37.5', 'longitude': '-85.5'})
        defaults = self.etl.get_suppressed_metric_defaults(sample_row)

        assert 'school_latitude' in defaults
        assert 'school_longitude' in defaults
        assert pd.isna(defaults['school_latitude'])
        assert pd.isna(defaults['school_longitude'])

    def test_transform_integration(self):
        """Test complete transform function integration."""
        # Create sample files
        sample_2024 = self.create_sample_2024_data()
        sample_2025 = self.create_sample_2025_data()

        # Save to test directory
        sample_2024.to_csv(self.sample_dir / "KYRC24_OVW_District_School_List.csv", index=False)
        sample_2025.to_csv(self.sample_dir / "KYRC25_OVW_District_School_List.csv", index=False)

        # Run transform
        config = {}
        transform(self.raw_dir, self.proc_dir, config)

        # Check output file was created
        output_file = self.proc_dir / "district_school_list.csv"
        assert output_file.exists()

        # Load and validate output
        result_df = pd.read_csv(output_file)
        assert not result_df.empty
        assert set(result_df.columns).issubset(set(KPI_COLUMNS))

        # Check that we have data from both source files
        source_files = result_df['source_file'].unique()
        assert any('KYRC24' in f for f in source_files)
        assert any('KYRC25' in f for f in source_files)

        # Check years
        years = result_df['year'].astype(str).unique()
        assert '2024' in years
        assert '2025' in years

        # All records should have "All Students" as student_group
        unique_groups = result_df['student_group'].unique()
        assert len(unique_groups) == 1
        assert unique_groups[0] == 'All Students'

    def test_latitude_longitude_ranges(self):
        """Test that coordinates are within valid Kentucky ranges."""
        sample_data = self.create_sample_2024_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        kpi_df = self.etl.convert_to_kpi_format(sample_data, "test_file.csv")

        # Check latitude range (Kentucky is ~36-39 degrees)
        lat_rows = kpi_df[kpi_df['metric'] == 'school_latitude']
        assert (lat_rows['value'] >= 35.0).all()
        assert (lat_rows['value'] <= 40.0).all()

        # Check longitude range (Kentucky is ~-89 to -82 degrees)
        lon_rows = kpi_df[kpi_df['metric'] == 'school_longitude']
        assert (lon_rows['value'] >= -90.0).all()
        assert (lon_rows['value'] <= -80.0).all()

    def test_historical_data_format(self):
        """Test that historical data format (pre-2024) is handled correctly."""
        # Historical format has UPPERCASE column names
        data = pd.DataFrame({
            'SCHOOL YEAR': ['20212022'],
            'COUNTY NUMBER': ['001'],
            'COUNTY NAME': ['ADAIR'],
            'DISTRICT NUMBER': ['001'],
            'DISTRICT NAME': ['Adair County'],
            'SCHOOL NUMBER': ['010'],
            'SCHOOL NAME': ['Adair County High School'],
            'SCHOOL CODE': ['001010'],
            'STATE SCHOOL ID': ['001001010'],
            'NCES ID': ['210003000001'],
            'CO-OP': ['GRREC'],
            'CO-OP CODE': ['902'],
            'SCHOOL TYPE': ['A1'],
            'LATITUDE': ['37.107858'],
            'LONGITUDE': ['-85.328527']
        })

        data = self.etl.normalize_column_names(data)

        # Verify column normalization worked
        assert 'latitude' in data.columns
        assert 'longitude' in data.columns

        row = data.iloc[0]
        metrics = self.etl.extract_metrics(row)

        assert metrics['school_latitude'] == 37.107858
        assert metrics['school_longitude'] == -85.328527
