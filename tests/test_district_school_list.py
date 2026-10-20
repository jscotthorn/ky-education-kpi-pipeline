"""
Tests for District School List ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
import yaml
from etl.constants import KPI_COLUMNS
import shutil
from etl.district_school_list import transform, DistrictSchoolListETL

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "district_school_list"


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

        # Two schools, and each one also emits the school-type and Title I
        # indicators. The journal 105 failure expected 4 (latitude and
        # longitude only). 2 coordinates + 10 type indicators + 3 Title I
        # indicators = 15, and 15 * 2 = 30. This sample has no grade columns.
        assert len(kpi_df) == 30

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


def _config():
    """Production mappings.yaml entry. This source has no dtype cast."""
    loaded = yaml.safe_load((ROOT / "config" / "mappings.yaml").read_text())
    return loaded["sources"]["district_school_list"]


def _fixture(name: str) -> bytes:
    return (FIXTURE_DIR / name).read_bytes()


def _run(tmp_path: Path, names_and_bytes: list) -> pd.DataFrame:
    raw_dir = tmp_path / "raw"
    source_dir = raw_dir / "district_school_list"
    source_dir.mkdir(parents=True)
    proc_dir = tmp_path / "processed"
    proc_dir.mkdir()
    for name, payload in names_and_bytes:
        (source_dir / name).write_bytes(payload)
    transform(raw_dir, proc_dir, _config())
    output = proc_dir / "district_school_list.csv"
    assert output.exists()
    return pd.read_csv(output, dtype=str, keep_default_na=False)


def _rows(df: pd.DataFrame, year: str, school_id: str) -> pd.DataFrame:
    return df[(df["year"] == year) & (df["school_id"] == school_id)]


def _one(df: pd.DataFrame, year: str, school_id: str, metric: str) -> pd.Series:
    matched = _rows(df, year, school_id)
    matched = matched[matched["metric"] == metric]
    assert len(matched) == 1, (year, school_id, metric, len(matched))
    return matched.iloc[0]


class TestDistrictSchoolListKYRC26:
    def setup_method(self):
        self.etl = DistrictSchoolListETL("district_school_list")

    def test_kyrc26_file_is_accepted(self):
        """include_kyrc26 is set, so the 2025-26 file is not skipped."""
        assert self.etl.include_kyrc26 is True
        assert self.etl.accepts_data_file(Path("KYRC26_OVW_District_School_List.csv"))
        assert not self.etl.accepts_data_file(Path("KYRC23_OVW_District_School_List.csv"))
        assert not self.etl.accepts_data_file(Path("DISTRICT_SCHOOL_LIST_17-18.xlsx"))
        assert not self.etl.accepts_data_file(Path("District_School_List_2026.CSV"))
        assert not self.etl.accepts_data_file(Path("District_School_List_2025.CSV"))
        assert not self.etl.accepts_data_file(Path("District_School_List_2024.CSV"))

    def test_registered_sources_are_the_permanent_files(self):
        """KYRC26 is registered. KYRC23 and the 2017-18 workbook are not.

        Every registered prior-year URL returned 200. No temporary OAA
        name was ever registered.
        """
        config = yaml.safe_load((ROOT / "config" / "kde_sources.yaml").read_text())
        names = []
        hosts = {}
        for entry in config["raw_directories"]["district_school_list"]:
            if isinstance(entry, dict):
                names.append(entry["file"])
                hosts[entry["file"]] = entry.get("url")
            else:
                names.append(entry)
        assert names == [
            "KYRC24_OVW_District_School_List.csv",
            "KYRC25_OVW_District_School_List.csv",
            "KYRC26_OVW_District_School_List.csv",
            "district_school_list_2023.csv",
            "district_school_list_2022.csv",
            "district_school_list_2021.csv",
            "district_school_list_2020.csv",
            "DISTRICT_SCHOOL_LIST_18-19.xlsx",
        ]
        assert hosts["KYRC25_OVW_District_School_List.csv"] == "kyrc25"
        assert hosts["KYRC26_OVW_District_School_List.csv"] == "kyrc26"
        assert "KYRC23_OVW_District_School_List.csv" not in names
        assert "DISTRICT_SCHOOL_LIST_17-18.xlsx" not in names

    def test_school_and_district_rows_resolve_coop(self):
        """Jefferson is GLEC. Adair stays GRREC. The district row is not a school.

        KYRC25 left the Jefferson co-op cell blank. It did not say GREC.
        KYRC26 fills that cell with GLEC and keeps co-op code 909.
        """
        frame = pd.read_csv(FIXTURE_DIR / "kyrc26_real_rows.csv", dtype=str, keep_default_na=False)
        normalized = self.etl.normalize_column_names(frame)
        school = normalized[normalized["school_code"] == "275004"].iloc[0]
        district = normalized[normalized["school_code"] == "275000"].iloc[0]
        adair = normalized[normalized["school_code"] == "001010"].iloc[0]
        assert school["co_op"] == "GLEC"
        assert school["co_op_code"] == "909"
        assert district["co_op"] == "GLEC"
        assert district["co_op_code"] == "909"
        assert adair["co_op"] == "GRREC"
        assert adair["co_op_code"] == "902"
        assert "GREC" not in set(normalized["co_op"])
        assert self.etl.should_skip_row(district)
        assert not self.etl.should_skip_row(school)
        ninth = normalized[normalized["school_code"] == "134150"].iloc[0]
        assert ninth["school_name"] == "Ninth District Elementary"
        assert not self.etl.should_skip_row(ninth)
        state = normalized[normalized["school_code"] == "999000"].iloc[0]
        assert state["district_name"] == "---State---"
        assert state["school_name"] == "---State---"
        assert not self.etl.should_skip_row(state)

    def test_both_coop_spellings_collapse_to_one_column(self):
        """A frame with Co-Op and CO-OP still resolves co_op.

        BaseETL maps both headers to the same field. A blank in the first
        spelling is filled from the second.
        """
        frame = pd.read_csv(FIXTURE_DIR / "kyrc26_real_rows.csv", dtype=str, keep_default_na=False)
        frame.insert(10, "Co-Op", "")
        frame.insert(11, "Co-Op Code", "")
        school_mask = frame["School Code"] == "275004"
        frame.loc[school_mask, "CO-OP"] = ""
        frame.loc[school_mask, "Co-Op"] = "GLEC"
        frame.loc[school_mask, "CO-OP Code"] = ""
        frame.loc[school_mask, "Co-Op Code"] = "909"
        district_mask = frame["School Code"] == "275000"
        frame.loc[district_mask, "Co-Op"] = "GLEC"
        frame.loc[district_mask, "Co-Op Code"] = "909"
        normalized = self.etl.normalize_column_names(frame)
        assert list(normalized.columns).count("co_op") == 1
        assert list(normalized.columns).count("co_op_code") == 1
        school = normalized[normalized["school_code"] == "275004"].iloc[0]
        district = normalized[normalized["school_code"] == "275000"].iloc[0]
        assert school["co_op"] == "GLEC"
        assert school["co_op_code"] == "909"
        assert district["co_op"] == "GLEC"
        assert not self.etl.should_skip_row(school)
        assert self.etl.should_skip_row(district)

    def test_kyrc26_real_rows(self, tmp_path):
        """State, Adair, Jefferson, a technical center, and a comma address.

        District aggregates are not KPI rows. A blank coordinate is not a
        school. There is no star and no suppressed group. Year is 2026.
        The state district is ---State---, not All Districts.
        """
        df = _run(tmp_path, [("KYRC26_OVW_District_School_List.csv", _fixture("kyrc26_real_rows.csv"))])
        assert list(df.columns) == KPI_COLUMNS
        keys = ["year", "school_id", "student_group", "metric"]
        assert not df.duplicated(keys).any()
        assert set(df["year"]) == {"2026"}
        assert set(df["student_group"]) == {"All Students"}
        assert set(df["suppressed"]) == {"N"}
        assert "001000" not in set(df["school_id"])
        assert "275000" not in set(df["school_id"])
        assert "132035" not in set(df["school_id"])

        state = _one(df, "2026", "999000", "school_latitude")
        assert state["district"] == "---State---"
        assert state["school_name"] == "---State---"
        assert state["value"] == "38.184805"
        assert state["co_op"] == ""
        assert _one(df, "2026", "999000", "school_longitude")["value"] == "-84.848799"
        assert _one(df, "2026", "999000", "school_low_grade")["value"] == "-1.0"
        assert _one(df, "2026", "999000", "school_high_grade")["value"] == "12.0"
        assert _one(df, "2026", "999000", "title_i_schoolwide")["value"] == "0.0"

        adair = _one(df, "2026", "001010", "school_latitude")
        assert adair["district"] == "Adair County"
        assert adair["school_name"] == "Adair County High School"
        assert adair["value"] == "37.107858"
        assert adair["co_op"] == "GRREC"
        assert adair["co_op_code"] == "902"
        assert adair["school_type"] == "A1"
        assert _one(df, "2026", "001010", "school_longitude")["value"] == "-85.328527"
        assert _one(df, "2026", "001010", "school_low_grade")["value"] == "9.0"
        assert _one(df, "2026", "001010", "school_high_grade")["value"] == "12.0"
        assert _one(df, "2026", "001010", "title_i_schoolwide")["value"] == "1.0"
        assert _one(df, "2026", "001010", "school_type_a2")["value"] == "0.0"

        taylor = _one(df, "2026", "275004", "school_latitude")
        assert taylor["co_op"] == "GLEC"
        assert taylor["co_op_code"] == "909"
        assert taylor["school_name"] == "Camp Taylor Elementary"
        assert taylor["value"] == "38.196553"
        assert _one(df, "2026", "275004", "school_longitude")["value"] == "-85.719415"
        assert _one(df, "2026", "275004", "school_low_grade")["value"] == "-1.0"
        assert _one(df, "2026", "275004", "school_high_grade")["value"] == "5.0"
        assert _one(df, "2026", "275004", "title_i_schoolwide")["value"] == "1.0"

        eastern = _one(df, "2026", "275007", "school_latitude")
        assert eastern["co_op"] == "GLEC"
        assert eastern["value"] == "38.243661"
        assert eastern["school_name"] == "Eastern High"
        assert _one(df, "2026", "275007", "title_i_eligible_no_program")["value"] == "1.0"
        assert _one(df, "2026", "275007", "title_i_schoolwide")["value"] == "0.0"

        tech = _one(df, "2026", "381904", "school_type_a2")
        assert tech["value"] == "1.0"
        assert tech["co_op"] == "WKEC"
        assert _one(df, "2026", "381904", "school_latitude")["value"] == "36.912372"
        assert _one(df, "2026", "381904", "title_i_schoolwide")["value"] == "0.0"

        baker = _one(df, "2026", "121026", "school_latitude")
        assert baker["value"] == "38.002886373"
        assert _one(df, "2026", "121026", "school_longitude")["value"] == "-84.219262528"
        assert _one(df, "2026", "121026", "school_low_grade")["value"] == "5.0"
        assert _one(df, "2026", "121026", "school_high_grade")["value"] == "6.0"

        ninth = _one(df, "2026", "134150", "school_latitude")
        assert ninth["school_name"] == "Ninth District Elementary"
        assert ninth["co_op"] == "NKCES"
        assert ninth["value"] == "39.052306"

    def test_prior_year_anchors_and_duplicate_school(self, tmp_path):
        """2020, 2023, 2024, and 2025 anchors. The repeated 2024 code is stored once.

        The two Knott County rows differ in the contact name, which is not
        a metric. Coordinates and classification match. 2023 state school
        id stays 999. Jefferson's 2025 co-op cell stays blank.
        """
        df = _run(
            tmp_path,
            [
                ("district_school_list_2020.csv", _fixture("district_school_list_2020_rows.csv")),
                ("district_school_list_2023.csv", _fixture("district_school_list_2023_rows.csv")),
                ("KYRC24_OVW_District_School_List.csv", _fixture("kyrc24_real_rows.csv")),
                ("KYRC25_OVW_District_School_List.csv", _fixture("kyrc25_real_rows.csv")),
            ],
        )
        keys = ["year", "school_id", "student_group", "metric"]
        assert not df.duplicated(keys).any()
        assert "061" not in set(df["school_id"])
        assert "001000" not in set(df["school_id"])

        adair_2020 = _one(df, "2020", "001010", "school_latitude")
        assert adair_2020["value"] == "37.107858"
        assert adair_2020["co_op"] == "GRREC"
        assert adair_2020["district"] == "Adair County"
        assert _one(df, "2020", "999", "school_latitude")["district"] == "State"
        assert _one(df, "2020", "999", "school_latitude")["school_name"] == "---State Total---"
        assert _one(df, "2020", "999", "school_latitude")["value"] == "38.184805"

        state_2023 = _one(df, "2023", "999", "school_latitude")
        assert state_2023["district"] == "State"
        assert state_2023["school_name"] == "---State Total---"
        assert state_2023["value"] == "38.184805"
        assert _one(df, "2023", "001010", "school_longitude")["value"] == "-85.328527"

        knot = _rows(df, "2024", "777125")
        assert len(knot) == 17
        assert _one(df, "2024", "777125", "school_latitude")["value"] == "37.319839"
        assert _one(df, "2024", "777125", "school_longitude")["value"] == "-82.958672"
        assert _one(df, "2024", "777125", "school_type_c2")["value"] == "1.0"
        assert _one(df, "2024", "777125", "school_low_grade")["value"] == "8.0"
        assert _one(df, "2024", "777125", "school_high_grade")["value"] == "12.0"
        assert _one(df, "2024", "999000", "school_latitude")["district"] == "State"
        assert _one(df, "2024", "999000", "school_latitude")["value"] == "38.184805"

        taylor = _one(df, "2025", "275004", "school_latitude")
        assert taylor["value"] == "38.196553"
        assert taylor["co_op"] == ""
        assert taylor["co_op_code"] == "909"
        assert _one(df, "2025", "999000", "school_latitude")["district"] == "State"
        assert _one(df, "2025", "999000", "school_latitude")["school_name"] == "---State Total---"
        assert _one(df, "2025", "001010", "school_latitude")["co_op"] == "GRREC"

    def test_leftover_files_do_not_add_rows(self, tmp_path):
        """A KYRC23 republish, a 2017-18 workbook, and a temp name add nothing."""
        from openpyxl import Workbook

        raw_dir = tmp_path / "raw" / "district_school_list"
        raw_dir.mkdir(parents=True)
        proc_dir = tmp_path / "processed"
        proc_dir.mkdir()
        (raw_dir / "KYRC26_OVW_District_School_List.csv").write_bytes(_fixture("kyrc26_real_rows.csv"))
        (raw_dir / "KYRC23_OVW_District_School_List.csv").write_bytes(_fixture("kyrc25_real_rows.csv"))
        (raw_dir / "District_School_List_2026.CSV").write_bytes(_fixture("kyrc26_real_rows.csv"))
        workbook = Workbook()
        sheet = workbook.active
        sheet.title = "DATA"
        sheet.append(["SCH_YEAR", "DIST_NAME", "SCH_NAME", "SCH_CD", "LATITUDE", "LONGITUDE", "SCH_TYPE"])
        sheet.append(["20172018", "State", "---State Total---", "999", "38.184805", "-84.848799", "A1"])
        workbook.save(raw_dir / "DISTRICT_SCHOOL_LIST_17-18.xlsx")
        transform(tmp_path / "raw", proc_dir, _config())
        df = pd.read_csv(proc_dir / "district_school_list.csv", dtype=str, keep_default_na=False)
        assert set(df["year"]) == {"2026"}
        assert set(df["source_file"]) == {"KYRC26_OVW_District_School_List.csv"}
        assert "2018" not in set(df["year"])

    def test_production_config_has_no_int_cast(self):
        """mappings.yaml does not cast this source. pandas stays unpinned."""
        config = _config()
        assert "dtype" not in config or not config.get("dtype")
