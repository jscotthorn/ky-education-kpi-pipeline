"""
Tests for Teacher Certification ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
import yaml
from etl.constants import KPI_COLUMNS
import shutil
from etl.teacher_certification import transform, TeacherCertificationETL

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "teacher_certification"


class TestTeacherCertificationETL:

    def setup_method(self):
        """Setup test directories and sample data."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)

        # Create sample raw data directory
        self.sample_dir = self.raw_dir / "teacher_certification"
        self.sample_dir.mkdir(parents=True)

        # Create ETL instance for testing
        self.etl = TeacherCertificationETL('teacher_certification')

    def teardown_method(self):
        """Clean up test directories."""
        shutil.rmtree(self.test_dir)

    def create_sample_2024_data(self):
        """Create sample KYRC24/25 format data."""
        data = pd.DataFrame({
            'School Year': ['20232024', '20232024', '20232024'],
            'County Number': ['001', '001', '001'],
            'County Name': ['Adair', 'Adair', 'Adair'],
            'District Number': ['001', '001', '001'],
            'District Name': ['Adair County', 'Adair County', 'Adair County'],
            'School Number': ['010', '011', ''],
            'School Name': ['Adair County High School', 'Adair County Middle', 'All Schools'],
            'School Code': ['001010', '001011', '001000'],
            'State School Id': ['001001010', '001001011', ''],
            'NCES ID': ['210003000001', '210003000002', ''],
            'CO-OP': ['GRREC', 'GRREC', 'GRREC'],
            'CO-OP Code': ['902', '902', '902'],
            'School Type': ['A1', 'A1', ''],
            'Teacher Count': ['50', '35', '185'],
            'Emergency Provisional Teacher Count': ['3', '2', '8'],
            'Percent Emergency Provisional Teachers': ['6.0', '5.7', '4.3'],
            'National Board Certified Count': ['5', '2', '15'],
            'Percent National Board Certified Teacher': ['10.0', '5.7', '8.1']
        })
        return data

    def create_sample_historical_data(self):
        """Create sample historical format data (uppercase columns)."""
        data = pd.DataFrame({
            'SCHOOL YEAR': ['20202021', '20202021'],
            'COUNTY NUMBER': ['001', '001'],
            'COUNTY NAME': ['ADAIR', 'ADAIR'],
            'DISTRICT NUMBER': ['001', '001'],
            'DISTRICT NAME': ['Adair County', 'Adair County'],
            'SCHOOL NUMBER': ['010', ''],
            'SCHOOL NAME': ['Adair County High School', '---District Total---'],
            'SCHOOL CODE': ['001010', '001'],
            'STATE SCHOOL ID': ['001001010', ''],
            'NCES ID': ['210003000001', ''],
            'CO-OP': ['GRREC', 'GRREC'],
            'CO-OP CODE': ['902', '902'],
            'SCHOOL TYPE': ['A1', ''],
            'TEACHER COUNT': ['51', '172'],
            'EMERGENCY/PROVISIONAL TEACHER COUNT': ['3', '6'],
            'PERCENT EMERGENCY/PROVISIONAL TEACHERS': ['5.88', '3.49'],
            'NATIONAL BOARD CERTIFIED COUNT': ['4', '14'],
            'PERCENT NATIONAL BOARD CERTIFIED TEACHERS': ['7.84', '8.14']
        })
        return data

    def test_column_mappings(self):
        """Test that column mappings include required fields."""
        mappings = self.etl.module_column_mappings

        # Test title case (KYRC24/25)
        assert 'Teacher Count' in mappings
        assert mappings['Teacher Count'] == 'teacher_count'
        assert 'Emergency Provisional Teacher Count' in mappings
        assert mappings['Emergency Provisional Teacher Count'] == 'emergency_provisional_count'
        assert 'National Board Certified Count' in mappings
        assert mappings['National Board Certified Count'] == 'national_board_certified_count'

        # Test uppercase variants (historical)
        assert 'EMERGENCY/PROVISIONAL TEACHER COUNT' in mappings
        assert 'PERCENT EMERGENCY/PROVISIONAL TEACHERS' in mappings
        assert 'NATIONAL BOARD CERTIFIED COUNT' in mappings

    def test_extract_metrics(self):
        """Test metric extraction from sample data."""
        sample_data = self.create_sample_2024_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        # Test first row (high school)
        row = sample_data.iloc[0]
        metrics = self.etl.extract_metrics(row)

        assert 'emergency_provisional_teacher_rate' in metrics
        assert metrics['emergency_provisional_teacher_rate'] == 6.0
        assert 'emergency_provisional_teacher_count' in metrics
        assert metrics['emergency_provisional_teacher_count'] == 3.0
        assert 'national_board_certified_rate' in metrics
        assert metrics['national_board_certified_rate'] == 10.0
        assert 'national_board_certified_count' in metrics
        assert metrics['national_board_certified_count'] == 5.0

    def test_extract_metrics_with_suppressed_values(self):
        """A ``*`` cell is stored as value NA with suppressed Y.

        These files have no Suppressed column. ``standardize_missing_values``
        records the star, and the KPI row keeps it. A published cell on
        the same row stays suppressed N.
        """
        data = pd.DataFrame({
            'School Year': ['20232024'],
            'County Number': ['001'],
            'District Number': ['001'],
            'School Code': ['001010'],
            'District Name': ['Test District'],
            'School Name': ['Test School'],
            'Teacher Count': ['10'],
            'Emergency Provisional Teacher Count': ['*'],  # Suppressed
            'Percent Emergency Provisional Teachers': ['*'],
            'National Board Certified Count': ['3'],
            'Percent National Board Certified Teacher': ['30.0']
        })

        data = self.etl.normalize_column_names(data)
        data = self.etl.standardize_missing_values(data)
        row = data.iloc[0]
        metrics = self.etl.extract_metrics(row)

        assert pd.isna(metrics['emergency_provisional_teacher_rate'])
        assert pd.isna(metrics['emergency_provisional_teacher_count'])
        assert metrics['national_board_certified_rate'] == 30.0
        assert metrics['national_board_certified_count'] == 3.0

        kpi = self.etl.convert_to_kpi_format(data, "KYRC24_OVW_Teacher_Certification_Data.csv")
        starred = kpi[kpi['metric'] == 'emergency_provisional_teacher_count'].iloc[0]
        assert starred['suppressed'] == 'Y'
        assert pd.isna(starred['value'])
        published = kpi[kpi['metric'] == 'national_board_certified_count'].iloc[0]
        assert published['suppressed'] == 'N'
        assert published['value'] == 3.0

    def test_extract_metrics_with_missing_data(self):
        """Test that rows with all missing data return empty metrics."""
        data = pd.DataFrame({
            'School Year': ['20232024'],
            'County Number': ['001'],
            'District Number': ['001'],
            'School Code': ['001010'],
            'District Name': ['Test District'],
            'School Name': ['Test School'],
            'Teacher Count': [pd.NA],
            'Emergency Provisional Teacher Count': [pd.NA],
            'Percent Emergency Provisional Teachers': [pd.NA],
            'National Board Certified Count': [pd.NA],
            'Percent National Board Certified Teacher': [pd.NA]
        })

        data = self.etl.normalize_column_names(data)
        row = data.iloc[0]
        metrics = self.etl.extract_metrics(row)

        # Should return empty dict when all values are missing
        assert len(metrics) == 0

    def test_negative_values_excluded(self):
        """Test that negative values are excluded."""
        data = pd.DataFrame({
            'School Year': ['20232024'],
            'County Number': ['001'],
            'District Number': ['001'],
            'School Code': ['001010'],
            'District Name': ['Test'],
            'School Name': ['Test'],
            'Teacher Count': ['50'],
            'Emergency Provisional Teacher Count': ['-5'],  # Invalid negative
            'Percent Emergency Provisional Teachers': ['10.0'],
            'National Board Certified Count': ['3'],
            'Percent National Board Certified Teacher': ['6.0']
        })

        data = self.etl.normalize_column_names(data)
        row = data.iloc[0]
        metrics = self.etl.extract_metrics(row)

        # Negative values should be excluded
        assert 'emergency_provisional_teacher_count' not in metrics
        # Other metrics should still be present
        assert 'emergency_provisional_teacher_rate' in metrics
        assert 'national_board_certified_count' in metrics

    def test_should_skip_row(self):
        """Test row skipping logic."""
        # Row with no school_code should be skipped
        row_no_code = pd.Series({
            'school_code': pd.NA,
            'emergency_provisional_count': 5,
            'emergency_provisional_percent': 10.0
        })
        assert self.etl.should_skip_row(row_no_code)

        # Row with empty school_code should be skipped
        row_empty_code = pd.Series({
            'school_code': '',
            'emergency_provisional_count': 5,
            'emergency_provisional_percent': 10.0
        })
        assert self.etl.should_skip_row(row_empty_code)

        # Valid row should not be skipped
        row_valid = pd.Series({
            'school_code': '001010',
            'emergency_provisional_count': 5,
            'emergency_provisional_percent': 10.0
        })
        assert not self.etl.should_skip_row(row_valid)

    def test_student_group_always_all_students(self):
        """Test that student_group is always 'All Students' for institutional data."""
        sample_data = self.create_sample_2024_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        kpi_df = self.etl.convert_to_kpi_format(sample_data, "test_file.csv")

        # All rows should have 'All Students' as student_group
        assert all(kpi_df['student_group'] == 'All Students')

    def test_convert_to_kpi_format(self):
        """Test conversion to KPI format."""
        sample_data = self.create_sample_2024_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        kpi_df = self.etl.convert_to_kpi_format(sample_data, "test_file.csv")

        # Check that DataFrame has correct columns
        assert set(kpi_df.columns).issubset(set(KPI_COLUMNS))

        # Check that we have the right number of KPI rows (4 metrics per school)
        assert len(kpi_df) == 12  # 3 schools × 4 metrics

        # Check specific metrics exist
        metrics = kpi_df['metric'].unique()
        assert 'emergency_provisional_teacher_rate' in metrics
        assert 'emergency_provisional_teacher_count' in metrics
        assert 'national_board_certified_rate' in metrics
        assert 'national_board_certified_count' in metrics

        # Check values are correct
        high_school_rows = kpi_df[kpi_df['school_id'] == '001010']
        ep_rate_row = high_school_rows[high_school_rows['metric'] == 'emergency_provisional_teacher_rate'].iloc[0]
        assert ep_rate_row['value'] == 6.0
        assert ep_rate_row['year'] == '2024'

    def test_suppressed_metric_defaults(self):
        """Test suppressed metric defaults."""
        sample_row = pd.Series({
            'emergency_provisional_percent': 5.0,
            'emergency_provisional_count': 3,
            'national_board_certified_percent': 10.0,
            'national_board_certified_count': 5
        })
        defaults = self.etl.get_suppressed_metric_defaults(sample_row)

        assert 'emergency_provisional_teacher_rate' in defaults
        assert 'emergency_provisional_teacher_count' in defaults
        assert 'national_board_certified_rate' in defaults
        assert 'national_board_certified_count' in defaults
        assert pd.isna(defaults['emergency_provisional_teacher_rate'])

    def test_transform_integration(self):
        """Test complete transform function integration."""
        # Create sample files
        sample_2024 = self.create_sample_2024_data()
        sample_historical = self.create_sample_historical_data()

        # Save to test directory
        sample_2024.to_csv(self.sample_dir / "KYRC24_OVW_Teacher_Certification_Data.csv", index=False)
        sample_historical.to_csv(self.sample_dir / "teacher_certifications_2021.csv", index=False)

        # Run transform
        config = {}
        transform(self.raw_dir, self.proc_dir, config)

        # Check output file was created
        output_file = self.proc_dir / "teacher_certification.csv"
        assert output_file.exists()

        # Load and validate output
        result_df = pd.read_csv(output_file)
        assert not result_df.empty
        assert set(result_df.columns).issubset(set(KPI_COLUMNS))

        # Check that we have data from both source files
        source_files = result_df['source_file'].unique()
        assert any('KYRC24' in f for f in source_files)
        assert any('2021' in f for f in source_files)

        # Check years
        years = result_df['year'].unique()
        assert 2024 in years or '2024' in years
        assert 2021 in years or '2021' in years

    def test_historical_uppercase_columns(self):
        """Test that historical uppercase columns are properly mapped."""
        sample_data = self.create_sample_historical_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        # Check columns were normalized
        assert 'teacher_count' in sample_data.columns
        assert 'emergency_provisional_count' in sample_data.columns
        assert 'emergency_provisional_percent' in sample_data.columns
        assert 'national_board_certified_count' in sample_data.columns
        assert 'national_board_certified_percent' in sample_data.columns

        # Test metric extraction
        row = sample_data.iloc[0]
        metrics = self.etl.extract_metrics(row)

        assert metrics['emergency_provisional_teacher_rate'] == 5.88
        assert metrics['emergency_provisional_teacher_count'] == 3.0
        assert metrics['national_board_certified_rate'] == 7.84
        assert metrics['national_board_certified_count'] == 4.0


class TestTeacherCertificationKyrc26:
    """KYRC26 opt-in. KYRC23 republishes 2022-23 and is skipped.

    Fixtures are verbatim rows from the downloaded files. The KYRC24
    headers keep their trailing spaces, the rate cells keep ``%``, and
    ``0.0`` is not rewritten to a blank.
    """

    def setup_method(self):
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        self.sample_dir = self.raw_dir / "teacher_certification"
        self.sample_dir.mkdir(parents=True)
        self.etl = TeacherCertificationETL("teacher_certification")

    def teardown_method(self):
        shutil.rmtree(self.test_dir)

    def _production_config(self) -> dict:
        config = yaml.safe_load((ROOT / "config" / "mappings.yaml").read_text())
        return config["sources"]["teacher_certification"]

    def _copy_fixtures(self) -> None:
        for path in FIXTURE_DIR.glob("*.csv"):
            (self.sample_dir / path.name).write_bytes(path.read_bytes())

    def test_kyrc26_file_is_accepted(self):
        """The permanent KYRC files and the 2020-2023 names stay in scope."""
        assert self.etl.include_kyrc26 is True
        assert self.etl.module_column_mappings["Teacher Count"] == "teacher_count"
        assert (
            self.etl.module_column_mappings["Emergency Provisional Teacher Count "]
            == "emergency_provisional_count"
        )
        assert "Rank I Teacher Count" not in self.etl.module_column_mappings
        assert "Percent Rank II Teachers" not in self.etl.module_column_mappings

        accepted = [
            "KYRC26_OVW_Teacher_Certification_Data.csv",
            "KYRC25_OVW_Teacher_Certification_Data.csv",
            "KYRC24_OVW_Teacher_Certification_Data.csv",
            "teacher_certifications_2023.csv",
            "teacher_certifications_2022.csv",
            "teacher_certifications_2021.csv",
            "teacher_certifications_2020.csv",
        ]
        for name in accepted:
            (self.sample_dir / name).write_text("School Year,School Code\n20252026,999000\n")
        (self.sample_dir / "KYRC23_OVW_Teacher_Certification_Data.csv").write_text(
            "School Year,School Code,Percent Emergency Provisional Teachers\n"
            "20222023,001010,2.6\n"
        )

        found = sorted(path.name for path, _sheet in self.etl._find_data_files(self.sample_dir))
        assert found == sorted(accepted)

    def test_kyrc23_republish_is_skipped(self):
        """A leftover KYRC23 file must not write a second copy of 2023.

        The datasets file is school year 20222023. It rounds the historical
        percent and stars counts the 2023 file still publishes.
        """
        republish = (
            "School Year,District Name,School Name,School Code,Teacher Count,"
            "Emergency Provisional Teacher Count,Percent Emergency Provisional Teachers,"
            "National Board Certified Count,Percent National Board Certified Teacher\n"
            "20222023,State,---State Total---,999,43169,1128,2.6,2076,4.8\n"
        )
        (self.sample_dir / "KYRC23_OVW_Teacher_Certification_Data.csv").write_text(republish)
        (self.sample_dir / "KYRC26_OVW_Teacher_Certification_Data.csv").write_bytes(
            (FIXTURE_DIR / "KYRC26_OVW_Teacher_Certification_Data.csv").read_bytes()
        )
        transform(self.raw_dir, self.proc_dir, self._production_config())
        result = pd.read_csv(self.proc_dir / "teacher_certification.csv", dtype=str)
        assert set(result["source_file"]) == {"KYRC26_OVW_Teacher_Certification_Data.csv"}
        assert "2023" not in set(result["year"])

    def test_real_rows_match_the_downloaded_files(self):
        """State, district, school, a real zero, a placeholder, and a blank.

        There is no Suppressed column. A starred cell is value NA and
        suppressed Y. A blank National Board cell is not stored as 0.
        A 0% with teachers is a real zero. A 0% with a teacher count of 0
        is a placeholder and is still stored. Rank I and Rank II are not
        metrics. The 2026 emergency percent is the published 14.3, not a
        rebuilt ratio.
        """
        self._copy_fixtures()
        transform(self.raw_dir, self.proc_dir, self._production_config())
        result = pd.read_csv(self.proc_dir / "teacher_certification.csv", dtype=str)
        published = result["value"].fillna("").ne("")
        assert (result.loc[published, "suppressed"] == "N").all()
        assert (result.loc[~published, "suppressed"] == "Y").all()
        assert (result["student_group"] == "All Students").all()
        assert set(result["metric"]) == {
            "emergency_provisional_teacher_rate",
            "emergency_provisional_teacher_count",
            "national_board_certified_rate",
            "national_board_certified_count",
        }
        # Allen County Career and Technical Center is blank on every measure.
        assert not (
            (result["year"] == "2026") & (result["school_id"] == "005901")
        ).any()

        def cell(year, school_id, metric):
            rows = result[
                (result["year"] == year)
                & (result["school_id"] == school_id)
                & (result["metric"] == metric)
            ]
            assert len(rows) == 1, f"{year} {school_id} {metric} -> {len(rows)}"
            return rows.iloc[0]

        def missing(year, school_id, metric):
            rows = result[
                (result["year"] == year)
                & (result["school_id"] == school_id)
                & (result["metric"] == metric)
            ]
            assert len(rows) == 0, f"{year} {school_id} {metric} was stored"

        def suppressed(year, school_id, metric):
            row = cell(year, school_id, metric)
            assert row["suppressed"] == "Y", f"{year} {school_id} {metric}"
            assert pd.isna(row["value"]) or str(row["value"]).strip() == ""

        state = cell("2026", "999000", "emergency_provisional_teacher_rate")
        assert float(state["value"]) == 14.3
        assert state["district"] == "All Districts"
        assert state["school_name"] == "---District Total---"
        assert state["source_file"] == "KYRC26_OVW_Teacher_Certification_Data.csv"
        assert float(cell("2026", "999000", "emergency_provisional_teacher_count")["value"]) == 6133.0
        assert float(cell("2026", "999000", "national_board_certified_rate")["value"]) == 2.7
        assert float(cell("2026", "999000", "national_board_certified_count")["value"]) == 1172.0

        fayette = cell("2026", "165000", "emergency_provisional_teacher_rate")
        assert float(fayette["value"]) == 11.6
        assert fayette["district"] == "Fayette County"
        assert fayette["nces_id"] == "2101860"
        assert fayette["co_op"] == "CKEC"
        assert fayette["co_op_code"] == "901"
        assert float(cell("2026", "165000", "emergency_provisional_teacher_count")["value"]) == 375.0
        assert float(cell("2026", "165000", "national_board_certified_rate")["value"]) == 4.9
        assert float(cell("2026", "165000", "national_board_certified_count")["value"]) == 159.0

        bryan = cell("2026", "165170", "emergency_provisional_teacher_rate")
        assert float(bryan["value"]) == 15.5
        assert bryan["school_name"] == "Bryan Station High School"
        assert bryan["nces_id"] == "210186000388"
        assert bryan["state_school_id"] == "034165170"
        assert bryan["school_type"] == "A1"
        assert float(cell("2026", "165170", "emergency_provisional_teacher_count")["value"]) == 20.0
        assert float(cell("2026", "165170", "national_board_certified_count")["value"]) == 5.0
        assert float(cell("2026", "165170", "national_board_certified_rate")["value"]) == 3.9

        adair = cell("2026", "001010", "emergency_provisional_teacher_rate")
        assert float(adair["value"]) == 21.2
        assert adair["nces_id"] == "210003000001"
        assert float(cell("2026", "001010", "emergency_provisional_teacher_count")["value"]) == 11.0
        # The count is blank, so the missing percent is not a zero.
        missing("2026", "001010", "national_board_certified_rate")
        missing("2026", "001010", "national_board_certified_count")
        assert float(cell("2026", "001000", "emergency_provisional_teacher_rate")["value"]) == 17.2
        assert float(cell("2026", "001000", "emergency_provisional_teacher_count")["value"]) == 31.0
        missing("2026", "001000", "national_board_certified_rate")

        # Prior-year anchors. 2025 Fayette keeps the comma-formatted count.
        assert float(cell("2025", "999000", "emergency_provisional_teacher_rate")["value"]) == 1.9
        assert float(cell("2025", "999000", "emergency_provisional_teacher_count")["value"]) == 826.0
        assert float(cell("2025", "999000", "national_board_certified_rate")["value"]) == 2.9
        assert float(cell("2025", "999000", "national_board_certified_count")["value"]) == 1234.0
        assert float(cell("2025", "165000", "emergency_provisional_teacher_rate")["value"]) == 1.2
        assert float(cell("2025", "165000", "emergency_provisional_teacher_count")["value"]) == 38.0
        assert float(cell("2025", "165000", "national_board_certified_rate")["value"]) == 5.8
        assert cell("2025", "165170", "national_board_certified_rate")["nces_id"] == "210186000000"
        assert float(cell("2025", "165170", "national_board_certified_count")["value"]) == 6.0
        # Emergency cells are *. National Board on the same row stays published.
        suppressed("2025", "165170", "emergency_provisional_teacher_rate")
        suppressed("2025", "165170", "emergency_provisional_teacher_count")
        assert float(cell("2025", "001000", "emergency_provisional_teacher_rate")["value"]) == 3.3
        suppressed("2025", "001000", "national_board_certified_count")
        suppressed("2025", "001000", "national_board_certified_rate")
        # Adair County High is starred on all four certification cells.
        for metric in (
            "emergency_provisional_teacher_rate",
            "emergency_provisional_teacher_count",
            "national_board_certified_rate",
            "national_board_certified_count",
        ):
            suppressed("2025", "001010", metric)

        # 0.0 with a blank teacher count and a blank emergency count.
        # The percent is a placeholder and is stored. The blank count is not.
        placeholder = cell("2025", "777108", "emergency_provisional_teacher_rate")
        assert float(placeholder["value"]) == 0.0
        assert placeholder["school_name"] == "Campbell County Area Technology Center"
        missing("2025", "777108", "emergency_provisional_teacher_count")
        assert float(cell("2025", "777108", "national_board_certified_rate")["value"]) == 0.0
        missing("2025", "777108", "national_board_certified_count")

        # Historical KYRC24 keeps the percent sign and the comma.
        assert float(cell("2024", "999000", "emergency_provisional_teacher_rate")["value"]) == 3.0
        assert float(cell("2024", "999000", "emergency_provisional_teacher_count")["value"]) == 1276.0
        assert float(cell("2024", "999000", "national_board_certified_rate")["value"]) == 2.6
        assert float(cell("2024", "999000", "national_board_certified_count")["value"]) == 1139.0
        # Blank teacher count. The published percent is still 1.0, not 0.
        assert float(cell("2024", "165000", "emergency_provisional_teacher_rate")["value"]) == 1.0
        assert float(cell("2024", "165000", "emergency_provisional_teacher_count")["value"]) == 31.0
        missing("2024", "165000", "national_board_certified_rate")
        missing("2024", "165000", "national_board_certified_count")
        # Baker Intermediate is starred on every certification cell, including
        # the teacher count. All four KPI metrics are stored as Y.
        for metric in (
            "emergency_provisional_teacher_rate",
            "emergency_provisional_teacher_count",
            "national_board_certified_rate",
            "national_board_certified_count",
        ):
            suppressed("2024", "121026", metric)

        breathitt = cell("2023", "061", "emergency_provisional_teacher_rate")
        assert float(breathitt["value"]) == 5.22
        assert breathitt["district"] == "Breathitt County"
        assert breathitt["school_name"] == "---District Total---"
        assert float(cell("2023", "061", "emergency_provisional_teacher_count")["value"]) == 6.0
        assert float(cell("2023", "061", "national_board_certified_rate")["value"]) == 3.48
        assert float(cell("2023", "061", "national_board_certified_count")["value"]) == 4.0
        assert float(cell("2023", "999", "emergency_provisional_teacher_rate")["value"]) == 2.61
        assert cell("2023", "999", "emergency_provisional_teacher_rate")["district"] == "State"
        assert cell("2023", "999", "emergency_provisional_teacher_rate")["school_name"] == "---State Total---"
        assert float(cell("2023", "999", "national_board_certified_count")["value"]) == 2086.0
        # 52 teachers and an emergency count of 0. The 0% is a real zero.
        assert float(cell("2023", "001010", "emergency_provisional_teacher_rate")["value"]) == 0.0
        assert float(cell("2023", "001010", "emergency_provisional_teacher_count")["value"]) == 0.0
        assert float(cell("2023", "001010", "national_board_certified_rate")["value"]) == 9.62
        # Allen County Career and Technical Center: the percents are * and
        # both counts are blank. Only the starred percents are stored.
        suppressed("2023", "005901", "emergency_provisional_teacher_rate")
        suppressed("2023", "005901", "national_board_certified_rate")
        missing("2023", "005901", "emergency_provisional_teacher_count")
        missing("2023", "005901", "national_board_certified_count")

        assert float(cell("2022", "999", "emergency_provisional_teacher_rate")["value"]) == 6.98
        assert float(cell("2022", "999", "national_board_certified_count")["value"]) == 3108.0
        # Teacher count 0, emergency count 1, published percent 0.
        sunshine = cell("2022", "236021", "emergency_provisional_teacher_rate")
        assert float(sunshine["value"]) == 0.0
        assert float(cell("2022", "236021", "emergency_provisional_teacher_count")["value"]) == 1.0
        assert float(cell("2022", "236021", "national_board_certified_count")["value"]) == 1.0

        # 40 teachers and an emergency count of 0. The 0% is a real zero.
        assert float(cell("2021", "001014", "emergency_provisional_teacher_rate")["value"]) == 0.0
        assert float(cell("2021", "001014", "emergency_provisional_teacher_count")["value"]) == 0.0
        assert float(cell("2021", "001014", "national_board_certified_rate")["value"]) == 2.5
        assert float(cell("2021", "999", "emergency_provisional_teacher_rate")["value"]) == 4.98
        assert float(cell("2021", "999", "national_board_certified_rate")["value"]) == 7.4
        assert cell("2021", "999", "emergency_provisional_teacher_rate")["district"] == "State"
        # Teacher count 0 and a published 0%. The emergency count is 2, not 0.
        academy = cell("2021", "042020", "emergency_provisional_teacher_rate")
        assert float(academy["value"]) == 0.0
        assert academy["school_name"] == "Virtual Academy"
        assert float(cell("2021", "042020", "emergency_provisional_teacher_count")["value"]) == 2.0
        assert float(cell("2021", "042020", "national_board_certified_count")["value"]) == 0.0

        # 2020 publishes counts and no percents.
        assert float(cell("2020", "999", "emergency_provisional_teacher_count")["value"]) == 2031.0
        assert float(cell("2020", "999", "national_board_certified_count")["value"]) == 3168.0
        assert cell("2020", "999", "emergency_provisional_teacher_count")["district"] == "State"
        assert cell("2020", "999", "emergency_provisional_teacher_count")["school_name"] == "---State Total---"
        missing("2020", "999", "emergency_provisional_teacher_rate")
        missing("2020", "999", "national_board_certified_rate")
        assert float(cell("2020", "001010", "emergency_provisional_teacher_count")["value"]) == 3.0
        assert float(cell("2020", "001010", "national_board_certified_count")["value"]) == 4.0
        missing("2020", "001010", "emergency_provisional_teacher_rate")

        # KYRC26 stars the emergency cells and leaves National Board blank.
        suppressed("2026", "011030", "emergency_provisional_teacher_rate")
        suppressed("2026", "011030", "emergency_provisional_teacher_count")
        missing("2026", "011030", "national_board_certified_rate")
        missing("2026", "011030", "national_board_certified_count")
        suppressed("2026", "015070", "emergency_provisional_teacher_rate")
        suppressed("2026", "015070", "emergency_provisional_teacher_count")
        missing("2026", "015070", "national_board_certified_count")
        # Teacher count 0, emergency count *, emergency percent blank.
        suppressed("2026", "197002", "emergency_provisional_teacher_count")
        missing("2026", "197002", "emergency_provisional_teacher_rate")
        missing("2026", "197002", "national_board_certified_count")
        missing("2026", "197002", "national_board_certified_rate")

    def test_comma_and_percent_sign_survive_an_int64_cast(self):
        """Comma counts and a ``%`` rate survive StringDtype and Int64.

        Production ``mappings.yaml`` has no dtype for this source. The cast
        here is the failure mode from issue #49: ``to_numeric`` on ``6,133``
        or ``3.0%`` becomes missing before ``extract_metrics`` unless the
        text is cleaned. Object and StringDtype both take that path.
        """
        (self.sample_dir / "KYRC26_OVW_Teacher_Certification_Data.csv").write_bytes(
            (FIXTURE_DIR / "KYRC26_OVW_Teacher_Certification_Data.csv").read_bytes()
        )
        (self.sample_dir / "KYRC24_OVW_Teacher_Certification_Data.csv").write_bytes(
            (FIXTURE_DIR / "KYRC24_OVW_Teacher_Certification_Data.csv").read_bytes()
        )
        config = self._production_config()
        config["dtype"] = {
            "teacher_count": "Int64",
            "emergency_provisional_count": "Int64",
            "national_board_certified_count": "Int64",
            "emergency_provisional_percent": "float64",
            "national_board_certified_percent": "float64",
        }
        transform(self.raw_dir, self.proc_dir, config)
        result = pd.read_csv(self.proc_dir / "teacher_certification.csv", dtype=str)

        def value(year, school_id, metric):
            rows = result[
                (result["year"] == year)
                & (result["school_id"] == school_id)
                & (result["metric"] == metric)
            ]
            assert len(rows) == 1
            return float(rows.iloc[0]["value"])

        assert value("2026", "999000", "emergency_provisional_teacher_count") == 6133.0
        assert value("2026", "999000", "national_board_certified_count") == 1172.0
        assert value("2026", "165000", "emergency_provisional_teacher_count") == 375.0
        assert value("2024", "999000", "emergency_provisional_teacher_count") == 1276.0
        assert value("2024", "999000", "emergency_provisional_teacher_rate") == 3.0
        assert value("2024", "999000", "national_board_certified_count") == 1139.0
        assert value("2024", "999000", "national_board_certified_rate") == 2.6

    def test_number_parsing_is_dtype_agnostic(self):
        """pandas 3 StringDtype and pandas 2 object follow the same rules.

        A ``*`` is included as NA. A blank is omitted. ``0`` stays 0. A
        comma and a percent sign are stripped. ``bool(pd.NA)`` is not used
        to detect the dtype. A blank percent is not turned into 0 when the
        count is positive.
        """
        frame = pd.DataFrame({
            "school_code": pd.Series(
                ["999000", "001014", "001010", "165000", "777108"], dtype="string"
            ),
            "teacher_count": pd.Series(["43,037", "40", "52", "", ""], dtype="string"),
            "emergency_provisional_count": pd.Series(
                ["6,133", "0", "11", "31", ""], dtype="string"
            ),
            "emergency_provisional_percent": pd.Series(
                ["14.3", "0", "21.2", "1.0%", ""], dtype="string"
            ),
            "national_board_certified_count": pd.Series(
                ["1,172", "1", "", "*", ""], dtype="string"
            ),
            "national_board_certified_percent": pd.Series(
                ["2.7", "2.50", "", "*", "0.0"], dtype="string"
            ),
        })
        assert not self.etl.should_skip_row(frame.iloc[0])
        assert self.etl.extract_metrics(frame.iloc[0]) == {
            "emergency_provisional_teacher_rate": 14.3,
            "emergency_provisional_teacher_count": 6133.0,
            "national_board_certified_rate": 2.7,
            "national_board_certified_count": 1172.0,
        }
        assert self.etl.extract_metrics(frame.iloc[1])["emergency_provisional_teacher_rate"] == 0.0
        assert self.etl.extract_metrics(frame.iloc[1])["emergency_provisional_teacher_count"] == 0.0
        blank = self.etl.extract_metrics(frame.iloc[2])
        assert "national_board_certified_rate" not in blank
        assert blank["emergency_provisional_teacher_count"] == 11.0
        assert self.etl.extract_metrics(frame.iloc[3])["emergency_provisional_teacher_rate"] == 1.0
        starred = self.etl.extract_metrics(frame.iloc[3])
        assert pd.isna(starred["national_board_certified_count"])
        assert pd.isna(starred["national_board_certified_rate"])
        assert self.etl.should_skip_row(frame.iloc[3]) is False
        assert self.etl.should_skip_row(frame.iloc[4]) is False
        assert self.etl.extract_metrics(frame.iloc[4]) == {
            "national_board_certified_rate": 0.0,
        }

        as_object = frame.astype(object)
        assert self.etl.extract_metrics(as_object.iloc[0])["emergency_provisional_teacher_count"] == 6133.0
        assert self.etl.extract_metrics(as_object.iloc[2])["emergency_provisional_teacher_rate"] == 21.2
        assert "national_board_certified_rate" not in self.etl.extract_metrics(as_object.iloc[2])
        assert self.etl.extract_metrics(as_object.iloc[1])["emergency_provisional_teacher_rate"] == 0.0
        object_star = self.etl.extract_metrics(as_object.iloc[3])
        assert pd.isna(object_star["national_board_certified_count"])
        assert pd.isna(object_star["national_board_certified_rate"])

    def test_repeated_school_codes_keep_the_larger_national_board_count(self):
        """2020-2022 repeat school codes. The larger National Board copy stays.

        The copies agree on the teacher count and on emergency/provisional.
        They disagree on National Board. Keeping the larger count is what
        makes the district National Board sum equal the state. A later
        filename that repeats a code is left as published. When the counts
        tie, the larger percent is kept.
        """
        header_2020 = (
            "SCHOOL YEAR,COUNTY NUMBER,COUNTY NAME,DISTRICT NUMBER,DISTRICT NAME,"
            "SCHOOL NUMBER,SCHOOL NAME,SCHOOL CODE,STATE SCHOOL ID,NCES ID,CO-OP,"
            "CO-OP CODE,TEACHER COUNT,EMERGENCY/PROVISIONAL TEACHER COUNT,"
            "NATIONAL BOARD CERTIFIED COUNT\n"
        )
        (self.sample_dir / "teacher_certifications_2020.csv").write_text(
            header_2020
            + "20192020,090,NELSON,017,Bardstown Independent,012,"
            "Bardstown Primary School,017012,090017012,210027002050,CKEC,901,36,0,6\n"
            + "20192020,090,NELSON,017,Bardstown Independent,012,"
            "Bardstown Primary School,017012,090017012,210027002050,CKEC,901,36,0,0\n"
        )
        header_2021 = (
            "SCHOOL YEAR,COUNTY NUMBER,COUNTY NAME,DISTRICT NUMBER,DISTRICT NAME,"
            "SCHOOL NUMBER,SCHOOL NAME,SCHOOL CODE,STATE SCHOOL ID,NCES ID,CO-OP,"
            "CO-OP CODE,SCHOOL TYPE,TEACHER COUNT,EMERGENCY/PROVISIONAL TEACHER COUNT,"
            "PERCENT EMERGENCY/PROVISIONAL TEACHERS,NATIONAL BOARD CERTIFIED COUNT,"
            "PERCENT NATIONAL BOARD CERTIFIED TEACHERS\n"
        )
        boone = (
            '20202021,008,BOONE,035,Boone County,"",---District Total---,035,"","",'
            "NKCES,905,\"\",1358,26,1.91,{count},{percent}\n"
        )
        # Same National Board count, so the larger percent is the tie-break.
        tie = (
            '20202021,001,ADAIR,001,Adair County,"",---District Total---,001,"","",'
            "GRREC,902,\"\",10,1,1.00,5,{percent}\n"
        )
        (self.sample_dir / "teacher_certifications_2021.csv").write_text(
            header_2021
            + boone.format(count=82, percent="6.04")
            + boone.format(count=7, percent="0.52")
            + tie.format(percent="1.00")
            + tie.format(percent="9.00")
        )
        barren = (
            '20212022,005,BARREN,021,Barren County,"",---District Total---,021,"","",'
            "GRREC,902,\"\",335,25,7.46,{count},{percent}\n"
        )
        (self.sample_dir / "teacher_certifications_2022.csv").write_text(
            header_2021
            + barren.format(count=0, percent="0")
            + barren.format(count=1, percent="0.30")
            + barren.format(count=24, percent="7.16")
        )
        # The same Boone pair under a later filename is not collapsed.
        (self.sample_dir / "KYRC26_OVW_Teacher_Certification_Data.csv").write_text(
            header_2021
            + boone.format(count=82, percent="6.04")
            + boone.format(count=7, percent="0.52")
        )
        transform(self.raw_dir, self.proc_dir, self._production_config())
        result = pd.read_csv(self.proc_dir / "teacher_certification.csv", dtype=str)

        def values(source, school_id, metric):
            rows = result[
                (result["source_file"] == source)
                & (result["school_id"] == school_id)
                & (result["metric"] == metric)
            ]
            return sorted(float(value) for value in rows["value"])

        assert values("teacher_certifications_2020.csv", "017012", "national_board_certified_count") == [6.0]
        assert values("teacher_certifications_2020.csv", "017012", "emergency_provisional_teacher_count") == [0.0]
        assert values("teacher_certifications_2021.csv", "035", "national_board_certified_count") == [82.0]
        assert values("teacher_certifications_2021.csv", "035", "national_board_certified_rate") == [6.04]
        assert values("teacher_certifications_2021.csv", "035", "emergency_provisional_teacher_count") == [26.0]
        assert values("teacher_certifications_2021.csv", "035", "emergency_provisional_teacher_rate") == [1.91]
        assert values("teacher_certifications_2021.csv", "001", "national_board_certified_count") == [5.0]
        assert values("teacher_certifications_2021.csv", "001", "national_board_certified_rate") == [9.0]
        assert values("teacher_certifications_2022.csv", "021", "national_board_certified_count") == [24.0]
        assert values("teacher_certifications_2022.csv", "021", "national_board_certified_rate") == [7.16]
        assert values("teacher_certifications_2022.csv", "021", "emergency_provisional_teacher_count") == [25.0]
        assert values(
            "KYRC26_OVW_Teacher_Certification_Data.csv",
            "035",
            "national_board_certified_count",
        ) == [7.0, 82.0]

    def test_missing_national_board_loses_to_a_number_and_a_star_beats_a_blank(self):
        """A number beats a ``*`` or a blank. A ``*`` beats a blank.

        The missing count sorts below 0, so flipping ``na_position`` to
        ``first`` would keep the blank or the star instead of the number.
        When both copies are missing, the starred copy is kept even if the
        blank copy is earlier in the file. File order does not decide that
        tie.
        """
        header = (
            "SCHOOL YEAR,COUNTY NUMBER,COUNTY NAME,DISTRICT NUMBER,DISTRICT NAME,"
            "SCHOOL NUMBER,SCHOOL NAME,SCHOOL CODE,STATE SCHOOL ID,NCES ID,CO-OP,"
            "CO-OP CODE,SCHOOL TYPE,TEACHER COUNT,EMERGENCY/PROVISIONAL TEACHER COUNT,"
            "PERCENT EMERGENCY/PROVISIONAL TEACHERS,NATIONAL BOARD CERTIFIED COUNT,"
            "PERCENT NATIONAL BOARD CERTIFIED TEACHERS\n"
        )
        row = (
            "20202021,001,ADAIR,001,Adair County,010,{name},001010,001001010,"
            "210003000001,GRREC,902,A1,40,0,0,{count},{percent}\n"
        )
        (self.sample_dir / "teacher_certifications_2021.csv").write_text(
            header
            # Blank is first. The published 0 is kept.
            + row.format(name="Blank Copy", count="", percent="")
            + row.format(name="Zero Copy", count="0", percent="0")
            # Star is first. The published 4 is kept.
            + row.format(name="Star Copy", count="*", percent="*").replace("001010", "001011")
            + row.format(name="Number Copy", count="4", percent="10").replace("001010", "001011")
            # Blank is first. The later star is kept.
            + row.format(name="Blank First", count="", percent="").replace("001010", "001012")
            + row.format(name="Star Second", count="*", percent="*").replace("001010", "001012")
            # Equal counts. The later starred percent is kept over the blank.
            + row.format(name="Percent Blank", count="3", percent="").replace("001010", "001013")
            + row.format(name="Percent Star", count="3", percent="*").replace("001010", "001013")
        )
        transform(self.raw_dir, self.proc_dir, self._production_config())
        result = pd.read_csv(self.proc_dir / "teacher_certification.csv", dtype=str)

        def rows_for(school_id, metric):
            return result[
                (result["school_id"] == school_id) & (result["metric"] == metric)
            ]

        zero = rows_for("001010", "national_board_certified_count")
        assert len(zero) == 1
        assert zero.iloc[0]["school_name"] == "Zero Copy"
        assert float(zero.iloc[0]["value"]) == 0.0
        assert zero.iloc[0]["suppressed"] == "N"

        number = rows_for("001011", "national_board_certified_count")
        assert len(number) == 1
        assert number.iloc[0]["school_name"] == "Number Copy"
        assert float(number.iloc[0]["value"]) == 4.0
        assert number.iloc[0]["suppressed"] == "N"

        starred = rows_for("001012", "national_board_certified_count")
        assert len(starred) == 1
        assert starred.iloc[0]["school_name"] == "Star Second"
        assert starred.iloc[0]["suppressed"] == "Y"
        assert pd.isna(starred.iloc[0]["value"]) or str(starred.iloc[0]["value"]).strip() == ""
        rate = rows_for("001012", "national_board_certified_rate")
        assert len(rate) == 1
        assert rate.iloc[0]["suppressed"] == "Y"

        percent = rows_for("001013", "national_board_certified_rate")
        assert len(percent) == 1
        assert percent.iloc[0]["school_name"] == "Percent Star"
        assert percent.iloc[0]["suppressed"] == "Y"
        count = rows_for("001013", "national_board_certified_count")
        assert len(count) == 1
        assert float(count.iloc[0]["value"]) == 3.0
        assert count.iloc[0]["suppressed"] == "N"


def test_registered_sources_are_the_permanent_files():
    """KYRC24, KYRC25, KYRC26, and 2020-2023 are registered.

    KYRC23 is not. The 2018-19 workbooks are not. No temporary OAA name is.
    """
    config = yaml.safe_load((ROOT / "config" / "kde_sources.yaml").read_text())
    names = []
    for entry in config["raw_directories"]["teacher_certification"]:
        names.append(entry["file"] if isinstance(entry, dict) else entry)

    assert names == [
        "KYRC24_OVW_Teacher_Certification_Data.csv",
        "KYRC25_OVW_Teacher_Certification_Data.csv",
        "KYRC26_OVW_Teacher_Certification_Data.csv",
        "teacher_certifications_2023.csv",
        "teacher_certifications_2022.csv",
        "teacher_certifications_2021.csv",
        "teacher_certifications_2020.csv",
    ]
    assert not any("KYRC23" in name for name in names)
    assert not any(name.endswith(".xlsx") for name in names)
    assert not any(name.startswith("Teacher_Certification") for name in names)
