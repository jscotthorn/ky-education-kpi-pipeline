"""
Tests for Teacher Turnover ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
import yaml
from etl.constants import KPI_COLUMNS
import shutil
from etl.teacher_turnover import transform, TeacherTurnoverETL

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "teacher_turnover"


class TestTeacherTurnoverETL:

    def setup_method(self):
        """Setup test directories and sample data."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)

        # Create sample raw data directory
        self.sample_dir = self.raw_dir / "teacher_turnover"
        self.sample_dir.mkdir(parents=True)

        # Create ETL instance for testing
        self.etl = TeacherTurnoverETL('teacher_turnover')

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
            'Teacher Turnover Count': ['8', '5', '25'],
            'Turnover Percent': ['16.0', '14.3', '13.5']
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
            'TEACHER COUNT': ['48', '183'],
            'TEACHER TURNOVER COUNT': ['3', '23'],
            'TURNOVER PERCENT': ['6.3', '12.6']
        })
        return data

    def test_column_mappings(self):
        """Test that column mappings include required fields."""
        mappings = self.etl.module_column_mappings

        assert 'Teacher Count' in mappings
        assert mappings['Teacher Count'] == 'teacher_count'
        assert 'Teacher Turnover Count' in mappings
        assert mappings['Teacher Turnover Count'] == 'teacher_turnover_count'
        assert 'Turnover Percent' in mappings
        assert mappings['Turnover Percent'] == 'turnover_percent'

        # Test uppercase variants
        assert 'TEACHER COUNT' in mappings
        assert 'TEACHER TURNOVER COUNT' in mappings
        assert 'TURNOVER PERCENT' in mappings

    def test_extract_metrics(self):
        """Test metric extraction from sample data."""
        sample_data = self.create_sample_2024_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        # Test first row (high school)
        row = sample_data.iloc[0]
        metrics = self.etl.extract_metrics(row)

        assert 'teacher_turnover_rate' in metrics
        assert metrics['teacher_turnover_rate'] == 16.0
        assert 'teacher_turnover_count' in metrics
        assert metrics['teacher_turnover_count'] == 8.0
        assert 'teacher_count' in metrics
        assert metrics['teacher_count'] == 50.0

    def test_extract_metrics_with_commas(self):
        """Test that comma-separated values are handled correctly."""
        data = pd.DataFrame({
            'School Year': ['20232024'],
            'County Number': ['001'],
            'District Number': ['001'],
            'School Code': ['001000'],
            'District Name': ['Test District'],
            'School Name': ['All Schools'],
            'Teacher Count': ['10,612'],  # Comma-separated
            'Teacher Turnover Count': ['2,154'],
            'Turnover Percent': ['20.3']
        })

        data = self.etl.normalize_column_names(data)
        row = data.iloc[0]
        metrics = self.etl.extract_metrics(row)

        assert metrics['teacher_count'] == 10612.0
        assert metrics['teacher_turnover_count'] == 2154.0

    def test_extract_metrics_with_missing_data(self):
        """Test that rows with missing data return empty metrics."""
        data = pd.DataFrame({
            'School Year': ['20232024'],
            'County Number': ['001'],
            'District Number': ['001'],
            'School Code': ['001010'],
            'District Name': ['Test District'],
            'School Name': ['Test School'],
            'Teacher Count': [pd.NA],
            'Teacher Turnover Count': [pd.NA],
            'Turnover Percent': [pd.NA]
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
            'Teacher Turnover Count': ['-5'],  # Invalid negative
            'Turnover Percent': ['10.0']
        })

        data = self.etl.normalize_column_names(data)
        row = data.iloc[0]
        metrics = self.etl.extract_metrics(row)

        # Negative values should be excluded
        assert 'teacher_turnover_count' not in metrics
        # Other metrics should still be present
        assert 'teacher_count' in metrics
        assert 'teacher_turnover_rate' in metrics

    def test_should_skip_row(self):
        """Test row skipping logic."""
        # Row with no school_code should be skipped
        row_no_code = pd.Series({
            'school_code': pd.NA,
            'teacher_count': 50,
            'turnover_percent': 15.0
        })
        assert self.etl.should_skip_row(row_no_code)

        # Row with empty school_code should be skipped
        row_empty_code = pd.Series({
            'school_code': '',
            'teacher_count': 50,
            'turnover_percent': 15.0
        })
        assert self.etl.should_skip_row(row_empty_code)

        # Valid row should not be skipped
        row_valid = pd.Series({
            'school_code': '001010',
            'teacher_count': 50,
            'turnover_percent': 15.0
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

        # Check that we have the right number of KPI rows (3 metrics per school)
        assert len(kpi_df) == 9  # 3 schools × 3 metrics

        # Check specific metrics exist
        metrics = kpi_df['metric'].unique()
        assert 'teacher_turnover_rate' in metrics
        assert 'teacher_turnover_count' in metrics
        assert 'teacher_count' in metrics

        # Check values are correct
        high_school_rows = kpi_df[kpi_df['school_id'] == '001010']
        rate_row = high_school_rows[high_school_rows['metric'] == 'teacher_turnover_rate'].iloc[0]
        assert rate_row['value'] == 16.0
        assert rate_row['year'] == '2024'

    def test_suppressed_metric_defaults(self):
        """Test suppressed metric defaults."""
        sample_row = pd.Series({
            'turnover_percent': 15.0,
            'teacher_turnover_count': 10,
            'teacher_count': 50
        })
        defaults = self.etl.get_suppressed_metric_defaults(sample_row)

        assert 'teacher_turnover_rate' in defaults
        assert 'teacher_turnover_count' in defaults
        assert 'teacher_count' in defaults
        assert pd.isna(defaults['teacher_turnover_rate'])

    def test_transform_integration(self):
        """Test complete transform function integration."""
        # Create sample files
        sample_2024 = self.create_sample_2024_data()
        sample_historical = self.create_sample_historical_data()

        # Save to test directory
        sample_2024.to_csv(self.sample_dir / "KYRC24_OVW_Teacher_Turnover.csv", index=False)
        sample_historical.to_csv(self.sample_dir / "teacher_turnover_2021.csv", index=False)

        # Run transform
        config = {}
        transform(self.raw_dir, self.proc_dir, config)

        # Check output file was created
        output_file = self.proc_dir / "teacher_turnover.csv"
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
        assert 'teacher_turnover_count' in sample_data.columns
        assert 'turnover_percent' in sample_data.columns

        # Test metric extraction
        row = sample_data.iloc[0]
        metrics = self.etl.extract_metrics(row)

        assert metrics['teacher_turnover_rate'] == 6.3
        assert metrics['teacher_turnover_count'] == 3.0
        assert metrics['teacher_count'] == 48.0


class TestTeacherTurnoverKyrc26:
    """KYRC26 opt-in. KYRC23 republishes 2022-23 and is skipped.

    Fixtures are verbatim rows from the downloaded files. ``N/A`` is kept
    as the text ``N/A``. The KYRC24 co-op cell ``-84.21926253`` is the
    historical value, not the datasets repair.
    """

    def setup_method(self):
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        self.sample_dir = self.raw_dir / "teacher_turnover"
        self.sample_dir.mkdir(parents=True)
        self.etl = TeacherTurnoverETL("teacher_turnover")

    def teardown_method(self):
        shutil.rmtree(self.test_dir)

    def _production_config(self) -> dict:
        config = yaml.safe_load((ROOT / "config" / "mappings.yaml").read_text())
        return config["sources"]["teacher_turnover"]

    def _copy_fixtures(self) -> None:
        for path in FIXTURE_DIR.glob("*.csv"):
            (self.sample_dir / path.name).write_bytes(path.read_bytes())

    def test_kyrc26_file_is_accepted(self):
        """The permanent KYRC files and the historical names stay in scope."""
        assert self.etl.include_kyrc26 is True
        assert self.etl.module_column_mappings["Teacher Count"] == "teacher_count"
        assert self.etl.module_column_mappings["Turnover Percent"] == "turnover_percent"
        assert self.etl.module_column_mappings["TEACHER COUNT"] == "teacher_count"
        assert "Instructional Staff Count" not in self.etl.module_column_mappings

        accepted = [
            "KYRC26_OVW_Teacher_Turnover.csv",
            "KYRC25_OVW_Teacher_Turnover.csv",
            "KYRC24_OVW_Teacher_Turnover.csv",
            "teacher_turnover_2023.csv",
            "teacher_turnover_2022.csv",
            "teacher_turnover_2021.csv",
            "teacher_turnover_2020.csv",
        ]
        for name in accepted:
            (self.sample_dir / name).write_text("School Year,School Code\n20252026,999000\n")
        (self.sample_dir / "KYRC23_OVW_Teacher_Turnover.csv").write_text(
            "School Year,School Code,Turnover Percent\n20222023,001010,0\n"
        )

        found = sorted(path.name for path, _sheet in self.etl._find_data_files(self.sample_dir))
        assert found == sorted(accepted)

    def test_kyrc23_republish_is_skipped(self):
        """A leftover KYRC23 file must not write a second copy of 2023.

        The datasets file is school year 20222023. It stores the historical
        Kentucky Tech ``N/A`` rows as 0. Accepting it would add those zeros
        beside the 2023 file, which emits nothing for ``N/A``.
        """
        republish = (
            "School Year,District Name,School Name,School Code,Teacher Count,"
            "Teacher Turnover Count,Turnover Percent\n"
            "20222023,Kentucky Tech System,Estill County Area Technology Center,"
            "777005,0,0,0.0\n"
            "20222023,All Districts,All Schools,999000,51598,12846,24.9\n"
        )
        (self.sample_dir / "KYRC23_OVW_Teacher_Turnover.csv").write_text(republish)
        (self.sample_dir / "KYRC26_OVW_Teacher_Turnover.csv").write_bytes(
            (FIXTURE_DIR / "KYRC26_OVW_Teacher_Turnover.csv").read_bytes()
        )
        transform(self.raw_dir, self.proc_dir, self._production_config())
        result = pd.read_csv(self.proc_dir / "teacher_turnover.csv", dtype=str)
        assert set(result["source_file"]) == {"KYRC26_OVW_Teacher_Turnover.csv"}
        assert "2023" not in set(result["year"])
        assert "777005" not in set(result["school_id"])

    def test_real_rows_match_the_downloaded_files(self):
        """State, district, school, a published zero, and a missing N/A row.

        There is no Suppressed column and no starred cell. Every stored row
        is ``suppressed`` ``N``. The rate is the published percent. 2021
        Brooklawn publishes 100, not 16/2. Paris High publishes 146.7.
        Instructional-staff columns are not metrics.
        """
        self._copy_fixtures()
        transform(self.raw_dir, self.proc_dir, self._production_config())
        result = pd.read_csv(self.proc_dir / "teacher_turnover.csv", dtype=str)
        assert (result["suppressed"] == "N").all()
        assert (result["student_group"] == "All Students").all()
        assert set(result["metric"]) == {
            "teacher_turnover_rate",
            "teacher_turnover_count",
            "teacher_count",
        }
        assert "777005" not in set(result["school_id"])

        def cell(year, school_id, metric):
            rows = result[
                (result["year"] == year)
                & (result["school_id"] == school_id)
                & (result["metric"] == metric)
            ]
            assert len(rows) == 1, f"{year} {school_id} {metric} -> {len(rows)}"
            return rows.iloc[0]

        state = cell("2026", "999000", "teacher_turnover_rate")
        assert float(state["value"]) == 10.5
        assert state["district"] == "All Districts"
        assert state["school_name"] == "---District Total---"
        assert state["source_file"] == "KYRC26_OVW_Teacher_Turnover.csv"
        assert float(cell("2026", "999000", "teacher_count")["value"]) == 40557.0
        assert float(cell("2026", "999000", "teacher_turnover_count")["value"]) == 4271.0

        fayette = cell("2026", "165000", "teacher_turnover_rate")
        assert float(fayette["value"]) == 15.4
        assert fayette["district"] == "Fayette County"
        assert fayette["nces_id"] == "2101860"
        assert fayette["co_op"] == "CKEC"
        assert float(cell("2026", "165000", "teacher_count")["value"]) == 2645.0

        bryan = cell("2026", "165170", "teacher_turnover_rate")
        assert float(bryan["value"]) == 15.1
        assert bryan["school_name"] == "Bryan Station High School"
        assert bryan["nces_id"] == "210186000388"
        assert bryan["state_school_id"] == "034165170"
        assert float(cell("2026", "165170", "teacher_count")["value"]) == 119.0
        assert float(cell("2026", "165170", "teacher_turnover_count")["value"]) == 18.0

        adair = cell("2026", "001010", "teacher_turnover_rate")
        assert float(adair["value"]) == 21.6
        assert adair["nces_id"] == "210003000001"

        # Published 0 with a positive teacher count, and a new school that is 0/0/0.
        zero = cell("2026", "015070", "teacher_turnover_rate")
        assert float(zero["value"]) == 0.0
        assert float(cell("2026", "015070", "teacher_count")["value"]) == 5.0
        assert float(cell("2026", "015070", "teacher_turnover_count")["value"]) == 0.0
        assert float(cell("2026", "011030", "teacher_turnover_rate")["value"]) == 0.0
        assert float(cell("2026", "011030", "teacher_count")["value"]) == 0.0

        # The published percent is not rebuilt, including a rate above 100.
        paris = cell("2026", "478020", "teacher_turnover_rate")
        assert float(paris["value"]) == 146.7
        assert float(cell("2026", "478020", "teacher_count")["value"]) == 15.0
        assert float(cell("2026", "478020", "teacher_turnover_count")["value"]) == 22.0

        # Prior-year anchors.
        assert float(cell("2025", "999000", "teacher_turnover_rate")["value"]) == 21.8
        assert float(cell("2025", "999000", "teacher_count")["value"]) == 48755.0
        assert float(cell("2025", "165000", "teacher_turnover_rate")["value"]) == 21.0
        assert float(cell("2025", "165170", "teacher_turnover_rate")["value"]) == 13.7
        assert cell("2025", "165170", "teacher_turnover_rate")["nces_id"] == "210186000000"
        assert float(cell("2024", "999000", "teacher_turnover_rate")["value"]) == 22.5
        assert float(cell("2024", "999000", "teacher_count")["value"]) == 49691.0
        baker = cell("2024", "121026", "teacher_turnover_rate")
        assert float(baker["value"]) == 21.4
        assert baker["co_op"] == "-84.21926253"
        assert baker["co_op_code"] == "CKEC"

        breathitt = cell("2023", "061", "teacher_turnover_rate")
        assert float(breathitt["value"]) == 22.8
        assert breathitt["district"] == "Breathitt County"
        assert breathitt["school_name"] == "---District Total---"
        assert float(cell("2023", "061", "teacher_count")["value"]) == 145.0
        assert float(cell("2023", "999", "teacher_turnover_rate")["value"]) == 24.9
        assert cell("2023", "999", "teacher_turnover_rate")["district"] == "State"
        assert cell("2023", "999", "teacher_turnover_rate")["school_name"] == "---State Total---"

        # 16 teachers left a school of 2. The file publishes 100, not 800.
        brooklawn = cell("2021", "275221", "teacher_turnover_rate")
        assert float(brooklawn["value"]) == 100.0
        assert float(cell("2021", "275221", "teacher_count")["value"]) == 2.0
        assert float(cell("2021", "275221", "teacher_turnover_count")["value"]) == 16.0
        assert float(cell("2021", "999", "teacher_turnover_rate")["value"]) == 16.2

        assert float(cell("2020", "999", "teacher_turnover_rate")["value"]) == 18.2
        assert float(cell("2020", "999", "teacher_count")["value"]) == 44784.0
        assert cell("2020", "999", "teacher_turnover_rate")["district"] == "State"
        # Zero teachers and a published percent are kept. The percent is not dropped.
        crossing = cell("2020", "525025", "teacher_turnover_rate")
        assert float(crossing["value"]) == 100.0
        assert float(cell("2020", "525025", "teacher_count")["value"]) == 0.0
        assert float(cell("2020", "525025", "teacher_turnover_count")["value"]) == 1.0
        assert float(cell("2020", "165", "teacher_turnover_rate")["value"]) == 23.2

        assert float(cell("2019", "999", "teacher_turnover_rate")["value"]) == 18.4
        assert float(cell("2019", "999", "teacher_count")["value"]) == 45261.0
        assert float(cell("2019", "001010", "teacher_turnover_rate")["value"]) == 12.1

    def test_comma_count_survives_an_int64_cast(self):
        """A comma-formatted count survives StringDtype and an Int64 cast.

        Production ``mappings.yaml`` has no dtype for this source. The cast
        here is the failure mode from issue #49: ``to_numeric`` on ``40,557``
        becomes missing before ``extract_metrics`` unless the comma is gone.
        """
        (self.sample_dir / "KYRC26_OVW_Teacher_Turnover.csv").write_bytes(
            (FIXTURE_DIR / "KYRC26_OVW_Teacher_Turnover.csv").read_bytes()
        )
        config = self._production_config()
        config["dtype"] = {
            "teacher_count": "Int64",
            "teacher_turnover_count": "Int64",
            "turnover_percent": "float64",
        }
        transform(self.raw_dir, self.proc_dir, config)
        result = pd.read_csv(self.proc_dir / "teacher_turnover.csv", dtype=str)
        state_count = result[
            (result["year"] == "2026")
            & (result["school_id"] == "999000")
            & (result["metric"] == "teacher_count")
        ]
        assert float(state_count.iloc[0]["value"]) == 40557.0
        fayette = result[
            (result["school_id"] == "165000")
            & (result["metric"] == "teacher_count")
        ]
        assert float(fayette.iloc[0]["value"]) == 2645.0

    def test_number_parsing_is_dtype_agnostic(self):
        """pandas 3 StringDtype and pandas 2 object follow the same rules.

        ``N/A`` stays missing. ``0`` stays 0. A comma is stripped.
        ``bool(pd.NA)`` is not used to detect the dtype. A blank percent
        is not turned into 0, even when the teacher count is positive.
        No registered year has a blank measure cell.
        """
        frame = pd.DataFrame({
            "school_code": pd.Series(["999000", "015070", "777005", "165000"], dtype="string"),
            "teacher_count": pd.Series(["40,557", "5", "N/A", "2,645"], dtype="string"),
            "teacher_turnover_count": pd.Series(["4,271", "0", "N/A", "407"], dtype="string"),
            "turnover_percent": pd.Series(["10.5", "0", "N/A", ""], dtype="string"),
        })
        assert not self.etl.should_skip_row(frame.iloc[0])
        assert self.etl.extract_metrics(frame.iloc[0]) == {
            "teacher_turnover_rate": 10.5,
            "teacher_turnover_count": 4271.0,
            "teacher_count": 40557.0,
        }
        assert self.etl.extract_metrics(frame.iloc[1])["teacher_turnover_rate"] == 0.0
        assert self.etl.extract_metrics(frame.iloc[1])["teacher_count"] == 5.0
        assert self.etl.extract_metrics(frame.iloc[2]) == {}
        blank = self.etl.extract_metrics(frame.iloc[3])
        assert "teacher_turnover_rate" not in blank
        assert blank["teacher_count"] == 2645.0

        as_object = frame.astype(object)
        assert self.etl.extract_metrics(as_object.iloc[2]) == {}
        assert self.etl.extract_metrics(as_object.iloc[0])["teacher_count"] == 40557.0
        assert self.etl.extract_metrics(as_object.iloc[1])["teacher_turnover_rate"] == 0.0


def test_registered_sources_are_the_permanent_files():
    """KYRC24, KYRC25, KYRC26, 2020-2023, and 2018-19 are registered.

    KYRC23 is not. The 2017-18 workbook is not. No temporary OAA name is.
    """
    config = yaml.safe_load((ROOT / "config" / "kde_sources.yaml").read_text())
    names = []
    for entry in config["raw_directories"]["teacher_turnover"]:
        names.append(entry["file"] if isinstance(entry, dict) else entry)

    assert names == [
        "KYRC24_OVW_Teacher_Turnover.csv",
        "KYRC25_OVW_Teacher_Turnover.csv",
        "KYRC26_OVW_Teacher_Turnover.csv",
        "teacher_turnover_2023.csv",
        "teacher_turnover_2022.csv",
        "teacher_turnover_2021.csv",
        "teacher_turnover_2020.csv",
        "TEACHER_TURNOVER_18-19.xlsx",
    ]
    assert not any("KYRC23" in name for name in names)
    assert not any("17-18" in name for name in names)
    assert not any(name.startswith("Teacher_Turnover_") for name in names)
