"""
Tests for Teacher Working Conditions ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
import yaml
from etl.constants import KPI_COLUMNS
import shutil
from etl.teacher_working_conditions import (
    transform, TeacherWorkingConditionsETL
)

ROOT = Path(__file__).resolve().parents[1]


class TestTeacherWorkingConditionsETL:

    def setup_method(self):
        """Setup test directories and sample data."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)

        # Create sample raw data directory
        self.sample_dir = self.raw_dir / "teacher_working_conditions"
        self.sample_dir.mkdir(parents=True)

        # Create ETL instance for testing
        self.etl = TeacherWorkingConditionsETL('teacher_working_conditions')

    def teardown_method(self):
        """Clean up test directories."""
        shutil.rmtree(self.test_dir)

    def create_sample_kyrc25_data(self):
        """Create sample KYRC24/25 format data."""
        data = pd.DataFrame({
            'School Year': [
                '20242025', '20242025', '20242025',
                '20242025', '20242025', '20242025'
            ],
            'County Number': ['001', '001', '001', '001', '001', '001'],
            'County Name': ['Adair', 'Adair', 'Adair', 'Adair', 'Adair', 'Adair'],
            'District Number': ['001', '001', '001', '001', '001', '001'],
            'District Name': [
                'Adair County', 'Adair County', 'Adair County',
                'Adair County', 'Adair County', 'Adair County'
            ],
            'School Number': ['', '', '', '010', '010', '010'],
            'School Name': [
                'All Schools', 'All Schools', 'All Schools',
                'Adair County High', 'Adair County High', 'Adair County High'
            ],
            'School Code': ['001000', '001000', '001000', '001010', '001010', '001010'],
            'State School Id': ['', '', '', '001001010', '001001010', '001001010'],
            'NCES ID': ['', '', '', '210003000001', '210003000001', '210003000001'],
            'CO-OP': ['GRREC', 'GRREC', 'GRREC', 'GRREC', 'GRREC', 'GRREC'],
            'CO-OP Code': ['902', '902', '902', '902', '902', '902'],
            'School Type': ['', '', '', 'A1', 'A1', 'A1'],
            'Impact Measure': [
                'Managing Student Behavior', 'School Climate', 'School Leadership',
                'Managing Student Behavior', 'School Climate', 'School Leadership'
            ],
            'Impact Value': ['67', '65', '69', '55', '58', '62']
        })
        return data

    def create_sample_historical_data(self):
        """Create sample historical format data (uppercase columns, Composite suffix)."""
        data = pd.DataFrame({
            'SCHOOL YEAR': [
                '20202021', '20202021', '20202021', '20202021',
                '20202021', '20202021', '20202021', '20202021'
            ],
            'COUNTY NUMBER': ['001', '001', '001', '001', '001', '001', '001', '001'],
            'COUNTY NAME': ['ADAIR', 'ADAIR', 'ADAIR', 'ADAIR', 'ADAIR', 'ADAIR', 'ADAIR', 'ADAIR'],
            'DISTRICT NUMBER': ['001', '001', '001', '001', '001', '001', '001', '001'],
            'DISTRICT NAME': [
                'Adair County', 'Adair County', 'Adair County', 'Adair County',
                'Adair County', 'Adair County', 'Adair County', 'Adair County'
            ],
            'SCHOOL NUMBER': ['', '', '', '', '010', '010', '010', '010'],
            'SCHOOL NAME': [
                '---District Total---', '---District Total---',
                '---District Total---', '---District Total---',
                'Adair County High School', 'Adair County High School',
                'Adair County High School', 'Adair County High School'
            ],
            'SCHOOL CODE': ['001', '001', '001', '001', '001010', '001010', '001010', '001010'],
            'STATE SCHOOL ID': ['', '', '', '', '001001010', '001001010', '001001010', '001001010'],
            'NCES ID': ['', '', '', '', '210003000001', '210003000001', '210003000001', '210003000001'],
            'CO-OP': ['GRREC', 'GRREC', 'GRREC', 'GRREC', 'GRREC', 'GRREC', 'GRREC', 'GRREC'],
            'CO-OP CODE': ['902', '902', '902', '902', '902', '902', '902', '902'],
            'SCHOOL TYPE': ['', '', '', '', 'A1', 'A1', 'A1', 'A1'],
            'IMPACT MEASURE': [
                'School Leadership Composite', 'Managing Student Behavior Composite',
                'Teaching Environment Composite', 'School Climate Composite',
                'School Leadership Composite', 'Managing Student Behavior Composite',
                'Teaching Environment Composite', 'School Climate Composite'
            ],
            'IMPACT VALUE': ['71.0', '64.0', '59.0', '68.0', '79.0', '72.0', '65.0', '75.0']
        })
        return data

    def test_column_mappings(self):
        """Test that column mappings include required fields."""
        mappings = self.etl.module_column_mappings

        # Test title case (KYRC24/25)
        assert 'Impact Measure' in mappings
        assert mappings['Impact Measure'] == 'impact_measure'
        assert 'Impact Value' in mappings
        assert mappings['Impact Value'] == 'impact_value'

        # Test uppercase variants (historical)
        assert 'IMPACT MEASURE' in mappings
        assert mappings['IMPACT MEASURE'] == 'impact_measure'
        assert 'IMPACT VALUE' in mappings
        assert mappings['IMPACT VALUE'] == 'impact_value'

    def test_impact_measure_map(self):
        """Test impact measure to metric suffix mapping."""
        measure_map = self.etl.IMPACT_MEASURE_MAP

        # KYRC24/25 format
        assert measure_map['Managing Student Behavior'] == 'managing_student_behavior'
        assert measure_map['School Climate'] == 'school_climate'
        assert measure_map['School Leadership'] == 'school_leadership'

        # Historical format with Composite suffix
        assert measure_map['Managing Student Behavior Composite'] == 'managing_student_behavior'
        assert measure_map['School Climate Composite'] == 'school_climate'
        assert measure_map['School Leadership Composite'] == 'school_leadership'
        assert measure_map['Teaching Environment Composite'] == 'teaching_environment'

    def test_extract_metrics_managing_student_behavior(self):
        """Test metric extraction for Managing Student Behavior."""
        sample_data = self.create_sample_kyrc25_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        # Test Managing Student Behavior row
        row = sample_data[sample_data['impact_measure'] == 'Managing Student Behavior'].iloc[0]
        metrics = self.etl.extract_metrics(row)

        assert 'teacher_working_conditions_managing_student_behavior' in metrics
        assert metrics['teacher_working_conditions_managing_student_behavior'] == 67.0

    def test_extract_metrics_school_climate(self):
        """Test metric extraction for School Climate."""
        sample_data = self.create_sample_kyrc25_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        # Test School Climate row
        row = sample_data[sample_data['impact_measure'] == 'School Climate'].iloc[0]
        metrics = self.etl.extract_metrics(row)

        assert 'teacher_working_conditions_school_climate' in metrics
        assert metrics['teacher_working_conditions_school_climate'] == 65.0

    def test_extract_metrics_school_leadership(self):
        """Test metric extraction for School Leadership."""
        sample_data = self.create_sample_kyrc25_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        # Test School Leadership row
        row = sample_data[sample_data['impact_measure'] == 'School Leadership'].iloc[0]
        metrics = self.etl.extract_metrics(row)

        assert 'teacher_working_conditions_school_leadership' in metrics
        assert metrics['teacher_working_conditions_school_leadership'] == 69.0

    def test_extract_metrics_historical_composite(self):
        """Test metric extraction for historical data with Composite suffix."""
        sample_data = self.create_sample_historical_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        # Test Teaching Environment Composite row
        row = sample_data[sample_data['impact_measure'] == 'Teaching Environment Composite'].iloc[0]
        metrics = self.etl.extract_metrics(row)

        assert 'teacher_working_conditions_teaching_environment' in metrics
        assert metrics['teacher_working_conditions_teaching_environment'] == 59.0

    def test_should_skip_row_invalid_measure(self):
        """Test that rows with invalid impact measures are skipped."""
        row_invalid = pd.Series({
            'school_code': '001010',
            'impact_measure': 'Unknown Measure',
            'impact_value': 70
        })
        assert self.etl.should_skip_row(row_invalid)

        row_valid = pd.Series({
            'school_code': '001010',
            'impact_measure': 'School Climate',
            'impact_value': 70
        })
        assert not self.etl.should_skip_row(row_valid)

    def test_should_skip_row_empty_measure(self):
        """Test that rows with empty impact measures are skipped."""
        row_empty = pd.Series({
            'school_code': '001010',
            'impact_measure': '',
            'impact_value': 70
        })
        assert self.etl.should_skip_row(row_empty)

    def test_create_kpi_template_all_students(self):
        """Test that KPI template sets student_group to All Students."""
        sample_data = self.create_sample_kyrc25_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        row = sample_data.iloc[0]
        template = self.etl.create_kpi_template(row, "test_file.csv")

        assert template['student_group'] == 'All Students'

    def test_convert_to_kpi_format(self):
        """Test KPI format conversion."""
        sample_data = self.create_sample_kyrc25_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        kpi_df = self.etl.convert_to_kpi_format(sample_data, "test_file.csv")

        # Check that DataFrame has correct columns
        assert set(kpi_df.columns).issubset(set(KPI_COLUMNS))

        # Check that all student_group values are 'All Students'
        assert all(kpi_df['student_group'] == 'All Students')

        # Check metric names
        metrics = kpi_df['metric'].unique()
        assert 'teacher_working_conditions_managing_student_behavior' in metrics
        assert 'teacher_working_conditions_school_climate' in metrics
        assert 'teacher_working_conditions_school_leadership' in metrics

    def test_convert_to_kpi_format_values(self):
        """Test that KPI format has correct values."""
        sample_data = self.create_sample_kyrc25_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        kpi_df = self.etl.convert_to_kpi_format(sample_data, "test_file.csv")

        # Filter to district total, Managing Student Behavior
        district_msb = kpi_df[
            (kpi_df['metric'] == 'teacher_working_conditions_managing_student_behavior') &
            (kpi_df['school_id'] == '001000')
        ]

        assert len(district_msb) == 1
        assert district_msb.iloc[0]['value'] == 67.0

        # Filter to school level, School Leadership
        school_sl = kpi_df[
            (kpi_df['metric'] == 'teacher_working_conditions_school_leadership') &
            (kpi_df['school_id'] == '001010')
        ]

        assert len(school_sl) == 1
        assert school_sl.iloc[0]['value'] == 62.0

    def test_suppressed_metric_defaults(self):
        """Test suppressed metric defaults."""
        sample_row = pd.Series({
            'impact_measure': 'School Climate',
            'impact_value': pd.NA
        })
        defaults = self.etl.get_suppressed_metric_defaults(sample_row)

        assert 'teacher_working_conditions_school_climate' in defaults
        assert pd.isna(defaults['teacher_working_conditions_school_climate'])

    def test_transform_integration(self):
        """Test complete transform function integration."""
        # Create sample files
        sample_kyrc25 = self.create_sample_kyrc25_data()
        sample_historical = self.create_sample_historical_data()

        # Administration-year files. The school-year cell, not the filename,
        # is what extract_year reads. 20232024 is the 2023-24 survey.
        # 20192020 is the January-February 2020 survey.
        sample_kyrc25["School Year"] = "20232024"
        sample_historical["SCHOOL YEAR"] = "20192020"
        sample_kyrc25.to_csv(
            self.sample_dir / "KYRC24_OVW_Teacher_Working_Conditions.csv",
            index=False
        )
        sample_historical.to_csv(
            self.sample_dir / "teacher_working_conditions_2020.csv",
            index=False
        )
        # A KYRC25 key the datasets file already supplies is not loaded.
        # 777135 is not in that file, so those cells are stored as 2024.
        (self.sample_dir / "KYRC25_OVW_Teacher_Working_Conditions.csv").write_text(
            "School Year,District Name,School Name,School Code,Impact Measure,Impact Value\n"
            "20242025,Adair County,All Schools,001000,School Climate,1\n"
            "20242025,Kentucky Tech System,Mason County Career Magnet School,777135,Managing Student Behavior,86\n"
        )
        (self.sample_dir / "teacher_working_conditions_2021.csv").write_text(
            "SCHOOL YEAR,IMPACT MEASURE,IMPACT VALUE\n20202021,School Climate Composite,1\n"
        )

        # Run transform
        config = {}
        transform(self.raw_dir, self.proc_dir, config)

        # Check output file was created
        output_file = self.proc_dir / "teacher_working_conditions.csv"
        assert output_file.exists()

        # Load and validate output. School codes keep their leading zeros.
        result_df = pd.read_csv(output_file, dtype=str)
        assert not result_df.empty
        assert set(result_df.columns).issubset(set(KPI_COLUMNS))

        # Check that we have data from both administration files
        source_files = set(result_df['source_file'].unique())
        assert "KYRC24_OVW_Teacher_Working_Conditions.csv" in source_files
        assert "teacher_working_conditions_2020.csv" in source_files
        assert "KYRC25_OVW_Teacher_Working_Conditions.csv" in source_files
        assert "teacher_working_conditions_2021.csv" not in source_files

        # 2021 is the republished copy. 777135 from KYRC25 is year 2024.
        years = set(result_df['year'].astype(str).unique())
        assert years == {"2024", "2020"}
        magnet = result_df[result_df["school_id"].astype(str) == "777135"]
        assert len(magnet) == 1
        assert float(magnet.iloc[0]["value"]) == 86.0
        assert magnet.iloc[0]["source_file"] == "KYRC25_OVW_Teacher_Working_Conditions.csv"
        adair = result_df[result_df["school_id"].astype(str) == "001000"]
        assert set(adair["source_file"]) == {"KYRC24_OVW_Teacher_Working_Conditions.csv"}
        assert float(adair[adair["metric"] == "teacher_working_conditions_school_climate"].iloc[0]["value"]) == 65.0

        # Check all metrics are present
        metrics = result_df['metric'].unique()
        assert 'teacher_working_conditions_managing_student_behavior' in metrics
        assert 'teacher_working_conditions_school_climate' in metrics
        assert 'teacher_working_conditions_school_leadership' in metrics
        # Teaching environment only in historical data
        assert 'teacher_working_conditions_teaching_environment' in metrics

    def test_decimal_values(self):
        """Test that decimal impact values are handled correctly."""
        data = pd.DataFrame({
            'School Year': ['20242025'],
            'County Number': ['001'],
            'County Name': ['Adair'],
            'District Number': ['001'],
            'District Name': ['Adair County'],
            'School Number': [''],
            'School Name': ['All Schools'],
            'School Code': ['001000'],
            'State School Id': [''],
            'NCES ID': [''],
            'CO-OP': ['GRREC'],
            'CO-OP Code': ['902'],
            'School Type': [''],
            'Impact Measure': ['School Climate'],
            'Impact Value': ['67.5']
        })

        data = self.etl.normalize_column_names(data)
        kpi_df = self.etl.convert_to_kpi_format(data, "test_file.csv")

        assert len(kpi_df) == 1
        assert kpi_df.iloc[0]['value'] == 67.5

    def test_missing_impact_value(self):
        """Test that rows with missing impact values are handled correctly."""
        data = pd.DataFrame({
            'School Year': ['20242025', '20242025'],
            'County Number': ['001', '001'],
            'County Name': ['Adair', 'Adair'],
            'District Number': ['001', '001'],
            'District Name': ['Adair County', 'Adair County'],
            'School Number': ['', ''],
            'School Name': ['All Schools', 'All Schools'],
            'School Code': ['001000', '001000'],
            'State School Id': ['', ''],
            'NCES ID': ['', ''],
            'CO-OP': ['GRREC', 'GRREC'],
            'CO-OP Code': ['902', '902'],
            'School Type': ['', ''],
            'Impact Measure': ['School Climate', 'School Leadership'],
            'Impact Value': ['', '70']  # First value is empty
        })

        data = self.etl.normalize_column_names(data)
        kpi_df = self.etl.convert_to_kpi_format(data, "test_file.csv")

        # Only School Leadership should be in output (School Climate has empty value)
        assert len(kpi_df) == 1
        assert kpi_df.iloc[0]['metric'] == 'teacher_working_conditions_school_leadership'
        assert kpi_df.iloc[0]['value'] == 70.0


class TestKyrc26TeacherWorkingConditions:
    """KYRC26 opt-in. KYRC23 republishes 2022-23 and is skipped."""

    def setup_method(self):
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        self.sample_dir = self.raw_dir / "teacher_working_conditions"
        self.sample_dir.mkdir(parents=True)
        self.etl = TeacherWorkingConditionsETL("teacher_working_conditions")

    def teardown_method(self):
        shutil.rmtree(self.test_dir)

    def _production_config(self) -> dict:
        config = yaml.safe_load((ROOT / "config" / "mappings.yaml").read_text())
        return config["sources"]["teacher_working_conditions"]

    def test_kyrc26_file_is_accepted(self):
        """The permanent KYRC files and the historical names stay in scope."""
        assert self.etl.include_kyrc26 is True
        assert self.etl.module_column_mappings["Impact Measure"] == "impact_measure"
        assert self.etl.module_column_mappings["Impact Value"] == "impact_value"
        assert self.etl.module_column_mappings["IMPACT MEASURE"] == "impact_measure"
        assert self.etl.module_column_mappings["IMPACT VALUE"] == "impact_value"

        accepted = [
            "KYRC26_OVW_Teacher_Working_Conditions.csv",
            "KYRC24_OVW_Teacher_Working_Conditions.csv",
            "KYRC24_OVW_Teacher_Working_Conditions_historical.csv",
            "KYRC25_OVW_Teacher_Working_Conditions.csv",
            "teacher_working_conditions_2022.csv",
            "teacher_working_conditions_2020.csv",
        ]
        skipped = [
            "KYRC23_OVW_Teacher_Working_Conditions.csv",
            "teacher_working_conditions_2023.csv",
            "teacher_working_conditions_2021.csv",
        ]
        for name in accepted + skipped:
            (self.sample_dir / name).write_text(
                "School Year,Impact Measure,Impact Value\n20252026,School Climate,67\n"
            )

        found = sorted(path.name for path, _sheet in self.etl._find_data_files(self.sample_dir))
        assert found == sorted(accepted)

    def test_kyrc23_republish_is_skipped(self):
        """A leftover KYRC23 file must not write a second copy of 2023.

        The datasets file is school year 20222023. It relabels the third
        construct School Climate and turns the 945 historical N/A cells into 0.
        Accepting it would add school_climate rows beside teaching_environment.
        """
        republish = (
            "School Year,District Name,School Name,School Code,Impact Measure,Impact Value\n"
            "20222023,Adair County,Adair County High School,001010,School Climate,0\n"
            "20222023,Adair County,Adair County High School,001010,Managing Student Behavior,76\n"
        )
        (self.sample_dir / "KYRC23_OVW_Teacher_Working_Conditions.csv").write_text(republish)
        (self.sample_dir / "teacher_working_conditions_2021.csv").write_text(
            "SCHOOL YEAR,DISTRICT NAME,SCHOOL NAME,SCHOOL CODE,IMPACT MEASURE,IMPACT VALUE\n"
            "20202021,State,---State Total---,999,Managing Student Behavior Composite,65.0\n"
        )
        (self.sample_dir / "teacher_working_conditions_2023.csv").write_text(
            "SCHOOL YEAR,DISTRICT NAME,SCHOOL NAME,SCHOOL CODE,IMPACT MEASURE,IMPACT VALUE\n"
            "20222023,State,---State Total---,999,Managing Student Behavior Composite,65\n"
        )
        (self.sample_dir / "KYRC25_OVW_Teacher_Working_Conditions.csv").write_text(
            "School Year,District Name,School Name,School Code,Impact Measure,Impact Value\n"
            "20242025,All Districts,All Schools,999000,Managing Student Behavior,67\n"
        )
        (self.sample_dir / "KYRC26_OVW_Teacher_Working_Conditions.csv").write_text(
            "School Year,District Name,School Name,School Code,Impact Measure,Impact Value\n"
            "20252026,All Districts,All Schools,999000,Managing Student Behavior,69\n"
        )
        transform(self.raw_dir, self.proc_dir, self._production_config())
        result = pd.read_csv(self.proc_dir / "teacher_working_conditions.csv", dtype=str)
        assert set(result["year"]) == {"2026"}
        assert set(result["source_file"]) == {"KYRC26_OVW_Teacher_Working_Conditions.csv"}
        assert float(result["value"].iloc[0]) == 69.0
        assert "teacher_working_conditions_school_climate" not in set(result["metric"])

    def test_na_row_emits_nothing_and_zero_is_kept(self):
        """N/A is not offered. A published 0 is a real percent favorable.

        The historical files store an insufficient result as the text N/A.
        There is no response-count column, so that text is not a zero.
        notes/8: a row that is not suppressed and has an empty value emits
        nothing. Boys & Girls Haven publishes School Leadership 0 on the
        2023-24 survey. The 2024-25 file is that survey again and is not
        loaded. The school is not in the 2026 file. This constructed row
        still has school year 20242025 so the parser's year rule can be
        seen apart from the filename skip.
        """
        text = (
            "School Year,District Name,School Name,School Code,Impact Measure,Impact Value\n"
            "20222023,Ashland Independent,Ashland Head Start,012012,School Leadership,N/A\n"
            "20222023,Ashland Independent,Ashland Head Start,012012,Managing Student Behavior,N/A\n"
            "20222023,Ashland Independent,Ashland Head Start,012012,Teaching Environment Composite,N/A\n"
            "20242025,Jefferson County,Boys & Girls Haven,275028,School Leadership,0\n"
            "20242025,Jefferson County,Boys & Girls Haven,275028,School Climate,28\n"
            "20252026,Fayette County,Bryan Station High School,165170,Managing Student Behavior,69\n"
        )
        (self.sample_dir / "KYRC26_OVW_Teacher_Working_Conditions.csv").write_text(text)
        transform(self.raw_dir, self.proc_dir, self._production_config())
        result = pd.read_csv(self.proc_dir / "teacher_working_conditions.csv", dtype=str)
        assert "012012" not in set(result["school_id"])
        assert set(result["school_id"]) == {"275028", "165170"}
        zero = result[result["school_id"] == "275028"]
        assert set(zero["metric"]) == {
            "teacher_working_conditions_school_leadership",
            "teacher_working_conditions_school_climate",
        }
        leadership = zero[zero["metric"] == "teacher_working_conditions_school_leadership"].iloc[0]
        assert float(leadership["value"]) == 0.0
        assert leadership["suppressed"] == "N"
        assert leadership["year"] == "2025"
        assert (result["suppressed"] == "N").all()

    def test_suppressed_row_is_kept_empty(self):
        """A suppressed construct is stored with an empty value.

        These files have no Suppressed column and no starred cell. The
        constructed Y row is the notes/8 shape: the value is empty and the
        row is not dropped.
        """
        text = (
            "School Year,District Name,School Name,School Code,Impact Measure,Impact Value,Suppressed\n"
            "20252026,All Districts,All Schools,999000,School Climate,,Y\n"
            "20252026,Fayette County,Bryan Station High School,165170,School Leadership,74,N\n"
        )
        (self.sample_dir / "KYRC26_OVW_Teacher_Working_Conditions.csv").write_text(text)
        transform(self.raw_dir, self.proc_dir, self._production_config())
        result = pd.read_csv(self.proc_dir / "teacher_working_conditions.csv", dtype=str)
        suppressed = result[result["suppressed"] == "Y"]
        assert len(suppressed) == 1
        assert suppressed.iloc[0]["school_id"] == "999000"
        assert suppressed.iloc[0]["metric"] == "teacher_working_conditions_school_climate"
        assert pd.isna(suppressed.iloc[0]["value"]) or suppressed.iloc[0]["value"] == ""
        kept = result[result["school_id"] == "165170"].iloc[0]
        assert float(kept["value"]) == 74.0
        assert kept["suppressed"] == "N"

    def test_percent_parsing_is_dtype_agnostic(self):
        """pandas 3 StringDtype and pandas 2 object follow the same rules.

        ``read_csv(..., dtype=str)`` returns StringDtype on pandas 3 and
        object on pandas 2.3.3. ``N/A`` stays missing. ``0`` stays 0.
        A comma is stripped. ``bool(pd.NA)`` is not used to detect the dtype.
        No registered percent contains a comma.
        """
        frame = pd.DataFrame({
            "impact_measure": pd.Series(
                ["School Climate", "School Leadership", "Managing Student Behavior", "School Climate"],
                dtype="string",
            ),
            "impact_value": pd.Series(["69", "0", "N/A", "1,00"], dtype="string"),
            "suppressed": pd.Series(["N", "N", "N", "N"], dtype="string"),
        })
        assert not self.etl.should_skip_row(frame.iloc[0])
        assert not self.etl.should_skip_row(frame.iloc[1])
        assert self.etl.should_skip_row(frame.iloc[2])
        assert not self.etl.should_skip_row(frame.iloc[3])
        assert self.etl.extract_metrics(frame.iloc[0]) == {
            "teacher_working_conditions_school_climate": 69.0
        }
        assert self.etl.extract_metrics(frame.iloc[1]) == {
            "teacher_working_conditions_school_leadership": 0.0
        }
        assert self.etl.extract_metrics(frame.iloc[2]) == {}
        assert self.etl.extract_metrics(frame.iloc[3]) == {
            "teacher_working_conditions_school_climate": 100.0
        }

        as_object = frame.astype(object)
        assert self.etl.should_skip_row(as_object.iloc[2])
        assert self.etl.extract_metrics(as_object.iloc[1])[
            "teacher_working_conditions_school_leadership"
        ] == 0.0

    def test_star_is_stored_suppressed(self):
        """A privacy star is value NA with suppressed Y. A blank is not stored.

        These files have no star today. The rule is the same one the other
        modules use: the star is not turned into 0 and is not dropped.
        """
        text = (
            "School Year,District Name,School Name,School Code,Impact Measure,Impact Value\n"
            "20252026,Fayette County,Bryan Station High School,165170,School Climate,*\n"
            "20252026,Fayette County,Bryan Station High School,165170,School Leadership,\n"
            "20252026,Fayette County,Bryan Station High School,165170,Managing Student Behavior,69\n"
        )
        (self.sample_dir / "KYRC26_OVW_Teacher_Working_Conditions.csv").write_text(text)
        transform(self.raw_dir, self.proc_dir, self._production_config())
        result = pd.read_csv(self.proc_dir / "teacher_working_conditions.csv", dtype=str)
        assert set(result["metric"]) == {
            "teacher_working_conditions_school_climate",
            "teacher_working_conditions_managing_student_behavior",
        }
        starred = result[result["metric"] == "teacher_working_conditions_school_climate"].iloc[0]
        assert starred["suppressed"] == "Y"
        assert pd.isna(starred["value"]) or starred["value"] == ""
        assert starred["school_id"] == "165170"
        behavior = result[
            result["metric"] == "teacher_working_conditions_managing_student_behavior"
        ].iloc[0]
        assert float(behavior["value"]) == 69.0
        assert behavior["suppressed"] == "N"

    def test_conflicting_glasgow_duplicate_is_not_stored_twice(self):
        """Glasgow 197 publishes two district totals. Agreeing percents stay once.

        Leadership and teaching environment disagree, so neither published
        number is chosen. Behavior is 69 on both rows and is stored once.
        """
        text = (
            "SCHOOL YEAR,DISTRICT NAME,SCHOOL NAME,SCHOOL CODE,IMPACT MEASURE,IMPACT VALUE\n"
            "20192020,Glasgow Independent,---District Total---,197,Managing Student Behavior Composite,69.0\n"
            "20192020,Glasgow Independent,---District Total---,197,Managing Student Behavior Composite,69.0\n"
            "20192020,Glasgow Independent,---District Total---,197,School Leadership Composite,66.0\n"
            "20192020,Glasgow Independent,---District Total---,197,School Leadership Composite,55.0\n"
            "20192020,Glasgow Independent,---District Total---,197,Teaching Environment Composite,79.0\n"
            "20192020,Glasgow Independent,---District Total---,197,Teaching Environment Composite,68.0\n"
        )
        (self.sample_dir / "teacher_working_conditions_2020.csv").write_text(text)
        transform(self.raw_dir, self.proc_dir, self._production_config())
        result = pd.read_csv(self.proc_dir / "teacher_working_conditions.csv", dtype=str)
        assert set(result["school_id"]) == {"197"}
        assert set(result["metric"]) == {
            "teacher_working_conditions_managing_student_behavior"
        }
        assert len(result) == 1
        assert float(result.iloc[0]["value"]) == 69.0
        assert result.iloc[0]["suppressed"] == "N"
        assert result.iloc[0]["year"] == "2020"
        keys = ["school_id", "year", "student_group", "metric"]
        assert not result.duplicated(keys).any()

    def test_datasets_file_wins_and_missing_codes_are_filled(self):
        """Datasets KYRC24 supplies the 2024 percents. Gaps are filled from the other copies.

        Carroll behavior 67 is the historical district total. The datasets
        file publishes 57, and that is the stored value. Craft Academy is
        only in the historical file. Mason County Career Magnet 777135 is
        only in KYRC25, whose school year is 20242025, and is stored as 2024.
        """
        header = "School Year,District Name,School Name,School Code,Impact Measure,Impact Value\n"
        (self.sample_dir / "KYRC24_OVW_Teacher_Working_Conditions.csv").write_text(
            header
            + "20232024,Carroll County,All Schools,101000,Managing Student Behavior,57\n"
            + "20232024,Carroll County,All Schools,101000,School Climate,51\n"
            + "20232024,Carroll County,All Schools,101000,School Leadership,60\n"
        )
        (self.sample_dir / "KYRC24_OVW_Teacher_Working_Conditions_historical.csv").write_text(
            header
            + "20232024,Carroll County,All Schools,101000,Managing Student Behavior,67\n"
            + "20232024,Craft Academy,All Schools,610000,Managing Student Behavior,75\n"
            + "20232024,Craft Academy,All Schools,610000,School Climate,75\n"
            + "20232024,Craft Academy,All Schools,610000,School Leadership,78\n"
            + "20232024,Craft Academy,The Craft Academy for Excellence in Science and Mathematics,610300,Managing Student Behavior,97\n"
            + "20232024,Craft Academy,The Craft Academy for Excellence in Science and Mathematics,610300,School Climate,96\n"
            + "20232024,Craft Academy,The Craft Academy for Excellence in Science and Mathematics,610300,School Leadership,98\n"
        )
        (self.sample_dir / "KYRC25_OVW_Teacher_Working_Conditions.csv").write_text(
            header
            + "20242025,Carroll County,All Schools,101000,Managing Student Behavior,57\n"
            + "20242025,Kentucky Tech System,Mason County Career Magnet School,777135,Managing Student Behavior,86\n"
            + "20242025,Kentucky Tech System,Mason County Career Magnet School,777135,School Climate,78\n"
            + "20242025,Kentucky Tech System,Mason County Career Magnet School,777135,School Leadership,78\n"
        )
        transform(self.raw_dir, self.proc_dir, self._production_config())
        result = pd.read_csv(self.proc_dir / "teacher_working_conditions.csv", dtype=str)
        behavior = "teacher_working_conditions_managing_student_behavior"
        climate = "teacher_working_conditions_school_climate"
        leadership = "teacher_working_conditions_school_leadership"
        carroll = result[result["school_id"] == "101000"]
        assert set(carroll["year"]) == {"2024"}
        assert set(carroll["source_file"]) == {"KYRC24_OVW_Teacher_Working_Conditions.csv"}
        assert {
            row["metric"]: float(row["value"]) for _, row in carroll.iterrows()
        } == {behavior: 57.0, climate: 51.0, leadership: 60.0}
        craft_district = result[result["school_id"] == "610000"]
        assert set(craft_district["source_file"]) == {
            "KYRC24_OVW_Teacher_Working_Conditions_historical.csv"
        }
        assert set(craft_district["year"]) == {"2024"}
        assert {
            row["metric"]: float(row["value"]) for _, row in craft_district.iterrows()
        } == {behavior: 75.0, climate: 75.0, leadership: 78.0}
        craft_school = result[result["school_id"] == "610300"]
        assert {
            row["metric"]: float(row["value"]) for _, row in craft_school.iterrows()
        } == {behavior: 97.0, climate: 96.0, leadership: 98.0}
        magnet = result[result["school_id"] == "777135"]
        assert set(magnet["year"]) == {"2024"}
        assert set(magnet["source_file"]) == {"KYRC25_OVW_Teacher_Working_Conditions.csv"}
        assert {
            row["metric"]: float(row["value"]) for _, row in magnet.iterrows()
        } == {behavior: 86.0, climate: 78.0, leadership: 78.0}
        assert "2025" not in set(result["year"])
        keys = ["school_id", "year", "student_group", "metric"]
        assert not result.duplicated(keys).any()
        assert len(result) == 12


def test_registered_sources_are_the_permanent_files():
    """The administration files are registered. The full republished copies are not.

    Datasets KYRC24 is the 2024 file. The historical copy is saved under
    another local name. KYRC25 is registered so 777135 can be filled.
    """
    config = yaml.safe_load((ROOT / "config" / "kde_sources.yaml").read_text())
    entries = config["raw_directories"]["teacher_working_conditions"]
    names = []
    for entry in entries:
        names.append(entry["file"] if isinstance(entry, dict) else entry)

    assert names == [
        "KYRC24_OVW_Teacher_Working_Conditions.csv",
        "KYRC24_OVW_Teacher_Working_Conditions.csv",
        "KYRC25_OVW_Teacher_Working_Conditions.csv",
        "KYRC26_OVW_Teacher_Working_Conditions.csv",
        "teacher_working_conditions_2022.csv",
        "teacher_working_conditions_2020.csv",
    ]
    datasets = entries[0]
    historical = entries[1]
    assert datasets["url"] == "kyrc25"
    assert "save_as" not in datasets
    assert historical["url"] == "historical"
    assert historical["save_as"] == "KYRC24_OVW_Teacher_Working_Conditions_historical.csv"
    assert entries[2]["url"] == "kyrc25"
    assert not any("KYRC23" in name for name in names)
    assert "teacher_working_conditions_2021.csv" not in names
    assert "teacher_working_conditions_2023.csv" not in names
    assert not any("Teacher_Working_Conditions_20" in name for name in names)
    assert not any(name.startswith("TELL_EQUITY") for name in names)
