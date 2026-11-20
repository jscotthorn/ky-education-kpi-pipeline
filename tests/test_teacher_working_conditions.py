"""
Tests for Teacher Working Conditions ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
from etl.constants import KPI_COLUMNS
import shutil
from etl.teacher_working_conditions import (
    transform, TeacherWorkingConditionsETL
)


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

        # Save to test directory
        sample_kyrc25.to_csv(
            self.sample_dir / "KYRC25_OVW_Teacher_Working_Conditions.csv",
            index=False
        )
        sample_historical.to_csv(
            self.sample_dir / "teacher_working_conditions_2021.csv",
            index=False
        )

        # Run transform
        config = {}
        transform(self.raw_dir, self.proc_dir, config)

        # Check output file was created
        output_file = self.proc_dir / "teacher_working_conditions.csv"
        assert output_file.exists()

        # Load and validate output
        result_df = pd.read_csv(output_file)
        assert not result_df.empty
        assert set(result_df.columns).issubset(set(KPI_COLUMNS))

        # Check that we have data from both source files
        source_files = result_df['source_file'].unique()
        assert any('KYRC25' in f for f in source_files)
        assert any('2021' in f for f in source_files)

        # Check years
        years = result_df['year'].unique()
        assert 2025 in years or '2025' in years
        assert 2021 in years or '2021' in years

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
