"""End-to-end integration tests for Safe Schools Discipline ETL"""
import shutil
import tempfile
from pathlib import Path

import pandas as pd
from etl.safe_schools_discipline import transform

from etl.constants import KPI_COLUMNS

class TestSafeSchoolsDisciplineEndToEnd:
    def setup_method(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.temp_dir / "raw"
        self.proc_dir = self.temp_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        self.sample_dir = self.raw_dir / "safe_schools_discipline"
        self.sample_dir.mkdir(parents=True)

    def teardown_method(self):
        shutil.rmtree(self.temp_dir)

    def create_kyrc24_discipline_data(self):
        """Create sample KYRC24 discipline resolutions data."""
        return pd.DataFrame({
            "School Year": ["20232024", "20232024"],
            "County Number": ["047", "047"],
            "County Name": ["Fayette", "Fayette"],
            "District Number": ["047", "047"],
            "District Name": ["Fayette County", "Fayette County"],
            "School Number": ["001", "001"],
            "School Name": ["Test Elementary", "Test Elementary"],
            "School Code": ["047001", "047001"],
            "State School Id": ["12345", "12345"],
            "NCES ID": ["210047000001", "210047000001"],
            "Demographic": ["All Students", "Male"],
            "Total": [200, 110],
            "Corporal Punishment (SSP5)": [0, 0],
            "Restraint (SSP7)": [4, 3],
            "Seclusion (SSP8)": [2, 1],
            "Expelled, Not Receiving Services (SSP2)": [1, 1],
            "Expelled, Receiving Services (SSP1)": [2, 1],
            "In-School Removal (INSR) Or In-District Removal (INDR) >=.5": [80, 45],
            "Out-Of-School Suspensions (SSP3)": [60, 35],
            "Removal By Hearing Officer (IAES2)": [0, 0],
            "Unilateral Removal By School Personnel (IAES1)": [2, 1]
        })

    def create_kyrc24_legal_data(self):
        """Create sample KYRC24 legal sanctions data."""
        return pd.DataFrame({
            "School Year": ["20232024", "20232024"],
            "County Number": ["047", "047"],
            "County Name": ["Fayette", "Fayette"],
            "District Number": ["047", "047"],
            "District Name": ["Fayette County", "Fayette County"],
            "School Number": ["001", "001"],
            "School Name": ["Test Elementary", "Test Elementary"],
            "School Code": ["047001", "047001"],
            "Demographic": ["All Students", "Male"],
            "Total": [50, 28],
            "Arrests": [3, 2],
            "Charges": [8, 5],
            "Civil Proceedings": [1, 0],
            "Court Designated Worker Involvement": [2, 1],
            "School Resource Officer Involvement": [35, 20]
        })

    def create_historical_discipline_data(self):
        """Create sample historical discipline data."""
        return pd.DataFrame({
            "SCHOOL YEAR": ["20222023", "20222023"],
            "COUNTY NUMBER": ["047", "047"],
            "COUNTY NAME": ["FAYETTE", "FAYETTE"],
            "DISTRICT NUMBER": ["047", "047"],
            "DISTRICT NAME": ["Fayette County", "Fayette County"],
            "SCHOOL NUMBER": ["001", "001"],
            "SCHOOL NAME": ["Test Elementary", "Test Elementary"],
            "SCHOOL CODE": ["047001", "047001"],
            "DEMOGRAPHIC": ["All Students", "Male"],
            "TOTAL DISCIPLINE RESOLUTIONS": [180, 95],
            "EXPELLED RECEIVING SERVICES SSP1": [1, 1],
            "EXPELLED NOT RECEIVING SERVICES SSP2": [0, 0],
            "OUT OF SCHOOL SUSPENSION SSP3": [55, 30],
            "CORPORAL PUNISHMENT SSP5": [0, 0],
            "IN-SCHOOL REMOVAL INSR": [70, 38],
            "RESTRAINT SSP7": [3, 2],
            "SECLUSION SSP8": [1, 0],
            "UNILATERAL REMOVAL BY SCHOOL PERSONNEL IAES1": [1, 1],
            "REMOVAL BY HEARING OFFICER IAES2": [0, 0]
        })

    def test_end_to_end_kyrc24_discipline_only(self):
        """Test with only KYRC24 discipline resolutions data."""
        df = self.create_kyrc24_discipline_data()
        df.to_csv(self.sample_dir / "KYRC24_SAFE_Discipline_Resolutions.csv", index=False)

        cfg = {"derive": {"processing_date": "2025-07-20"}}
        transform(self.raw_dir, self.proc_dir, cfg)

        # Check output exists
        output_file = self.proc_dir / "safe_schools_discipline.csv"
        assert output_file.exists()

        # Validate output structure
        df_result = pd.read_csv(output_file)
        assert len(df_result) > 0

        # Check required columns
        required_columns = KPI_COLUMNS
        for col in required_columns:
            assert col in df_result.columns

        # Check that we have discipline metrics
        metrics = df_result['metric'].unique()
        expected_metrics = [
            'restraint_rate', 'seclusion_rate', 'expelled_not_receiving_services_rate',
            'expelled_receiving_services_rate', 'in_school_removal_rate',
            'out_of_school_suspension_rate', 'unilateral_removal_rate'
        ]
        for metric in expected_metrics:
            assert metric in metrics

        # Check source file
        assert all(df_result['source_file'] == 'KYRC24_SAFE_Discipline_Resolutions.csv')

        # Check year
        assert all(df_result['year'] == 2024)

        # Check data quality
        assert df_result['value'].min() >= 0
        rate_values = df_result[df_result['metric'].str.contains('_rate')]['value']
        assert rate_values.max() <= 100
        assert df_result['district'].notna().all()
        assert df_result['school_id'].notna().all()

    def test_end_to_end_kyrc24_legal_only(self):
        """Test with only KYRC24 legal sanctions data."""
        df = self.create_kyrc24_legal_data()
        df.to_csv(self.sample_dir / "KYRC24_SAFE_Legal_Sanctions.csv", index=False)

        cfg = {"derive": {"processing_date": "2025-07-20"}}
        transform(self.raw_dir, self.proc_dir, cfg)

        # Check output exists
        output_file = self.proc_dir / "safe_schools_discipline.csv"
        assert output_file.exists()

        # Validate output structure
        df_result = pd.read_csv(output_file)
        assert len(df_result) > 0

        # Check that we have legal metrics
        metrics = df_result['metric'].unique()
        expected_metrics = [
            'arrest_rate', 'charges_rate', 'civil_proceedings_rate',
            'court_designated_worker_rate', 'school_resource_officer_rate'
        ]
        for metric in expected_metrics:
            assert metric in metrics

        # Check source file
        assert all(df_result['source_file'] == 'KYRC24_SAFE_Legal_Sanctions.csv')

    def test_end_to_end_historical_only(self):
        """Test with only historical discipline data."""
        df = self.create_historical_discipline_data()
        df.to_csv(self.sample_dir / "safe_schools_discipline_2022.csv", index=False)

        cfg = {"derive": {"processing_date": "2025-07-20"}}
        transform(self.raw_dir, self.proc_dir, cfg)

        # Check output exists
        output_file = self.proc_dir / "safe_schools_discipline.csv"
        assert output_file.exists()

        # Validate output structure
        df_result = pd.read_csv(output_file)
        assert len(df_result) > 0

        # Check source file
        assert all(df_result['source_file'] == 'safe_schools_discipline_2022.csv')

        # Check year
        assert all(df_result['year'] == 2023)  # School year 20222023 = 2023

    def test_end_to_end_all_files(self):
        """Test with all file types combined."""
        # Create all data files
        discipline_df = self.create_kyrc24_discipline_data()
        legal_df = self.create_kyrc24_legal_data()
        historical_df = self.create_historical_discipline_data()

        discipline_df.to_csv(self.sample_dir / "KYRC24_SAFE_Discipline_Resolutions.csv", index=False)
        legal_df.to_csv(self.sample_dir / "KYRC24_SAFE_Legal_Sanctions.csv", index=False)
        historical_df.to_csv(self.sample_dir / "safe_schools_discipline_2022.csv", index=False)

        cfg = {"derive": {"processing_date": "2025-07-20", "data_quality_flag": "validated"}}
        transform(self.raw_dir, self.proc_dir, cfg)

        # Check output exists
        output_file = self.proc_dir / "safe_schools_discipline.csv"
        assert output_file.exists()

        # Validate output structure
        df_result = pd.read_csv(output_file)
        assert len(df_result) > 0

        # Check we have data from all sources
        source_files = df_result['source_file'].unique()
        expected_sources = [
            'KYRC24_SAFE_Discipline_Resolutions.csv',
            'KYRC24_SAFE_Legal_Sanctions.csv',
            'safe_schools_discipline_2022.csv'
        ]
        for source in expected_sources:
            assert source in source_files

        # Check we have data from multiple years
        years = df_result['year'].unique()
        assert 2024 in years  # From KYRC24 data
        assert 2023 in years  # From historical data (school year 20222023)

        # Check we have both discipline and legal metrics
        metrics = df_result['metric'].unique()
        discipline_metrics = ['restraint_rate', 'out_of_school_suspension_rate', 'in_school_removal_rate']
        legal_metrics = ['arrest_rate', 'charges_rate', 'school_resource_officer_rate']
        
        for metric in discipline_metrics:
            assert metric in metrics
        for metric in legal_metrics:
            assert metric in metrics

        # Check demographic mapping worked
        student_groups = df_result['student_group'].unique()
        assert 'All Students' in student_groups
        assert 'Male' in student_groups

        # Validate data quality
        assert df_result['value'].min() >= 0
        rate_values = df_result[df_result['metric'].str.contains('_rate')]['value']
        assert rate_values.max() <= 100
        assert df_result['district'].notna().all()
        assert df_result['school_id'].notna().all()
        assert df_result['metric'].notna().all()
        assert df_result['value'].notna().all()

        # Check that standard KPI structure is maintained
        assert len(df_result.columns) == 19  # Standard KPI format has 19 columns

    def test_end_to_end_multiple_historical_years(self):
        """Test with multiple historical years."""
        historical_2021 = self.create_historical_discipline_data()
        historical_2021['SCHOOL YEAR'] = '20202021'
        
        historical_2022 = self.create_historical_discipline_data()
        historical_2022['SCHOOL YEAR'] = '20212022'

        historical_2021.to_csv(self.sample_dir / "safe_schools_discipline_2021.csv", index=False)
        historical_2022.to_csv(self.sample_dir / "safe_schools_discipline_2022.csv", index=False)

        cfg = {"derive": {"processing_date": "2025-07-20"}}
        transform(self.raw_dir, self.proc_dir, cfg)

        # Check output exists
        output_file = self.proc_dir / "safe_schools_discipline.csv"
        assert output_file.exists()

        # Validate output structure
        df_result = pd.read_csv(output_file)
        assert len(df_result) > 0

        # Check we have data from both years
        years = df_result['year'].unique()
        assert 2021 in years  # School year 20202021
        assert 2022 in years  # School year 20212022

        # Check we have data from both files
        source_files = df_result['source_file'].unique()
        assert 'safe_schools_discipline_2021.csv' in source_files
        assert 'safe_schools_discipline_2022.csv' in source_files

    def test_end_to_end_no_files(self):
        """Test behavior when no files are present."""
        cfg = {"derive": {"processing_date": "2025-07-20"}}
        transform(self.raw_dir, self.proc_dir, cfg)

        # Should not create output file when no input files
        output_file = self.proc_dir / "safe_schools_discipline.csv"
        assert not output_file.exists()

    def test_end_to_end_empty_file(self):
        """Test behavior with empty input file."""
        # Create empty CSV file
        empty_df = pd.DataFrame()
        empty_df.to_csv(self.sample_dir / "KYRC24_SAFE_Discipline_Resolutions.csv", index=False)

        cfg = {"derive": {"processing_date": "2025-07-20"}}
        transform(self.raw_dir, self.proc_dir, cfg)

        # Should create output file but with no data rows
        output_file = self.proc_dir / "safe_schools_discipline.csv"
        if output_file.exists():
            df_result = pd.read_csv(output_file)
            assert len(df_result) == 0