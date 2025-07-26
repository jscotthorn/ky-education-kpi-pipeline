"""
Tests for Safe Schools Discipline ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
from etl.constants import KPI_COLUMNS
import tempfile
import shutil
from etl.safe_schools_discipline import transform, SafeSchoolsDisciplineETL


class TestSafeSchoolsDisciplineETL:
    
    def setup_method(self):
        """Setup test directories and sample data."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        
        # Create sample raw data directory
        self.sample_dir = self.raw_dir / "safe_schools_discipline"
        self.sample_dir.mkdir(parents=True)
    
    def teardown_method(self):
        """Clean up test directories."""
        shutil.rmtree(self.test_dir)
    
    def create_sample_kyrc24_discipline_data(self):
        """Create sample KYRC24 discipline resolutions data."""
        data = pd.DataFrame({
            'School Year': ['20232024', '20232024', '20232024'],
            'County Number': ['001', '001', '001'],
            'County Name': ['Adair', 'Adair', 'Adair'],
            'District Number': ['001', '001', '001'],
            'District Name': ['Adair County', 'Adair County', 'Adair County'],
            'School Number': ['001', '001', '001'],
            'School Name': ['Test Elementary', 'Test Elementary', 'Test Elementary'],
            'School Code': ['001001', '001001', '001001'],
            'State School Id': ['12345', '12345', '12345'],
            'NCES ID': ['210001000001', '210001000001', '210001000001'],
            'Demographic': ['All Students', 'Female', 'Male'],
            'Total': [100, 45, 55],
            'Corporal Punishment (SSP5)': [0, 0, 0],
            'Restraint (SSP7)': [2, 1, 1],
            'Seclusion (SSP8)': [1, 0, 1],
            'Expelled, Not Receiving Services (SSP2)': [0, 0, 0],
            'Expelled, Receiving Services (SSP1)': [1, 0, 1],
            'In-School Removal (INSR) Or In-District Removal (INDR) >=.5': [45, 20, 25],
            'Out-Of-School Suspensions (SSP3)': [30, 12, 18],
            'Removal By Hearing Officer (IAES2)': [0, 0, 0],
            'Unilateral Removal By School Personnel (IAES1)': [1, 0, 1]
        })
        return data
        
    def create_sample_kyrc24_legal_data(self):
        """Create sample KYRC24 legal sanctions data."""
        data = pd.DataFrame({
            'School Year': ['20232024', '20232024', '20232024'],
            'County Number': ['001', '001', '001'],
            'County Name': ['Adair', 'Adair', 'Adair'],
            'District Number': ['001', '001', '001'],
            'District Name': ['Adair County', 'Adair County', 'Adair County'],
            'School Number': ['001', '001', '001'],
            'School Name': ['Test Elementary', 'Test Elementary', 'Test Elementary'],
            'School Code': ['001001', '001001', '001001'],
            'Demographic': ['All Students', 'Female', 'Male'],
            'Total': [20, 8, 12],
            'Arrests': [2, 1, 1],
            'Charges': [5, 2, 3],
            'Civil Proceedings': [0, 0, 0],
            'Court Designated Worker Involvement': [1, 0, 1],
            'School Resource Officer Involvement': [15, 6, 9]
        })
        return data
        
    def create_sample_historical_data(self):
        """Create sample historical discipline data."""
        data = pd.DataFrame({
            'SCHOOL YEAR': ['20222023', '20222023', '20222023'],
            'COUNTY NUMBER': ['001', '001', '001'],
            'COUNTY NAME': ['ADAIR', 'ADAIR', 'ADAIR'],
            'DISTRICT NUMBER': ['001', '001', '001'],
            'DISTRICT NAME': ['Adair County', 'Adair County', 'Adair County'],
            'SCHOOL NUMBER': ['001', '001', '001'],
            'SCHOOL NAME': ['Test Elementary', 'Test Elementary', 'Test Elementary'],
            'SCHOOL CODE': ['001001', '001001', '001001'],
            'DEMOGRAPHIC': ['All Students', 'Female', 'Male'],
            'TOTAL DISCIPLINE RESOLUTIONS': [85, 38, 47],
            'EXPELLED RECEIVING SERVICES SSP1': [1, 0, 1],
            'EXPELLED NOT RECEIVING SERVICES SSP2': [0, 0, 0],
            'OUT OF SCHOOL SUSPENSION SSP3': [25, 10, 15],
            'CORPORAL PUNISHMENT SSP5': [0, 0, 0],
            'IN-SCHOOL REMOVAL INSR': [40, 18, 22],
            'RESTRAINT SSP7': [1, 1, 0],
            'SECLUSION SSP8': [0, 0, 0],
            'UNILATERAL REMOVAL BY SCHOOL PERSONNEL IAES1': [1, 0, 1],
            'REMOVAL BY HEARING OFFICER IAES2': [0, 0, 0]
        })
        return data
    
    def test_etl_initialization(self):
        """Test ETL class initialization."""
        etl = SafeSchoolsDisciplineETL()
        assert etl.source_name == "safe_schools_discipline"
        assert hasattr(etl, 'module_column_mappings')
        assert hasattr(etl, 'extract_metrics')
        assert hasattr(etl, 'get_suppressed_metric_defaults')
    
    def test_module_column_mappings(self):
        """Test module column mappings."""
        etl = SafeSchoolsDisciplineETL()
        mappings = etl.module_column_mappings
        
        # Test KYRC24 mappings
        assert 'Corporal Punishment (SSP5)' in mappings
        assert mappings['Corporal Punishment (SSP5)'] == 'corporal_punishment_count'
        assert 'Arrests' in mappings
        assert mappings['Arrests'] == 'arrest_count'
        
        # Test historical mappings
        assert 'EXPELLED RECEIVING SERVICES SSP1' in mappings
        assert mappings['EXPELLED RECEIVING SERVICES SSP1'] == 'expelled_receiving_services_count'
    
    def test_extract_metrics_kyrc24_discipline(self):
        """Test metric extraction from KYRC24 discipline data."""
        etl = SafeSchoolsDisciplineETL()
        
        # Test row with discipline counts
        row = pd.Series({
            'total': 100,
            'restraint_count': 2,
            'out_of_school_suspension_count': 30,
            'in_school_removal_count': 45,
            'expelled_receiving_services_count': 1,
            'unilateral_removal_count': 1
        })
        
        metrics = etl.extract_metrics(row)
        
        assert 'restraint_rate' in metrics
        assert metrics['restraint_rate'] == 2.0
        assert 'out_of_school_suspension_rate' in metrics
        assert metrics['out_of_school_suspension_rate'] == 30.0
        assert 'in_school_removal_rate' in metrics
        assert metrics['in_school_removal_rate'] == 45.0
        assert 'expelled_receiving_services_rate' in metrics
        assert metrics['expelled_receiving_services_rate'] == 1.0
        assert 'unilateral_removal_rate' in metrics
        assert metrics['unilateral_removal_rate'] == 1.0
        
    def test_extract_metrics_kyrc24_legal(self):
        """Test metric extraction from KYRC24 legal sanctions data."""
        etl = SafeSchoolsDisciplineETL()
        
        # Test row with legal sanctions counts
        row = pd.Series({
            'total': 20,
            'arrest_count': 2,
            'charges_count': 5,
            'court_designated_worker_count': 1,
            'school_resource_officer_count': 15
        })
        
        metrics = etl.extract_metrics(row)
        
        assert 'arrest_rate' in metrics
        assert metrics['arrest_rate'] == 10.0
        assert 'charges_rate' in metrics
        assert metrics['charges_rate'] == 25.0
        assert 'court_designated_worker_rate' in metrics
        assert metrics['court_designated_worker_rate'] == 5.0
        assert 'school_resource_officer_rate' in metrics
        assert metrics['school_resource_officer_rate'] == 75.0
    
    def test_extract_metrics_historical(self):
        """Test metric extraction from historical data."""
        etl = SafeSchoolsDisciplineETL()
        
        # Test row with historical discipline counts
        row = pd.Series({
            'total_discipline_resolutions': 85,
            'expelled_receiving_services_count': 1,
            'out_of_school_suspension_count': 25,
            'in_school_removal_count': 40,
            'restraint_count': 1,
            'unilateral_removal_count': 1
        })
        
        metrics = etl.extract_metrics(row)
        
        assert 'expelled_receiving_services_rate' in metrics
        assert metrics['expelled_receiving_services_rate'] == round(1/85*100, 2)
        assert 'out_of_school_suspension_rate' in metrics
        assert metrics['out_of_school_suspension_rate'] == round(25/85*100, 2)
        assert 'in_school_removal_rate' in metrics
        assert metrics['in_school_removal_rate'] == round(40/85*100, 2)
        assert 'restraint_rate' in metrics
        assert metrics['restraint_rate'] == round(1/85*100, 2)
        assert 'unilateral_removal_rate' in metrics
        assert metrics['unilateral_removal_rate'] == round(1/85*100, 2)
    
    def test_extract_metrics_zero_total(self):
        """Test metric extraction with zero total."""
        etl = SafeSchoolsDisciplineETL()
        
        # Test row with zero total
        row = pd.Series({
            'total': 0,
            'restraint_count': 1,
            'out_of_school_suspension_count': 2
        })
        
        metrics = etl.extract_metrics(row)
        assert len(metrics) == 0  # Should return no metrics when total is 0
    
    def test_extract_metrics_zero_counts(self):
        """Test metric extraction with zero counts."""
        etl = SafeSchoolsDisciplineETL()
        
        # Test row with zero counts (should not include zero rates)
        row = pd.Series({
            'total': 100,
            'restraint_count': 0,
            'out_of_school_suspension_count': 10,
            'in_school_removal_count': 0
        })
        
        metrics = etl.extract_metrics(row)
        
        # Only non-zero rates should be included
        assert 'restraint_rate' not in metrics
        assert 'in_school_removal_rate' not in metrics
        assert 'out_of_school_suspension_rate' in metrics
        assert metrics['out_of_school_suspension_rate'] == 10.0
    
    def test_get_suppressed_metric_defaults(self):
        """Test suppressed metric defaults."""
        etl = SafeSchoolsDisciplineETL()
        row = pd.Series({'total': 100})
        defaults = etl.get_suppressed_metric_defaults(row)
        
        # Should include all possible metrics with NA values
        expected_metrics = [
            'corporal_punishment_rate', 'restraint_rate', 'seclusion_rate',
            'expelled_not_receiving_services_rate', 'expelled_receiving_services_rate',
            'in_school_removal_rate', 'out_of_school_suspension_rate',
            'removal_by_hearing_officer_rate', 'unilateral_removal_rate',
            'arrest_rate', 'charges_rate', 'civil_proceedings_rate',
            'court_designated_worker_rate', 'school_resource_officer_rate'
        ]
        
        for metric in expected_metrics:
            assert metric in defaults
            assert pd.isna(defaults[metric])
    
    def test_transform_integration(self):
        """Test the complete transform integration."""
        # Create sample data files
        kyrc24_discipline = self.create_sample_kyrc24_discipline_data()
        kyrc24_legal = self.create_sample_kyrc24_legal_data()
        historical = self.create_sample_historical_data()
        
        # Save sample data to test directories
        kyrc24_discipline.to_csv(self.sample_dir / "KYRC24_SAFE_Discipline_Resolutions.csv", index=False)
        kyrc24_legal.to_csv(self.sample_dir / "KYRC24_SAFE_Legal_Sanctions.csv", index=False)
        historical.to_csv(self.sample_dir / "safe_schools_discipline_2022.csv", index=False)
        
        # Run transform
        test_config = {
            "derive": {
                "processing_date": "2025-07-20",
                "data_quality_flag": "test"
            }
        }
        
        transform(self.raw_dir, self.proc_dir, test_config)
        
        # Check output file exists
        output_file = self.proc_dir / "safe_schools_discipline.csv"
        assert output_file.exists()
        
        # Load and validate output
        result_df = pd.read_csv(output_file)
        
        # Check required columns
        required_columns = KPI_COLUMNS
        for col in required_columns:
            assert col in result_df.columns
        
        # Check we have data from all sources
        source_files = result_df['source_file'].unique()
        assert 'KYRC24_SAFE_Discipline_Resolutions.csv' in source_files
        assert 'KYRC24_SAFE_Legal_Sanctions.csv' in source_files
        assert 'safe_schools_discipline_2022.csv' in source_files
        
        # Check metric types
        metrics = result_df['metric'].unique()
        assert 'restraint_rate' in metrics
        assert 'out_of_school_suspension_rate' in metrics
        assert 'arrest_rate' in metrics
        assert 'school_resource_officer_rate' in metrics
        
        # Check years
        years = result_df['year'].unique()
        assert 2024 in years  # From KYRC24 data
        assert 2023 in years  # From historical data (school year 20222023)
        
        # Check value ranges
        assert result_df['value'].min() >= 0
        assert result_df['value'].max() <= 100
        
        # Check no null values in required fields
        assert result_df['district'].notna().all()
        assert result_df['school_id'].notna().all()
        assert result_df['metric'].notna().all()
        assert result_df['value'].notna().all()


class TestSafeSchoolsDisciplineHelpers:
    """Test helper functions if any are created."""
    
    def test_safe_schools_discipline_transform_function(self):
        """Test the transform function can be imported and called."""
        # This is a basic smoke test
        assert callable(transform)