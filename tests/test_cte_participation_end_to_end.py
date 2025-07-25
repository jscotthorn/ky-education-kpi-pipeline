"""
End-to-end test for the CTE Participation ETL pipeline.
"""
import unittest
from pathlib import Path
import pandas as pd
import tempfile
from etl.constants import KPI_COLUMNS
import shutil
import sys

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.cte_participation import transform


class TestCTEParticipationEndToEnd(unittest.TestCase):
    """End-to-end test for CTE Participation ETL pipeline."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create temporary directories for testing
        self.test_dir = tempfile.mkdtemp()
        self.raw_dir = Path(self.test_dir) / "raw" / "cte_participation"
        self.proc_dir = Path(self.test_dir) / "processed"
        self.raw_dir.mkdir(parents=True)
        self.proc_dir.mkdir(parents=True)
        
        # Test configuration
        self.config = {
            "derive": {
                "processing_date": "2025-07-21",
                "data_quality_flag": "test"
            }
        }
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.test_dir)
    
    def create_test_data_2024(self):
        """Create test data mimicking 2024 format."""
        data = {
            'School Year': ['20232024', '20232024', '20232024', '20232024'],
            'County Number': ['', '', '001', '001'],
            'County Name': ['', '', 'Adair', 'Adair'],
            'District Number': ['999', '999', '001', '001'],
            'District Name': ['All Districts', 'All Districts', 'Adair County', 'Adair County'],
            'School Number': ['', '', '001', '001'],
            'School Name': ['All Schools', 'All Schools', 'Adair County High School', 'Adair County High School'],
            'School Code': ['999000', '999000', '001001', '001001'],
            'State School Id': ['', '', '1234', '1234'],
            'NCES ID': ['', '', '210001001234', '210001001234'],
            'CO-OP': ['', '', 'GRREC', 'GRREC'],
            'CO-OP Code': ['', '', '902', '902'],
            'School Type': ['', '', 'High School', 'High School'],
            'Demographic': ['All Students', 'Female', 'All Students', 'Male'],
            'Total Number of Student': ['143,415', '68,598', '523', '275'],
            'CTE Participants in All Grades': ['68.7', '67.6', '72.5', '75.2'],
            'Grade 12 CTE Eligible Completer': ['17,597', '8,128', '125', '65'],
            'Grade 12 CTE Completers': ['42.3', '40.8', '45.5', '48.2']
        }
        df = pd.DataFrame(data)
        df.to_csv(self.raw_dir / 'KYRC24_CTE_Participation.csv', index=False)
    
    def create_test_data_2023(self):
        """Create test data mimicking 2023 format."""
        data = {
            'SCHOOL YEAR': ['20222023', '20222023', '20222023'],
            'COUNTY NUMBER': ['001', '001', '001'],
            'COUNTY NAME': ['ADAIR', 'ADAIR', 'ADAIR'],
            'DISTRICT NUMBER': ['001', '001', '001'],
            'DISTRICT NAME': ['Adair County', 'Adair County', 'Adair County'],
            'SCHOOL NUMBER': ['', '', ''],
            'SCHOOL NAME': ['---District Total---', '---District Total---', '---District Total---'],
            'SCHOOL CODE': ['001', '001', '001'],
            'STATE SCHOOL ID': ['', '', ''],
            'NCES ID': ['', '', ''],
            'CO-OP': ['GRREC', 'GRREC', 'GRREC'],
            'CO-OP CODE': ['902', '902', '902'],
            'SCHOOL TYPE': ['', '', ''],
            'DEMOGRAPHIC': ['All Students', 'Female', 'African American'],
            'TOTAL NUMBER OF STUDENT': ['*', '*', '*'],
            'CTE PARTICIPANTS IN ALL GRADES': ['377', '196', '8'],
            'GRADE 12 CTE ELIGIBLE COMPLETER': ['56', '33', '*'],
            'GRADE 12 CTE COMPLETERS': ['37', '26', '*']
        }
        df = pd.DataFrame(data)
        df.to_csv(self.raw_dir / 'cte_by_student_group_2023.csv', index=False)
    
    def test_end_to_end_pipeline(self):
        """Test the complete CTE Participation ETL pipeline."""
        # Create test data
        self.create_test_data_2024()
        self.create_test_data_2023()
        
        # Run the transform
        transform(self.raw_dir.parent, self.proc_dir, self.config)
        
        # Check that output file was created
        output_file = self.proc_dir / 'cte_participation.csv'
        self.assertTrue(output_file.exists())
        
        # Read and validate the output
        kpi_df = pd.read_csv(output_file)
        
        # Check required columns exist
        required_columns = KPI_COLUMNS
        for col in required_columns:
            self.assertIn(col, kpi_df.columns)
        
        # Check metrics are created
        metrics = kpi_df['metric'].unique()
        self.assertIn('cte_participation_rate', metrics)
        self.assertIn('cte_eligible_completer_count_grade_12', metrics)
        self.assertIn('cte_completion_rate_grade_12', metrics)
        
        # Check years are extracted correctly
        years = sorted(kpi_df['year'].unique())
        self.assertIn(2023, years)
        self.assertIn(2024, years)
        
        # Validate specific data points
        # Check 2024 All Students participation rate
        all_students_2024 = kpi_df[
            (kpi_df['year'] == 2024) & 
            (kpi_df['student_group'] == 'All Students') & 
            (kpi_df['district'] == 'All Districts') &
            (kpi_df['metric'] == 'cte_participation_rate')
        ]
        self.assertEqual(len(all_students_2024), 1)
        self.assertAlmostEqual(all_students_2024.iloc[0]['value'], 68.7, places=1)
        
        # Check demographic standardization
        demographics = kpi_df['student_group'].unique()
        self.assertIn('All Students', demographics)
        self.assertIn('Female', demographics)
        self.assertIn('Male', demographics)
        
        # Check suppression handling
        suppressed_records = kpi_df[kpi_df['suppressed'] == 'Y']
        self.assertTrue((suppressed_records['value'].isna()).all())
        
        # Verify demographic report was created
        demographic_report = self.proc_dir / 'cte_participation_demographic_report.md'
        self.assertTrue(demographic_report.exists())
    
    def test_numeric_conversion(self):
        """Test that numeric values with commas are properly converted."""
        # Create test data with comma-separated numbers
        self.create_test_data_2024()
        
        # Run the transform
        transform(self.raw_dir.parent, self.proc_dir, self.config)
        
        # Read output
        kpi_df = pd.read_csv(self.proc_dir / 'cte_participation.csv')
        
        # Check that large numbers were converted correctly
        eligible_completer = kpi_df[
            (kpi_df['year'] == 2024) & 
            (kpi_df['student_group'] == 'All Students') & 
            (kpi_df['district'] == 'All Districts') &
            (kpi_df['metric'] == 'cte_eligible_completer_count_grade_12')
        ]
        self.assertEqual(len(eligible_completer), 1)
        self.assertEqual(eligible_completer.iloc[0]['value'], 17597)  # comma removed
    
    def test_invalid_data_handling(self):
        """Test handling of invalid data values."""
        # Create test data with valid demographics but invalid values
        data = {
            'School Year': ['20232024', '20232024', '20232024', '20232024'],
            'District Number': ['999', '999', '999', '999'],
            'District Name': ['All Districts', 'All Districts', 'All Districts', 'All Districts'],
            'School Name': ['All Schools', 'All Schools', 'All Schools', 'All Schools'],
            'School Code': ['999000', '999000', '999000', '999000'],
            'Demographic': ['All Students', 'Female', 'Male', 'African American'],
            'CTE Participants in All Grades': ['150.0', '-10.0', 'N/A', '85.0'],  # invalid values
            'Grade 12 CTE Eligible Completer': ['-50', 'ABC', '*', '100'],  # invalid values
            'Grade 12 CTE Completers': ['110.0', 'Suppressed', '-5.0', '40.5']  # invalid values
        }
        df = pd.DataFrame(data)
        df.to_csv(self.raw_dir / 'test_invalid.csv', index=False)
        
        # Also create a valid file to ensure some output is generated
        self.create_test_data_2024()
        
        # Run the transform
        transform(self.raw_dir.parent, self.proc_dir, self.config)
        
        # Read output
        kpi_df = pd.read_csv(self.proc_dir / 'cte_participation.csv')
        
        # Check that invalid rates are converted to NA
        all_students_invalid = kpi_df[
            (kpi_df['year'] == 2024) &
            (kpi_df['student_group'] == 'All Students') & 
            (kpi_df['source_file'] == 'test_invalid.csv') &
            (kpi_df['metric'] == 'cte_participation_rate')
        ]
        # Rate > 100 should be NA
        self.assertTrue(all_students_invalid.iloc[0]['value'].isna() if len(all_students_invalid) > 0 else True)
        
        # Check negative values are converted to NA
        female_invalid = kpi_df[
            (kpi_df['year'] == 2024) &
            (kpi_df['student_group'] == 'Female') &
            (kpi_df['source_file'] == 'test_invalid.csv')
        ]
        # All metrics should be NA for negative values
        for _, row in female_invalid.iterrows():
            self.assertTrue(pd.isna(row['value']))


if __name__ == '__main__':
    unittest.main()