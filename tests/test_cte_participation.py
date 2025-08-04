"""
Unit tests for the CTE Participation ETL module.
"""
import unittest
from pathlib import Path
import pandas as pd
import numpy as np
import sys

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.cte_participation import CTEParticipationETL, clean_cte_data


class TestCTEParticipationETL(unittest.TestCase):
    """Test CTE Participation ETL functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.etl = CTEParticipationETL('cte_participation')
    
    def test_module_column_mappings(self):
        """Test that column mappings are correctly defined."""
        mappings = self.etl.module_column_mappings
        
        # Check that essential mappings exist
        self.assertIn('CTE Participants in All Grades', mappings)
        self.assertIn('Grade 12 CTE Eligible Completer', mappings)
        self.assertIn('Grade 12 CTE Completers', mappings)
        
        # Check mapping values follow naming convention
        self.assertEqual(mappings['CTE Participants in All Grades'], 'cte_participation_rate')
        self.assertEqual(mappings['Grade 12 CTE Eligible Completer'], 'cte_eligible_completer_count')
        self.assertEqual(mappings['Grade 12 CTE Completers'], 'cte_completion_rate')
    
    def test_extract_metrics_with_all_data(self):
        """Test metric extraction with complete data."""
        row = pd.Series({
            'cte_participation_rate': 75.5,
            'cte_eligible_completer_count': 125,
            'cte_completion_rate': 45.2
        })
        
        metrics = self.etl.extract_metrics(row)
        
        self.assertEqual(metrics['cte_participation_rate'], 75.5)
        self.assertEqual(metrics['cte_eligible_completer_count_grade_12'], 125)
        self.assertEqual(metrics['cte_completion_rate_grade_12'], 45.2)
    
    def test_extract_metrics_with_missing_data(self):
        """Test metric extraction with missing data."""
        row = pd.Series({
            'cte_participation_rate': 75.5,
            'cte_eligible_completer_count': pd.NA,
            'cte_completion_rate': pd.NA
        })
        
        metrics = self.etl.extract_metrics(row)
        
        # Should only include non-NA metrics
        self.assertEqual(metrics['cte_participation_rate'], 75.5)
        self.assertNotIn('cte_eligible_completer_count_grade_12', metrics)
        self.assertNotIn('cte_completion_rate_grade_12', metrics)
    
    def test_extract_metrics_with_all_na(self):
        """Test metric extraction when all values are NA."""
        row = pd.Series({
            'cte_participation_rate': pd.NA,
            'cte_eligible_completer_count': pd.NA,
            'cte_completion_rate': pd.NA
        })
        
        metrics = self.etl.extract_metrics(row)
        
        # Should return empty dict for all NA values
        self.assertEqual(metrics, {})
    
    def test_get_suppressed_metric_defaults(self):
        """Test default values for suppressed metrics."""
        row = pd.Series({})
        defaults = self.etl.get_suppressed_metric_defaults(row)
        
        self.assertTrue(pd.isna(defaults['cte_participation_rate']))
        self.assertTrue(pd.isna(defaults['cte_eligible_completer_count_grade_12']))
        self.assertTrue(pd.isna(defaults['cte_completion_rate_grade_12']))
    
    def test_clean_cte_data_valid_rates(self):
        """Test cleaning of valid CTE rate data."""
        df = pd.DataFrame({
            'cte_participation_rate': [75.5, 80.2, 0.0, 100.0],
            'cte_completion_rate': [45.2, 50.5, 0.0, 100.0],
            'cte_eligible_completer_count': [100, 200, 0, 500]
        })
        
        cleaned_df = clean_cte_data(df)
        
        # All values should remain unchanged
        pd.testing.assert_frame_equal(df, cleaned_df)
    
    def test_clean_cte_data_invalid_rates(self):
        """Test cleaning of invalid CTE rate data."""
        df = pd.DataFrame({
            'cte_participation_rate': [75.5, -10.0, 150.0, pd.NA],
            'cte_completion_rate': [45.2, -5.0, 120.0, pd.NA],
            'cte_eligible_completer_count': [100, -50, 200, pd.NA]
        })
        
        cleaned_df = clean_cte_data(df)
        
        # Check that invalid values are converted to NA
        self.assertEqual(cleaned_df['cte_participation_rate'][0], 75.5)
        self.assertTrue(pd.isna(cleaned_df['cte_participation_rate'][1]))  # negative
        self.assertTrue(pd.isna(cleaned_df['cte_participation_rate'][2]))  # > 100
        self.assertTrue(pd.isna(cleaned_df['cte_participation_rate'][3]))  # already NA
        
        # Check count cleaning
        self.assertEqual(cleaned_df['cte_eligible_completer_count'][0], 100)
        self.assertTrue(pd.isna(cleaned_df['cte_eligible_completer_count'][1]))  # negative
        self.assertEqual(cleaned_df['cte_eligible_completer_count'][2], 200)  # valid
    
    def test_clean_cte_data_string_values(self):
        """Test cleaning of string values that should be numeric."""
        df = pd.DataFrame({
            'cte_participation_rate': ['75.5', 'N/A', '*', '80.2'],
            'cte_completion_rate': ['45.2', 'Suppressed', '-', '50.5'],
            'cte_eligible_completer_count': ['100', 'N/A', '*', '200']
        })
        
        cleaned_df = clean_cte_data(df)
        
        # Check that valid strings are converted to numbers
        self.assertEqual(cleaned_df['cte_participation_rate'][0], 75.5)
        self.assertEqual(cleaned_df['cte_participation_rate'][3], 80.2)
        
        # Check that invalid strings are converted to NA
        self.assertTrue(pd.isna(cleaned_df['cte_participation_rate'][1]))
        self.assertTrue(pd.isna(cleaned_df['cte_participation_rate'][2]))
    
    def test_extract_metrics_naming_convention(self):
        """Test that extracted metrics follow naming conventions."""
        row = pd.Series({
            'cte_participation_rate': 75.5,
            'cte_eligible_completer_count': 125,
            'cte_completion_rate': 45.2
        })
        
        metrics = self.etl.extract_metrics(row)
        
        # Check that metric names follow conventions
        for metric_name in metrics.keys():
            # Rates should end with _rate
            if 'rate' in metric_name:
                self.assertTrue(metric_name.endswith('_rate') or metric_name.endswith('_rate_grade_12'))
            
            # Counts should contain _count_
            if 'count' in metric_name:
                self.assertIn('_count_', metric_name)


if __name__ == '__main__':
    unittest.main()