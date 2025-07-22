"""Unit tests for Safe Schools Climate ETL module."""
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import sys

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.safe_schools_climate import (
    SafeSchoolsClimateETL,
    calculate_policy_compliance_rate,
    clean_index_scores
)


class TestCalculatePolicyComplianceRate:
    """Test policy compliance rate calculation."""
    
    def test_all_yes_responses(self):
        """Test with all Yes responses."""
        row = pd.Series({
            'visitors_sign_in': 'Yes',
            'classroom_doors_lock': 'Yes',
            'classroom_phones': 'Yes'
        })
        policy_columns = ['visitors_sign_in', 'classroom_doors_lock', 'classroom_phones']
        
        result = calculate_policy_compliance_rate(row, policy_columns)
        assert result == 100.0
    
    def test_mixed_responses(self):
        """Test with mixed Yes/No responses."""
        row = pd.Series({
            'visitors_sign_in': 'Yes',
            'classroom_doors_lock': 'No',
            'classroom_phones': 'Yes',
            'annual_climate_survey': 'No'
        })
        policy_columns = ['visitors_sign_in', 'classroom_doors_lock', 
                         'classroom_phones', 'annual_climate_survey']
        
        result = calculate_policy_compliance_rate(row, policy_columns)
        assert result == 50.0
    
    def test_case_insensitive(self):
        """Test that YES, yes, Yes are all handled."""
        row = pd.Series({
            'visitors_sign_in': 'YES',
            'classroom_doors_lock': 'yes',
            'classroom_phones': 'Yes'
        })
        policy_columns = ['visitors_sign_in', 'classroom_doors_lock', 'classroom_phones']
        
        result = calculate_policy_compliance_rate(row, policy_columns)
        assert result == 100.0
    
    def test_missing_values(self):
        """Test with missing values."""
        row = pd.Series({
            'visitors_sign_in': 'Yes',
            'classroom_doors_lock': pd.NA,
            'classroom_phones': 'No'
        })
        policy_columns = ['visitors_sign_in', 'classroom_doors_lock', 'classroom_phones']
        
        result = calculate_policy_compliance_rate(row, policy_columns)
        assert result == 50.0  # Only counts non-NA values
    
    def test_all_missing(self):
        """Test with all missing values."""
        row = pd.Series({
            'visitors_sign_in': pd.NA,
            'classroom_doors_lock': pd.NA
        })
        policy_columns = ['visitors_sign_in', 'classroom_doors_lock']
        
        result = calculate_policy_compliance_rate(row, policy_columns)
        assert pd.isna(result)


class TestCleanIndexScores:
    """Test index score cleaning."""
    
    def test_valid_scores(self):
        """Test with valid scores."""
        df = pd.DataFrame({
            'climate_index': [75.5, 80.2, 65.0],
            'safety_index': [70.0, 85.5, 60.0]
        })
        
        result = clean_index_scores(df)
        assert result['climate_index'].tolist() == [75.5, 80.2, 65.0]
        assert result['safety_index'].tolist() == [70.0, 85.5, 60.0]
    
    def test_invalid_scores(self):
        """Test with scores outside 0-100 range."""
        df = pd.DataFrame({
            'climate_index': [75.5, -10, 105.0],
            'safety_index': [70.0, 85.5, 150.0]
        })
        
        result = clean_index_scores(df)
        assert result['climate_index'].tolist()[0] == 75.5
        assert pd.isna(result['climate_index'].tolist()[1])
        assert pd.isna(result['climate_index'].tolist()[2])
        assert result['safety_index'].tolist()[0] == 70.0
        assert pd.isna(result['safety_index'].tolist()[2])
    
    def test_non_numeric_values(self):
        """Test with non-numeric values."""
        df = pd.DataFrame({
            'climate_index': ['75.5', 'N/A', '80'],
            'safety_index': ['70', '', '85.5']
        })
        
        result = clean_index_scores(df)
        assert result['climate_index'].tolist()[0] == 75.5
        assert pd.isna(result['climate_index'].tolist()[1])
        assert result['climate_index'].tolist()[2] == 80.0


class TestSafeSchoolsClimateETL:
    """Test SafeSchoolsClimateETL class."""
    
    @pytest.fixture
    def etl(self):
        """Create ETL instance."""
        return SafeSchoolsClimateETL('safe_schools_climate')
    
    def test_column_mappings(self, etl):
        """Test column mappings are defined."""
        mappings = etl.module_column_mappings

        assert 'Are visitors to the building required to sign-in?' in mappings
        assert mappings['Are visitors to the building required to sign-in?'] == 'visitors_sign_in'
        assert 'Visitors required to sign-in' in mappings
        assert mappings['Visitors required to sign-in'] == 'visitors_sign_in'
        assert 'CLIMATE INDEX' in mappings
        assert mappings['CLIMATE INDEX'] == 'climate_index'
    
    def test_extract_metrics_precautionary(self, etl):
        """Test metric extraction for precautionary measures."""
        row = pd.Series({
            'visitors_sign_in': 'Yes',
            'classroom_doors_lock': 'No',
            'classroom_phones': 'Yes',
            'annual_climate_survey': 'Yes'
        })
        
        metrics = etl.extract_metrics(row)
        assert 'safety_policy_compliance_rate' in metrics
        assert metrics['safety_policy_compliance_rate'] == 75.0

    def test_extract_metrics_precautionary_synonyms(self, etl):
        """Ensure synonyms from older files are recognized."""
        df = pd.DataFrame([
            {
                'Visitors required to sign-in': 'Yes',
                'All classroom doors lock from the inside.': 'Yes',
                'All classrooms have access to telephone': 'No',
                'Student survey data collected and used': 'Yes'
            }
        ])

        norm_df = etl.normalize_column_names(df)
        metrics = etl.extract_metrics(norm_df.iloc[0])
        assert 'safety_policy_compliance_rate' in metrics
        assert metrics['safety_policy_compliance_rate'] == 75.0
    
    def test_extract_metrics_index_scores(self, etl):
        """Test metric extraction for index scores."""
        row = pd.Series({
            'climate_index': 75.5,
            'safety_index': 80.0
        })
        
        metrics = etl.extract_metrics(row)
        assert 'climate_index_score' in metrics
        assert 'safety_index_score' in metrics
        assert metrics['climate_index_score'] == 75.5
        assert metrics['safety_index_score'] == 80.0
    
    def test_extract_metrics_missing_values(self, etl):
        """Test metric extraction with missing values."""
        row = pd.Series({
            'climate_index': pd.NA,
            'safety_index': 80.0
        })
        
        metrics = etl.extract_metrics(row)
        # When climate_index is NA, it's not included in metrics
        assert 'climate_index_score' not in metrics
        assert metrics['safety_index_score'] == 80.0
    
    def test_get_suppressed_metric_defaults(self, etl):
        """Test suppressed metric defaults based on data type."""
        # Test with index score data
        row_index = pd.Series({'climate_index': 75.0, 'safety_index': 70.0})
        defaults = etl.get_suppressed_metric_defaults(row_index)
        assert 'climate_index_score' in defaults
        assert 'safety_index_score' in defaults
        assert 'safety_policy_compliance_rate' not in defaults
        
        # Test with policy data
        row_policy = pd.Series({'visitors_sign_in': 'Yes', 'classroom_doors_lock': 'No'})
        defaults = etl.get_suppressed_metric_defaults(row_policy)
        assert 'climate_index_score' not in defaults
        assert 'safety_index_score' not in defaults
        assert 'safety_policy_compliance_rate' in defaults
        
        # Test with no relevant data
        row_empty = pd.Series({})
        defaults = etl.get_suppressed_metric_defaults(row_empty)
        assert len(defaults) == 0
    
    def test_get_files_to_process(self, etl, tmp_path):
        """Test file selection logic."""
        # Create test directory structure
        module_dir = tmp_path / 'safe_schools_climate'
        module_dir.mkdir()
        
        # Create test files
        (module_dir / 'KYRC24_SAFE_Precautionary_Measures.csv').touch()
        (module_dir / 'quality_of_school_climate_and_safety_survey_index_scores_2022.csv').touch()
        (module_dir / 'quality_of_school_climate_and_safety_survey_index_scores_2023.csv').touch()
        (module_dir / 'quality_of_school_climate_and_safety_survey_elementary_school_2023.csv').touch()
        
        files = etl.get_files_to_process(tmp_path)
        file_names = [f.name for f in files]
        
        # Should include all quality survey files now
        assert len(files) == 4
        assert 'KYRC24_SAFE_Precautionary_Measures.csv' in file_names
        assert 'quality_of_school_climate_and_safety_survey_index_scores_2022.csv' in file_names
        assert 'quality_of_school_climate_and_safety_survey_index_scores_2023.csv' in file_names
        assert 'quality_of_school_climate_and_safety_survey_elementary_school_2023.csv' in file_names


if __name__ == "__main__":
    pytest.main([__file__, "-v"])