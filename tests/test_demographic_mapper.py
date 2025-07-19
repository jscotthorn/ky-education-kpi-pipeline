"""
Tests for the demographic mapping functionality.
Validates standardization, year-specific mapping, and validation features.
"""
import pytest
import pandas as pd
from pathlib import Path
from etl.demographic_mapper import DemographicMapper, standardize_demographics, validate_demographic_coverage


class TestDemographicMapper:
    """Test the DemographicMapper class functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mapper = DemographicMapper()
    
    def test_basic_mapping(self):
        """Test basic demographic mapping functionality."""
        # Test 2024 mapping variations
        test_cases = [
            ("Non Economically Disadvantaged", "2024", "Non-Economically Disadvantaged"),
            ("Non English Learner", "2024", "Non-English Learner"),
            ("Non-Foster", "2024", "Non-Foster Care"),
            ("Student without Disabilities (IEP)", "2024", "Students without IEP"),
            ("All Students", "2024", "All Students"),
            ("Female", "2024", "Female"),
            ("Male", "2024", "Male")
        ]
        
        for original, year, expected in test_cases:
            result = self.mapper.map_demographic(original, year, "test")
            assert result == expected, f"Expected {expected}, got {result} for {original}"
    
    def test_case_insensitive_mapping(self):
        """Test case-insensitive demographic mapping."""
        test_cases = [
            ("all students", "2024", "All Students"),
            ("female", "2024", "Female"),
            ("MALE", "2024", "Male"),
            ("african american", "2024", "African American")
        ]
        
        for original, year, expected in test_cases:
            result = self.mapper.map_demographic(original, year, "test")
            assert result == expected, f"Expected {expected}, got {result} for {original}"
    
    def test_year_specific_mapping(self):
        """Test year-specific mapping rules."""
        # Test that monitored English learner categories are kept distinct
        result_2022 = self.mapper.map_demographic("Non-English Learner or monitored", "2022", "test")
        assert result_2022 == "Non-English Learner or monitored"
        
        result_2023 = self.mapper.map_demographic("Non-English Learner or monitored", "2023", "test")
        assert result_2023 == "Non-English Learner or monitored"
        
        # Test that regular English Learner categories remain distinct
        result_el = self.mapper.map_demographic("English Learner", "2022", "test")
        assert result_el == "English Learner"
        
        result_el_monitored = self.mapper.map_demographic("English Learner including Monitored", "2021", "test")
        assert result_el_monitored == "English Learner including Monitored"
        
        # Test that 2024 mappings work
        result_2024 = self.mapper.map_demographic("Non Economically Disadvantaged", "2024", "test")
        assert result_2024 == "Non-Economically Disadvantaged"
    
    def test_missing_values(self):
        """Test handling of missing/null demographic values."""
        test_cases = [
            (None, "2024", "All Students"),
            ("", "2024", "All Students"),
            (pd.NA, "2024", "All Students")
        ]
        
        for original, year, expected in test_cases:
            result = self.mapper.map_demographic(original, year, "test")
            assert result == expected, f"Expected {expected}, got {result} for {original}"
    
    def test_unmapped_demographics(self):
        """Test handling of demographics without mappings."""
        # New demographics that don't have mappings should return original
        result = self.mapper.map_demographic("Some New Category", "2024", "test")
        assert result == "Some New Category"
        
        # Should log a warning but not fail
        assert len(self.mapper.audit_log) > 0
    
    def test_series_mapping(self):
        """Test mapping of pandas Series."""
        demographics = pd.Series([
            "All Students",
            "Non Economically Disadvantaged", 
            "Non English Learner",
            "Student without Disabilities (IEP)"
        ])
        
        result = self.mapper.map_demographics_series(demographics, "2024", "test")
        
        expected = pd.Series([
            "All Students",
            "Non-Economically Disadvantaged",
            "Non-English Learner", 
            "Students without IEP"
        ])
        
        pd.testing.assert_series_equal(result, expected)
    
    def test_validation(self):
        """Test demographic validation functionality."""
        # Test with core demographics present in all years
        core_demographics = [
            "All Students", "Female", "Male", "African American",
            "American Indian or Alaska Native", "Asian", "Hispanic or Latino",
            "Native Hawaiian or Pacific Islander", "Two or More Races",
            "White (non-Hispanic)", "Economically Disadvantaged", "English Learner",
            "Foster Care", "Homeless", "Migrant", "Students with Disabilities (IEP)"
        ]
        
        validation = self.mapper.validate_demographics(core_demographics, "2024")
        
        assert "All Students" in validation["valid"]
        assert "Female" in validation["valid"]
        assert "African American" in validation["valid"]
        assert "English Learner" in validation["valid"]
        assert validation["year"] == "2024"
        
        # Test missing required demographics
        minimal_demographics = ["All Students", "Female", "Male"]
        validation = self.mapper.validate_demographics(minimal_demographics, "2024")
        
        assert len(validation["missing_required"]) > 0
        assert "African American" in validation["missing_required"]
        assert "English Learner" in validation["missing_required"]
        assert "Students with Disabilities (IEP)" in validation["missing_required"]
    
    def test_audit_logging(self):
        """Test audit logging functionality."""
        # Clear existing audit log
        self.mapper.audit_log = []
        
        # Perform some mappings
        self.mapper.map_demographic("Non Economically Disadvantaged", "2024", "test.csv")
        self.mapper.map_demographic("All Students", "2024", "test.csv")
        
        # Check audit log
        assert len(self.mapper.audit_log) == 2
        
        audit_df = self.mapper.get_audit_report()
        assert len(audit_df) == 2
        assert "original" in audit_df.columns
        assert "mapped" in audit_df.columns
        assert "year" in audit_df.columns
        assert "source_file" in audit_df.columns
        
        # Check specific entries
        assert audit_df.iloc[0]["original"] == "Non Economically Disadvantaged"
        assert audit_df.iloc[0]["mapped"] == "Non-Economically Disadvantaged"
        assert audit_df.iloc[0]["year"] == "2024"
        assert audit_df.iloc[0]["source_file"] == "test.csv"
    
    def test_standard_demographics_list(self):
        """Test getting standard demographics list."""
        standards = self.mapper.get_standard_demographics()
        
        assert isinstance(standards, list)
        assert "All Students" in standards
        assert "Female" in standards
        assert "Male" in standards
        assert "Non-Economically Disadvantaged" in standards
        assert len(standards) > 20  # Should have many standard demographics


class TestDemographicMapperIntegration:
    """Test integration with the graduation rates pipeline."""
    
    def test_graduation_rates_mapping(self):
        """Test that graduation rates pipeline uses demographic mapping correctly."""
        processed_file = Path("/Users/scott/Projects/equity-etl/data/processed/graduation_rates.csv")
        
        if not processed_file.exists():
            pytest.skip("Processed graduation_rates.csv not found. Run ETL pipeline first.")
        
        df = pd.read_csv(processed_file)
        
        # Check that standardized demographics are present
        demographics = df['student_group'].unique()
        
        # 2024 data should have standardized names
        data_2024 = df[df['year'] == 2024]
        if len(data_2024) > 0:
            demographics_2024 = data_2024['student_group'].unique()
            
            # Should contain standardized forms, not original 2024 forms
            assert "Non-Economically Disadvantaged" in demographics_2024
            assert "Non-English Learner" in demographics_2024
            assert "Non-Foster Care" in demographics_2024
            assert "Students without IEP" in demographics_2024
            
            # Should NOT contain unstandardized forms
            assert "Non Economically Disadvantaged" not in demographics_2024
            assert "Non English Learner" not in demographics_2024
            assert "Non-Foster" not in demographics_2024
            assert "Student without Disabilities (IEP)" not in demographics_2024
    
    def test_audit_file_exists(self):
        """Test that demographic audit file is created."""
        audit_file = Path("/Users/scott/Projects/equity-etl/data/processed/graduation_rates_demographic_audit.csv")
        
        if not audit_file.exists():
            pytest.skip("Demographic audit file not found. Run ETL pipeline first.")
        
        audit_df = pd.read_csv(audit_file)
        
        # Check audit file structure
        required_columns = ["original", "mapped", "year", "source_file", "mapping_type", "timestamp"]
        for col in required_columns:
            assert col in audit_df.columns
        
        # Should have mappings for 2024 data
        mappings_2024 = audit_df[audit_df['year'] == 2024]
        assert len(mappings_2024) > 0
        
        # Should have some year-specific mappings
        year_specific = audit_df[audit_df['mapping_type'] == 'year_specific']
        assert len(year_specific) > 0


class TestConvenienceFunctions:
    """Test convenience functions for demographic mapping."""
    
    def test_standardize_demographics_function(self):
        """Test the standalone standardize_demographics function."""
        demographics = pd.Series([
            "Non Economically Disadvantaged",
            "All Students", 
            "Non English Learner"
        ])
        
        result = standardize_demographics(demographics, "2024", "test.csv")
        
        expected = pd.Series([
            "Non-Economically Disadvantaged",
            "All Students",
            "Non-English Learner"
        ])
        
        pd.testing.assert_series_equal(result, expected)
    
    def test_validate_demographic_coverage_function(self):
        """Test the standalone validate_demographic_coverage function."""
        demographics = ["All Students", "Female", "Male"]
        
        result = validate_demographic_coverage(demographics, "2024")
        
        assert isinstance(result, dict)
        assert "valid" in result
        assert "missing_required" in result
        assert "missing_optional" in result
        assert "unexpected" in result
        assert result["year"] == "2024"


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])