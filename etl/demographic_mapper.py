# Copyright 2025 Kentucky Open Government Coalition
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Demographic Mapping Module

Provides standardized demographic label mapping across all ETL pipelines to ensure
consistent longitudinal reporting. Handles year-specific variations, naming
inconsistencies, and new/removed categories.
"""
from typing import Dict, List, Optional, Union
import pandas as pd
import logging
from pathlib import Path
import yaml

logger = logging.getLogger(__name__)


class DemographicMapper:
    """
    Centralized demographic mapping service for consistent student group standardization.
    
    Features:
    - Year-specific mapping rules
    - Standardized demographic labels
    - Validation and error reporting
    - Audit trail for mapping decisions
    """
    
    def __init__(self, config_path: Optional[Path] = None):
        """Initialize mapper with configuration."""
        if config_path is None:
            config_path = Path(__file__).parent.parent / "config" / "demographic_mappings.yaml"
        
        self.config_path = config_path
        self.mappings = self._load_mappings()
        self.audit_log = []
    
    def _load_mappings(self) -> Dict:
        """Load demographic mapping configuration."""
        try:
            if self.config_path.exists():
                with open(self.config_path, 'r') as f:
                    return yaml.safe_load(f)
            else:
                logger.warning(f"Mapping config not found at {self.config_path}, using defaults")
                return self._get_default_mappings()
        except Exception as e:
            logger.error(f"Error loading demographic mappings: {e}")
            return self._get_default_mappings()
    
    def _get_default_mappings(self) -> Dict:
        """Return default demographic mappings based on analysis."""
        return {
            "standard_demographics": [
                "All Students",
                "Female", 
                "Male",
                "African American",
                "American Indian or Alaska Native",
                "Asian",
                "Hispanic or Latino",
                "Native Hawaiian or Pacific Islander", 
                "Two or More Races",
                "White (non-Hispanic)",
                "Economically Disadvantaged",
                "Non-Economically Disadvantaged",
                "English Learner",
                "Non-English Learner", 
                "Foster Care",
                "Non-Foster Care",
                "Homeless",
                "Non-Homeless",
                "Migrant",
                "Non-Migrant",
                "Students with Disabilities (IEP)",
                "Students without IEP",
                "English Learner including Monitored",
                "Military Dependent",
                "Non-Military",
                "Gifted and Talented"
            ],
            "mappings": {
                # 2024 naming variations to standard format
                "Non Economically Disadvantaged": "Non-Economically Disadvantaged",
                "Non English Learner": "Non-English Learner", 
                "Non-Foster": "Non-Foster Care",
                "Student without Disabilities (IEP)": "Students without IEP",
                
                # Case variations
                "all students": "All Students",
                "female": "Female",
                "male": "Male",
                "african american": "African American",
                "american indian or alaska native": "American Indian or Alaska Native",
                "asian": "Asian", 
                "hispanic or latino": "Hispanic or Latino",
                "native hawaiian or pacific islander": "Native Hawaiian or Pacific Islander",
                "two or more races": "Two or More Races",
                "white (non-hispanic)": "White (non-Hispanic)",
                "economically disadvantaged": "Economically Disadvantaged",
                "english learner": "English Learner",
                "foster care": "Foster Care",
                "homeless": "Homeless",
                "migrant": "Migrant",
                "students with disabilities (iep)": "Students with Disabilities (IEP)",
                "military dependent": "Military Dependent",
                
                # Handle monitored English learner variations  
                "Non-English Learner or monitored": "Non-English Learner",
                "English Learner or monitored": "English Learner including Monitored"
            },
            "year_specific": {
                "2021": {
                    "available_demographics": [
                        "All Students", "Female", "Male", "African American",
                        "American Indian or Alaska Native", "Asian", "Hispanic or Latino",
                        "Native Hawaiian or Pacific Islander", "Two or More Races", 
                        "White (non-Hispanic)", "Economically Disadvantaged",
                        "Non-Economically Disadvantaged", "English Learner", 
                        "Non-English Learner", "Foster Care", "Non-Foster Care",
                        "Homeless", "Migrant", "Students with Disabilities (IEP)",
                        "Students without IEP", "English Learner including Monitored"
                    ]
                },
                "2022": {
                    "available_demographics": [
                        "All Students", "Female", "Male", "African American",
                        "American Indian or Alaska Native", "Asian", "Hispanic or Latino", 
                        "Native Hawaiian or Pacific Islander", "Two or More Races",
                        "White (non-Hispanic)", "Economically Disadvantaged",
                        "Non-Economically Disadvantaged", "English Learner",
                        "Non-English Learner", "Foster Care", "Non-Foster Care", 
                        "Homeless", "Migrant", "Students with Disabilities (IEP)",
                        "Students without IEP", "English Learner including Monitored",
                        "Non-English Learner or monitored"
                    ],
                    "mappings": {
                        "Non-English Learner or monitored": "Non-English Learner"
                    }
                },
                "2023": {
                    "available_demographics": [
                        "All Students", "Female", "Male", "African American",
                        "American Indian or Alaska Native", "Asian", "Hispanic or Latino",
                        "Native Hawaiian or Pacific Islander", "Two or More Races", 
                        "White (non-Hispanic)", "Economically Disadvantaged",
                        "Non-Economically Disadvantaged", "English Learner",
                        "Non-English Learner", "Foster Care", "Non-Foster Care",
                        "Homeless", "Migrant", "Students with Disabilities (IEP)", 
                        "Students without IEP", "English Learner including Monitored",
                        "Non-English Learner or monitored"
                    ],
                    "mappings": {
                        "Non-English Learner or monitored": "Non-English Learner"
                    }
                },
                "2024": {
                    "available_demographics": [
                        "All Students", "Female", "Male", "African American",
                        "American Indian or Alaska Native", "Asian", "Hispanic or Latino",
                        "Native Hawaiian or Pacific Islander", "Two or More Races",
                        "White (non-Hispanic)", "Economically Disadvantaged", 
                        "Non Economically Disadvantaged", "English Learner",
                        "Non English Learner", "Foster Care", "Non-Foster",
                        "Non-Homeless", "Homeless", "Non-Migrant", "Migrant",
                        "Students with Disabilities (IEP)", "Student without Disabilities (IEP)",
                        "Military Dependent", "Non-Military"
                    ],
                    "mappings": {
                        "Non Economically Disadvantaged": "Non-Economically Disadvantaged",
                        "Non English Learner": "Non-English Learner",
                        "Non-Foster": "Non-Foster Care", 
                        "Student without Disabilities (IEP)": "Students without IEP"
                    }
                }
            },
            "validation": {
                "required_demographics": [
                    "All Students", "Female", "Male", "African American", "White (non-Hispanic)",
                    "Economically Disadvantaged", "Students with Disabilities (IEP)"
                ],
                "allow_missing": [
                    "English Learner including Monitored", "Military Dependent", 
                    "Non-Homeless", "Non-Migrant", "Non-Military"
                ]
            }
        }
    
    def map_demographic(self, demographic: str, year: str, source_file: str = "unknown") -> str:
        """
        Map a demographic label to the standardized format.
        
        Args:
            demographic: Original demographic label
            year: Data year (e.g., "2024") 
            source_file: Source filename for audit trail
            
        Returns:
            Standardized demographic label
        """
        if pd.isna(demographic) or demographic == "":
            return "All Students"
        
        original_demographic = str(demographic).strip()
        
        # Check if already in standard demographics (no mapping needed)
        standard_demographics = self.mappings.get("standard_demographics", [])
        if original_demographic in standard_demographics:
            self._log_mapping(original_demographic, original_demographic, year, source_file, "standard")
            return original_demographic
        
        # Try year-specific mappings first
        if year in self.mappings.get("year_specific", {}):
            year_mappings = self.mappings["year_specific"][year].get("mappings", {})
            if year_mappings and original_demographic in year_mappings:
                mapped = year_mappings[original_demographic]
                self._log_mapping(original_demographic, mapped, year, source_file, "year_specific")
                return mapped
        
        # Try general mappings
        general_mappings = self.mappings.get("mappings", {})
        if original_demographic in general_mappings:
            mapped = general_mappings[original_demographic]
            self._log_mapping(original_demographic, mapped, year, source_file, "general")
            return mapped
        
        # Try case-insensitive lookup
        lower_demographic = original_demographic.lower()
        for key, value in general_mappings.items():
            if key.lower() == lower_demographic:
                self._log_mapping(original_demographic, value, year, source_file, "case_insensitive")
                return value
        
        # No mapping found - log warning and return original
        logger.warning(f"No mapping found for demographic '{original_demographic}' in year {year}")
        self._log_mapping(original_demographic, original_demographic, year, source_file, "no_mapping")
        return original_demographic
    
    def map_demographics_series(self, demographics: pd.Series, year: str, source_file: str = "unknown") -> pd.Series:
        """Map an entire pandas Series of demographics."""
        return demographics.apply(lambda x: self.map_demographic(x, year, source_file))
    
    def validate_demographics(self, demographics: List[str], year: str) -> Dict[str, List[str]]:
        """
        Validate a list of demographics against expected categories for a given year.
        
        Returns:
            Dictionary with 'valid', 'missing', 'unexpected' lists
        """
        # Get expected demographics for year
        year_config = self.mappings.get("year_specific", {}).get(year, {})
        expected = set(year_config.get("available_demographics", []))
        required = set(self.mappings.get("validation", {}).get("required_demographics", []))
        allow_missing = set(self.mappings.get("validation", {}).get("allow_missing", []))
        
        actual = set(demographics)
        
        # Find missing required demographics  
        missing_required = required - actual - allow_missing
        missing_optional = (expected - actual) - allow_missing
        
        # Find unexpected demographics
        unexpected = actual - expected
        
        return {
            "valid": list(actual & expected),
            "missing_required": list(missing_required),
            "missing_optional": list(missing_optional), 
            "unexpected": list(unexpected),
            "year": year,
            "total_expected": len(expected),
            "total_actual": len(actual)
        }
    
    def _log_mapping(self, original: str, mapped: str, year: str, source_file: str, mapping_type: str):
        """Log demographic mapping for audit trail."""
        self.audit_log.append({
            "original": original,
            "mapped": mapped, 
            "year": year,
            "source_file": source_file,
            "mapping_type": mapping_type,
            "timestamp": pd.Timestamp.now().isoformat()
        })
    
    def get_audit_report(self) -> pd.DataFrame:
        """Return audit log as DataFrame."""
        return pd.DataFrame(self.audit_log)
    
    def get_standard_demographics(self) -> List[str]:
        """Return list of all standard demographic categories."""
        return self.mappings.get("standard_demographics", [])
    
    def save_audit_log(self, output_path: Path):
        """Save audit log to CSV file."""
        if self.audit_log:
            audit_df = self.get_audit_report()
            audit_df.to_csv(output_path, index=False)
            logger.info(f"Demographic mapping audit log saved to {output_path}")


def create_demographic_mapper(config_path: Optional[Path] = None) -> DemographicMapper:
    """Factory function to create a demographic mapper instance."""
    return DemographicMapper(config_path)


# Convenience functions for common operations
def standardize_demographics(demographics: pd.Series, year: str, source_file: str = "unknown", 
                           config_path: Optional[Path] = None) -> pd.Series:
    """Convenience function to standardize a pandas Series of demographics."""
    mapper = create_demographic_mapper(config_path)
    return mapper.map_demographics_series(demographics, year, source_file)


def validate_demographic_coverage(demographics: List[str], year: str,
                                config_path: Optional[Path] = None) -> Dict[str, List[str]]:
    """Convenience function to validate demographic coverage for a year."""
    mapper = create_demographic_mapper(config_path)
    return mapper.validate_demographics(demographics, year)


if __name__ == "__main__":
    # Test the mapper
    mapper = DemographicMapper()
    
    # Test some mappings
    test_cases = [
        ("Non Economically Disadvantaged", "2024"),
        ("Non English Learner", "2024"), 
        ("Non-Foster", "2024"),
        ("Student without Disabilities (IEP)", "2024"),
        ("All Students", "2021"),
        ("English Learner including Monitored", "2021")
    ]
    
    print("Testing demographic mappings:")
    for demographic, year in test_cases:
        mapped = mapper.map_demographic(demographic, year, "test")
        print(f"{demographic} ({year}) -> {mapped}")
    
    # Test validation
    print("\nTesting validation:")
    test_demographics = ["All Students", "Female", "Male", "Non Economically Disadvantaged"] 
    validation = mapper.validate_demographics(test_demographics, "2024")
    print(f"Validation results: {validation}")