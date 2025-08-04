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
from typing import Dict, List, Optional
import pandas as pd
import logging
from pathlib import Path
from ruamel import yaml

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
            yaml_parser = yaml.YAML(typ="safe", pure=True)
            with open(self.config_path, "r") as f:
                return yaml_parser.load(f)
        except FileNotFoundError as e:
            logger.error(f"Mapping config not found at {self.config_path}")
            raise e
        except Exception as e:
            logger.error(f"Error loading demographic mappings: {e}")
            raise e
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
    
    def save_audit_report(
        self,
        output_path: Path,
        validation_results: Optional[List[Dict[str, List[str]]]] = None,
    ) -> None:
        """Save concise audit information as a markdown report.

        The report summarizes which files were processed, validation results, and
        counts of mapping types used. This keeps the audit file small while still
        providing useful traceability information.
        """

        lines = ["# Demographic Mapping Summary", ""]

        # List processed files and mapping counts
        if self.audit_log:
            audit_df = self.get_audit_report()

            unique_files = sorted(audit_df["source_file"].dropna().unique())
            if unique_files:
                lines.append("## Files Processed")
                for f in unique_files:
                    lines.append(f"- {f}")
                lines.append("")

            mapping_counts = audit_df["mapping_type"].value_counts().to_dict()
            lines.append("## Mapping Types")
            for mtype, count in mapping_counts.items():
                lines.append(f"- {mtype}: {count}")
            lines.append("")

        # Include validation summary for each year
        if validation_results:
            lines.append("## Validation Summary")
            for result in validation_results:
                lines.append(f"### Year {result['year']}")
                lines.append(f"- Found demographics: {len(result['valid'])}")
                if result.get("missing_required"):
                    missing_req = ", ".join(sorted(result["missing_required"]))
                    lines.append(f"- Missing required: {missing_req}")
                if result.get("missing_optional"):
                    missing_opt = ", ".join(sorted(result["missing_optional"]))
                    lines.append(f"- Missing optional: {missing_opt}")
                lines.append("")

        output_path.write_text("\n".join(lines))
        logger.info(f"Demographic mapping report saved to {output_path}")


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

