"""
Unit tests for the CTE Participation ETL module.
"""
import unittest
from pathlib import Path
import pandas as pd
import numpy as np
import shutil
import sys
import tempfile

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import yaml

from etl.cte_participation import CTEParticipationETL, clean_cte_data

ROOT = Path(__file__).resolve().parents[1]


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


class TestKyrc26CTEParticipation:
    """KYRC26 opt-in. There is no temporary extract to skip."""

    def setup_method(self):
        self.test_dir = Path(tempfile.mkdtemp())
        self.sample_dir = self.test_dir / "cte_participation"
        self.sample_dir.mkdir(parents=True)
        self.etl = CTEParticipationETL("cte_participation")

    def teardown_method(self):
        shutil.rmtree(self.test_dir)

    def test_kyrc26_file_is_accepted(self):
        """The permanent KYRC files and the 2022-2023 historical names stay in scope."""
        assert self.etl.include_kyrc26 is True
        assert self.etl.module_column_mappings["All High School CTE Participants"] == "cte_participant_count"
        assert self.etl.module_column_mappings["All High School Enrollments"] == "total_student_count"
        assert self.etl.module_column_mappings["12th Grade Completers"] == "cte_completer_count_grade_12"
        assert self.etl.module_column_mappings["12th Grade Participants"] == "cte_participant_count_grade_12"

        accepted = [
            "KYRC26_CTE_Participation.csv",
            "KYRC25_CTE_Participation.csv",
            "KYRC24_CTE_Participation.csv",
            "cte_by_student_group_2023.csv",
            "cte_by_student_group_2022.csv",
        ]
        for name in accepted:
            (self.sample_dir / name).write_text("School Year\n20252026\n")

        found = sorted(path.name for path, _sheet in self.etl._find_data_files(self.sample_dir))
        assert found == sorted(accepted)

    def test_blank_row_and_unmapped_sibling_emit_nothing(self):
        """An empty non-Y row is not offered. Sibling CTE files map no measure.

        notes/8: a row that is not suppressed and has every value empty is
        the absence of a row. These files have no Suppressed column. The
        base converter already skips an empty metric, so the blank row emits
        nothing. Career Pathways, Student Objectives, and the Perkins report
        do not use these measure headers and have no Demographic column, so
        they also emit nothing and need no filename guard.
        """
        blank = pd.DataFrame({
            "School Year": ["20252026"],
            "District Name": ["All Districts"],
            "School Name": ["All Schools"],
            "School Code": ["999000"],
            "Demographic": ["All Students"],
            "All High School CTE Participants": [""],
            "All High School Enrollments": [""],
            "12th Grade Completers": [""],
            "12th Grade Participants": [""],
        })
        pathways = pd.DataFrame({
            "School Year": ["20252026"],
            "District Name": ["All Districts"],
            "School Name": ["All Schools"],
            "School Code": ["999000"],
            "Program Area": ["Agricultural Education"],
            "Pathways Available": ["Agribusiness Systems"],
            "Active Enrollment": ["2,546"],
            "Concentrator Students": ["2,002"],
            "Pathway Completers": ["352"],
        })
        objectives = pd.DataFrame({
            "School Year": ["20252026"],
            "District Name": ["All Districts"],
            "School Name": ["All Schools"],
            "School Code": ["999000"],
            "Percent 12th Grade Exploring": ["15.7"],
            "Percent 12th Grade Concentrating": ["37.7"],
            "Percent 12th Grade Completing": ["46.5"],
        })
        perkins = pd.DataFrame({
            "School Year": ["20252026"],
            "District Name": ["All Districts"],
            "School Name": ["All Schools"],
            "School Code": ["999000"],
            "Performance Measure": ["1S1 - Four-Year Graduation Rate"],
            "Total Enrollment": ["35,721"],
            "Students Meeting Benchmark": ["35,192"],
            "Performance Goal": ["Met"],
        })
        for frame, name in (
            (blank, "blank.csv"),
            (pathways, "KYRC26_CTE_Career_Pathways.csv"),
            (objectives, "KYRC26_CTE_Student_Objectives.csv"),
            (perkins, "KYRC26_CTE_Perkins_Report.csv"),
        ):
            normalized = self.etl.standardize_missing_values(self.etl.normalize_column_names(frame))
            kpi = self.etl.convert_to_kpi_format(normalized, name)
            assert kpi.empty, name

        for name in (
            "KYRC26_CTE_Career_Pathways.csv",
            "KYRC26_CTE_Student_Objectives.csv",
            "KYRC26_CTE_Perkins_Report.csv",
        ):
            (self.sample_dir / name).write_text("School Year\n20252026\n")
            assert self.etl.accepts_data_file(self.sample_dir / name)


def test_registered_sources_are_the_permanent_files():
    """2024-25 is already the permanent KYRC25 file. No temporary name is registered."""
    config = yaml.safe_load((ROOT / "config" / "kde_sources.yaml").read_text())
    names = []
    for entry in config["raw_directories"]["cte_participation"]:
        names.append(entry["file"] if isinstance(entry, dict) else entry)

    assert names == [
        "KYRC24_CTE_Participation.csv",
        "KYRC25_CTE_Participation.csv",
        "KYRC26_CTE_Participation.csv",
        "cte_by_student_group_2023.csv",
        "cte_by_student_group_2022.csv",
    ]


if __name__ == '__main__':
    unittest.main()