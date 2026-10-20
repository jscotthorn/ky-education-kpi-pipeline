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
        # 2022 and 2023 uppercase headers are counts. The title-case KYRC24
        # headers above stay the published rate and the eligible count.
        self.assertEqual(mappings['CTE PARTICIPANTS IN ALL GRADES'], 'cte_participant_count')
        self.assertEqual(mappings['GRADE 12 CTE COMPLETERS'], 'cte_completer_count_grade_12')
        self.assertEqual(mappings['TOTAL NUMBER OF STUDENT'], 'total_student_count')
        self.assertEqual(mappings['Total Number of Student'], 'cte_participant_count')
    
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
        
        # A negative is missing. A published rate above 100 is kept.
        self.assertEqual(cleaned_df['cte_participation_rate'][0], 75.5)
        self.assertTrue(pd.isna(cleaned_df['cte_participation_rate'][1]))  # negative
        self.assertEqual(cleaned_df['cte_participation_rate'][2], 150.0)  # published, above 100
        self.assertTrue(pd.isna(cleaned_df['cte_participation_rate'][3]))  # already NA
        self.assertEqual(cleaned_df['cte_completion_rate'][2], 120.0)
        
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


def _kpi_from_frame(etl: CTEParticipationETL, frame: pd.DataFrame, name: str) -> pd.DataFrame:
    """Run one frame through the same steps process() uses."""
    normalized = etl.standardize_missing_values(etl.normalize_column_names(frame))
    return etl.convert_to_kpi_format(normalized, name)


def test_2022_counts_are_not_stored_as_rates():
    """Uppercase 2022 cells are counts. A star is suppressed. A rate is absent."""
    etl = CTEParticipationETL("cte_participation")
    frame = pd.DataFrame({
        "SCHOOL YEAR": ["20212022", "20212022"],
        "DISTRICT NAME": ["State", "Fayette County"],
        "SCHOOL NAME": ["---State Total---", "Bryan Station High School"],
        "SCHOOL CODE": ["999", "165170"],
        "DEMOGRAPHIC": ["All Students", "All Students"],
        "TOTAL NUMBER OF STUDENT": ["*", "2017"],
        "CTE PARTICIPANTS IN ALL GRADES": ["167510", "1528"],
        "GRADE 12 CTE ELIGIBLE COMPLETER": ["43229", "413"],
        "GRADE 12 CTE COMPLETERS": ["13624", "77"],
    })
    kpi = _kpi_from_frame(etl, frame, "cte_by_student_group_2022.csv")
    assert set(kpi["metric"]) == {
        "cte_high_school_enrollment_count",
        "cte_participant_count",
        "cte_eligible_completer_count_grade_12",
        "cte_completer_count_grade_12",
    }
    bryan = kpi[kpi["school_id"] == "165170"]
    values = dict(zip(bryan["metric"], bryan["value"]))
    assert values == {
        "cte_high_school_enrollment_count": 2017.0,
        "cte_participant_count": 1528.0,
        "cte_eligible_completer_count_grade_12": 413.0,
        "cte_completer_count_grade_12": 77.0,
    }
    assert (bryan["suppressed"] == "N").all()
    state_enrollment = kpi[
        (kpi["school_id"] == "999") & (kpi["metric"] == "cte_high_school_enrollment_count")
    ]
    assert len(state_enrollment) == 1
    assert state_enrollment.iloc[0]["suppressed"] == "Y"
    assert pd.isna(state_enrollment.iloc[0]["value"])
    assert float(kpi[(kpi["school_id"] == "999") & (kpi["metric"] == "cte_participant_count")].iloc[0]["value"]) == 167510.0


def test_published_rate_above_100_is_kept_and_blank_is_skipped():
    """KYRC24 stores the printed rate. Zero stays. A star is Y. A blank is absent."""
    etl = CTEParticipationETL("cte_participation")
    frame = pd.DataFrame({
        "School Year": ["20232024", "20232024", "20232024"],
        "District Name": ["Harlan County", "Fayette County", "Fayette County"],
        "School Name": ["Harlan County High School", "Fayette County Learning Center", "Bryan Station High School"],
        "School Code": ["235025", "165898", "165170"],
        "Demographic": ["American Indian or Alaska Native", "All Students", "Foster Care"],
        "Total Number of Student": ["4", "26", "0"],
        "CTE Participants in All Grades": ["400.0", "0.0", "--"],
        "Grade 12 CTE Eligible Completer": ["*", "0", ""],
        "Grade 12 CTE Completers": ["*", "N/A", "*"],
    })
    kpi = _kpi_from_frame(etl, frame, "KYRC24_CTE_Participation.csv")
    harlan = kpi[kpi["school_id"] == "235025"]
    rate = harlan[harlan["metric"] == "cte_participation_rate"].iloc[0]
    assert float(rate["value"]) == 400.0
    assert rate["suppressed"] == "N"
    assert float(harlan[harlan["metric"] == "cte_participant_count"].iloc[0]["value"]) == 4.0
    starred = harlan[harlan["metric"] == "cte_eligible_completer_count_grade_12"].iloc[0]
    assert starred["suppressed"] == "Y"
    assert pd.isna(starred["value"])

    learning = kpi[kpi["school_id"] == "165898"]
    zero_rate = learning[learning["metric"] == "cte_participation_rate"].iloc[0]
    assert float(zero_rate["value"]) == 0.0
    assert zero_rate["suppressed"] == "N"
    zero_count = learning[learning["metric"] == "cte_eligible_completer_count_grade_12"].iloc[0]
    assert float(zero_count["value"]) == 0.0
    assert "cte_completion_rate_grade_12" not in set(learning["metric"])

    foster = kpi[kpi["school_id"] == "165170"]
    assert "cte_participation_rate" not in set(foster["metric"])
    assert "cte_eligible_completer_count_grade_12" not in set(foster["metric"])
    assert float(foster[foster["metric"] == "cte_participant_count"].iloc[0]["value"]) == 0.0
    completion = foster[foster["metric"] == "cte_completion_rate_grade_12"].iloc[0]
    assert completion["suppressed"] == "Y"
    keys = kpi[["school_id", "year", "student_group", "metric"]]
    assert not keys.duplicated().any()


def test_kyrc25_keeps_counts_and_does_not_calculate_participation():
    """Counts stay. The participation rate is not calculated. Completion is."""
    etl = CTEParticipationETL("cte_participation")
    frame = pd.DataFrame({
        "School Year": ["20242025", "20242025"],
        "District Name": ["Allen County", "Fayette County"],
        "School Name": ["Allen County Career and Technical Center", "Bryan Station High School"],
        "School Code": ["005901", "165170"],
        "Demographic": ["All Students", "Military Dependent"],
        "All High School CTE Participants": ["725", "*"],
        "All High School Enrollments": ["N/A", "3"],
        "12th Grade Completers": ["107", "*"],
        "12th Grade Participants": ["233", "*"],
    })
    kpi = _kpi_from_frame(etl, frame, "KYRC25_CTE_Participation.csv")
    allen = kpi[kpi["school_id"] == "005901"]
    assert "cte_participation_rate" not in set(allen["metric"])
    assert "cte_high_school_enrollment_count" not in set(allen["metric"])
    assert float(allen[allen["metric"] == "cte_participant_count"].iloc[0]["value"]) == 725.0
    assert float(allen[allen["metric"] == "cte_completion_rate_grade_12"].iloc[0]["value"]) == 45.9
    military = kpi[kpi["school_id"] == "165170"]
    assert "cte_participation_rate" not in set(military["metric"])
    assert (military[military["metric"] == "cte_participant_count"]["suppressed"] == "Y").all()
    assert float(military[military["metric"] == "cte_high_school_enrollment_count"].iloc[0]["value"]) == 3.0
    assert (military[military["metric"] == "cte_completer_count_grade_12"]["suppressed"] == "Y").all()
    completion = military[military["metric"] == "cte_completion_rate_grade_12"].iloc[0]
    assert completion["suppressed"] == "Y"
    assert pd.isna(completion["value"])


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