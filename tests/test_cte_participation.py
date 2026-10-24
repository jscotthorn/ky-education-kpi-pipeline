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
        # KYRC24 title case. The header says eligible. The cells are completers.
        self.assertEqual(mappings['Grade 12 CTE Eligible Completer'], 'cte_completer_count_grade_12')
        self.assertEqual(mappings['Grade 12 CTE Completers'], 'cte_completion_rate')
        # 2022 and 2023 uppercase headers are counts. Eligible stays eligible.
        self.assertEqual(mappings['CTE PARTICIPANTS IN ALL GRADES'], 'cte_participant_count')
        self.assertEqual(mappings['GRADE 12 CTE ELIGIBLE COMPLETER'], 'cte_eligible_completer_count')
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
    """KYRC24 stores the printed rate. A star is Y. ``--`` and ``N/A`` are absent.

    The four rows are copied from the registered historical KYRC24 file.
    Harlan County High School ``235025`` American Indian is 400.0 on 4
    participants. Pike County Virtual ``491020`` All Students is 0.0 on 108
    participants. Belfry ``777101`` All Students has ``--``. Allen County
    Career and Technical Center ``005901`` All Students has ``N/A``.
    ``Grade 12 CTE Eligible Completer`` is the completer count.
    """
    fixture = ROOT / "tests" / "fixtures" / "cte_participation" / "KYRC24_CTE_Participation.csv"
    text = fixture.read_text(encoding="utf-8")
    verbatim = [
        "20232024,048,HARLAN,235,Harlan County,025,Harlan County High School,235025,048235025,210254002213,SESC,907,A1,American Indian or Alaska Native,4,400.0,*,*",
        "20232024,098,PIKE,491,Pike County,020,Pike County Virtual Academy,491020,098491020,210480002532,KVEC,904,A8,All Students,108,0.0,*,*",
        "20232024,037,FRANKLIN,777,Kentucky Tech System,101,Belfry Area Technology Center,777101,037777101,210028902107,,,C2,All Students,523,--,85,42.1",
        "20232024,002,ALLEN,005,Allen County,901,Allen County Career and Technical Center,005901,002005901,210007002061,GRREC,902,A2,All Students,704,N/A,76,40.4",
    ]
    for line in verbatim:
        assert line in text.splitlines()

    frame = pd.read_csv(fixture, dtype=str, keep_default_na=False)
    frame = frame[frame["School Code"].isin(["235025", "491020", "777101", "005901"])]
    etl = CTEParticipationETL("cte_participation")
    kpi = _kpi_from_frame(etl, frame, "KYRC24_CTE_Participation.csv")

    harlan = kpi[kpi["school_id"] == "235025"]
    rate = harlan[harlan["metric"] == "cte_participation_rate"].iloc[0]
    assert float(rate["value"]) == 400.0
    assert rate["suppressed"] == "N"
    assert float(harlan[harlan["metric"] == "cte_participant_count"].iloc[0]["value"]) == 4.0
    starred = harlan[harlan["metric"] == "cte_completer_count_grade_12"].iloc[0]
    assert starred["suppressed"] == "Y"
    assert pd.isna(starred["value"])
    assert "cte_eligible_completer_count_grade_12" not in set(harlan["metric"])

    pike = kpi[kpi["school_id"] == "491020"]
    zero_rate = pike[pike["metric"] == "cte_participation_rate"].iloc[0]
    assert float(zero_rate["value"]) == 0.0
    assert zero_rate["suppressed"] == "N"
    assert float(pike[pike["metric"] == "cte_participant_count"].iloc[0]["value"]) == 108.0

    belfry = kpi[kpi["school_id"] == "777101"]
    assert "cte_participation_rate" not in set(belfry["metric"])
    assert float(belfry[belfry["metric"] == "cte_participant_count"].iloc[0]["value"]) == 523.0
    assert float(belfry[belfry["metric"] == "cte_completer_count_grade_12"].iloc[0]["value"]) == 85.0
    assert float(belfry[belfry["metric"] == "cte_completion_rate_grade_12"].iloc[0]["value"]) == 42.1

    allen = kpi[kpi["school_id"] == "005901"]
    assert "cte_participation_rate" not in set(allen["metric"])
    assert float(allen[allen["metric"] == "cte_participant_count"].iloc[0]["value"]) == 704.0
    assert float(allen[allen["metric"] == "cte_completer_count_grade_12"].iloc[0]["value"]) == 76.0
    assert float(allen[allen["metric"] == "cte_completion_rate_grade_12"].iloc[0]["value"]) == 40.4
    assert "cte_eligible_completer_count_grade_12" not in set(kpi["metric"])
    keys = kpi[["school_id", "year", "student_group", "metric"]]
    assert not keys.duplicated().any()


def test_star_does_not_overwrite_a_published_number(caplog):
    """A starred duplicate keeps the number. The rows are synthetic.

    No registered file has two rows for the same school, year, and
    demographic. School code ``SYNSTAR`` is not a district code.
    """
    etl = CTEParticipationETL("cte_participation")
    frame = pd.DataFrame({
        "School Year": ["20232024", "20232024"],
        "District Name": ["Synthetic District", "Synthetic District"],
        "School Name": ["Synthetic School", "Synthetic School"],
        "School Code": ["SYNSTAR", "SYNSTAR"],
        "Demographic": ["All Students", "All Students"],
        "Total Number of Student": ["10", "*"],
        "CTE Participants in All Grades": ["*", "80.0"],
        "Grade 12 CTE Eligible Completer": ["4", "*"],
        "Grade 12 CTE Completers": ["*", "*"],
    })
    with caplog.at_level("WARNING"):
        kpi = _kpi_from_frame(etl, frame, "synthetic_duplicate.csv")
    participants = kpi[kpi["metric"] == "cte_participant_count"].iloc[0]
    assert float(participants["value"]) == 10.0
    assert participants["suppressed"] == "N"
    rate = kpi[kpi["metric"] == "cte_participation_rate"].iloc[0]
    assert float(rate["value"]) == 80.0
    assert rate["suppressed"] == "N"
    completers = kpi[kpi["metric"] == "cte_completer_count_grade_12"].iloc[0]
    assert float(completers["value"]) == 4.0
    assert completers["suppressed"] == "N"
    assert "two cte_participant_count values" in caplog.text
    assert "two cte_participation_rate values" in caplog.text
    keys = kpi[["school_id", "year", "student_group", "metric"]]
    assert not keys.duplicated().any()


def test_less_than_10_is_suppressed_not_blank():
    """``<10`` is value NA with suppressed Y. The row is synthetic.

    No registered CTE file contains ``<10``. School code ``SYNLT10`` is
    not a district code.
    """
    etl = CTEParticipationETL("cte_participation")
    frame = pd.DataFrame({
        "School Year": ["20232024"],
        "District Name": ["Synthetic District"],
        "School Name": ["Synthetic School"],
        "School Code": ["SYNLT10"],
        "Demographic": ["All Students"],
        "Total Number of Student": ["<10"],
        "CTE Participants in All Grades": ["12.5"],
        "Grade 12 CTE Eligible Completer": ["<10"],
        "Grade 12 CTE Completers": ["--"],
    })
    kpi = _kpi_from_frame(etl, frame, "synthetic_lt10.csv")
    assert set(kpi["metric"]) == {
        "cte_participant_count",
        "cte_participation_rate",
        "cte_completer_count_grade_12",
    }
    count = kpi[kpi["metric"] == "cte_participant_count"].iloc[0]
    assert count["suppressed"] == "Y"
    assert pd.isna(count["value"])
    completers = kpi[kpi["metric"] == "cte_completer_count_grade_12"].iloc[0]
    assert completers["suppressed"] == "Y"
    assert pd.isna(completers["value"])
    assert float(kpi[kpi["metric"] == "cte_participation_rate"].iloc[0]["value"]) == 12.5
    assert "cte_completion_rate_grade_12" not in set(kpi["metric"])


def test_kyrc25_keeps_counts_and_does_not_calculate_rates():
    """Counts stay. Neither participation nor completion is calculated."""
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
    assert "cte_completion_rate_grade_12" not in set(allen["metric"])
    assert float(allen[allen["metric"] == "cte_completer_count_grade_12"].iloc[0]["value"]) == 107.0
    assert float(allen[allen["metric"] == "cte_participant_count_grade_12"].iloc[0]["value"]) == 233.0
    military = kpi[kpi["school_id"] == "165170"]
    assert "cte_participation_rate" not in set(military["metric"])
    assert "cte_completion_rate_grade_12" not in set(military["metric"])
    assert (military[military["metric"] == "cte_participant_count"]["suppressed"] == "Y").all()
    assert float(military[military["metric"] == "cte_high_school_enrollment_count"].iloc[0]["value"]) == 3.0
    assert (military[military["metric"] == "cte_completer_count_grade_12"]["suppressed"] == "Y").all()
    assert (military[military["metric"] == "cte_participant_count_grade_12"]["suppressed"] == "Y").all()


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