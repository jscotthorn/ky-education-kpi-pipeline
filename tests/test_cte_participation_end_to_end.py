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


FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "cte_participation"

# Values the base-branch parser already wrote. Participation rate, then the
# grade-12 completion rate and its two counts when the file is the KYRC25
# count format. KYRC24 stores the rate directly and an eligible-completer count.
PRIOR_RATE_ANCHORS = {
    ("2024", "999000"): 68.7,
    ("2024", "165000"): 64.3,
    ("2024", "165170"): 61.3,
    ("2024", "001010"): 63.9,
    ("2025", "999000"): 70.3,
    ("2025", "165000"): 70.7,
    ("2025", "165170"): 65.6,
    ("2025", "001010"): 59.2,
}

# KYRC25 count-format grade 12: rate, completer count, participant count.
PRIOR_GRADE12_ANCHORS = {
    ("2024", "999000"): (42.3, 17597, None),
    ("2024", "165000"): (38.5, 1053, None),
    ("2024", "165170"): (29.5, 136, None),
    ("2024", "001010"): (26.2, 38, None),
    ("2025", "999000"): (44.6, 19096, 42834),
    ("2025", "165000"): (43.6, 1281, 2937),
    ("2025", "165170"): (30.2, 152, 504),
    ("2025", "001010"): (23.3, 40, 172),
}

# The two columns the parser maps to rates (participants, completers) hold
# counts, as do all four 2022 and 2023 measure columns. TOTAL NUMBER OF
# STUDENT and GRADE 12 CTE ELIGIBLE COMPLETER are already mapped as counts.
# None of the headers is named like a rate. The base parser publishes a
# count of 100 or below as the rate and nulls a count above 100. Bryan
# Station 2022 stores completers 77 of 413 eligible (18.6%)
# and the published completion-rate metric is 77. Participants above 100 are
# nulled, so there is no participation rate. Pre-existing data loss, issue
# #47 (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/47).
# Not changed here. The assertions below are the published values.
OLDER_ANCHORS = {
    ("2022", "999"): {"cte_eligible_completer_count_grade_12": 43229},
    ("2022", "165170"): {
        "cte_completion_rate_grade_12": 77.0,
        "cte_eligible_completer_count_grade_12": 413,
    },
    ("2023", "999"): {"cte_eligible_completer_count_grade_12": 25097},
    ("2023", "165170"): {
        "cte_completion_rate_grade_12": 50.0,
        "cte_eligible_completer_count_grade_12": 188,
    },
}


def _run_fixtures(tmp_path: Path, filenames: list) -> pd.DataFrame:
    """Copy committed real-row excerpts into a raw directory and transform them."""
    raw_dir = tmp_path / "raw"
    source_dir = raw_dir / "cte_participation"
    source_dir.mkdir(parents=True)
    proc_dir = tmp_path / "processed"
    proc_dir.mkdir()
    for name in filenames:
        (source_dir / name).write_bytes((FIXTURE_DIR / name).read_bytes())
    transform(raw_dir, proc_dir, {"derive": {"processing_date": "2026-10-17"}})
    output = proc_dir / "cte_participation.csv"
    assert output.exists(), filenames
    return pd.read_csv(output, dtype=str)


def _comparable(df: pd.DataFrame) -> pd.DataFrame:
    """Drop the processing timestamp and sort so two runs can be compared."""
    columns = [column for column in df.columns if column != "last_updated"]
    return df[columns].sort_values(columns).reset_index(drop=True)


def _rows(df: pd.DataFrame, year: str, school_id: str, student_group: str) -> pd.DataFrame:
    return df[
        (df["year"] == year)
        & (df["school_id"] == school_id)
        & (df["student_group"] == student_group)
    ]


def _metric_value(df: pd.DataFrame, year: str, school_id: str, metric: str, student_group: str = "All Students"):
    matches = _rows(df, year, school_id, student_group)
    matches = matches[matches["metric"] == metric]
    assert len(matches) == 1, (year, school_id, metric, student_group, len(matches))
    return matches.iloc[0]


def test_kyrc26_real_rows_and_prior_year_unchanged(tmp_path):
    """2025-26 KPIs from real CTE rows, with fixed earlier-year anchors.

    State All Students is 144,661 participants and 201,255 enrollments.
    The participation rate is 71.9 and the grade-12 rate is 45.4. KPI year
    is 2026, from school year 20252026. District is All Districts.
    The 2022-2025 anchors are values the base parser already wrote.
    """
    prior_names = [
        "cte_by_student_group_2022.csv",
        "cte_by_student_group_2023.csv",
        "KYRC24_CTE_Participation.csv",
        "KYRC25_CTE_Participation.csv",
    ]
    current_names = prior_names + ["KYRC26_CTE_Participation.csv"]
    prior_only = _run_fixtures(tmp_path / "prior", prior_names)
    combined = _run_fixtures(tmp_path / "combined", current_names)

    # Same-branch check only: adding the 2026 excerpt does not rewrite earlier rows.
    # The fixed anchors below are the prior-year regression.
    prior_slice = combined[combined["year"].isin(["2022", "2023", "2024", "2025"])]
    assert _comparable(prior_slice).equals(_comparable(prior_only))

    assert set(combined["source_file"]) == set(current_names)
    assert "20252026" not in set(combined["year"])
    assert set(combined.loc[combined["source_file"].str.startswith("KYRC26_"), "year"]) == {"2026"}

    for (year, school_id), rate in PRIOR_RATE_ANCHORS.items():
        row = _metric_value(combined, year, school_id, "cte_participation_rate")
        assert float(row["value"]) == rate
        assert row["suppressed"] == "N"
    for (year, school_id), (rate, eligible_or_completers, participants) in PRIOR_GRADE12_ANCHORS.items():
        completion = _metric_value(combined, year, school_id, "cte_completion_rate_grade_12")
        assert float(completion["value"]) == rate
        assert completion["suppressed"] == "N"
        if participants is None:
            count = _metric_value(combined, year, school_id, "cte_eligible_completer_count_grade_12")
            assert float(count["value"]) == eligible_or_completers
        else:
            completers = _metric_value(combined, year, school_id, "cte_completer_count_grade_12")
            grade_participants = _metric_value(combined, year, school_id, "cte_participant_count_grade_12")
            assert float(completers["value"]) == eligible_or_completers
            assert float(grade_participants["value"]) == participants

    for (year, school_id), expected in OLDER_ANCHORS.items():
        for metric, value in expected.items():
            row = _metric_value(combined, year, school_id, metric)
            assert float(row["value"]) == value
            assert row["suppressed"] == "N"
        if school_id == "999":
            assert _metric_value(combined, year, school_id, "cte_eligible_completer_count_grade_12")["district"] == "State"
            assert _metric_value(combined, year, school_id, "cte_eligible_completer_count_grade_12")["school_name"] == "---State Total---"

    bryan_2024 = _metric_value(combined, "2024", "165170", "cte_participation_rate")
    bryan_2025 = _metric_value(combined, "2025", "165170", "cte_participation_rate")
    assert bryan_2024["nces_id"] == "210186000388"
    assert bryan_2025["nces_id"] == "210186000000"
    assert bryan_2024["district"] == "Fayette County"
    assert bryan_2025["school_name"] == "Bryan Station High School"

    state = _metric_value(combined, "2026", "999000", "cte_participation_rate")
    assert float(state["value"]) == 71.9
    assert state["suppressed"] == "N"
    assert state["district"] == "All Districts"
    assert state["school_name"] == "---District Total---"
    assert state["year"] == "2026"
    assert state["school_id"] == "999000"
    assert state["source_file"] == "KYRC26_CTE_Participation.csv"
    assert float(_metric_value(combined, "2026", "999000", "cte_completion_rate_grade_12")["value"]) == 45.4
    assert float(_metric_value(combined, "2026", "999000", "cte_completer_count_grade_12")["value"]) == 19990
    assert float(_metric_value(combined, "2026", "999000", "cte_participant_count_grade_12")["value"]) == 44038

    district = _metric_value(combined, "2026", "165000", "cte_participation_rate")
    assert float(district["value"]) == 75.3
    assert district["district"] == "Fayette County"
    assert district["school_name"] == "---District Total---"
    assert district["school_id"] == "165000"
    assert district["co_op"] == "CKEC"
    assert district["co_op_code"] == "901"
    assert district["nces_id"] == "2101860"
    assert district["suppressed"] == "N"
    assert float(_metric_value(combined, "2026", "165000", "cte_completion_rate_grade_12")["value"]) == 47.7
    assert float(_metric_value(combined, "2026", "165000", "cte_completer_count_grade_12")["value"]) == 1404
    assert float(_metric_value(combined, "2026", "165000", "cte_participant_count_grade_12")["value"]) == 2946

    school = _metric_value(combined, "2026", "165170", "cte_participation_rate")
    assert float(school["value"]) == 60.6
    assert school["district"] == "Fayette County"
    assert school["school_name"] == "Bryan Station High School"
    assert school["state_school_id"] == "034165170"
    assert school["nces_id"] == "210186000388"
    assert school["school_type"] == "A1"
    assert school["county_name"] == "FAYETTE"
    assert school["co_op"] == "CKEC"
    assert school["co_op_code"] == "901"
    assert school["suppressed"] == "N"
    assert float(_metric_value(combined, "2026", "165170", "cte_completion_rate_grade_12")["value"]) == 29.9
    assert float(_metric_value(combined, "2026", "165170", "cte_completer_count_grade_12")["value"]) == 144
    assert float(_metric_value(combined, "2026", "165170", "cte_participant_count_grade_12")["value"]) == 482

    adair = _metric_value(combined, "2026", "001010", "cte_participation_rate")
    assert float(adair["value"]) == 55.5
    assert adair["district"] == "Adair County"
    assert adair["school_name"] == "Adair County High School"
    assert adair["nces_id"] == "210003000001"
    assert adair["co_op"] == "GRREC"
    assert adair["co_op_code"] == "902"
    assert float(_metric_value(combined, "2026", "001010", "cte_completion_rate_grade_12")["value"]) == 20.2

    # Partial suppression. Participants 15 and enrollments 18 are published.
    # The two 12th-grade cells are *. The published rate stays required.
    # Current parser drops the starred cells instead of emitting suppressed=Y.
    # That deviates from notes/8. Tracked in issue #46
    # (https://github.com/jscotthorn/ky-education-kpi-pipeline/issues/46).
    # Issue #44 is the same kind of call for postsecondary enrollment and
    # does not cover this file. Update the absence below if #46 starts
    # emitting Y. Do not treat the absence as the intended design.
    partial = _rows(combined, "2026", "165000", "American Indian or Alaska Native")
    assert set(partial["metric"]) == {"cte_participation_rate"}
    assert float(partial.iloc[0]["value"]) == 83.3
    assert partial.iloc[0]["suppressed"] == "N"
    assert partial.iloc[0]["district"] == "Fayette County"

    # Foster Care publishes the participation rate from 21 / 49. Grade-12
    # completers are * and grade-12 participants are 6, so the current parser
    # emits no grade-12 metric. The published rate stays required. The absent
    # grade-12 metrics are the notes/8 deviation described above.
    foster = _rows(combined, "2026", "165170", "Foster Care")
    assert set(foster["metric"]) == {"cte_participation_rate"}
    assert float(foster.iloc[0]["value"]) == 42.9
    assert foster.iloc[0]["suppressed"] == "N"
    assert foster.iloc[0]["school_name"] == "Bryan Station High School"

    # Fully starred in KYRC24 and KYRC26. KYRC25 omitted the row. The current
    # parser emits nothing. See issue #46; do not treat the drop as the design.
    assert _rows(combined, "2024", "165170", "Native Hawaiian or Pacific Islander").empty
    assert _rows(combined, "2026", "165170", "Native Hawaiian or Pacific Islander").empty

    # Career center: enrollment is the text N/A (not applicable; students count
    # at their home school). pandas reads N/A as missing, so there is no
    # participation rate. Grade 12 is published (122 / 241). Treating N/A as
    # an empty denominator stays correct, and the published counts stay required.
    allen = _rows(combined, "2026", "005901", "All Students")
    assert set(allen["metric"]) == {
        "cte_completion_rate_grade_12",
        "cte_completer_count_grade_12",
        "cte_participant_count_grade_12",
    }
    assert float(_metric_value(combined, "2026", "005901", "cte_completion_rate_grade_12")["value"]) == 50.6
    assert float(_metric_value(combined, "2026", "005901", "cte_completer_count_grade_12")["value"]) == 122
    assert float(_metric_value(combined, "2026", "005901", "cte_participant_count_grade_12")["value"]) == 241
    assert allen.iloc[0]["school_name"] == "Allen County Career and Technical Center"
    assert allen.iloc[0]["school_type"] == "A2"
    assert allen.iloc[0]["nces_id"] == "210007002061"
    assert (allen["suppressed"] == "N").all()

    # Star beside an N/A enrollment. Every measure is * or N/A, so the current
    # parser emits nothing. N/A is an empty denominator. The stars are the
    # notes/8 deviation in issue #46, not a locked design.
    assert _rows(combined, "2026", "005901", "American Indian or Alaska Native").empty

    # The 2025 career-center row has the same N/A enrollment. Published
    # grade-12 values stay what the base parser wrote. NCES id differs.
    allen_2025 = _rows(combined, "2025", "005901", "All Students")
    assert "cte_participation_rate" not in set(allen_2025["metric"])
    assert float(_metric_value(combined, "2025", "005901", "cte_completion_rate_grade_12")["value"]) == 45.9
    assert float(_metric_value(combined, "2025", "005901", "cte_completer_count_grade_12")["value"]) == 107
    assert float(_metric_value(combined, "2025", "005901", "cte_participant_count_grade_12")["value"]) == 233
    assert allen_2025.iloc[0]["nces_id"] == "210007000000"

    # KYRC25 Bryan Station Military Dependent publishes only the enrollment
    # cell (3). Participants and both grade-12 cells are *. Enrollment is a
    # denominator, not its own KPI, so the current parser emits no row.
    # The starred cells are the notes/8 deviation above.
    assert _rows(combined, "2025", "165170", "Military Dependent").empty

    # 2024 Foster Care publishes the participation rate. The grade-12 cells
    # are *. The published rate is the anchor. Absence of the starred metrics
    # is the same notes/8 deviation, not a new KYRC26 rule.
    foster_2024 = _rows(combined, "2024", "165170", "Foster Care")
    assert set(foster_2024["metric"]) == {"cte_participation_rate"}
    assert float(foster_2024.iloc[0]["value"]) == 29.0
    assert foster_2024.iloc[0]["suppressed"] == "N"


if __name__ == '__main__':
    unittest.main()