"""End-to-end tests for Kentucky Summative Assessment pipeline"""
import tempfile
import shutil
from pathlib import Path
import pandas as pd
from etl.kentucky_summative_assessment import transform


class TestKentuckySummativeAssessmentEndToEnd:
    def setup_method(self):
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        self.sample_dir = self.raw_dir / "kentucky_summative_assessment"
        self.sample_dir.mkdir(parents=True)

    def teardown_method(self):
        shutil.rmtree(self.test_dir)

    def create_grade_file(self):
        df = pd.DataFrame(
            {
                "School Year": ["20232024", "20232024"],
                "County Name": ["FAYETTE", "FAYETTE"],
                "District Name": ["Fayette County", "Fayette County"],
                "School Name": ["Test School", "Test School"],
                "School Code": ["165101", "165101"],
                "Grade": ["Grade 10", "Grade 11"],
                "Subject": ["Mathematics", "Reading"],
                "Demographic": ["All Students", "All Students"],
                "Suppressed": ["N", "Y"],
                "Novice": [30, "*"],
                "Apprentice": [25, "*"],
                "Proficient": [35, "*"],
                "Distinguished": [10, "*"],
                "Proficient / Distinguished": [45, "*"],
            }
        )
        df.to_csv(self.sample_dir / "grade.csv", index=False)

    def create_level_file(self):
        df = pd.DataFrame(
            {
                "SCHOOL YEAR": ["20222023"],
                "COUNTY NAME": ["FAYETTE"],
                "DISTRICT NAME": ["Fayette County"],
                "SCHOOL NAME": ["Test School"],
                "SCHOOL CODE": ["165101"],
                "LEVEL": ["HS"],
                "SUBJECT": ["MA"],
                "DEMOGRAPHIC": ["All Students"],
                "SUPPRESSED": ["N"],
                "NOVICE": [20],
                "APPRENTICE": [30],
                "PROFICIENT": [40],
                "DISTINGUISHED": [10],
                "PROFICIENT/DISTINGUISHED": [50],
            }
        )
        df.to_csv(self.sample_dir / "level.csv", index=False)

    def test_pipeline_end_to_end(self):
        self.create_grade_file()
        self.create_level_file()
        config = {"derive": {"processing_date": "2024-07-22"}}
        transform(self.raw_dir, self.proc_dir, config)
        out_file = self.proc_dir / "kentucky_summative_assessment.csv"
        assert out_file.exists()
        df = pd.read_csv(out_file)
        assert not df.empty
        assert "mathematics_novice_rate_grade_10" in df["metric"].values
        assert "reading_novice_rate_grade_11" in df["metric"].values
        assert "mathematics_apprentice_rate_high" in df["metric"].values
        suppressed = df[df["suppressed"] == "Y"]
        assert suppressed["value"].isna().all()
