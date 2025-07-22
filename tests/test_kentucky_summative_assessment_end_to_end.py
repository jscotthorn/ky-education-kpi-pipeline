"""End-to-end tests for Kentucky Summative Assessment pipeline"""
import shutil
import tempfile
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

    def create_data(self):
        return pd.DataFrame({
            "School Year": ["20232024", "20232024"],
            "County Name": ["ADAIR", "ADAIR"],
            "District Name": ["Adair County", "Adair County"],
            "School Name": ["All Schools", "All Schools"],
            "School Code": ["001000", "001000"],
            "Level": ["Elementary School", "Elementary School"],
            "Subject": ["Mathematics", "Mathematics"],
            "Demographic": ["All Students", "Hispanic"],
            "Suppressed": ["N", "Y"],
            "Novice": [28, "*"],
            "Apprentice": [32, "*"],
            "Proficient": [31, "*"],
            "Distinguished": [10, "*"],
            "Proficient / Distinguished": [41, "*"],
            "Content Index": [58.7, "*"],
        })

    def create_grade_data(self):
        return pd.DataFrame({
            "SCHOOL YEAR": ["20222023"],
            "COUNTY NAME": ["ADAIR"],
            "DISTRICT NAME": ["Adair County"],
            "SCHOOL NAME": ["All Schools"],
            "SCHOOL CODE": ["001000"],
            "GRADE": ["03"],
            "SUBJECT": ["MA"],
            "DEMOGRAPHIC": ["All Students"],
            "SUPPRESSED": ["N"],
            "NOVICE": [28],
            "APPRENTICE": [32],
            "PROFICIENT": [31],
            "DISTINGUISHED": [10],
            "PROFICIENT/DISTINGUISHED": [41],
        })

    def test_end_to_end(self):
        df = self.create_data()
        sample_file = self.sample_dir / "sample.csv"
        df.to_csv(sample_file, index=False)
        transform(self.raw_dir, self.proc_dir, {"derive": {}})
        output = self.proc_dir / "kentucky_summative_assessment.csv"
        assert output.exists()
        df_out = pd.read_csv(output)
        assert not df_out.empty
        assert set(df_out["suppressed"].unique()).issubset({"Y", "N"})

    def test_grade_and_level(self):
        df_level = self.create_data().head(1)
        df_grade = self.create_grade_data()
        df_level.to_csv(self.sample_dir / "level.csv", index=False)
        df_grade.to_csv(self.sample_dir / "grade.csv", index=False)
        transform(self.raw_dir, self.proc_dir, {"derive": {}})
        output = self.proc_dir / "kentucky_summative_assessment.csv"
        assert output.exists()
        df_out = pd.read_csv(output)
        metrics = df_out["metric"].unique()
        assert any("grade_3" in m for m in metrics)
        assert any(m.endswith("elementary") for m in metrics)
