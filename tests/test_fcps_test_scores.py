"""
Tests for FCPS Test Scores ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
import shutil
from etl.fcps_test_scores import transform


class TestFCPSTestScores:
    
    def setup_method(self):
        """Setup test directories and sample data."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        
        # Create sample raw data
        sample_dir = self.raw_dir / "fcps_test_scores" / "20240101"
        sample_dir.mkdir(parents=True)
        
        sample_data = pd.DataFrame({
            "Student ID": ["001", "002", "003"],
            "Test Score": [85, 92, 78],
            "Subject": ["Math", "Reading", "Math"],
            "Grade Level": [3, 4, 3]
        })
        sample_data.to_csv(sample_dir / "test_scores.csv", index=False)
    
    def teardown_method(self):
        """Clean up test directories."""
        shutil.rmtree(self.test_dir)
    
    def test_transform_basic(self):
        """Test basic transform functionality."""
        config = {
            "rename": {
                "Student ID": "student_id",
                "Test Score": "score",
                "Subject": "subject",
                "Grade Level": "grade"
            },
            "dtype": {
                "student_id": "str",
                "score": "float64",
                "grade": "int64"
            },
            "derive": {
                "school_year": "2023-24"
            }
        }
        
        transform(self.raw_dir, self.proc_dir, config)
        
        # Check output file exists
        output_file = self.proc_dir / "fcps_test_scores.csv"
        assert output_file.exists()
        
        # Check data transformations
        df = pd.read_csv(output_file)
        assert "student_id" in df.columns
        assert "score" in df.columns
        assert "school_year" in df.columns
        assert df["school_year"].iloc[0] == "2023-24"
        assert len(df) == 3
    
    def test_transform_no_data(self):
        """Test transform when no raw data exists."""
        empty_raw_dir = self.test_dir / "empty_raw"
        empty_raw_dir.mkdir()
        
        config = {}
        transform(empty_raw_dir, self.proc_dir, config)
        
        # Should not create output file
        output_file = self.proc_dir / "fcps_test_scores.csv"
        assert not output_file.exists()