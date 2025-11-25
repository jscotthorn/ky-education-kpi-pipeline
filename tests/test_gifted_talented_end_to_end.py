"""
End-to-end tests for Gifted and Talented ETL module.

Tests the full pipeline with real data files.
"""
import pytest
from pathlib import Path
import pandas as pd
from etl.constants import KPI_COLUMNS
from etl.gifted_talented import transform


class TestGiftedTalentedEndToEnd:
    """End-to-end tests using real data."""

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        """Setup test directories."""
        self.test_dir = tmp_path
        self.raw_dir = Path(__file__).parent.parent / "data" / "raw"
        self.proc_dir = tmp_path / "processed"
        self.proc_dir.mkdir()

    def test_e2e_full_pipeline(self):
        """Test the full pipeline with real data files."""
        # Skip if data doesn't exist
        data_dir = self.raw_dir / "gifted_talented"
        if not data_dir.exists() or len(list(data_dir.glob("*.csv")) + list(data_dir.glob("*.CSV"))) == 0:
            pytest.skip("No real data available for e2e test")

        config = {
            "derive": {
                "processing_date": "2025-11-24",
                "data_quality_flag": "reviewed"
            }
        }

        transform(self.raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "gifted_talented.csv"
        assert output_file.exists(), "Output file should be created"

        df = pd.read_csv(output_file, dtype={'school_id': str})

        # Validate KPI format
        for col in KPI_COLUMNS:
            assert col in df.columns, f"Required KPI column '{col}' missing"

        # Validate expected metrics exist
        metrics = df['metric'].unique()
        expected_metric_prefixes = ['gifted_participation_count_']

        found_metrics = [m for m in metrics if any(m.startswith(prefix) for prefix in expected_metric_prefixes)]
        assert len(found_metrics) > 0, "Expected gifted participation metrics not found"

        # Validate years present
        years = df['year'].unique()
        assert len(years) >= 1, "Should have at least one year of data"

        # Validate value ranges
        non_suppressed = df[df['suppressed'] == 'N']
        if len(non_suppressed) > 0:
            values = pd.to_numeric(non_suppressed['value'], errors='coerce').dropna()
            if len(values) > 0:
                assert values.min() >= 0, "Counts should be non-negative"

    def test_e2e_demographics_present(self):
        """Test that demographics are properly processed."""
        data_dir = self.raw_dir / "gifted_talented"
        if not data_dir.exists() or len(list(data_dir.glob("*.csv")) + list(data_dir.glob("*.CSV"))) == 0:
            pytest.skip("No real data available for e2e test")

        config = {"derive": {"processing_date": "2025-11-24"}}

        transform(self.raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "gifted_talented.csv"
        df = pd.read_csv(output_file)

        # Verify demographics are present
        student_groups = df['student_group'].unique()
        assert 'All Students' in student_groups, "All Students group should be present"
        assert len(student_groups) > 1, "Should have multiple demographic groups"

    def test_e2e_grade_level_metrics(self):
        """Test that grade level metrics are extracted."""
        data_dir = self.raw_dir / "gifted_talented"
        if not data_dir.exists() or len(list(data_dir.glob("*.csv")) + list(data_dir.glob("*.CSV"))) == 0:
            pytest.skip("No real data available for e2e test")

        config = {"derive": {"processing_date": "2025-11-24"}}

        transform(self.raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "gifted_talented.csv"
        df = pd.read_csv(output_file)

        # Check that grade level metrics exist
        metrics = df['metric'].unique()
        grade_metrics = [m for m in metrics if 'grade_' in m]
        assert len(grade_metrics) > 0, "Grade level metrics should be present"

        # Check for specific grade levels
        expected_grades = ['grade_1', 'grade_5', 'grade_9']
        for grade in expected_grades:
            grade_metric = f'gifted_participation_count_{grade}'
            assert grade_metric in metrics, f"Expected metric {grade_metric} not found"

    def test_e2e_multiple_years(self):
        """Test that multiple years are processed."""
        data_dir = self.raw_dir / "gifted_talented"
        if not data_dir.exists() or len(list(data_dir.glob("*.csv")) + list(data_dir.glob("*.CSV"))) == 0:
            pytest.skip("No real data available for e2e test")

        config = {"derive": {"processing_date": "2025-11-24"}}

        transform(self.raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "gifted_talented.csv"
        df = pd.read_csv(output_file)

        # Check multiple years are present
        years = df['year'].unique()
        assert len(years) >= 2, "Should have at least 2 years of data"

        # Check for expected years (2021-2025)
        expected_years = [2021, 2022, 2023, 2024, 2025]
        for year in expected_years:
            if year in years:
                year_data = df[df['year'] == year]
                assert len(year_data) > 0, f"Year {year} should have data"

    def test_e2e_school_level_data(self):
        """Test that school-level data is present."""
        data_dir = self.raw_dir / "gifted_talented"
        if not data_dir.exists() or len(list(data_dir.glob("*.csv")) + list(data_dir.glob("*.CSV"))) == 0:
            pytest.skip("No real data available for e2e test")

        config = {"derive": {"processing_date": "2025-11-24"}}

        transform(self.raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "gifted_talented.csv"
        df = pd.read_csv(output_file, dtype={'school_id': str})

        # Check for school-level records
        unique_schools = df['school_id'].nunique()
        assert unique_schools >= 1, "Should have at least one school/district"

        # Check district totals are present
        district_totals = df[df['school_name'].str.contains('Total|All Schools', na=False)]
        assert len(district_totals) > 0, "Should have district totals"

    def test_e2e_source_file_tracking(self):
        """Test that source file tracking is working correctly."""
        data_dir = self.raw_dir / "gifted_talented"
        if not data_dir.exists() or len(list(data_dir.glob("*.csv")) + list(data_dir.glob("*.CSV"))) == 0:
            pytest.skip("No real data available for e2e test")

        config = {"derive": {"processing_date": "2025-11-24"}}

        transform(self.raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "gifted_talented.csv"
        df = pd.read_csv(output_file)

        # Check source files are tracked
        source_files = df['source_file'].unique()
        assert len(source_files) >= 1, "Should track source files"

        # Verify expected source file patterns
        has_grade_level = any('grade_level' in sf.lower() or 'gifted_and_talented' in sf.lower() for sf in source_files)
        assert has_grade_level, "Should have grade level or historical source files"
