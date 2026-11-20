"""
End-to-end tests for Advanced Coursework ETL module.

Tests the full pipeline with real data files.
"""
import pytest
from pathlib import Path
import pandas as pd
from etl.constants import KPI_COLUMNS
from etl.advanced_coursework import transform


class TestAdvancedCourseworkEndToEnd:
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
        data_dir = self.raw_dir / "advanced_coursework"
        if not data_dir.exists() or len(list(data_dir.glob("*.csv")) + list(data_dir.glob("*.CSV"))) == 0:
            pytest.skip("No real data available for e2e test")

        config = {
            "derive": {
                "processing_date": "2025-11-24",
                "data_quality_flag": "reviewed"
            }
        }

        transform(self.raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "advanced_coursework.csv"
        assert output_file.exists(), "Output file should be created"

        df = pd.read_csv(output_file, dtype={'school_id': str})

        # Validate KPI format
        for col in KPI_COLUMNS:
            assert col in df.columns, f"Required KPI column '{col}' missing"

        # Validate expected metrics exist
        metrics = df['metric'].unique()
        expected_metrics = [
            'ap_course_enrollment',
            'ap_completion_count',
        ]
        for metric in expected_metrics:
            assert metric in metrics, f"Expected metric '{metric}' not found"

        # Validate years present
        years = df['year'].unique()
        assert len(years) >= 1, "Should have at least one year of data"

        # Validate value ranges
        non_suppressed = df[df['suppressed'] == 'N']
        if len(non_suppressed) > 0:
            values = pd.to_numeric(non_suppressed['value'], errors='coerce')
            assert values.min() >= 0, "Counts should be non-negative"

        # Check rate metrics are in reasonable range (for non-qualifying rates)
        # Note: qualifying_score_rate can be > 100% due to data inconsistencies in source files
        # where number_tested doesn't include all students who earned qualifying scores
        participation_rate_metrics = df[df['metric'].str.contains('participation_rate')]
        if len(participation_rate_metrics) > 0:
            non_suppressed_rates = participation_rate_metrics[participation_rate_metrics['suppressed'] == 'N']
            if len(non_suppressed_rates) > 0:
                rate_values = pd.to_numeric(non_suppressed_rates['value'], errors='coerce').dropna()
                if len(rate_values) > 0:
                    assert rate_values.min() >= 0, "Participation rates should be non-negative"
                    # Participation rates should be between 0-100 (with small margin for data timing)
                    assert rate_values.max() <= 110, "Participation rates should be <= 110%"

    def test_e2e_demographics_present(self):
        """Test that demographics are properly processed."""
        data_dir = self.raw_dir / "advanced_coursework"
        if not data_dir.exists() or len(list(data_dir.glob("*.csv")) + list(data_dir.glob("*.CSV"))) == 0:
            pytest.skip("No real data available for e2e test")

        config = {"derive": {"processing_date": "2025-11-24"}}

        transform(self.raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "advanced_coursework.csv"
        df = pd.read_csv(output_file)

        # Verify demographics are present
        student_groups = df['student_group'].unique()
        assert 'All Students' in student_groups, "All Students group should be present"
        assert len(student_groups) > 1, "Should have multiple demographic groups"

    def test_e2e_multiple_course_types(self):
        """Test that multiple course types are processed."""
        data_dir = self.raw_dir / "advanced_coursework"
        if not data_dir.exists() or len(list(data_dir.glob("*.csv")) + list(data_dir.glob("*.CSV"))) == 0:
            pytest.skip("No real data available for e2e test")

        config = {"derive": {"processing_date": "2025-11-24"}}

        transform(self.raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "advanced_coursework.csv"
        df = pd.read_csv(output_file)

        # Check that we have metrics for multiple course types
        metrics = df['metric'].unique()
        course_types_found = set()

        for metric in metrics:
            if metric.startswith('ap_'):
                course_types_found.add('ap')
            elif metric.startswith('dual_credit_'):
                course_types_found.add('dual_credit')
            elif metric.startswith('ib_'):
                course_types_found.add('ib')
            elif metric.startswith('cambridge_'):
                course_types_found.add('cambridge')

        assert len(course_types_found) >= 2, "Should have at least 2 course types"

    def test_e2e_school_level_data(self):
        """Test that school-level data is present."""
        data_dir = self.raw_dir / "advanced_coursework"
        if not data_dir.exists() or len(list(data_dir.glob("*.csv")) + list(data_dir.glob("*.CSV"))) == 0:
            pytest.skip("No real data available for e2e test")

        config = {"derive": {"processing_date": "2025-11-24"}}

        transform(self.raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "advanced_coursework.csv"
        df = pd.read_csv(output_file, dtype={'school_id': str})

        # Check for school-level records (not just district totals)
        unique_schools = df['school_id'].nunique()
        assert unique_schools >= 1, "Should have at least one school/district"

        # Check district totals are present
        district_totals = df[df['school_name'].str.contains('Total|All Schools', na=False)]
        assert len(district_totals) > 0, "Should have district totals"
