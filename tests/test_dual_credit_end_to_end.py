"""
End-to-end tests for Dual Credit ETL module.

Tests the full pipeline with real data files.
"""
import pytest
from pathlib import Path
import pandas as pd
from etl.constants import KPI_COLUMNS
from etl.dual_credit import transform


class TestDualCreditEndToEnd:
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
        data_dir = self.raw_dir / "dual_credit"
        if not data_dir.exists() or len(list(data_dir.glob("*.csv")) + list(data_dir.glob("*.CSV"))) == 0:
            pytest.skip("No real data available for e2e test")

        config = {
            "derive": {
                "processing_date": "2025-11-24",
                "data_quality_flag": "reviewed"
            }
        }

        transform(self.raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "dual_credit.csv"
        assert output_file.exists(), "Output file should be created"

        df = pd.read_csv(output_file, dtype={'school_id': str})

        # Validate KPI format
        for col in KPI_COLUMNS:
            assert col in df.columns, f"Required KPI column '{col}' missing"

        # Validate expected metrics exist
        metrics = df['metric'].unique()
        expected_metrics = [
            'dual_credit_enrollment',
            'dual_credit_completion_count',
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

    def test_e2e_demographics_present(self):
        """Test that demographics are properly processed."""
        data_dir = self.raw_dir / "dual_credit"
        if not data_dir.exists() or len(list(data_dir.glob("*.csv")) + list(data_dir.glob("*.CSV"))) == 0:
            pytest.skip("No real data available for e2e test")

        config = {"derive": {"processing_date": "2025-11-24"}}

        transform(self.raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "dual_credit.csv"
        df = pd.read_csv(output_file)

        # Verify demographics are present
        student_groups = df['student_group'].unique()
        assert 'All Students' in student_groups, "All Students group should be present"
        assert len(student_groups) > 1, "Should have multiple demographic groups"

    def test_e2e_completion_rate_calculated(self):
        """Test that completion rate is calculated correctly."""
        data_dir = self.raw_dir / "dual_credit"
        if not data_dir.exists() or len(list(data_dir.glob("*.csv")) + list(data_dir.glob("*.CSV"))) == 0:
            pytest.skip("No real data available for e2e test")

        config = {"derive": {"processing_date": "2025-11-24"}}

        transform(self.raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "dual_credit.csv"
        df = pd.read_csv(output_file)

        # Check that completion rate metric exists
        completion_rate = df[df['metric'] == 'dual_credit_completion_rate']
        assert len(completion_rate) > 0, "Completion rate should be calculated"

        # Verify rates are reasonable (0-100%)
        if len(completion_rate) > 0:
            non_suppressed = completion_rate[completion_rate['suppressed'] == 'N']
            if len(non_suppressed) > 0:
                rates = pd.to_numeric(non_suppressed['value'], errors='coerce').dropna()
                if len(rates) > 0:
                    assert rates.min() >= 0, "Completion rates should be >= 0"
                    assert rates.max() <= 100, "Completion rates should be <= 100"

    def test_e2e_school_level_data(self):
        """Test that school-level data is present."""
        data_dir = self.raw_dir / "dual_credit"
        if not data_dir.exists() or len(list(data_dir.glob("*.csv")) + list(data_dir.glob("*.CSV"))) == 0:
            pytest.skip("No real data available for e2e test")

        config = {"derive": {"processing_date": "2025-11-24"}}

        transform(self.raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "dual_credit.csv"
        df = pd.read_csv(output_file, dtype={'school_id': str})

        # Check for school-level records
        unique_schools = df['school_id'].nunique()
        assert unique_schools >= 1, "Should have at least one school/district"

    def test_e2e_no_ap_or_ib_data(self):
        """Test that only dual credit data is extracted (no AP/IB)."""
        data_dir = self.raw_dir / "dual_credit"
        if not data_dir.exists() or len(list(data_dir.glob("*.csv")) + list(data_dir.glob("*.CSV"))) == 0:
            pytest.skip("No real data available for e2e test")

        config = {"derive": {"processing_date": "2025-11-24"}}

        transform(self.raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "dual_credit.csv"
        df = pd.read_csv(output_file)

        # Verify no AP/IB metrics are present
        metrics = df['metric'].unique()
        for metric in metrics:
            assert not metric.startswith('ap_'), f"AP metric {metric} should not be in dual credit output"
            assert not metric.startswith('ib_'), f"IB metric {metric} should not be in dual credit output"
