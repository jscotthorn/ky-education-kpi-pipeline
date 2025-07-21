from __future__ import annotations

from pathlib import Path
import tempfile
import shutil
from typing import Callable, Optional

import pandas as pd


class BaseEndToEndTest:
    """Shared utilities for end-to-end ETL tests."""

    KPI_COLUMNS = [
        "district",
        "school_id",
        "school_name",
        "year",
        "student_group",
        "metric",
        "value",
        "suppressed",
        "source_file",
        "last_updated",
    ]

    def setup_method(self) -> None:
        """Create temporary directories for raw and processed data."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)

    def teardown_method(self) -> None:
        """Remove temporary directories created for the test."""
        shutil.rmtree(self.test_dir)

    def write_raw(self, df: pd.DataFrame, relative_path: str) -> Path:
        """Write a dataframe to the raw directory and return the file path."""
        path = self.raw_dir / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False)
        return path

    def run_etl(
        self,
        transform_func: Callable[[Path, Path, dict], None],
        config: Optional[dict] = None,
    ) -> None:
        """Execute the provided transform function using the temp dirs."""
        if config is None:
            config = {"derive": {"processing_date": "2024-01-01"}}
        transform_func(self.raw_dir, self.proc_dir, config)

    def assert_kpi_format(self, df: pd.DataFrame) -> None:
        """Assert that the dataframe matches the standard KPI column format."""
        assert not df.empty, "Output should not be empty"
        assert len(df.columns) == len(self.KPI_COLUMNS), "Should have exactly 10 columns in KPI format"
        for col in self.KPI_COLUMNS:
            assert col in df.columns, f"Missing required column: {col}"
