"""
End-to-end validation test for students taught by ineffective teachers ETL pipeline.
Tests that sample rows from each source file are correctly transformed to KPI format.
"""
import pytest
import pandas as pd
import numpy as np
import yaml
from etl.constants import KPI_COLUMNS
from pathlib import Path
from etl.students_taught_by_ineffective_teachers import (
    transform, StudentsTaughtByIneffectiveTeachersETL
)

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "students_taught_by_ineffective_teachers"


class TestStudentsTaughtByIneffectiveTeachersEndToEnd:
    """Test complete transformation from raw data to KPI format."""

    def setup_method(self):
        """Setup ETL instance for testing."""
        self.etl = StudentsTaughtByIneffectiveTeachersETL('students_taught_by_ineffective_teachers')

    def test_source_to_kpi_transformation(self):
        """Test that 10 random rows from each source file are correctly represented in processed file."""
        # Paths to actual data files
        raw_data_dir = Path("data/raw/students_taught_by_ineffective_teachers")
        processed_file = Path("data/processed/students_taught_by_ineffective_teachers.csv")

        # Ensure processed file exists
        if not processed_file.exists():
            pytest.skip("Processed students_taught_by_ineffective_teachers.csv not found. Run ETL pipeline first.")

        # Load processed KPI data
        kpi_df = pd.read_csv(processed_file)

        # Test each source file
        for source_file in raw_data_dir.glob("*.csv"):
            print(f"\nTesting {source_file.name}...")
            self._test_source_file_transformation(source_file, kpi_df)

    def _test_source_file_transformation(self, source_file: Path, kpi_df: pd.DataFrame):
        """Test transformation of a single source file."""
        # Load source data
        source_df = pd.read_csv(source_file, encoding='utf-8-sig')

        if source_df.empty:
            pytest.skip(f"Source file {source_file.name} is empty")

        # Filter to rows with Title I status
        title_i_col = None
        for col in source_df.columns:
            if 'title' in col.lower() and 'status' in col.lower():
                title_i_col = col
                break

        if title_i_col is None:
            pytest.skip(f"Source file {source_file.name} has no Title I status column")

        has_data = source_df[title_i_col].isin(['Title 1', 'Title I', 'Not Title 1', 'Not Title I', 'Equity Gap'])
        source_df_with_data = source_df[has_data]

        if source_df_with_data.empty:
            pytest.skip(f"Source file {source_file.name} has no rows with Title I data")

        # Take 10 random rows (or all if less than 10)
        sample_size = min(10, len(source_df_with_data))
        sample_rows = source_df_with_data.sample(n=sample_size, random_state=42)

        print(f"Testing {sample_size} rows from {source_file.name}")

        # Process sample rows through the same transformation pipeline
        processed_sample = self._process_sample_rows(sample_rows, source_file.name)

        if processed_sample.empty:
            print(f"  No KPI rows produced (likely all suppressed)")
            return

        # Validate each processed row exists in KPI data
        for _, expected_row in processed_sample.iterrows():
            self._validate_kpi_row_exists(expected_row, kpi_df, source_file.name)

    def _process_sample_rows(self, sample_df: pd.DataFrame, source_filename: str) -> pd.DataFrame:
        """Process sample rows through the transformation pipeline."""
        # Apply the same transformations as the ETL pipeline
        df = sample_df.copy()

        # Apply normalization
        df = self.etl.normalize_column_names(df)
        df = self.etl.standardize_missing_values(df)

        # Add source file for tracking
        df['source_file'] = source_filename

        # Convert to KPI format
        kpi_df = self.etl.convert_to_kpi_format(df, source_filename)

        return kpi_df

    def _validate_kpi_row_exists(self, expected_row: pd.Series, kpi_df: pd.DataFrame, source_filename: str):
        """Validate that a specific KPI row exists in the processed data."""
        # Use direct matching for reliability
        matching_rows = self._direct_match(expected_row, kpi_df)

        # Assert that we found matching rows
        assert len(matching_rows) > 0, (
            f"No KPI row found for {source_filename}\n"
            f"Expected: school_id={expected_row['school_id']}, year={expected_row['year']}, "
            f"student_group={expected_row['student_group']}, metric={expected_row['metric']}\n"
            f"Source file: {expected_row.get('source_file', 'unknown')}"
        )

        # Validate the value is correct (within tolerance for floating point)
        if 'value' in expected_row and pd.notna(expected_row['value']):
            expected_value = float(expected_row['value'])
            actual_values = matching_rows['value'].values

            # Check if any matching row has the expected value (within tolerance)
            value_match = any(abs(actual_val - expected_value) < 0.01 for actual_val in actual_values if pd.notna(actual_val))

            assert value_match, (
                f"Value mismatch for {source_filename}. "
                f"Expected value: {expected_value}, "
                f"Actual values: {actual_values}"
            )

    def _direct_match(self, expected_row: pd.Series, kpi_df: pd.DataFrame) -> pd.DataFrame:
        """Direct matching on key fields."""
        matches = kpi_df

        # Match on school_id
        if 'school_id' in expected_row and pd.notna(expected_row['school_id']):
            school_id_str = str(expected_row['school_id'])
            matches = matches[matches['school_id'].astype(str) == school_id_str]

        # Match on year (handle both string and int)
        if 'year' in expected_row and pd.notna(expected_row['year']):
            year_val = str(expected_row['year'])
            matches = matches[matches['year'].astype(str) == year_val]

        # Match on student_group
        if 'student_group' in expected_row and pd.notna(expected_row['student_group']):
            matches = matches[matches['student_group'] == expected_row['student_group']]

        # Match on metric
        if 'metric' in expected_row and pd.notna(expected_row['metric']):
            matches = matches[matches['metric'] == expected_row['metric']]

        # Match on source_file
        if 'source_file' in expected_row and pd.notna(expected_row['source_file']):
            matches = matches[matches['source_file'] == expected_row['source_file']]

        return matches

    def test_all_expected_metrics_present(self):
        """Test that all ineffective teacher metrics are present in the processed data."""
        processed_file = Path("data/processed/students_taught_by_ineffective_teachers.csv")

        if not processed_file.exists():
            pytest.skip("Processed students_taught_by_ineffective_teachers.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        # Expected metrics
        expected_metrics = [
            'students_taught_by_ineffective_teachers_rate_title_1',
            'students_taught_by_ineffective_teachers_rate_not_title_1',
        ]

        actual_metrics = set(kpi_df['metric'].unique())

        for expected_metric in expected_metrics:
            assert expected_metric in actual_metrics, f"Missing expected metric: {expected_metric}"

        print(f"✓ All {len(expected_metrics)} expected ineffective teacher metrics found")
        print(f"  Actual metrics: {sorted(actual_metrics)}")

    def test_demographic_coverage(self):
        """Test that all expected demographics are present in the student_group column."""
        processed_file = Path("data/processed/students_taught_by_ineffective_teachers.csv")

        if not processed_file.exists():
            pytest.skip("Processed students_taught_by_ineffective_teachers.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        # Expected demographics
        expected_demographics = [
            'All Students',
            'White (non-Hispanic)',
            'Non-White',
            'Economically Disadvantaged',
            'Non-Economically Disadvantaged',
            'Students with Disabilities (IEP)',
            'Student without Disabilities (IEP)',
            'English Learner',
            'Non-English Learner'
        ]

        actual_demographics = set(kpi_df['student_group'].unique())

        for demo in expected_demographics:
            assert demo in actual_demographics, f"Missing expected demographic: {demo}"

        print(f"✓ All {len(expected_demographics)} expected demographics found")
        print(f"  Total unique demographics: {len(actual_demographics)}")

    def test_value_ranges(self):
        """Test that ineffective teacher rates are within reasonable ranges."""
        processed_file = Path("data/processed/students_taught_by_ineffective_teachers.csv")

        if not processed_file.exists():
            pytest.skip("Processed students_taught_by_ineffective_teachers.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)
        values = kpi_df['value'].dropna()

        # For non-gap metrics, rates should generally be 0-100
        non_gap_df = kpi_df[~kpi_df['metric'].str.contains('equity_gap')]
        non_gap_values = non_gap_df['value'].dropna()

        assert (non_gap_values >= 0).all(), "Found negative ineffective teacher rates"

        # Note: Values above 100% may occur in source data. Flag for awareness but don't exclude.
        outliers_above_100 = (non_gap_values > 100).sum()
        if outliers_above_100 > 0:
            print(f"ℹ Found {outliers_above_100} ineffective teacher rates above 100% (source data anomaly)")
            print(f"  Max value: {non_gap_values.max():.1f}%")

        print(f"✓ Non-gap value range: {non_gap_values.min():.1f}% - {non_gap_values.max():.1f}%")

        # Equity gap can be negative (lower rate in Title 1 vs non-Title 1)
        gap_df = kpi_df[kpi_df['metric'].str.contains('equity_gap')]
        if not gap_df.empty:
            gap_values = gap_df['value'].dropna()
            print(f"✓ Equity gap range: {gap_values.min():.1f}% - {gap_values.max():.1f}%")

    def test_required_kpi_columns(self):
        """Test that all required KPI columns are present."""
        processed_file = Path("data/processed/students_taught_by_ineffective_teachers.csv")

        if not processed_file.exists():
            pytest.skip("Processed students_taught_by_ineffective_teachers.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        for required_col in KPI_COLUMNS:
            assert required_col in kpi_df.columns, f"Missing required KPI column: {required_col}"

        print(f"✓ All {len(KPI_COLUMNS)} required KPI columns present")

    def test_years_coverage(self):
        """Test that data from all expected years is present."""
        processed_file = Path("data/processed/students_taught_by_ineffective_teachers.csv")

        if not processed_file.exists():
            pytest.skip("Processed students_taught_by_ineffective_teachers.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        # Expected years. 2020 is the 2019-20 file. 2026 is KYRC26.
        expected_years = {'2020', '2021', '2022', '2023', '2024', '2025', '2026'}
        actual_years = set(kpi_df['year'].astype(str).unique())

        for year in expected_years:
            assert year in actual_years, f"Missing data for year: {year}"

        print(f"✓ Data present for all {len(expected_years)} expected years: {sorted(actual_years)}")

    def test_title_i_stratification(self):
        """Test that data is properly stratified by Title I status."""
        processed_file = Path("data/processed/students_taught_by_ineffective_teachers.csv")

        if not processed_file.exists():
            pytest.skip("Processed students_taught_by_ineffective_teachers.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        # Check that both Title 1 and Not Title 1 metrics exist
        metrics = kpi_df['metric'].unique()

        has_title_1 = any('title_1' in m and 'not_title_1' not in m for m in metrics)
        has_not_title_1 = any('not_title_1' in m for m in metrics)

        assert has_title_1, "Missing Title 1 metrics"
        assert has_not_title_1, "Missing Not Title 1 metrics"

        # Count rows by metric type
        title_1_count = len(kpi_df[kpi_df['metric'].str.contains('title_1') & ~kpi_df['metric'].str.contains('not_title_1')])
        not_title_1_count = len(kpi_df[kpi_df['metric'].str.contains('not_title_1')])

        print(f"✓ Title I stratification validated:")
        print(f"  Title 1 rows: {title_1_count:,}")
        print(f"  Not Title 1 rows: {not_title_1_count:,}")


def _run_fixtures(tmp_path: Path, filenames: list) -> pd.DataFrame:
    """Copy committed real-row excerpts into a raw directory and transform them."""
    raw_dir = tmp_path / "raw"
    source_dir = raw_dir / "students_taught_by_ineffective_teachers"
    source_dir.mkdir(parents=True)
    proc_dir = tmp_path / "processed"
    proc_dir.mkdir()
    for name in filenames:
        (source_dir / name).write_bytes((FIXTURE_DIR / name).read_bytes())
    config = yaml.safe_load((ROOT / "config" / "mappings.yaml").read_text())
    transform(raw_dir, proc_dir, config["sources"]["students_taught_by_ineffective_teachers"])
    output = proc_dir / "students_taught_by_ineffective_teachers.csv"
    assert output.exists(), filenames
    return pd.read_csv(output, dtype=str)


def _comparable(df: pd.DataFrame) -> pd.DataFrame:
    """Drop the processing timestamp and sort so two runs can be compared."""
    columns = [column for column in df.columns if column != "last_updated"]
    return df[columns].sort_values(columns).reset_index(drop=True)


def _one(df: pd.DataFrame, year: str, school_id: str, metric: str, group: str = "All Students") -> pd.Series:
    matches = df[
        (df["year"] == year)
        & (df["school_id"] == school_id)
        & (df["metric"] == metric)
        & (df["student_group"] == group)
    ]
    assert len(matches) == 1, (year, school_id, metric, group, len(matches))
    row = matches.iloc[0]
    assert row["suppressed"] == "N"
    return row


def test_kyrc26_real_rows_and_prior_year_anchors(tmp_path):
    """2025-26 KPIs from real rows, with earlier-year anchors.

    Values are the published percents. State Title I All Students is 0.5
    and Not Title I is 0.1. English learners in Title I schools are the
    published 0.0. KPI year is 2026, from school year 20252026. District
    is All Districts. There is no Suppressed column and no starred cell.
    The text N/A and #N/A emit nothing. A published 0 is kept.
    """
    names = [
        "students_taught_by_ineffective_teachers_2020.csv",
        "students_taught_by_ineffective_teachers_2021.csv",
        "students_taught_by_ineffective_teachers_2022.csv",
        "students_taught_by_ineffective_teachers_2023.csv",
        "KYRC24_OVW_Students_Taught_by_Ineffective_Teachers.csv",
        "KYRC25_OVW_Students_Taught_by_Ineffective_Teachers.csv",
    ]
    prior_only = _run_fixtures(tmp_path / "prior", names)
    combined = _run_fixtures(tmp_path / "combined", names + [
        "KYRC26_OVW_Students_Taught_by_Ineffective_Teachers.csv",
    ])
    prior_years = ["2020", "2021", "2022", "2023", "2024", "2025"]
    prior_slice = combined[combined["year"].isin(prior_years)]
    assert _comparable(prior_slice).equals(_comparable(prior_only))

    kyrc26 = combined[combined["source_file"].str.startswith("KYRC26_")]
    assert set(kyrc26["year"]) == {"2026"}
    assert set(kyrc26["suppressed"]) == {"N"}

    state = _one(combined, "2026", "999000", "students_taught_by_ineffective_teachers_rate_title_1")
    assert float(state["value"]) == 0.5
    assert state["district"] == "All Districts"
    assert state["school_name"] == "---District Total---"
    assert state["district_number"] == "999"
    groups = {
        "Non-White": 0.2,
        "White (non-Hispanic)": 0.4,
        "Economically Disadvantaged": 0.3,
        "Non-Economically Disadvantaged": 0.2,
        "Students with Disabilities (IEP)": 0.1,
        "Student without Disabilities (IEP)": 0.4,
        "English Learner": 0.0,
        "Non-English Learner": 0.4,
    }
    for group, value in groups.items():
        row = _one(
            combined, "2026", "999000",
            "students_taught_by_ineffective_teachers_rate_title_1", group,
        )
        assert float(row["value"]) == value

    not_title = _one(
        combined, "2026", "999000",
        "students_taught_by_ineffective_teachers_rate_not_title_1",
    )
    assert float(not_title["value"]) == 0.1
    assert float(_one(
        combined, "2026", "999000",
        "students_taught_by_ineffective_teachers_rate_not_title_1", "Non-White",
    )["value"]) == 0.0

    fayette = _one(
        combined, "2026", "165000", "students_taught_by_ineffective_teachers_rate_title_1",
    )
    assert float(fayette["value"]) == 1.2
    assert fayette["district"] == "Fayette County"
    assert fayette["nces_id"] == "2101860"
    assert fayette["co_op"] == "CKEC"
    assert fayette["co_op_code"] == "901"
    assert fayette["county_name"] == "FAYETTE"
    assert fayette["district_number"] == "165"
    assert float(_one(
        combined, "2026", "165000",
        "students_taught_by_ineffective_teachers_rate_not_title_1",
    )["value"]) == 0.0

    bryan = _one(
        combined, "2026", "165170", "students_taught_by_ineffective_teachers_rate_title_1",
    )
    assert float(bryan["value"]) == 0.0
    assert bryan["school_name"] == "Bryan Station High School"
    assert bryan["state_school_id"] == "034165170"
    assert bryan["nces_id"] == "210186000388"
    assert bryan["school_type"] == "A1"

    adair_high = _one(
        combined, "2026", "001010", "students_taught_by_ineffective_teachers_rate_title_1",
    )
    assert float(adair_high["value"]) == 0.0
    assert adair_high["nces_id"] == "210003000001"

    # Prior-year anchors from the downloaded files. KYRC25 Bryan Station's
    # NCES id is the published 210186000000. It is not rewritten.
    assert float(_one(
        combined, "2025", "999000", "students_taught_by_ineffective_teachers_rate_title_1",
    )["value"]) == 0.8
    assert float(_one(
        combined, "2025", "999000", "students_taught_by_ineffective_teachers_rate_not_title_1",
    )["value"]) == 0.0
    assert float(_one(
        combined, "2025", "165000", "students_taught_by_ineffective_teachers_rate_title_1",
    )["value"]) == 4.3
    assert _one(
        combined, "2025", "165170", "students_taught_by_ineffective_teachers_rate_title_1",
    )["nces_id"] == "210186000000"
    adair_2025 = combined[
        (combined["year"] == "2025")
        & (combined["school_id"] == "001000")
        & (combined["metric"] == "students_taught_by_ineffective_teachers_rate_title_1")
        & (combined["student_group"] == "All Students")
    ]
    assert len(adair_2025) == 1
    assert float(adair_2025["value"].iloc[0]) == 0.0

    assert float(_one(
        combined, "2024", "999000", "students_taught_by_ineffective_teachers_rate_title_1",
    )["value"]) == 0.7
    non_white_2024 = combined[
        (combined["year"] == "2024")
        & (combined["school_id"] == "999000")
        & (combined["student_group"] == "Non-White")
    ]
    assert non_white_2024.empty
    bath = combined[(combined["year"] == "2024") & (combined["school_id"] == "025000")]
    assert float(_one(
        combined, "2024", "025000",
        "students_taught_by_ineffective_teachers_rate_not_title_1",
    )["value"]) == 0.0
    assert "White (non-Hispanic)" not in set(bath["student_group"])

    state_2023 = _one(
        combined, "2023", "999", "students_taught_by_ineffective_teachers_rate_title_1",
    )
    assert float(state_2023["value"]) == 0.88
    assert state_2023["district"] == "State"
    assert state_2023["school_name"] == "---State Total---"
    assert float(_one(
        combined, "2023", "999", "students_taught_by_ineffective_teachers_rate_not_title_1",
    )["value"]) == 0.04
    gap_2023 = combined[
        (combined["year"] == "2023")
        & (combined["school_id"] == "999")
        & (combined["metric"] == "students_taught_by_ineffective_teachers_rate_equity_gap")
    ]
    assert gap_2023.empty
    breathitt = _one(
        combined, "2023", "061", "students_taught_by_ineffective_teachers_rate_title_1",
    )
    assert float(breathitt["value"]) == 0.0
    assert breathitt["district"] == "Breathitt County"
    assert "012012" not in set(combined["school_id"])
    shelby = _one(
        combined, "2023", "275610", "students_taught_by_ineffective_teachers_rate_title_1",
    )
    assert float(shelby["value"]) == 0.91
    shelby_rows = combined[combined["school_id"] == "275610"]
    assert "White (non-Hispanic)" not in set(shelby_rows["student_group"])
    assert float(_one(
        combined, "2023", "275610",
        "students_taught_by_ineffective_teachers_rate_title_1", "Non-White",
    )["value"]) == 20.45

    assert float(_one(
        combined, "2022", "999", "students_taught_by_ineffective_teachers_rate_title_1",
    )["value"]) == 0.69
    assert float(_one(
        combined, "2022", "999", "students_taught_by_ineffective_teachers_rate_not_title_1",
    )["value"]) == 0.30
    assert float(_one(
        combined, "2021", "999", "students_taught_by_ineffective_teachers_rate_title_1",
    )["value"]) == 0.43
    assert "011030" not in set(combined["school_id"])

    state_2020 = _one(
        combined, "2020", "999", "students_taught_by_ineffective_teachers_rate_title_1",
    )
    assert float(state_2020["value"]) == 0.29
    assert state_2020["district"] == "State"
    assert state_2020["school_name"] == "---State Total---"
    assert state_2020["school_id"] == "999"
    gap = _one(
        combined, "2020", "999",
        "students_taught_by_ineffective_teachers_rate_equity_gap", "Non-White",
    )
    assert float(gap["value"]) == 0.09
    gap_all = combined[
        (combined["year"] == "2020")
        & (combined["school_id"] == "999")
        & (combined["metric"] == "students_taught_by_ineffective_teachers_rate_equity_gap")
        & (combined["student_group"] == "All Students")
    ]
    assert gap_all.empty
    sunshine = combined[combined["school_id"] == "236021"]
    assert set(sunshine["student_group"]) == {"Students with Disabilities (IEP)"}
    assert float(sunshine["value"].iloc[0]) == 0.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
