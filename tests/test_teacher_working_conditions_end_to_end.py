"""
End-to-end validation test for teacher working conditions ETL pipeline.
Tests that sample rows from each source file are correctly transformed to KPI format.
"""
import pytest
import pandas as pd
import numpy as np
import yaml
from etl.constants import KPI_COLUMNS
from pathlib import Path
from etl.teacher_working_conditions import (
    transform, TeacherWorkingConditionsETL
)


class TestTeacherWorkingConditionsEndToEnd:
    """Test complete transformation from raw data to KPI format."""

    def setup_method(self):
        """Setup ETL instance for testing."""
        self.etl = TeacherWorkingConditionsETL('teacher_working_conditions')

    def test_source_to_kpi_transformation(self):
        """Test that 10 random rows from each source file are correctly represented in processed file."""
        # Paths to actual data files
        raw_data_dir = Path("data/raw/teacher_working_conditions")
        processed_file = Path("data/processed/teacher_working_conditions.csv")

        # Ensure processed file exists
        if not processed_file.exists():
            pytest.skip("Processed teacher_working_conditions.csv not found. Run ETL pipeline first.")

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

        # Filter to rows with valid impact measures
        impact_measure_col = None
        for col in source_df.columns:
            if 'impact' in col.lower() and 'measure' in col.lower():
                impact_measure_col = col
                break

        if impact_measure_col is None:
            pytest.skip(f"Source file {source_file.name} has no impact measure column")

        # Get valid impact measures
        valid_measures = list(self.etl.IMPACT_MEASURE_MAP.keys())
        has_valid_measure = source_df[impact_measure_col].isin(valid_measures)
        source_df_with_data = source_df[has_valid_measure]

        if source_df_with_data.empty:
            pytest.skip(f"Source file {source_file.name} has no rows with valid impact measures")

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

        # Match on metric
        if 'metric' in expected_row and pd.notna(expected_row['metric']):
            matches = matches[matches['metric'] == expected_row['metric']]

        # Match on source_file
        if 'source_file' in expected_row and pd.notna(expected_row['source_file']):
            matches = matches[matches['source_file'] == expected_row['source_file']]

        return matches

    def test_all_expected_metrics_present(self):
        """Test that all teacher working conditions metrics are present in the processed data."""
        processed_file = Path("data/processed/teacher_working_conditions.csv")

        if not processed_file.exists():
            pytest.skip("Processed teacher_working_conditions.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        # Expected metrics (at minimum the three KYRC24/25 measures)
        expected_metrics = [
            'teacher_working_conditions_managing_student_behavior',
            'teacher_working_conditions_school_climate',
            'teacher_working_conditions_school_leadership',
        ]

        actual_metrics = set(kpi_df['metric'].unique())

        for expected_metric in expected_metrics:
            assert expected_metric in actual_metrics, f"Missing expected metric: {expected_metric}"

        print(f"✓ All {len(expected_metrics)} expected teacher working conditions metrics found")
        print(f"  Actual metrics: {sorted(actual_metrics)}")

        # Teaching environment is only in historical data
        if 'teacher_working_conditions_teaching_environment' in actual_metrics:
            print(f"  ✓ Historical metric 'teaching_environment' also present")

    def test_all_student_group_all_students(self):
        """Test that all records have student_group = 'All Students' (school-level data only)."""
        processed_file = Path("data/processed/teacher_working_conditions.csv")

        if not processed_file.exists():
            pytest.skip("Processed teacher_working_conditions.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        unique_student_groups = kpi_df['student_group'].unique()
        assert len(unique_student_groups) == 1, f"Expected only 'All Students', found: {unique_student_groups}"
        assert unique_student_groups[0] == 'All Students', f"Expected 'All Students', found: {unique_student_groups[0]}"

        print(f"✓ All {len(kpi_df):,} records have student_group = 'All Students'")

    def test_value_ranges(self):
        """Test that teacher working conditions scores are within reasonable ranges."""
        processed_file = Path("data/processed/teacher_working_conditions.csv")

        if not processed_file.exists():
            pytest.skip("Processed teacher_working_conditions.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)
        values = kpi_df['value'].dropna()

        # Index scores should be 0-100 (percentage-like)
        assert (values >= 0).all(), "Found negative teacher working conditions scores"
        assert (values <= 100).all(), "Found teacher working conditions scores above 100"

        print(f"✓ Value range: {values.min():.1f} - {values.max():.1f}")
        print(f"  Mean: {values.mean():.1f}, Median: {values.median():.1f}")

    def test_required_kpi_columns(self):
        """Test that all required KPI columns are present."""
        processed_file = Path("data/processed/teacher_working_conditions.csv")

        if not processed_file.exists():
            pytest.skip("Processed teacher_working_conditions.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        for required_col in KPI_COLUMNS:
            assert required_col in kpi_df.columns, f"Missing required KPI column: {required_col}"

        print(f"✓ All {len(KPI_COLUMNS)} required KPI columns present")

    def test_years_coverage(self):
        """Test that data from all expected years is present."""
        processed_file = Path("data/processed/teacher_working_conditions.csv")

        if not processed_file.exists():
            pytest.skip("Processed teacher_working_conditions.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        # 2020 is the registered 2019-20 file. 2026 is KYRC26.
        expected_years = {'2020', '2021', '2022', '2023', '2024', '2025', '2026'}
        actual_years = set(kpi_df['year'].astype(str).unique())

        for year in expected_years:
            assert year in actual_years, f"Missing data for year: {year}"

        print(f"✓ Data present for all {len(expected_years)} expected years: {sorted(actual_years)}")

    def test_metrics_per_school(self):
        """Test that each school has multiple metrics (one per impact measure)."""
        processed_file = Path("data/processed/teacher_working_conditions.csv")

        if not processed_file.exists():
            pytest.skip("Processed teacher_working_conditions.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        # Group by school_id and year, count metrics
        metrics_per_school = kpi_df.groupby(['school_id', 'year'])['metric'].nunique()

        # Most schools should have at least 3 metrics (KYRC24/25) or 4 (historical with teaching environment)
        schools_with_multiple = (metrics_per_school >= 3).sum()
        total_school_years = len(metrics_per_school)

        print(f"✓ {schools_with_multiple:,} of {total_school_years:,} school-year combinations have 3+ metrics")
        print(f"  Metric counts: min={metrics_per_school.min()}, max={metrics_per_school.max()}, mean={metrics_per_school.mean():.1f}")

    def test_historical_teaching_environment(self):
        """Test that Teaching Environment metric is present in historical data."""
        processed_file = Path("data/processed/teacher_working_conditions.csv")

        if not processed_file.exists():
            pytest.skip("Processed teacher_working_conditions.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        # Teaching Environment Composite is the 2020-2023 label. KYRC24-26
        # publish School Climate instead, so those years do not have this metric.
        teaching_env = kpi_df[kpi_df['metric'] == 'teacher_working_conditions_teaching_environment']

        if len(teaching_env) > 0:
            teaching_env_years = set(teaching_env['year'].astype(str).unique())
            print(f"✓ Teaching Environment metric found in years: {sorted(teaching_env_years)}")

            assert teaching_env_years <= {'2020', '2021', '2022', '2023'}, (
                f"Teaching Environment should only be in 2020-2023, found in: {teaching_env_years}"
            )
        else:
            print("ℹ Teaching Environment metric not found (may not be in source data)")

    def test_district_totals_included(self):
        """Test that district-level totals are included."""
        processed_file = Path("data/processed/teacher_working_conditions.csv")

        if not processed_file.exists():
            pytest.skip("Processed teacher_working_conditions.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        # Check for district totals (school_name contains 'District Total' or 'All Schools')
        district_totals = kpi_df[kpi_df['school_name'].str.contains('District Total|All Schools', case=False, na=False)]

        assert len(district_totals) > 0, "No district-level totals found"

        print(f"✓ District-level totals: {len(district_totals):,} rows")

    def test_minimal_duplicate_records(self):
        """Test that duplicate records are minimal (source data may have some duplicates)."""
        processed_file = Path("data/processed/teacher_working_conditions.csv")

        if not processed_file.exists():
            pytest.skip("Processed teacher_working_conditions.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        # Check for duplicates on key columns
        duplicates = kpi_df.duplicated(subset=['school_id', 'year', 'metric', 'source_file'], keep=False)
        duplicate_count = duplicates.sum()

        # Note: Source data from KDE contains some duplicate rows (e.g., school 197 in 2021-2023)
        # We allow a small number of duplicates from source data quality issues
        max_allowed_duplicates = 30  # Small percentage of total rows

        if duplicate_count > 0:
            print(f"ℹ Found {duplicate_count} duplicate records (likely from source data)")
            dup_df = kpi_df[duplicates]
            print(f"  Affected schools: {dup_df['school_id'].unique()}")

        assert duplicate_count <= max_allowed_duplicates, (
            f"Found {duplicate_count} duplicate records, exceeds threshold of {max_allowed_duplicates}"
        )

        if duplicate_count == 0:
            print(f"✓ No duplicate records found")
        else:
            print(f"✓ Duplicate count ({duplicate_count}) within acceptable threshold")


def _run_fixtures(tmp_path: Path, filenames: list) -> pd.DataFrame:
    """Copy committed real-row excerpts into a raw directory and transform them."""
    raw_dir = tmp_path / "raw"
    source_dir = raw_dir / "teacher_working_conditions"
    source_dir.mkdir(parents=True)
    proc_dir = tmp_path / "processed"
    proc_dir.mkdir()
    fixture_dir = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "teacher_working_conditions"
    for name in filenames:
        (source_dir / name).write_bytes((fixture_dir / name).read_bytes())
    config = yaml.safe_load(
        (Path(__file__).resolve().parents[1] / "config" / "mappings.yaml").read_text()
    )
    transform(raw_dir, proc_dir, config["sources"]["teacher_working_conditions"])
    output = proc_dir / "teacher_working_conditions.csv"
    assert output.exists(), filenames
    return pd.read_csv(output, dtype=str)


def _comparable(df: pd.DataFrame) -> pd.DataFrame:
    """Drop the processing timestamp and sort so two runs can be compared."""
    columns = [column for column in df.columns if column != "last_updated"]
    return df[columns].sort_values(columns).reset_index(drop=True)


def _scores(df: pd.DataFrame, year: str, school_id: str) -> dict:
    matches = df[(df["year"] == year) & (df["school_id"] == school_id)]
    assert len(matches) > 0, (year, school_id)
    assert (matches["student_group"] == "All Students").all()
    assert (matches["suppressed"] == "N").all()
    return {row["metric"]: float(row["value"]) for _, row in matches.iterrows()}


def test_kyrc26_real_rows_and_prior_year_anchors(tmp_path):
    """2025-26 percents from real rows, with earlier-year anchors.

    State percent favorable is 69 behavior, 67 climate, and 70 leadership.
    KPI year is 2026, from school year 20252026. District is All Districts.
    There is no Suppressed column and no starred cell. The 2023 Ashland
    Head Start rows are the text N/A, kept verbatim in the fixture, and
    emit nothing. Boys & Girls Haven leadership is the published 0.
    """
    fixture_dir = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "teacher_working_conditions"
    na_text = (fixture_dir / "teacher_working_conditions_2023.csv").read_text(encoding="utf-8")
    assert "N/A" in na_text
    assert "Ashland Head Start" in na_text

    prior_names = [
        "teacher_working_conditions_2020.csv",
        "teacher_working_conditions_2021.csv",
        "teacher_working_conditions_2022.csv",
        "teacher_working_conditions_2023.csv",
        "KYRC24_OVW_Teacher_Working_Conditions.csv",
        "KYRC25_OVW_Teacher_Working_Conditions.csv",
    ]
    current_names = prior_names + ["KYRC26_OVW_Teacher_Working_Conditions.csv"]
    prior_only = _run_fixtures(tmp_path / "prior", prior_names)
    combined = _run_fixtures(tmp_path / "combined", current_names)

    # Same-branch check only: adding the 2026 excerpt does not rewrite earlier rows.
    # The fixed anchors below are the prior-year regression.
    prior_years = ["2020", "2021", "2022", "2023", "2024", "2025"]
    prior_slice = combined[combined["year"].isin(prior_years)]
    assert _comparable(prior_slice).equals(_comparable(prior_only))

    assert set(combined["source_file"]) == set(current_names)
    assert "20252026" not in set(combined["year"])
    assert set(combined.loc[combined["source_file"].str.startswith("KYRC26_"), "year"]) == {"2026"}
    assert "012012" not in set(combined["school_id"])

    behavior = "teacher_working_conditions_managing_student_behavior"
    climate = "teacher_working_conditions_school_climate"
    leadership = "teacher_working_conditions_school_leadership"
    teaching = "teacher_working_conditions_teaching_environment"

    state = combined[(combined["year"] == "2026") & (combined["school_id"] == "999000")]
    assert _scores(combined, "2026", "999000") == {behavior: 69.0, climate: 67.0, leadership: 70.0}
    assert set(state["district"]) == {"All Districts"}
    assert set(state["school_name"]) == {"---District Total---"}
    assert set(state["source_file"]) == {"KYRC26_OVW_Teacher_Working_Conditions.csv"}

    district = combined[(combined["year"] == "2026") & (combined["school_id"] == "165000")]
    assert _scores(combined, "2026", "165000") == {behavior: 69.0, climate: 69.0, leadership: 70.0}
    assert set(district["district"]) == {"Fayette County"}
    assert set(district["school_name"]) == {"---District Total---"}
    assert set(district["co_op"]) == {"CKEC"}
    assert set(district["co_op_code"]) == {"901"}
    assert set(district["nces_id"]) == {"2101860"}
    assert set(district["county_name"]) == {"FAYETTE"}

    school = combined[(combined["year"] == "2026") & (combined["school_id"] == "165170")]
    assert _scores(combined, "2026", "165170") == {behavior: 69.0, climate: 73.0, leadership: 74.0}
    assert set(school["school_name"]) == {"Bryan Station High School"}
    assert set(school["district"]) == {"Fayette County"}
    assert set(school["state_school_id"]) == {"034165170"}
    assert set(school["nces_id"]) == {"210186000388"}
    assert set(school["school_type"]) == {"A1"}
    assert set(school["co_op"]) == {"CKEC"}
    assert set(school["co_op_code"]) == {"901"}

    # Prior-year anchors. Values are the file cells, including the published 0.
    assert _scores(combined, "2025", "999000") == {behavior: 67.0, climate: 65.0, leadership: 69.0}
    row = combined[(combined["year"] == "2025") & (combined["school_id"] == "999000")].iloc[0]
    assert row["district"] == "All Districts"
    assert row["school_name"] == "---District Total---"
    assert row["source_file"] == "KYRC25_OVW_Teacher_Working_Conditions.csv"
    assert _scores(combined, "2025", "165170") == {behavior: 58.0, climate: 61.0, leadership: 60.0}
    bryan_2025 = combined[(combined["year"] == "2025") & (combined["school_id"] == "165170")].iloc[0]
    assert bryan_2025["nces_id"] == "210186000000"
    assert _scores(combined, "2025", "001010") == {behavior: 67.0, climate: 45.0, leadership: 55.0}
    adair = combined[(combined["year"] == "2025") & (combined["school_id"] == "001010")].iloc[0]
    assert adair["nces_id"] == "210003000000"
    assert adair["school_name"] == "Adair County High School"
    assert _scores(combined, "2025", "275028") == {behavior: 42.0, climate: 28.0, leadership: 0.0}
    haven = combined[(combined["year"] == "2025") & (combined["school_id"] == "275028")].iloc[0]
    assert haven["school_name"] == "Boys & Girls Haven"
    assert haven["co_op"] == "GREC"
    assert haven["co_op_code"] == "909"

    assert _scores(combined, "2024", "999000") == {behavior: 67.0, climate: 65.0, leadership: 69.0}
    assert _scores(combined, "2024", "165170") == {behavior: 58.0, climate: 61.0, leadership: 60.0}
    row = combined[(combined["year"] == "2024") & (combined["school_id"] == "165170")].iloc[0]
    assert row["nces_id"] == "210186000388"
    assert row["source_file"] == "KYRC24_OVW_Teacher_Working_Conditions.csv"

    assert _scores(combined, "2023", "999") == {behavior: 65.0, leadership: 64.0, teaching: 60.0}
    state_2023 = combined[(combined["year"] == "2023") & (combined["school_id"] == "999")].iloc[0]
    assert state_2023["district"] == "State"
    assert state_2023["school_name"] == "---State Total---"
    assert state_2023["source_file"] == "teacher_working_conditions_2023.csv"
    # The historical file types this district total as 20022023. The ending year is still 2023.
    assert _scores(combined, "2023", "061") == {behavior: 77.0, leadership: 76.0, teaching: 74.0}
    breathitt = combined[(combined["year"] == "2023") & (combined["school_id"] == "061")].iloc[0]
    assert breathitt["district"] == "Breathitt County"
    assert breathitt["school_name"] == "---District Total---"
    assert _scores(combined, "2023", "001010") == {behavior: 76.0, leadership: 74.0, teaching: 59.0}
    assert combined[(combined["year"] == "2023") & (combined["school_id"] == "001010")].iloc[0]["nces_id"] == "210003000001"

    assert _scores(combined, "2022", "999") == {behavior: 65.0, leadership: 64.0, teaching: 60.0}
    assert _scores(combined, "2022", "165170") == {behavior: 41.0, leadership: 38.0, teaching: 40.0}
    assert _scores(combined, "2021", "999") == {behavior: 65.0, leadership: 66.0, teaching: 62.0}
    assert _scores(combined, "2021", "165170") == {behavior: 45.0, leadership: 44.0, teaching: 39.0}
    assert "586010" not in set(combined.loc[combined["year"] == "2021", "school_id"])

    assert _scores(combined, "2020", "999") == {behavior: 65.0, leadership: 66.0, teaching: 62.0}
    state_2020 = combined[(combined["year"] == "2020") & (combined["school_id"] == "999")].iloc[0]
    assert state_2020["district"] == "State"
    assert state_2020["school_name"] == "---State Total---"
    assert state_2020["source_file"] == "teacher_working_conditions_2020.csv"
    assert _scores(combined, "2020", "165170") == {behavior: 45.0, leadership: 44.0, teaching: 39.0}
    assert _scores(combined, "2020", "586010") == {behavior: 67.0, leadership: 67.0, teaching: 62.0}
    west_point = combined[(combined["year"] == "2020") & (combined["school_id"] == "586010")].iloc[0]
    assert west_point["school_name"] == "West Point Elementary School"
    assert west_point["district"] == "West Point Independent"
    assert west_point["nces_id"] == "210585001375"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
