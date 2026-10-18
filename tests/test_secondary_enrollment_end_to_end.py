"""
End-to-end validation test for secondary enrollment ETL pipeline.
Tests that sample rows from each source file are correctly transformed to KPI format.
"""
import pytest
import pandas as pd
import numpy as np
import yaml
from etl.constants import KPI_COLUMNS
from pathlib import Path
from etl.secondary_enrollment import transform, SecondaryEnrollmentETL


class TestSecondaryEnrollmentEndToEnd:
    """Test complete transformation from raw data to KPI format."""

    def setup_method(self):
        """Setup ETL instance for testing."""
        self.etl = SecondaryEnrollmentETL('secondary_enrollment')

    def test_source_to_kpi_transformation(self):
        """Test that 10 random rows from each source file are correctly represented in processed file."""
        # Paths to actual data files
        raw_data_dir = Path("data/raw/secondary_enrollment")
        processed_file = Path("data/processed/secondary_enrollment.csv")

        # Ensure processed file exists
        if not processed_file.exists():
            pytest.skip("Processed secondary_enrollment.csv not found. Run ETL pipeline first.")

        # Load processed KPI data
        kpi_df = pd.read_csv(processed_file)

        # Test each source file
        for source_file in raw_data_dir.glob("*.csv"):
            print(f"\nTesting {source_file.name}...")
            self._test_source_file_transformation(source_file, kpi_df)

    def _test_source_file_transformation(self, source_file: Path, kpi_df: pd.DataFrame):
        """Test transformation of a single source file."""
        # Load source data
        source_df = pd.read_csv(source_file)

        if source_df.empty:
            pytest.skip(f"Source file {source_file.name} is empty")

        # Take 10 random rows (or all if less than 10)
        sample_size = min(10, len(source_df))
        sample_rows = source_df.sample(n=sample_size, random_state=42)

        print(f"Testing {sample_size} rows from {source_file.name}")

        # Test each sample row
        for idx, row in sample_rows.iterrows():
            self._test_row_transformation(row, kpi_df, source_file.name)

    def _test_row_transformation(self, source_row: pd.Series, kpi_df: pd.DataFrame, source_filename: str):
        """Test that a single source row is correctly transformed to KPI format."""
        # Apply column normalization to match ETL processing
        normalized_row = self._normalize_row_columns(source_row)

        # Extract expected metrics using ETL logic
        expected_metrics = self.etl.extract_metrics(normalized_row)

        # Create expected identifiers for matching KPI rows
        expected_identifiers = self._extract_identifiers(source_row)

        # Find corresponding KPI rows
        kpi_rows = self._find_kpi_rows(kpi_df, expected_identifiers, source_filename)

        if kpi_rows.empty:
            print(f"Warning: No KPI rows found for identifiers: {expected_identifiers}")
            return

        # Test each expected metric
        for metric_name, expected_value in expected_metrics.items():
            if pd.notna(expected_value) and expected_value != 0:
                self._test_metric_in_kpi(kpi_rows, metric_name, expected_value, expected_identifiers)

    def _normalize_row_columns(self, row: pd.Series) -> pd.Series:
        """Apply column name normalization like the ETL does."""
        # Create a single-row DataFrame to use ETL normalization
        temp_df = pd.DataFrame([row])
        normalized_df = self.etl.normalize_column_names(temp_df)
        return normalized_df.iloc[0]

    def _extract_identifiers(self, source_row: pd.Series) -> dict:
        """Extract identifying information from source row."""
        identifiers = {}

        # Map common identifier fields (handle both formats)
        identifier_mappings = {
            'district': ['District Name', 'DISTRICT NAME'],
            'school_name': ['School Name', 'SCHOOL NAME'],
            'student_group': ['Demographic', 'DEMOGRAPHIC'],
            'county_number': ['County Number', 'COUNTY NUMBER'],
            'district_number': ['District Number', 'DISTRICT NUMBER'],
            'school_code': ['School Code', 'SCHOOL CODE']
        }

        for kpi_field, source_fields in identifier_mappings.items():
            for field in source_fields:
                if field in source_row.index and pd.notna(source_row[field]):
                    identifiers[kpi_field] = str(source_row[field]).strip()
                    break

        return identifiers

    def _find_kpi_rows(self, kpi_df: pd.DataFrame, identifiers: dict, source_filename: str) -> pd.DataFrame:
        """Find KPI rows matching the given identifiers."""
        mask = pd.Series([True] * len(kpi_df))

        # Apply filters for each identifier
        for field, value in identifiers.items():
            if field in kpi_df.columns:
                mask &= (kpi_df[field].astype(str).str.strip() == value)

        # Filter by source file
        if 'source_file' in kpi_df.columns:
            mask &= (kpi_df['source_file'] == source_filename)

        return kpi_df[mask]

    def _test_metric_in_kpi(self, kpi_rows: pd.DataFrame, metric_name: str, expected_value: float, identifiers: dict):
        """Test that a specific metric appears correctly in KPI data."""
        metric_rows = kpi_rows[kpi_rows['metric'] == metric_name]

        if metric_rows.empty:
            print(f"Warning: Metric '{metric_name}' not found in KPI data for identifiers: {identifiers}")
            return

        # Should have exactly one row for this metric
        assert len(metric_rows) == 1, f"Expected 1 row for metric '{metric_name}', found {len(metric_rows)}"

        kpi_row = metric_rows.iloc[0]
        actual_value = kpi_row['value']

        # Test the value
        if pd.isna(expected_value):
            assert pd.isna(actual_value), f"Expected NaN for metric '{metric_name}', got {actual_value}"
        else:
            assert actual_value == expected_value, f"Expected {expected_value} for metric '{metric_name}', got {actual_value}"

        # Test that suppression flag is consistent
        if pd.isna(actual_value):
            assert kpi_row.get('suppressed', 'N') == 'Y', f"Expected suppressed='Y' for NaN value in metric '{metric_name}'"
        else:
            assert kpi_row.get('suppressed', 'N') == 'N', f"Expected suppressed='N' for non-NaN value in metric '{metric_name}'"

    def test_kpi_format_compliance(self):
        """Test that the processed file complies with KPI format requirements."""
        processed_file = Path("data/processed/secondary_enrollment.csv")

        if not processed_file.exists():
            pytest.skip("Processed secondary_enrollment.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        # Test column structure
        assert set(kpi_df.columns) == set(KPI_COLUMNS), "KPI file missing required columns"

        # Test that we have data
        assert not kpi_df.empty, "KPI file should not be empty"

        # Test required fields are not all null
        required_fields = ['year', 'metric', 'district', 'school_name', 'student_group']
        for field in required_fields:
            assert not kpi_df[field].isna().all(), f"Field '{field}' should not be all null"

        # Test metric names follow expected patterns with secondary_enrollment prefix
        metrics = kpi_df['metric'].unique()
        expected_metric_patterns = [
            'secondary_enrollment_total',
            'secondary_enrollment_grade_',
            'secondary_enrollment_middle',
            'secondary_enrollment_high_school'
        ]

        found_patterns = []
        for metric in metrics:
            for pattern in expected_metric_patterns:
                if pattern in metric:
                    found_patterns.append(pattern)
                    break

        assert len(found_patterns) > 0, f"No expected metric patterns found in: {list(metrics)}"

        # Ensure no student_enrollment metrics (should be secondary_enrollment)
        for metric in metrics:
            assert not metric.startswith('student_enrollment_'), \
                f"Found student_enrollment metric '{metric}' - should be secondary_enrollment"

    def test_enrollment_value_validation(self):
        """Test that enrollment values meet validation requirements."""
        processed_file = Path("data/processed/secondary_enrollment.csv")

        if not processed_file.exists():
            pytest.skip("Processed secondary_enrollment.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        # Test that non-null values are non-negative
        non_null_values = kpi_df['value'].dropna()
        if not non_null_values.empty:
            assert (non_null_values >= 0).all(), "All enrollment values should be non-negative"

        # Test that values are integers (enrollment counts)
        for value in non_null_values.head(100):  # Test first 100 for performance
            if pd.notna(value):
                assert value == int(value), f"Enrollment value {value} should be an integer"

    def test_demographic_coverage(self):
        """Test that standard demographic groups are represented."""
        processed_file = Path("data/processed/secondary_enrollment.csv")

        if not processed_file.exists():
            pytest.skip("Processed secondary_enrollment.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        # Check for standard demographic groups
        student_groups = kpi_df['student_group'].unique()

        expected_groups = ['All Students', 'Female', 'Male']
        for group in expected_groups:
            assert group in student_groups, f"Expected demographic group '{group}' not found"

    def test_source_file_coverage(self):
        """Test that all source files are represented in the processed data."""
        processed_file = Path("data/processed/secondary_enrollment.csv")
        raw_data_dir = Path("data/raw/secondary_enrollment")

        if not processed_file.exists():
            pytest.skip("Processed secondary_enrollment.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        # Get all source files that should be processed
        expected_sources = [f.name for f in raw_data_dir.glob("*.csv")]

        if not expected_sources:
            pytest.skip("No source files found in raw data directory")

        # Get actual source files from processed data
        actual_sources = kpi_df['source_file'].unique()

        # Check that we have representation from multiple sources
        assert len(actual_sources) > 0, "No source files represented in processed data"

        # Log coverage for debugging
        print(f"Expected sources: {expected_sources}")
        print(f"Actual sources: {list(actual_sources)}")

        # Check that major file types are represented if they exist
        has_kyrc25 = any('KYRC25' in f for f in expected_sources)
        has_historical = any('secondary_enrollment_202' in f for f in expected_sources)

        if has_kyrc25:
            assert any('KYRC25' in f for f in actual_sources), "KYRC25 files should be represented"
        if has_historical:
            assert any('secondary_enrollment_202' in f for f in actual_sources), "Historical secondary enrollment files should be represented"

    def test_aggregated_metrics_consistency(self):
        """Test that aggregated metrics are consistent with individual grade metrics."""
        processed_file = Path("data/processed/secondary_enrollment.csv")

        if not processed_file.exists():
            pytest.skip("Processed secondary_enrollment.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        # Find a few test cases where we can verify aggregations
        # Group by identifiers to find complete records
        test_groups = kpi_df.groupby(['district', 'school_name', 'student_group', 'source_file']).filter(
            lambda x: len(x) >= 5  # Has multiple metrics
        ).groupby(['district', 'school_name', 'student_group', 'source_file'])

        tested_cases = 0
        for (district, school, group, source), group_df in test_groups:
            # Test high school aggregation (grades 9-12)
            high_school_grades = ['secondary_enrollment_grade_9', 'secondary_enrollment_grade_10',
                                  'secondary_enrollment_grade_11', 'secondary_enrollment_grade_12']

            hs_metrics = group_df[group_df['metric'].isin(high_school_grades)]
            hs_agg = group_df[group_df['metric'] == 'secondary_enrollment_high_school']

            if len(hs_metrics) > 0 and len(hs_agg) == 1:
                individual_sum = hs_metrics['value'].sum()
                agg_value = hs_agg['value'].iloc[0]

                if pd.notna(individual_sum) and pd.notna(agg_value):
                    assert abs(individual_sum - agg_value) < 0.01, \
                        f"High school aggregation mismatch: {individual_sum} vs {agg_value} for {district}/{school}/{group}"
                    tested_cases += 1

            # Test middle school aggregation (grades 6-8)
            middle_grades = ['secondary_enrollment_grade_6', 'secondary_enrollment_grade_7',
                             'secondary_enrollment_grade_8']

            middle_metrics = group_df[group_df['metric'].isin(middle_grades)]
            middle_agg = group_df[group_df['metric'] == 'secondary_enrollment_middle']

            if len(middle_metrics) > 0 and len(middle_agg) == 1:
                individual_sum = middle_metrics['value'].sum()
                agg_value = middle_agg['value'].iloc[0]

                if pd.notna(individual_sum) and pd.notna(agg_value):
                    assert abs(individual_sum - agg_value) < 0.01, \
                        f"Middle school aggregation mismatch: {individual_sum} vs {agg_value} for {district}/{school}/{group}"
                    tested_cases += 1

            # Stop after testing a few cases for performance
            if tested_cases >= 6:
                break

        print(f"Tested {tested_cases} aggregation cases")

    def test_no_duplicate_records(self):
        """Test that there are no duplicate records for same school/year/metric/student_group."""
        processed_file = Path("data/processed/secondary_enrollment.csv")

        if not processed_file.exists():
            pytest.skip("Processed secondary_enrollment.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        # Check for duplicates
        duplicate_check = kpi_df.groupby(['year', 'school_id', 'metric', 'student_group']).size()
        duplicates = duplicate_check[duplicate_check > 1]

        if len(duplicates) > 0:
            print(f"Found {len(duplicates)} duplicate combinations:")
            print(duplicates.head(10))

        assert len(duplicates) == 0, f"Found {len(duplicates)} duplicate school/year/metric/student_group combinations"

    def test_year_coverage(self):
        """Test that expected years are represented in the data."""
        processed_file = Path("data/processed/secondary_enrollment.csv")

        if not processed_file.exists():
            pytest.skip("Processed secondary_enrollment.csv not found. Run ETL pipeline first.")

        kpi_df = pd.read_csv(processed_file)

        years = kpi_df['year'].unique()
        print(f"Years in data: {sorted(years)}")

        # Should have multiple years
        assert len(years) >= 2, f"Expected multiple years, found: {years}"

        # Should include recent years (2020-2025 range based on source files)
        expected_recent_years = [2020, 2021, 2022, 2023, 2025]
        found_years = [y for y in expected_recent_years if y in years]
        assert len(found_years) >= 3, f"Expected at least 3 recent years, found: {found_years}"


FIXTURE_DIR = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "secondary_enrollment"


def _run_fixtures(tmp_path: Path, filenames: list) -> pd.DataFrame:
    """Copy committed real-row excerpts into a raw directory and transform them."""
    raw_dir = tmp_path / "raw"
    source_dir = raw_dir / "secondary_enrollment"
    source_dir.mkdir(parents=True)
    proc_dir = tmp_path / "processed"
    proc_dir.mkdir()
    for name in filenames:
        (source_dir / name).write_bytes((FIXTURE_DIR / name).read_bytes())
    config = yaml.safe_load(
        (Path(__file__).resolve().parents[1] / "config" / "mappings.yaml").read_text()
    )
    transform(raw_dir, proc_dir, config["sources"]["secondary_enrollment"])
    output = proc_dir / "secondary_enrollment.csv"
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
    """2025-26 KPIs from real secondary-enrollment rows, with earlier-year anchors.

    State All Students is 30,193. KPI year is 2026, from school year 20252026.
    District is All Districts. The 2022-2025 anchors are the published source
    values, including the comma-formatted 2024 state total 28,978 and the
    2025 state total 29,360.
    """
    prior_names = [
        "secondary_enrollment_2022.csv",
        "secondary_enrollment_2023.csv",
        "KYRC24_OVW_Secondary_Enrollment.csv",
        "KYRC25_OVW_Secondary_Enrollment.csv",
    ]
    current_names = prior_names + ["KYRC26_OVW_Secondary_Enrollment.csv"]
    prior_only = _run_fixtures(tmp_path / "prior", prior_names)
    combined = _run_fixtures(tmp_path / "combined", current_names)

    # Same-branch check only: adding the 2026 excerpt does not rewrite earlier rows.
    # The fixed anchors below are the prior-year regression.
    prior_slice = combined[combined["year"].isin(["2022", "2023", "2024", "2025"])]
    assert _comparable(prior_slice).equals(_comparable(prior_only))

    assert set(combined["source_file"]) == set(current_names)
    assert "20252026" not in set(combined["year"])
    assert set(combined.loc[combined["source_file"].str.startswith("KYRC26_"), "year"]) == {"2026"}

    state = _metric_value(combined, "2026", "999000", "secondary_enrollment_total")
    assert float(state["value"]) == 30193
    assert state["suppressed"] == "N"
    assert state["district"] == "All Districts"
    assert state["school_name"] == "---District Total---"
    assert state["year"] == "2026"
    assert state["school_id"] == "999000"
    assert state["source_file"] == "KYRC26_OVW_Secondary_Enrollment.csv"
    assert float(_metric_value(combined, "2026", "999000", "secondary_enrollment_grade_6")["value"]) == 9
    assert float(_metric_value(combined, "2026", "999000", "secondary_enrollment_grade_7")["value"]) == 95
    assert float(_metric_value(combined, "2026", "999000", "secondary_enrollment_grade_8")["value"]) == 829
    assert float(_metric_value(combined, "2026", "999000", "secondary_enrollment_grade_9")["value"]) == 6287
    assert float(_metric_value(combined, "2026", "999000", "secondary_enrollment_grade_10")["value"]) == 7707
    assert float(_metric_value(combined, "2026", "999000", "secondary_enrollment_grade_11")["value"]) == 8192
    assert float(_metric_value(combined, "2026", "999000", "secondary_enrollment_grade_12")["value"]) == 6985
    assert float(_metric_value(combined, "2026", "999000", "secondary_enrollment_grade_14")["value"]) == 26
    assert float(_metric_value(combined, "2026", "999000", "secondary_enrollment_middle")["value"]) == 933
    assert float(_metric_value(combined, "2026", "999000", "secondary_enrollment_high_school")["value"]) == 29171

    district = _metric_value(combined, "2026", "165000", "secondary_enrollment_total")
    # 97 is Tates Creek Middle 1 + Tates Creek High 16 + Frederick Douglass 70
    # + Success Academy 10. The district grade cells sum to 96 because Tates
    # Creek Middle's All Grades cell is 1 and its grade 6-8 cells are 0.
    assert float(district["value"]) == 97
    assert district["district"] == "Fayette County"
    assert district["school_name"] == "---District Total---"
    assert district["school_id"] == "165000"
    assert district["co_op"] == "CKEC"
    assert district["co_op_code"] == "901"
    assert district["nces_id"] == "2101860"
    assert district["county_name"] == "FAYETTE"
    assert district["suppressed"] == "N"
    assert float(_metric_value(combined, "2026", "165000", "secondary_enrollment_grade_7")["value"]) == 1
    assert float(_metric_value(combined, "2026", "165000", "secondary_enrollment_grade_12")["value"]) == 15
    assert float(_metric_value(combined, "2026", "165000", "secondary_enrollment_middle")["value"]) == 26
    assert float(_metric_value(combined, "2026", "165000", "secondary_enrollment_high_school")["value"]) == 70
    # Grade 6 and grade 14 are 0. A published zero is omitted by the existing
    # value > 0 rule. Whether that zero should be a KPI row is a pre-existing
    # design question, not suppression.
    fayette = _rows(combined, "2026", "165000", "All Students")
    assert "secondary_enrollment_grade_6" not in set(fayette["metric"])
    assert "secondary_enrollment_grade_14" not in set(fayette["metric"])

    school = _metric_value(combined, "2026", "035160", "secondary_enrollment_total")
    assert float(school["value"]) == 1300
    assert school["district"] == "Boone County"
    assert school["school_name"] == "Ignite Institute"
    assert school["state_school_id"] == "008035160"
    assert school["nces_id"] == "210051002536"
    assert school["school_type"] == "A2"
    assert school["county_name"] == "BOONE"
    assert school["co_op"] == "NKCES"
    assert school["co_op_code"] == "905"
    assert school["suppressed"] == "N"
    assert float(_metric_value(combined, "2026", "035160", "secondary_enrollment_grade_8")["value"]) == 67
    assert float(_metric_value(combined, "2026", "035160", "secondary_enrollment_grade_9")["value"]) == 454
    assert float(_metric_value(combined, "2026", "035160", "secondary_enrollment_grade_12")["value"]) == 245
    assert float(_metric_value(combined, "2026", "035160", "secondary_enrollment_middle")["value"]) == 67
    assert float(_metric_value(combined, "2026", "035160", "secondary_enrollment_high_school")["value"]) == 1233
    # Grades 6, 7, and 14 are blank. They are not in the All Grades sum
    # (67 + 454 + 280 + 254 + 245 = 1,300). A blank grade is not offered.
    ignite = _rows(combined, "2026", "035160", "All Students")
    assert "secondary_enrollment_grade_6" not in set(ignite["metric"])
    assert "secondary_enrollment_grade_7" not in set(ignite["metric"])
    assert "secondary_enrollment_grade_14" not in set(ignite["metric"])

    # Grade 6 is blank. Grades 9-12 and 14 are 0. Grade 7 is 1 and grade 8 is 15.
    tates = _rows(combined, "2026", "165105", "All Students")
    assert set(tates["metric"]) == {
        "secondary_enrollment_total",
        "secondary_enrollment_grade_7",
        "secondary_enrollment_grade_8",
        "secondary_enrollment_middle",
    }
    assert float(_metric_value(combined, "2026", "165105", "secondary_enrollment_total")["value"]) == 16
    assert float(_metric_value(combined, "2026", "165105", "secondary_enrollment_grade_7")["value"]) == 1
    assert float(_metric_value(combined, "2026", "165105", "secondary_enrollment_grade_8")["value"]) == 15
    assert tates["school_name"].iloc[0] == "Tates Creek High School"
    assert tates["nces_id"].iloc[0] == "210186000383"
    assert (tates["suppressed"] == "N").all()

    # Grades 6-8 are 0. Grades 9-12 and 14 are blank. The total of 1 is published.
    middle = _rows(combined, "2026", "165100", "All Students")
    assert set(middle["metric"]) == {"secondary_enrollment_total"}
    assert float(middle["value"].iloc[0]) == 1
    assert middle["school_name"].iloc[0] == "Tates Creek Middle School"
    assert middle["suppressed"].iloc[0] == "N"

    # There is no Suppressed column and no star. A count of 1 is published.
    # Grades 8-11 are 0. Grades 6, 7, and 14 are blank. Grade 12 is 1, so the
    # high-school aggregate is that same 1.
    indigenous = _rows(combined, "2026", "035160", "American Indian or Alaska Native")
    assert set(indigenous["metric"]) == {
        "secondary_enrollment_total",
        "secondary_enrollment_grade_12",
        "secondary_enrollment_high_school",
    }
    assert float(_metric_value(combined, "2026", "035160", "secondary_enrollment_total", "American Indian or Alaska Native")["value"]) == 1
    assert float(_metric_value(combined, "2026", "035160", "secondary_enrollment_grade_12", "American Indian or Alaska Native")["value"]) == 1
    assert float(_metric_value(combined, "2026", "035160", "secondary_enrollment_high_school", "American Indian or Alaska Native")["value"]) == 1
    assert (indigenous["suppressed"] == "N").all()

    # Prior-year anchors. The 2024 and 2025 state totals are comma-formatted.
    # 2024 All Grades equals the published grade cells: 822 + 28,140 + 16.
    row = _metric_value(combined, "2024", "999000", "secondary_enrollment_total")
    assert float(row["value"]) == 28978
    assert row["district"] == "All Districts"
    assert row["school_name"] == "---District Total---"
    assert row["year"] == "2024"
    assert row["suppressed"] == "N"
    assert row["source_file"] == "KYRC24_OVW_Secondary_Enrollment.csv"
    assert float(_metric_value(combined, "2024", "999000", "secondary_enrollment_grade_9")["value"]) == 6273
    assert float(_metric_value(combined, "2024", "999000", "secondary_enrollment_middle")["value"]) == 822
    assert float(_metric_value(combined, "2024", "999000", "secondary_enrollment_high_school")["value"]) == 28140
    assert float(_metric_value(combined, "2024", "999000", "secondary_enrollment_grade_14")["value"]) == 16

    row = _metric_value(combined, "2025", "999000", "secondary_enrollment_total")
    assert float(row["value"]) == 29360
    assert row["district"] == "All Districts"
    assert row["school_name"] == "---District Total---"
    assert row["suppressed"] == "N"
    assert float(_metric_value(combined, "2025", "999000", "secondary_enrollment_grade_9")["value"]) == 6064
    assert float(_metric_value(combined, "2025", "999000", "secondary_enrollment_grade_12")["value"]) == 6671
    assert float(_metric_value(combined, "2025", "999000", "secondary_enrollment_middle")["value"]) == 959
    assert float(_metric_value(combined, "2025", "999000", "secondary_enrollment_high_school")["value"]) == 28385
    assert float(_metric_value(combined, "2025", "165000", "secondary_enrollment_total")["value"]) == 87
    assert float(_metric_value(combined, "2025", "165105", "secondary_enrollment_total")["value"]) == 7
    assert _metric_value(combined, "2025", "165105", "secondary_enrollment_total")["nces_id"] == "210186000000"
    assert float(_metric_value(combined, "2025", "165105", "secondary_enrollment_grade_7")["value"]) == 1

    # 2022 and 2023 state rows are district State and school ---State Total---,
    # school code 999. Totals have no thousands separator in the source file.
    for year, total in (("2023", 28723), ("2022", 28465)):
        row = _metric_value(combined, year, "999", "secondary_enrollment_total")
        assert float(row["value"]) == total
        assert row["district"] == "State"
        assert row["school_name"] == "---State Total---"
        assert row["suppressed"] == "N"

    # The 2023 Consolidated Student Group state row is empty in every count
    # column. It is not a suppression. It emits no KPI row.
    consolidated = combined[
        (combined["year"] == "2023")
        & (combined["student_group"] == "Consolidated Student Group")
    ]
    assert consolidated.empty
