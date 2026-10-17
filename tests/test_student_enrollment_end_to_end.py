"""
End-to-end validation test for student enrollment ETL pipeline.
Tests that sample rows from each source file are correctly transformed to KPI format.
"""
import pytest
import pandas as pd
import numpy as np
from etl.constants import KPI_COLUMNS
from pathlib import Path
from etl.student_enrollment import transform, StudentEnrollmentETL


class TestStudentEnrollmentEndToEnd:
    """Test complete transformation from raw data to KPI format."""
    
    def setup_method(self):
        """Setup ETL instance for testing."""
        self.etl = StudentEnrollmentETL('student_enrollment')
    
    def test_source_to_kpi_transformation(self):
        """Test that 10 random rows from each source file are correctly represented in processed file."""
        # Paths to actual data files
        raw_data_dir = Path("data/raw/student_enrollment")
        processed_file = Path("data/processed/student_enrollment.csv")
        
        # Ensure processed file exists
        if not processed_file.exists():
            pytest.skip("Processed student_enrollment.csv not found. Run ETL pipeline first.")
        
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
        processed_file = Path("data/processed/student_enrollment.csv")
        
        if not processed_file.exists():
            pytest.skip("Processed student_enrollment.csv not found. Run ETL pipeline first.")
        
        kpi_df = pd.read_csv(processed_file)
        
        # Test column structure
        assert set(kpi_df.columns) == set(KPI_COLUMNS), "KPI file missing required columns"
        
        # Test that we have data
        assert not kpi_df.empty, "KPI file should not be empty"
        
        # Test required fields are not all null
        required_fields = ['year', 'metric', 'district', 'school_name', 'student_group']
        for field in required_fields:
            assert not kpi_df[field].isna().all(), f"Field '{field}' should not be all null"
        
        # Test metric names follow expected patterns
        metrics = kpi_df['metric'].unique()
        expected_metric_patterns = [
            'student_enrollment_total',
            'student_enrollment_preschool',
            'student_enrollment_kindergarten',
            'student_enrollment_grade_',
            'student_enrollment_primary',
            'student_enrollment_middle',
            'student_enrollment_secondary'
        ]
        
        found_patterns = []
        for metric in metrics:
            for pattern in expected_metric_patterns:
                if pattern in metric:
                    found_patterns.append(pattern)
                    break
        
        assert len(found_patterns) > 0, f"No expected metric patterns found in: {list(metrics)}"
    
    def test_enrollment_value_validation(self):
        """Test that enrollment values meet validation requirements."""
        processed_file = Path("data/processed/student_enrollment.csv")
        
        if not processed_file.exists():
            pytest.skip("Processed student_enrollment.csv not found. Run ETL pipeline first.")
        
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
        processed_file = Path("data/processed/student_enrollment.csv")
        
        if not processed_file.exists():
            pytest.skip("Processed student_enrollment.csv not found. Run ETL pipeline first.")
        
        kpi_df = pd.read_csv(processed_file)
        
        # Check for standard demographic groups
        student_groups = kpi_df['student_group'].unique()
        
        expected_groups = ['All Students', 'Female', 'Male']
        for group in expected_groups:
            assert group in student_groups, f"Expected demographic group '{group}' not found"

    def test_source_file_coverage(self):
        """Test that all source files are represented in the processed data."""
        processed_file = Path("data/processed/student_enrollment.csv")
        raw_data_dir = Path("data/raw/student_enrollment")
        
        if not processed_file.exists():
            pytest.skip("Processed student_enrollment.csv not found. Run ETL pipeline first.")
        
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
        has_kyrc24 = any('KYRC24' in f for f in expected_sources)
        has_primary = any('primary_enrollment' in f for f in expected_sources)
        has_secondary = any('secondary_enrollment' in f for f in expected_sources)
        
        if has_kyrc24:
            assert any('KYRC24' in f for f in actual_sources), "KYRC24 files should be represented"
        if has_primary:
            assert any('primary_enrollment' in f for f in actual_sources), "Primary enrollment files should be represented"
        if has_secondary:
            assert any('secondary_enrollment' in f for f in actual_sources), "Secondary enrollment files should be represented"

    def test_aggregated_metrics_consistency(self):
        """Test that aggregated metrics are consistent with individual grade metrics."""
        processed_file = Path("data/processed/student_enrollment.csv")
        
        if not processed_file.exists():
            pytest.skip("Processed student_enrollment.csv not found. Run ETL pipeline first.")
        
        kpi_df = pd.read_csv(processed_file)
        
        # Find a few test cases where we can verify aggregations
        # Group by identifiers to find complete records
        test_groups = kpi_df.groupby(['district', 'school_name', 'student_group', 'source_file']).filter(
            lambda x: len(x) >= 5  # Has multiple metrics
        ).groupby(['district', 'school_name', 'student_group', 'source_file'])
        
        tested_cases = 0
        for (district, school, group, source), group_df in test_groups:
            # Test primary aggregation if we have individual primary grades
            primary_grades = ['student_enrollment_preschool', 'student_enrollment_kindergarten', 
                            'student_enrollment_grade_1', 'student_enrollment_grade_2',
                            'student_enrollment_grade_3', 'student_enrollment_grade_4',
                            'student_enrollment_grade_5']
            
            primary_metrics = group_df[group_df['metric'].isin(primary_grades)]
            primary_agg = group_df[group_df['metric'] == 'student_enrollment_primary']
            
            if len(primary_metrics) > 0 and len(primary_agg) == 1:
                individual_sum = primary_metrics['value'].sum()
                agg_value = primary_agg['value'].iloc[0]
                
                if pd.notna(individual_sum) and pd.notna(agg_value):
                    assert abs(individual_sum - agg_value) < 0.01, \
                        f"Primary aggregation mismatch: {individual_sum} vs {agg_value} for {district}/{school}/{group}"
                    tested_cases += 1
            
            # Stop after testing a few cases for performance
            if tested_cases >= 3:
                break
        
        print(f"Tested {tested_cases} aggregation cases")


FIXTURE_DIR = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "student_enrollment"


def _run_fixtures(tmp_path: Path, filenames: list) -> pd.DataFrame:
    """Copy committed real-row excerpts into a raw directory and transform them."""
    raw_dir = tmp_path / "raw"
    source_dir = raw_dir / "student_enrollment"
    source_dir.mkdir(parents=True)
    proc_dir = tmp_path / "processed"
    proc_dir.mkdir()
    for name in filenames:
        (source_dir / name).write_bytes((FIXTURE_DIR / name).read_bytes())
    transform(raw_dir, proc_dir, {"derive": {"processing_date": "2026-10-17"}})
    output = proc_dir / "student_enrollment.csv"
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
    """2025-26 KPIs from real enrollment rows, with fixed earlier-year anchors.

    State All Students is 669,865. Preschool is 29,782 and the grade columns
    sum to that total. KPI year is 2026, from school year 20252026. District
    is All Districts. The 2022-2025 anchors are the published source values.
    """
    prior_names = [
        "primary_enrollment_2022.csv",
        "primary_enrollment_2023.csv",
        "KYRC24_OVW_Student_Enrollment.csv",
        "KYRC25_OVW_Student_Enrollment.csv",
    ]
    current_names = prior_names + ["KYRC26_OVW_Student_Enrollment.csv"]
    prior_only = _run_fixtures(tmp_path / "prior", prior_names)
    combined = _run_fixtures(tmp_path / "combined", current_names)

    # Same-branch check only: adding the 2026 excerpt does not rewrite earlier rows.
    # The fixed anchors below are the prior-year regression.
    prior_slice = combined[combined["year"].isin(["2022", "2023", "2024", "2025"])]
    assert _comparable(prior_slice).equals(_comparable(prior_only))

    assert set(combined["source_file"]) == set(current_names)
    assert "20252026" not in set(combined["year"])
    assert set(combined.loc[combined["source_file"].str.startswith("KYRC26_"), "year"]) == {"2026"}

    state = _metric_value(combined, "2026", "999000", "student_enrollment_total")
    assert float(state["value"]) == 669865
    assert state["suppressed"] == "N"
    assert state["district"] == "All Districts"
    assert state["school_name"] == "---District Total---"
    assert state["year"] == "2026"
    assert state["school_id"] == "999000"
    assert state["source_file"] == "KYRC26_OVW_Student_Enrollment.csv"
    assert float(_metric_value(combined, "2026", "999000", "student_enrollment_preschool")["value"]) == 29782
    assert float(_metric_value(combined, "2026", "999000", "student_enrollment_kindergarten")["value"]) == 46806
    assert float(_metric_value(combined, "2026", "999000", "student_enrollment_grade_12")["value"]) == 49115
    assert float(_metric_value(combined, "2026", "999000", "student_enrollment_grade_14")["value"]) == 634
    assert float(_metric_value(combined, "2026", "999000", "student_enrollment_primary")["value"]) == 323375
    assert float(_metric_value(combined, "2026", "999000", "student_enrollment_middle")["value"]) == 145235
    assert float(_metric_value(combined, "2026", "999000", "student_enrollment_secondary")["value"]) == 200621

    district = _metric_value(combined, "2026", "165000", "student_enrollment_total")
    assert float(district["value"]) == 43057
    assert district["district"] == "Fayette County"
    assert district["school_name"] == "---District Total---"
    assert district["school_id"] == "165000"
    assert district["co_op"] == "CKEC"
    assert district["co_op_code"] == "901"
    assert district["nces_id"] == "2101860"
    assert district["county_name"] == "FAYETTE"
    assert district["suppressed"] == "N"
    assert float(_metric_value(combined, "2026", "165000", "student_enrollment_preschool")["value"]) == 1251
    assert float(_metric_value(combined, "2026", "165000", "student_enrollment_primary")["value"]) == 20480
    assert float(_metric_value(combined, "2026", "165000", "student_enrollment_middle")["value"]) == 9333
    assert float(_metric_value(combined, "2026", "165000", "student_enrollment_secondary")["value"]) == 13218
    assert float(_metric_value(combined, "2026", "165000", "student_enrollment_grade_14")["value"]) == 26

    school = _metric_value(combined, "2026", "165170", "student_enrollment_total")
    assert float(school["value"]) == 2241
    assert school["district"] == "Fayette County"
    assert school["school_name"] == "Bryan Station High School"
    assert school["state_school_id"] == "034165170"
    assert school["nces_id"] == "210186000388"
    assert school["school_type"] == "A1"
    assert school["county_name"] == "FAYETTE"
    assert school["co_op"] == "CKEC"
    assert school["co_op_code"] == "901"
    assert school["suppressed"] == "N"
    assert float(_metric_value(combined, "2026", "165170", "student_enrollment_grade_9")["value"]) == 621
    assert float(_metric_value(combined, "2026", "165170", "student_enrollment_grade_12")["value"]) == 567
    assert float(_metric_value(combined, "2026", "165170", "student_enrollment_grade_14")["value"]) == 2
    assert float(_metric_value(combined, "2026", "165170", "student_enrollment_secondary")["value"]) == 2239
    # Preschool through grade 8 are blank. They are not in the All Grades sum.
    # A blank grade is not offered and emits no KPI row.
    bryan = _rows(combined, "2026", "165170", "All Students")
    assert "student_enrollment_preschool" not in set(bryan["metric"])
    assert "student_enrollment_kindergarten" not in set(bryan["metric"])
    assert "student_enrollment_grade_8" not in set(bryan["metric"])
    assert "student_enrollment_primary" not in set(bryan["metric"])
    assert "student_enrollment_middle" not in set(bryan["metric"])

    adair = _metric_value(combined, "2026", "001010", "student_enrollment_total")
    assert float(adair["value"]) == 818
    assert adair["district"] == "Adair County"
    assert adair["school_name"] == "Adair County High School"
    assert adair["nces_id"] == "210003000001"
    assert adair["co_op"] == "GRREC"
    assert adair["co_op_code"] == "902"
    assert float(_metric_value(combined, "2026", "001010", "student_enrollment_secondary")["value"]) == 816
    assert float(_metric_value(combined, "2026", "001010", "student_enrollment_grade_14")["value"]) == 2

    # Small counts are published. There is no Suppressed column and no star.
    # Grade 9 and 10 are 0 (offered, zero students). Grade 11 and 12 are 1.
    # Preschool through grade 8 are blank (not offered). A 0 is omitted by the
    # existing value > 0 rule. The published 1s and the total stay required.
    hawaii = _rows(combined, "2026", "165170", "Native Hawaiian or Pacific Islander")
    assert set(hawaii["metric"]) == {
        "student_enrollment_total",
        "student_enrollment_grade_11",
        "student_enrollment_grade_12",
        "student_enrollment_secondary",
    }
    assert float(_metric_value(combined, "2026", "165170", "student_enrollment_total", "Native Hawaiian or Pacific Islander")["value"]) == 2
    assert float(_metric_value(combined, "2026", "165170", "student_enrollment_grade_11", "Native Hawaiian or Pacific Islander")["value"]) == 1
    assert float(_metric_value(combined, "2026", "165170", "student_enrollment_grade_12", "Native Hawaiian or Pacific Islander")["value"]) == 1
    assert (hawaii["suppressed"] == "N").all()

    # Grade 9 is 2 and grade 11 is 1. Grade 10, 12, and 14 are 0.
    indigenous = _rows(combined, "2026", "165170", "American Indian or Alaska Native")
    assert set(indigenous["metric"]) == {
        "student_enrollment_total",
        "student_enrollment_grade_9",
        "student_enrollment_grade_11",
        "student_enrollment_secondary",
    }
    assert float(_metric_value(combined, "2026", "165170", "student_enrollment_total", "American Indian or Alaska Native")["value"]) == 3
    assert float(_metric_value(combined, "2026", "165170", "student_enrollment_grade_9", "American Indian or Alaska Native")["value"]) == 2

    # Prior-year anchors. These are the published counts, including the
    # comma-formatted totals.
    for year, total, preschool in (
        ("2025", 682013, 30754),
        ("2024", 686224, 31467),
    ):
        row = _metric_value(combined, year, "999000", "student_enrollment_total")
        assert float(row["value"]) == total
        assert row["district"] == "All Districts"
        assert row["school_name"] == "---District Total---"
        assert row["suppressed"] == "N"
        assert float(_metric_value(combined, year, "999000", "student_enrollment_preschool")["value"]) == preschool

    # 2024 state All Students primary, from the real KYRC24 fixture row.
    # Every primary grade is comma-formatted. pandas 3 without the
    # StringDtype cleaner drops the aggregate. pandas 2.3.3 already
    # writes 331,857, and this branch restores that sum:
    # 31,467 + 49,681 + 50,859 + 52,241 + 48,560 + 49,321 + 49,728.
    assert float(_metric_value(combined, "2024", "999000", "student_enrollment_primary")["value"]) == 331857

    assert float(_metric_value(combined, "2025", "165000", "student_enrollment_total")["value"]) == 44390
    assert float(_metric_value(combined, "2024", "165000", "student_enrollment_total")["value"]) == 44362
    assert _metric_value(combined, "2025", "165000", "student_enrollment_total")["nces_id"] == "2101860"
    # KYRC24 historical Fayette district NCES ID is blank.
    assert pd.isna(_metric_value(combined, "2024", "165000", "student_enrollment_total")["nces_id"])

    assert float(_metric_value(combined, "2025", "165170", "student_enrollment_total")["value"]) == 2262
    assert float(_metric_value(combined, "2024", "165170", "student_enrollment_total")["value"]) == 2266
    assert _metric_value(combined, "2025", "165170", "student_enrollment_total")["nces_id"] == "210186000000"
    assert _metric_value(combined, "2024", "165170", "student_enrollment_total")["nces_id"] == "210186000388"

    assert float(_metric_value(combined, "2025", "001010", "student_enrollment_total")["value"]) == 860
    assert float(_metric_value(combined, "2024", "001010", "student_enrollment_total")["value"]) == 847
    assert _metric_value(combined, "2025", "001010", "student_enrollment_total")["nces_id"] == "210003000000"
    assert _metric_value(combined, "2024", "001010", "student_enrollment_total")["nces_id"] == "210003000001"
    assert "student_enrollment_grade_14" not in set(_rows(combined, "2025", "001010", "All Students")["metric"])
    assert "student_enrollment_grade_14" not in set(_rows(combined, "2024", "001010", "All Students")["metric"])

    # KYRC25 Bryan Station Native Hawaiian publishes 1 in grades 10 and 11.
    # Grades 9, 12, and 14 are 0. Elementary grades are blank.
    hawaii_2025 = _rows(combined, "2025", "165170", "Native Hawaiian or Pacific Islander")
    assert set(hawaii_2025["metric"]) == {
        "student_enrollment_total",
        "student_enrollment_grade_10",
        "student_enrollment_grade_11",
        "student_enrollment_secondary",
    }
    assert float(_metric_value(combined, "2025", "165170", "student_enrollment_grade_10", "Native Hawaiian or Pacific Islander")["value"]) == 1
    assert float(_metric_value(combined, "2025", "165170", "student_enrollment_total", "Native Hawaiian or Pacific Islander")["value"]) == 2

    # 2022 and 2023 state rows are district State and school ---State Total---,
    # school code 999. Totals have no thousands separator in the source file.
    for year, total in (("2023", 687294), ("2022", 685401)):
        row = _metric_value(combined, year, "999", "student_enrollment_total")
        assert float(row["value"]) == total
        assert row["district"] == "State"
        assert row["school_name"] == "---State Total---"
        assert row["suppressed"] == "N"