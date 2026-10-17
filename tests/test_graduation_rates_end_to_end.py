"""
End-to-end validation test for graduation rates ETL pipeline.
Tests that sample rows from each source file are correctly transformed to KPI format.
"""
import pytest
import pandas as pd
import numpy as np
from etl.constants import KPI_COLUMNS
from pathlib import Path
from etl.graduation_rates import transform, GraduationRatesETL


class TestGraduationRatesEndToEnd:
    """Test complete transformation from raw data to KPI format."""
    
    def test_source_to_kpi_transformation(self):
        """Test that 10 random rows from each source file are correctly represented in processed file."""
        # Paths to actual data files
        raw_data_dir = Path("data/raw/graduation_rates")
        processed_file = Path("data/processed/graduation_rates.csv")
        
        # Ensure processed file exists
        if not processed_file.exists():
            pytest.skip("Processed graduation_rates.csv not found. Run ETL pipeline first.")
        
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
        
        # Process sample rows through the same transformation pipeline
        processed_sample = self._process_sample_rows(sample_rows, source_file.name)
        
        # Validate each processed row exists in KPI data
        for _, expected_row in processed_sample.iterrows():
            self._validate_kpi_row_exists(expected_row, kpi_df, source_file.name)
    
    def _process_sample_rows(self, sample_df: pd.DataFrame, source_filename: str) -> pd.DataFrame:
        """Process sample rows through the transformation pipeline."""
        # Apply the same transformations as the ETL pipeline using BaseETL
        etl = GraduationRatesETL('graduation_rates')
        df = sample_df.copy()
        
        # Apply normalization
        df = etl.normalize_column_names(df)
        df = etl.standardize_missing_values(df)
        
        # Add source file for tracking
        df['source_file'] = source_filename
        
        # Convert to KPI format
        kpi_df = etl.convert_to_kpi_format(df, source_filename)
        
        return kpi_df
    
    def _validate_kpi_row_exists(self, expected_row: pd.Series, kpi_df: pd.DataFrame, source_filename: str):
        """Validate that a specific KPI row exists in the processed data."""
        # Build query conditions - be more flexible with matching
        conditions = []
        
        # Match on key identifying fields
        if 'school_id' in expected_row and pd.notna(expected_row['school_id']):
            # Convert to string for comparison
            school_id_str = str(expected_row['school_id'])
            conditions.append(f"school_id == '{school_id_str}'")
        
        if 'year' in expected_row and pd.notna(expected_row['year']):
            conditions.append(f"year == '{expected_row['year']}'")
        
        if 'student_group' in expected_row and pd.notna(expected_row['student_group']):
            # Escape single quotes in student group names
            student_group = str(expected_row['student_group']).replace("'", "\\'")
            conditions.append(f"student_group == '{student_group}'")
        
        if 'metric' in expected_row and pd.notna(expected_row['metric']):
            conditions.append(f"metric == '{expected_row['metric']}'")
        
        if 'source_file' in expected_row and pd.notna(expected_row['source_file']):
            conditions.append(f"source_file == '{expected_row['source_file']}'")
        
        # Use direct matching for reliability
        matching_rows = self._direct_match(expected_row, kpi_df)
        
        # Assert that we found matching rows
        assert len(matching_rows) > 0, (
            f"No KPI row found for {source_filename} with conditions: {conditions}\n"
            f"Expected: {expected_row.to_dict()}\n"
            f"Available school_ids for this student_group: {self._get_debug_info(expected_row, kpi_df)}"
        )
        
        # Validate the value is correct (within tolerance for floating point)
        if 'value' in expected_row and pd.notna(expected_row['value']):
            expected_value = float(expected_row['value'])
            actual_values = matching_rows['value'].values
            
            # Check if any matching row has the expected value (within tolerance)
            value_match = any(abs(actual_val - expected_value) < 0.01 for actual_val in actual_values)
            
            assert value_match, (
                f"Value mismatch for {source_filename}. "
                f"Expected: {expected_value}, Found: {actual_values.tolist()}"
            )
        
        print(f"✓ Validated KPI row: {expected_row['school_id']} - {expected_row['student_group']} - {expected_row['metric']} = {expected_row['value']}")
    
    def _direct_match(self, expected_row: pd.Series, kpi_df: pd.DataFrame) -> pd.DataFrame:
        """Direct matching fallback when query fails."""
        mask = pd.Series(True, index=kpi_df.index)
        
        if 'school_id' in expected_row and pd.notna(expected_row['school_id']):
            # Handle both string and numeric school_id comparisons
            expected_school_id = str(expected_row['school_id'])
            # Remove .0 if present in expected value
            if expected_school_id.endswith('.0'):
                expected_school_id = expected_school_id[:-2]
            # Convert KPI school_id to int then string to remove .0
            mask &= kpi_df['school_id'].astype(int).astype(str) == expected_school_id
        
        if 'year' in expected_row and pd.notna(expected_row['year']):
            # Convert year to int for comparison  
            expected_year = int(expected_row['year'])
            mask &= kpi_df['year'] == expected_year
        
        if 'student_group' in expected_row and pd.notna(expected_row['student_group']):
            mask &= kpi_df['student_group'] == str(expected_row['student_group'])
        
        if 'metric' in expected_row and pd.notna(expected_row['metric']):
            mask &= kpi_df['metric'] == str(expected_row['metric'])
        
        if 'source_file' in expected_row and pd.notna(expected_row['source_file']):
            mask &= kpi_df['source_file'] == str(expected_row['source_file'])
        
        return kpi_df[mask]
    
    def _get_debug_info(self, expected_row: pd.Series, kpi_df: pd.DataFrame) -> list:
        """Get debug information for troubleshooting."""
        if 'student_group' in expected_row and 'source_file' in expected_row:
            matching_df = kpi_df[
                (kpi_df['student_group'] == expected_row['student_group']) &
                (kpi_df['source_file'] == expected_row['source_file'])
            ]
            return matching_df['school_id'].unique().tolist()[:5]  # Return first 5 for brevity
        return []


class TestGraduationRatesDataQuality:
    """Test data quality of the processed graduation rates."""
    
    def test_kpi_format_compliance(self):
        """Test that processed file follows KPI format requirements."""
        processed_file = Path("data/processed/graduation_rates.csv")
        
        if not processed_file.exists():
            pytest.skip("Processed graduation_rates.csv not found. Run ETL pipeline first.")
        
        kpi_df = pd.read_csv(processed_file)
        
        # Test required columns exist
        required_columns = KPI_COLUMNS
        
        for col in required_columns:
            assert col in kpi_df.columns, f"Required column '{col}' missing from KPI file"
        
        # Test data types
        assert pd.api.types.is_numeric_dtype(kpi_df['value']) or kpi_df['value'].dtype == 'object', "Value column should be numeric or allow NaN"
        assert pd.api.types.is_object_dtype(kpi_df['metric']), "Metric column should be string"
        assert pd.api.types.is_object_dtype(kpi_df['student_group']), "Student_group column should be string"
        assert pd.api.types.is_object_dtype(kpi_df['suppressed']), "Suppressed column should be string"
        
        # Test suppressed column has valid values
        valid_suppressed_values = {'Y', 'N'}
        assert set(kpi_df['suppressed'].unique()).issubset(valid_suppressed_values), f"Suppressed column should only contain Y/N values"
        
        # Test no completely empty rows (excluding suppressed records)
        non_suppressed = kpi_df[kpi_df['suppressed'] == 'N']
        if len(non_suppressed) > 0:
            assert not non_suppressed.isnull().all(axis=1).any(), "Found completely empty non-suppressed rows"
        
        # Test graduation rate values are reasonable (excluding suppressed records)
        grad_rate_rows = kpi_df[(kpi_df['metric'].str.contains('graduation_rate', na=False)) & (kpi_df['suppressed'] == 'N')]
        if len(grad_rate_rows) > 0:
            assert grad_rate_rows['value'].min() >= 0, "Graduation rates should be >= 0"
            assert grad_rate_rows['value'].max() <= 100, "Graduation rates should be <= 100"
        
        # Test that suppressed records have NaN values
        suppressed_rows = kpi_df[kpi_df['suppressed'] == 'Y']
        if len(suppressed_rows) > 0:
            assert suppressed_rows['value'].isna().all(), "Suppressed records should have NaN values"
    
    def test_metric_coverage(self):
        """Test that expected metrics are present."""
        processed_file = Path("data/processed/graduation_rates.csv")
        
        if not processed_file.exists():
            pytest.skip("Processed graduation_rates.csv not found. Run ETL pipeline first.")
        
        kpi_df = pd.read_csv(processed_file)
        
        # Test expected metrics exist
        metrics = kpi_df['metric'].unique()
        
        # Rate metrics should always exist
        assert 'graduation_rate_4_year' in metrics, "4-year graduation rate metric missing"
        
        # 5-year rate should exist for some years (2021 data has it)
        if len(kpi_df[kpi_df['source_file'].str.contains('2021', na=False)]) > 0:
            assert 'graduation_rate_5_year' in metrics, "5-year graduation rate metric missing from 2021 data"
        
        # Count metrics should exist for files that have count data (2021 data)
        count_data_files = kpi_df[kpi_df['source_file'].str.contains('2021', na=False)]
        if len(count_data_files) > 0:
            assert 'graduation_count_4_year' in metrics, "4-year graduation count metric missing from 2021 data"
            assert 'graduation_total_4_year' in metrics, "4-year graduation total metric missing from 2021 data"
            assert 'graduation_count_5_year' in metrics, "5-year graduation count metric missing from 2021 data"
            assert 'graduation_total_5_year' in metrics, "5-year graduation total metric missing from 2021 data"
        
        print(f"Found metrics: {list(metrics)}")
        
        # Test metric naming convention
        rate_metrics = [m for m in metrics if m.endswith('_rate_4_year') or m.endswith('_rate_5_year')]
        count_metrics = [m for m in metrics if m.endswith('_count_4_year') or m.endswith('_count_5_year')]
        total_metrics = [m for m in metrics if m.endswith('_total_4_year') or m.endswith('_total_5_year')]
        
        print(f"Rate metrics: {len(rate_metrics)}")
        print(f"Count metrics: {len(count_metrics)}")
        print(f"Total metrics: {len(total_metrics)}")
        
        # Verify count and total metrics have integer values (excluding suppressed records)
        if count_metrics:
            count_data = kpi_df[kpi_df['metric'].isin(count_metrics)]
            
            # Test non-suppressed records have integer values
            non_suppressed_counts = count_data[count_data['suppressed'] == 'N']['value']
            if len(non_suppressed_counts) > 0:
                assert all(non_suppressed_counts % 1 == 0), "Non-suppressed count metrics should have integer values"
            
            # Test suppressed records have NaN values
            suppressed_counts = count_data[count_data['suppressed'] == 'Y']['value']
            if len(suppressed_counts) > 0:
                assert all(suppressed_counts.isna()), "Suppressed count metrics should have NaN values"
            
        if total_metrics:
            total_data = kpi_df[kpi_df['metric'].isin(total_metrics)]
            
            # Test non-suppressed records have integer values
            non_suppressed_totals = total_data[total_data['suppressed'] == 'N']['value']
            if len(non_suppressed_totals) > 0:
                assert all(non_suppressed_totals % 1 == 0), "Non-suppressed total metrics should have integer values"
            
            # Test suppressed records have NaN values
            suppressed_totals = total_data[total_data['suppressed'] == 'Y']['value']
            if len(suppressed_totals) > 0:
                assert all(suppressed_totals.isna()), "Suppressed total metrics should have NaN values"
    
    def test_source_file_tracking(self):
        """Test that source file tracking is working correctly."""
        processed_file = Path("data/processed/graduation_rates.csv")
        
        if not processed_file.exists():
            pytest.skip("Processed graduation_rates.csv not found. Run ETL pipeline first.")
        
        kpi_df = pd.read_csv(processed_file)
        
        # Test source files are tracked
        source_files = kpi_df['source_file'].unique()
        
        expected_files = [
            'KYRC24_ACCT_4_Year_High_School_Graduation.csv',
            'graduation_rate_2021.csv',
            'graduation_rate_2022.csv', 
            'graduation_rate_2023.csv'
        ]
        
        for expected_file in expected_files:
            assert expected_file in source_files, f"Expected source file {expected_file} not found in tracking"
        
        print(f"Source files tracked: {list(source_files)}")
    
    def test_student_group_consistency(self):
        """Test that student groups are consistently named."""
        processed_file = Path("data/processed/graduation_rates.csv")
        
        if not processed_file.exists():
            pytest.skip("Processed graduation_rates.csv not found. Run ETL pipeline first.")
        
        kpi_df = pd.read_csv(processed_file)
        
        # Test common student groups exist
        student_groups = kpi_df['student_group'].unique()
        
        expected_groups = ['All Students', 'Female', 'Male']
        
        for expected_group in expected_groups:
            assert expected_group in student_groups, f"Expected student group '{expected_group}' not found"
        
        # Test no null or empty student groups
        assert not kpi_df['student_group'].isnull().any(), "Found null student groups"
        assert not (kpi_df['student_group'] == '').any(), "Found empty student groups"
        
        print(f"Student groups found: {len(student_groups)} unique groups")
        print(f"Sample groups: {list(student_groups)[:10]}")
    
    def test_expanded_kpi_format(self):
        """Test that expanded KPI format with counts and totals is working correctly."""
        processed_file = Path("data/processed/graduation_rates.csv")
        
        if not processed_file.exists():
            pytest.skip("Processed graduation_rates.csv not found. Run ETL pipeline first.")
        
        kpi_df = pd.read_csv(processed_file)
        
        # Focus on 2021 data which should have count information
        data_2021 = kpi_df[kpi_df['source_file'].str.contains('2021', na=False)]
        
        if len(data_2021) == 0:
            pytest.skip("No 2021 data found (count data unavailable)")
        
        # Test that we have rate, count, and total metrics for the same school/group combinations
        # Group by school_id, student_group, and year
        grouped = data_2021.groupby(['school_id', 'student_group', 'year'])
        
        complete_metric_sets = 0
        
        for (school_id, student_group, year), group in grouped:
            metrics = set(group['metric'].values)
            
            # Check for 4-year graduation metrics
            if 'graduation_rate_4_year' in metrics:
                has_count = 'graduation_count_4_year' in metrics
                has_total = 'graduation_total_4_year' in metrics
                
                if has_count and has_total:
                    complete_metric_sets += 1
                    
                    # Verify rate calculation consistency
                    rate_row = group[group['metric'] == 'graduation_rate_4_year']
                    count_row = group[group['metric'] == 'graduation_count_4_year']
                    total_row = group[group['metric'] == 'graduation_total_4_year']
                    
                    if len(rate_row) > 0 and len(count_row) > 0 and len(total_row) > 0:
                        rate_value = rate_row['value'].iloc[0]
                        count_value = count_row['value'].iloc[0]
                        total_value = total_row['value'].iloc[0]
                        
                        # Calculate expected rate
                        if total_value > 0:
                            expected_rate = (count_value / total_value) * 100
                            # Allow for small floating point differences
                            assert abs(rate_value - expected_rate) < 0.1, (
                                f"Rate calculation inconsistent for {school_id}-{student_group}: "
                                f"Rate={rate_value}, Count={count_value}, Total={total_value}, "
                                f"Expected Rate={expected_rate:.2f}"
                            )
        
        print(f"Found {complete_metric_sets} complete metric sets (rate + count + total)")
        assert complete_metric_sets > 0, "No complete metric sets found in 2021 data"


FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "graduation_rates"

# 4-year anchors the base-branch parser already emitted. A full-file run of
# that parser wrote 7,606 KPI rows from the KYRC24 4-year file and 7,622 from
# the KYRC25 4-year file. These are those files' state, district, and school
# All Students rates.
FOUR_YEAR_ANCHORS = {
    ("2024", "999000"): "92.3",
    ("2024", "165000"): "92.8",
    ("2024", "165170"): "94.1",
    ("2025", "999000"): "93.6",
    ("2025", "165000"): "92.6",
    ("2025", "165170"): "93.7",
}

# The base parser emitted no rows from ``5 Year Cohort Graduation Rate``.
# These are the source values that header carries, locked once it is mapped.
FIVE_YEAR_ANCHORS = {
    ("2024", "999000"): "93.4",
    ("2024", "165170"): "91.6",
    ("2025", "999000"): "94.2",
    ("2025", "165000"): "94.4",
    ("2025", "165170"): "95.2",
}


def _run_fixtures(tmp_path: Path, filenames: list[str]) -> pd.DataFrame:
    """Copy committed real-row excerpts into a raw directory and transform them."""
    raw_dir = tmp_path / "raw"
    source_dir = raw_dir / "graduation_rates"
    source_dir.mkdir(parents=True)
    proc_dir = tmp_path / "processed"
    proc_dir.mkdir()
    for name in filenames:
        (source_dir / name).write_bytes((FIXTURE_DIR / name).read_bytes())
    transform(raw_dir, proc_dir, {"derive": {"processing_date": "2026-10-17"}})
    output = proc_dir / "graduation_rates.csv"
    assert output.exists(), filenames
    return pd.read_csv(output, dtype=str)


def _comparable(df: pd.DataFrame) -> pd.DataFrame:
    """Drop the processing timestamp and sort so two runs can be compared."""
    columns = [column for column in df.columns if column != "last_updated"]
    return df[columns].sort_values(columns).reset_index(drop=True)


def _metric_rows(
    df: pd.DataFrame, year: str, school_id: str, metric: str, student_group: str
) -> pd.DataFrame:
    return df[
        (df["year"] == year)
        & (df["school_id"] == school_id)
        & (df["metric"] == metric)
        & (df["student_group"] == student_group)
    ]


def _metric_value(
    df: pd.DataFrame, year: str, school_id: str, metric: str, student_group: str
) -> pd.Series:
    matches = _metric_rows(df, year, school_id, metric, student_group)
    assert len(matches) == 1, (year, school_id, metric, student_group, len(matches))
    return matches.iloc[0]


def test_leftover_temporary_file_does_not_duplicate_2024(tmp_path):
    """A leftover Graduation_Rate_2024.CSV must not add a second 2024 row.

    KYRC24 cohort files are the 2023-24 source. The temporary row uses
    different rates so a duplicate would be visible. State anchors stay
    92.3 (4-year) and 93.4 (5-year).
    """
    raw_dir = tmp_path / "raw"
    source_dir = raw_dir / "graduation_rates"
    source_dir.mkdir(parents=True)
    proc_dir = tmp_path / "processed"
    proc_dir.mkdir()
    for name in (
        "KYRC24_ACCT_4_Year_High_School_Graduation.csv",
        "KYRC24_ACCT_5_Year_High_School_Graduation.csv",
    ):
        (source_dir / name).write_bytes((FIXTURE_DIR / name).read_bytes())
    (source_dir / "Graduation_Rate_2024.CSV").write_text(
        "School Year,School Code,District Name,School Name,Demographic,"
        "Suppressed 4-Year,4-Year Graduation Rate,Suppressed 5-Year,5-Year Graduation Rate\n"
        "20232024,999000,All Districts,All Schools,All Students,N,50.0,N,51.0\n"
    )
    transform(raw_dir, proc_dir, {"derive": {"processing_date": "2026-10-17"}})
    output = pd.read_csv(proc_dir / "graduation_rates.csv", dtype=str)

    assert set(output["source_file"]) == {
        "KYRC24_ACCT_4_Year_High_School_Graduation.csv",
        "KYRC24_ACCT_5_Year_High_School_Graduation.csv",
    }
    assert "50.0" not in set(output["value"].dropna())
    assert "51.0" not in set(output["value"].dropna())
    assert _metric_value(
        output, "2024", "999000", "graduation_rate_4_year", "All Students"
    )["value"] == "92.3"
    assert _metric_value(
        output, "2024", "999000", "graduation_rate_5_year", "All Students"
    )["value"] == "93.4"


def test_leftover_temporary_file_does_not_duplicate_2025(tmp_path):
    """A leftover Graduation_Rate_2025.CSV must not add a second 2025 row.

    The permanent KYRC25 fixtures keep state 93.6 and 94.2. The temporary
    row uses different rates so a duplicate would be visible.
    """
    raw_dir = tmp_path / "raw"
    source_dir = raw_dir / "graduation_rates"
    source_dir.mkdir(parents=True)
    proc_dir = tmp_path / "processed"
    proc_dir.mkdir()
    for name in (
        "KYRC25_ACCT_4_Year_High_School_Graduation.csv",
        "KYRC25_ACCT_5_Year_High_School_Graduation.csv",
    ):
        (source_dir / name).write_bytes((FIXTURE_DIR / name).read_bytes())
    (source_dir / "Graduation_Rate_2025.CSV").write_text(
        "School Year,School Code,District Name,School Name,Demographic,"
        "Suppressed 4-Year,4-Year Graduation Rate,Suppressed 5-Year,5-Year Graduation Rate\n"
        "20242025,999000,All Districts,All Schools,All Students,N,50.0,N,51.0\n"
    )
    transform(raw_dir, proc_dir, {"derive": {"processing_date": "2026-10-17"}})
    output = pd.read_csv(proc_dir / "graduation_rates.csv", dtype=str)

    assert set(output["source_file"]) == {
        "KYRC25_ACCT_4_Year_High_School_Graduation.csv",
        "KYRC25_ACCT_5_Year_High_School_Graduation.csv",
    }
    assert "50.0" not in set(output["value"].dropna())
    assert "51.0" not in set(output["value"].dropna())
    assert _metric_value(
        output, "2025", "999000", "graduation_rate_4_year", "All Students"
    )["value"] == "93.6"
    assert _metric_value(
        output, "2025", "999000", "graduation_rate_5_year", "All Students"
    )["value"] == "94.2"
    assert _metric_value(
        output, "2025", "165000", "graduation_rate_4_year", "All Students"
    )["value"] == "92.6"
    assert _metric_value(
        output, "2025", "165170", "graduation_rate_5_year", "All Students"
    )["value"] == "95.2"


def test_blank_suppression_is_skipped(tmp_path):
    """A blank Suppressed cell is missing. A Suppressed=Y cell is kept.

    Bryan Station American Indian, 4-year, was ``*`` / Y in KYRC25 and is
    blank / blank in KYRC26, so 2026 emits no row. The blank pattern is not
    new: KYRC25 already uses it for that school's Military Dependent row and
    for Adair County High School American Indian. KYRC24 stores the empty
    rate as one space and that row is skipped too.
    """
    output = _run_fixtures(
        tmp_path,
        [
            "KYRC24_ACCT_4_Year_High_School_Graduation.csv",
            "KYRC24_ACCT_5_Year_High_School_Graduation.csv",
            "KYRC25_ACCT_4_Year_High_School_Graduation.csv",
            "KYRC26_ACCT_4_Year_High_School_Graduation.csv",
            "KYRC26_ACCT_5_Year_High_School_Graduation.csv",
        ],
    )
    absent = "American Indian or Alaska Native"
    for year, school_id, metric in (
        ("2026", "165170", "graduation_rate_4_year"),
        ("2026", "001010", "graduation_rate_4_year"),
        ("2025", "001010", "graduation_rate_4_year"),
        ("2024", "001010", "graduation_rate_4_year"),
        ("2024", "001010", "graduation_rate_5_year"),
    ):
        assert _metric_rows(output, year, school_id, metric, absent).empty

    assert _metric_rows(
        output, "2025", "165170", "graduation_rate_4_year", "Military Dependent"
    ).empty
    assert _metric_rows(
        output, "2026", "165170", "graduation_rate_5_year", "Military Dependent"
    ).empty

    prior = _metric_value(
        output, "2025", "165170", "graduation_rate_4_year", absent
    )
    assert prior["suppressed"] == "Y"
    assert pd.isna(prior["value"])
    assert prior["source_file"] == "KYRC25_ACCT_4_Year_High_School_Graduation.csv"

    current = _metric_value(
        output, "2026", "165170", "graduation_rate_4_year", "Military Dependent"
    )
    assert current["suppressed"] == "Y"
    assert pd.isna(current["value"])
    assert current["source_file"] == "KYRC26_ACCT_4_Year_High_School_Graduation.csv"


def test_kyrc26_real_rows_and_prior_year_unchanged(tmp_path):
    """2025-26 KPIs from real cohort rows, with fixed earlier-year anchors.

    State rates are 94.1 (4-year) and 94.5 (5-year). KPI year is 2026, from
    school year 20252026. The 4-year anchors are values the base parser
    already wrote. The 5-year anchors are source values from the cohort
    header that parser did not map.
    """
    prior_names = [
        "KYRC24_ACCT_4_Year_High_School_Graduation.csv",
        "KYRC24_ACCT_5_Year_High_School_Graduation.csv",
        "KYRC25_ACCT_4_Year_High_School_Graduation.csv",
        "KYRC25_ACCT_5_Year_High_School_Graduation.csv",
    ]
    current_names = prior_names + [
        "KYRC26_ACCT_4_Year_High_School_Graduation.csv",
        "KYRC26_ACCT_5_Year_High_School_Graduation.csv",
    ]
    prior_only = _run_fixtures(tmp_path / "prior", prior_names)
    combined = _run_fixtures(tmp_path / "combined", current_names)

    # Same-branch check only: adding the 2026 excerpts does not rewrite earlier rows.
    # The fixed anchors below are the prior-year regression.
    prior_slice = combined[combined["year"].isin(["2024", "2025"])]
    assert _comparable(prior_slice).equals(_comparable(prior_only))

    assert set(combined["source_file"]) == set(current_names)
    assert "20252026" not in set(combined["year"])
    assert set(combined.loc[combined["source_file"].str.startswith("KYRC26_"), "year"]) == {"2026"}

    state_4 = _metric_value(
        combined, "2026", "999000", "graduation_rate_4_year", "All Students"
    )
    assert state_4["value"] == "94.1"
    assert state_4["suppressed"] == "N"
    assert state_4["district"] == "All Districts"
    assert state_4["school_name"] == "---District Total---"
    assert state_4["source_file"] == "KYRC26_ACCT_4_Year_High_School_Graduation.csv"

    state_5 = _metric_value(
        combined, "2026", "999000", "graduation_rate_5_year", "All Students"
    )
    assert state_5["value"] == "94.5"
    assert state_5["suppressed"] == "N"
    assert state_5["year"] == "2026"
    assert state_5["source_file"] == "KYRC26_ACCT_5_Year_High_School_Graduation.csv"

    district_4 = _metric_value(
        combined, "2026", "165000", "graduation_rate_4_year", "All Students"
    )
    assert district_4["value"] == "93.9"
    assert district_4["district"] == "Fayette County"
    assert district_4["school_name"] == "---District Total---"
    assert district_4["school_id"] == "165000"
    assert district_4["co_op"] == "CKEC"
    assert district_4["co_op_code"] == "901"
    assert district_4["nces_id"] == "2101860"
    assert district_4["suppressed"] == "N"

    district_5 = _metric_value(
        combined, "2026", "165000", "graduation_rate_5_year", "All Students"
    )
    assert district_5["value"] == "93.7"
    assert district_5["school_id"] == "165000"

    school_4 = _metric_value(
        combined, "2026", "165170", "graduation_rate_4_year", "All Students"
    )
    assert school_4["value"] == "93.8"
    assert school_4["district"] == "Fayette County"
    assert school_4["school_name"] == "Bryan Station High School"
    assert school_4["state_school_id"] == "034165170"
    assert school_4["nces_id"] == "210186000388"
    assert school_4["suppressed"] == "N"

    school_5 = _metric_value(
        combined, "2026", "165170", "graduation_rate_5_year", "All Students"
    )
    assert school_5["value"] == "94.0"
    assert school_5["school_name"] == "Bryan Station High School"
    assert school_5["suppressed"] == "N"

    adair = _metric_value(
        combined, "2026", "001010", "graduation_rate_4_year", "All Students"
    )
    assert adair["value"] == "97.6"
    assert adair["school_name"] == "Adair County High School"
    assert adair["school_id"] == "001010"

    for year, school_id, metric, student_group in (
        ("2026", "165170", "graduation_rate_4_year", "Asian"),
        ("2026", "001010", "graduation_rate_4_year", "Foster Care"),
        ("2026", "001010", "graduation_rate_5_year", "Foster Care"),
        ("2026", "999000", "graduation_rate_5_year", "American Indian or Alaska Native"),
        ("2024", "999000", "graduation_rate_4_year", "Military Dependent"),
    ):
        row = _metric_value(combined, year, school_id, metric, student_group)
        assert row["suppressed"] == "Y"
        assert pd.isna(row["value"])

    # State 5-year American Indian was published in KYRC25 and is suppressed in KYRC26.
    prior_state_group = _metric_value(
        prior_only,
        "2025",
        "999000",
        "graduation_rate_5_year",
        "American Indian or Alaska Native",
    )
    assert prior_state_group["value"] == "88.0"
    assert prior_state_group["suppressed"] == "N"

    for (year, school_id), expected in FOUR_YEAR_ANCHORS.items():
        row = _metric_value(
            prior_only, year, school_id, "graduation_rate_4_year", "All Students"
        )
        assert row["value"] == expected
        assert row["suppressed"] == "N"
    for (year, school_id), expected in FIVE_YEAR_ANCHORS.items():
        row = _metric_value(
            prior_only, year, school_id, "graduation_rate_5_year", "All Students"
        )
        assert row["value"] == expected
        assert row["suppressed"] == "N"


if __name__ == "__main__":
    # Run specific test
    test = TestGraduationRatesEndToEnd()
    test.test_source_to_kpi_transformation()
    
    # Run data quality tests
    quality_test = TestGraduationRatesDataQuality()
    quality_test.test_kpi_format_compliance()
    quality_test.test_metric_coverage()
    quality_test.test_source_file_tracking()
    quality_test.test_student_group_consistency()
    
    print("All tests passed!")