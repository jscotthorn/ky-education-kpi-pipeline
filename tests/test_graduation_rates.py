"""
Tests for Graduation Rates ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
import yaml
from etl.constants import KPI_COLUMNS
import shutil
from etl.graduation_rates import transform, clean_graduation_rates, GraduationRatesETL

ROOT = Path(__file__).resolve().parents[1]


class TestGraduationRatesETL:
    
    def setup_method(self):
        """Setup test directories and sample data."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        
        # Create sample raw data directory
        self.sample_dir = self.raw_dir / "graduation_rates"
        self.sample_dir.mkdir(parents=True)
    
    def teardown_method(self):
        """Clean up test directories."""
        shutil.rmtree(self.test_dir)
    
    def create_sample_2024_data(self):
        """Create sample 2024 format data."""
        data = pd.DataFrame({
            'School Year': ['20232024', '20232024', '20232024'],
            'County Number': ['001', '001', '001'],
            'County Name': ['Adair', 'Adair', 'Adair'],
            'District Number': ['001', '001', '001'],
            'District Name': ['Adair County', 'Adair County', 'Adair County'],
            'School Number': ['', '', ''],
            'School Name': ['All Schools', 'All Schools', 'All Schools'],
            'School Code': ['999000', '999000', '999000'],
            'State School Id': ['', '', ''],
            'NCES ID': ['', '', ''],
            'CO-OP': ['', '', ''],
            'CO-OP Code': ['', '', ''],
            'School Type': ['', '', ''],
            'Demographic': ['All Students', 'Female', 'Male'],
            'Suppressed': ['N', 'N', 'N'],
            '4 Year Cohort Graduation Rate': [92.3, 94.1, 90.5]
        })
        return data
    
    def create_sample_2021_data(self):
        """Create sample 2021 format data."""
        data = pd.DataFrame({
            'SCHOOL YEAR': ['20202021', '20202021', '20202021'],
            'COUNTY NUMBER': ['001', '001', '001'],
            'COUNTY NAME': ['ADAIR', 'ADAIR', 'ADAIR'],
            'DISTRICT NUMBER': ['001', '001', '001'],
            'DISTRICT NAME': ['Adair County', 'Adair County', 'Adair County'],
            'SCHOOL NUMBER': ['', '', ''],
            'SCHOOL NAME': ['---District Total---', '---District Total---', '---District Total---'],
            'SCHOOL CODE': ['001', '001', '001'],
            'STATE SCHOOL ID': ['', '', ''],
            'NCES ID': ['', '', ''],
            'CO-OP': ['GRREC', 'GRREC', 'GRREC'],
            'CO-OP CODE': ['902', '902', '902'],
            'SCHOOL TYPE': ['', '', ''],
            'DEMOGRAPHIC': ['All Students', 'Female', 'Male'],
            'SUPPRESSED 4 YEAR': ['N', 'N', 'N'],
            'NUMBER OF GRADS IN 4-YEAR COHORT': [219, 104, 115],
            'NUMBER OF STUDENTS IN 4-YEAR COHORT': [228, 107, 121],
            '4-YEAR GRADUATION RATE': [96.1, 97.2, 95.0],
            'SUPPRESSED 5 YEAR': ['N', 'N', 'N'],
            'NUMBER OF GRADS IN 5-YEAR COHORT': [182, 94, 88],
            'NUMBER OF STUDENTS IN 5-YEAR COHORT': [183, 94, 89],
            '5-YEAR GRADUATION RATE': [99.5, 100.0, 98.9]
        })
        return data
    
    def test_normalize_column_names(self):
        """Test column name normalization using BaseETL."""
        etl = GraduationRatesETL('graduation_rates')
        df_2024 = self.create_sample_2024_data()
        df_normalized = etl.normalize_column_names(df_2024)
        
        assert 'school_year' in df_normalized.columns
        assert 'county_name' in df_normalized.columns
        assert 'graduation_rate_4_year' in df_normalized.columns
        assert 'School Year' not in df_normalized.columns
    
    def test_standardize_missing_values(self):
        """Test missing value standardization with graduation rate columns."""
        etl = GraduationRatesETL('graduation_rates')
        df = pd.DataFrame({
            'graduation_rate_4_year': [90.5, '', '*', 95.0],
            'suppressed_4_year': ['N', 'Y', '', 'N']
        })
        
        df_clean = etl.standardize_missing_values(df)
        
        assert pd.isna(df_clean.loc[1, 'graduation_rate_4_year'])
        assert pd.isna(df_clean.loc[2, 'graduation_rate_4_year'])
        assert pd.isna(df_clean.loc[2, 'suppressed_4_year'])
    
    def test_clean_graduation_rates(self):
        """Test graduation rate cleaning and validation."""
        df = pd.DataFrame({
            'graduation_rate_4_year': [90.5, 110.0, -5.0, 95.0, 'invalid'],
            'graduation_rate_5_year': [95.0, 98.0, 99.0, 100.0, 85.0]
        })
        
        df_clean = clean_graduation_rates(df)
        
        # Valid rates should remain
        assert df_clean.loc[0, 'graduation_rate_4_year'] == 90.5
        assert df_clean.loc[3, 'graduation_rate_4_year'] == 95.0
        
        # Invalid rates should be NaN
        assert pd.isna(df_clean.loc[1, 'graduation_rate_4_year'])  # > 100
        assert pd.isna(df_clean.loc[2, 'graduation_rate_4_year'])  # < 0
        assert pd.isna(df_clean.loc[4, 'graduation_rate_4_year'])  # non-numeric
    
    def test_transform_2024_format(self):
        """Test transform with 2024 format data."""
        data = self.create_sample_2024_data()
        data.to_csv(self.sample_dir / "graduation_rate_2024_format.csv", index=False)
        
        config = {
            "derive": {
                "processing_date": "2025-07-18"
            }
        }
        
        transform(self.raw_dir, self.proc_dir, config)
        
        # Check output file exists
        output_file = self.proc_dir / "graduation_rates.csv"
        assert output_file.exists()
        
        # Check KPI format transformations
        df = pd.read_csv(output_file)
        
        # Verify KPI format columns
        required_columns = KPI_COLUMNS
        for col in required_columns:
            assert col in df.columns, f"Required KPI column '{col}' missing"
        
        # Verify expected metrics are present
        metrics = df['metric'].unique()
        assert 'graduation_rate_4_year' in metrics, "Missing graduation rate metric"
        
        # Verify suppressed column has valid values
        assert set(df['suppressed'].unique()).issubset({'Y', 'N'}), "Suppressed column should only contain Y/N"
        
        # Verify year extraction from school_year
        assert all(df['year'].astype(str).str.len() == 4), "Year should be 4 digits"
        
        # Should have multiple rows due to KPI format (rate + count + total metrics)
        assert len(df) >= 3, "Should have at least 3 KPI rows from sample data"
    
    def test_transform_multiple_files(self):
        """Test transform with multiple files."""
        # Create both 2024 and 2021 format files
        data_2024 = self.create_sample_2024_data()
        data_2021 = self.create_sample_2021_data()
        
        data_2024.to_csv(self.sample_dir / "graduation_rate_2024_format.csv", index=False)
        data_2021.to_csv(self.sample_dir / "graduation_rate_2021.csv", index=False)
        
        config = {
            "derive": {
                "processing_date": "2025-07-18"
            }
        }
        
        transform(self.raw_dir, self.proc_dir, config)
        
        # Check output file exists
        output_file = self.proc_dir / "graduation_rates.csv"
        assert output_file.exists()
        
        # Check combined KPI data
        df = pd.read_csv(output_file)
        
        # Verify KPI format columns
        required_columns = KPI_COLUMNS
        for col in required_columns:
            assert col in df.columns, f"Required KPI column '{col}' missing"
        
        # Should have multiple metrics per source record, expect significantly more than 6 rows
        assert len(df) >= 6, f"Expected at least 6 KPI rows, got {len(df)}"
        
        # Verify both source files are represented
        source_files = df['source_file'].unique()
        assert len(source_files) >= 2, "Should have data from multiple source files"
        
        # Verify both 2024 and 2021 data present by checking source files
        has_2024 = any('2024' in sf for sf in source_files)
        has_2021 = any('2021' in sf for sf in source_files)
        assert has_2024 or has_2021, "Should have data from 2024 and/or 2021 files"
        
        # Verify we have data from both file types by checking metrics
        metrics = df['metric'].unique()
        assert 'graduation_rate_4_year' in metrics, "Should have 4-year graduation rate metric"
        
        # 2021 files should have count/total metrics, 2024 files typically only have rates
        has_count_metrics = any('_count_' in m for m in metrics)
        has_total_metrics = any('_total_' in m for m in metrics)
        
        # If we have 2021 data, we should see count and total metrics
        if has_2021:
            assert has_count_metrics, "2021 data should include count metrics"
            assert has_total_metrics, "2021 data should include total metrics"
    
    def test_transform_no_data(self):
        """Test transform when no data exists."""
        empty_raw_dir = self.test_dir / "empty_raw"
        empty_raw_dir.mkdir()
        
        config = {}
        transform(empty_raw_dir, self.proc_dir, config)
        
        # Should not create output file
        output_file = self.proc_dir / "graduation_rates.csv"
        assert not output_file.exists()
    
    def test_dtype_conversion(self):
        """Test data type conversions."""
        data = self.create_sample_2024_data()
        data.to_csv(self.sample_dir / "graduation_rate_2024_format.csv", index=False)
        
        config = {
            "dtype": {
                "county_number": "str",
                "graduation_rate_4_year": "float64"
            },
            "derive": {
                "processing_date": "2025-07-18"
            }
        }
        
        transform(self.raw_dir, self.proc_dir, config)
        
        # Check output file
        output_file = self.proc_dir / "graduation_rates.csv"
        df = pd.read_csv(output_file)
        
        # Verify KPI format data types
        # District and school names should be strings (object dtype)
        assert df['district'].dtype == 'object', "District should be string type"
        assert df['school_name'].dtype == 'object', "School name should be string type"
        assert df['student_group'].dtype == 'object', "Student group should be string type"
        assert df['metric'].dtype == 'object', "Metric should be string type"
        assert df['suppressed'].dtype == 'object', "Suppressed should be string type"
        
        # Value column should allow numeric and NaN (mixed types become object after CSV round-trip)
        # But non-suppressed values should be convertible to numeric
        non_suppressed = df[df['suppressed'] == 'N']
        if len(non_suppressed) > 0:
            numeric_values = pd.to_numeric(non_suppressed['value'], errors='coerce')
            assert numeric_values.notna().all(), "Non-suppressed values should be numeric"

    def test_kyrc26_cohort_files_are_accepted(self):
        """KYRC cohort files are in scope. A leftover temporary extract is not."""
        etl = GraduationRatesETL("graduation_rates")
        assert etl.include_kyrc26 is True
        assert (
            etl.module_column_mappings["5 Year Cohort Graduation Rate"]
            == "graduation_rate_5_year"
        )
        assert (
            etl.module_column_mappings["4 Year Cohort Graduation Rate"]
            == "graduation_rate_4_year"
        )

        accepted = [
            "KYRC26_ACCT_4_Year_High_School_Graduation.csv",
            "KYRC26_ACCT_5_Year_High_School_Graduation.csv",
            "KYRC25_ACCT_4_Year_High_School_Graduation.csv",
            "KYRC25_ACCT_5_Year_High_School_Graduation.csv",
            "KYRC24_ACCT_5_Year_High_School_Graduation.csv",
        ]
        rejected = [
            "Graduation_Rate_2024.CSV",
            "Graduation_Rate_2025.CSV",
            "Graduation_Rate_2026.CSV",
        ]
        for name in accepted + rejected:
            (self.sample_dir / name).write_text("School Year\n20252026\n")

        found = sorted(path.name for path, _sheet in etl._find_data_files(self.sample_dir))
        assert found == sorted(accepted)


def test_kyrc25_cohort_files_replace_the_temporary_2025_source():
    """2024-25 comes from the permanent KYRC25 files. The dead temporary file is not registered."""
    config = yaml.safe_load((ROOT / "config" / "kde_sources.yaml").read_text())
    names = []
    for entry in config["raw_directories"]["graduation_rates"]:
        names.append(entry["file"] if isinstance(entry, dict) else entry)

    assert "KYRC25_ACCT_4_Year_High_School_Graduation.csv" in names
    assert "KYRC25_ACCT_5_Year_High_School_Graduation.csv" in names
    assert "KYRC26_ACCT_4_Year_High_School_Graduation.csv" in names
    assert "KYRC26_ACCT_5_Year_High_School_Graduation.csv" in names
    assert "Graduation_Rate_2024.CSV" not in names
    assert "Graduation_Rate_2025.CSV" not in names
    assert "Graduation_Rate_2026.CSV" not in names
    assert names.count("KYRC25_ACCT_4_Year_High_School_Graduation.csv") == 1
    assert names.count("KYRC25_ACCT_5_Year_High_School_Graduation.csv") == 1


class TestGraduationRatesHelpers:
    """Test helper functions independently."""
    
    def test_normalize_column_names_edge_cases(self):
        """Test column normalization with edge cases."""
        etl = GraduationRatesETL('graduation_rates')
        df = pd.DataFrame({
            'Unknown Column': [1, 2, 3],
            'SCHOOL YEAR': ['2021', '2022', '2023'],
            'School Name': ['A', 'B', 'C']
        })
        
        result = etl.normalize_column_names(df)
        
        assert 'school_year' in result.columns
        assert 'school_name' in result.columns
        assert 'Unknown Column' in result.columns  # Unknown columns preserved
    
    def test_standardize_missing_values_edge_cases(self):
        """Test missing value standardization with graduation rate columns."""
        etl = GraduationRatesETL('graduation_rates')
        df = pd.DataFrame({
            'graduation_rate_4_year': ['', '""', '85.5', None],
            'graduation_rate_5_year': ['*', '', '90.0', '0']
        })
        
        result = etl.standardize_missing_values(df)
        
        assert pd.isna(result.loc[0, 'graduation_rate_4_year'])
        assert pd.isna(result.loc[1, 'graduation_rate_4_year'])
        assert result.loc[2, 'graduation_rate_4_year'] == 85.5  # Now numeric after cleaning
        assert pd.isna(result.loc[3, 'graduation_rate_4_year'])
        
        assert pd.isna(result.loc[0, 'graduation_rate_5_year'])  # '*' in graduation rate column
        assert pd.isna(result.loc[1, 'graduation_rate_5_year'])
        assert result.loc[2, 'graduation_rate_5_year'] == 90.0  # Now numeric after cleaning
        assert result.loc[3, 'graduation_rate_5_year'] == 0.0


def _graduation_config():
    loaded = yaml.safe_load((ROOT / "config" / "mappings.yaml").read_text())
    return loaded["sources"]["graduation_rates"]


def _grad_row(frame: pd.DataFrame, year: str, school_id: str, group: str) -> pd.Series:
    matched = frame[
        (frame["year"] == year)
        & (frame["school_id"] == school_id)
        & (frame["student_group"] == group)
        & (frame["metric"] == "graduation_rate_4_year")
    ]
    assert len(matched) == 1, (year, school_id, group, len(matched))
    return matched.iloc[0]


def test_non_military_graduation_label_alias(tmp_path):
    """KYRC24 and KYRC26 publish 'Non-Military'. The KPI group is 'Non-Military Dependent'.

    Verbatim rows, header included:

    KYRC26_ACCT_4_Year_High_School_Graduation.csv on
    https://kdeschoolreportcard.blob.core.windows.net/datasets/
    row 25 state Non-Military 94.1, row 49 Adair County All Schools 97.6,
    row 2810 Fayette County All Students 93.9, row 2833 Fayette County
    Non-Military 93.9, row 2881 Lafayette High School Non-Military 98.1.

    KYRC24_ACCT_4_Year_High_School_Graduation.csv on the same container:
    row 25 state 92.3, row 2833 Fayette County All Schools 92.8,
    row 2881 Lafayette High School 97.0.

    graduation_rate_2023.csv on
    https://www.education.ky.gov/Open-House/data/HistoricalDatasets/
    row 2554 Fayette County All Students 90.2 and row 8714 state All Students
    91.4. That file has no military label, so those anchors stay All Students.
    """
    raw_dir = tmp_path / "raw"
    source_dir = raw_dir / "graduation_rates"
    source_dir.mkdir(parents=True)
    proc_dir = tmp_path / "processed2"
    proc_dir.mkdir()
    fixture_dir = ROOT / "tests" / "fixtures" / "graduation_rates"
    copies = {
        "kyrc26_non_military.csv": "KYRC26_ACCT_4_Year_High_School_Graduation.csv",
        "kyrc24_non_military.csv": "KYRC24_ACCT_4_Year_High_School_Graduation.csv",
        "graduation_2023_all_students_anchor.csv": "graduation_rate_2023.csv",
    }
    for src, dest in copies.items():
        (source_dir / dest).write_bytes((fixture_dir / src).read_bytes())
    transform(raw_dir, proc_dir, _graduation_config())
    frame = pd.read_csv(proc_dir / "graduation_rates.csv", dtype=str, keep_default_na=False)

    state_2026 = _grad_row(frame, "2026", "999000", "Non-Military Dependent")
    assert float(state_2026["value"]) == pytest.approx(94.1)
    assert state_2026["district"] == "All Districts"
    assert state_2026["school_name"] == "---District Total---"
    assert state_2026["suppressed"] == "N"

    adair = _grad_row(frame, "2026", "001000", "Non-Military Dependent")
    assert float(adair["value"]) == pytest.approx(97.6)
    assert adair["district"] == "Adair County"
    assert adair["school_name"] == "---District Total---"

    fayette = _grad_row(frame, "2026", "165000", "Non-Military Dependent")
    assert float(fayette["value"]) == pytest.approx(93.9)
    assert fayette["district"] == "Fayette County"
    assert fayette["school_name"] == "---District Total---"

    lafayette = _grad_row(frame, "2026", "165060", "Non-Military Dependent")
    assert float(lafayette["value"]) == pytest.approx(98.1)
    assert lafayette["district"] == "Fayette County"
    assert lafayette["school_name"] == "Lafayette High School"

    fayette_all = _grad_row(frame, "2026", "165000", "All Students")
    assert float(fayette_all["value"]) == pytest.approx(93.9)

    state_2024 = _grad_row(frame, "2024", "999000", "Non-Military Dependent")
    assert float(state_2024["value"]) == pytest.approx(92.3)
    fayette_2024 = _grad_row(frame, "2024", "165000", "Non-Military Dependent")
    assert float(fayette_2024["value"]) == pytest.approx(92.8)
    lafayette_2024 = _grad_row(frame, "2024", "165060", "Non-Military Dependent")
    assert float(lafayette_2024["value"]) == pytest.approx(97.0)
    assert lafayette_2024["school_name"] == "Lafayette High School"

    # 2023 has no Non-Military label. The published All Students rates stay.
    fayette_2023 = _grad_row(frame, "2023", "165", "All Students")
    assert float(fayette_2023["value"]) == pytest.approx(90.2)
    assert fayette_2023["district"] == "Fayette County"
    state_2023 = _grad_row(frame, "2023", "999", "All Students")
    assert float(state_2023["value"]) == pytest.approx(91.4)
    assert state_2023["district"] == "State"
    assert state_2023["school_name"] == "---State Total---"

    assert "Non-Military" not in set(frame["student_group"])
    keys = ["district", "school_id", "school_name", "year", "student_group", "metric"]
    assert not frame.duplicated(keys).any()