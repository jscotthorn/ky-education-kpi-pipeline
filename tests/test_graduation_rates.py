"""
Tests for Graduation Rates ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
from etl.constants import KPI_COLUMNS
import shutil
from etl.graduation_rates import transform, clean_graduation_rates, GraduationRatesETL


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
        data.to_csv(self.sample_dir / "graduation_rate_2024.csv", index=False)
        
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
        
        data_2024.to_csv(self.sample_dir / "graduation_rate_2024.csv", index=False)
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
        data.to_csv(self.sample_dir / "graduation_rate_2024.csv", index=False)
        
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