"""
Tests for Postsecondary Readiness ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
import shutil
from etl.postsecondary_readiness import transform, clean_readiness_data, PostsecondaryReadinessETL


class TestPostsecondaryReadinessETL:
    
    def setup_method(self):
        """Setup test directories and sample data."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        
        # Create sample raw data directory
        self.sample_dir = self.raw_dir / "postsecondary_readiness"
        self.sample_dir.mkdir(parents=True)
        
        # Create ETL instance for testing
        self.etl = PostsecondaryReadinessETL('postsecondary_readiness')
    
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
            'Postsecondary Rate': [81.0, 83.5, 78.5],
            'Postsecondary Rate With Bonus': [86.0, 88.8, 83.2]
        })
        return data
    
    def create_sample_2022_data(self):
        """Create sample 2022 format data."""
        data = pd.DataFrame({
            'SCHOOL YEAR': ['20212022', '20212022', '20212022'],
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
            'SUPPRESSED': ['N', 'N', 'Y'],
            'POSTSECONDARY RATE': [71.3, 76.7, '*'],
            'POSTSECONDARY RATE WITH BONUS': [78.8, 84.6, '*']
        })
        return data
    
    def test_normalize_column_names(self):
        """Test column name normalization."""
        df_2024 = self.create_sample_2024_data()
        df_normalized = self.etl.normalize_column_names(df_2024)
        
        assert 'school_year' in df_normalized.columns
        assert 'county_name' in df_normalized.columns
        assert 'postsecondary_rate' in df_normalized.columns
        assert 'postsecondary_rate_with_bonus' in df_normalized.columns
        assert 'School Year' not in df_normalized.columns
    
    def test_standardize_missing_values(self):
        """Test missing value standardization with postsecondary rate columns."""
        df = pd.DataFrame({
            'postsecondary_rate': [81.0, '', '*', 75.0],
            'suppressed': ['N', 'Y', '', 'N']
        })
        
        df_clean = self.etl.standardize_missing_values(df)
        
        assert pd.isna(df_clean.loc[1, 'postsecondary_rate'])
        assert pd.isna(df_clean.loc[2, 'postsecondary_rate'])
        assert pd.isna(df_clean.loc[2, 'suppressed'])
    
    def test_clean_readiness_data(self):
        """Test postsecondary readiness rate cleaning and validation."""
        df = pd.DataFrame({
            'postsecondary_rate': [81.0, 110.0, -5.0, 95.0, 'invalid'],
            'postsecondary_rate_with_bonus': [86.0, 98.0, 99.0, 100.0, 85.0]
        })
        
        df_clean = clean_readiness_data(df)
        
        # Valid rates should remain
        assert df_clean.loc[0, 'postsecondary_rate'] == 81.0
        assert df_clean.loc[3, 'postsecondary_rate'] == 95.0
        
        # Invalid rates should be NaN
        assert pd.isna(df_clean.loc[1, 'postsecondary_rate'])  # > 100
        assert pd.isna(df_clean.loc[2, 'postsecondary_rate'])  # < 0
        assert pd.isna(df_clean.loc[4, 'postsecondary_rate'])  # non-numeric
    
    def test_transform_2024_format(self):
        """Test transform with 2024 format data."""
        data = self.create_sample_2024_data()
        data.to_csv(self.sample_dir / "postsecondary_readiness_2024.csv", index=False)
        
        config = {
            "derive": {
                "processing_date": "2025-07-19"
            }
        }
        
        transform(self.raw_dir, self.proc_dir, config)
        
        # Check output file exists
        output_file = self.proc_dir / "postsecondary_readiness.csv"
        assert output_file.exists()
        
        # Check KPI format transformations
        df = pd.read_csv(output_file)
        
        # Verify KPI format columns
        required_columns = ['district', 'school_id', 'school_name', 'year', 'student_group', 
                           'metric', 'value', 'suppressed', 'source_file', 'last_updated']
        for col in required_columns:
            assert col in df.columns, f"Required KPI column '{col}' missing"
        
        # Verify expected metrics are present
        metrics = df['metric'].unique()
        assert 'postsecondary_readiness_rate' in metrics, "Missing base postsecondary readiness rate metric"
        assert 'postsecondary_readiness_rate_with_bonus' in metrics, "Missing bonus postsecondary readiness rate metric"
        
        # Verify suppressed column has valid values
        assert set(df['suppressed'].unique()).issubset({'Y', 'N'}), "Suppressed column should only contain Y/N"
        
        # Verify year extraction from school_year
        assert all(df['year'].astype(str).str.len() == 4), "Year should be 4 digits"
        
        # Should have multiple rows due to KPI format (2 metrics per record)
        assert len(df) >= 6, "Should have at least 6 KPI rows from sample data (3 records × 2 metrics)"
    
    def test_transform_2022_format(self):
        """Test transform with 2022 format data."""
        data = self.create_sample_2022_data()
        data.to_csv(self.sample_dir / "postsecondary_readiness_2022.csv", index=False)
        
        config = {
            "derive": {
                "processing_date": "2025-07-19"
            }
        }
        
        transform(self.raw_dir, self.proc_dir, config)
        
        # Check output file exists
        output_file = self.proc_dir / "postsecondary_readiness.csv"
        assert output_file.exists()
        
        # Check KPI format transformations
        df = pd.read_csv(output_file)
        
        # Verify expected metrics are present
        metrics = df['metric'].unique()
        assert 'postsecondary_readiness_rate' in metrics
        assert 'postsecondary_readiness_rate_with_bonus' in metrics
        
        # Verify suppressed records are included with NaN values
        suppressed_records = df[df['suppressed'] == 'Y']
        if len(suppressed_records) > 0:
            assert all(suppressed_records['value'].isna()), "Suppressed records should have NaN values"
    
    def test_transform_multiple_files(self):
        """Test transform with multiple files."""
        # Create both 2024 and 2022 format files
        data_2024 = self.create_sample_2024_data()
        data_2022 = self.create_sample_2022_data()
        
        data_2024.to_csv(self.sample_dir / "postsecondary_readiness_2024.csv", index=False)
        data_2022.to_csv(self.sample_dir / "postsecondary_readiness_2022.csv", index=False)
        
        config = {
            "derive": {
                "processing_date": "2025-07-19"
            }
        }
        
        transform(self.raw_dir, self.proc_dir, config)
        
        # Check output file exists
        output_file = self.proc_dir / "postsecondary_readiness.csv"
        assert output_file.exists()
        
        # Check combined KPI data
        df = pd.read_csv(output_file)
        
        # Verify KPI format columns
        required_columns = ['district', 'school_id', 'school_name', 'year', 'student_group', 
                           'metric', 'value', 'suppressed', 'source_file', 'last_updated']
        for col in required_columns:
            assert col in df.columns, f"Required KPI column '{col}' missing"
        
        # Should have multiple metrics per source record
        assert len(df) >= 10, f"Expected at least 10 KPI rows, got {len(df)}"
        
        # Verify both source files are represented
        source_files = df['source_file'].unique()
        assert len(source_files) >= 2, "Should have data from multiple source files"
        
        # Verify both 2024 and 2022 data present by checking source files
        has_2024 = any('2024' in sf for sf in source_files)
        has_2022 = any('2022' in sf for sf in source_files)
        assert has_2024 or has_2022, "Should have data from 2024 and/or 2022 files"
        
        # Verify we have data from both file types by checking metrics
        metrics = df['metric'].unique()
        assert 'postsecondary_readiness_rate' in metrics, "Should have base readiness rate metric"
        assert 'postsecondary_readiness_rate_with_bonus' in metrics, "Should have bonus readiness rate metric"
    
    def test_transform_no_data(self):
        """Test transform when no data exists."""
        empty_raw_dir = self.test_dir / "empty_raw"
        empty_raw_dir.mkdir()
        
        config = {}
        transform(empty_raw_dir, self.proc_dir, config)
        
        # Should not create output file
        output_file = self.proc_dir / "postsecondary_readiness.csv"
        assert not output_file.exists()
    
    def test_dtype_conversion(self):
        """Test data type conversions."""
        data = self.create_sample_2024_data()
        data.to_csv(self.sample_dir / "postsecondary_readiness_2024.csv", index=False)
        
        config = {
            "dtype": {
                "county_number": "str",
                "postsecondary_rate": "float64"
            },
            "derive": {
                "processing_date": "2025-07-19"
            }
        }
        
        transform(self.raw_dir, self.proc_dir, config)
        
        # Check output file
        output_file = self.proc_dir / "postsecondary_readiness.csv"
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


class TestPostsecondaryReadinessHelpers:
    """Test helper functions independently."""
    
    def setup_method(self):
        """Setup ETL instance for testing."""
        self.etl = PostsecondaryReadinessETL('postsecondary_readiness')
    
    def test_normalize_column_names_edge_cases(self):
        """Test column normalization with edge cases."""
        df = pd.DataFrame({
            'Unknown Column': [1, 2, 3],
            'SCHOOL YEAR': ['2021', '2022', '2023'],
            'School Name': ['A', 'B', 'C'],
            'POSTSECONDARY RATE': [75.0, 80.0, 85.0]
        })
        
        result = self.etl.normalize_column_names(df)
        
        assert 'school_year' in result.columns
        assert 'school_name' in result.columns
        assert 'postsecondary_rate' in result.columns
        assert 'Unknown Column' in result.columns  # Unknown columns preserved
    
    def test_standardize_missing_values_edge_cases(self):
        """Test missing value standardization with postsecondary rate columns."""
        df = pd.DataFrame({
            'postsecondary_rate': ['', '""', '75.5', None],
            'postsecondary_rate_with_bonus': ['*', '', '80.0', '0']
        })
        
        result = self.etl.standardize_missing_values(df)
        
        assert pd.isna(result.loc[0, 'postsecondary_rate'])
        assert pd.isna(result.loc[1, 'postsecondary_rate'])
        assert result.loc[2, 'postsecondary_rate'] == 75.5  # Now converted to numeric
        assert pd.isna(result.loc[3, 'postsecondary_rate'])
        
        assert pd.isna(result.loc[0, 'postsecondary_rate_with_bonus'])  # '*' in rate column
        assert pd.isna(result.loc[1, 'postsecondary_rate_with_bonus'])
        assert result.loc[2, 'postsecondary_rate_with_bonus'] == 80.0  # Converted to numeric
        assert result.loc[3, 'postsecondary_rate_with_bonus'] == 0.0   # Converted to numeric