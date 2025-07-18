"""
Tests for Graduation Rates ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
import shutil
from etl.graduation_rates import transform, normalize_column_names, standardize_missing_values, clean_graduation_rates


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
        """Test column name normalization."""
        df_2024 = self.create_sample_2024_data()
        df_normalized = normalize_column_names(df_2024)
        
        assert 'school_year' in df_normalized.columns
        assert 'county_name' in df_normalized.columns
        assert 'graduation_rate_4_year' in df_normalized.columns
        assert 'School Year' not in df_normalized.columns
    
    def test_standardize_missing_values(self):
        """Test missing value standardization."""
        df = pd.DataFrame({
            'rate': [90.5, '', '*', 95.0],
            'suppressed': ['N', 'Y', '', 'N']
        })
        
        df_clean = standardize_missing_values(df)
        
        assert pd.isna(df_clean.loc[1, 'rate'])
        assert pd.isna(df_clean.loc[2, 'rate'])
        assert pd.isna(df_clean.loc[2, 'suppressed'])
    
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
        
        # Check data transformations
        df = pd.read_csv(output_file)
        assert 'school_year' in df.columns
        assert 'graduation_rate_4_year' in df.columns
        assert 'processing_date' in df.columns
        assert 'data_source' in df.columns
        assert df['data_source'].iloc[0] == '2024_simplified'
        assert len(df) == 3
    
    def test_transform_2021_format(self):
        """Test transform with 2021 format data."""
        data = self.create_sample_2021_data()
        data.to_csv(self.sample_dir / "graduation_rate_2021.csv", index=False)
        
        config = {
            "derive": {
                "processing_date": "2025-07-18"
            }
        }
        
        transform(self.raw_dir, self.proc_dir, config)
        
        # Check output file exists
        output_file = self.proc_dir / "graduation_rates.csv"
        assert output_file.exists()
        
        # Check data transformations
        df = pd.read_csv(output_file)
        assert 'graduation_rate_4_year' in df.columns
        assert 'graduation_rate_5_year' in df.columns
        assert 'grads_4_year_cohort' in df.columns
        assert 'students_4_year_cohort' in df.columns
        assert df['data_source'].iloc[0] == '2021_detailed'
        assert len(df) == 3
    
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
        
        # Check combined data
        df = pd.read_csv(output_file)
        assert len(df) == 6  # 3 from each file
        assert 'data_source' in df.columns
        
        # Check both data sources are present
        sources = df['data_source'].unique()
        assert '2024_simplified' in sources
        assert '2021_detailed' in sources
    
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
        
        # Verify data types (pandas may read as object from CSV, but should be convertible)
        assert df['county_number'].dtype == 'object'  # String type
        assert pd.to_numeric(df['graduation_rate_4_year'], errors='coerce').notna().all()


class TestGraduationRatesHelpers:
    """Test helper functions independently."""
    
    def test_normalize_column_names_edge_cases(self):
        """Test column normalization with edge cases."""
        df = pd.DataFrame({
            'Unknown Column': [1, 2, 3],
            'SCHOOL YEAR': ['2021', '2022', '2023'],
            'School Name': ['A', 'B', 'C']
        })
        
        result = normalize_column_names(df)
        
        assert 'school_year' in result.columns
        assert 'school_name' in result.columns
        assert 'Unknown Column' in result.columns  # Unknown columns preserved
    
    def test_standardize_missing_values_edge_cases(self):
        """Test missing value standardization with edge cases."""
        df = pd.DataFrame({
            'col1': ['', '""', 'valid', None],
            'col2': ['*', '', 'valid', '0']
        })
        
        result = standardize_missing_values(df)
        
        assert pd.isna(result.loc[0, 'col1'])
        assert pd.isna(result.loc[1, 'col1'])
        assert result.loc[2, 'col1'] == 'valid'
        assert pd.isna(result.loc[3, 'col1'])
        
        assert pd.isna(result.loc[0, 'col2'])  # '*' in rate column would be handled
        assert pd.isna(result.loc[1, 'col2'])
        assert result.loc[2, 'col2'] == 'valid'
        assert result.loc[3, 'col2'] == '0'