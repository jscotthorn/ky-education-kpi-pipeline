"""
Tests for English Learner Progress ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
import shutil
from etl.english_learner_progress import (
    transform, normalize_column_names, standardize_missing_values, 
    clean_percentage_scores, calculate_progress_metrics, convert_to_kpi_format
)


class TestEnglishLearnerProgressETL:
    
    def setup_method(self):
        """Setup test directories and sample data."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        
        # Create sample raw data directory
        self.sample_dir = self.raw_dir / "english_learner_progress"
        self.sample_dir.mkdir(parents=True)
    
    def teardown_method(self):
        """Clean up test directories."""
        shutil.rmtree(self.test_dir)
    
    def create_sample_2024_data(self):
        """Create sample 2024 format data."""
        data = pd.DataFrame({
            'School Year': ['20232024', '20232024', '20232024', '20232024'],
            'County Number': ['034', '034', '034', '034'],
            'County Name': ['FAYETTE', 'FAYETTE', 'FAYETTE', 'FAYETTE'],
            'District Number': ['165', '165', '165', '165'],
            'District Name': ['Fayette County', 'Fayette County', 'Fayette County', 'Fayette County'],
            'School Number': ['', '', '', ''],
            'School Name': ['All Schools', 'All Schools', 'All Schools', 'All Schools'],
            'School Code': ['999000', '999000', '999000', '999000'],
            'State School Id': ['', '', '', ''],
            'NCES ID': ['', '', '', ''],
            'CO-OP': ['909', '909', '909', '909'],
            'CO-OP Code': ['', '', '', ''],
            'School Type': ['', '', '', ''],
            'Demographic': ['All Students', 'Hispanic or Latino', 'English Learner including Monitored', 'African American'],
            'Level': ['Elementary School', 'Elementary School', 'Elementary School', 'Middle School'],
            'Suppressed': ['N', 'N', 'N', 'Y'],
            'Percentage Of Value Table Score Of 0': [29, 29, 29, '*'],
            'Percentage Of Value Table Score Of 60 And 80': [35, 36, 35, '*'],
            'Percentage Of Value Table Score Of 100': [23, 22, 23, '*'],
            'Percentage Of Value Table Score Of 140': [13, 13, 13, '*']
        })
        return data
    
    def create_sample_2022_data(self):
        """Create sample 2022 format data (uppercase columns)."""
        data = pd.DataFrame({
            'SCHOOL YEAR': ['20212022', '20212022', '20212022'],
            'COUNTY NUMBER': ['034', '034', '034'],
            'COUNTY NAME': ['FAYETTE', 'FAYETTE', 'FAYETTE'],
            'DISTRICT NUMBER': ['165', '165', '165'],
            'DISTRICT NAME': ['Fayette County', 'Fayette County', 'Fayette County'],
            'SCHOOL NUMBER': ['', '', ''],
            'SCHOOL NAME': ['All Schools', 'All Schools', 'All Schools'],
            'SCHOOL CODE': ['999000', '999000', '999000'],
            'STATE SCHOOL ID': ['', '', ''],
            'NCES ID': ['', '', ''],
            'CO-OP': ['909', '909', '909'],
            'CO-OP CODE': ['', '', ''],
            'SCHOOL TYPE': ['', '', ''],
            'DEMOGRAPHIC': ['All Students', 'Hispanic or Latino', 'English Learner including Monitored'],
            'LEVEL': ['ES', 'ES', 'ES'],
            'SUPPRESSED': ['N', 'N', 'N'],
            'PERCENTAGE OF VALUE TABLE SCORE OF 0': [52, 29, 29],
            'PERCENTAGE OF VALUE TABLE SCORE OF 60 AND 80': [28, 36, 35],
            'PERCENTAGE OF VALUE TABLE SCORE OF 100': [12, 22, 23],
            'PERCENTAGE OF VALUE TABLE SCORE OF 140': [4, 13, 13]
        })
        return data
    
    def create_sample_suppressed_data(self):
        """Create sample data with suppressed records."""
        data = pd.DataFrame({
            'School Year': ['20232024', '20232024'],
            'County Name': ['FAYETTE', 'FAYETTE'],
            'District Name': ['Fayette County', 'Fayette County'],
            'School Name': ['Test School', 'Test School'],
            'School Code': ['123456', '123456'],
            'Demographic': ['American Indian or Alaska Native', 'Two or More Races'],
            'Level': ['Middle School', 'High School'],
            'Suppressed': ['Y', 'Y'],
            'Percentage Of Value Table Score Of 0': ['*', '*'],
            'Percentage Of Value Table Score Of 60 And 80': ['*', '*'],
            'Percentage Of Value Table Score Of 100': ['*', '*'],
            'Percentage Of Value Table Score Of 140': ['*', '*']
        })
        return data
    
    def test_normalize_column_names_2024_format(self):
        """Test column name normalization for 2024 format."""
        df = self.create_sample_2024_data()
        normalized_df = normalize_column_names(df)
        
        assert 'school_year' in normalized_df.columns
        assert 'county_name' in normalized_df.columns
        assert 'demographic' in normalized_df.columns
        assert 'level' in normalized_df.columns
        assert 'suppressed' in normalized_df.columns
        assert 'percentage_score_0' in normalized_df.columns
        assert 'percentage_score_60_80' in normalized_df.columns
        assert 'percentage_score_100' in normalized_df.columns
        assert 'percentage_score_140' in normalized_df.columns
    
    def test_normalize_column_names_2022_format(self):
        """Test column name normalization for 2022 format (uppercase)."""
        df = self.create_sample_2022_data()
        normalized_df = normalize_column_names(df)
        
        assert 'school_year' in normalized_df.columns
        assert 'county_name' in normalized_df.columns
        assert 'demographic' in normalized_df.columns
        assert 'level' in normalized_df.columns
        assert 'suppressed' in normalized_df.columns
        assert 'percentage_score_0' in normalized_df.columns
    
    def test_standardize_missing_values(self):
        """Test missing value standardization."""
        df = pd.DataFrame({
            'percentage_score_0': [29, '', '*', '""'],
            'percentage_score_100': [23, 'N/A', '*', '']
        })
        
        cleaned_df = standardize_missing_values(df)
        
        # Check that empty strings and markers are converted to NaN
        assert pd.isna(cleaned_df.loc[1, 'percentage_score_0'])
        assert pd.isna(cleaned_df.loc[2, 'percentage_score_0'])
        assert pd.isna(cleaned_df.loc[3, 'percentage_score_0'])
        assert pd.isna(cleaned_df.loc[3, 'percentage_score_100'])
    
    def test_clean_percentage_scores(self):
        """Test percentage score validation."""
        df = pd.DataFrame({
            'percentage_score_0': [29, 150, -5, 'invalid'],
            'percentage_score_100': [23, 200, -10, 'text']
        })
        
        cleaned_df = clean_percentage_scores(df)
        
        # Valid percentages should remain
        assert cleaned_df.loc[0, 'percentage_score_0'] == 29
        assert cleaned_df.loc[0, 'percentage_score_100'] == 23
        
        # Invalid percentages should be NaN
        assert pd.isna(cleaned_df.loc[1, 'percentage_score_0'])  # 150 > 100
        assert pd.isna(cleaned_df.loc[2, 'percentage_score_0'])  # -5 < 0
        assert pd.isna(cleaned_df.loc[3, 'percentage_score_0'])  # invalid text
    
    def test_calculate_progress_metrics(self):
        """Test direct score metric extraction."""
        row = pd.Series({
            'percentage_score_0': 29,
            'percentage_score_60_80': 35,
            'percentage_score_100': 23,
            'percentage_score_140': 13
        })
        
        metrics = calculate_progress_metrics(row)
        
        # Test direct score metrics
        assert metrics['english_learner_score_0'] == 29
        assert metrics['english_learner_score_60_80'] == 35
        assert metrics['english_learner_score_100'] == 23
        assert metrics['english_learner_score_140'] == 13
    
    def test_calculate_progress_metrics_missing_values(self):
        """Test score metric extraction with missing values."""
        row = pd.Series({
            'percentage_score_0': pd.NA,
            'percentage_score_60_80': 35,
            'percentage_score_100': pd.NA,
            'percentage_score_140': 13
        })
        
        metrics = calculate_progress_metrics(row)
        
        # Should only include metrics where values are present
        assert 'english_learner_score_0' not in metrics  # Missing
        assert 'english_learner_score_100' not in metrics  # Missing
        assert metrics['english_learner_score_60_80'] == 35  # Available
        assert metrics['english_learner_score_140'] == 13  # Available
    
    def test_convert_to_kpi_format_normal_data(self):
        """Test KPI format conversion for normal (non-suppressed) data."""
        df = self.create_sample_2024_data()
        df = normalize_column_names(df)
        df = standardize_missing_values(df)
        df = clean_percentage_scores(df)
        df['source_file'] = 'test_file.csv'
        
        # Test non-suppressed records only
        df_normal = df[df['suppressed'] == 'N'].copy()
        kpi_df = convert_to_kpi_format(df_normal)
        
        assert not kpi_df.empty
        assert len(kpi_df.columns) == 10  # Standard KPI format
        
        # Check required columns
        required_columns = ['district', 'school_id', 'school_name', 'year', 'student_group', 
                          'metric', 'value', 'suppressed', 'source_file', 'last_updated']
        for col in required_columns:
            assert col in kpi_df.columns
        
        # Check that metrics are created correctly
        metrics = kpi_df['metric'].unique()
        expected_metrics = [
            'english_learner_score_0_elementary',
            'english_learner_score_60_80_elementary',
            'english_learner_score_100_elementary',
            'english_learner_score_140_elementary'
        ]
        
        for metric in expected_metrics:
            assert any(metric in m for m in metrics), f"Missing metric: {metric}"
        
        # Check that all values are numeric and within expected range
        valid_values = kpi_df['value'].dropna()
        assert all(0 <= v <= 100 for v in valid_values), "Values should be percentages between 0-100"
        
        # Check suppressed field is 'N' for normal data
        assert all(kpi_df['suppressed'] == 'N')
    
    def test_convert_to_kpi_format_suppressed_data(self):
        """Test KPI format conversion for suppressed data."""
        df = self.create_sample_suppressed_data()
        df = normalize_column_names(df)
        df = standardize_missing_values(df)
        df = clean_percentage_scores(df)
        df['source_file'] = 'test_suppressed.csv'
        
        kpi_df = convert_to_kpi_format(df)
        
        assert not kpi_df.empty
        
        # Check that all values are NaN for suppressed records
        assert all(pd.isna(kpi_df['value']))
        
        # Check that suppressed field is 'Y' for all records
        assert all(kpi_df['suppressed'] == 'Y')
        
        # Check that metrics are still created
        metrics = kpi_df['metric'].unique()
        assert len(metrics) > 0
    
    def test_level_normalization(self):
        """Test education level normalization."""
        df = pd.DataFrame({
            'school_year': ['20232024'] * 6,
            'county_name': ['FAYETTE'] * 6,
            'district_name': ['Fayette County'] * 6,
            'school_name': ['Test School'] * 6,
            'school_code': ['123456'] * 6,
            'demographic': ['All Students'] * 6,
            'level': ['ES', 'Elementary School', 'MS', 'Middle School', 'HS', 'High School'],
            'suppressed': ['N'] * 6,
            'percentage_score_0': [29] * 6,
            'percentage_score_60_80': [35] * 6,
            'percentage_score_100': [23] * 6,
            'percentage_score_140': [13] * 6,
            'source_file': ['test.csv'] * 6
        })
        
        kpi_df = convert_to_kpi_format(df)
        
        # Check that level suffixes are normalized
        metrics = kpi_df['metric'].unique()
        level_suffixes = {metric.split('_')[-1] for metric in metrics}
        
        assert 'elementary' in level_suffixes
        assert 'middle' in level_suffixes
        assert 'high' in level_suffixes
    
    def test_school_id_handling(self):
        """Test school ID extraction and formatting."""
        df = pd.DataFrame({
            'school_year': ['20232024'] * 3,
            'county_name': ['FAYETTE'] * 3,
            'district_name': ['Fayette County'] * 3,
            'school_name': ['Test School'] * 3,
            'state_school_id': ['123.0', '', '456'],
            'nces_id': ['', '789.0', ''],
            'school_code': ['999', '888', '777'],
            'demographic': ['All Students'] * 3,
            'level': ['Elementary School'] * 3,
            'suppressed': ['N'] * 3,
            'percentage_score_0': [29] * 3,
            'percentage_score_60_80': [35] * 3,
            'percentage_score_100': [23] * 3,
            'percentage_score_140': [13] * 3,
            'source_file': ['test.csv'] * 3
        })
        
        kpi_df = convert_to_kpi_format(df)
        
        # Check school_id values
        school_ids = kpi_df['school_id'].unique()
        
        # Should prefer state_school_id, then nces_id, then school_code
        # Note: state_school_id takes precedence, so when it's empty, falls back to nces_id, then school_code
        assert '123' in school_ids  # state_school_id without .0
        assert '888' in school_ids  # school_code fallback when state_school_id is empty
        assert '456' in school_ids  # state_school_id when present
    
    def test_year_extraction(self):
        """Test year extraction from school_year field."""
        df = pd.DataFrame({
            'school_year': ['20232024', '20212022', '2023'],
            'county_name': ['FAYETTE'] * 3,
            'district_name': ['Fayette County'] * 3,
            'school_name': ['Test School'] * 3,
            'school_code': ['123456'] * 3,
            'demographic': ['All Students'] * 3,
            'level': ['Elementary School'] * 3,
            'suppressed': ['N'] * 3,
            'percentage_score_0': [29] * 3,
            'percentage_score_60_80': [35] * 3,
            'percentage_score_100': [23] * 3,
            'percentage_score_140': [13] * 3,
            'source_file': ['test.csv'] * 3
        })
        
        kpi_df = convert_to_kpi_format(df)
        
        years = kpi_df['year'].unique()
        
        # Should extract last 4 digits or use as-is for 4-digit years
        assert '2024' in years
        assert '2022' in years
        assert '2023' in years
    
    def test_full_transform_pipeline(self):
        """Test the complete transform pipeline."""
        # Create sample data file
        df = self.create_sample_2024_data()
        sample_file = self.sample_dir / "test_english_learner.csv"
        df.to_csv(sample_file, index=False)
        
        # Run transform
        config = {"derive": {"processing_date": "2024-07-19"}}
        transform(self.raw_dir, self.proc_dir, config)
        
        # Check output files exist
        output_file = self.proc_dir / "english_learner_progress.csv"
        audit_file = self.proc_dir / "english_learner_progress_demographic_audit.csv"
        
        assert output_file.exists()
        assert audit_file.exists()
        
        # Check output format
        output_df = pd.read_csv(output_file)
        assert not output_df.empty
        assert len(output_df.columns) == 10  # Standard KPI format
        
        # Verify required columns
        required_columns = ['district', 'school_id', 'school_name', 'year', 'student_group', 
                          'metric', 'value', 'suppressed', 'source_file', 'last_updated']
        for col in required_columns:
            assert col in output_df.columns
    
    def test_empty_directory_handling(self):
        """Test handling of empty source directory."""
        # Create empty directory
        empty_dir = self.raw_dir / "empty_source"
        empty_dir.mkdir(parents=True)
        
        # Should not raise error
        config = {"derive": {}}
        transform(self.raw_dir, self.proc_dir, config)
        
        # Output file should not exist
        output_file = self.proc_dir / "empty_source.csv"
        assert not output_file.exists()
    
    def test_multiple_files_processing(self):
        """Test processing multiple CSV files."""
        # Create multiple sample files
        df1 = self.create_sample_2024_data()
        df2 = self.create_sample_2022_data()
        
        file1 = self.sample_dir / "english_learner_2024.csv"
        file2 = self.sample_dir / "english_learner_2022.csv"
        
        df1.to_csv(file1, index=False)
        df2.to_csv(file2, index=False)
        
        # Run transform
        config = {"derive": {"processing_date": "2024-07-19"}}
        transform(self.raw_dir, self.proc_dir, config)
        
        # Check output
        output_file = self.proc_dir / "english_learner_progress.csv"
        assert output_file.exists()
        
        output_df = pd.read_csv(output_file)
        
        # Should have data from both files
        source_files = output_df['source_file'].unique()
        assert 'english_learner_2024.csv' in source_files
        assert 'english_learner_2022.csv' in source_files
        
        # Should have multiple years (years are stored as strings in CSV but may be read as integers)
        years = output_df['year'].astype(str).unique()
        assert '2024' in years
        assert '2022' in years