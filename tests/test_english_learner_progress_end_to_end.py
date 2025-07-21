"""
End-to-end integration tests for English Learner Progress ETL pipeline
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
import shutil
from etl.english_learner_progress import transform


class TestEnglishLearnerProgressEndToEnd:
    
    def setup_method(self):
        """Setup test directories and realistic sample data."""
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
    
    def create_realistic_2024_data(self):
        """Create realistic 2024 format data with Fayette County schools."""
        data = pd.DataFrame({
            'School Year': ['20232024'] * 20,
            'County Number': ['034'] * 20,
            'County Name': ['FAYETTE'] * 20,
            'District Number': ['165'] * 20,
            'District Name': ['Fayette County'] * 20,
            'School Number': ['101', '102', '103', '104', '105'] * 4,
            'School Name': [
                'Bryan Station High School', 'Lafayette High School', 'Henry Clay High School',
                'Tates Creek High School', 'Paul Laurence Dunbar High School'
            ] * 4,
            'School Code': ['165101', '165102', '165103', '165104', '165105'] * 4,
            'State School Id': ['21101034101', '21101034102', '21101034103', '21101034104', '21101034105'] * 4,
            'NCES ID': ['210540001015', '210540001016', '210540001017', '210540001018', '210540001019'] * 4,
            'CO-OP': ['909'] * 20,
            'CO-OP Code': [''] * 20,
            'School Type': [''] * 20,
            'Demographic': [
                'All Students', 'Female', 'Male', 'African American'
            ] * 5,
            'Level': ['Elementary School'] * 8 + ['Middle School'] * 8 + ['High School'] * 4,
            'Suppressed': ['N'] * 18 + ['Y'] * 2,  # Last 2 records suppressed
            'Percentage Of Value Table Score Of 0': [
                25, 22, 28, 32,  # Elementary - All, Female, Male, African American
                35, 31, 38, 42,  # Elementary continued
                45, 41, 48, 52,  # Middle School
                65, 62, 67, 70,  # Middle School continued
                75, 72, '*', '*'  # High School (last 2 suppressed)
            ],
            'Percentage Of Value Table Score Of 60 And 80': [
                40, 42, 38, 36,  # Elementary
                35, 38, 33, 31,  # Elementary continued
                30, 33, 28, 26,  # Middle School
                25, 28, 23, 20,  # Middle School continued
                15, 18, '*', '*'  # High School (last 2 suppressed)
            ],
            'Percentage Of Value Table Score Of 100': [
                25, 26, 24, 22,  # Elementary
                22, 24, 21, 19,  # Elementary continued
                18, 19, 17, 15,  # Middle School
                16, 17, 15, 13,  # Middle School continued
                8, 9, '*', '*'   # High School (last 2 suppressed)
            ],
            'Percentage Of Value Table Score Of 140': [
                10, 10, 10, 10,  # Elementary
                8, 7, 8, 8,      # Elementary continued
                7, 7, 7, 7,      # Middle School
                4, 3, 5, 7,      # Middle School continued
                2, 1, '*', '*'   # High School (last 2 suppressed)
            ]
        })
        return data
    
    def create_realistic_2022_data(self):
        """Create realistic 2022 format data with uppercase columns."""
        data = pd.DataFrame({
            'SCHOOL YEAR': ['20212022'] * 12,
            'COUNTY NUMBER': ['034'] * 12,
            'COUNTY NAME': ['FAYETTE'] * 12,
            'DISTRICT NUMBER': ['165'] * 12,
            'DISTRICT NAME': ['Fayette County'] * 12,
            'SCHOOL NUMBER': ['101', '102', '103'] * 4,
            'SCHOOL NAME': [
                'Bryan Station High School', 'Lafayette High School', 'Henry Clay High School'
            ] * 4,
            'SCHOOL CODE': ['165101', '165102', '165103'] * 4,
            'STATE SCHOOL ID': ['21101034101', '21101034102', '21101034103'] * 4,
            'NCES ID': ['210540001015', '210540001016', '210540001017'] * 4,
            'CO-OP': ['909'] * 12,
            'CO-OP CODE': [''] * 12,
            'SCHOOL TYPE': [''] * 12,
            'DEMOGRAPHIC': [
                'All Students', 'Hispanic or Latino', 'English Learner including Monitored', 'Female'
            ] * 3,
            'LEVEL': ['ES'] * 4 + ['MS'] * 4 + ['HS'] * 4,
            'SUPPRESSED': ['N'] * 10 + ['Y'] * 2,  # Last 2 records suppressed
            'PERCENTAGE OF VALUE TABLE SCORE OF 0': [
                50, 52, 48, 45,  # Elementary
                60, 58, 62, 55,  # Middle School
                70, 68, '*', '*'  # High School (last 2 suppressed)
            ],
            'PERCENTAGE OF VALUE TABLE SCORE OF 60 AND 80': [
                30, 28, 32, 35,  # Elementary
                25, 27, 23, 30,  # Middle School
                20, 22, '*', '*'  # High School (last 2 suppressed)
            ],
            'PERCENTAGE OF VALUE TABLE SCORE OF 100': [
                15, 15, 15, 15,  # Elementary
                12, 12, 12, 12,  # Middle School
                8, 8, '*', '*'    # High School (last 2 suppressed)
            ],
            'PERCENTAGE OF VALUE TABLE SCORE OF 140': [
                5, 5, 5, 5,      # Elementary
                3, 3, 3, 3,      # Middle School
                2, 2, '*', '*'   # High School (last 2 suppressed)
            ]
        })
        return data
    
    def test_end_to_end_single_file_processing(self):
        """Test complete pipeline with single file."""
        # Create and save sample data
        df = self.create_realistic_2024_data()
        sample_file = self.sample_dir / "KYRC24_ACCT_English_Learners_Progress_Proficiency_Rate.csv"
        df.to_csv(sample_file, index=False)
        
        # Run full ETL pipeline
        config = {
            "derive": {
                "processing_date": "2024-07-19",
                "data_quality_flag": "reviewed"
            }
        }
        
        transform(self.raw_dir, self.proc_dir, config)
        
        # Verify outputs exist
        output_file = self.proc_dir / "english_learner_progress.csv"
        audit_file = self.proc_dir / "english_learner_progress_demographic_report.md"
        
        assert output_file.exists(), "Main output file should exist"
        assert audit_file.exists(), "Demographic report should exist"
        
        # Load and verify output format
        output_df = pd.read_csv(output_file)
        
        assert not output_df.empty, "Output should not be empty"
        assert len(output_df.columns) == 10, "Should have exactly 10 columns in KPI format"
        
        # Verify required columns
        expected_columns = [
            'district', 'school_id', 'school_name', 'year', 'student_group',
            'metric', 'value', 'suppressed', 'source_file', 'last_updated'
        ]
        for col in expected_columns:
            assert col in output_df.columns, f"Missing required column: {col}"
        
        # Verify data quality
        assert output_df['district'].iloc[0] == 'Fayette County', "District should be Fayette County"
        assert output_df['year'].iloc[0] == 2024, "Year should be 2024"
        assert len(output_df['school_name'].unique()) > 1, "Should have multiple schools"
        assert len(output_df['student_group'].unique()) > 1, "Should have multiple demographics"
        
        # Verify metrics are created correctly
        metrics = output_df['metric'].unique()
        expected_metric_patterns = [
            'english_learner_score_0',
            'english_learner_score_60_80',
            'english_learner_score_100',
            'english_learner_score_140'
        ]
        
        for pattern in expected_metric_patterns:
            matching_metrics = [m for m in metrics if pattern in m]
            assert len(matching_metrics) > 0, f"Should have metrics containing '{pattern}'"
        
        # Verify education levels in metrics
        level_suffixes = ['elementary', 'middle', 'high']
        for suffix in level_suffixes:
            level_metrics = [m for m in metrics if m.endswith(suffix)]
            assert len(level_metrics) > 0, f"Should have metrics for {suffix} level"
        
        # Verify suppression handling
        suppressed_records = output_df[output_df['suppressed'] == 'Y']
        non_suppressed_records = output_df[output_df['suppressed'] == 'N']
        
        assert len(suppressed_records) > 0, "Should have some suppressed records"
        assert len(non_suppressed_records) > 0, "Should have some non-suppressed records"
        
        # All suppressed records should have NaN values
        assert suppressed_records['value'].isna().all(), "Suppressed records should have NaN values"
        
        # Non-suppressed records should have numeric values
        non_suppressed_values = non_suppressed_records['value'].dropna()
        assert len(non_suppressed_values) > 0, "Should have non-suppressed values"
        assert all(0 <= v <= 100 for v in non_suppressed_values), "Values should be percentages 0-100"
        
        # Verify school ID formatting
        school_ids = output_df['school_id'].unique()
        for school_id in school_ids:
            if pd.notna(school_id) and school_id != 'unknown':
                # Convert to string for checking (may be read as int/float from CSV)
                school_id_str = str(school_id)
                assert '.' not in school_id_str or school_id_str.endswith('.0'), "School IDs should be clean integers"
    
    def test_end_to_end_multiple_files_processing(self):
        """Test complete pipeline with multiple files across years."""
        # Create and save multiple files
        df_2024 = self.create_realistic_2024_data()
        df_2022 = self.create_realistic_2022_data()
        
        file_2024 = self.sample_dir / "english_language_proficiency_2024.csv"
        file_2022 = self.sample_dir / "english_language_proficiency_2022.csv"
        
        df_2024.to_csv(file_2024, index=False)
        df_2022.to_csv(file_2022, index=False)
        
        # Run full ETL pipeline
        config = {
            "derive": {
                "processing_date": "2024-07-19",
                "data_quality_flag": "reviewed"
            }
        }
        
        transform(self.raw_dir, self.proc_dir, config)
        
        # Verify outputs
        output_file = self.proc_dir / "english_learner_progress.csv"
        assert output_file.exists()
        
        output_df = pd.read_csv(output_file)
        
        # Verify multi-year processing
        years = output_df['year'].unique()
        assert 2024 in years, "Should include 2024 data"
        assert 2022 in years, "Should include 2022 data"
        
        # Verify multiple source files
        source_files = output_df['source_file'].unique()
        assert 'english_language_proficiency_2024.csv' in source_files
        assert 'english_language_proficiency_2022.csv' in source_files
        
        # Verify both uppercase and mixed case column handling
        year_2024_data = output_df[output_df['year'] == 2024]
        year_2022_data = output_df[output_df['year'] == 2022]
        
        assert len(year_2024_data) > 0, "Should have 2024 data"
        assert len(year_2022_data) > 0, "Should have 2022 data"
        
        # Verify consistent metric naming across years
        metrics_2024 = set(year_2024_data['metric'].unique())
        metrics_2022 = set(year_2022_data['metric'].unique())
        
        # Should have some common metrics
        common_metrics = metrics_2024.intersection(metrics_2022)
        assert len(common_metrics) > 0, "Should have common metrics across years"
    
    def test_end_to_end_data_validation(self):
        """Test data validation and quality checks."""
        # Create data with various edge cases
        df = self.create_realistic_2024_data()
        
        # Add some edge cases
        edge_cases = pd.DataFrame({
            'School Year': ['20232024'] * 4,
            'County Number': ['034'] * 4,
            'County Name': ['FAYETTE'] * 4,
            'District Number': ['165'] * 4,
            'District Name': ['Fayette County'] * 4,
            'School Number': ['999'] * 4,
            'School Name': ['Edge Case School'] * 4,
            'School Code': ['165999'] * 4,
            'State School Id': [''] * 4,  # Empty state school ID
            'NCES ID': ['999.0'] * 4,      # NCES ID with .0
            'CO-OP': ['909'] * 4,
            'CO-OP Code': [''] * 4,
            'School Type': [''] * 4,
            'Demographic': ['All Students', 'White (non-Hispanic)', 'Asian', 'Two or More Races'],
            'Level': ['Elementary School'] * 4,
            'Suppressed': ['N'] * 4,
            'Percentage Of Value Table Score Of 0': [0, 100, 50, ''],  # Edge values and empty
            'Percentage Of Value Table Score Of 60 And 80': [0, 0, 30, 35],
            'Percentage Of Value Table Score Of 100': [0, 0, 15, 25],
            'Percentage Of Value Table Score Of 140': [100, 0, 5, 10]  # Max value
        })
        
        combined_df = pd.concat([df, edge_cases], ignore_index=True)
        
        sample_file = self.sample_dir / "test_edge_cases.csv"
        combined_df.to_csv(sample_file, index=False)
        
        # Run ETL pipeline
        config = {"derive": {"processing_date": "2024-07-19"}}
        transform(self.raw_dir, self.proc_dir, config)
        
        # Verify output
        output_file = self.proc_dir / "english_learner_progress.csv"
        output_df = pd.read_csv(output_file)
        
        # Verify edge case handling
        edge_school_data = output_df[output_df['school_name'] == 'Edge Case School']
        assert len(edge_school_data) > 0, "Should process edge case school"
        
        # Verify school ID handling (falls back to school_code when state_school_id is empty)
        edge_school_ids = edge_school_data['school_id'].unique()
        # Convert to string for comparison (may be read as int from CSV)
        edge_school_ids_str = [str(id_val) for id_val in edge_school_ids]
        assert '165999' in edge_school_ids_str, "Should handle school ID fallback to school_code"
        
        # Verify value ranges
        valid_values = output_df['value'].dropna()
        assert all(0 <= v <= 100 for v in valid_values), "All values should be valid percentages"
        
        # Verify metric calculations with edge values
        edge_metrics = edge_school_data[edge_school_data['suppressed'] == 'N']
        score_metrics = edge_metrics[edge_metrics['metric'].str.contains('score_')]
        
        # Should have calculated score metrics
        assert len(score_metrics) > 0, "Should calculate score metrics for edge cases"
    
    def test_end_to_end_demographic_audit(self):
        """Test demographic mapping and audit functionality."""
        # Create data with various demographic variations
        df = self.create_realistic_2024_data()
        sample_file = self.sample_dir / "demographic_test.csv"
        df.to_csv(sample_file, index=False)
        
        # Run ETL pipeline
        config = {"derive": {"processing_date": "2024-07-19"}}
        transform(self.raw_dir, self.proc_dir, config)
        
        # Check audit file
        audit_file = self.proc_dir / "english_learner_progress_demographic_report.md"
        assert audit_file.exists(), "Demographic report should be created"

        content = audit_file.read_text()
        assert "Mapping Log" in content
        
        # Verify demographic standardization
        output_file = self.proc_dir / "english_learner_progress.csv"
        output_df = pd.read_csv(output_file)
        
        student_groups = output_df['student_group'].unique()
        
        # Should have standardized demographic names
        assert 'All Students' in student_groups
        assert 'African American' in student_groups
        assert 'Female' in student_groups
        assert 'Male' in student_groups
        
        # Should not have any obviously non-standard formats
        for group in student_groups:
            assert group != 'all students', "Should standardize to proper case"
            assert group != 'AFRICAN AMERICAN', "Should standardize to proper case"
    
    def test_end_to_end_error_handling(self):
        """Test error handling for malformed data."""
        # Create malformed data
        malformed_df = pd.DataFrame({
            'School Year': ['invalid', '20232024'],
            'County Name': ['', 'FAYETTE'],
            'District Name': ['', 'Fayette County'],
            'School Name': ['', 'Test School'],
            'School Code': ['', '123456'],
            'Demographic': ['', 'All Students'],
            'Level': ['', 'Elementary School'],
            'Suppressed': ['Maybe', 'N'],  # Invalid suppression value
            'Percentage Of Value Table Score Of 0': ['invalid', '29'],
            'Percentage Of Value Table Score Of 60 And 80': ['-5', '35'],  # Invalid negative
            'Percentage Of Value Table Score Of 100': ['150', '23'],  # Invalid >100
            'Percentage Of Value Table Score Of 140': ['text', '13']
        })
        
        malformed_file = self.sample_dir / "malformed_data.csv"
        malformed_df.to_csv(malformed_file, index=False)
        
        # Should not crash - should handle errors gracefully
        config = {"derive": {"processing_date": "2024-07-19"}}
        transform(self.raw_dir, self.proc_dir, config)
        
        # Should still produce output (at least for valid records)
        output_file = self.proc_dir / "english_learner_progress.csv"
        if output_file.exists():
            output_df = pd.read_csv(output_file)
            # If any data was processed, it should be in valid format
            if not output_df.empty:
                assert len(output_df.columns) == 10, "Output should maintain KPI format even with errors"
    
    def test_end_to_end_performance_with_large_dataset(self):
        """Test performance with larger dataset."""
        # Create larger dataset by replicating data
        base_df = self.create_realistic_2024_data()
        
        # Replicate data with different school codes to simulate larger dataset
        large_dfs = []
        for i in range(10):  # 10x the data
            df_copy = base_df.copy()
            df_copy['School Code'] = df_copy['School Code'].astype(str) + f"{i:02d}"
            df_copy['School Name'] = df_copy['School Name'] + f" Campus {i}"
            large_dfs.append(df_copy)
        
        large_df = pd.concat(large_dfs, ignore_index=True)
        
        large_file = self.sample_dir / "large_dataset.csv"
        large_df.to_csv(large_file, index=False)
        
        # Run ETL pipeline and measure basic functionality
        config = {"derive": {"processing_date": "2024-07-19"}}
        transform(self.raw_dir, self.proc_dir, config)
        
        # Verify output scales correctly
        output_file = self.proc_dir / "english_learner_progress.csv"
        assert output_file.exists()
        
        output_df = pd.read_csv(output_file)
        assert len(output_df) > len(base_df) * 5, "Should scale with input size"
        
        # Verify data quality maintained with large dataset
        assert len(output_df.columns) == 10
        assert output_df['district'].notna().all()
        assert output_df['year'].notna().all()
        assert len(output_df['school_name'].unique()) >= 50, "Should have many unique schools"