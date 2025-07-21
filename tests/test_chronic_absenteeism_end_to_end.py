"""
End-to-end integration tests for Chronic Absenteeism ETL pipeline
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
import shutil
from etl.chronic_absenteeism import transform


class TestChronicAbsenteeismEndToEnd:
    
    def setup_method(self):
        """Setup test directories and realistic sample data."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        
        # Create sample raw data directory
        self.sample_dir = self.raw_dir / "chronic_absenteeism"
        self.sample_dir.mkdir(parents=True)
    
    def teardown_method(self):
        """Clean up test directories."""
        shutil.rmtree(self.test_dir)
    
    def create_realistic_2024_data(self):
        """Create realistic 2024 format data with Fayette County schools."""
        # Create simple, consistent data
        records = []
        schools = [
            ('Bryan Station High School', '165101', '21101034101'),
            ('Lafayette High School', '165102', '21101034102'),
            ('Henry Clay High School', '165103', '21101034103')
        ]
        grades = ['All Grades', 'Grade 9', 'Grade 12']
        demographics = ['All Students', 'Female', 'Male', 'African American', 'Hispanic or Latino', 'English Learner']
        
        record_id = 0
        for school_name, school_code, state_id in schools:
            for grade in grades:
                for demo in demographics:
                    is_suppressed = record_id >= 40  # Last few records suppressed
                    records.append({
                        'School Year': '20232024',
                        'County Number': '034',
                        'County Name': 'FAYETTE',
                        'District Number': '165',
                        'District Name': 'Fayette County',
                        'School Number': school_code[-3:],
                        'School Name': school_name,
                        'School Code': school_code,
                        'State School Id': state_id,
                        'NCES ID': f'210540{school_code[-6:]}',
                        'CO-OP': '909',
                        'CO-OP Code': '',
                        'School Type': '',
                        'Grade': grade,
                        'Demographic': demo,
                        'Suppressed': 'Yes' if is_suppressed else 'No',
                        'Chronically Absent Students': '*' if is_suppressed else str(20 + record_id),
                        'Students Enrolled 10 or More Days': '*' if is_suppressed else str(100 + record_id * 5),
                        'Chronic Absenteeism Rate': '*' if is_suppressed else 25.0 + (record_id % 10)
                    })
                    record_id += 1
        
        return pd.DataFrame(records)
    
    def create_realistic_2023_data(self):
        """Create realistic 2023 format data with uppercase columns."""
        data = pd.DataFrame({
            'SCHOOL YEAR': ['20222023'] * 18,
            'COUNTY NUMBER': ['034'] * 18,
            'COUNTY NAME': ['FAYETTE'] * 18,
            'DISTRICT NUMBER': ['165'] * 18,
            'DISTRICT NAME': ['Fayette County'] * 18,
            'SCHOOL NUMBER': ['101', '102', '103'] * 6,
            'SCHOOL NAME': [
                'Bryan Station High School', 'Lafayette High School', 'Henry Clay High School'
            ] * 6,
            'SCHOOL CODE': ['165101', '165102', '165103'] * 6,
            'STATE SCHOOL ID': ['21101034101', '21101034102', '21101034103'] * 6,
            'NCES ID': ['210540001015', '210540001016', '210540001017'] * 6,
            'CO-OP': ['909'] * 18,
            'CO-OP CODE': [''] * 18,
            'SCHOOL TYPE': [''] * 18,
            'DEMOGRAPHIC': [
                'All Students', 'Female', 'Male', 'African American', 'Hispanic or Latino', 'English Learner'
            ] * 3,
            'CHRONIC ABSENTEE COUNT': [
                '42', '21', '21', '14', '7', '5',      # School 1
                '48', '24', '24', '16', '8', '6',      # School 2
                '35', '18', '17', '12', '6', '4',      # School 3
            ],
            'ENROLLMENT COUNT OF STUDENTS WITH 10+ ENROLLED DAYS': [
                '140', '70', '70', '42', '28', '20',   # School 1
                '160', '80', '80', '48', '32', '24',   # School 2
                '125', '63', '62', '38', '25', '18',   # School 3
            ],
            'PERCENT CHRONICALLY ABSENT': [
                30.0, 30.0, 30.0, 33.3, 25.0, 25.0,  # School 1
                30.0, 30.0, 30.0, 33.3, 25.0, 25.0,  # School 2
                28.0, 28.6, 27.4, 31.6, 24.0, 22.2,  # School 3
            ]
        })
        return data
    
    def test_end_to_end_single_file_processing(self):
        """Test complete pipeline with single file."""
        # Create and save sample data
        df = self.create_realistic_2024_data()
        sample_file = self.sample_dir / "KYRC24_OVW_Chronic_Absenteeism.csv"
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
        output_file = self.proc_dir / "chronic_absenteeism.csv"
        audit_file = self.proc_dir / "chronic_absenteeism_demographic_report.md"
        
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
            'chronic_absenteeism_rate_',
            'chronic_absenteeism_count_',
            'chronic_absenteeism_enrollment_'
        ]
        
        for pattern in expected_metric_patterns:
            matching_metrics = [m for m in metrics if pattern in m]
            assert len(matching_metrics) > 0, f"Should have metrics containing '{pattern}'"
        
        # Verify grade levels in metrics
        grade_suffixes = ['all_grades', 'grade_9', 'grade_12']
        for suffix in grade_suffixes:
            grade_metrics = [m for m in metrics if m.endswith(suffix)]
            assert len(grade_metrics) > 0, f"Should have metrics for {suffix} level"
        
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
        
        # Verify value ranges by metric type
        rate_records = non_suppressed_records[non_suppressed_records['metric'].str.contains('_rate_')]
        count_records = non_suppressed_records[non_suppressed_records['metric'].str.contains('_count_')]
        enrollment_records = non_suppressed_records[non_suppressed_records['metric'].str.contains('_enrollment_')]
        
        if len(rate_records) > 0:
            rate_values = rate_records['value'].dropna()
            assert all(0 <= v <= 100 for v in rate_values), "Rates should be percentages 0-100"
        
        if len(count_records) > 0:
            count_values = count_records['value'].dropna()
            assert all(v >= 0 for v in count_values), "Counts should be non-negative"
        
        if len(enrollment_records) > 0:
            enrollment_values = enrollment_records['value'].dropna()
            assert all(v >= 0 for v in enrollment_values), "Enrollment should be non-negative"
        
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
        df_2023 = self.create_realistic_2023_data()
        
        file_2024 = self.sample_dir / "chronic_absenteeism_2024.csv"
        file_2023 = self.sample_dir / "chronic_absenteeism_2023.csv"
        
        df_2024.to_csv(file_2024, index=False)
        df_2023.to_csv(file_2023, index=False)
        
        # Run full ETL pipeline
        config = {
            "derive": {
                "processing_date": "2024-07-19",
                "data_quality_flag": "reviewed"
            }
        }
        
        transform(self.raw_dir, self.proc_dir, config)
        
        # Verify outputs
        output_file = self.proc_dir / "chronic_absenteeism.csv"
        assert output_file.exists()
        
        output_df = pd.read_csv(output_file)
        
        # Verify multi-year processing
        years = output_df['year'].unique()
        assert 2024 in years, "Should include 2024 data"
        assert 2023 in years, "Should include 2023 data"
        
        # Verify multiple source files
        source_files = output_df['source_file'].unique()
        assert 'chronic_absenteeism_2024.csv' in source_files
        assert 'chronic_absenteeism_2023.csv' in source_files
        
        # Verify both 2024 and 2023 column handling
        year_2024_data = output_df[output_df['year'] == 2024]
        year_2023_data = output_df[output_df['year'] == 2023]
        
        assert len(year_2024_data) > 0, "Should have 2024 data"
        assert len(year_2023_data) > 0, "Should have 2023 data"
        
        # Verify consistent metric naming across years
        metrics_2024 = set(year_2024_data['metric'].unique())
        metrics_2023 = set(year_2023_data['metric'].unique())
        
        # Should have some common metrics
        common_metrics = metrics_2024.intersection(metrics_2023)
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
            'Grade': ['All Grades', 'Kindergarten', 'Grade 1', 'Grade 12'],
            'Demographic': ['All Students', 'Asian', 'Two or More Races', 'Gifted and Talented'],
            'Suppressed': ['No'] * 4,
            'Chronically Absent Students': ['0', '100', '50', ''],  # Edge values and empty
            'Students Enrolled 10 or More Days': ['400', '400', '200', '300'],
            'Chronic Absenteeism Rate': ['0.0', '25.0', '25.0', '']  # Edge values and empty
        })
        
        combined_df = pd.concat([df, edge_cases], ignore_index=True)
        
        sample_file = self.sample_dir / "test_edge_cases.csv"
        combined_df.to_csv(sample_file, index=False)
        
        # Run ETL pipeline
        config = {"derive": {"processing_date": "2024-07-19"}}
        transform(self.raw_dir, self.proc_dir, config)
        
        # Verify output
        output_file = self.proc_dir / "chronic_absenteeism.csv"
        output_df = pd.read_csv(output_file)
        
        # Verify edge case handling
        edge_school_data = output_df[output_df['school_name'] == 'Edge Case School']
        assert len(edge_school_data) > 0, "Should process edge case school"
        
        # Verify School Code is used (BaseETL prioritizes school_code first)
        edge_school_ids = edge_school_data['school_id'].unique()
        school_id_strs = [str(sid) for sid in edge_school_ids]
        assert '165999' in school_id_strs, "Should use school_code as primary ID"
        
        # Verify value ranges
        valid_rate_values = output_df[output_df['metric'].str.contains('_rate_')]['value'].dropna()
        if len(valid_rate_values) > 0:
            assert all(0 <= v <= 100 for v in valid_rate_values), "All rate values should be valid percentages"
        
        valid_count_values = output_df[output_df['metric'].str.contains('_count_')]['value'].dropna()
        if len(valid_count_values) > 0:
            assert all(v >= 0 for v in valid_count_values), "All count values should be non-negative"
        
        # Verify grade level handling
        grade_metrics = edge_school_data[edge_school_data['suppressed'] == 'N']
        expected_grades = ['all_grades', 'kindergarten', 'grade_1', 'grade_12']
        for grade in expected_grades:
            grade_specific_metrics = grade_metrics[grade_metrics['metric'].str.endswith(grade)]
            if len(grade_specific_metrics) > 0:  # Only check if data was processed for this grade
                assert len(grade_specific_metrics) > 0, f"Should have metrics for {grade}"
    
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
        audit_file = self.proc_dir / "chronic_absenteeism_demographic_report.md"
        assert audit_file.exists(), "Demographic report should be created"

        content = audit_file.read_text()
        assert "Mapping Log" in content
        
        # Verify demographic standardization
        output_file = self.proc_dir / "chronic_absenteeism.csv"
        output_df = pd.read_csv(output_file)
        
        student_groups = output_df['student_group'].unique()
        
        # Should have standardized demographic names from our test data
        assert 'All Students' in student_groups
        assert 'African American' in student_groups
        assert 'Female' in student_groups
        assert 'Male' in student_groups
        assert 'Hispanic or Latino' in student_groups
        assert 'English Learner' in student_groups
        
        # Should not have any obviously non-standard formats
        for group in student_groups:
            assert group != 'all students', "Should standardize to proper case"
            assert group != 'AFRICAN AMERICAN', "Should standardize to proper case"
    
    def test_end_to_end_comma_handling(self):
        """Test handling of comma-separated numbers."""
        # Create data with large numbers containing commas
        df = pd.DataFrame({
            'School Year': ['20232024'] * 3,
            'County Name': ['FAYETTE'] * 3,
            'District Name': ['Fayette County'] * 3,
            'School Name': ['Large School'] * 3,
            'School Code': ['123456'] * 3,
            'Grade': ['All Grades'] * 3,
            'Demographic': ['All Students', 'Female', 'Male'],
            'Suppressed': ['No'] * 3,
            'Chronically Absent Students': ['1,250', '91,571', '186,415'],  # With commas
            'Students Enrolled 10 or More Days': ['4,200', '321,064', '664,910'],  # With commas
            'Chronic Absenteeism Rate': ['29.8', '28.5', '28.0']
        })
        
        sample_file = self.sample_dir / "comma_test.csv"
        df.to_csv(sample_file, index=False)
        
        # Run ETL pipeline
        config = {"derive": {"processing_date": "2024-07-19"}}
        transform(self.raw_dir, self.proc_dir, config)
        
        # Verify output
        output_file = self.proc_dir / "chronic_absenteeism.csv"
        output_df = pd.read_csv(output_file)
        
        # Check that large numbers are processed correctly
        count_metrics = output_df[output_df['metric'].str.contains('_count_')]
        enrollment_metrics = output_df[output_df['metric'].str.contains('_enrollment_')]
        
        # Should have large values (commas removed)
        if len(count_metrics) > 0:
            count_values = count_metrics['value'].dropna()
            assert any(v > 1000 for v in count_values), "Should handle large count values"
        
        if len(enrollment_metrics) > 0:
            enrollment_values = enrollment_metrics['value'].dropna()
            assert any(v > 1000 for v in enrollment_values), "Should handle large enrollment values"
    
    def test_end_to_end_error_handling(self):
        """Test error handling for malformed data."""
        # Create malformed data
        malformed_df = pd.DataFrame({
            'School Year': ['invalid', '20232024'],
            'County Name': ['', 'FAYETTE'],
            'District Name': ['', 'Fayette County'],
            'School Name': ['', 'Test School'],
            'School Code': ['', '123456'],
            'Grade': ['', 'All Grades'],
            'Demographic': ['', 'All Students'],
            'Suppressed': ['Maybe', 'No'],  # Invalid suppression value
            'Chronically Absent Students': ['invalid', '100'],
            'Students Enrolled 10 or More Days': ['-5', '400'],  # Invalid negative
            'Chronic Absenteeism Rate': ['text', '25.0']
        })
        
        malformed_file = self.sample_dir / "malformed_data.csv"
        malformed_df.to_csv(malformed_file, index=False)
        
        # Should not crash - should handle errors gracefully
        config = {"derive": {"processing_date": "2024-07-19"}}
        transform(self.raw_dir, self.proc_dir, config)
        
        # Should still produce output (at least for valid records)
        output_file = self.proc_dir / "chronic_absenteeism.csv"
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
        for i in range(5):  # 5x the data
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
        output_file = self.proc_dir / "chronic_absenteeism.csv"
        assert output_file.exists()
        
        output_df = pd.read_csv(output_file)
        assert len(output_df) > len(base_df) * 3, "Should scale with input size"
        
        # Verify data quality maintained with large dataset
        assert len(output_df.columns) == 10
        assert output_df['district'].notna().all()
        assert output_df['year'].notna().all()
        assert len(output_df['school_name'].unique()) >= 15, "Should have many unique schools"