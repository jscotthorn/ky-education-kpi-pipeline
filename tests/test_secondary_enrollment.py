"""
Tests for Secondary Enrollment ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
from etl.constants import KPI_COLUMNS
import shutil
from etl.secondary_enrollment import transform, clean_enrollment_data, SecondaryEnrollmentETL


class TestSecondaryEnrollmentETL:

    def setup_method(self):
        """Setup test directories and sample data."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)

        # Create sample raw data directory
        self.sample_dir = self.raw_dir / "secondary_enrollment"
        self.sample_dir.mkdir(parents=True)

        # Create ETL instance for testing
        self.etl = SecondaryEnrollmentETL('secondary_enrollment')

    def teardown_method(self):
        """Clean up test directories."""
        shutil.rmtree(self.test_dir)

    def create_sample_kyrc25_data(self):
        """Create sample KYRC25 format secondary enrollment data."""
        data = pd.DataFrame({
            'School Year': ['20242025', '20242025', '20242025'],
            'County Number': ['001', '001', '001'],
            'County Name': ['Adair', 'Adair', 'Adair'],
            'District Number': ['001', '001', '001'],
            'District Name': ['Adair County', 'Adair County', 'Adair County'],
            'School Number': ['010', '010', '010'],
            'School Name': ['Adair County High School', 'Adair County High School', 'Adair County High School'],
            'School Code': ['001010', '001010', '001010'],
            'State School Id': ['001001010', '001001010', '001001010'],
            'NCES ID': ['210003000001', '210003000001', '210003000001'],
            'CO-OP': ['GRREC', 'GRREC', 'GRREC'],
            'CO-OP Code': ['902', '902', '902'],
            'School Type': ['A1', 'A1', 'A1'],
            'Demographic': ['All Students', 'Female', 'Male'],
            'All Grades': ['800', '400', '400'],
            'Grade 6': ['0', '0', '0'],
            'Grade 7': ['0', '0', '0'],
            'Grade 8': ['0', '0', '0'],
            'Grade 9': ['200', '100', '100'],
            'Grade 10': ['200', '100', '100'],
            'Grade 11': ['200', '100', '100'],
            'Grade 12': ['200', '100', '100'],
            'Grade 14': ['0', '0', '0']
        })
        return data

    def create_sample_historical_data(self):
        """Create sample historical secondary enrollment format data."""
        data = pd.DataFrame({
            'SCHOOL YEAR': ['20222023', '20222023', '20222023'],
            'COUNTY NUMBER': ['001', '001', '001'],
            'COUNTY NAME': ['ADAIR', 'ADAIR', 'ADAIR'],
            'DISTRICT NUMBER': ['001', '001', '001'],
            'DISTRICT NAME': ['Adair County', 'Adair County', 'Adair County'],
            'SCHOOL NUMBER': ['010', '010', '010'],
            'SCHOOL NAME': ['Adair County High School', 'Adair County High School', 'Adair County High School'],
            'SCHOOL CODE': ['001010', '001010', '001010'],
            'STATE SCHOOL ID': ['001001010', '001001010', '001001010'],
            'NCES ID': ['210003000001', '210003000001', '210003000001'],
            'CO-OP': ['GRREC', 'GRREC', 'GRREC'],
            'CO-OP CODE': ['902', '902', '902'],
            'SCHOOL TYPE': ['A1', 'A1', 'A1'],
            'DEMOGRAPHIC': ['All Students', 'Female', 'Male'],
            'TOTAL STUDENT COUNT': ['600', '300', '300'],
            'PRESCHOOL COUNT': ['0', '0', '0'],
            'KINDERGARTEN COUNT': ['0', '0', '0'],
            'GRADE1 COUNT': ['0', '0', '0'],
            'GRADE2 COUNT': ['0', '0', '0'],
            'GRADE3 COUNT': ['0', '0', '0'],
            'GRADE4 COUNT': ['0', '0', '0'],
            'GRADE5 COUNT': ['0', '0', '0'],
            'GRADE6 COUNT': ['0', '0', '0'],
            'GRADE7 COUNT': ['0', '0', '0'],
            'GRADE8 COUNT': ['0', '0', '0'],
            'GRADE9 COUNT': ['150', '75', '75'],
            'GRADE10 COUNT': ['150', '75', '75'],
            'GRADE11 COUNT': ['150', '75', '75'],
            'GRADE12 COUNT': ['150', '75', '75'],
            'GRADE14 COUNT': ['0', '0', '0']
        })
        return data

    def create_sample_middle_school_data(self):
        """Create sample data for a middle school (grades 6-8)."""
        data = pd.DataFrame({
            'School Year': ['20242025', '20242025', '20242025'],
            'County Number': ['001', '001', '001'],
            'County Name': ['Adair', 'Adair', 'Adair'],
            'District Number': ['001', '001', '001'],
            'District Name': ['Adair County', 'Adair County', 'Adair County'],
            'School Number': ['015', '015', '015'],
            'School Name': ['Adair County Middle School', 'Adair County Middle School', 'Adair County Middle School'],
            'School Code': ['001015', '001015', '001015'],
            'State School Id': ['001001015', '001001015', '001001015'],
            'NCES ID': ['210003000002', '210003000002', '210003000002'],
            'CO-OP': ['GRREC', 'GRREC', 'GRREC'],
            'CO-OP Code': ['902', '902', '902'],
            'School Type': ['A1', 'A1', 'A1'],
            'Demographic': ['All Students', 'Female', 'Male'],
            'All Grades': ['450', '220', '230'],
            'Grade 6': ['150', '75', '75'],
            'Grade 7': ['150', '70', '80'],
            'Grade 8': ['150', '75', '75'],
            'Grade 9': ['0', '0', '0'],
            'Grade 10': ['0', '0', '0'],
            'Grade 11': ['0', '0', '0'],
            'Grade 12': ['0', '0', '0'],
            'Grade 14': ['0', '0', '0']
        })
        return data

    def test_column_mappings(self):
        """Test that column mappings include both KYRC25 and historical formats."""
        mappings = self.etl.module_column_mappings

        # Test KYRC25 format mappings
        assert 'All Grades' in mappings
        assert mappings['All Grades'] == 'all_grades'
        assert 'Grade 6' in mappings
        assert mappings['Grade 6'] == 'grade_6'
        assert 'Grade 12' in mappings
        assert mappings['Grade 12'] == 'grade_12'

        # Test historical format mappings
        assert 'TOTAL STUDENT COUNT' in mappings
        assert mappings['TOTAL STUDENT COUNT'] == 'total_student_count'
        assert 'GRADE6 COUNT' in mappings
        assert mappings['GRADE6 COUNT'] == 'grade_6'
        assert 'GRADE9 COUNT' in mappings
        assert mappings['GRADE9 COUNT'] == 'grade_9'

    def test_extract_metrics_kyrc25_format(self):
        """Test metric extraction from KYRC25 format data."""
        sample_data = self.create_sample_kyrc25_data()
        # Normalize columns first
        sample_data = self.etl.normalize_column_names(sample_data)

        # Test first row (All Students)
        row = sample_data.iloc[0]
        metrics = self.etl.extract_metrics(row)

        # Check that metrics are extracted correctly with secondary_enrollment prefix
        assert metrics['secondary_enrollment_total'] == 800
        assert metrics['secondary_enrollment_grade_9'] == 200
        assert metrics['secondary_enrollment_grade_10'] == 200
        assert metrics['secondary_enrollment_grade_11'] == 200
        assert metrics['secondary_enrollment_grade_12'] == 200

        # Check aggregated metrics
        assert metrics['secondary_enrollment_high_school'] == 800  # grades 9-12
        assert 'secondary_enrollment_middle' not in metrics  # No middle grades

    def test_extract_metrics_historical_format(self):
        """Test metric extraction from historical format data."""
        sample_data = self.create_sample_historical_data()
        # Normalize columns first
        sample_data = self.etl.normalize_column_names(sample_data)

        # Test first row (All Students)
        row = sample_data.iloc[0]
        metrics = self.etl.extract_metrics(row)

        # Check that metrics are extracted correctly
        assert metrics['secondary_enrollment_total'] == 600
        assert metrics['secondary_enrollment_grade_9'] == 150
        assert metrics['secondary_enrollment_grade_10'] == 150
        assert metrics['secondary_enrollment_grade_11'] == 150
        assert metrics['secondary_enrollment_grade_12'] == 150

        # Check aggregated metrics
        assert metrics['secondary_enrollment_high_school'] == 600  # grades 9-12

    def test_extract_metrics_middle_school(self):
        """Test metric extraction for middle school data."""
        sample_data = self.create_sample_middle_school_data()
        # Normalize columns first
        sample_data = self.etl.normalize_column_names(sample_data)

        # Test first row (All Students)
        row = sample_data.iloc[0]
        metrics = self.etl.extract_metrics(row)

        # Check that metrics are extracted correctly
        assert metrics['secondary_enrollment_total'] == 450
        assert metrics['secondary_enrollment_grade_6'] == 150
        assert metrics['secondary_enrollment_grade_7'] == 150
        assert metrics['secondary_enrollment_grade_8'] == 150

        # Check aggregated metrics
        assert metrics['secondary_enrollment_middle'] == 450  # grades 6-8
        assert 'secondary_enrollment_high_school' not in metrics  # No high school grades

    def test_clean_enrollment_data(self):
        """Test enrollment data cleaning function."""
        # Create test data with various issues
        test_data = pd.DataFrame({
            'all_grades': ['1,000', '500', '-10', 'invalid'],
            'grade_9': ['100', '50', '0', '25'],
            'grade_10': [200, 150, -5, 175]
        })

        cleaned = clean_enrollment_data(test_data)

        # Check comma removal and numeric conversion
        assert cleaned['all_grades'].iloc[0] == 1000
        assert cleaned['all_grades'].iloc[1] == 500

        # Check negative value handling
        assert pd.isna(cleaned['all_grades'].iloc[2])  # Negative should become NaN
        assert pd.isna(cleaned['grade_10'].iloc[2])  # Negative should become NaN

        # Check invalid value handling
        assert pd.isna(cleaned['all_grades'].iloc[3])  # Invalid should become NaN

        # Check valid values preserved
        assert cleaned['grade_9'].iloc[0] == 100
        assert cleaned['grade_10'].iloc[1] == 150

    def test_convert_to_kpi_format(self):
        """Test conversion to KPI format."""
        sample_data = self.create_sample_kyrc25_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        kpi_df = self.etl.convert_to_kpi_format(sample_data, "test_file.csv")

        # Check that DataFrame has correct columns
        assert set(kpi_df.columns) == set(KPI_COLUMNS)

        # Check that we have multiple metrics per row
        assert len(kpi_df) > len(sample_data)

        # Check specific metrics exist with secondary_enrollment prefix
        metrics = kpi_df['metric'].unique()
        assert 'secondary_enrollment_total' in metrics
        assert 'secondary_enrollment_grade_9' in metrics
        assert 'secondary_enrollment_high_school' in metrics

        # Check that zero values are excluded
        grade_6_rows = kpi_df[kpi_df['metric'] == 'secondary_enrollment_grade_6']
        assert len(grade_6_rows) == 0  # Should be excluded since all are 0

        # Check that non-zero values are included
        grade_9_rows = kpi_df[kpi_df['metric'] == 'secondary_enrollment_grade_9']
        assert len(grade_9_rows) == 3  # Should have 3 rows (All Students, Female, Male)

    def test_metric_names_have_secondary_prefix(self):
        """Test that all metrics have secondary_enrollment prefix, not student_enrollment."""
        sample_data = self.create_sample_kyrc25_data()
        sample_data = self.etl.normalize_column_names(sample_data)

        kpi_df = self.etl.convert_to_kpi_format(sample_data, "test_file.csv")

        # All metrics should start with secondary_enrollment
        for metric in kpi_df['metric'].unique():
            assert metric.startswith('secondary_enrollment_'), \
                f"Metric '{metric}' should start with 'secondary_enrollment_'"

        # None should start with student_enrollment
        for metric in kpi_df['metric'].unique():
            assert not metric.startswith('student_enrollment_'), \
                f"Metric '{metric}' should not start with 'student_enrollment_'"

    def test_suppressed_metric_defaults(self):
        """Test suppressed metric defaults."""
        sample_row = pd.Series({
            'demographic': 'All Students',
            'all_grades': '*',
            'grade_6': '*',
            'grade_7': '*',
            'grade_8': '*',
            'grade_9': '*',
            'grade_10': '*',
            'grade_11': '*',
            'grade_12': '*'
        })
        defaults = self.etl.get_suppressed_metric_defaults(sample_row)

        # Check that all expected metrics have NaN defaults with secondary prefix
        expected_metrics = [
            'secondary_enrollment_total',
            'secondary_enrollment_grade_6',
            'secondary_enrollment_grade_7',
            'secondary_enrollment_grade_8',
            'secondary_enrollment_grade_9',
            'secondary_enrollment_grade_10',
            'secondary_enrollment_grade_11',
            'secondary_enrollment_grade_12',
            'secondary_enrollment_middle',
            'secondary_enrollment_high_school'
        ]

        for metric in expected_metrics:
            assert metric in defaults, f"Expected metric '{metric}' in defaults"
            assert pd.isna(defaults[metric]), f"Expected NaN for metric '{metric}'"

    def test_transform_integration(self):
        """Test complete transform function integration."""
        # Create sample files
        sample_kyrc25 = self.create_sample_kyrc25_data()
        sample_historical = self.create_sample_historical_data()

        # Save to test directory
        sample_kyrc25.to_csv(self.sample_dir / "KYRC25_OVW_Secondary_Enrollment.csv", index=False)
        sample_historical.to_csv(self.sample_dir / "secondary_enrollment_2023.csv", index=False)

        # Run transform
        config = {}
        transform(self.raw_dir, self.proc_dir, config)

        # Check output file was created
        output_file = self.proc_dir / "secondary_enrollment.csv"
        assert output_file.exists()

        # Load and validate output
        result_df = pd.read_csv(output_file)
        assert not result_df.empty
        assert set(result_df.columns) == set(KPI_COLUMNS)

        # Check that we have data from all source files
        source_files = result_df['source_file'].unique()
        assert any('KYRC25' in f for f in source_files)
        assert any('secondary_enrollment_2023' in f for f in source_files)

        # All metrics should be secondary_enrollment
        for metric in result_df['metric'].unique():
            assert metric.startswith('secondary_enrollment_')

    def test_data_type_validation(self):
        """Test that enrollment values are properly validated as numeric."""
        # Test with mixed data types
        sample_data = pd.DataFrame({
            'School Year': ['20242025'],
            'County Number': ['001'],
            'County Name': ['Test'],
            'District Number': ['001'],
            'District Name': ['Test District'],
            'School Number': ['010'],
            'School Name': ['Test School'],
            'School Code': ['001010'],
            'State School Id': ['001001010'],
            'NCES ID': ['123456'],
            'CO-OP': ['TEST'],
            'CO-OP Code': ['999'],
            'School Type': ['A1'],
            'Demographic': ['All Students'],
            'All Grades': ['1000'],  # String number
            'Grade 9': [250],        # Integer
            'Grade 10': [250.0],     # Float
            'Grade 11': ['invalid'], # Invalid
            'Grade 12': ['-50']      # Negative
        })

        sample_data = self.etl.normalize_column_names(sample_data)
        cleaned_data = self.etl.standardize_missing_values(sample_data)

        # Check conversions
        assert cleaned_data['all_grades'].iloc[0] == 1000
        assert cleaned_data['grade_9'].iloc[0] == 250
        assert cleaned_data['grade_10'].iloc[0] == 250.0
        assert pd.isna(cleaned_data['grade_11'].iloc[0])  # Invalid should be NaN
        assert pd.isna(cleaned_data['grade_12'].iloc[0])  # Negative should be NaN


class TestCleanEnrollmentData:
    """Test the standalone clean_enrollment_data function."""

    def test_comma_removal(self):
        """Test removal of commas from numbers."""
        df = pd.DataFrame({
            'all_grades': ['1,000', '2,500', '500'],
            'grade_9': ['100', '200', '300']
        })

        result = clean_enrollment_data(df)
        assert result['all_grades'].iloc[0] == 1000
        assert result['all_grades'].iloc[1] == 2500
        assert result['all_grades'].iloc[2] == 500

    def test_negative_value_handling(self):
        """Test that negative values are converted to NaN."""
        df = pd.DataFrame({
            'all_grades': [1000, -10, 500],
            'grade_9': [100, -5, 50]
        })

        result = clean_enrollment_data(df)
        assert result['all_grades'].iloc[0] == 1000
        assert pd.isna(result['all_grades'].iloc[1])
        assert result['all_grades'].iloc[2] == 500
        assert pd.isna(result['grade_9'].iloc[1])

    def test_invalid_value_handling(self):
        """Test that invalid values are converted to NaN."""
        df = pd.DataFrame({
            'all_grades': ['1000', 'invalid', '500'],
            'grade_9': ['100', 'text', '50']
        })

        result = clean_enrollment_data(df)
        assert result['all_grades'].iloc[0] == 1000
        assert pd.isna(result['all_grades'].iloc[1])
        assert result['all_grades'].iloc[2] == 500
        assert pd.isna(result['grade_9'].iloc[1])
