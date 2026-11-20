"""
Tests for Advanced Coursework ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
from etl.constants import KPI_COLUMNS
import shutil
from etl.advanced_coursework import transform, AdvancedCourseworkETL, clean_numeric_with_commas


class TestAdvancedCourseworkETL:

    def setup_method(self):
        """Setup test directories and sample data."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)

        # Create sample raw data directory
        self.sample_dir = self.raw_dir / "advanced_coursework"
        self.sample_dir.mkdir(parents=True)

        # Create ETL instance for testing
        self.etl = AdvancedCourseworkETL('advanced_coursework')

    def teardown_method(self):
        """Clean up test directories."""
        shutil.rmtree(self.test_dir)

    def create_participation_data(self):
        """Create sample participation/performance format data."""
        data = pd.DataFrame({
            'School Year': ['20232024', '20232024', '20232024', '20232024'],
            'County Number': ['001', '001', '001', '001'],
            'County Name': ['Adair', 'Adair', 'Adair', 'Adair'],
            'District Number': ['001', '001', '001', '001'],
            'District Name': ['Adair County', 'Adair County', 'Adair County', 'Adair County'],
            'School Number': ['', '', '', ''],
            'School Name': ['---District Total---', '---District Total---', '---District Total---', '---District Total---'],
            'School Code': ['001', '001', '001', '001'],
            'State School Id': ['', '', '', ''],
            'NCES ID': ['', '', '', ''],
            'CO-OP': ['GRREC', 'GRREC', 'GRREC', 'GRREC'],
            'CO-OP Code': ['902', '902', '902', '902'],
            'School Type': ['', '', '', ''],
            'Demographic': ['All Students', 'All Students', 'Female', 'Female'],
            'Advanced Course Type': ['AP', 'IB', 'AP', 'IB'],
            'Course Enrollment': ['150', '50', '85', '30'],
            'Course Completers': ['140', '48', '80', '28'],
            'Number Tested': ['120', '40', '70', '25'],
            'Earned Qualifying Score': ['80', '35', '45', '20'],
        })
        return data

    def create_historical_data(self):
        """Create sample historical format data (uppercase columns)."""
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
            'DEMOGRAPHIC': ['All Students', 'All Students', 'Female'],
            'ADVANCED COURSE TYPE': ['AP', 'IB', 'AP'],
            'COURSE ENROLLMENT': ['100', '20', '55'],
            'COURSE COMPLETERS': ['95', '18', '50'],
            'NUMBER TESTED': ['85', '15', '45'],
            'EARNED QUALIFYING SCORE': ['60', '12', '35'],
        })
        return data

    def test_normalize_column_names(self):
        """Test column name normalization."""
        df = self.create_participation_data()
        df_normalized = self.etl.normalize_column_names(df)

        assert 'school_year' in df_normalized.columns
        assert 'county_name' in df_normalized.columns
        assert 'course_type' in df_normalized.columns
        assert 'course_enrollment' in df_normalized.columns
        assert 'Course Enrollment' not in df_normalized.columns

    def test_clean_numeric_with_commas(self):
        """Test numeric value cleaning with commas."""
        series = pd.Series(['56,374', '100', '*', '', '---'])
        result = clean_numeric_with_commas(series)

        assert result.iloc[0] == 56374.0
        assert result.iloc[1] == 100.0
        assert pd.isna(result.iloc[2])
        assert pd.isna(result.iloc[3])

    def test_extract_metrics_ap(self):
        """Test metric extraction from AP row."""
        row = pd.Series({
            'course_type': 'AP',
            'course_enrollment': '150',
            'course_completers': '140',
            'number_tested': '120',
            'qualifying_score': '80',
        })

        metrics = self.etl.extract_metrics(row)

        assert 'ap_course_enrollment' in metrics
        assert metrics['ap_course_enrollment'] == 150
        assert metrics['ap_completion_count'] == 140
        assert metrics['ap_tested_count'] == 120
        assert metrics['ap_qualifying_score_count'] == 80
        assert 'ap_qualifying_score_rate' in metrics
        assert round(metrics['ap_qualifying_score_rate'], 1) == 66.7

    def test_extract_metrics_ib(self):
        """Test metric extraction from IB row."""
        row = pd.Series({
            'course_type': 'IB',
            'course_enrollment': '50',
            'course_completers': '48',
            'number_tested': '40',
            'qualifying_score': '35',
        })

        metrics = self.etl.extract_metrics(row)

        assert 'ib_course_enrollment' in metrics
        assert metrics['ib_course_enrollment'] == 50
        assert 'ib_qualifying_score_rate' in metrics
        assert metrics['ib_qualifying_score_rate'] == 87.5

    def test_extract_metrics_skips_dual_credit(self):
        """Test that dual credit rows return no metrics."""
        row = pd.Series({
            'course_type': 'Dual Credit',
            'course_enrollment': '100',
        })

        metrics = self.etl.extract_metrics(row)
        assert len(metrics) == 0

    def test_transform_participation_data(self):
        """Test transform with participation format data."""
        data = self.create_participation_data()
        data.to_csv(self.sample_dir / "advanced_courses_participation_and_performance_2024.csv", index=False)

        config = {
            "derive": {
                "processing_date": "2025-11-24"
            }
        }

        transform(self.raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "advanced_coursework.csv"
        assert output_file.exists()

        df = pd.read_csv(output_file)

        # Verify KPI format columns
        for col in KPI_COLUMNS:
            assert col in df.columns, f"Required KPI column '{col}' missing"

        # Verify expected metrics are present
        metrics = df['metric'].unique()
        assert 'ap_course_enrollment' in metrics
        assert 'ib_course_enrollment' in metrics

        # Verify demographics are present
        student_groups = df['student_group'].unique()
        assert 'All Students' in student_groups
        assert 'Female' in student_groups

    def test_transform_historical_data(self):
        """Test transform with historical format data."""
        data = self.create_historical_data()
        data.to_csv(self.sample_dir / "advanced_courses_participation_and_performance_2022.csv", index=False)

        config = {
            "derive": {
                "processing_date": "2025-11-24"
            }
        }

        transform(self.raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "advanced_coursework.csv"
        assert output_file.exists()

        df = pd.read_csv(output_file)

        # Verify metrics
        metrics = df['metric'].unique()
        assert 'ap_course_enrollment' in metrics

        # Verify year extraction
        assert '2022' in df['year'].astype(str).values

    def test_suppressed_values(self):
        """Test handling of suppressed values."""
        data = pd.DataFrame({
            'School Year': ['20232024'],
            'County Number': ['001'],
            'County Name': ['Adair'],
            'District Number': ['001'],
            'District Name': ['Adair County'],
            'School Number': [''],
            'School Name': ['---District Total---'],
            'School Code': ['001'],
            'State School Id': [''],
            'NCES ID': [''],
            'CO-OP': [''],
            'CO-OP Code': [''],
            'School Type': [''],
            'Demographic': ['All Students'],
            'Advanced Course Type': ['AP'],
            'Course Enrollment': ['*'],
            'Course Completers': ['*'],
            'Number Tested': ['*'],
            'Earned Qualifying Score': ['*'],
        })
        data.to_csv(self.sample_dir / "test_suppressed.csv", index=False)

        config = {"derive": {"processing_date": "2025-11-24"}}
        transform(self.raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "advanced_coursework.csv"

        # When all values are suppressed, no file is written or file is empty/header only
        if output_file.exists() and output_file.stat().st_size > 0:
            try:
                df = pd.read_csv(output_file)
                # If file has data, all values should be NA
                if len(df) > 0:
                    assert df['value'].isna().all()
            except pd.errors.EmptyDataError:
                # Empty file is also valid (all suppressed)
                pass
        # If file doesn't exist, that's also valid (all suppressed)


class TestAdvancedCourseworkHelpers:
    """Test helper functions independently."""

    def setup_method(self):
        """Setup ETL instance for testing."""
        self.etl = AdvancedCourseworkETL('advanced_coursework')

    def test_get_suppressed_metric_defaults_ap(self):
        """Test suppressed metric defaults for AP."""
        row = pd.Series({'course_type': 'AP'})

        defaults = self.etl.get_suppressed_metric_defaults(row)

        assert 'ap_course_enrollment' in defaults
        assert pd.isna(defaults['ap_course_enrollment'])

    def test_get_suppressed_metric_defaults_ib(self):
        """Test suppressed metric defaults for IB."""
        row = pd.Series({'course_type': 'IB'})

        defaults = self.etl.get_suppressed_metric_defaults(row)

        assert 'ib_course_enrollment' in defaults
        assert pd.isna(defaults['ib_course_enrollment'])

    def test_should_skip_row_with_demographic(self):
        """Test row skipping with demographic column."""
        row_with_demo = pd.Series({
            'demographic': 'All Students',
        })
        row_no_demo = pd.Series({
            'demographic': pd.NA,
        })

        # Row with demographic should not be skipped
        assert not self.etl.should_skip_row(row_with_demo)

        # Row without demographic should not be skipped (for offered files)
        assert not self.etl.should_skip_row(row_no_demo)
