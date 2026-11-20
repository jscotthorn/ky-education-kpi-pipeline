"""
Tests for Dual Credit ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
from etl.constants import KPI_COLUMNS
import shutil
from etl.dual_credit import transform, DualCreditETL, clean_numeric_with_commas


class TestDualCreditETL:

    def setup_method(self):
        """Setup test directories and sample data."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)

        # Create sample raw data directory
        self.sample_dir = self.raw_dir / "dual_credit"
        self.sample_dir.mkdir(parents=True)

        # Create ETL instance for testing
        self.etl = DualCreditETL('dual_credit')

    def teardown_method(self):
        """Clean up test directories."""
        shutil.rmtree(self.test_dir)

    def create_participation_data(self):
        """Create sample dual credit participation data."""
        data = pd.DataFrame({
            'School Year': ['20242025', '20242025', '20242025', '20242025'],
            'County Number': ['001', '001', '001', '001'],
            'County Name': ['Adair', 'Adair', 'Adair', 'Adair'],
            'District Number': ['001', '001', '001', '001'],
            'District Name': ['Adair County', 'Adair County', 'Adair County', 'Adair County'],
            'School Number': ['', '', '', ''],
            'School Name': ['All Schools', 'All Schools', 'All Schools', 'All Schools'],
            'School Code': ['999000', '999000', '999000', '999000'],
            'State School Id': ['', '', '', ''],
            'NCES ID': ['', '', '', ''],
            'CO-OP': ['', '', '', ''],
            'CO-OP Code': ['', '', '', ''],
            'School Type': ['', '', '', ''],
            'Demographic': ['All Students', 'Female', 'Male', 'Economically Disadvantaged'],
            'Advanced Course Type': ['Dual Credit', 'Dual Credit', 'Dual Credit', 'Dual Credit'],
            'Subject': ['All Enrollments', 'All Enrollments', 'All Enrollments', 'All Enrollments'],
            'Course Enrollment': ['52,170', '28,500', '23,670', '18,000'],
            'Course Completers': ['48,729', '26,800', '21,929', '16,500'],
            'Students With Qualifying Score': ['46,828', '25,500', '21,328', '15,800'],
        })
        return data

    def create_offered_data(self):
        """Create sample dual credit courses offered data."""
        data = pd.DataFrame({
            'School Year': ['20242025', '20242025', '20242025'],
            'County Number': ['', '', ''],
            'County Name': ['', '', ''],
            'District Number': ['999', '999', '999'],
            'District Name': ['All Districts', 'All Districts', 'All Districts'],
            'School Number': ['', '', ''],
            'School Name': ['All Schools', 'All Schools', 'All Schools'],
            'School Code': ['999000', '999000', '999000'],
            'State School Id': ['', '', ''],
            'NCES ID': ['', '', ''],
            'CO-OP': ['', '', ''],
            'CO-OP Code': ['', '', ''],
            'School Type': ['', '', ''],
            'Advanced Course Type': ['Dual Credit', 'Dual Credit', 'Dual Credit'],
            'Subject': ['All Enrollments', 'Mathematics', 'English/Language Arts'],
            'Course Enrollment': ['136,572', '952', '15,000'],
            'Course Completers': ['124,028', '836', '14,200'],
            'Students With Qualifying Score': ['117,355', '737', '13,800'],
        })
        return data

    def test_normalize_column_names(self):
        """Test column name normalization."""
        df = self.create_participation_data()
        df_normalized = self.etl.normalize_column_names(df)

        assert 'school_year' in df_normalized.columns
        assert 'course_type' in df_normalized.columns
        assert 'course_enrollment' in df_normalized.columns

    def test_clean_numeric_with_commas(self):
        """Test numeric value cleaning with commas."""
        series = pd.Series(['52,170', '100', '*', '', '---'])
        result = clean_numeric_with_commas(series)

        assert result.iloc[0] == 52170.0
        assert result.iloc[1] == 100.0
        assert pd.isna(result.iloc[2])
        assert pd.isna(result.iloc[3])

    def test_extract_metrics(self):
        """Test metric extraction from dual credit row."""
        row = pd.Series({
            'course_enrollment': '1000',
            'course_completers': '950',
            'qualifying_score': '900',
        })

        metrics = self.etl.extract_metrics(row)

        assert 'dual_credit_enrollment' in metrics
        assert metrics['dual_credit_enrollment'] == 1000
        assert metrics['dual_credit_completion_count'] == 950
        assert metrics['dual_credit_qualifying_score_count'] == 900
        assert 'dual_credit_completion_rate' in metrics
        assert metrics['dual_credit_completion_rate'] == 95.0

    def test_extract_metrics_with_qualifying_rate(self):
        """Test qualifying score rate calculation."""
        row = pd.Series({
            'course_enrollment': '100',
            'course_completers': '80',
            'qualifying_score': '72',
        })

        metrics = self.etl.extract_metrics(row)

        # Rate is based on completers, not enrollment
        assert 'dual_credit_qualifying_score_rate' in metrics
        assert metrics['dual_credit_qualifying_score_rate'] == 90.0

    def test_transform_participation_data(self):
        """Test transform with participation data."""
        data = self.create_participation_data()
        data.to_csv(self.sample_dir / "KYRC25_EDOP_Dual_Credit_Participation_and_Performance.csv", index=False)

        config = {
            "derive": {
                "processing_date": "2025-11-24"
            }
        }

        transform(self.raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "dual_credit.csv"
        assert output_file.exists()

        df = pd.read_csv(output_file)

        # Verify KPI format columns
        for col in KPI_COLUMNS:
            assert col in df.columns, f"Required KPI column '{col}' missing"

        # Verify expected metrics are present
        metrics = df['metric'].unique()
        assert 'dual_credit_enrollment' in metrics
        assert 'dual_credit_completion_count' in metrics

        # Verify demographics are present
        student_groups = df['student_group'].unique()
        assert 'All Students' in student_groups
        assert 'Female' in student_groups

    def test_transform_offered_data(self):
        """Test transform with courses offered data."""
        data = self.create_offered_data()
        data.to_csv(self.sample_dir / "KYRC25_EDOP_Dual_Credit_Courses_Offered.csv", index=False)

        config = {
            "derive": {
                "processing_date": "2025-11-24"
            }
        }

        transform(self.raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "dual_credit.csv"
        assert output_file.exists()

        df = pd.read_csv(output_file)

        # Should have data from offered file
        assert len(df) > 0
        assert 'dual_credit_enrollment' in df['metric'].values

    def test_suppressed_values(self):
        """Test handling of suppressed values."""
        data = pd.DataFrame({
            'School Year': ['20242025'],
            'County Number': ['001'],
            'County Name': ['Adair'],
            'District Number': ['001'],
            'District Name': ['Adair County'],
            'School Number': [''],
            'School Name': ['All Schools'],
            'School Code': ['001'],
            'State School Id': [''],
            'NCES ID': [''],
            'CO-OP': [''],
            'CO-OP Code': [''],
            'School Type': [''],
            'Demographic': ['All Students'],
            'Advanced Course Type': ['Dual Credit'],
            'Subject': ['All Enrollments'],
            'Course Enrollment': ['*'],
            'Course Completers': ['*'],
            'Students With Qualifying Score': ['*'],
        })
        data.to_csv(self.sample_dir / "test_suppressed.csv", index=False)

        config = {"derive": {"processing_date": "2025-11-24"}}
        transform(self.raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "dual_credit.csv"

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

    def test_has_dual_credit_program_indicator(self):
        """Test has_dual_credit_program boolean indicator."""
        row_with_enrollment = pd.Series({
            'course_enrollment': '100',
            'course_completers': '80',
        })
        row_zero_enrollment = pd.Series({
            'course_enrollment': '0',
            'course_completers': '0',
        })

        metrics_with = self.etl.extract_metrics(row_with_enrollment)
        metrics_zero = self.etl.extract_metrics(row_zero_enrollment)

        assert metrics_with.get('has_dual_credit_program') == 1
        assert metrics_zero.get('has_dual_credit_program') == 0


class TestDualCreditHelpers:
    """Test helper functions independently."""

    def setup_method(self):
        """Setup ETL instance for testing."""
        self.etl = DualCreditETL('dual_credit')

    def test_get_suppressed_metric_defaults(self):
        """Test suppressed metric defaults."""
        row = pd.Series({})

        defaults = self.etl.get_suppressed_metric_defaults(row)

        assert 'dual_credit_enrollment' in defaults
        assert pd.isna(defaults['dual_credit_enrollment'])
        assert 'dual_credit_completion_count' in defaults
        assert pd.isna(defaults['dual_credit_completion_count'])

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
