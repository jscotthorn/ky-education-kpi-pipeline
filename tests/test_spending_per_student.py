"""
Tests for Spending Per Student ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
from etl.constants import KPI_COLUMNS
import shutil
from etl.spending_per_student import transform, clean_spending_values, SpendingPerStudentETL


class TestSpendingPerStudentETL:

    def setup_method(self):
        """Setup test directories and sample data."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)

        # Create sample raw data directory
        self.sample_dir = self.raw_dir / "spending_per_student"
        self.sample_dir.mkdir(parents=True)

        # Create ETL instance for testing
        self.etl = SpendingPerStudentETL('spending_per_student')

    def teardown_method(self):
        """Clean up test directories."""
        shutil.rmtree(self.test_dir)

    def create_sample_2024_data(self):
        """Create sample 2024 format data with comma-separated values."""
        data = pd.DataFrame({
            'School Year': ['20232024', '20232024', '20232024'],
            'County Number': ['001', '001', '001'],
            'County Name': ['Adair', 'Adair', 'Adair'],
            'District Number': ['001', '001', '001'],
            'District Name': ['Adair County', 'Adair County', 'Adair County'],
            'School Number': ['', '010', '012'],
            'School Name': ['All Schools', 'Adair County High School', 'Adair Learning Academy'],
            'School Code': ['001', '001010', '001012'],
            'State School Id': ['', '001001010', '001001012'],
            'NCES ID': ['', '210003000001', '210003001913'],
            'CO-OP': ['GRREC', 'GRREC', 'GRREC'],
            'CO-OP CODE': ['902', '902', '902'],
            'School Type': ['', 'A1', 'A6'],
            'Personnel Expenditures State Local Per Student': ['10,387', '9,827', ''],
            'Non-Personnel Expenditures State Local Per Student': ['2,448', '2,136', ''],
            'Personnel Spending per Student - Federal Funds': ['1,271', '450', ''],
            'Non-Personnel Spending per Student - Federal Funds': ['82', '184', ''],
            'Total Spending per Student - State/Local Funds': ['12,835', '12,187', ''],
            'Total Expenditures Federal Per Student': ['1,353', '688', ''],
            'Total Expenditures Per Student': ['15,946', '13,160', '']
        })
        return data

    def create_sample_2020_data(self):
        """Create sample 2020 format data with uppercase columns and plain numbers."""
        data = pd.DataFrame({
            'SCHOOL YEAR': ['20192020', '20192020', '20192020'],
            'COUNTY NUMBER': ['001', '001', '001'],
            'COUNTY NAME': ['ADAIR', 'ADAIR', 'ADAIR'],
            'DISTRICT NUMBER': ['001', '001', '001'],
            'DISTRICT NAME': ['Adair County', 'Adair County', 'Adair County'],
            'SCHOOL NUMBER': ['', '010', '012'],
            'SCHOOL NAME': ['---District Total---', 'Adair County High School', 'Adair Learning Academy'],
            'SCHOOL CODE': ['001', '001010', '001012'],
            'STATE SCHOOL ID': ['', '001001010', '001001012'],
            'NCES ID': ['', '210003000001', '210003001913'],
            'CO-OP': ['GRREC', 'GRREC', 'GRREC'],
            'CO-OP CODE': ['902', '902', '902'],
            'MEMBERSHIP': ['2598', '798', '17'],
            'Personnel Spending per Student - Federal Funds': [897.0, 698.0, None],
            'Non-Personnel Spending per Student - Federal Funds': [142.0, 122.0, None],
            'Total Spending per Student - Federal Funds': [1039.0, 820.0, None],
            'Personnel Spending per Student - State/Local Funds': [8496.0, 8161.0, None],
            'Non-Personnel Spending per Student - State/Local Funds': [1534.0, 1617.0, None],
            'Total Spending per Student - State/Local Funds': [10029.0, 9778.0, None],
            'Total Spending per Student - All Fund Sources': [12174.0, 11646.0, None]
        })
        return data

    def test_normalize_column_names(self):
        """Test column name normalization for 2024 format."""
        df_2024 = self.create_sample_2024_data()
        df_normalized = self.etl.normalize_column_names(df_2024)

        assert 'school_year' in df_normalized.columns
        assert 'county_name' in df_normalized.columns
        assert 'personnel_spending_state_local' in df_normalized.columns
        assert 'total_spending_all_funds' in df_normalized.columns
        assert 'School Year' not in df_normalized.columns

    def test_normalize_column_names_2020_format(self):
        """Test column name normalization for 2020 uppercase format."""
        df_2020 = self.create_sample_2020_data()
        df_normalized = self.etl.normalize_column_names(df_2020)

        assert 'school_year' in df_normalized.columns
        assert 'county_name' in df_normalized.columns
        assert 'personnel_spending_federal' in df_normalized.columns
        assert 'total_spending_all_funds' in df_normalized.columns
        assert 'membership' in df_normalized.columns
        assert 'SCHOOL YEAR' not in df_normalized.columns

    def test_clean_spending_values(self):
        """Test spending value cleaning (comma removal and numeric conversion)."""
        df = pd.DataFrame({
            'personnel_spending_state_local': ['10,387', '9,827', '', '12,500'],
            'total_spending_federal': ['1,353', '688', '*', '-100'],
            'non_numeric_col': ['text1', 'text2', 'text3', 'text4']
        })

        df_clean = clean_spending_values(df)

        # Check comma removal and numeric conversion
        assert df_clean.loc[0, 'personnel_spending_state_local'] == 10387.0
        assert df_clean.loc[1, 'personnel_spending_state_local'] == 9827.0
        assert pd.isna(df_clean.loc[2, 'personnel_spending_state_local'])

        # Check negative value handling
        assert pd.isna(df_clean.loc[3, 'total_spending_federal'])  # Negative becomes NaN

        # Non-spending columns should remain unchanged
        assert df_clean['non_numeric_col'].tolist() == ['text1', 'text2', 'text3', 'text4']

    def test_should_skip_row(self):
        """Test row skipping logic."""
        # Test with valid school code - should NOT skip
        row_valid = pd.Series({
            'school_code': '001010',
            'school_name': 'Test School'
        })
        assert not self.etl.should_skip_row(row_valid)

        # Test with missing school code - should skip
        row_no_code = pd.Series({
            'school_code': '',
            'school_name': 'Test School'
        })
        assert self.etl.should_skip_row(row_no_code)

        # Test with district total - should NOT skip (spending data includes these)
        row_district = pd.Series({
            'school_code': '001',
            'school_name': '---District Total---'
        })
        assert not self.etl.should_skip_row(row_district)

    def test_create_kpi_template(self):
        """Test KPI template creation with All Students demographic."""
        row = pd.Series({
            'school_code': '001010',
            'school_name': 'Test School',
            'district_name': 'Test District',
            'county_number': '001',
            'county_name': 'Test County',
            'district_number': '001',
            'school_year': '20232024',
            'suppressed': 'N'
        })

        template = self.etl.create_kpi_template(row, 'test_file.csv')

        # Check that student_group is always "All Students"
        assert template['student_group'] == 'All Students'
        assert template['school_id'] == '001010'
        assert template['year'] == '2024'
        assert template['suppressed'] == 'N'

    def test_extract_metrics(self):
        """Test metric extraction from spending data row."""
        row = pd.Series({
            'personnel_spending_federal': 450.0,
            'non_personnel_spending_federal': 184.0,
            'total_spending_federal': 688.0,
            'personnel_spending_state_local': 9827.0,
            'non_personnel_spending_state_local': 2136.0,
            'total_spending_state_local': 12187.0,
            'total_spending_all_funds': 13160.0
        })

        metrics = self.etl.extract_metrics(row)

        # Verify all 7 metrics are extracted
        assert len(metrics) == 7
        assert metrics['personnel_spending_per_student_federal'] == 450.0
        assert metrics['non_personnel_spending_per_student_federal'] == 184.0
        assert metrics['total_spending_per_student_federal'] == 688.0
        assert metrics['personnel_spending_per_student_state_local'] == 9827.0
        assert metrics['non_personnel_spending_per_student_state_local'] == 2136.0
        assert metrics['total_spending_per_student_state_local'] == 12187.0
        assert metrics['total_spending_per_student_all_funds'] == 13160.0

    def test_extract_metrics_partial_data(self):
        """Test metric extraction with some missing values."""
        row = pd.Series({
            'personnel_spending_federal': 450.0,
            'non_personnel_spending_federal': None,
            'total_spending_federal': 688.0,
            'personnel_spending_state_local': None,
            'non_personnel_spending_state_local': 2136.0,
            'total_spending_state_local': None,
            'total_spending_all_funds': 13160.0
        })

        metrics = self.etl.extract_metrics(row)

        # Only non-null metrics should be extracted
        assert len(metrics) == 4
        assert 'personnel_spending_per_student_federal' in metrics
        assert 'total_spending_per_student_federal' in metrics
        assert 'non_personnel_spending_per_student_state_local' in metrics
        assert 'total_spending_per_student_all_funds' in metrics

    def test_get_suppressed_metric_defaults(self):
        """Test default metrics for suppressed records."""
        row = pd.Series({})
        defaults = self.etl.get_suppressed_metric_defaults(row)

        # Should have all 7 metrics with NA values
        assert len(defaults) == 7
        assert all(pd.isna(val) for val in defaults.values())

    def test_transform_2024_format(self):
        """Test full transform with 2024 format data."""
        data = self.create_sample_2024_data()
        data.to_csv(self.sample_dir / "spending_per_student_2024.csv", index=False)

        config = {
            "derive": {
                "processing_date": "2025-11-24"
            }
        }

        transform(self.raw_dir, self.proc_dir, config)

        # Check output file exists
        output_file = self.proc_dir / "spending_per_student.csv"
        assert output_file.exists()

        # Check KPI format transformations
        df = pd.read_csv(output_file)

        # Verify KPI format columns
        required_columns = KPI_COLUMNS
        for col in required_columns:
            assert col in df.columns, f"Required KPI column '{col}' missing"

        # Verify all spending metrics are present
        metrics = df['metric'].unique()
        expected_metrics = [
            'personnel_spending_per_student_federal',
            'non_personnel_spending_per_student_federal',
            'total_spending_per_student_federal',
            'personnel_spending_per_student_state_local',
            'non_personnel_spending_per_student_state_local',
            'total_spending_per_student_state_local',
            'total_spending_per_student_all_funds'
        ]
        for expected_metric in expected_metrics:
            assert expected_metric in metrics, f"Missing spending metric: {expected_metric}"

        # Verify values were properly cleaned (no commas)
        assert df['value'].notna().any()
        values = df['value'].dropna()
        assert all(isinstance(v, (int, float)) for v in values)

        # Verify student group is always "All Students"
        assert (df['student_group'] == 'All Students').all()

        # Verify district total is preserved (standardized to ---District Total---)
        district_total_rows = df[df['school_name'] == '---District Total---']
        assert len(district_total_rows) > 0

    def test_transform_2020_format(self):
        """Test full transform with 2020 format data."""
        data = self.create_sample_2020_data()
        data.to_csv(self.sample_dir / "spending_per_student_2020.csv", index=False)

        config = {
            "derive": {
                "processing_date": "2025-11-24"
            }
        }

        transform(self.raw_dir, self.proc_dir, config)

        # Check output file exists
        output_file = self.proc_dir / "spending_per_student.csv"
        assert output_file.exists()

        df = pd.read_csv(output_file)

        # Verify year extraction (stored as integer)
        assert 2020 in df['year'].unique()

        # Verify district total standardization
        district_rows = df[df['school_name'] == '---District Total---']
        assert len(district_rows) > 0

        # Verify all metrics extracted from 2020 format
        metrics = df['metric'].unique()
        assert len(metrics) == 7

    def test_mixed_year_formats(self):
        """Test transform with both 2020 and 2024 format files."""
        data_2024 = self.create_sample_2024_data()
        data_2020 = self.create_sample_2020_data()

        data_2024.to_csv(self.sample_dir / "spending_per_student_2024.csv", index=False)
        data_2020.to_csv(self.sample_dir / "spending_per_student_2020.csv", index=False)

        config = {
            "derive": {
                "processing_date": "2025-11-24"
            }
        }

        transform(self.raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "spending_per_student.csv"
        df = pd.read_csv(output_file)

        # Verify both years are present (stored as integers)
        years = df['year'].unique()
        assert 2020 in years
        assert 2024 in years

        # Verify consistent student group across all years
        assert (df['student_group'] == 'All Students').all()

        # Verify all metrics present for both years
        for year in [2020, 2024]:
            year_df = df[df['year'] == year]
            metrics = year_df['metric'].unique()
            assert len(metrics) == 7, f"Expected 7 metrics for year {year}, got {len(metrics)}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
