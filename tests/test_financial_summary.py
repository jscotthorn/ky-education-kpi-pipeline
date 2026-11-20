"""
Tests for Financial Summary ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
from etl.constants import KPI_COLUMNS
import shutil
from etl.financial_summary import transform, clean_numeric_values, FinancialSummaryETL


class TestFinancialSummaryETL:

    def setup_method(self):
        """Setup test directories and sample data."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)

        # Create sample raw data directory
        self.sample_dir = self.raw_dir / "financial_summary"
        self.sample_dir.mkdir(parents=True)

        # Create ETL instance for testing
        self.etl = FinancialSummaryETL('financial_summary')

    def teardown_method(self):
        """Clean up test directories."""
        shutil.rmtree(self.test_dir)

    def create_sample_kyrc24_data(self):
        """Create sample KYRC24 format data with comma-separated values."""
        data = pd.DataFrame({
            'School Year': ['20232024', '20232024', '20232024', '20232024'],
            'County Number': ['', '001', '002', '056'],
            'County Name': ['', 'ADAIR', 'ALLEN', 'JEFFERSON'],
            'District Number': ['999', '001', '005', '006'],
            'District Name': ['All Districts', 'Adair County', 'Allen County', 'Anchorage Independent'],
            'School Number': ['', '', '', ''],
            'School Name': ['All Schools', 'All Schools', 'All Schools', 'All Schools'],
            'School Code': ['999000', '001000', '005000', '006000'],
            'State School Id': ['', '', '', ''],
            'NCES ID': ['', '', '', ''],
            'CO-OP': ['', 'GRREC', 'GRREC', 'OVEC'],
            'CO-OP Code': ['', '902', '902', '906'],
            'School Type': ['', '', '', ''],
            'End-of-Year Student Membership': ['636,427', '2,546', '2,977', '397'],
            'Fund Balance': ['1,896,359,959', '992,533', '7,365,132', '3,343,446'],
            'Fund Balance %': ['30', '5', '34', '43'],
            'Certified Staff': ['52,601', '219', '236', '47'],
            'Certified Staff Teachers': ['43,103', '187', '185', '40'],
            'Classified Staff': ['45,454', '168', '203', '29']
        })
        return data

    def create_sample_2020_data(self):
        """Create sample 2020 format data with uppercase columns."""
        data = pd.DataFrame({
            'SCHOOL YEAR': ['20192020', '20192020', '20192020'],
            'COUNTY NUMBER': ['001', '002', '056'],
            'COUNTY NAME': ['ADAIR', 'ALLEN', 'JEFFERSON'],
            'DISTRICT NUMBER': ['001', '005', '006'],
            'DISTRICT NAME': ['Adair County', 'Allen County', 'Anchorage Independent'],
            'SCHOOL NUMBER': ['', '', ''],
            'SCHOOL NAME': ['---District Total---', '---District Total---', '---District Total---'],
            'SCHOOL CODE': ['001', '005', '006'],
            'STATE SCHOOL ID': ['', '', ''],
            'NCES ID': ['', '', ''],
            'CO-OP': ['GRREC', 'GRREC', 'OVEC'],
            'CO-OP CODE': ['902', '902', '906'],
            'MEMBERSHIP': ['2598', '2968', '392'],
            'FUND BALANCE': ['3434770', '2705900', '3384033'],
            'FUND BALANCE %': ['18.4', '12.81', '50.9'],
            'FTE CERTIFIED STAFF': ['205', '220.9515', '44.7106'],
            'FTE CERTIFIED STAFF - TEACHERS': ['175', '185.3064', '38.0351'],
            'FTE CLASSIFIED STAFF': ['172', '204.49', '27.68']
        })
        return data

    def test_normalize_column_names_kyrc24(self):
        """Test column name normalization for KYRC24 format."""
        df_kyrc24 = self.create_sample_kyrc24_data()
        df_normalized = self.etl.normalize_column_names(df_kyrc24)

        assert 'school_year' in df_normalized.columns
        assert 'county_name' in df_normalized.columns
        assert 'membership' in df_normalized.columns
        assert 'fund_balance' in df_normalized.columns
        assert 'fund_balance_pct' in df_normalized.columns
        assert 'certified_staff' in df_normalized.columns
        assert 'certified_staff_teachers' in df_normalized.columns
        assert 'classified_staff' in df_normalized.columns
        assert 'School Year' not in df_normalized.columns

    def test_normalize_column_names_2020_format(self):
        """Test column name normalization for 2020 uppercase format."""
        df_2020 = self.create_sample_2020_data()
        df_normalized = self.etl.normalize_column_names(df_2020)

        assert 'school_year' in df_normalized.columns
        assert 'county_name' in df_normalized.columns
        assert 'membership' in df_normalized.columns
        assert 'fund_balance' in df_normalized.columns
        assert 'fund_balance_pct' in df_normalized.columns
        assert 'certified_staff' in df_normalized.columns
        assert 'certified_staff_teachers' in df_normalized.columns
        assert 'classified_staff' in df_normalized.columns
        assert 'SCHOOL YEAR' not in df_normalized.columns

    def test_clean_numeric_values(self):
        """Test numeric value cleaning (comma removal and numeric conversion)."""
        df = pd.DataFrame({
            'membership': ['636,427', '2,546', '', '2,977'],
            'fund_balance': ['1,896,359,959', '992,533', '*', '7,365,132'],
            'fund_balance_pct': ['30', '5', '34', '43'],
            'non_numeric_col': ['text1', 'text2', 'text3', 'text4']
        })

        numeric_cols = ['membership', 'fund_balance', 'fund_balance_pct']
        df_clean = clean_numeric_values(df, numeric_cols)

        # Check comma removal and numeric conversion
        assert df_clean.loc[0, 'membership'] == 636427.0
        assert df_clean.loc[1, 'membership'] == 2546.0
        assert pd.isna(df_clean.loc[2, 'membership'])

        # Check fund balance
        assert df_clean.loc[0, 'fund_balance'] == 1896359959.0
        assert df_clean.loc[1, 'fund_balance'] == 992533.0

        # Non-numeric columns should remain unchanged
        assert df_clean['non_numeric_col'].tolist() == ['text1', 'text2', 'text3', 'text4']

    def test_should_skip_row(self):
        """Test row skipping logic."""
        # Test with valid district school code - should NOT skip
        row_valid = pd.Series({
            'school_code': '001000',
            'school_name': 'All Schools'
        })
        assert not self.etl.should_skip_row(row_valid)

        # Test with missing school code - should skip
        row_no_code = pd.Series({
            'school_code': '',
            'school_name': 'Test District'
        })
        assert self.etl.should_skip_row(row_no_code)

        # Test with state total (999) - should skip
        row_state_total = pd.Series({
            'school_code': '999',
            'school_name': 'All Districts'
        })
        assert self.etl.should_skip_row(row_state_total)

        # Test with state total (999000) - should skip
        row_state_total_2 = pd.Series({
            'school_code': '999000',
            'school_name': 'All Schools'
        })
        assert self.etl.should_skip_row(row_state_total_2)

        # Test with historical district total format - should NOT skip
        row_district = pd.Series({
            'school_code': '001',
            'school_name': '---District Total---'
        })
        assert not self.etl.should_skip_row(row_district)

    def test_create_kpi_template(self):
        """Test KPI template creation with All Students demographic."""
        row = pd.Series({
            'school_code': '001000',
            'school_name': 'All Schools',
            'district_name': 'Adair County',
            'county_number': '001',
            'county_name': 'ADAIR',
            'district_number': '001',
            'school_year': '20232024',
            'suppressed': 'N'
        })

        template = self.etl.create_kpi_template(row, 'test_file.csv')

        # Check that student_group is always "All Students"
        assert template['student_group'] == 'All Students'
        assert template['school_id'] == '001000'
        assert template['year'] == '2024'
        assert template['suppressed'] == 'N'

    def test_extract_metrics(self):
        """Test metric extraction from financial summary data row."""
        row = pd.Series({
            'membership': 2546.0,
            'fund_balance': 992533.0,
            'fund_balance_pct': 5.0,
            'certified_staff': 219.0,
            'certified_staff_teachers': 187.0,
            'classified_staff': 168.0
        })

        metrics = self.etl.extract_metrics(row)

        # Verify all 8 metrics are extracted (including derived)
        assert len(metrics) == 8
        assert metrics['eoy_student_membership'] == 2546.0
        assert metrics['fund_balance'] == 992533.0
        assert metrics['fund_balance_pct'] == 5.0
        assert metrics['certified_staff_fte'] == 219.0
        assert metrics['certified_staff_teachers_fte'] == 187.0
        assert metrics['certified_staff_non_teachers_fte'] == 32.0  # Derived: 219 - 187
        assert metrics['classified_staff_fte'] == 168.0
        assert metrics['total_staff_fte'] == 387.0  # Derived: 219 + 168

    def test_extract_metrics_partial_data(self):
        """Test metric extraction with some missing values."""
        row = pd.Series({
            'membership': 2546.0,
            'fund_balance': None,
            'fund_balance_pct': None,
            'certified_staff': 219.0,
            'certified_staff_teachers': 187.0,
            'classified_staff': None
        })

        metrics = self.etl.extract_metrics(row)

        # Only non-null metrics should be extracted
        assert 'eoy_student_membership' in metrics
        assert 'fund_balance' not in metrics
        assert 'fund_balance_pct' not in metrics
        assert 'certified_staff_fte' in metrics
        assert 'certified_staff_teachers_fte' in metrics
        assert 'certified_staff_non_teachers_fte' in metrics  # Can still be derived
        assert 'classified_staff_fte' not in metrics
        assert 'total_staff_fte' not in metrics  # Cannot be derived without classified staff

    def test_get_suppressed_metric_defaults(self):
        """Test default metrics for suppressed records."""
        row = pd.Series({})
        defaults = self.etl.get_suppressed_metric_defaults(row)

        # Should have all 8 metrics with NA values
        assert len(defaults) == 8
        assert all(pd.isna(val) for val in defaults.values())

    def test_transform_kyrc24_format(self):
        """Test full transform with KYRC24 format data."""
        data = self.create_sample_kyrc24_data()
        data.to_csv(self.sample_dir / "KYRC24_FT_Financial_Summary.csv", index=False)

        config = {
            "derive": {
                "processing_date": "2025-11-24"
            }
        }

        transform(self.raw_dir, self.proc_dir, config)

        # Check output file exists
        output_file = self.proc_dir / "financial_summary.csv"
        assert output_file.exists()

        # Check KPI format transformations
        df = pd.read_csv(output_file)

        # Verify KPI format columns
        required_columns = KPI_COLUMNS
        for col in required_columns:
            assert col in df.columns, f"Required KPI column '{col}' missing"

        # Verify all financial summary metrics are present
        metrics = df['metric'].unique()
        expected_metrics = [
            'eoy_student_membership',
            'fund_balance',
            'fund_balance_pct',
            'certified_staff_fte',
            'certified_staff_teachers_fte',
            'certified_staff_non_teachers_fte',
            'classified_staff_fte',
            'total_staff_fte'
        ]
        for expected_metric in expected_metrics:
            assert expected_metric in metrics, f"Missing financial metric: {expected_metric}"

        # Verify values were properly cleaned (no commas)
        assert df['value'].notna().any()
        values = df['value'].dropna()
        assert all(isinstance(v, (int, float)) for v in values)

        # Verify student group is always "All Students"
        assert (df['student_group'] == 'All Students').all()

        # Verify state total rows are filtered out
        state_rows = df[df['school_code'] == '999000']
        assert len(state_rows) == 0

        # Verify district rows are preserved
        district_rows = df[df['school_code'] != '999000']
        assert len(district_rows) > 0

    def test_transform_2020_format(self):
        """Test full transform with 2020 format data."""
        data = self.create_sample_2020_data()
        data.to_csv(self.sample_dir / "financial_summary_2020.csv", index=False)

        config = {
            "derive": {
                "processing_date": "2025-11-24"
            }
        }

        transform(self.raw_dir, self.proc_dir, config)

        # Check output file exists
        output_file = self.proc_dir / "financial_summary.csv"
        assert output_file.exists()

        df = pd.read_csv(output_file)

        # Verify year extraction (stored as integer)
        assert 2020 in df['year'].unique()

        # Verify district total standardization
        district_rows = df[df['school_name'] == '---District Total---']
        assert len(district_rows) > 0

        # Verify all 8 metrics extracted
        metrics = df['metric'].unique()
        assert len(metrics) == 8

    def test_mixed_year_formats(self):
        """Test transform with both 2020 and KYRC24 format files."""
        data_kyrc24 = self.create_sample_kyrc24_data()
        data_2020 = self.create_sample_2020_data()

        data_kyrc24.to_csv(self.sample_dir / "KYRC24_FT_Financial_Summary.csv", index=False)
        data_2020.to_csv(self.sample_dir / "financial_summary_2020.csv", index=False)

        config = {
            "derive": {
                "processing_date": "2025-11-24"
            }
        }

        transform(self.raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "financial_summary.csv"
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
            assert len(metrics) == 8, f"Expected 8 metrics for year {year}, got {len(metrics)}"

    def test_derived_metrics_calculation(self):
        """Test calculation of derived metrics."""
        # Create data where derived metrics can be verified
        data = pd.DataFrame({
            'SCHOOL YEAR': ['20232024'],
            'COUNTY NUMBER': ['001'],
            'COUNTY NAME': ['ADAIR'],
            'DISTRICT NUMBER': ['001'],
            'DISTRICT NAME': ['Adair County'],
            'SCHOOL NUMBER': [''],
            'SCHOOL NAME': ['---District Total---'],
            'SCHOOL CODE': ['001'],
            'STATE SCHOOL ID': [''],
            'NCES ID': [''],
            'CO-OP': ['GRREC'],
            'CO-OP CODE': ['902'],
            'MEMBERSHIP': ['1000'],
            'FUND BALANCE': ['500000'],
            'FUND BALANCE %': ['25'],
            'FTE CERTIFIED STAFF': ['100'],
            'FTE CERTIFIED STAFF - TEACHERS': ['75'],
            'FTE CLASSIFIED STAFF': ['50']
        })
        data.to_csv(self.sample_dir / "financial_summary_test.csv", index=False)

        config = {"derive": {"processing_date": "2025-11-24"}}
        transform(self.raw_dir, self.proc_dir, config)

        df = pd.read_csv(self.proc_dir / "financial_summary.csv")

        # Verify we have data
        assert len(df) > 0, "No data in output file"

        # Verify derived metrics (school_code may be int or string)
        non_teachers_row = df[df['metric'] == 'certified_staff_non_teachers_fte']
        total_staff_row = df[df['metric'] == 'total_staff_fte']

        assert len(non_teachers_row) > 0, "Missing certified_staff_non_teachers_fte metric"
        assert len(total_staff_row) > 0, "Missing total_staff_fte metric"

        non_teachers = non_teachers_row['value'].iloc[0]
        total_staff = total_staff_row['value'].iloc[0]

        assert non_teachers == 25.0  # 100 - 75
        assert total_staff == 150.0  # 100 + 50


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
