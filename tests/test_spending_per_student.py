"""
Tests for Spending Per Student ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
from etl.constants import KPI_COLUMNS
import shutil
from etl.spending_per_student import (
    transform,
    clean_spending_values,
    select_district_state_spending,
    SpendingPerStudentETL,
)


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

    def test_comma_formatted_values_on_string_and_object(self):
        """Thousands separators survive both dtypes.

        pandas 3 ``read_csv(..., dtype=str)`` is StringDtype. pandas 2.3.3
        is object. ``20,164`` has to stay 20164 on both. A published 0
        stays 0. A ``*`` and a blank become NA.
        """
        for dtype in ("string", "object"):
            frame = pd.DataFrame({
                "total_spending_all_funds": pd.Series(
                    ["20,164", "0", "*", ""], dtype=dtype
                ),
                "personnel_spending_federal": pd.Series(
                    ["287", "0", "100", ""], dtype=dtype
                ),
            })
            cleaned = clean_spending_values(frame)
            assert cleaned.loc[0, "total_spending_all_funds"] == 20164
            assert cleaned.loc[1, "total_spending_all_funds"] == 0
            assert pd.isna(cleaned.loc[2, "total_spending_all_funds"])
            assert pd.isna(cleaned.loc[3, "total_spending_all_funds"])
            assert cleaned.loc[0, "personnel_spending_federal"] == 287
            assert cleaned.loc[1, "personnel_spending_federal"] == 0

    def test_comma_formatted_fixture(self):
        """A comma-formatted KYRC row keeps published dollars, stars, blanks, and zeros.

        Henry Clay, Raceland-Worthington, Hopkins County Career and
        Technology Center, and UK Early Childhood Lab are copied
        byte-for-byte from ``KYRC24_FT_Spending_per_Student.csv``.
        Henry Clay's total is the quoted ``20,164``. Raceland's
        non-personnel federal cell is the published 0. Hopkins publishes
        0 in every spending column. UK Early Childhood Lab's spending
        cells are blank and are not stored.

        No registered spending file has a ``*``. That case is a labelled
        synthetic row, school code ``SYNSTAR``. A ``*`` is value NA with
        suppressed Y. The other cells on that row stay as written. Keys
        are unique.
        """
        fixture = Path("tests/fixtures/spending_per_student/comma_formatted_kyrc.csv")
        fixture_lines = fixture.read_text(encoding="utf-8").splitlines()
        verbatim_rows = {
            "Henry Clay High School": (
                '20232024,034,FAYETTE,165,Fayette County,039,Henry Clay High School,'
                '165039,034165039,210186000364,CKEC,901,A1,"14,081","2,972",287,955,'
                '"17,053","1,242","20,164"'
            ),
            "Raceland-Worthington Middle School": (
                "20232024,045,GREENUP,502,Raceland-Worthington Independent,030,"
                "Raceland-Worthington Middle School,502030,045502030,210498001241,"
                'KEDC,903,A1,"7,604","2,768",428,0,"10,357",381,"15,894"'
            ),
            "Hopkins Co Career & Technology Center": (
                "20232024,054,HOPKINS,265,Hopkins County,105,"
                "Hopkins Co Career & Technology Center,265105,054265105,"
                "210286002425,WKEC,908,A2,0,0,0,0,0,0,0"
            ),
            "UK Early Childhood Lab": (
                "20232024,034,FAYETTE,165,Fayette County,009,UK Early Childhood Lab,"
                "165009,034165009,210186002385,CKEC,901,A4,,,,,,,"
            ),
        }
        source = Path("data/raw/spending_per_student/KYRC24_FT_Spending_per_Student.csv")
        source_lines = (
            source.read_text(encoding="utf-8-sig").splitlines() if source.exists() else []
        )
        for label, source_row in verbatim_rows.items():
            fixture_row = next(line for line in fixture_lines if f",{label}," in line)
            assert fixture_row == source_row
            if source_lines:
                assert source_row in source_lines

        shutil.copy(fixture, self.sample_dir / fixture.name)

        transform(self.raw_dir, self.proc_dir, {"derive": {"processing_date": "2025-11-24"}})
        df = pd.read_csv(
            self.proc_dir / "spending_per_student.csv",
            dtype={
                "school_id": str,
                "school_code": str,
                "district_number": str,
                "county_number": str,
                "co_op_code": str,
            },
        )

        assert not df.duplicated(["year", "school_id", "metric", "student_group"]).any()
        assert (df["student_group"] == "All Students").all()
        assert set(df["year"].unique()) == {2024}
        assert not (df["school_id"].astype(str) == "165009").any()

        def cell(school_id: str, metric: str) -> pd.Series:
            match = df[(df["school_id"].astype(str) == school_id) & (df["metric"] == metric)]
            assert len(match) == 1, f"{school_id} {metric} rows={len(match)}"
            return match.iloc[0]

        henry = cell("165039", "total_spending_per_student_all_funds")
        assert henry["value"] == 20164
        assert henry["suppressed"] == "N"
        assert henry["district"] == "Fayette County"
        assert henry["school_name"] == "Henry Clay High School"
        assert henry["county_number"] == "034"
        assert henry["co_op"] == "CKEC"
        assert henry["co_op_code"] == "901"
        assert cell("165039", "personnel_spending_per_student_state_local")["value"] == 14081
        assert cell("165039", "non_personnel_spending_per_student_state_local")["value"] == 2972
        assert cell("165039", "personnel_spending_per_student_federal")["value"] == 287
        assert cell("165039", "non_personnel_spending_per_student_federal")["value"] == 955
        assert cell("165039", "total_spending_per_student_state_local")["value"] == 17053
        assert cell("165039", "total_spending_per_student_federal")["value"] == 1242

        raceland_zero = cell("502030", "non_personnel_spending_per_student_federal")
        assert raceland_zero["value"] == 0
        assert raceland_zero["suppressed"] == "N"
        assert raceland_zero["district"] == "Raceland-Worthington Independent"
        assert cell("502030", "total_spending_per_student_all_funds")["value"] == 15894
        assert cell("502030", "personnel_spending_per_student_state_local")["value"] == 7604

        for metric in (
            "personnel_spending_per_student_federal",
            "non_personnel_spending_per_student_federal",
            "total_spending_per_student_federal",
            "personnel_spending_per_student_state_local",
            "non_personnel_spending_per_student_state_local",
            "total_spending_per_student_state_local",
            "total_spending_per_student_all_funds",
        ):
            zero_row = cell("265105", metric)
            assert zero_row["district"] == "Hopkins County"
            assert zero_row["school_name"] == "Hopkins Co Career & Technology Center"
            assert zero_row["value"] == 0
            assert zero_row["suppressed"] == "N"

        starred = cell("SYNSTAR", "personnel_spending_per_student_state_local")
        assert starred["district"] == "Synthetic Star"
        assert starred["suppressed"] == "Y"
        assert pd.isna(starred["value"])
        assert cell("SYNSTAR", "non_personnel_spending_per_student_state_local")["value"] == 0
        assert cell("SYNSTAR", "non_personnel_spending_per_student_state_local")["suppressed"] == "N"
        blank_federal = df[
            (df["school_id"].astype(str) == "SYNSTAR")
            & (df["metric"] == "non_personnel_spending_per_student_federal")
        ]
        assert len(blank_federal) == 0

    def test_level_values_on_string_and_object(self):
        """A comma in the District column survives object and StringDtype.

        The School column on a district row is the published 0. That 0 is
        not the district amount. The state row's District column is 0.
        The state amount is the State column.
        """
        frame = pd.DataFrame({
            "School Year": ["20232024", "20232024", "20232024"],
            "District Number": ["999", "165", "165"],
            "District Name": ["All Districts", "Fayette County", "Fayette County"],
            "School Name": ["All Schools", "All Schools", "Arlington Elementary School"],
            "School Code": ["999000", "165000", "165005"],
            "Metric": [
                "Total Expenditures Per Student",
                "Total Expenditures Per Student",
                "Total Expenditures Per Student",
            ],
            "School": ["0", "0", ""],
            "District": ["0", "22,322", "22,322"],
            "State": ["19,020", "19,020", "19,020"],
        })
        for dtype in ("string", "object"):
            selected = select_district_state_spending(frame.astype(dtype))
            assert set(selected["School Code"].astype(str)) == {"165", "999"}
            fayette = selected[selected["School Code"].astype(str) == "165"].iloc[0]
            assert fayette["District Name"] == "Fayette County"
            assert fayette["School Name"] == "---District Total---"
            assert fayette["Total Expenditures Per Student"] == "22,322"
            state = selected[selected["School Code"].astype(str) == "999"].iloc[0]
            assert state["District Name"] == "State"
            assert state["School Name"] == "---State Total---"
            assert state["Total Expenditures Per Student"] == "19,020"
            normalized = self.etl.normalize_column_names(selected)
            cleaned = self.etl.standardize_missing_values(normalized)
            fayette_clean = cleaned[cleaned["school_code"].astype(str) == "165"].iloc[0]
            state_clean = cleaned[cleaned["school_code"].astype(str) == "999"].iloc[0]
            assert fayette_clean["total_spending_all_funds"] == 22322
            assert state_clean["total_spending_all_funds"] == 19020

    def test_district_state_fixture_lines_match_source(self):
        """Real district and state fixture lines are copied verbatim from source.

        Synthetic star and published-0 lines are not in the source files.
        """
        fixture_dir = Path("tests/fixtures/spending_per_student/district_state")
        raw_dir = Path("data/raw/spending_per_student")
        if not raw_dir.exists():
            pytest.skip(
                "data/raw/spending_per_student is absent, so the real fixture "
                "lines were not compared to the source files"
            )
        for fixture in sorted(fixture_dir.glob("*.csv")):
            source = raw_dir / fixture.name
            source_lines = set(source.read_text(encoding="utf-8-sig").splitlines())
            for line in fixture.read_text(encoding="utf-8").splitlines()[1:]:
                if not line.strip():
                    continue
                if "Synthetic" in line:
                    assert line not in source_lines
                else:
                    assert line in source_lines

    def test_district_and_state_rows_2024_2025(self):
        """Long KYRC24/KYRC25 files add district and state rows.

        Fayette all-funds is the published 22,322 in 2024 and 22,974 in
        2025. The state is 19,020 and 19,973. Labels match the 2021-2023
        spending rows: school 165, district Fayette County, school name
        ---District Total---, and state 999 / State / ---State Total---.
        A school row and a blank district cell are not stored. A synthetic
        star is NA with suppressed Y. A synthetic published 0 is 0 with
        suppressed N. The all-funds figure is the published cell.
        """
        fixture_dir = Path("tests/fixtures/spending_per_student/district_state")
        for fixture in fixture_dir.glob("*.csv"):
            shutil.copy(fixture, self.sample_dir / fixture.name)

        transform(self.raw_dir, self.proc_dir, {"derive": {"processing_date": "2025-11-24"}})
        df = pd.read_csv(
            self.proc_dir / "spending_per_student.csv",
            dtype={
                "school_id": str,
                "school_code": str,
                "district_number": str,
                "county_number": str,
                "co_op_code": str,
                "year": str,
            },
        )
        assert not df.duplicated(["year", "school_id", "metric", "student_group"]).any()
        assert (df["student_group"] == "All Students").all()
        assert set(df["school_id"]) & {"165000", "999000", "165005", "602", "602000"} == set()

        expected = {
            ("2024", "165"): {
                "personnel_spending_per_student_federal": 666,
                "non_personnel_spending_per_student_federal": 989,
                "total_spending_per_student_federal": 1655,
                "personnel_spending_per_student_state_local": 14881,
                "non_personnel_spending_per_student_state_local": 2907,
                "total_spending_per_student_state_local": 17788,
                "total_spending_per_student_all_funds": 22322,
            },
            ("2024", "999"): {
                "personnel_spending_per_student_federal": 1153,
                "non_personnel_spending_per_student_federal": 795,
                "total_spending_per_student_federal": 1948,
                "personnel_spending_per_student_state_local": 10930,
                "non_personnel_spending_per_student_state_local": 2612,
                "total_spending_per_student_state_local": 13542,
                "total_spending_per_student_all_funds": 19020,
            },
            ("2025", "165"): {
                "personnel_spending_per_student_federal": 623,
                "non_personnel_spending_per_student_federal": 290,
                "total_spending_per_student_federal": 913,
                "personnel_spending_per_student_state_local": 15550,
                "non_personnel_spending_per_student_state_local": 2652,
                "total_spending_per_student_state_local": 18202,
                "total_spending_per_student_all_funds": 22974,
            },
            ("2025", "999"): {
                "personnel_spending_per_student_federal": 935,
                "non_personnel_spending_per_student_federal": 480,
                "total_spending_per_student_federal": 1415,
                "personnel_spending_per_student_state_local": 11787,
                "non_personnel_spending_per_student_state_local": 2843,
                "total_spending_per_student_state_local": 14630,
                "total_spending_per_student_all_funds": 19973,
            },
        }
        sources = {
            "personnel_spending_per_student_federal": "FT_Personnel_Spending_per_Student_Federal_Funds.csv",
            "non_personnel_spending_per_student_federal": "FT_Personnel_Spending_per_Student_Federal_Funds.csv",
            "total_spending_per_student_federal": "FT_Total_Spending_per_Student_Federal_Funds.csv",
            "personnel_spending_per_student_state_local": "FT_Personnel_Spending_per_Student_State_Local_Funds.csv",
            "non_personnel_spending_per_student_state_local": "FT_Personnel_Spending_per_Student_State_Local_Funds.csv",
            "total_spending_per_student_state_local": "FT_Total_Spending_per_Student_State_Local_Funds.csv",
            "total_spending_per_student_all_funds": "FT_Total_Spending_per_Student_All_Fund_Sources.csv",
        }

        def cell(year: str, school_id: str, metric: str) -> pd.Series:
            match = df[
                (df["year"] == year)
                & (df["school_id"] == school_id)
                & (df["metric"] == metric)
            ]
            assert len(match) == 1, f"{year} {school_id} {metric} rows={len(match)}"
            return match.iloc[0]

        for (year, school_id), metrics in expected.items():
            for metric, amount in metrics.items():
                row = cell(year, school_id, metric)
                assert row["value"] == amount
                assert row["suppressed"] == "N"
                assert row["district_number"] == school_id
                assert row["school_code"] == school_id
                assert row["source_file"].endswith(sources[metric])
                release = "KYRC24" if year == "2024" else "KYRC25"
                assert row["source_file"].startswith(release)
            if school_id == "165":
                sample = cell(year, school_id, "total_spending_per_student_all_funds")
                assert sample["district"] == "Fayette County"
                assert sample["school_name"] == "---District Total---"
                assert sample["county_number"] == "034"
                assert sample["county_name"] == "FAYETTE"
                assert sample["co_op"] == "CKEC"
                assert sample["co_op_code"] == "901"
            else:
                sample = cell(year, school_id, "total_spending_per_student_all_funds")
                assert sample["district"] == "State"
                assert sample["school_name"] == "---State Total---"
                assert pd.isna(sample["county_number"])
                assert pd.isna(sample["co_op"])

        # 1,655 + 17,788 is 19,443. KDE published 22,322. Store 22,322.
        assert cell("2024", "165", "total_spending_per_student_federal")["value"] == 1655
        assert cell("2024", "165", "total_spending_per_student_state_local")["value"] == 17788
        assert cell("2024", "165", "total_spending_per_student_all_funds")["value"] == 22322

        starred = cell("2024", "998", "total_spending_per_student_all_funds")
        assert starred["district"] == "Synthetic Star"
        assert starred["school_name"] == "---District Total---"
        assert starred["suppressed"] == "Y"
        assert pd.isna(starred["value"])
        zero = cell("2024", "997", "total_spending_per_student_all_funds")
        assert zero["district"] == "Synthetic Zero"
        assert zero["school_name"] == "---District Total---"
        assert zero["value"] == 0
        assert zero["suppressed"] == "N"
        assert not (df["school_id"] == "602").any()
        assert not (df["school_id"] == "602000").any()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
