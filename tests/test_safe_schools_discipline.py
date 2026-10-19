"""
Tests for Safe Schools Discipline ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
from etl.constants import KPI_COLUMNS
import tempfile
import shutil
from etl.safe_schools_discipline import transform, SafeSchoolsDisciplineETL


class TestSafeSchoolsDisciplineETL:
    
    def setup_method(self):
        """Setup test directories and sample data."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        
        # Create sample raw data directory
        self.sample_dir = self.raw_dir / "safe_schools_discipline"
        self.sample_dir.mkdir(parents=True)
    
    def teardown_method(self):
        """Clean up test directories."""
        shutil.rmtree(self.test_dir)
    
    def create_sample_kyrc24_discipline_data(self):
        """Create sample KYRC24 discipline resolutions data."""
        data = pd.DataFrame({
            'School Year': ['20232024', '20232024', '20232024'],
            'County Number': ['001', '001', '001'],
            'County Name': ['Adair', 'Adair', 'Adair'],
            'District Number': ['001', '001', '001'],
            'District Name': ['Adair County', 'Adair County', 'Adair County'],
            'School Number': ['001', '001', '001'],
            'School Name': ['Test Elementary', 'Test Elementary', 'Test Elementary'],
            'School Code': ['001001', '001001', '001001'],
            'State School Id': ['12345', '12345', '12345'],
            'NCES ID': ['210001000001', '210001000001', '210001000001'],
            'Demographic': ['All Students', 'Female', 'Male'],
            'Total': [100, 45, 55],
            'Corporal Punishment (SSP5)': [0, 0, 0],
            'Restraint (SSP7)': [2, 1, 1],
            'Seclusion (SSP8)': [1, 0, 1],
            'Expelled, Not Receiving Services (SSP2)': [0, 0, 0],
            'Expelled, Receiving Services (SSP1)': [1, 0, 1],
            'In-School Removal (INSR) Or In-District Removal (INDR) >=.5': [45, 20, 25],
            'Out-Of-School Suspensions (SSP3)': [30, 12, 18],
            'Removal By Hearing Officer (IAES2)': [0, 0, 0],
            'Unilateral Removal By School Personnel (IAES1)': [1, 0, 1]
        })
        return data
        
    def create_sample_kyrc24_legal_data(self):
        """Create sample KYRC24 legal sanctions data."""
        data = pd.DataFrame({
            'School Year': ['20232024', '20232024', '20232024'],
            'County Number': ['001', '001', '001'],
            'County Name': ['Adair', 'Adair', 'Adair'],
            'District Number': ['001', '001', '001'],
            'District Name': ['Adair County', 'Adair County', 'Adair County'],
            'School Number': ['001', '001', '001'],
            'School Name': ['Test Elementary', 'Test Elementary', 'Test Elementary'],
            'School Code': ['001001', '001001', '001001'],
            'Demographic': ['All Students', 'Female', 'Male'],
            'Total': [20, 8, 12],
            'Arrests': [2, 1, 1],
            'Charges': [5, 2, 3],
            'Civil Proceedings': [0, 0, 0],
            'Court Designated Worker Involvement': [1, 0, 1],
            'School Resource Officer Involvement': [15, 6, 9]
        })
        return data
        
    def create_sample_historical_data(self):
        """Create sample historical discipline data."""
        data = pd.DataFrame({
            'SCHOOL YEAR': ['20222023', '20222023', '20222023'],
            'COUNTY NUMBER': ['001', '001', '001'],
            'COUNTY NAME': ['ADAIR', 'ADAIR', 'ADAIR'],
            'DISTRICT NUMBER': ['001', '001', '001'],
            'DISTRICT NAME': ['Adair County', 'Adair County', 'Adair County'],
            'SCHOOL NUMBER': ['001', '001', '001'],
            'SCHOOL NAME': ['Test Elementary', 'Test Elementary', 'Test Elementary'],
            'SCHOOL CODE': ['001001', '001001', '001001'],
            'DEMOGRAPHIC': ['All Students', 'Female', 'Male'],
            'TOTAL DISCIPLINE RESOLUTIONS': [85, 38, 47],
            'EXPELLED RECEIVING SERVICES SSP1': [1, 0, 1],
            'EXPELLED NOT RECEIVING SERVICES SSP2': [0, 0, 0],
            'OUT OF SCHOOL SUSPENSION SSP3': [25, 10, 15],
            'CORPORAL PUNISHMENT SSP5': [0, 0, 0],
            'IN-SCHOOL REMOVAL INSR': [40, 18, 22],
            'RESTRAINT SSP7': [1, 1, 0],
            'SECLUSION SSP8': [0, 0, 0],
            'UNILATERAL REMOVAL BY SCHOOL PERSONNEL IAES1': [1, 0, 1],
            'REMOVAL BY HEARING OFFICER IAES2': [0, 0, 0]
        })
        return data
    
    def test_etl_initialization(self):
        """Test ETL class initialization."""
        etl = SafeSchoolsDisciplineETL()
        assert etl.source_name == "safe_schools_discipline"
        assert hasattr(etl, 'module_column_mappings')
        assert hasattr(etl, 'extract_metrics')
        assert hasattr(etl, 'get_suppressed_metric_defaults')
    
    def test_module_column_mappings(self):
        """Test module column mappings."""
        etl = SafeSchoolsDisciplineETL()
        mappings = etl.module_column_mappings
        
        # Test KYRC24 mappings
        assert 'Corporal Punishment (SSP5)' in mappings
        assert mappings['Corporal Punishment (SSP5)'] == 'corporal_punishment_count'
        assert 'Arrests' in mappings
        assert mappings['Arrests'] == 'arrest_count'
        
        # Test historical mappings
        assert 'EXPELLED RECEIVING SERVICES SSP1' in mappings
        assert mappings['EXPELLED RECEIVING SERVICES SSP1'] == 'expelled_receiving_services_count'
    
    def test_extract_metrics_kyrc24_discipline(self):
        """Test metric extraction from KYRC24 discipline data."""
        etl = SafeSchoolsDisciplineETL()
        
        # Test row with discipline counts
        row = pd.Series({
            'total': 100,
            'restraint_count': 2,
            'out_of_school_suspension_count': 30,
            'in_school_removal_count': 45,
            'expelled_receiving_services_count': 1,
            'unilateral_removal_count': 1
        })
        
        metrics = etl.extract_metrics(row)
        
        # These columns are counts. A rate is not derived.
        assert 'restraint_count' in metrics
        assert metrics['restraint_count'] == 2
        assert 'out_of_school_suspension_count' in metrics
        assert metrics['out_of_school_suspension_count'] == 30
        assert 'in_school_removal_count' in metrics
        assert metrics['in_school_removal_count'] == 45
        assert 'expelled_receiving_services_count' in metrics
        assert metrics['expelled_receiving_services_count'] == 1
        assert 'unilateral_removal_count' in metrics
        assert metrics['unilateral_removal_count'] == 1
        assert 'restraint_rate' not in metrics
        assert 'total' not in metrics
        
    def test_extract_metrics_kyrc24_legal(self):
        """Test metric extraction from KYRC24 legal sanctions data."""
        etl = SafeSchoolsDisciplineETL()
        
        # Test row with legal sanctions counts
        row = pd.Series({
            'total': 20,
            'arrest_count': 2,
            'charges_count': 5,
            'court_designated_worker_count': 1,
            'school_resource_officer_count': 15
        })
        
        metrics = etl.extract_metrics(row)
        
        assert 'arrest_count' in metrics
        assert metrics['arrest_count'] == 2
        assert 'charges_count' in metrics
        assert metrics['charges_count'] == 5
        assert 'court_designated_worker_count' in metrics
        assert metrics['court_designated_worker_count'] == 1
        assert 'school_resource_officer_count' in metrics
        assert metrics['school_resource_officer_count'] == 15
        assert 'arrest_rate' not in metrics
        assert 'total' not in metrics
    
    def test_extract_metrics_historical(self):
        """Test metric extraction from historical data."""
        etl = SafeSchoolsDisciplineETL()
        
        # Test row with historical discipline counts
        row = pd.Series({
            'total_discipline_resolutions': 85,
            'expelled_receiving_services_count': 1,
            'out_of_school_suspension_count': 25,
            'in_school_removal_count': 40,
            'restraint_count': 1,
            'unilateral_removal_count': 1
        })
        
        metrics = etl.extract_metrics(row)
        
        assert metrics['expelled_receiving_services_count'] == 1
        assert metrics['out_of_school_suspension_count'] == 25
        assert metrics['in_school_removal_count'] == 40
        assert metrics['restraint_count'] == 1
        assert metrics['unilateral_removal_count'] == 1
        assert 'expelled_receiving_services_rate' not in metrics
        assert 'total_discipline_resolutions' not in metrics
    
    def test_extract_metrics_zero_total(self):
        """A zero in the unused Total column does not drop published counts."""
        etl = SafeSchoolsDisciplineETL()
        
        row = pd.Series({
            'total': 0,
            'restraint_count': 1,
            'out_of_school_suspension_count': 2
        })
        
        metrics = etl.extract_metrics(row)
        assert metrics['restraint_count'] == 1
        assert metrics['out_of_school_suspension_count'] == 2
        assert 'total' not in metrics
        assert sum(metrics.values()) != 0
    
    def test_extract_metrics_zero_counts(self):
        """A published 0 is kept. Issue #52."""
        etl = SafeSchoolsDisciplineETL()
        
        row = pd.Series({
            'total': 100,
            'restraint_count': 0,
            'out_of_school_suspension_count': 10,
            'in_school_removal_count': 0
        })
        
        metrics = etl.extract_metrics(row)
        
        assert metrics['restraint_count'] == 0
        assert metrics['in_school_removal_count'] == 0
        assert metrics['out_of_school_suspension_count'] == 10
        assert 'restraint_rate' not in metrics

    def test_extract_metrics_negative_count(self):
        """A negative count is dropped. A published count on the same row is kept."""
        etl = SafeSchoolsDisciplineETL()
        row = pd.Series({
            'out_of_school_suspension_count': -1,
            'restraint_count': 4,
        })
        metrics = etl.extract_metrics(row)
        assert 'out_of_school_suspension_count' not in metrics
        assert metrics['restraint_count'] == 4
    
    def test_get_suppressed_metric_defaults(self):
        """Defaults cover the count columns present on the row, not rate names."""
        etl = SafeSchoolsDisciplineETL()
        row = pd.Series({
            'restraint_count': pd.NA,
            'out_of_school_suspension_count': pd.NA,
        })
        defaults = etl.get_suppressed_metric_defaults(row)
        
        assert 'restraint_count' in defaults
        assert 'out_of_school_suspension_count' in defaults
        assert pd.isna(defaults['restraint_count'])
        assert pd.isna(defaults['out_of_school_suspension_count'])
        assert 'arrest_count' not in defaults
        assert 'restraint_rate' not in defaults
    
    def test_transform_integration(self):
        """Test the complete transform integration."""
        # Create sample data files
        kyrc24_discipline = self.create_sample_kyrc24_discipline_data()
        kyrc24_legal = self.create_sample_kyrc24_legal_data()
        historical = self.create_sample_historical_data()
        
        # Save sample data to test directories
        kyrc24_discipline.to_csv(self.sample_dir / "KYRC24_SAFE_Discipline_Resolutions.csv", index=False)
        kyrc24_legal.to_csv(self.sample_dir / "KYRC24_SAFE_Legal_Sanctions.csv", index=False)
        historical.to_csv(self.sample_dir / "safe_schools_discipline_2022.csv", index=False)
        
        # Run transform
        test_config = {
            "derive": {
                "processing_date": "2025-07-20",
                "data_quality_flag": "test"
            }
        }
        
        transform(self.raw_dir, self.proc_dir, test_config)
        
        # Check output file exists
        output_file = self.proc_dir / "safe_schools_discipline.csv"
        assert output_file.exists()
        
        # Load and validate output
        result_df = pd.read_csv(output_file)
        
        # Check required columns
        required_columns = KPI_COLUMNS
        for col in required_columns:
            assert col in result_df.columns
        
        # Check we have data from all sources
        source_files = result_df['source_file'].unique()
        assert 'KYRC24_SAFE_Discipline_Resolutions.csv' in source_files
        assert 'KYRC24_SAFE_Legal_Sanctions.csv' in source_files
        assert 'safe_schools_discipline_2022.csv' in source_files
        
        # Check metric types. These files publish counts, including zeros.
        metrics = result_df['metric'].unique()
        assert 'restraint_count' in metrics
        assert 'out_of_school_suspension_count' in metrics
        assert 'arrest_count' in metrics
        assert 'school_resource_officer_count' in metrics
        assert 'restraint_rate' not in metrics
        zeros = result_df[result_df['metric'] == 'corporal_punishment_count']
        assert len(zeros) > 0
        assert (zeros['value'] == 0).all()
        assert (zeros['suppressed'] == 'N').all()
        
        # Check years
        years = result_df['year'].unique()
        assert 2024 in years  # From KYRC24 data
        assert 2023 in years  # From historical data (school year 20222023)
        
        # Counts are non-negative. They are not capped at 100.
        assert result_df['value'].min() >= 0
        
        # Check no null values in required fields
        assert result_df['district'].notna().all()
        assert result_df['school_id'].notna().all()
        assert result_df['metric'].notna().all()
        assert result_df['value'].notna().all()


class TestSafeSchoolsDisciplineHelpers:
    """Test helper functions if any are created."""
    
    def test_safe_schools_discipline_transform_function(self):
        """Test the transform function can be imported and called."""
        # This is a basic smoke test
        assert callable(transform)


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "safe_schools_discipline"
ROOT = Path(__file__).resolve().parents[1]


def _copy_fixture(dest_dir: Path, fixture_name: str, source_name: str) -> None:
    shutil.copy(FIXTURE_DIR / fixture_name, dest_dir / source_name)


def _run(raw_dir: Path, proc_dir: Path) -> pd.DataFrame:
    transform(raw_dir.parent, proc_dir, {"derive": {}})
    return pd.read_csv(proc_dir / "safe_schools_discipline.csv", dtype=str, keep_default_na=False)


def _num(value: str) -> float:
    """Parse a KPI value written from a float. An empty cell is not a number."""
    assert value != ""
    return float(value)


def _cell(df: pd.DataFrame, year: str, school_id: str, group: str, metric: str) -> pd.Series:
    match = df[
        (df["year"] == year)
        & (df["school_id"] == school_id)
        & (df["student_group"] == group)
        & (df["metric"] == metric)
    ]
    assert len(match) == 1, (year, school_id, group, metric, len(match))
    return match.iloc[0]


class TestSafeSchoolsDisciplineKyrc26:
    """Real rows from the downloaded files, plus the issue #52 rules."""

    def test_include_kyrc26_accepts_the_file(self):
        etl = SafeSchoolsDisciplineETL()
        assert etl.include_kyrc26 is True
        assert etl.accepts_data_file(Path("KYRC26_SAFE_Discipline_Resolutions.csv"))
        assert etl.accepts_data_file(Path("KYRC26_SAFE_Legal_Sanctions.csv"))

    def test_kyrc_header_aliases(self):
        mappings = SafeSchoolsDisciplineETL().module_column_mappings
        assert mappings["Out-of-School Suspensions (SSP3)"] == "out_of_school_suspension_count"
        assert mappings["Out-Of-School Suspensions (SSP3)"] == "out_of_school_suspension_count"
        assert mappings["Removal by Hearing Officer (IAES2)"] == "removal_by_hearing_officer_count"
        assert mappings["Unilateral Removal by School Personnel (IAES1)"] == "unilateral_removal_count"
        assert mappings["In-School Removal (INSR) or In-District Removal (INDR) >=.5"] == "in_school_removal_count"
        assert mappings["Expelled, Not Receiving Services (SSP2)"] == "expelled_not_receiving_services_count"
        assert mappings["Expelled, Not Receiving Services (Ssp2)"] == "expelled_not_receiving_services_count"

    def test_kyrc23_and_2018_workbook_are_skipped(self):
        etl = SafeSchoolsDisciplineETL()
        assert etl.accepts_data_file(Path("KYRC23_SAFE_Discipline_Resolutions.csv")) is False
        assert etl.accepts_data_file(Path("KYRC23_SAFE_Legal_Sanctions.csv")) is False
        assert etl.accepts_data_file(Path("SAFE_SCHOOLS_18-19.xlsx")) is False
        # The annual file and the permanent KYRC25 name are still accepted.
        assert etl.accepts_data_file(Path("safe_schools_discipline_2023.csv")) is True
        assert etl.accepts_data_file(Path("KYRC25_SAFE_Discipline_Resolutions.csv")) is True

    def test_stringdtype_comma_zero_and_star(self):
        """pandas 3 StringDtype keeps a comma count, a published 0, and a star."""
        etl = SafeSchoolsDisciplineETL()
        df = pd.DataFrame({
            "demographic": ["All Students"],
            "school_code": ["999000"],
            "district_name": ["All Districts"],
            "school_name": ["All Schools"],
            "school_year": ["20252026"],
            "out_of_school_suspension_count": pd.Series(["35,818"], dtype="string"),
            "corporal_punishment_count": pd.Series(["0"], dtype="string"),
            "removal_by_hearing_officer_count": pd.Series(["*"], dtype="string"),
        })
        cleaned = etl.standardize_missing_values(df)
        kpi = etl.convert_to_kpi_format(cleaned, "KYRC26_SAFE_Discipline_Resolutions.csv")
        oss = kpi[kpi["metric"] == "out_of_school_suspension_count"].iloc[0]
        assert oss["value"] == 35818
        assert oss["suppressed"] == "N"
        zero = kpi[kpi["metric"] == "corporal_punishment_count"].iloc[0]
        assert zero["value"] == 0
        assert zero["suppressed"] == "N"
        star = kpi[kpi["metric"] == "removal_by_hearing_officer_count"].iloc[0]
        assert pd.isna(star["value"])
        assert star["suppressed"] == "Y"

    def test_blank_cell_is_skipped_and_total_events_is_skipped(self, tmp_path):
        """A blank is not a star and is not stored. An all-blank row is skipped."""
        raw = tmp_path / "raw" / "safe_schools_discipline"
        raw.mkdir(parents=True)
        proc = tmp_path / "processed"
        proc.mkdir()
        pd.DataFrame({
            "School Year": ["20252026", "20252026", "20252026"],
            "District Name": ["Adair County", "Adair County", "Adair County"],
            "School Name": ["Adair County High School"] * 3,
            "School Code": ["001010", "001010", "001010"],
            "Demographic": ["All Students", "Migrant", "Total Events"],
            "Out-of-School Suspensions (SSP3)": ["10", "", "99"],
            "Corporal Punishment (SSP5)": ["0", "", "1"],
            "Restraint (SSP7)": ["", "", "2"],
        }).to_csv(raw / "KYRC26_SAFE_Discipline_Resolutions.csv", index=False)
        df = _run(raw, proc)
        kept = df[df["student_group"] == "All Students"]
        assert set(kept["metric"]) == {
            "out_of_school_suspension_count",
            "corporal_punishment_count",
        }
        assert _num(_cell(df, "2026", "001010", "All Students", "out_of_school_suspension_count")["value"]) == 10
        assert _num(_cell(df, "2026", "001010", "All Students", "corporal_punishment_count")["value"]) == 0
        assert "Migrant" not in set(df["student_group"])
        assert "Total Events" not in set(df["student_group"])
        assert "99" not in set(df["value"].astype(str))
        # Total Events is a published row. It is not stored, and Female
        # plus Male is not written in its place.
        assert not df["metric"].str.endswith("_rate").any()
        assert "total" not in set(df["metric"])

    def test_real_rows_and_prior_year_anchors(self, tmp_path):
        """State, district, school, a suppressed group, and prior-year anchors."""
        raw = tmp_path / "raw" / "safe_schools_discipline"
        raw.mkdir(parents=True)
        proc = tmp_path / "processed"
        proc.mkdir()
        _copy_fixture(raw, "kyrc26_discipline_rows.csv", "KYRC26_SAFE_Discipline_Resolutions.csv")
        _copy_fixture(raw, "kyrc26_legal_rows.csv", "KYRC26_SAFE_Legal_Sanctions.csv")
        _copy_fixture(raw, "kyrc25_discipline_state.csv", "KYRC25_SAFE_Discipline_Resolutions.csv")
        _copy_fixture(raw, "kyrc24_discipline_state.csv", "KYRC24_SAFE_Discipline_Resolutions.csv")
        _copy_fixture(raw, "historical_2023_state.csv", "safe_schools_discipline_2023.csv")
        state_2020 = pd.read_csv(FIXTURE_DIR / "historical_2020_state.csv", dtype=str)
        all_star = pd.read_csv(FIXTURE_DIR / "historical_2020_all_star.csv", dtype=str)
        pd.concat([state_2020, all_star], ignore_index=True).to_csv(
            raw / "safe_schools_discipline_2020.csv", index=False
        )

        df = _run(raw, proc)
        keys = ["year", "school_id", "student_group", "metric"]
        assert not df.duplicated(keys).any()

        # 2026 state. Counts, including the published zeros. Year is the
        # report-card year. District is All Districts.
        oss = _cell(df, "2026", "999000", "All Students", "out_of_school_suspension_count")
        assert _num(oss["value"]) == 35818
        assert oss["suppressed"] == "N"
        assert oss["district"] == "All Districts"
        assert oss["school_name"] == "---District Total---"
        assert _num(_cell(df, "2026", "999000", "All Students", "in_school_removal_count")["value"]) == 46101
        assert _num(_cell(df, "2026", "999000", "All Students", "corporal_punishment_count")["value"]) == 0
        assert _num(_cell(df, "2026", "999000", "All Students", "removal_by_hearing_officer_count")["value"]) == 0
        assert _num(_cell(df, "2026", "999000", "All Students", "expelled_not_receiving_services_count")["value"]) == 13

        # Fayette district and Bryan Station. Restraint at Bryan Station is *.
        fayette = _cell(df, "2026", "165000", "All Students", "out_of_school_suspension_count")
        assert _num(fayette["value"]) == 2060
        assert fayette["district"] == "Fayette County"
        assert fayette["co_op"] == "CKEC"
        assert fayette["co_op_code"] == "901"
        bryan = _cell(df, "2026", "165170", "All Students", "out_of_school_suspension_count")
        assert _num(bryan["value"]) == 190
        assert bryan["school_name"] == "Bryan Station High School"
        restraint = _cell(df, "2026", "165170", "All Students", "restraint_count")
        assert restraint["value"] == ""
        assert restraint["suppressed"] == "Y"
        assert _num(_cell(df, "2026", "165170", "All Students", "corporal_punishment_count")["value"]) == 0

        # Adair County High, English Learner: OSS and in-school removal are *.
        el = _cell(df, "2026", "001010", "English Learner", "out_of_school_suspension_count")
        assert el["value"] == ""
        assert el["suppressed"] == "Y"
        assert _cell(df, "2026", "001010", "English Learner", "in_school_removal_count")["suppressed"] == "Y"
        assert _num(_cell(df, "2026", "001010", "English Learner", "restraint_count")["value"]) == 0

        # Legal sanctions, including a published 0 and a starred cell.
        arrests = _cell(df, "2026", "999000", "All Students", "arrest_count")
        assert _num(arrests["value"]) == 95
        assert _num(_cell(df, "2026", "999000", "All Students", "charges_count")["value"]) == 1159
        assert _num(_cell(df, "2026", "165170", "All Students", "arrest_count")["value"]) == 0
        cdw = _cell(df, "2026", "165000", "All Students", "court_designated_worker_count")
        assert cdw["value"] == ""
        assert cdw["suppressed"] == "Y"

        # Prior years. Published numbers stay. A star is suppressed Y.
        assert _num(_cell(df, "2025", "999000", "All Students", "out_of_school_suspension_count")["value"]) == 35886
        assert _num(_cell(df, "2025", "999000", "All Students", "expelled_not_receiving_services_count")["value"]) == 10
        assert _num(_cell(df, "2025", "999000", "All Students", "corporal_punishment_count")["value"]) == 0
        assert _num(_cell(df, "2024", "999000", "All Students", "out_of_school_suspension_count")["value"]) == 38983
        iaes = _cell(df, "2024", "999000", "All Students", "removal_by_hearing_officer_count")
        assert iaes["value"] == ""
        assert iaes["suppressed"] == "Y"
        assert _num(_cell(df, "2024", "999000", "All Students", "corporal_punishment_count")["value"]) == 0
        state_2023 = _cell(df, "2023", "999", "All Students", "out_of_school_suspension_count")
        assert _num(state_2023["value"]) == 40768
        assert state_2023["district"] == "State"
        assert _cell(df, "2023", "999", "All Students", "removal_by_hearing_officer_count")["suppressed"] == "Y"
        assert _num(_cell(df, "2020", "999", "All Students", "out_of_school_suspension_count")["value"]) == 28027
        assert _cell(df, "2020", "999", "All Students", "removal_by_hearing_officer_count")["suppressed"] == "Y"

        # The 2020 Adair American Indian row is all stars. It is not stored.
        adair_star = df[
            (df["year"] == "2020")
            & (df["school_id"] == "001010")
            & (df["student_group"] == "American Indian or Alaska Native")
        ]
        assert adair_star.empty


def test_registered_sources_are_the_permanent_files():
    """KYRC25 fills 2024-25. KYRC23 and the OAA names are not registered."""
    import yaml

    config = yaml.safe_load((ROOT / "config" / "kde_sources.yaml").read_text())
    names = []
    hosts = {}
    for entry in config["raw_directories"]["safe_schools_discipline"]:
        if isinstance(entry, dict):
            names.append(entry["file"])
            hosts[entry["file"]] = entry.get("url")
        else:
            names.append(str(entry))
    assert "KYRC25_SAFE_Discipline_Resolutions.csv" in names
    assert "KYRC25_SAFE_Legal_Sanctions.csv" in names
    assert hosts["KYRC25_SAFE_Discipline_Resolutions.csv"] == "kyrc25"
    assert hosts["KYRC25_SAFE_Legal_Sanctions.csv"] == "kyrc25"
    assert hosts["KYRC26_SAFE_Discipline_Resolutions.csv"] == "kyrc26"
    assert hosts["KYRC26_SAFE_Legal_Sanctions.csv"] == "kyrc26"
    assert "KYRC23_SAFE_Discipline_Resolutions.csv" not in names
    assert "KYRC23_SAFE_Legal_Sanctions.csv" not in names
    assert "SAFE_SCHOOLS_18-19.xlsx" in names
    for name in (
        "safe_schools_discipline_2020.csv",
        "safe_schools_discipline_2021.csv",
        "safe_schools_discipline_2022.csv",
        "safe_schools_discipline_2023.csv",
        "KYRC24_SAFE_Discipline_Resolutions.csv",
        "KYRC24_SAFE_Legal_Sanctions.csv",
    ):
        assert name in names
    # The shared safe_schools download list is not this module's registration.
    shared = []
    for entry in config["raw_directories"]["safe_schools"]:
        shared.append(entry["file"] if isinstance(entry, dict) else str(entry))
    assert "KYRC25_SAFE_Discipline_Resolutions.csv" not in shared
    assert "KYRC25_SAFE_Legal_Sanctions.csv" not in shared