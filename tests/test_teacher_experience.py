"""
Tests for Teacher Experience ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
import yaml
from etl.constants import KPI_COLUMNS
import shutil
from etl.teacher_experience import transform, TeacherExperienceETL

ROOT = Path(__file__).resolve().parents[1]


class TestTeacherExperienceETL:
    
    def setup_method(self):
        """Setup test directories and sample data."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        
        # Create sample raw data directory
        self.sample_dir = self.raw_dir / "teacher_experience"
        self.sample_dir.mkdir(parents=True)
        
        # Create ETL instance for testing
        self.etl = TeacherExperienceETL('teacher_experience')
    
    def teardown_method(self):
        """Clean up test directories."""
        shutil.rmtree(self.test_dir)
    
    def create_sample_2024_data(self):
        """Create sample KYRC24 format data."""
        data = pd.DataFrame({
            'School Year': ['20232024', '20232024', '20232024'],
            'County Number': ['001', '001', '001'],
            'County Name': ['Adair', 'Adair', 'Adair'],
            'District Number': ['001', '001', '001'],
            'District Name': ['Adair County', 'Adair County', 'Adair County'],
            'School Number': ['010', '010', '999'],
            'School Name': ['Adair County High School', 'Adair County Middle', 'All Schools'],
            'School Code': ['001010', '001011', '001000'],
            'State School Id': ['001001010', '001001011', ''],
            'NCES ID': ['210003000001', '210003000002', ''],
            'CO-OP': ['GRREC', 'GRREC', 'GRREC'],
            'CO-OP Code': ['902', '902', '902'],
            'School Type': ['A1', 'A1', ''],
            'Educator Count': ['45', '32', '177'],
            'Average Years of Experience': ['12.5', '8.3', '10.2']
        })
        return data
    
    def create_sample_historical_data(self):
        """Create sample historical format data (lowercase columns)."""
        data = pd.DataFrame({
            'School Year': ['20222023', '20222023'],
            'County Number': ['001', '001'],
            'County Name': ['Adair', 'Adair'],
            'District Number': ['001', '001'],
            'District Name': ['Adair County', 'Adair County'],
            'School Number': ['010', '999'],
            'School Name': ['Adair County High School', 'All Schools'],
            'School Code': ['001010', '001000'],
            'State School Id': ['001001010', ''],
            'NCES ID': ['210003000001', ''],
            'CO-OP': ['GRREC', 'GRREC'],
            'CO-OP Code': ['902', '902'],
            'School Type': ['A1', ''],
            'Educator Count': ['43', '170'],
            'Average Years of Experience': ['11.8', '9.9']
        })
        return data

    def test_column_mappings(self):
        """Test that column mappings include required fields."""
        mappings = self.etl.module_column_mappings
        
        assert 'Educator Count' in mappings
        assert mappings['Educator Count'] == 'educator_count'
        assert 'Average Years of Experience' in mappings
        assert mappings['Average Years of Experience'] == 'average_years_experience'

    def test_extract_metrics(self):
        """Test metric extraction from sample data."""
        sample_data = self.create_sample_2024_data()
        sample_data = self.etl.normalize_column_names(sample_data)
        
        # Test first row (high school)
        row = sample_data.iloc[0]
        metrics = self.etl.extract_metrics(row)
        
        assert 'teacher_average_years_experience' in metrics
        assert metrics['teacher_average_years_experience'] == 12.5
        
        # Test second row (middle school)
        row = sample_data.iloc[1]
        metrics = self.etl.extract_metrics(row)
        assert metrics['teacher_average_years_experience'] == 8.3

    def test_extract_metrics_with_missing_data(self):
        """Test that rows with missing data are handled correctly."""
        data = pd.DataFrame({
            'School Year': ['20232024'],
            'County Number': ['001'],
            'District Number': ['001'],
            'School Number': ['010'],
            'School Code': ['001010'],
            'District Name': ['Test District'],
            'School Name': ['Test School'],
            'Educator Count': ['0'],  # No educators
            'Average Years of Experience': [pd.NA]  # Missing
        })
        
        data = self.etl.normalize_column_names(data)
        row = data.iloc[0]
        
        # Should skip row if educator count is 0 or missing
        assert self.etl.should_skip_row(row)

    def test_negative_values_excluded(self):
        """Test that negative experience values are excluded."""
        data = pd.DataFrame({
            'School Year': ['20232024'],
            'County Number': ['001'],
            'District Number': ['001'],
            'School Number': ['010'],
            'School Code': ['001010'],
            'District Name': ['Test'],
            'School Name': ['Test'],
            'Educator Count': ['10'],
            'Average Years of Experience': ['-5.0']  # Invalid negative
        })
        
        data = self.etl.normalize_column_names(data)
        row = data.iloc[0]
        metrics = self.etl.extract_metrics(row)
        
        # Negative values should be excluded
        assert 'teacher_average_years_experience' not in metrics

    def test_convert_to_kpi_format(self):
        """Test conversion to KPI format."""
        sample_data = self.create_sample_2024_data()
        sample_data = self.etl.normalize_column_names(sample_data)
        
        kpi_df = self.etl.convert_to_kpi_format(sample_data, "test_file.csv")
        
        # Check that DataFrame has correct columns
        assert set(kpi_df.columns).issubset(set(KPI_COLUMNS))
        
        # Check that we have the right number of KPI rows (one metric per school)
        assert len(kpi_df) == 3  # 3 schools
        
        # Check specific metric exists
        metrics = kpi_df['metric'].unique()
        assert 'teacher_average_years_experience' in metrics
        
        # Check values are correct
        high_school_row = kpi_df[kpi_df['school_id'] == '001010'].iloc[0]
        assert high_school_row['value'] == 12.5
        assert high_school_row['year'] == '2024'

    def test_suppressed_metric_defaults(self):
        """Test suppressed metric defaults (inherited from base)."""
        sample_row = pd.Series({'educator_count': 5, 'average_years_experience': 10.0})
        defaults = self.etl.get_suppressed_metric_defaults(sample_row)
        
        # Base class returns empty dict for institutional data without demographics
        assert isinstance(defaults, dict)

    def test_should_skip_row(self):
        """Test row skipping logic."""
        # Row with no educator count should be skipped
        row_no_educators = pd.Series({
            'educator_count': pd.NA,
            'average_years_experience': 10.0,
            'demographic': 'All Students'
        })
        assert self.etl.should_skip_row(row_no_educators)
        
        # Row with zero educators should be skipped  
        row_zero_educators = pd.Series({
            'educator_count': 0,
            'average_years_experience': 10.0,
            'demographic': 'All Students'
        })
        assert self.etl.should_skip_row(row_zero_educators)
        
        # Row with no experience data should be skipped
        row_no_experience = pd.Series({
            'educator_count': 10,
            'average_years_experience': pd.NA,
            'demographic': 'All Students'
        })
        assert self.etl.should_skip_row(row_no_experience)
        
        # Valid row should not be skipped
        row_valid = pd.Series({
            'educator_count': 10,
            'average_years_experience': 12.5,
            'demographic': 'All Students'
        })
        assert not self.etl.should_skip_row(row_valid)

    def test_transform_integration(self):
        """Test complete transform function integration."""
        # Create sample files
        sample_2024 = self.create_sample_2024_data()
        sample_historical = self.create_sample_historical_data()
        
        # Save to test directory
        sample_2024.to_csv(self.sample_dir / "KYRC24_OVW_Average_Years_School_Experience.csv", index=False)
        sample_historical.to_csv(self.sample_dir / "average_years_school_experience_2023.csv", index=False)
        
        # Run transform
        config = {}
        transform(self.raw_dir, self.proc_dir, config)
        
        # Check output file was created
        output_file = self.proc_dir / "teacher_experience.csv"
        assert output_file.exists()
        
        # Load and validate output
        result_df = pd.read_csv(output_file)
        assert not result_df.empty
        assert set(result_df.columns).issubset(set(KPI_COLUMNS))
        
        # Check that we have data from both source files
        source_files = result_df['source_file'].unique()
        assert any('KYRC24' in f for f in source_files)
        assert any('2023' in f for f in source_files)
        
        # Check years (years are integers in the DataFrame)
        years = result_df['year'].unique()
        assert 2024 in years
        assert 2023 in years

    def test_data_type_validation(self):
        """Test that experience values are properly validated as numeric."""
        sample_data = pd.DataFrame({
            'School Year': ['20232024', '20232024', '20232024'],
            'County Number': ['001', '001', '001'],
            'District Number': ['001', '001', '001'],
            'School Number': ['010', '011', '012'],
            'School Code': ['001010', '001011', '001012'],
            'District Name': ['Test', 'Test', 'Test'],
            'School Name': ['School A', 'School B', 'School C'],
            'Educator Count': ['10', '15', '20'],
            'Average Years of Experience': ['12.5', 'invalid', '8.3']  # One invalid
        })
        
        sample_data = self.etl.normalize_column_names(sample_data)
        
        # Process through convert_to_kpi_format
        kpi_df = self.etl.convert_to_kpi_format(sample_data, "test.csv")
        
        # Should only have 2 valid rows (invalid excluded)
        assert len(kpi_df) == 2
        assert 12.5 in kpi_df['value'].values
        assert 8.3 in kpi_df['value'].values


class TestKyrc26TeacherExperience:
    """KYRC26 opt-in. KYRC23 republishes 2022-23 and is skipped."""

    def setup_method(self):
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        self.sample_dir = self.raw_dir / "teacher_experience"
        self.sample_dir.mkdir(parents=True)
        self.etl = TeacherExperienceETL("teacher_experience")

    def teardown_method(self):
        shutil.rmtree(self.test_dir)

    def test_kyrc26_file_is_accepted(self):
        """The permanent KYRC files and the historical names stay in scope."""
        assert self.etl.include_kyrc26 is True
        assert self.etl.module_column_mappings["Educator Count"] == "educator_count"
        assert self.etl.module_column_mappings["Average Years of Experience"] == "average_years_experience"
        assert self.etl.module_column_mappings["EDUCATOR COUNT"] == "educator_count"
        assert self.etl.module_column_mappings["CNTEXP"] == "educator_count"

        accepted = [
            "KYRC26_OVW_Average_Years_School_Experience.csv",
            "KYRC25_OVW_Average_Years_School_Experience.csv",
            "KYRC24_OVW_Average_Years_School_Experience.csv",
            "average_years_school_experience_2023.csv",
            "average_years_school_experience_2022.csv",
            "average_years_school_experience_2021.csv",
            "average_years_school_experience_2020.csv",
        ]
        for name in accepted:
            (self.sample_dir / name).write_text("School Year,Educator Count\n20252026,10\n")
        (self.sample_dir / "KYRC23_OVW_Average_Years_School_Experience.csv").write_text(
            "School Year,Educator Count,Average Years of Experience\n20222023,10,12.0\n"
        )

        found = sorted(path.name for path, _sheet in self.etl._find_data_files(self.sample_dir))
        assert found == sorted(accepted)

    def test_kyrc23_republish_is_skipped(self):
        """A leftover KYRC23 file must not write a second copy of 2023.

        The datasets file matches the registered 2022-23 extract on educator
        count and average years. School codes are the same, so accepting it
        would duplicate those KPI rows.
        """
        republish = (
            "School Year,District Name,School Name,School Code,Educator Count,"
            "Average Years of Experience\n"
            "20222023,Adair County,Adair County High School,001010,55,15.60\n"
        )
        (self.sample_dir / "KYRC23_OVW_Average_Years_School_Experience.csv").write_text(republish)
        (self.sample_dir / "KYRC26_OVW_Average_Years_School_Experience.csv").write_text(
            "School Year,District Name,School Name,School Code,Educator Count,"
            "Average Years of Experience\n"
            "20252026,All Districts,All Schools,999000,54,12.20\n"
        )
        config = yaml.safe_load((ROOT / "config" / "mappings.yaml").read_text())
        transform(self.raw_dir, self.proc_dir, config["sources"]["teacher_experience"])
        result = pd.read_csv(self.proc_dir / "teacher_experience.csv", dtype=str)
        assert set(result["year"]) == {"2026"}
        assert set(result["source_file"]) == {"KYRC26_OVW_Average_Years_School_Experience.csv"}
        assert float(result["value"].iloc[0]) == 12.20

    def test_blank_row_emits_nothing(self):
        """An empty measure row is not offered and emits no KPI row.

        notes/8: a row that is not suppressed and has every value empty is
        the absence of a row. These files have no Suppressed column and no
        all-blank measure row. The configured Int64 cast stores the empty
        count as pd.NA. The lookup uses pd.isna so that value is not
        evaluated as a boolean.
        """
        text = (
            "School Year,District Name,School Name,School Code,Educator Count,"
            "Average Years of Experience\n"
            "20252026,All Districts,All Schools,999000,,\n"
            "20252026,Adair County,Adair County High School,001010,54,\n"
            "20252026,Fayette County,Bryan Station High School,165170,133,14.29\n"
        )
        (self.sample_dir / "KYRC26_OVW_Average_Years_School_Experience.csv").write_text(text)
        config = yaml.safe_load((ROOT / "config" / "mappings.yaml").read_text())
        transform(self.raw_dir, self.proc_dir, config["sources"]["teacher_experience"])
        result = pd.read_csv(self.proc_dir / "teacher_experience.csv", dtype=str)
        assert list(result["school_id"]) == ["165170"]
        assert float(result["value"].iloc[0]) == 14.29
        assert result["suppressed"].iloc[0] == "N"
        assert result["year"].iloc[0] == "2026"

    def test_comma_removal_string_dtype(self):
        """pandas 3 StringDtype keeps a thousands separator until it is stripped.

        ``read_csv(..., dtype=str)`` returns StringDtype on pandas 3 and
        object on pandas 2.3.3. The state count ``39,242`` has to survive
        both, including the production Int64 cast. ``bool(pd.NA)`` is not
        used to detect that dtype.
        """
        frame = pd.DataFrame({
            "educator_count": pd.Series(["39,242", "2,919", pd.NA], dtype="string"),
            "average_years_experience": pd.Series(["12.20", "13.38", ""], dtype="string"),
        })
        cleaned = self.etl.standardize_missing_values(frame)
        assert cleaned["educator_count"].iloc[0] == 39242
        assert cleaned["educator_count"].iloc[1] == 2919
        assert pd.isna(cleaned["educator_count"].iloc[2])
        assert cleaned["average_years_experience"].iloc[0] == 12.20
        assert cleaned["average_years_experience"].iloc[1] == 13.38
        assert pd.isna(cleaned["average_years_experience"].iloc[2])

        cleaned["educator_count"] = pd.to_numeric(
            cleaned["educator_count"], errors="coerce"
        ).astype("Int64")
        cleaned["average_years_experience"] = pd.to_numeric(
            cleaned["average_years_experience"], errors="coerce"
        ).astype("float64")
        assert not self.etl.should_skip_row(cleaned.iloc[0])
        assert self.etl.should_skip_row(cleaned.iloc[2])
        metrics = self.etl.extract_metrics(cleaned.iloc[0])
        assert metrics["teacher_average_years_experience"] == 12.20


def test_registered_sources_are_the_permanent_files():
    """KYRC24, KYRC25, and KYRC26 are registered. KYRC23 is not."""
    config = yaml.safe_load((ROOT / "config" / "kde_sources.yaml").read_text())
    names = []
    for entry in config["raw_directories"]["teacher_experience"]:
        names.append(entry["file"] if isinstance(entry, dict) else entry)

    assert names == [
        "KYRC24_OVW_Average_Years_School_Experience.csv",
        "KYRC25_OVW_Average_Years_School_Experience.csv",
        "KYRC26_OVW_Average_Years_School_Experience.csv",
        "average_years_school_experience_2023.csv",
        "average_years_school_experience_2022.csv",
        "average_years_school_experience_2021.csv",
        "average_years_school_experience_2020.csv",
        "SCHOOL_EXPERIENCE_18-19.xlsx",
    ]
    assert not any("KYRC23" in name for name in names)
    assert not any("Average_Years_School_Experience_20" in name for name in names)
