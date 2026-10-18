"""
Tests for Gifted and Talented ETL module
"""
import pytest
from pathlib import Path
import pandas as pd
import tempfile
import yaml
from etl.constants import KPI_COLUMNS
import shutil
from etl.gifted_talented import transform, GiftedTalentedETL

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "gifted_talented"


class TestGiftedTalentedETL:

    def setup_method(self):
        """Setup test directories and sample data."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)

        # Create sample raw data directory
        self.sample_dir = self.raw_dir / "gifted_talented"
        self.sample_dir.mkdir(parents=True)

        # Create ETL instance for testing
        self.etl = GiftedTalentedETL('gifted_talented')

    def teardown_method(self):
        """Clean up test directories."""
        shutil.rmtree(self.test_dir)

    def create_grade_level_data(self):
        """Create sample KYRC25 grade level format data."""
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
            'Demographic': ['All Students', 'Female', 'Male'],
            'All Grades': ['90,490', '45,336', '45,154'],
            'K': ['2,785', '1,339', '1,446'],
            'Grade 1': ['6,109', '2,856', '3,253'],
            'Grade 2': ['8,126', '3,865', '4,261'],
            'Grade 3': ['9,278', '4,408', '4,870'],
            'Grade 4': ['5,327', '2,542', '2,785'],
            'Grade 5': ['7,011', '3,560', '3,451'],
            'Grade 6': ['7,419', '3,709', '3,710'],
            'Grade 7': ['6,954', '3,498', '3,456'],
            'Grade 8': ['7,316', '3,710', '3,606'],
            'Grade 9': ['7,387', '3,817', '3,570'],
            'Grade 10': ['7,868', '4,165', '3,703'],
            'Grade 11': ['7,596', '3,969', '3,627'],
            'Grade 12': ['7,313', '3,898', '3,415'],
            'Grade 14': ['*', '', '*'],
        })
        return data

    def create_historical_data(self):
        """Create sample historical format data (gifted_and_talented_YYYY.csv)."""
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
            'DEMOGRAPHIC': ['All Students', 'Female', 'Male'],
            'TOTAL COUNT': ['239', '116', '123'],
            'PRESCHOOL COUNT': ['*', '*', '*'],
            'KINDERGARTEN COUNT': ['*', '*', '*'],
            'GRADE1 COUNT': ['12', '3', '9'],
            'GRADE2 COUNT': ['20', '5', '15'],
            'GRADE3 COUNT': ['24', '8', '16'],
            'GRADE4 COUNT': ['14', '6', '8'],
            'GRADE5 COUNT': ['10', '7', '3'],
            'GRADE6 COUNT': ['17', '8', '9'],
            'GRADE7 COUNT': ['27', '19', '8'],
            'GRADE8 COUNT': ['17', '11', '6'],
            'GRADE9 COUNT': ['23', '7', '16'],
            'GRADE10 COUNT': ['18', '8', '10'],
            'GRADE11 COUNT': ['39', '22', '17'],
            'GRADE12 COUNT': ['18', '12', '6'],
            'GRADE14 COUNT': ['*', '*', '*'],
            'CREATIVE OR DIVERGENT THINKING': ['11', '8', '3'],
            'GENERAL INTELLECTUAL ABILITY': ['20', '11', '9'],
            'PRIMARY TALENT POOL': ['56', '16', '40'],
            'PSYCHO SOCIAL LEADERSHIP': ['36', '24', '12'],
            'SPECIFIC ACADEMIC APTITUDE_LANGUAGE ARTS': ['69', '37', '32'],
            'SPECIFIC ACADEMIC APTITUDE_MATH': ['31', '15', '16'],
            'SPECIFIC ACADEMIC APTITUDE_SCIENCE': ['25', '8', '17'],
            'SPECIFIC ACADEMIC APTITUDE_SOCIAL STUDIES': ['20', '11', '9'],
            'VISUAL AND PERFORMING ARTS_ART': ['12', '9', '3'],
            'VISUAL AND PERFORMING ARTS_DANCE': ['*', '*', '*'],
            'VISUAL AND PERFORMING ARTS_DRAMA': ['3', '3', '*'],
            'VISUAL AND PERFORMING ARTS_MUSIC': ['5', '3', '2'],
        })
        return data

    def test_normalize_column_names(self):
        """Test column name normalization."""
        df = self.create_grade_level_data()
        df_normalized = self.etl.normalize_column_names(df)

        assert 'school_year' in df_normalized.columns
        assert 'demographic' in df_normalized.columns
        assert 'all_grades' in df_normalized.columns
        assert 'kindergarten' in df_normalized.columns
        assert 'grade_1' in df_normalized.columns

    def test_normalize_historical_columns(self):
        """Test column normalization for historical format."""
        df = self.create_historical_data()
        df_normalized = self.etl.normalize_column_names(df)

        assert 'school_year' in df_normalized.columns
        assert 'demographic' in df_normalized.columns
        assert 'grade_1' in df_normalized.columns

    def test_extract_metrics_grade_level(self):
        """Test metric extraction from grade level data."""
        row = pd.Series({
            'all_grades': 90490.0,
            'kindergarten': 2785.0,
            'grade_1': 6109.0,
            'grade_2': 8126.0,
            'source_file': 'KYRC25_EDOP_Gifted_Participation_by_Grade_Level.csv'
        })

        metrics = self.etl.extract_metrics(row)

        assert 'gifted_participation_count_all_grades' in metrics
        assert metrics['gifted_participation_count_all_grades'] == 90490
        assert 'gifted_participation_count_kindergarten' in metrics
        assert metrics['gifted_participation_count_kindergarten'] == 2785
        assert 'gifted_participation_count_grade_1' in metrics
        assert metrics['gifted_participation_count_grade_1'] == 6109

    def test_transform_grade_level_data(self):
        """Test transform with grade level format data."""
        data = self.create_grade_level_data()
        data.to_csv(self.sample_dir / "KYRC25_EDOP_Gifted_Participation_by_Grade_Level.csv", index=False)

        config = {
            "derive": {
                "processing_date": "2025-11-24"
            }
        }

        transform(self.raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "gifted_talented.csv"
        assert output_file.exists()

        df = pd.read_csv(output_file)

        # Verify KPI format columns
        for col in KPI_COLUMNS:
            assert col in df.columns, f"Required KPI column '{col}' missing"

        # Verify expected metrics are present
        metrics = df['metric'].unique()
        assert 'gifted_participation_count_all_grades' in metrics
        assert 'gifted_participation_count_kindergarten' in metrics
        assert 'gifted_participation_count_grade_1' in metrics

        # Verify demographics are present
        student_groups = df['student_group'].unique()
        assert 'All Students' in student_groups
        assert 'Female' in student_groups

    def test_transform_historical_data(self):
        """Test transform with historical format data."""
        data = self.create_historical_data()
        data.to_csv(self.sample_dir / "gifted_and_talented_2022.csv", index=False)

        config = {
            "derive": {
                "processing_date": "2025-11-24"
            }
        }

        transform(self.raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "gifted_talented.csv"
        assert output_file.exists()

        df = pd.read_csv(output_file)

        # Verify metrics - historical format uses individual grade columns
        metrics = df['metric'].unique()
        assert 'gifted_participation_count_grade_1' in metrics

        # Verify year extraction
        assert '2022' in df['year'].astype(str).values

    def test_transform_multiple_files(self):
        """Test transform with multiple files."""
        grade_data = self.create_grade_level_data()
        historical_data = self.create_historical_data()

        grade_data.to_csv(self.sample_dir / "KYRC25_EDOP_Gifted_Participation_by_Grade_Level.csv", index=False)
        historical_data.to_csv(self.sample_dir / "gifted_and_talented_2022.csv", index=False)

        config = {
            "derive": {
                "processing_date": "2025-11-24"
            }
        }

        transform(self.raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "gifted_talented.csv"
        assert output_file.exists()

        df = pd.read_csv(output_file)

        # Verify both source files are represented
        source_files = df['source_file'].unique()
        assert len(source_files) >= 2

        # Should have multiple years
        years = df['year'].unique()
        assert len(years) >= 2

    def test_transform_no_data(self):
        """Test transform when no data exists."""
        empty_raw_dir = self.test_dir / "empty_raw"
        empty_raw_dir.mkdir()

        config = {}
        transform(empty_raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "gifted_talented.csv"
        assert not output_file.exists()

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
            'All Grades': ['*'],
            'K': ['*'],
            'Grade 1': ['*'],
            'Grade 2': ['*'],
            'Grade 3': ['*'],
            'Grade 4': ['*'],
            'Grade 5': ['*'],
            'Grade 6': ['*'],
            'Grade 7': ['*'],
            'Grade 8': ['*'],
            'Grade 9': ['*'],
            'Grade 10': ['*'],
            'Grade 11': ['*'],
            'Grade 12': ['*'],
            'Grade 14': ['*'],
        })
        data.to_csv(self.sample_dir / "test_suppressed.csv", index=False)

        config = {"derive": {"processing_date": "2025-11-24"}}
        transform(self.raw_dir, self.proc_dir, config)

        output_file = self.proc_dir / "gifted_talented.csv"
        df = pd.read_csv(output_file, dtype=str, keep_default_na=False)
        assert len(df) == 15
        assert set(df["metric"]) == {
            "gifted_participation_count_all_grades",
            "gifted_participation_count_kindergarten",
            "gifted_participation_count_grade_1",
            "gifted_participation_count_grade_2",
            "gifted_participation_count_grade_3",
            "gifted_participation_count_grade_4",
            "gifted_participation_count_grade_5",
            "gifted_participation_count_grade_6",
            "gifted_participation_count_grade_7",
            "gifted_participation_count_grade_8",
            "gifted_participation_count_grade_9",
            "gifted_participation_count_grade_10",
            "gifted_participation_count_grade_11",
            "gifted_participation_count_grade_12",
            "gifted_participation_count_grade_14",
        }
        assert (df["suppressed"] == "Y").all()
        assert (df["value"] == "").all()
        assert set(df["year"]) == {"2025"}


    def _production_config(self) -> dict:
        config = yaml.safe_load((ROOT / "config" / "mappings.yaml").read_text())
        return config["sources"]["gifted_talented"]

    def _copy_fixtures(self) -> None:
        for path in FIXTURE_DIR.glob("*.csv"):
            (self.sample_dir / path.name).write_bytes(path.read_bytes())

    def test_kyrc26_file_is_accepted(self):
        """KYRC26 is in scope. Grade 14 stays mapped for the years that have it."""
        assert self.etl.include_kyrc26 is True
        assert self.etl.accepts_data_file(Path("KYRC26_EDOP_Gifted_Participation_by_Grade_Level.csv"))
        assert self.etl.accepts_data_file(Path("KYRC26_EDOP_Gifted_Participation_by_Category.csv"))
        assert self.etl.accepts_data_file(Path("gifted_and_talented_2020.csv"))
        assert self.etl.accepts_data_file(Path("KYRC24_EDOP_Gifted_Participation_by_Grade_Level.csv"))
        mappings = self.etl.module_column_mappings
        assert mappings["TOTAL COUNT"] == "all_grades"
        assert mappings["Grade 14"] == "grade_14"
        assert mappings["ACADEMICARTS_CNT"] == "specific_academic_aptitude_language_arts"
        assert mappings["VISPERFART_CNT"] == "visual_and_performing_arts_art"

    def test_kyrc23_same_year_file_is_not_loaded(self):
        """KYRC23 is school year 20222023, already loaded from the historical file.

        A leftover copy must not write 2023 a second time. The guessed OAA
        names were never registered, so they are not skipped by name.
        """
        assert not self.etl.accepts_data_file(Path("KYRC23_EDOP_Gifted_Participation_by_Grade_Level.csv"))
        assert not self.etl.accepts_data_file(Path("KYRC23_EDOP_Gifted_Participation_by_Category.csv"))
        assert self.etl.accepts_data_file(Path("Gifted_Talented_2026.CSV"))
        data = pd.DataFrame({
            "School Year": ["20222023"],
            "District Name": ["All Districts"],
            "School Name": ["All Schools"],
            "School Code": ["999000"],
            "Demographic": ["All Students"],
            "All Grades": ["87,613"],
        })
        data.to_csv(
            self.sample_dir / "KYRC23_EDOP_Gifted_Participation_by_Grade_Level.csv",
            index=False,
        )
        transform(self.raw_dir, self.proc_dir, self._production_config())
        assert not (self.proc_dir / "gifted_talented.csv").exists()

    def test_blank_row_is_skipped_in_every_year(self):
        """A blank measure row is not stored. The skip is not limited to one year.

        A starred cell on the same shape is stored. No registered grade file
        has a row whose measure cells are all blank.
        """
        blank = pd.DataFrame({
            "School Year": ["20232024", "20252026"],
            "District Name": ["Adair County", "Adair County"],
            "School Name": ["Adair County High School", "Adair County High School"],
            "School Code": ["001010", "001010"],
            "Demographic": ["All Students", "All Students"],
            "All Grades": ["", ""],
            "K": ["", ""],
            "Grade 12": ["", ""],
        })
        starred = pd.DataFrame({
            "School Year": ["20252026"],
            "District Name": ["Adair County"],
            "School Name": ["Adair County High School"],
            "School Code": ["001010"],
            "Demographic": ["Homeless"],
            "All Grades": ["*"],
            "K": ["*"],
            "Grade 12": ["*"],
        })
        blank.to_csv(self.sample_dir / "blank_rows.csv", index=False)
        starred.to_csv(self.sample_dir / "starred_2026.csv", index=False)
        transform(self.raw_dir, self.proc_dir, self._production_config())
        df = pd.read_csv(self.proc_dir / "gifted_talented.csv", dtype=str, keep_default_na=False)
        assert set(df["year"]) == {"2026"}
        assert set(df["student_group"]) == {"Homeless"}
        assert (df["suppressed"] == "Y").all()
        assert (df["value"] == "").all()

    def test_published_zero_count_is_kept(self):
        """A published 0 is a count. Adair Learning Academy's category total is 0.

        Dropping zeros would still pass a fixture that only has positive
        state totals. This row keeps 0, suppressed N, on the grade total
        and on a talent category.
        """
        self._copy_fixtures()
        transform(self.raw_dir, self.proc_dir, self._production_config())
        df = pd.read_csv(self.proc_dir / "gifted_talented.csv", dtype=str, keep_default_na=False)
        total = df[
            (df["year"] == "2026")
            & (df["school_id"] == "001012")
            & (df["metric"] == "gifted_participation_count_all_grades")
        ]
        assert len(total) == 1
        assert total.iloc[0]["suppressed"] == "N"
        assert float(total.iloc[0]["value"]) == 0.0
        assert total.iloc[0]["source_file"] == "KYRC26_EDOP_Gifted_Participation_by_Category.csv"
        creative = df[
            (df["year"] == "2026")
            & (df["school_id"] == "001012")
            & (df["metric"] == "gifted_category_count_creative_or_divergent_thinking")
        ]
        assert len(creative) == 1
        assert creative.iloc[0]["suppressed"] == "N"
        assert float(creative.iloc[0]["value"]) == 0.0

    def test_repeated_school_key_keeps_the_larger_total(self):
        """2022 repeats Covington Independent 134. Keep total 105, not 3.

        The grade cells on the 105 row are the ones the previous parser
        stored. The 3 row is stars on those grades. A number beats a star.
        """
        self._copy_fixtures()
        transform(self.raw_dir, self.proc_dir, self._production_config())
        df = pd.read_csv(self.proc_dir / "gifted_talented.csv", dtype=str, keep_default_na=False)
        keys = ["year", "school_id", "student_group", "metric"]
        assert df.groupby(keys).size().max() == 1
        total = df[
            (df["year"] == "2022")
            & (df["school_id"] == "134")
            & (df["student_group"] == "All Students")
            & (df["metric"] == "gifted_participation_count_all_grades")
        ]
        assert len(total) == 1
        assert float(total.iloc[0]["value"]) == 105.0
        assert total.iloc[0]["suppressed"] == "N"
        grade_1 = df[
            (df["year"] == "2022")
            & (df["school_id"] == "134")
            & (df["metric"] == "gifted_participation_count_grade_1")
        ]
        assert len(grade_1) == 1
        assert float(grade_1.iloc[0]["value"]) == 6.0
        assert grade_1.iloc[0]["suppressed"] == "N"
        # Preschool is starred on every 2022 row, so it is not written.
        # Kindergarten on the 105 row is also a star and is still stored.
        # Dropping every 2022 star would fail that assertion.
        preschool = df[
            (df["year"] == "2022")
            & (df["school_id"] == "134")
            & (df["metric"] == "gifted_participation_count_preschool")
        ]
        assert len(preschool) == 0
        kindergarten = df[
            (df["year"] == "2022")
            & (df["school_id"] == "134")
            & (df["student_group"] == "All Students")
            & (df["metric"] == "gifted_participation_count_kindergarten")
        ]
        assert len(kindergarten) == 1
        assert kindergarten.iloc[0]["suppressed"] == "Y"
        assert kindergarten.iloc[0]["value"] == ""

    def test_2020_2023_always_starred_slices_are_not_written(self):
        """Preschool, grade 14, and Consolidated Student Group are omitted only in 2020-2023.

        The skip is the year and the slice. It does not drop a column
        because every cell is a star, and it does not drop a star in
        another group. Widening the year set, adding kindergarten to the
        metric set, or dropping every star fails the keep assertions:
        2019 Consolidated Student Group and grade 14, a 2020 kindergarten
        star, a 2020 consolidated total that is a number, and a 2024
        preschool star.
        """
        rows = [
            # year, school, group, total, preschool, kindergarten, grade 14
            # One school code per year. A repeated school key collapses
            # inside the file, and that collapse is not what this test checks.
            ("20182019", "2019", "All Students", "10", "*", "2", "1"),
            ("20182019", "2019", "Consolidated Student Group", "50", "*", "*", "0"),
            ("20192020", "2020", "All Students", "40", "*", "*", "*"),
            ("20192020", "2020", "Consolidated Student Group", "12", "*", "4", "*"),
            ("20202021", "2021", "All Students", "5", "*", "*", "*"),
            ("20212022", "2022", "All Students", "6", "*", "8", "*"),
            ("20222023", "2023", "All Students", "9", "*", "7", "*"),
            ("20222023", "2023", "Consolidated Student Group", "*", "*", "*", "*"),
            ("20232024", "2024", "All Students", "11", "*", "*", "3"),
        ]
        frame = pd.DataFrame(rows, columns=[
            "SCHOOL YEAR",
            "SCHOOL CODE",
            "DEMOGRAPHIC",
            "TOTAL COUNT",
            "PRESCHOOL COUNT",
            "KINDERGARTEN COUNT",
            "GRADE14 COUNT",
        ])
        frame.insert(1, "DISTRICT NAME", "Adair County")
        frame.insert(2, "SCHOOL NAME", "Adair County High School")
        frame.to_csv(self.sample_dir / "gifted_and_talented_years.csv", index=False)
        transform(self.raw_dir, self.proc_dir, self._production_config())
        df = pd.read_csv(self.proc_dir / "gifted_talented.csv", dtype=str, keep_default_na=False)

        def present(year, group, metric):
            matched = df[
                (df["year"] == year)
                & (df["school_id"] == year)
                & (df["student_group"] == group)
                & (df["metric"] == metric)
            ]
            assert len(matched) == 1, (year, group, metric, len(matched))
            return matched.iloc[0]

        def missing(year, group, metric):
            matched = df[
                (df["year"] == year)
                & (df["school_id"] == year)
                & (df["student_group"] == group)
                & (df["metric"] == metric)
            ]
            assert len(matched) == 0, (year, group, metric, len(matched))

        assert float(present("2019", "All Students", "gifted_participation_count_grade_14")["value"]) == 1.0
        preschool_2019 = present("2019", "All Students", "gifted_participation_count_preschool")
        assert preschool_2019["suppressed"] == "Y"
        assert float(present("2019", "Consolidated Student Group", "gifted_participation_count_all_grades")["value"]) == 50.0
        assert float(present("2019", "Consolidated Student Group", "gifted_participation_count_grade_14")["value"]) == 0.0

        assert float(present("2020", "All Students", "gifted_participation_count_all_grades")["value"]) == 40.0
        kindergarten_2020 = present("2020", "All Students", "gifted_participation_count_kindergarten")
        assert kindergarten_2020["suppressed"] == "Y"
        assert kindergarten_2020["value"] == ""
        missing("2020", "All Students", "gifted_participation_count_preschool")
        missing("2020", "All Students", "gifted_participation_count_grade_14")
        for metric in (
            "gifted_participation_count_all_grades",
            "gifted_participation_count_preschool",
            "gifted_participation_count_kindergarten",
            "gifted_participation_count_grade_14",
        ):
            missing("2020", "Consolidated Student Group", metric)

        for year in ("2021", "2022", "2023"):
            missing(year, "All Students", "gifted_participation_count_preschool")
            missing(year, "All Students", "gifted_participation_count_grade_14")
        kindergarten_2021 = present("2021", "All Students", "gifted_participation_count_kindergarten")
        assert kindergarten_2021["suppressed"] == "Y"
        assert float(present("2022", "All Students", "gifted_participation_count_kindergarten")["value"]) == 8.0
        assert float(present("2023", "All Students", "gifted_participation_count_kindergarten")["value"]) == 7.0
        for metric in (
            "gifted_participation_count_all_grades",
            "gifted_participation_count_preschool",
            "gifted_participation_count_kindergarten",
            "gifted_participation_count_grade_14",
        ):
            missing("2023", "Consolidated Student Group", metric)

        preschool_2024 = present("2024", "All Students", "gifted_participation_count_preschool")
        assert preschool_2024["suppressed"] == "Y"
        assert preschool_2024["value"] == ""
        assert float(present("2024", "All Students", "gifted_participation_count_grade_14")["value"]) == 3.0

    def test_comma_count_survives_string_dtype_and_int64(self):
        """pandas 3 dtype=str is StringDtype. ``90,464`` has to survive an Int64 cast.

        Production mappings.yaml has no dtype cast for this source. The
        cleaner still strips the comma so a later cast does not drop the
        state total.
        """
        raw = pd.Series(["90,464", "0", "*", ""], dtype="string")
        frame = pd.DataFrame({"all_grades": raw, "grade_1": raw})
        cleaned = self.etl.standardize_missing_values(frame.copy())
        assert cleaned["all_grades"].iloc[0] == "90464"
        assert bool(cleaned["_star_all_grades"].iloc[2])
        assert not bool(cleaned["_star_all_grades"].iloc[0])
        assert not bool(cleaned["_star_all_grades"].iloc[1])
        assert not bool(cleaned["_star_all_grades"].iloc[3])
        cast = cleaned["all_grades"].astype("Int64")
        assert int(cast.iloc[0]) == 90464
        assert int(cast.iloc[1]) == 0
        assert pd.isna(cast.iloc[2])
        assert pd.isna(cast.iloc[3])

    def test_real_rows_match_the_downloaded_files(self):
        """State, district, school, a partial star, a published 0, and prior anchors.

        Category All Students is the same total as All Grades, so it is
        stored once. Talent categories use a different metric name.
        KYRC26 has no Grade 14 column. A blank grade cell is not stored.
        ``20022023`` on the Breathitt district total is year 2023.
        """
        self._copy_fixtures()
        transform(self.raw_dir, self.proc_dir, self._production_config())
        df = pd.read_csv(self.proc_dir / "gifted_talented.csv", dtype=str, keep_default_na=False)
        keys = ["year", "school_id", "student_group", "metric"]
        assert df.groupby(keys).size().max() == 1

        def cell(year, school_id, group, metric):
            rows = df[
                (df["year"] == year)
                & (df["school_id"] == school_id)
                & (df["student_group"] == group)
                & (df["metric"] == metric)
            ]
            assert len(rows) == 1, (year, school_id, group, metric, len(rows))
            return rows.iloc[0]

        def absent(year, school_id, group, metric):
            rows = df[
                (df["year"] == year)
                & (df["school_id"] == school_id)
                & (df["student_group"] == group)
                & (df["metric"] == metric)
            ]
            assert len(rows) == 0, (year, school_id, group, metric, len(rows))

        state = cell("2026", "999000", "All Students", "gifted_participation_count_all_grades")
        assert float(state["value"]) == 90464.0
        assert state["suppressed"] == "N"
        assert state["district"] == "All Districts"
        assert state["school_name"] == "---District Total---"
        assert state["source_file"] == "KYRC26_EDOP_Gifted_Participation_by_Grade_Level.csv"
        assert float(cell("2026", "999000", "All Students", "gifted_participation_count_kindergarten")["value"]) == 2616.0
        assert float(cell("2026", "999000", "All Students", "gifted_participation_count_grade_12")["value"]) == 7380.0
        assert "gifted_participation_count_grade_14" not in set(df.loc[df["year"] == "2026", "metric"])
        creative = cell("2026", "999000", "All Students", "gifted_category_count_creative_or_divergent_thinking")
        assert float(creative["value"]) == 7030.0
        assert creative["suppressed"] == "N"
        assert creative["source_file"] == "KYRC26_EDOP_Gifted_Participation_by_Category.csv"

        fayette = cell("2026", "165000", "All Students", "gifted_participation_count_all_grades")
        assert float(fayette["value"]) == 10937.0
        assert fayette["district"] == "Fayette County"
        assert fayette["nces_id"] == "2101860"
        assert fayette["co_op"] == "CKEC"
        assert fayette["co_op_code"] == "901"
        assert fayette["school_name"] == "---District Total---"
        bryan = cell("2026", "165170", "All Students", "gifted_participation_count_all_grades")
        assert float(bryan["value"]) == 238.0
        assert bryan["school_name"] == "Bryan Station High School"
        assert bryan["nces_id"] == "210186000388"
        assert bryan["state_school_id"] == "034165170"
        assert bryan["school_type"] == "A1"
        # High school grades are published. K through grade 8 are blank, not zero.
        assert float(cell("2026", "165170", "All Students", "gifted_participation_count_grade_9")["value"]) == 64.0
        assert "gifted_participation_count_kindergarten" not in set(
            df.loc[(df["year"] == "2026") & (df["school_id"] == "165170"), "metric"]
        )

        native = cell(
            "2026",
            "999000",
            "American Indian or Alaska Native",
            "gifted_participation_count_all_grades",
        )
        assert float(native["value"]) == 105.0
        assert native["suppressed"] == "N"
        starred = cell(
            "2026",
            "999000",
            "American Indian or Alaska Native",
            "gifted_participation_count_kindergarten",
        )
        assert starred["value"] == ""
        assert starred["suppressed"] == "Y"
        assert float(cell(
            "2026",
            "999000",
            "American Indian or Alaska Native",
            "gifted_participation_count_grade_1",
        )["value"]) == 10.0

        # Prior-year anchors. Published values, not a same-branch diff.
        prior = cell("2025", "999000", "All Students", "gifted_participation_count_all_grades")
        assert float(prior["value"]) == 90490.0
        assert prior["source_file"] == "KYRC25_EDOP_Gifted_Participation_by_Grade_Level.csv"
        # KYRC25 still publishes Grade 14. The state cell is a star.
        grade_14 = cell("2025", "999000", "All Students", "gifted_participation_count_grade_14")
        assert grade_14["suppressed"] == "Y"
        assert grade_14["value"] == ""
        assert float(cell("2025", "999000", "All Students", "gifted_participation_count_kindergarten")["value"]) == 2785.0

        historical = cell("2023", "999", "All Students", "gifted_participation_count_all_grades")
        assert float(historical["value"]) == 87613.0
        assert historical["district"] == "State"
        assert historical["school_name"] == "---State Total---"
        assert historical["source_file"] == "gifted_and_talented_2023.csv"
        assert float(cell("2023", "999", "All Students", "gifted_participation_count_kindergarten")["value"]) == 3025.0
        assert float(cell(
            "2023",
            "999",
            "All Students",
            "gifted_category_count_creative_or_divergent_thinking",
        )["value"]) == 6039.0
        absent("2023", "999", "All Students", "gifted_participation_count_preschool")
        absent("2023", "999", "All Students", "gifted_participation_count_grade_14")

        breathitt = cell("2023", "061", "All Students", "gifted_participation_count_all_grades")
        assert float(breathitt["value"]) == 239.0
        assert breathitt["district"] == "Breathitt County"
        assert breathitt["school_name"] == "---District Total---"
        assert float(cell("2023", "061", "All Students", "gifted_participation_count_kindergarten")["value"]) == 17.0
        # Dance is a star on this 2023 row and is not one of the omitted slices.
        dance = cell(
            "2023",
            "061",
            "All Students",
            "gifted_category_count_visual_and_performing_arts_dance",
        )
        assert dance["suppressed"] == "Y"
        assert dance["value"] == ""

    def test_collapse_repeated_keys_overlays_a_number_from_the_smaller_row(self):
        """The merge itself keeps 105 and overlays a number the 105 row starred.

        This calls ``_collapse_repeated_keys`` on one frame. It does not
        go through ``process``. A later per-metric dedupe cannot hide a
        no-op here: two rows would remain, and grade 1 would stay a star.
        """
        frame = pd.DataFrame([
            {
                "school_code": "134",
                "school_name": "smaller copy",
                "demographic": "All Students",
                "all_grades": "3",
                "grade_1": "6",
                "_star_all_grades": False,
                "_star_grade_1": False,
            },
            {
                "school_code": "134",
                "school_name": "larger copy",
                "demographic": "All Students",
                "all_grades": "105",
                "grade_1": pd.NA,
                "_star_all_grades": False,
                "_star_grade_1": True,
            },
        ])
        collapsed = self.etl._collapse_repeated_keys(frame, "gifted_and_talented_2022.csv")
        assert len(collapsed) == 1
        row = collapsed.iloc[0]
        assert row["school_name"] == "larger copy"
        assert row["all_grades"] == "105"
        assert row["grade_1"] == "6"
        assert not bool(row["_star_grade_1"])

    def test_star_text_fallback_does_not_treat_a_blank_as_a_star(self):
        """A row with no ``_star_`` column uses the cell text.

        ``*`` is stored as suppressed Y. A blank is skipped. Treating the
        blank as a star would keep the row.
        """
        blank = pd.Series({
            "demographic": "All Students",
            "all_grades": "",
            "school_code": "001010",
            "school_year": "20252026",
            "district_name": "Adair County",
            "school_name": "Adair County High School",
        })
        assert self.etl.should_skip_row(blank)
        assert self.etl.extract_metrics(blank) == {}

        star = blank.copy()
        star["all_grades"] = "*"
        assert not self.etl.should_skip_row(star)
        metrics = self.etl.extract_metrics(star)
        assert "gifted_participation_count_all_grades" in metrics
        assert pd.isna(metrics["gifted_participation_count_all_grades"])

        kpi = self.etl.convert_to_kpi_format(pd.DataFrame([star]), "star_fallback.csv")
        assert len(kpi) == 1
        assert kpi.iloc[0]["suppressed"] == "Y"
        assert pd.isna(kpi.iloc[0]["value"])

    def test_wide_kyrc24_category_file(self):
        """KYRC24 category rows are wide. The count sits on the diagonal.

        All Students uses All Grades. Creative uses its own column. The
        other columns on that row are blank and are not stored as zeros.
        """
        self._copy_fixtures()
        transform(self.raw_dir, self.proc_dir, self._production_config())
        df = pd.read_csv(self.proc_dir / "gifted_talented.csv", dtype=str, keep_default_na=False)
        year = df[
            (df["year"] == "2024")
            & (df["school_id"] == "999000")
            & (df["source_file"] == "KYRC24_EDOP_Gifted_Participation_by_Category.csv")
        ]
        assert set(year["metric"]) == {
            "gifted_participation_count_all_grades",
            "gifted_category_count_creative_or_divergent_thinking",
        }
        total = year[year["metric"] == "gifted_participation_count_all_grades"].iloc[0]
        assert float(total["value"]) == 88706.0
        assert total["suppressed"] == "N"
        assert total["student_group"] == "All Students"
        creative = year[year["metric"] == "gifted_category_count_creative_or_divergent_thinking"].iloc[0]
        assert float(creative["value"]) == 6277.0
        assert creative["suppressed"] == "N"
        assert "gifted_category_count_general_intellectual_ability" not in set(year["metric"])

    def test_2019_workbook_maps_disability_from_dgroup(self):
        """2019 Students with Disabilities is ``Studw/disab.``, not code CSG.

        The shared map sends CSG to Students with Disabilities and drops
        ACD. This workbook's DGROUP says CSG is Consolidated Student Group
        and ACD is the disability count. A CSV that still carries the code
        CSG keeps the shared map.
        """
        workbook = pd.DataFrame([
            {
                "SCH_YEAR": "20182019",
                "DIST_NAME": "State",
                "SCH_NAME": "---State Total---",
                "SCH_CD": "999",
                "DEMO": "TST",
                "DGROUP": "All Students",
                "GT_CNT": "94644",
            },
            {
                "SCH_YEAR": "20182019",
                "DIST_NAME": "State",
                "SCH_NAME": "---State Total---",
                "SCH_CD": "999",
                "DEMO": "CSG",
                "DGROUP": "Consolidated Student Group",
                "GT_CNT": "14874",
            },
            {
                "SCH_YEAR": "20182019",
                "DIST_NAME": "State",
                "SCH_NAME": "---State Total---",
                "SCH_CD": "999",
                "DEMO": "ACD",
                "DGROUP": "Studw/disab.",
                "GT_CNT": "3136",
            },
            {
                "SCH_YEAR": "20182019",
                "DIST_NAME": "State",
                "SCH_NAME": "---State Total---",
                "SCH_CD": "999",
                "DEMO": "LEP",
                "DGROUP": "EL",
                "GT_CNT": "797",
            },
            {
                "SCH_YEAR": "20182019",
                "DIST_NAME": "State",
                "SCH_NAME": "---State Total---",
                "SCH_CD": "999",
                "DEMO": "LUP",
                "DGROUP": "Free/Reduced",
                "GT_CNT": "34919",
            },
            {
                "SCH_YEAR": "20182019",
                "DIST_NAME": "State",
                "SCH_NAME": "---State Total---",
                "SCH_CD": "999",
                "DEMO": "ETH",
                "DGROUP": "Hispanic",
                "GT_CNT": "3616",
            },
            {
                "SCH_YEAR": "20182019",
                "DIST_NAME": "State",
                "SCH_NAME": "---State Total---",
                "SCH_CD": "999",
                "DEMO": "MIL",
                "DGROUP": "Military Connected",
                "GT_CNT": "215",
            },
        ])
        workbook.to_excel(
            self.sample_dir / "GIFTED_AND_TALENTED_18-19.xlsx",
            sheet_name="DATA",
            index=False,
        )
        coded = pd.DataFrame({
            "SCHOOL YEAR": ["20182019"],
            "DISTRICT NAME": ["State"],
            "SCHOOL NAME": ["---State Total---"],
            "SCHOOL CODE": ["999"],
            "DEMOGRAPHIC": ["CSG"],
            "TOTAL COUNT": ["14874"],
        })
        coded.to_csv(self.sample_dir / "gifted_codes_2019.csv", index=False)
        transform(self.raw_dir, self.proc_dir, self._production_config())
        df = pd.read_csv(self.proc_dir / "gifted_talented.csv", dtype=str, keep_default_na=False)
        book = df[df["source_file"] == "GIFTED_AND_TALENTED_18-19.xlsx"]
        metric = "gifted_participation_count_all_grades"

        def value_for(group: str) -> float:
            rows = book[(book["student_group"] == group) & (book["metric"] == metric)]
            assert len(rows) == 1, (group, len(rows))
            return float(rows.iloc[0]["value"])

        assert value_for("All Students") == 94644.0
        assert value_for("Consolidated Student Group") == 14874.0
        assert value_for("Students with Disabilities (IEP)") == 3136.0
        assert value_for("English Learner") == 797.0
        assert value_for("Economically Disadvantaged") == 34919.0
        assert value_for("Hispanic or Latino") == 3616.0
        assert value_for("Military Dependent") == 215.0
        coded_rows = df[df["source_file"] == "gifted_codes_2019.csv"]
        assert set(coded_rows["student_group"]) == {"Students with Disabilities (IEP)"}
        assert float(coded_rows.iloc[0]["value"]) == 14874.0


def test_registered_sources_are_the_permanent_files():
    """KYRC24 through KYRC26 and the 2020-2023 historical files are registered.

    KYRC23 is not registered. ``gifted_and_talented_2020.csv`` is the
    2019-20 gap. The 2017-18 workbook is earlier than the series.
    """
    config = yaml.safe_load((ROOT / "config" / "kde_sources.yaml").read_text())
    names = []
    for entry in config["raw_directories"]["gifted_talented"]:
        names.append(entry["file"] if isinstance(entry, dict) else entry)

    assert "KYRC26_EDOP_Gifted_Participation_by_Grade_Level.csv" in names
    assert "KYRC26_EDOP_Gifted_Participation_by_Category.csv" in names
    assert "gifted_and_talented_2020.csv" in names
    assert "GIFTED_AND_TALENTED_18-19.xlsx" in names
    assert not any(name.startswith("KYRC23_") for name in names)
    assert not any(name.startswith("Gifted_") for name in names)
    assert "GIFTED_AND_TALENTED_17-18.xlsx" not in names


class TestGiftedTalentedHelpers:
    """Test helper functions independently."""

    def setup_method(self):
        """Setup ETL instance for testing."""
        self.etl = GiftedTalentedETL('gifted_talented')

    def test_get_suppressed_metric_defaults(self):
        """Test suppressed metric defaults."""
        row = pd.Series({
            'source_file': 'test.csv'
        })

        defaults = self.etl.get_suppressed_metric_defaults(row)

        # Should have defaults for grade levels
        assert 'gifted_participation_count_all_grades' in defaults
        assert pd.isna(defaults['gifted_participation_count_all_grades'])

    def test_should_skip_row_with_valid_demographic(self):
        """A demographic with a count is kept. A demographic with no measure is not.

        A blank measure cell is not a star. The row is skipped when every
        grade and category cell is blank, in every year.
        """
        empty = pd.Series({
            'demographic': 'All Students',
            'source_file': 'test.csv'
        })
        assert self.etl.should_skip_row(empty)

        published = pd.Series({
            'demographic': 'All Students',
            'all_grades': '12',
            'source_file': 'test.csv'
        })
        assert not self.etl.should_skip_row(published)

    def test_should_skip_row_without_demographic(self):
        """Test that rows without demographics are skipped."""
        row = pd.Series({
            'demographic': pd.NA,
            'source_file': 'test.csv'
        })

        assert self.etl.should_skip_row(row)

    def test_standardize_missing_values(self):
        """Test missing value standardization."""
        df = pd.DataFrame({
            'all_grades': ['1,000', '*', '500', ''],
            'grade_1': ['100', '50', '*', '25']
        })

        df_clean = self.etl.standardize_missing_values(df)

        # Check that commas are removed on object and on StringDtype.
        assert df_clean.loc[0, 'all_grades'] == '1000'
        # The star is recorded before the cell becomes NA.
        assert bool(df_clean.loc[1, '_star_all_grades'])
        assert not bool(df_clean.loc[0, '_star_all_grades'])
        assert not bool(df_clean.loc[3, '_star_all_grades'])
        # Suppression markers are converted to pd.NA (string '<NA>' on some builds)
        assert df_clean.loc[1, 'all_grades'] is pd.NA or str(df_clean.loc[1, 'all_grades']) == '<NA>'
