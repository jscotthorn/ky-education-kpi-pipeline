"""Tests for the CTE pathways ETL."""
from pathlib import Path

import pandas as pd
import yaml

from etl.constants import KPI_COLUMNS
from etl.cte_pathways import CTEPathwaysETL, clean_numeric_with_commas, transform

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "cte_pathways"


def _config():
    """Production mappings.yaml entry. This source has no dtype cast."""
    loaded = yaml.safe_load((ROOT / "config" / "mappings.yaml").read_text())
    return loaded["sources"]["cte_pathways"]


def _run(tmp_path: Path, names_and_bytes: list) -> pd.DataFrame:
    raw_dir = tmp_path / "raw"
    source_dir = raw_dir / "cte_pathways"
    source_dir.mkdir(parents=True)
    proc_dir = tmp_path / "processed"
    proc_dir.mkdir()
    for name, payload in names_and_bytes:
        (source_dir / name).write_bytes(payload)
    transform(raw_dir, proc_dir, _config())
    output = proc_dir / "cte_pathways.csv"
    assert output.exists(), [name for name, _payload in names_and_bytes]
    return pd.read_csv(output, dtype=str, keep_default_na=False)


def _fixture(name: str) -> bytes:
    return (FIXTURE_DIR / name).read_bytes()


def _rows(df: pd.DataFrame, year: str, school_id: str) -> pd.DataFrame:
    return df[(df["year"] == year) & (df["school_id"] == school_id)]


def _one(df: pd.DataFrame, year: str, school_id: str, metric: str) -> pd.Series:
    """The single aggregate row for this school, year, and metric."""
    matched = _rows(df, year, school_id)
    matched = matched[matched["metric"] == metric]
    assert len(matched) == 1, (year, school_id, metric, matched.to_dict("records"))
    return matched.iloc[0]


def _assert_unique_keys(df: pd.DataFrame) -> None:
    """(year, school_id, student_group, metric) is unique."""
    keys = ["year", "school_id", "student_group", "metric"]
    assert not df.duplicated(keys).any()


class TestCTEPathwaysETL:
    def setup_method(self):
        self.etl = CTEPathwaysETL("cte_pathways")

    def test_kyrc26_file_is_accepted(self):
        """include_kyrc26 is set, so the 2025-26 file is not skipped."""
        assert self.etl.include_kyrc26 is True
        assert self.etl.accepts_data_file(Path("KYRC26_CTE_Career_Pathways.csv"))
        assert not self.etl.accepts_data_file(Path("KYRC23_CTE_Career_Pathways.csv"))

    def test_registered_sources_are_the_permanent_files(self):
        """2020, 2022, and the 2018-19 workbook fill gaps. KYRC23 is absent.

        KYRC23 republishes 2022-23. The guessed OAA names were never
        registered. CAREER_PATHWAYS_17-18.xlsx is earlier than this series.
        """
        config = yaml.safe_load((ROOT / "config" / "kde_sources.yaml").read_text())
        names = []
        hosts = {}
        for entry in config["raw_directories"]["cte_pathways"]:
            if isinstance(entry, dict):
                names.append(entry["file"])
                hosts[entry["file"]] = entry.get("url")
            else:
                names.append(entry)
        assert names == [
            "KYRC24_CTE_Career_Pathways.csv",
            "KYRC25_CTE_Career_Pathways.csv",
            "KYRC26_CTE_Career_Pathways.csv",
            "career_pathways_2023.csv",
            "career_pathways_2022.csv",
            "career_pathways_2021.csv",
            "career_pathways_2020.csv",
            "CAREER_PATHWAYS_18-19.xlsx",
        ]
        assert hosts["KYRC25_CTE_Career_Pathways.csv"] == "kyrc25"
        assert hosts["KYRC26_CTE_Career_Pathways.csv"] == "kyrc26"
        assert "KYRC23_CTE_Career_Pathways.csv" not in names
        assert "CAREER_PATHWAYS_17-18.xlsx" not in names
        assert not any(name.lower().startswith("career_pathways_2024") for name in names)

    def test_kyrc26_real_rows(self, tmp_path):
        """State, district 606, Caldwell, and Bellevue from the KYRC26 file.

        Pathways Available is the pathway name. The state fixture has two
        pathways, so the pathway count is 2 rather than the file's 113.
        Agribusiness concentrators are the text ``2,002``. Corrections
        enrollment is ``*``, so the state enrollment total is suppressed
        and the partial sum is not stored. Caldwell's seven pathways are
        all published. Bellevue keeps the concentrator sum and suppresses
        enrollment because Accounting is ``*``. District 606 and Model
        High School publish the same two pathways.
        """
        df = _run(tmp_path, [("KYRC26_CTE_Career_Pathways.csv", _fixture("kyrc26_real_rows.csv"))])
        assert list(df.columns) == KPI_COLUMNS
        _assert_unique_keys(df)
        assert set(df["student_group"]) == {"All Students"}

        state_count = _one(df, "2026", "999000", "num_cte_pathways")
        assert state_count["value"] == "2.0"
        assert state_count["suppressed"] == "N"
        assert state_count["district"] == "All Districts"
        assert state_count["school_name"] == "---District Total---"
        state_enrollment = _one(df, "2026", "999000", "cte_pathway_enrollment")
        assert state_enrollment["value"] == ""
        assert state_enrollment["suppressed"] == "Y"
        # 2,002 + 15. The comma is stripped on StringDtype.
        assert _one(df, "2026", "999000", "cte_concentrator_count")["value"] == "2017.0"
        assert _one(df, "2026", "999000", "cte_concentrator_count")["suppressed"] == "N"
        assert _one(df, "2026", "999000", "cte_pathway_completer_count")["suppressed"] == "Y"
        assert "cte_pathway_completion_rate" not in set(df["metric"])

        caldwell = _one(df, "2026", "081010", "num_cte_pathways")
        assert caldwell["value"] == "7.0"
        assert caldwell["school_name"] == "Caldwell County High School"
        assert caldwell["district"] == "Caldwell County"
        assert _one(df, "2026", "081010", "cte_pathway_enrollment")["value"] == "556.0"
        assert _one(df, "2026", "081010", "cte_concentrator_count")["value"] == "413.0"
        assert _one(df, "2026", "081010", "cte_pathway_completer_count")["value"] == "143.0"
        assert set(_rows(df, "2026", "081010")["suppressed"]) == {"N"}

        bellevue_enrollment = _one(df, "2026", "032010", "cte_pathway_enrollment")
        assert bellevue_enrollment["value"] == ""
        assert bellevue_enrollment["suppressed"] == "Y"
        assert _one(df, "2026", "032010", "num_cte_pathways")["value"] == "2.0"
        assert _one(df, "2026", "032010", "cte_concentrator_count")["value"] == "41.0"
        assert _one(df, "2026", "032010", "cte_pathway_completer_count")["suppressed"] == "Y"

        for school_id in ("606000", "606460"):
            count = _one(df, "2026", school_id, "num_cte_pathways")
            assert count["value"] == "2.0"
            assert count["district"] == "Model Laboratory Schools at EKU"
            assert _one(df, "2026", school_id, "cte_pathway_enrollment")["value"] == "51.0"
            assert _one(df, "2026", school_id, "cte_pathway_enrollment")["suppressed"] == "N"
            assert _one(df, "2026", school_id, "cte_concentrator_count")["suppressed"] == "Y"
            assert _one(df, "2026", school_id, "cte_pathway_completer_count")["suppressed"] == "Y"
        assert _one(df, "2026", "606460", "num_cte_pathways")["school_name"] == "Model High School"
        assert _one(df, "2026", "606460", "num_cte_pathways")["school_type"] == "B1"

    def test_duplicate_program_area_counts_once(self, tmp_path):
        """2021 lists Design Engineering under Manufacturing and STEM.

        The two rows are the same school and the same counts. The pathway
        count is 1 and the concentrator total is 13, not 26. Enrollment
        and completers are ``*`` on both rows.
        """
        df = _run(tmp_path, [("career_pathways_2021.csv", _fixture("career_pathways_2021_trigg.csv"))])
        _assert_unique_keys(df)
        assert _one(df, "2021", "555070", "num_cte_pathways")["value"] == "1.0"
        assert _one(df, "2021", "555070", "cte_concentrator_count")["value"] == "13.0"
        assert _one(df, "2021", "555070", "cte_concentrator_count")["suppressed"] == "N"
        assert _one(df, "2021", "555070", "cte_pathway_enrollment")["suppressed"] == "Y"
        assert _one(df, "2021", "555070", "cte_pathway_completer_count")["suppressed"] == "Y"
        assert "cte_pathway_completion_rate" not in set(df["metric"])
        assert _one(df, "2021", "555070", "num_cte_pathways")["district"] == "Trigg County"

    def test_blank_row_is_not_offered(self, tmp_path):
        """A row with an empty enrollment, concentrator, and completer is skipped.

        No registered file has this row. The blank is not a star and does
        not add a pathway. Caldwell stays at 7 pathways.
        """
        blank = b"20252026,017,CALDWELL,081,Caldwell County,010,Caldwell County High School,081010,017081010,210084000166,WKEC,908,A1,Agricultural Education,Not Offered,,,"
        payload = _fixture("kyrc26_real_rows.csv").rstrip(b"\n") + b"\n" + blank + b"\n"
        df = _run(tmp_path, [("KYRC26_CTE_Career_Pathways.csv", payload)])
        assert _one(df, "2026", "081010", "num_cte_pathways")["value"] == "7.0"
        assert _one(df, "2026", "081010", "cte_pathway_enrollment")["value"] == "556.0"

    def test_published_zero_is_kept(self, tmp_path):
        """2019 publishes 0. Culinary enrollment 0 stays in the school sum.

        Preparatory students are not concentrators. The workbook column
        is not mapped, so there is no concentrator metric and no rate.
        """
        df = _run(tmp_path, [("CAREER_PATHWAYS_18-19.csv", _fixture("career_pathways_2019_ballard.csv"))])
        _assert_unique_keys(df)
        assert _one(df, "2019", "015010", "num_cte_pathways")["value"] == "3.0"
        assert _one(df, "2019", "015010", "cte_pathway_enrollment")["value"] == "115.0"
        assert _one(df, "2019", "015010", "cte_pathway_completer_count")["value"] == "6.0"
        assert set(_rows(df, "2019", "015010")["suppressed"]) == {"N"}
        assert "cte_concentrator_count" not in set(df["metric"])
        assert "cte_pathway_completion_rate" not in set(df["metric"])
        assert _one(df, "2019", "015010", "num_cte_pathways")["school_name"] == "Ballard Memorial High School"

    def test_stringdtype_comma_is_a_number(self):
        """pandas 3 dtype=str is StringDtype. ``2,546`` stays 2546.

        The same strip works for object text. A published 0 concentrator
        total does not produce a rate.
        """
        for dtype in ("string", object):
            frame = pd.DataFrame(
                {
                    "School Year": ["20252026", "20252026"],
                    "District Name": ["All Districts", "Caldwell County"],
                    "School Name": ["All Schools", "Caldwell County High School"],
                    "School Code": ["999000", "081010"],
                    "Pathways Available": ["Agribusiness Systems", "Accounting"],
                    "Active Enrollment": pd.Series(["2,546", "0"], dtype=dtype),
                    "Concentrator Students": pd.Series(["2,002", "0"], dtype=dtype),
                    "Pathway Completers": pd.Series(["352", "0"], dtype=dtype),
                }
            )
            cleaned = clean_numeric_with_commas(frame["Active Enrollment"])
            assert list(cleaned) == [2546, 0]
            etl = CTEPathwaysETL("cte_pathways")
            prepared = etl.standardize_missing_values(etl.normalize_column_names(frame))
            result = etl.convert_to_kpi_format(prepared, "KYRC26_CTE_Career_Pathways.csv")
            state = result[(result["school_id"] == "999000") & (result["metric"] == "cte_pathway_enrollment")]
            assert float(state.iloc[0]["value"]) == 2546.0
            assert state.iloc[0]["suppressed"] == "N"
            school = result[result["school_id"] == "081010"]
            concentrators = school[school["metric"] == "cte_concentrator_count"]
            assert float(concentrators.iloc[0]["value"]) == 0.0
            assert "cte_pathway_completion_rate" not in set(school["metric"])

    def test_leftover_kyrc23_does_not_duplicate_2023(self, tmp_path):
        """A leftover KYRC23 file is the same school year and must not load.

        The historical row is Accounting enrollment 10. The KYRC23 row is
        a different pathway. Loading both would make the pathway count 2.
        """
        historical = (
            b"SCHOOL YEAR,DISTRICT NAME,SCHOOL NAME,SCHOOL CODE,PROGRAM AREA,"
            b"PATHWAYS AVAILABLE,ACTIVE ENROLLMENTS COUNT,CONCENTRATOR STUDENTS COUNT,"
            b"PATHWAY COMPLETERS COUNT\n"
            b"20222023,Adair County,Adair County High School,001010,Agriculture,"
            b"Accounting,10,4,3\n"
        )
        kyrc23 = historical.replace(b"Accounting,10,4,3", b"Welding,8,2,1")
        df = _run(
            tmp_path,
            [
                ("career_pathways_2023.csv", historical),
                ("KYRC23_CTE_Career_Pathways.csv", kyrc23),
            ],
        )
        assert _one(df, "2023", "001010", "num_cte_pathways")["value"] == "1.0"
        assert _one(df, "2023", "001010", "cte_pathway_enrollment")["value"] == "10.0"
        assert set(df["source_file"]) == {"career_pathways_2023.csv"}

    def test_disagreeing_listings_suppress_the_total(self):
        """Two program areas with different counts are not added.

        No registered file does this. A partial sum would hide the conflict,
        so the measure is NA with suppressed Y and the pathway still counts
        once.
        """
        frame = pd.DataFrame(
            {
                "School Year": ["20202021", "20202021"],
                "District Name": ["Trigg County", "Trigg County"],
                "School Name": ["Trigg County High School", "Trigg County High School"],
                "School Code": ["555070", "555070"],
                "Program Area": ["Manufacturing", "STEM"],
                "Pathways Available": ["Design Engineering", "Design Engineering"],
                "Active Enrollment": ["10", "12"],
                "Concentrator Students": ["13", "13"],
                "Pathway Completers": ["*", "*"],
            }
        )
        etl = CTEPathwaysETL("cte_pathways")
        prepared = etl.standardize_missing_values(etl.normalize_column_names(frame))
        result = etl.convert_to_kpi_format(prepared, "career_pathways_2021.csv")
        count = result[result["metric"] == "num_cte_pathways"].iloc[0]
        assert float(count["value"]) == 1.0
        enrollment = result[result["metric"] == "cte_pathway_enrollment"].iloc[0]
        assert enrollment["suppressed"] == "Y"
        assert pd.isna(enrollment["value"])
        concentrators = result[result["metric"] == "cte_concentrator_count"].iloc[0]
        assert float(concentrators["value"]) == 13.0
        assert concentrators["suppressed"] == "N"

    def test_all_star_pathway_counts(self):
        """A pathway whose counts are all ``*`` is still a pathway.

        Each total is NA with suppressed Y. The count is suppressed N.
        """
        frame = pd.DataFrame(
            {
                "School Year": ["20252026"],
                "District Name": ["Bellevue Independent"],
                "School Name": ["Bellevue High School"],
                "School Code": ["032010"],
                "Pathways Available": ["Accounting"],
                "Active Enrollment": ["*"],
                "Concentrator Students": ["*"],
                "Pathway Completers": ["*"],
            }
        )
        etl = CTEPathwaysETL("cte_pathways")
        prepared = etl.standardize_missing_values(etl.normalize_column_names(frame))
        result = etl.convert_to_kpi_format(prepared, "KYRC26_CTE_Career_Pathways.csv")
        count = result[result["metric"] == "num_cte_pathways"].iloc[0]
        assert float(count["value"]) == 1.0
        assert count["suppressed"] == "N"
        suppressed = result[result["metric"] != "num_cte_pathways"]
        assert set(suppressed["suppressed"]) == {"Y"}
        assert suppressed["value"].isna().all()
        assert set(suppressed["metric"]) == {
            "cte_pathway_enrollment",
            "cte_concentrator_count",
            "cte_pathway_completer_count",
        }
        assert "cte_pathway_completion_rate" not in set(result["metric"])

    def test_file_without_measure_columns_writes_nothing(self, tmp_path):
        """A stray 2017-18 file has no mapped measure columns.

        ``_row_is_blank`` is true when no measure column is present, so
        the file does not add a 2018 row under a blank pathway name.
        """
        stray = (
            b"SCH_YEAR,DIST_NAME,SCH_NAME,SCH_CD,SOME_PATHWAY_LABEL\n"
            b"20172018,Adair County,Adair County High School,001010,Accounting\n"
        )
        df = _run(
            tmp_path,
            [
                ("CAREER_PATHWAYS_17-18.csv", stray),
                ("CAREER_PATHWAYS_18-19.csv", _fixture("career_pathways_2019_ballard.csv")),
            ],
        )
        assert "2018" not in set(df["year"])
        assert _one(df, "2019", "015010", "num_cte_pathways")["value"] == "3.0"
        assert self.etl._row_is_blank(pd.Series({"school_year": "20172018", "school_code": "001010"}))
        blank_measures = pd.Series(
            {
                "active_enrollment": "",
                "concentrator_students": "",
                "pathway_completers": "",
            }
        )
        assert self.etl._row_is_blank(blank_measures)

    def test_pathway_name_whitespace_counts_once(self):
        """Leading and trailing spaces are the same pathway name.

        The two rows agree, so the pathway counts once and the enrollment
        is 17, not 34.
        """
        frame = pd.DataFrame(
            {
                "School Year": ["20252026", "20252026"],
                "District Name": ["Caldwell County", "Caldwell County"],
                "School Name": ["Caldwell County High School", "Caldwell County High School"],
                "School Code": ["081010", "081010"],
                "Program Area": ["Business", "Business"],
                "Pathways Available": ["Accounting", " Accounting "],
                "Active Enrollment": ["17", "17"],
                "Concentrator Students": ["17", "17"],
                "Pathway Completers": ["16", "16"],
            }
        )
        prepared = self.etl.standardize_missing_values(self.etl.normalize_column_names(frame))
        result = self.etl.convert_to_kpi_format(prepared, "KYRC26_CTE_Career_Pathways.csv")
        _assert_unique_keys(result)
        count = result[result["metric"] == "num_cte_pathways"].iloc[0]
        assert float(count["value"]) == 1.0
        enrollment = result[result["metric"] == "cte_pathway_enrollment"].iloc[0]
        assert float(enrollment["value"]) == 17.0
        assert enrollment["suppressed"] == "N"

    def test_output_has_no_duplicate_keys(self, tmp_path):
        """Three source files loaded together still have one row per key."""
        df = _run(
            tmp_path,
            [
                ("KYRC26_CTE_Career_Pathways.csv", _fixture("kyrc26_real_rows.csv")),
                ("career_pathways_2021.csv", _fixture("career_pathways_2021_trigg.csv")),
                ("CAREER_PATHWAYS_18-19.csv", _fixture("career_pathways_2019_ballard.csv")),
            ],
        )
        _assert_unique_keys(df)
        assert set(df["year"]) == {"2019", "2021", "2026"}
        assert _one(df, "2026", "081010", "num_cte_pathways")["value"] == "7.0"
        assert _one(df, "2021", "555070", "num_cte_pathways")["value"] == "1.0"
        assert _one(df, "2019", "015010", "num_cte_pathways")["value"] == "3.0"
