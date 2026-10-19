"""Tests for the career readiness indicators ETL."""
import io
from pathlib import Path

import pandas as pd
import yaml

from etl.constants import KPI_COLUMNS
from etl.career_readiness_indicators import (
    CareerReadinessIndicatorsETL,
    clean_numeric_with_commas,
    transform,
)

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "career_readiness_indicators"

# Published counts. Not co-op, internship, AP, or a transition-readiness rate.
# Those four names were mappings with no source column.
_PHANTOM_METRICS = {
    "career_readiness_cooperative_education_count",
    "career_readiness_internship_count",
    "career_readiness_advanced_placement_count",
    "career_readiness_transition_readiness_count",
}


def _config():
    """Production mappings.yaml entry. This source has no dtype cast."""
    loaded = yaml.safe_load((ROOT / "config" / "mappings.yaml").read_text())
    return loaded["sources"]["career_readiness_indicators"]


def _run(tmp_path: Path, names_and_bytes: list) -> pd.DataFrame:
    raw_dir = tmp_path / "raw"
    source_dir = raw_dir / "career_readiness_indicators"
    source_dir.mkdir(parents=True)
    proc_dir = tmp_path / "processed"
    proc_dir.mkdir()
    for name, payload in names_and_bytes:
        (source_dir / name).write_bytes(payload)
    transform(raw_dir, proc_dir, _config())
    output = proc_dir / "career_readiness_indicators.csv"
    assert output.exists(), [name for name, _payload in names_and_bytes]
    return pd.read_csv(output, dtype=str, keep_default_na=False)


def _fixture(name: str) -> bytes:
    return (FIXTURE_DIR / name).read_bytes()


def _rows(df: pd.DataFrame, year: str, school_id: str) -> pd.DataFrame:
    return df[(df["year"] == year) & (df["school_id"] == school_id)]


def _one(df: pd.DataFrame, year: str, school_id: str, metric: str) -> pd.Series:
    """The single row for this school, year, and metric."""
    matched = _rows(df, year, school_id)
    matched = matched[matched["metric"] == metric]
    assert len(matched) == 1, (year, school_id, metric, matched.to_dict("records"))
    return matched.iloc[0]


def _assert_unique_keys(df: pd.DataFrame) -> None:
    """(year, school_id, student_group, metric) is unique."""
    keys = ["year", "school_id", "student_group", "metric"]
    assert not df.duplicated(keys).any()


def _assert_number(df: pd.DataFrame, year: str, school_id: str, metric: str, value: str) -> None:
    row = _one(df, year, school_id, metric)
    assert row["value"] == value
    assert row["suppressed"] == "N"


def _assert_star(df: pd.DataFrame, year: str, school_id: str, metric: str) -> None:
    row = _one(df, year, school_id, metric)
    assert row["value"] == ""
    assert row["suppressed"] == "Y"


class TestCareerReadinessIndicatorsETL:
    def setup_method(self):
        self.etl = CareerReadinessIndicatorsETL("career_readiness_indicators")

    def test_kyrc26_file_is_accepted(self):
        """include_kyrc26 is set, so the 2025-26 file is not skipped."""
        assert self.etl.include_kyrc26 is True
        assert self.etl.accepts_data_file(Path("KYRC26_CTE_Career_Readiness_Indicators.csv"))
        assert not self.etl.accepts_data_file(Path("KYRC23_CTE_Career_Readiness_Indicators.csv"))
        assert not self.etl.accepts_data_file(Path("CTE_TRANSITION_READINESS_17-18.xlsx"))
        assert not self.etl.accepts_data_file(Path("Career_Readiness_Indicators_2025.CSV"))

    def test_registered_sources_are_the_permanent_files(self):
        """2022 and the 2018-19 workbook fill gaps. KYRC23 is absent.

        KYRC23 republishes 2022-23. The 2017-18 workbook is earlier than
        this series. The guessed OAA names were never registered.
        """
        config = yaml.safe_load((ROOT / "config" / "kde_sources.yaml").read_text())
        names = []
        hosts = {}
        for entry in config["raw_directories"]["career_readiness_indicators"]:
            if isinstance(entry, dict):
                names.append(entry["file"])
                hosts[entry["file"]] = entry.get("url")
            else:
                names.append(entry)
        assert names == [
            "KYRC24_CTE_Career_Readiness_Indicators.csv",
            "KYRC25_CTE_Career_Readiness_Indicators.csv",
            "KYRC26_CTE_Career_Readiness_Indicators.csv",
            "career_readiness_indicators_2023.csv",
            "career_readiness_indicators_2022.csv",
            "CTE_TRANSITION_READINESS_18-19.xlsx",
        ]
        assert hosts["KYRC25_CTE_Career_Readiness_Indicators.csv"] == "kyrc25"
        assert hosts["KYRC26_CTE_Career_Readiness_Indicators.csv"] == "kyrc26"
        assert "KYRC23_CTE_Career_Readiness_Indicators.csv" not in names
        assert "CTE_TRANSITION_READINESS_17-18.xlsx" not in names
        assert "career_readiness_indicators_2021.csv" not in names
        assert "career_readiness_indicators_2020.csv" not in names
        assert not any("career_studies" in name for name in names)

    def test_kyrc26_real_rows(self, tmp_path):
        """State, Adair, Caldwell, Bellevue, Bryan Station, and Model.

        Each column is the count KDE published. A star on apprenticeship
        does not suppress dual credit. There is no pathway name to count
        and no rate. District 606 publishes the starred Model row. Year
        is 2026 and the state district is All Districts.
        """
        df = _run(tmp_path, [("KYRC26_CTE_Career_Readiness_Indicators.csv", _fixture("kyrc26_real_rows.csv"))])
        assert list(df.columns) == KPI_COLUMNS
        _assert_unique_keys(df)
        assert set(df["student_group"]) == {"All Students"}
        assert set(df["year"]) == {"2026"}
        assert _PHANTOM_METRICS.isdisjoint(set(df["metric"]))
        assert not df["metric"].str.contains("rate").any()
        assert "career_readiness_exceptional_work_experience_count" not in set(df["metric"])

        state = _one(df, "2026", "999000", "career_readiness_total_graduates_count")
        assert state["district"] == "All Districts"
        assert state["school_name"] == "---District Total---"
        assert state["value"] == "49751.0"
        assert state["suppressed"] == "N"
        _assert_number(df, "2026", "999000", "career_readiness_industry_certification_count", "18286.0")
        _assert_number(df, "2026", "999000", "career_readiness_cte_end_of_program_assessment_count", "12757.0")
        _assert_number(df, "2026", "999000", "career_readiness_apprenticeship_count", "452.0")
        _assert_number(df, "2026", "999000", "career_readiness_dual_credit_cte_count", "18267.0")
        _assert_number(df, "2026", "999000", "career_readiness_more_than_one_indicator_count", "16262.0")
        _assert_number(df, "2026", "999000", "career_readiness_no_indicator_count", "15898.0")
        _assert_number(df, "2026", "999000", "career_readiness_work_based_learning_count", "5115.0")
        # Eight published columns. They are not added into one total.
        assert len(_rows(df, "2026", "999000")) == 8

        _assert_number(df, "2026", "001000", "career_readiness_industry_certification_count", "107.0")
        _assert_number(df, "2026", "001000", "career_readiness_total_graduates_count", "199.0")
        _assert_star(df, "2026", "001000", "career_readiness_work_based_learning_count")
        assert _one(df, "2026", "001000", "career_readiness_industry_certification_count")["district"] == "Adair County"
        assert _one(df, "2026", "001000", "career_readiness_industry_certification_count")["school_name"] == "---District Total---"

        _assert_number(df, "2026", "001010", "career_readiness_total_graduates_count", "198.0")
        _assert_number(df, "2026", "001010", "career_readiness_dual_credit_cte_count", "128.0")
        _assert_star(df, "2026", "001010", "career_readiness_work_based_learning_count")
        assert _one(df, "2026", "001010", "career_readiness_total_graduates_count")["school_name"] == "Adair County High School"

        caldwell = _rows(df, "2026", "081010")
        assert set(caldwell["suppressed"]) == {"N"}
        assert len(caldwell) == 8
        _assert_number(df, "2026", "081010", "career_readiness_total_graduates_count", "140.0")
        _assert_number(df, "2026", "081010", "career_readiness_industry_certification_count", "65.0")
        _assert_number(df, "2026", "081010", "career_readiness_cte_end_of_program_assessment_count", "58.0")
        _assert_number(df, "2026", "081010", "career_readiness_apprenticeship_count", "6.0")
        _assert_number(df, "2026", "081010", "career_readiness_dual_credit_cte_count", "35.0")
        _assert_number(df, "2026", "081010", "career_readiness_more_than_one_indicator_count", "60.0")
        _assert_number(df, "2026", "081010", "career_readiness_no_indicator_count", "34.0")
        _assert_number(df, "2026", "081010", "career_readiness_work_based_learning_count", "34.0")

        # A star suppresses that column only.
        _assert_star(df, "2026", "032010", "career_readiness_apprenticeship_count")
        _assert_star(df, "2026", "032010", "career_readiness_work_based_learning_count")
        _assert_number(df, "2026", "032010", "career_readiness_industry_certification_count", "7.0")
        _assert_number(df, "2026", "032010", "career_readiness_dual_credit_cte_count", "6.0")
        _assert_number(df, "2026", "032010", "career_readiness_total_graduates_count", "51.0")

        _assert_star(df, "2026", "165170", "career_readiness_apprenticeship_count")
        _assert_number(df, "2026", "165170", "career_readiness_industry_certification_count", "236.0")
        _assert_number(df, "2026", "165170", "career_readiness_dual_credit_cte_count", "200.0")
        assert _one(df, "2026", "165170", "career_readiness_industry_certification_count")["school_name"] == "Bryan Station High School"

        _assert_number(df, "2026", "606460", "career_readiness_dual_credit_cte_count", "5.0")
        _assert_number(df, "2026", "606460", "career_readiness_no_indicator_count", "50.0")
        _assert_number(df, "2026", "606460", "career_readiness_total_graduates_count", "55.0")
        _assert_star(df, "2026", "606460", "career_readiness_industry_certification_count")
        _assert_star(df, "2026", "606460", "career_readiness_cte_end_of_program_assessment_count")
        _assert_star(df, "2026", "606460", "career_readiness_apprenticeship_count")
        _assert_star(df, "2026", "606460", "career_readiness_more_than_one_indicator_count")
        _assert_star(df, "2026", "606460", "career_readiness_work_based_learning_count")
        assert _one(df, "2026", "606460", "career_readiness_dual_credit_cte_count")["district"] == "Model Laboratory Schools at EKU"

    def test_prior_year_published_values(self, tmp_path):
        """2022-2025 anchors. Published numbers stay. Blanks are not stored as 0.

        2023 industry certifications use the plural header. Breathitt's
        district row says school year 20022023; the last four digits are
        2023 and the school id stays 061. KYRC24 keeps a published 0.
        KYRC25 stores a star as suppressed Y.
        """
        df = _run(
            tmp_path,
            [
                ("career_readiness_indicators_2022.csv", _fixture("career_readiness_2022_rows.csv")),
                ("career_readiness_indicators_2023.csv", _fixture("career_readiness_2023_rows.csv")),
                ("KYRC24_CTE_Career_Readiness_Indicators.csv", _fixture("kyrc24_real_rows.csv")),
                ("KYRC25_CTE_Career_Readiness_Indicators.csv", _fixture("kyrc25_real_rows.csv")),
            ],
        )
        _assert_unique_keys(df)
        assert _PHANTOM_METRICS.isdisjoint(set(df["metric"]))

        _assert_number(df, "2022", "999", "career_readiness_total_graduates_count", "47061.0")
        _assert_number(df, "2022", "999", "career_readiness_industry_certification_count", "5594.0")
        _assert_number(df, "2022", "999", "career_readiness_cte_end_of_program_assessment_count", "5888.0")
        _assert_number(df, "2022", "999", "career_readiness_dual_credit_cte_count", "3991.0")
        _assert_number(df, "2022", "999", "career_readiness_apprenticeship_count", "18.0")
        _assert_number(df, "2022", "999", "career_readiness_more_than_one_indicator_count", "6486.0")
        _assert_number(df, "2022", "999", "career_readiness_no_indicator_count", "25084.0")
        assert _one(df, "2022", "999", "career_readiness_total_graduates_count")["district"] == "State"
        assert _one(df, "2022", "999", "career_readiness_total_graduates_count")["school_name"] == "---State Total---"
        augusta = _rows(df, "2022", "013011")
        assert "career_readiness_industry_certification_count" not in set(augusta["metric"])
        assert "career_readiness_exceptional_work_experience_count" not in set(augusta["metric"])
        assert "career_readiness_apprenticeship_count" not in set(augusta["metric"])
        _assert_number(df, "2022", "013011", "career_readiness_total_graduates_count", "24.0")
        _assert_number(df, "2022", "013011", "career_readiness_dual_credit_cte_count", "8.0")
        _assert_number(df, "2022", "013011", "career_readiness_more_than_one_indicator_count", "2.0")
        _assert_number(df, "2022", "013011", "career_readiness_no_indicator_count", "14.0")
        assert "0.0" not in set(augusta["value"])

        _assert_number(df, "2023", "999", "career_readiness_industry_certification_count", "14055.0")
        _assert_number(df, "2023", "999", "career_readiness_dual_credit_cte_count", "12097.0")
        _assert_number(df, "2023", "999", "career_readiness_cte_end_of_program_assessment_count", "11771.0")
        _assert_number(df, "2023", "999", "career_readiness_apprenticeship_count", "261.0")
        _assert_number(df, "2023", "999", "career_readiness_total_graduates_count", "44599.0")
        _assert_number(df, "2023", "999", "career_readiness_more_than_one_indicator_count", "6181.0")
        _assert_number(df, "2023", "999", "career_readiness_no_indicator_count", "21345.0")
        assert "career_readiness_exceptional_work_experience_count" not in set(_rows(df, "2023", "999")["metric"])
        # The bad school-year cell is 20022023. The id is not padded.
        _assert_number(df, "2023", "061", "career_readiness_total_graduates_count", "115.0")
        _assert_number(df, "2023", "061", "career_readiness_industry_certification_count", "59.0")
        assert "career_readiness_apprenticeship_count" not in set(_rows(df, "2023", "061")["metric"])
        assert "061000" not in set(df["school_id"])
        assert "999000" not in set(df.loc[df["year"] == "2023", "school_id"])

        _assert_number(df, "2024", "999000", "career_readiness_industry_certification_count", "13548.0")
        _assert_number(df, "2024", "999000", "career_readiness_dual_credit_cte_count", "13527.0")
        _assert_number(df, "2024", "999000", "career_readiness_apprenticeship_count", "289.0")
        _assert_number(df, "2024", "999000", "career_readiness_cte_end_of_program_assessment_count", "12650.0")
        _assert_number(df, "2024", "999000", "career_readiness_work_based_learning_count", "2163.0")
        _assert_number(df, "2024", "999000", "career_readiness_no_indicator_count", "19736.0")
        assert "career_readiness_total_graduates_count" not in set(df.loc[df["year"] == "2024", "metric"])
        _assert_number(df, "2024", "880100", "career_readiness_industry_certification_count", "0.0")
        _assert_number(df, "2024", "880100", "career_readiness_dual_credit_cte_count", "1.0")
        _assert_number(df, "2024", "880100", "career_readiness_no_indicator_count", "5.0")
        _assert_number(df, "2024", "880101", "career_readiness_no_indicator_count", "17.0")
        _assert_number(df, "2024", "880101", "career_readiness_industry_certification_count", "1.0")

        _assert_number(df, "2025", "999000", "career_readiness_industry_certification_count", "16334.0")
        _assert_number(df, "2025", "999000", "career_readiness_total_graduates_count", "49663.0")
        _assert_number(df, "2025", "999000", "career_readiness_dual_credit_cte_count", "16201.0")
        _assert_number(df, "2025", "999000", "career_readiness_work_based_learning_count", "4403.0")
        assert _one(df, "2025", "999000", "career_readiness_total_graduates_count")["district"] == "All Districts"
        starred = _rows(df, "2025", "880000")
        assert len(starred) == 8
        assert set(starred["suppressed"]) == {"Y"}
        assert set(starred["value"]) == {""}
        assert set(starred["district"]) == {"Kentucky National Guard Youth ChalleNGe Academies"}

    def test_2019_workbook_has_no_demographic_column(self, tmp_path):
        """The 2018-19 workbook has no DEMO or DGROUP column.

        School id 999 is not padded. Exceptional work experience is stored
        on its own metric. There is no end-of-program column. A blank
        apprenticeship is not stored as 0. More than one indicator stays 1.
        """
        df = _run(tmp_path, [("CTE_TRANSITION_READINESS_18-19.csv", _fixture("career_readiness_2019_rows.csv"))])
        _assert_unique_keys(df)
        assert set(df["student_group"]) == {"All Students"}
        assert set(df["year"]) == {"2019"}
        assert "999000" not in set(df["school_id"])
        assert "301000" not in set(df["school_id"])
        state = _one(df, "2019", "999", "career_readiness_total_graduates_count")
        assert state["district"] == "State"
        assert state["school_name"] == "---State Total---"
        assert state["value"] == "46602.0"
        _assert_number(df, "2019", "999", "career_readiness_industry_certification_count", "6545.0")
        _assert_number(df, "2019", "999", "career_readiness_dual_credit_cte_count", "7660.0")
        _assert_number(df, "2019", "999", "career_readiness_apprenticeship_count", "703.0")
        _assert_number(df, "2019", "999", "career_readiness_exceptional_work_experience_count", "68.0")
        _assert_number(df, "2019", "999", "career_readiness_more_than_one_indicator_count", "1.0")
        _assert_number(df, "2019", "999", "career_readiness_no_indicator_count", "3466.0")
        assert "career_readiness_cte_end_of_program_assessment_count" not in set(df["metric"])
        assert "career_readiness_work_based_learning_count" not in set(df["metric"])
        adair = _rows(df, "2019", "001010")
        assert "career_readiness_exceptional_work_experience_count" not in set(adair["metric"])
        assert "career_readiness_more_than_one_indicator_count" not in set(adair["metric"])
        _assert_number(df, "2019", "001010", "career_readiness_apprenticeship_count", "7.0")
        _assert_number(df, "2019", "001010", "career_readiness_industry_certification_count", "45.0")
        _assert_number(df, "2019", "301410", "career_readiness_more_than_one_indicator_count", "1.0")
        assert "career_readiness_apprenticeship_count" not in set(_rows(df, "2019", "301410")["metric"])
        _assert_number(df, "2019", "301", "career_readiness_more_than_one_indicator_count", "1.0")
        assert _one(df, "2019", "301", "career_readiness_total_graduates_count")["school_name"] == "---District Total---"

    def test_blank_row_is_skipped_and_blank_cell_is_not_zero(self, tmp_path):
        """A row with every measure empty is not offered.

        A blank beside a published count is omitted. It is not stored as 0.
        """
        header = (
            "School Year,District Name,School Name,School Code,Total Graduates,"
            "Industry Certification,Apprenticeship\n"
        )
        body = header + "20252026,Adair County,Adair County High School,001010,10,4,\n"
        body += "20252026,Nowhere,Empty High School,000001,,,\n"
        df = _run(tmp_path, [("KYRC26_CTE_Career_Readiness_Indicators.csv", body.encode())])
        assert set(df["school_id"]) == {"001010"}
        _assert_number(df, "2026", "001010", "career_readiness_total_graduates_count", "10.0")
        _assert_number(df, "2026", "001010", "career_readiness_industry_certification_count", "4.0")
        assert "career_readiness_apprenticeship_count" not in set(df["metric"])

    def test_stringdtype_comma_is_a_count(self):
        """A thousands comma survives StringDtype and object text.

        No registered career-readiness file contains a comma. The cleaner
        does not check ``dtype == object``, and a star is classified
        before the cell is coerced. ``bool(pd.NA)`` is not evaluated.
        """
        series = pd.Series(["1,234", pd.NA, "0", "*"], dtype="string")
        cleaned = clean_numeric_with_commas(series)
        assert float(cleaned.iloc[0]) == 1234.0
        assert pd.isna(cleaned.iloc[1])
        assert float(cleaned.iloc[2]) == 0.0
        assert pd.isna(cleaned.iloc[3])

        frame = pd.DataFrame(
            {
                "School Year": ["20252026"],
                "District Name": ["All Districts"],
                "School Name": ["All Schools"],
                "School Code": ["999000"],
                "Industry Certification": pd.Series(["1,234"], dtype="string"),
                "Apprenticeship": pd.Series(["*"], dtype="string"),
                "Dual Credit": pd.Series(["0"], dtype="string"),
            }
        )
        prepared = self.etl.standardize_missing_values(self.etl.normalize_column_names(frame))
        result = self.etl.convert_to_kpi_format(prepared, "KYRC26_CTE_Career_Readiness_Indicators.csv")
        industry = result[result["metric"] == "career_readiness_industry_certification_count"].iloc[0]
        assert float(industry["value"]) == 1234.0
        assert industry["suppressed"] == "N"
        apprenticeship = result[result["metric"] == "career_readiness_apprenticeship_count"].iloc[0]
        assert apprenticeship["suppressed"] == "Y"
        assert pd.isna(apprenticeship["value"])
        dual = result[result["metric"] == "career_readiness_dual_credit_cte_count"].iloc[0]
        assert float(dual["value"]) == 0.0
        assert dual["suppressed"] == "N"

    def test_kyrc23_republish_is_not_loaded(self, tmp_path):
        """KYRC23 matches 2022-23 and must not write that year again."""
        historical = _fixture("career_readiness_2023_rows.csv")
        republish = (
            "School Year,District Name,School Name,School Code,Industry Certification\n"
            "20222023,All Districts,All Schools,999000,99999\n"
        ).encode()
        df = _run(
            tmp_path,
            [
                ("career_readiness_indicators_2023.csv", historical),
                ("KYRC23_CTE_Career_Readiness_Indicators.csv", republish),
            ],
        )
        assert set(df["source_file"]) == {"career_readiness_indicators_2023.csv"}
        _assert_number(df, "2023", "999", "career_readiness_industry_certification_count", "14055.0")
        assert "999000" not in set(df["school_id"])

    def test_leftover_temporary_and_2017_files_write_nothing(self, tmp_path):
        """A leftover temp name or the 2017-18 workbook does not add a year.

        The 2017-18 sheet uses IND_CERTS_CNT, which this module maps. The
        filename skip is what keeps 2018 out. The guessed OAA name would
        otherwise write 2025 beside the permanent KYRC25 file.
        """
        workbook = pd.DataFrame(
            {
                "SCH_YEAR": ["20172018"],
                "DIST_NAME": ["State"],
                "SCH_NAME": ["---State Total---"],
                "SCH_CD": ["999"],
                "IND_CERTS_CNT": ["9999"],
            }
        )
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            workbook.to_excel(writer, sheet_name="DATA", index=False)
        leftover = (
            "School Year,District Name,School Name,School Code,Industry Certification\n"
            "20242025,All Districts,All Schools,999000,1\n"
        ).encode()
        df = _run(
            tmp_path,
            [
                ("KYRC25_CTE_Career_Readiness_Indicators.csv", _fixture("kyrc25_real_rows.csv")),
                ("Career_Readiness_Indicators_2025.CSV", leftover),
                ("CTE_TRANSITION_READINESS_17-18.xlsx", buffer.getvalue()),
            ],
        )
        assert set(df["year"]) == {"2025"}
        assert set(df["source_file"]) == {"KYRC25_CTE_Career_Readiness_Indicators.csv"}
        _assert_number(df, "2025", "999000", "career_readiness_industry_certification_count", "16334.0")

    def test_row_without_measure_columns_is_blank(self, tmp_path):
        """Pathways Available is not a measure in this file.

        A file with only an identity column and a pathway name writes no
        KPI row. A blank measure row next to a published row is skipped.
        """
        names_only = (
            "School Year,District Name,School Name,School Code,Pathways Available\n"
            "20252026,Adair County,Adair County High School,001010,Accounting\n"
        ).encode()
        df = _run(
            tmp_path,
            [
                ("KYRC26_CTE_Career_Readiness_Indicators.csv", _fixture("kyrc26_real_rows.csv")),
                ("pathways_available_only.csv", names_only),
            ],
        )
        _assert_unique_keys(df)
        assert set(df["source_file"]) == {"KYRC26_CTE_Career_Readiness_Indicators.csv"}
        assert "Accounting" not in set(df["metric"])
        _assert_number(df, "2026", "999000", "career_readiness_total_graduates_count", "49751.0")

    def test_disagreeing_school_listings_are_not_added(self):
        """Two rows for one school are not summed.

        No registered file repeats a school code. The industry cells
        disagree, so that metric is suppressed. Dual credit agrees and
        stays 5. The partial sum 22 is not stored.
        """
        frame = pd.DataFrame(
            {
                "School Year": ["20252026", "20252026"],
                "District Name": ["Caldwell County", "Caldwell County"],
                "School Name": ["Caldwell County High School", "Caldwell County High School"],
                "School Code": ["081010", "081010"],
                "Industry Certification": ["10", "12"],
                "Dual Credit": ["5", "5"],
            }
        )
        prepared = self.etl.standardize_missing_values(self.etl.normalize_column_names(frame))
        result = self.etl.convert_to_kpi_format(prepared, "KYRC26_CTE_Career_Readiness_Indicators.csv")
        keys = ["year", "school_id", "student_group", "metric"]
        assert not result.duplicated(keys).any()
        industry = result[result["metric"] == "career_readiness_industry_certification_count"].iloc[0]
        assert industry["suppressed"] == "Y"
        assert pd.isna(industry["value"])
        dual = result[result["metric"] == "career_readiness_dual_credit_cte_count"].iloc[0]
        assert float(dual["value"]) == 5.0
        assert dual["suppressed"] == "N"

    def test_output_has_no_duplicate_keys(self, tmp_path):
        """Files for several years still have one row per key."""
        df = _run(
            tmp_path,
            [
                ("KYRC26_CTE_Career_Readiness_Indicators.csv", _fixture("kyrc26_real_rows.csv")),
                ("KYRC25_CTE_Career_Readiness_Indicators.csv", _fixture("kyrc25_real_rows.csv")),
                ("KYRC24_CTE_Career_Readiness_Indicators.csv", _fixture("kyrc24_real_rows.csv")),
                ("career_readiness_indicators_2023.csv", _fixture("career_readiness_2023_rows.csv")),
                ("career_readiness_indicators_2022.csv", _fixture("career_readiness_2022_rows.csv")),
                ("CTE_TRANSITION_READINESS_18-19.csv", _fixture("career_readiness_2019_rows.csv")),
            ],
        )
        _assert_unique_keys(df)
        assert set(df["year"]) == {"2019", "2022", "2023", "2024", "2025", "2026"}
        _assert_number(df, "2026", "999000", "career_readiness_total_graduates_count", "49751.0")
        _assert_number(df, "2025", "999000", "career_readiness_industry_certification_count", "16334.0")
        _assert_number(df, "2024", "880100", "career_readiness_industry_certification_count", "0.0")
        _assert_number(df, "2019", "999", "career_readiness_exceptional_work_experience_count", "68.0")
        assert _PHANTOM_METRICS.isdisjoint(set(df["metric"]))
