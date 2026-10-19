"""End-to-end integration tests for Kindergarten Readiness ETL"""
import shutil
import tempfile
from pathlib import Path

import pandas as pd
import yaml

from etl.kindergarten_readiness import transform


class TestKindergartenReadinessEndToEnd:
    def setup_method(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.temp_dir / "raw"
        self.proc_dir = self.temp_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        self.sample_dir = self.raw_dir / "kindergarten_readiness"
        self.sample_dir.mkdir(parents=True)

    def teardown_method(self):
        shutil.rmtree(self.temp_dir)

    def create_counts_data(self):
        return pd.DataFrame({
            "School Year": ["20232024", "20232024"],
            "District Name": ["Fayette County", "Fayette County"],
            "School Code": ["1001", "1001"],
            "School Name": ["Test School", "Test School"],
            "Demographic": ["All Students", "All Students"],
            "Ready With Interventions": [10, 8],
            "Ready": [20, 15],
            "Ready With Enrichments": [5, 4],
            "Total Ready": [35, 27],
            "Suppressed": ["N", "N"],
            "Prior Setting": ["All Students", "Child Care"],
        })

    def create_percent_data(self):
        return pd.DataFrame({
            "SCHOOL YEAR": ["20212022"],
            "DISTRICT NAME": ["Fayette County"],
            "SCHOOL CODE": ["1001"],
            "SCHOOL NAME": ["Test School"],
            "DEMOGRAPHIC": ["All Students"],
            "TOTAL PERCENT READY": [55.0],
            "NUMBER TESTED": [100],
            "SUPPRESSED": ["N"],
            "Prior Setting": ["All Students"],
        })

    def test_end_to_end_single_file(self):
        df = self.create_counts_data()
        df.to_csv(self.sample_dir / "counts.csv", index=False)
        transform(self.raw_dir, self.proc_dir, {"derive": {"processing_date": "2025-07-20"}})
        out_file = self.proc_dir / "kindergarten_readiness.csv"
        assert out_file.exists()
        result = pd.read_csv(out_file)
        assert len(result) == 5
        assert set(result["metric"].unique()) == {
            "kindergarten_ready_with_interventions_rate",
            "kindergarten_ready_rate",
            "kindergarten_ready_with_enrichments_rate",
            "kindergarten_readiness_rate",
            "kindergarten_child_care_rate",
        }

    def test_end_to_end_multiple_files(self):
        self.create_counts_data().to_csv(self.sample_dir / "counts.csv", index=False)
        self.create_percent_data().to_csv(self.sample_dir / "percent.csv", index=False)
        transform(self.raw_dir, self.proc_dir, {"derive": {"processing_date": "2025-07-20"}})
        out_file = self.proc_dir / "kindergarten_readiness.csv"
        assert out_file.exists()
        result = pd.read_csv(out_file)
        assert len(result) == 8
        metrics = set(result["metric"].unique())
        expected = {
            "kindergarten_ready_with_interventions_rate",
            "kindergarten_ready_rate",
            "kindergarten_ready_with_enrichments_rate",
            "kindergarten_readiness_count",
            "kindergarten_readiness_total",
            "kindergarten_readiness_rate",
            "kindergarten_child_care_rate",
        }
        assert metrics == expected


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "kindergarten_readiness"

def _run_fixtures(tmp_path: Path, filenames: list[str]) -> pd.DataFrame:
    """Copy committed real-row excerpts into a raw directory and transform them."""
    raw_dir = tmp_path / "raw"
    source_dir = raw_dir / "kindergarten_readiness"
    source_dir.mkdir(parents=True)
    proc_dir = tmp_path / "processed"
    proc_dir.mkdir()
    for name in filenames:
        target = source_dir / name
        target.write_bytes((FIXTURE_DIR / name).read_bytes())
    transform(raw_dir, proc_dir, {"derive": {"processing_date": "2026-10-19"}})
    output = proc_dir / "kindergarten_readiness.csv"
    assert output.exists(), filenames
    return pd.read_csv(output, dtype=str)


def _metric_rows(
    df: pd.DataFrame, year: str, school_id: str, metric: str, student_group: str
) -> pd.DataFrame:
    return df[
        (df["year"] == year)
        & (df["school_id"] == school_id)
        & (df["metric"] == metric)
        & (df["student_group"] == student_group)
    ]


def _metric_value(
    df: pd.DataFrame, year: str, school_id: str, metric: str, student_group: str = "All Students"
) -> pd.Series:
    matches = _metric_rows(df, year, school_id, metric, student_group)
    assert len(matches) == 1, (year, school_id, metric, student_group, len(matches))
    return matches.iloc[0]


def _assert_composite(df: pd.DataFrame, year: str, school_id: str, published: tuple[str, str, str, str]) -> None:
    """Each rate is the published percent. Composites do not emit _count or _total."""
    interventions, ready, enrichments, total = published
    expected = {
        "kindergarten_ready_with_interventions_rate": interventions,
        "kindergarten_ready_rate": ready,
        "kindergarten_ready_with_enrichments_rate": enrichments,
        "kindergarten_readiness_rate": total,
    }
    for metric, value in expected.items():
        row = _metric_value(df, year, school_id, metric)
        assert row["value"] == f"{float(value):.1f}", (metric, row["value"])
        assert row["suppressed"] == "N"
    for metric in (
        "kindergarten_readiness_count",
        "kindergarten_readiness_total",
        "kindergarten_ready_with_interventions_count",
        "kindergarten_ready_count",
        "kindergarten_ready_with_enrichments_count",
    ):
        assert _metric_rows(df, year, school_id, metric, "All Students").empty


def test_kyrc26_sources_and_leftover_names():
    """2024-25 comes from the KYRC25 composite. The dead temporary file is not registered."""
    config = yaml.safe_load((ROOT / "config" / "kde_sources.yaml").read_text())
    names = []
    for entry in config["raw_directories"]["kindergarten_readiness"]:
        names.append(entry["file"] if isinstance(entry, dict) else entry)

    assert "KYRC26_ASMT_Kindergarten_Screen_Composite.csv" in names
    assert "KYRC25_ASMT_Kindergarten_Screen_Composite.csv" in names
    assert "KYRC24_ASMT_Kindergarten_Screen_Composite.csv" in names
    assert "KSCREEN_18-19.xlsx" in names
    assert "KSCREEN_17-18.xlsx" in names
    assert "kindergarten_screen_2023.csv" in names
    assert "Kindergarten_Screen_2025.CSV" not in names
    assert "Kindergarten_Screen_2026.CSV" not in names
    assert "KYRC23_ASMT_Kindergarten_Screen_Composite.csv" not in names
    assert names.count("KYRC25_ASMT_Kindergarten_Screen_Composite.csv") == 1
    assert names.count("KYRC26_ASMT_Kindergarten_Screen_Composite.csv") == 1


def test_leftover_temporary_file_does_not_duplicate_a_year(tmp_path):
    """A leftover Kindergarten_Screen_YYYY.CSV must not add a second year.

    The temporary row uses district ``State``, a 3-digit code, and a
    different rate so a duplicate would be visible. The KYRC26 state rate
    stays 45.
    """
    raw_dir = tmp_path / "raw"
    source_dir = raw_dir / "kindergarten_readiness"
    source_dir.mkdir(parents=True)
    proc_dir = tmp_path / "processed"
    proc_dir.mkdir()
    name = "KYRC26_ASMT_Kindergarten_Screen_Composite.csv"
    (source_dir / name).write_bytes((FIXTURE_DIR / name).read_bytes())
    header = (
        "School Year,School Code,District Name,School Name,Demographic,PRIOR_SETTING,Suppressed,"
        "Percent Ready with Interventions,Percent Ready,Percent Ready With Enrichments,Total Percent Ready\n"
    )
    for filename, year in (
        ("Kindergarten_Screen_2024.CSV", "20232024"),
        ("Kindergarten_Screen_2025.CSV", "20242025"),
        ("Kindergarten_Screen_2026.CSV", "20252026"),
    ):
        (source_dir / filename).write_text(
            header + f"{year},999,State,,All Students,All Students,N,10,20,70,90\n"
        )
    (source_dir / "KYRC23_ASMT_Kindergarten_Screen_Composite.csv").write_text(
        "School Year,School Code,District Name,School Name,Demographic,Prior Setting,Suppressed,"
        "Ready With Interventions,Ready,Ready With Enrichments,Total Ready\n"
        "20222023,999000,All Districts,All Schools,All Students,All Students,N,1,2,3,4\n"
    )

    transform(raw_dir, proc_dir, {"derive": {"processing_date": "2026-10-19"}})
    output = pd.read_csv(proc_dir / "kindergarten_readiness.csv", dtype=str)

    assert set(output["source_file"]) == {name}
    assert "90.0" not in set(output["value"].dropna())
    assert "4.0" not in set(output["value"].dropna())
    assert "State" not in set(output["district"])
    state = _metric_value(output, "2026", "999000", "kindergarten_readiness_rate")
    assert state["value"] == "45.0"
    assert state["district"] == "All Districts"


def test_blank_row_is_skipped_and_star_is_kept(tmp_path):
    """A blank suppression cell emits nothing. A star is value NA with suppressed Y.

    Adair County Primary Center ``001020``, American Indian, is blank in
    KYRC25 and KYRC26 and emits no row. The KYRC24 twin is ``Y`` and is
    kept. African American is ``Y`` in KYRC26. Two or More Races publishes
    enrichments 0, which is kept.
    """
    output = _run_fixtures(
        tmp_path,
        [
            "KYRC24_ASMT_Kindergarten_Screen_Composite.csv",
            "KYRC25_ASMT_Kindergarten_Screen_Composite.csv",
            "KYRC26_ASMT_Kindergarten_Screen_Composite.csv",
        ],
    )
    for year in ("2025", "2026"):
        assert _metric_rows(
            output, year, "001020", "kindergarten_readiness_rate", "American Indian or Alaska Native"
        ).empty

    kept = _metric_value(
        output, "2024", "001020", "kindergarten_readiness_rate", "American Indian or Alaska Native"
    )
    assert kept["suppressed"] == "Y"
    assert pd.isna(kept["value"])

    starred = _metric_value(
        output, "2026", "001020", "kindergarten_readiness_rate", "African American"
    )
    assert starred["suppressed"] == "Y"
    assert pd.isna(starred["value"])

    zero_rate = _metric_value(
        output,
        "2026",
        "001020",
        "kindergarten_ready_with_enrichments_rate",
        "Two or More Races",
    )
    assert zero_rate["value"] == "0.0"
    assert zero_rate["suppressed"] == "N"

    # Poage Elementary also publishes enrichments 0 on All Students.
    poage = _metric_value(
        output, "2026", "012110", "kindergarten_ready_with_enrichments_rate"
    )
    assert poage["value"] == "0.0"
    assert poage["suppressed"] == "N"


def test_output_has_no_duplicate_keys(tmp_path):
    """One key is year, school, student group, and metric, across every file."""
    output = _run_fixtures(tmp_path, sorted(path.name for path in FIXTURE_DIR.iterdir()))
    keys = ["year", "school_id", "student_group", "metric"]
    assert not output.duplicated(keys).any()
    assert len(output) == len(output.drop_duplicates(keys))


def test_kyrc26_real_rows_and_prior_year_unchanged(tmp_path):
    """2025-26 KPIs from real composite rows, with fixed earlier-year anchors.

    The composite columns are percents. The count metrics store those
    percents. They are not divided by a tested count. State All Students,
    prior setting All Students, is 55 / 38 / 7 / 45 and the readiness rate
    is 45.
    """
    output = _run_fixtures(
        tmp_path,
        [
            "KYRC24_ASMT_Kindergarten_Screen_Composite.csv",
            "KYRC25_ASMT_Kindergarten_Screen_Composite.csv",
            "KYRC26_ASMT_Kindergarten_Screen_Composite.csv",
            "kindergarten_screen_2020.csv",
            "KSCREEN_18-19.xlsx",
            "KSCREEN_17-18.xlsx",
        ],
    )

    _assert_composite(output, "2026", "999000", ("55", "38", "7", "45"))
    state = _metric_value(output, "2026", "999000", "kindergarten_readiness_rate")
    assert state["district"] == "All Districts"
    assert state["school_name"] == "---District Total---"
    assert state["source_file"] == "KYRC26_ASMT_Kindergarten_Screen_Composite.csv"

    fayette = _metric_value(output, "2026", "165000", "kindergarten_readiness_rate")
    assert fayette["value"] == "47.0"
    assert fayette["district"] == "Fayette County"
    assert fayette["nces_id"] == "2101860"
    assert fayette["co_op"] == "CKEC"
    assert fayette["co_op_code"] == "901"
    _assert_composite(output, "2026", "165000", ("53", "38", "9", "47"))

    adair = _metric_value(output, "2026", "001020", "kindergarten_readiness_rate")
    assert adair["value"] == "36.0"
    assert adair["school_name"] == "Adair County Primary Center"
    assert adair["state_school_id"] == "001001020"
    assert adair["nces_id"] == "210003002345"
    assert adair["school_type"] == "A1"
    _assert_composite(output, "2026", "001020", ("64", "34", "2", "36"))

    child_care = _metric_value(output, "2026", "999000", "kindergarten_child_care_rate")
    assert child_care["value"] == "66.0"
    assert child_care["suppressed"] == "N"
    assert _metric_rows(
        output, "2026", "999000", "kindergarten_child_care_count", "All Students"
    ).empty
    # Prior setting Kindergarten publishes 52 / 41 / 6 / 48. The components
    # sum to 99. The stored rate is the published 48.
    kindergarten_prior = _metric_value(output, "2026", "999000", "kindergarten_kindergarten_rate")
    assert kindergarten_prior["value"] == "48.0"
    assert _metric_rows(
        output, "2026", "999000", "kindergarten_kindergarten_count", "All Students"
    ).empty

    # KYRC25 permanent file. Adair district matches the November 2025 temporary sample 76/23/1/24.
    _assert_composite(output, "2025", "999000", ("54", "39", "7", "46"))
    _assert_composite(output, "2025", "165000", ("52", "38", "10", "48"))
    _assert_composite(output, "2025", "001000", ("76", "23", "1", "24"))
    assert _metric_value(output, "2025", "001000", "kindergarten_readiness_rate")["district"] == "Adair County"

    # KYRC24 anchors. Fayette's components are 50 + 41 + 10 = 101. The stored
    # rate is the published 50, not 50/101*100.
    _assert_composite(output, "2024", "999000", ("52", "40", "8", "48"))
    _assert_composite(output, "2024", "165000", ("50", "41", "10", "50"))
    assert _metric_value(output, "2024", "165000", "kindergarten_readiness_rate")["value"] == "50.0"

    # 2019-20 percent file. Rate is the published percent. Total is the
    # published tested count. Count is the existing round(rate/100*tested).
    prior = _metric_value(output, "2020", "999", "kindergarten_readiness_rate")
    assert prior["value"] == "51.0"
    assert prior["district"] == "State"
    assert prior["school_name"] == "---State Total---"
    assert _metric_value(output, "2020", "999", "kindergarten_readiness_total")["value"] == "47787.0"
    assert _metric_value(output, "2020", "999", "kindergarten_readiness_count")["value"] == "24371.0"
    disability_2020 = _metric_value(
        output, "2020", "999", "kindergarten_readiness_rate", "Students with Disabilities (IEP)"
    )
    assert disability_2020["value"] == "35.0"
    assert _metric_value(
        output, "2020", "999", "kindergarten_readiness_total", "Students with Disabilities (IEP)"
    )["value"] == "7173.0"


def test_2019_workbook_reads_coversheet_labels(tmp_path):
    """ACD is Students with Disabilities. The shared map drops that code.

    The 2018-19 coversheet says ACD is Disabilities (IEP) and CSG is
    Consolidated Student Group. The data file has ACD and no CSG row.
    State tested count is 6,104 and total ready is 34.2. The 2020
    disability tested count is 7,173 and total ready is 35.0. Issue #76.
    The comma in ``6,104`` is kept. A blank American Indian row is skipped.
    """
    output = _run_fixtures(
        tmp_path,
        ["KSCREEN_18-19.xlsx", "KSCREEN_17-18.xlsx", "kindergarten_screen_2020.csv"],
    )
    disability = _metric_value(
        output, "2019", "999", "kindergarten_readiness_rate", "Students with Disabilities (IEP)"
    )
    assert disability["value"] == "34.2"
    assert disability["suppressed"] == "N"
    assert disability["source_file"] == "KSCREEN_18-19.xlsx"
    assert disability["district"] == "State"
    assert _metric_value(
        output, "2019", "999", "kindergarten_readiness_total", "Students with Disabilities (IEP)"
    )["value"] == "6104.0"
    assert _metric_value(
        output, "2019", "999", "kindergarten_readiness_count", "Students with Disabilities (IEP)"
    )["value"] == "2088.0"

    # Adair ACD publishes enrichments 0 and a tested count with no comma.
    adair = _metric_value(
        output, "2019", "001020", "kindergarten_ready_with_enrichments_rate", "Students with Disabilities (IEP)"
    )
    assert adair["value"] == "0.0"
    assert adair["suppressed"] == "N"

    # American Indian at Adair is suppressed N with every measure empty.
    assert _metric_rows(
        output, "2019", "001020", "kindergarten_readiness_rate", "American Indian or Alaska Native"
    ).empty

    state_2019 = _metric_value(output, "2019", "999", "kindergarten_readiness_rate")
    assert state_2019["value"] == "51.1"
    assert _metric_value(output, "2019", "999", "kindergarten_readiness_total")["value"] == "47906.0"

    # 2017-18 is a real earlier year. State disability total ready is 32.1.
    disability_2018 = _metric_value(
        output, "2018", "999", "kindergarten_readiness_rate", "Students with Disabilities (IEP)"
    )
    assert disability_2018["value"] == "32.1"
    assert disability_2018["source_file"] == "KSCREEN_17-18.xlsx"
    assert _metric_value(
        output, "2018", "999", "kindergarten_readiness_total", "Students with Disabilities (IEP)"
    )["value"] == "5359.0"
    assert _metric_value(output, "2018", "999", "kindergarten_readiness_rate")["value"] == "51.4"

    # 2020 still comes from the percent file, not from a swapped code.
    assert _metric_value(
        output, "2020", "999", "kindergarten_readiness_rate", "Students with Disabilities (IEP)"
    )["value"] == "35.0"

    # Workbook state and co-op rows have no county. The stored county is
    # blank. pandas 2 would otherwise write the text "nan".
    assert pd.isna(state_2019["county_number"])


def test_2020_suppressed_rate_and_zero_tested_are_real_rows(tmp_path):
    """All Students prior, suppressed Y, NUMBER TESTED published, percent blank.

    Adair County district ``001``, African American, prior setting All
    Students, tested 3, percent blank, suppressed Y. The rate and the count
    are stored as NA / Y. The tested count is also NA on a Y row. That
    stored-as-NA total predates this change.

    The same district, American Indian, is suppressed N with NUMBER TESTED
    0 and a blank percent. The total 0 is kept and no rate is invented.
    """
    output = _run_fixtures(tmp_path, ["kindergarten_screen_2020.csv"])
    starred = _metric_value(
        output, "2020", "001", "kindergarten_readiness_rate", "African American"
    )
    assert starred["suppressed"] == "Y"
    assert pd.isna(starred["value"])
    assert starred["district"] == "Adair County"
    assert starred["school_name"] == "---District Total---"
    count = _metric_value(
        output, "2020", "001", "kindergarten_readiness_count", "African American"
    )
    assert count["suppressed"] == "Y"
    assert pd.isna(count["value"])
    total = _metric_value(
        output, "2020", "001", "kindergarten_readiness_total", "African American"
    )
    assert total["suppressed"] == "Y"
    assert pd.isna(total["value"])

    zero = _metric_value(
        output,
        "2020",
        "001",
        "kindergarten_readiness_total",
        "American Indian or Alaska Native",
    )
    assert zero["value"] == "0.0"
    assert zero["suppressed"] == "N"
    assert _metric_rows(
        output,
        "2020",
        "001",
        "kindergarten_readiness_rate",
        "American Indian or Alaska Native",
    ).empty
    assert _metric_rows(
        output,
        "2020",
        "001",
        "kindergarten_readiness_count",
        "American Indian or Alaska Native",
    ).empty


def test_starred_prior_setting_does_not_duplicate_the_published_rate(tmp_path):
    """A starred prior setting beside a published All Students row is one rate.

    Allen County ``005``, All Students, prior setting All Students, is
    63.5 of 211. Prior setting Unknown on that same row is suppressed Y,
    tested 3, percent blank. The old defaults wrote a second
    ``kindergarten_readiness_rate`` for that prior setting. The Unknown
    row is only the prior-setting placeholder. State Funded is the
    published prior-setting metric on the same district.
    """
    output = _run_fixtures(tmp_path, ["kindergarten_screen_2020.csv"])
    keys = ["year", "school_id", "student_group", "metric"]
    assert not output.duplicated(keys).any()

    published = _metric_value(output, "2020", "005", "kindergarten_readiness_rate")
    assert published["value"] == "63.5"
    assert published["suppressed"] == "N"
    assert published["district"] == "Allen County"
    assert _metric_value(output, "2020", "005", "kindergarten_readiness_total")["value"] == "211.0"
    assert _metric_value(output, "2020", "005", "kindergarten_readiness_count")["value"] == "134.0"
    assert len(_metric_rows(output, "2020", "005", "kindergarten_readiness_rate", "All Students")) == 1

    placeholder = _metric_value(output, "2020", "005", "kindergarten_unknown_rate")
    assert placeholder["suppressed"] == "Y"
    assert pd.isna(placeholder["value"])
    unknown_count = _metric_value(output, "2020", "005", "kindergarten_unknown_count")
    assert unknown_count["suppressed"] == "Y"
    assert pd.isna(unknown_count["value"])

    state_funded = _metric_value(output, "2020", "005", "kindergarten_state_funded_rate")
    assert state_funded["value"] == "72.8"
    assert state_funded["suppressed"] == "N"
    assert _metric_value(output, "2020", "005", "kindergarten_state_funded_count")["value"] == "59.0"


def test_2017_coop_total_uses_the_2019_label(tmp_path):
    """2017-18 districts 901-909 are co-op totals, not unnamed districts.

    The workbook labels them ``---District Total---`` with a blank name.
    The stored row uses the 2019 co-op name, ``---COOP Total---``, and
    the abbreviation. County number is blank, not the text ``nan``.
    """
    output = _run_fixtures(tmp_path, ["KSCREEN_17-18.xlsx"])
    coop = _metric_value(output, "2018", "901", "kindergarten_readiness_rate")
    assert coop["value"] == "51.3"
    assert coop["suppressed"] == "N"
    assert coop["district"] == "Central Kentucky Educational Cooperative"
    assert coop["school_name"] == "---COOP Total---"
    assert coop["co_op"] == "CKEC"
    assert coop["co_op_code"] == "901"
    assert coop["source_file"] == "KSCREEN_17-18.xlsx"
    assert pd.isna(coop["county_number"])
    assert _metric_value(output, "2018", "901", "kindergarten_readiness_total")["value"] == "6685.0"
    assert _metric_value(output, "2018", "901", "kindergarten_readiness_count")["value"] == "3429.0"

    state = _metric_value(output, "2018", "999", "kindergarten_readiness_rate")
    assert state["district"] == "State"
    assert state["school_name"] == "---State Total---"
    assert pd.isna(state["county_number"])


def test_composite_rate_is_the_published_percent(tmp_path):
    """KYRC24-26 rates are the published percents. No _count or _total.

    State 2026 All Students is 55 / 38 / 7 / 45, so the readiness rate is
    45. Prior setting Kindergarten publishes 52 / 41 / 6 / 48. Those
    components sum to 99. The stored rate is the published 48, not
    48/99*100. The stored text is the published value, not a long float.
    """
    output = _run_fixtures(
        tmp_path,
        [
            "KYRC24_ASMT_Kindergarten_Screen_Composite.csv",
            "KYRC25_ASMT_Kindergarten_Screen_Composite.csv",
            "KYRC26_ASMT_Kindergarten_Screen_Composite.csv",
        ],
    )
    state = _metric_value(output, "2026", "999000", "kindergarten_readiness_rate")
    assert state["value"] == "45.0"
    assert state["suppressed"] == "N"

    kindergarten_prior = _metric_value(
        output, "2026", "999000", "kindergarten_kindergarten_rate"
    )
    assert kindergarten_prior["value"] == "48.0"
    assert "000000" not in kindergarten_prior["value"]

    composite = output[output["year"].isin(["2024", "2025", "2026"])]
    assert not composite["metric"].str.endswith("_count").any()
    assert "kindergarten_readiness_total" not in set(composite["metric"])
    # 2018-21 still has the estimated count. This fixture set has no such year.


def test_starred_composite_prior_setting_has_no_count(tmp_path):
    """A starred non-All prior setting on a composite emits the rate only.

    Adair County Primary Center ``001020``, demographic All Students, prior
    setting Kindergarten, is the verbatim KYRC26 row: suppressed Y and
    starred measure cells. The stored metric is ``kindergarten_kindergarten_rate``
    with value NA and suppressed Y. Re-emitting the prior-setting ``_count``
    placeholder on the 2024-26 composites would add
    ``kindergarten_kindergarten_count`` for this row.
    """
    output = _run_fixtures(
        tmp_path, ["KYRC26_ASMT_Kindergarten_Screen_Composite.csv"]
    )
    keys = ["year", "school_id", "student_group", "metric"]
    assert not output.duplicated(keys).any()

    rate = _metric_value(output, "2026", "001020", "kindergarten_kindergarten_rate")
    assert rate["suppressed"] == "Y"
    assert pd.isna(rate["value"])
    assert rate["student_group"] == "All Students"
    assert _metric_rows(
        output, "2026", "001020", "kindergarten_kindergarten_count", "All Students"
    ).empty

