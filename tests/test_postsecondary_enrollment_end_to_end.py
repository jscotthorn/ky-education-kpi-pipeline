"""End-to-end tests for postsecondary enrollment ETL"""
import pandas as pd
import tempfile
import shutil
from pathlib import Path
from etl.postsecondary_enrollment import transform


class TestPostsecondaryEnrollmentE2E:
    def setup_method(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.temp_dir / "raw"
        self.proc_dir = self.temp_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        self.source_dir = self.raw_dir / "postsecondary_enrollment"
        self.source_dir.mkdir(parents=True)

    def teardown_method(self):
        shutil.rmtree(self.temp_dir)

    def test_full_pipeline(self):
        data = pd.DataFrame({
            "School Year": ["20232024"],
            "District Name": ["Test District"],
            "School Code": ["1001"],
            "School Name": ["Test School"],
            "Demographic": ["All Students"],
            "Total In Group": [100],
            "Public College Enrolled In State": [40],
            "Private College Enrolled In State": [20],
            "College Enrolled In State": [60],
            "Percentage Public College Enrolled In State": ["40%"],
            "Percentage Private College Enrolled In State": ["20%"],
            "Percentage College Enrolled In State": ["60%"],
        })
        data.to_csv(self.source_dir / "sample.csv", index=False)

        transform(self.raw_dir, self.proc_dir, {"derive": {"processing_date": "2024-07-19"}})
        out_file = self.proc_dir / "postsecondary_enrollment.csv"
        assert out_file.exists()
        df = pd.read_csv(out_file)
        assert len(df) == 7
        assert set(df["metric"].unique()) == {
            "postsecondary_enrollment_total_in_cohort",
            "postsecondary_enrollment_public_ky_college_count",
            "postsecondary_enrollment_private_ky_college_count",
            "postsecondary_enrollment_total_ky_college_count",
            "postsecondary_enrollment_public_ky_college_rate",
            "postsecondary_enrollment_private_ky_college_rate",
            "postsecondary_enrollment_total_ky_college_rate",
        }


FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "postsecondary_enrollment"

# Values the base-branch parser already wrote from the KYRC24 and KYRC25 files.
# Order is cohort, public count, private count, total count, public rate,
# private rate, total rate.
PRIOR_ANCHORS = {
    ("2024", "999000"): (46577, 18196, 3361, 21557, 39.1, 7.2, 46.3),
    ("2024", "165000"): (3053, 1352, 177, 1529, 44.3, 5.8, 50.1),
    ("2024", "165170"): (448, 175, 25, 200, 39.1, 5.6, 44.6),
    ("2024", "001000"): (199, 39, 39, 78, 19.6, 19.6, 39.2),
    ("2025", "999000"): (47237, 18323, 3179, 21502, 38.8, 6.7, 45.5),
    ("2025", "165000"): (3217, 1377, 163, 1540, 42.8, 5.1, 47.9),
    ("2025", "165170"): (532, 198, 18, 216, 37.2, 3.4, 40.6),
    ("2025", "001000"): (174, 33, 34, 67, 19.0, 19.5, 38.5),
}

METRICS = (
    "postsecondary_enrollment_total_in_cohort",
    "postsecondary_enrollment_public_ky_college_count",
    "postsecondary_enrollment_private_ky_college_count",
    "postsecondary_enrollment_total_ky_college_count",
    "postsecondary_enrollment_public_ky_college_rate",
    "postsecondary_enrollment_private_ky_college_rate",
    "postsecondary_enrollment_total_ky_college_rate",
)

PRIVATE_METRICS = (
    "postsecondary_enrollment_private_ky_college_count",
    "postsecondary_enrollment_private_ky_college_rate",
)


def _run_fixtures(tmp_path: Path, filenames: list[str]) -> pd.DataFrame:
    """Copy committed real-row excerpts into a raw directory and transform them."""
    raw_dir = tmp_path / "raw"
    source_dir = raw_dir / "postsecondary_enrollment"
    source_dir.mkdir(parents=True)
    proc_dir = tmp_path / "processed"
    proc_dir.mkdir()
    for name in filenames:
        (source_dir / name).write_bytes((FIXTURE_DIR / name).read_bytes())
    transform(raw_dir, proc_dir, {"derive": {"processing_date": "2026-10-17"}})
    output = proc_dir / "postsecondary_enrollment.csv"
    assert output.exists(), filenames
    return pd.read_csv(output, dtype=str)


def _comparable(df: pd.DataFrame) -> pd.DataFrame:
    """Drop the processing timestamp and sort so two runs can be compared."""
    columns = [column for column in df.columns if column != "last_updated"]
    return df[columns].sort_values(columns).reset_index(drop=True)


def _rows(df: pd.DataFrame, year: str, school_id: str, student_group: str) -> pd.DataFrame:
    return df[
        (df["year"] == year)
        & (df["school_id"] == school_id)
        & (df["student_group"] == student_group)
    ]


def _metric_value(df: pd.DataFrame, year: str, school_id: str, metric: str, student_group: str):
    matches = _rows(df, year, school_id, student_group)
    matches = matches[matches["metric"] == metric]
    assert len(matches) == 1, (year, school_id, metric, student_group, len(matches))
    return matches.iloc[0]


def _assert_metrics(df: pd.DataFrame, year: str, school_id: str, expected: tuple, student_group: str = "All Students") -> None:
    for metric, value in zip(METRICS, expected):
        row = _metric_value(df, year, school_id, metric, student_group)
        assert float(row["value"]) == value
        assert row["suppressed"] == "N"


def _assert_suppressed(df: pd.DataFrame, year: str, school_id: str, metrics, student_group: str = "All Students") -> None:
    for metric in metrics:
        row = _metric_value(df, year, school_id, metric, student_group)
        assert row["suppressed"] == "Y"
        assert pd.isna(row["value"])


def test_kyrc26_real_rows_and_prior_year_unchanged(tmp_path):
    """2025-26 KPIs from real transition rows, with fixed earlier-year anchors.

    State All Students is cohort 48,798 and total in-state rate 45.8. KPI
    year is 2026, from school year 20252026. District is All Districts.
    The 2024 and 2025 anchors are values the base parser already wrote.
    """
    prior_names = [
        "KYRC24_ADLF_Transition_to_In_State_Postsecondary_Education.csv",
        "KYRC25_ADLF_Transition_to_In_State_Postsecondary_Education.csv",
    ]
    current_names = prior_names + [
        "KYRC26_ADLF_Transition_to_In_State_Postsecondary_Education.csv",
    ]
    prior_only = _run_fixtures(tmp_path / "prior", prior_names)
    combined = _run_fixtures(tmp_path / "combined", current_names)

    # Same-branch check only: adding the 2026 excerpt does not rewrite earlier rows.
    # The fixed anchors below are the prior-year regression.
    prior_slice = combined[combined["year"].isin(["2024", "2025"])]
    assert _comparable(prior_slice).equals(_comparable(prior_only))

    assert set(combined["source_file"]) == set(current_names)
    assert "20252026" not in set(combined["year"])
    assert set(combined.loc[combined["source_file"].str.startswith("KYRC26_"), "year"]) == {"2026"}
    # A published number stays suppressed N. A * beside a cohort greater than
    # 0 is suppressed Y with an empty value. Issue #44.
    published = combined[combined["suppressed"] == "N"]
    suppressed = combined[combined["suppressed"] == "Y"]
    assert not published.empty
    assert published["value"].notna().all()
    assert not suppressed.empty
    assert suppressed["value"].isna().all()

    for (year, school_id), expected in PRIOR_ANCHORS.items():
        _assert_metrics(combined, year, school_id, expected)

    # KYRC25 stored Bryan Station's NCES id as 210186000000. KYRC24 and KYRC26
    # both use 210186000388. The 2025 value has to stay what the base parser wrote.
    bryan_2024 = _metric_value(
        combined, "2024", "165170", "postsecondary_enrollment_total_ky_college_rate", "All Students"
    )
    bryan_2025 = _metric_value(
        combined, "2025", "165170", "postsecondary_enrollment_total_ky_college_rate", "All Students"
    )
    assert bryan_2024["nces_id"] == "210186000388"
    assert bryan_2025["nces_id"] == "210186000000"
    assert bryan_2024["district"] == "Fayette County"
    assert bryan_2025["school_name"] == "Bryan Station High School"

    state = _metric_value(
        combined, "2026", "999000", "postsecondary_enrollment_total_ky_college_rate", "All Students"
    )
    assert float(state["value"]) == 45.8
    assert state["suppressed"] == "N"
    assert state["district"] == "All Districts"
    assert state["school_name"] == "---District Total---"
    assert state["year"] == "2026"
    assert state["school_id"] == "999000"
    assert state["source_file"] == "KYRC26_ADLF_Transition_to_In_State_Postsecondary_Education.csv"
    _assert_metrics(combined, "2026", "999000", (48798, 18769, 3563, 22332, 38.5, 7.3, 45.8))

    district = _metric_value(
        combined, "2026", "165000", "postsecondary_enrollment_total_ky_college_rate", "All Students"
    )
    assert float(district["value"]) == 47.7
    assert district["district"] == "Fayette County"
    assert district["school_name"] == "---District Total---"
    assert district["school_id"] == "165000"
    assert district["co_op"] == "CKEC"
    assert district["co_op_code"] == "901"
    assert district["nces_id"] == "2101860"
    assert district["suppressed"] == "N"
    _assert_metrics(combined, "2026", "165000", (3340, 1407, 187, 1594, 42.1, 5.6, 47.7))

    school = _metric_value(
        combined, "2026", "165170", "postsecondary_enrollment_total_ky_college_rate", "All Students"
    )
    assert float(school["value"]) == 43.5
    assert school["district"] == "Fayette County"
    assert school["school_name"] == "Bryan Station High School"
    assert school["state_school_id"] == "034165170"
    assert school["nces_id"] == "210186000388"
    assert school["school_type"] == "A1"
    assert school["county_name"] == "FAYETTE"
    assert school["suppressed"] == "N"
    _assert_metrics(combined, "2026", "165170", (577, 225, 26, 251, 39.0, 4.5, 43.5))

    adair = _metric_value(
        combined, "2026", "001000", "postsecondary_enrollment_total_ky_college_rate", "All Students"
    )
    assert float(adair["value"]) == 38.6
    assert adair["district"] == "Adair County"
    assert adair["school_name"] == "---District Total---"
    assert adair["nces_id"] == "2100030"
    assert adair["co_op"] == "GRREC"
    assert adair["co_op_code"] == "902"
    _assert_metrics(combined, "2026", "001000", (215, 36, 47, 83, 16.7, 21.9, 38.6))

    # Cohort cell is * on these groups, so the row stays unclassified and
    # emits nothing. Collins Lane 181025 is the KYRC25 group issue #44 calls
    # padding; its in-file cohort is *, not 0.
    for year in ("2024", "2025", "2026"):
        assert _rows(combined, year, "165170", "Asian").empty
    for year in ("2025", "2026"):
        assert _rows(combined, year, "165000", "American Indian or Alaska Native").empty
    for year in ("2024", "2025"):
        assert _rows(combined, year, "001010", "All Students").empty
    assert _rows(combined, "2025", "181025", "All Students").empty

    # Adair County High School, KYRC26 line for school 001010, All Students.
    # Cohort 201 is published. The six enrollment cells are *.
    # Source: datasets KYRC26_ADLF_Transition_to_In_State_Postsecondary_Education.csv
    adair_school = _rows(combined, "2026", "001010", "All Students")
    assert set(adair_school["metric"]) == set(METRICS)
    cohort = _metric_value(
        combined, "2026", "001010", "postsecondary_enrollment_total_in_cohort", "All Students"
    )
    assert float(cohort["value"]) == 201
    assert cohort["suppressed"] == "N"
    assert cohort["school_name"] == "Adair County High School"
    assert cohort["nces_id"] == "210003000001"
    _assert_suppressed(
        combined,
        "2026",
        "001010",
        [metric for metric in METRICS if metric != "postsecondary_enrollment_total_in_cohort"],
    )

    # Bowling Green High School 042070, All Students. Private count and rate
    # are *. Public 151 and total 167 stay published. 167 - 151 = 16 is not stored.
    bowling = _rows(combined, "2026", "042070", "All Students")
    assert set(bowling["metric"]) == set(METRICS)
    bowling_values = {
        "postsecondary_enrollment_total_in_cohort": 301,
        "postsecondary_enrollment_public_ky_college_count": 151,
        "postsecondary_enrollment_total_ky_college_count": 167,
        "postsecondary_enrollment_public_ky_college_rate": 50.2,
        "postsecondary_enrollment_total_ky_college_rate": 55.5,
    }
    for metric, value in bowling_values.items():
        row = _metric_value(combined, "2026", "042070", metric, "All Students")
        assert float(row["value"]) == value
        assert row["suppressed"] == "N"
    _assert_suppressed(combined, "2026", "042070", PRIVATE_METRICS)
    assert bowling.iloc[0]["school_name"] == "Bowling Green High School"
    assert bowling.iloc[0]["district"] == "Bowling Green Independent"

    # Henry Clay High School 165039, African American. Fayette school group
    # with a published cohort and starred public and private cells.
    # Source row in the KYRC26 fixture, copied from the datasets file.
    henry = _rows(combined, "2026", "165039", "African American")
    assert set(henry["metric"]) == set(METRICS)
    assert henry.iloc[0]["district"] == "Fayette County"
    assert henry.iloc[0]["school_name"] == "Henry Clay High School"
    for metric, value in (
        ("postsecondary_enrollment_total_in_cohort", 142),
        ("postsecondary_enrollment_total_ky_college_count", 56),
        ("postsecondary_enrollment_total_ky_college_rate", 39.4),
    ):
        row = _metric_value(combined, "2026", "165039", metric, "African American")
        assert float(row["value"]) == value
        assert row["suppressed"] == "N"
    _assert_suppressed(
        combined,
        "2026",
        "165039",
        (
            "postsecondary_enrollment_public_ky_college_count",
            "postsecondary_enrollment_private_ky_college_count",
            "postsecondary_enrollment_public_ky_college_rate",
            "postsecondary_enrollment_private_ky_college_rate",
        ),
        "African American",
    )

    # District 602 All Students rows are fully starred, including the cohort.
    assert _rows(combined, "2026", "602000", "All Students").empty
    assert _rows(combined, "2026", "602176", "All Students").empty
    assert "602000" not in set(combined["school_id"])
    assert "602176" not in set(combined["school_id"])


# Published All Students cells from the historical files. Order matches METRICS.
# State school id stays 999. Fayette district school id stays 165.
HISTORICAL_ANCHORS = {
    ("2021", "999"): (45688, 17629, 3642, 21271, 38.6, 8.0, 46.6),
    ("2021", "001"): (184, 34, 57, 91, 18.5, 31.0, 49.5),
    ("2021", "165"): (2783, 1217, 182, 1399, 43.7, 6.5, 50.3),
    ("2021", "165170"): (389, 139, 25, 164, 35.7, 6.4, 42.2),
    ("2022", "999"): (45410, 16626, 3236, 19862, 36.6, 7.1, 43.7),
    ("2022", "001"): (226, 25, 61, 86, 11.1, 27.0, 38.1),
    ("2022", "165"): (2896, 1155, 181, 1336, 39.9, 6.3, 46.1),
    ("2022", "165170"): (420, 133, 22, 155, 31.7, 5.2, 36.9),
    ("2023", "999"): (45366, 17626, 2940, 20566, 38.85, 6.48, 45.33),
    ("2023", "001"): (168, 29, 39, 68, 17.26, 23.21, 40.48),
    ("2023", "165"): (2979, 1297, 152, 1449, 43.54, 5.10, 48.64),
    ("2023", "165170"): (467, 163, 20, 183, 34.90, 4.28, 39.19),
}

COUNT_METRICS = (
    "postsecondary_enrollment_public_ky_college_count",
    "postsecondary_enrollment_private_ky_college_count",
    "postsecondary_enrollment_total_ky_college_count",
)
RATE_METRICS = (
    "postsecondary_enrollment_public_ky_college_rate",
    "postsecondary_enrollment_private_ky_college_rate",
    "postsecondary_enrollment_total_ky_college_rate",
)


def test_historical_real_rows_store_stars_and_keep_published_numbers(tmp_path):
    """2021-2023 anchors, one real star, and a published 0 beside that star.

    Rows are copied from the historical Open House files:
    https://www.education.ky.gov/Open-House/data/HistoricalDatasets/transition_in_state_postsecondary_education_2021.csv
    and the 2022 and 2023 files of the same name.

    2021 Fayette district Migrant is file line 1624: cohort 13, public 5,
    private ``*``, total 5, rates 38.5% / 0.0% / 38.5%. The 0.0% stays 0.
    ``<10`` does not appear on this row.

    2022 Adair County district Students with Disabilities (IEP) is file
    line 12: cohort 15 and three count stars. The three rates are ``**``
    and are omitted. 2023 file line 9 is the same shape with cohort 10.
    """
    output = _run_fixtures(
        tmp_path,
        [
            "transition_in_state_postsecondary_education_2021.csv",
            "transition_in_state_postsecondary_education_2022.csv",
            "transition_in_state_postsecondary_education_2023.csv",
        ],
    )
    for (year, school_id), expected in HISTORICAL_ANCHORS.items():
        _assert_metrics(output, year, school_id, expected)

    state = _metric_value(
        output, "2021", "999", "postsecondary_enrollment_total_ky_college_rate", "All Students"
    )
    assert state["district"] == "State"
    assert state["school_name"] == "---State Total---"
    fayette = _metric_value(
        output, "2021", "165", "postsecondary_enrollment_total_in_cohort", "All Students"
    )
    assert fayette["district"] == "Fayette County"
    assert fayette["school_name"] == "---District Total---"
    bryan = _metric_value(
        output, "2021", "165170", "postsecondary_enrollment_total_ky_college_count", "All Students"
    )
    assert bryan["district"] == "Fayette County"
    assert bryan["school_name"] == "Bryan Station High School"
    assert float(bryan["value"]) == 164

    migrant = _rows(output, "2021", "165", "Migrant")
    assert set(migrant["metric"]) == set(METRICS)
    for metric, value in (
        ("postsecondary_enrollment_total_in_cohort", 13),
        ("postsecondary_enrollment_public_ky_college_count", 5),
        ("postsecondary_enrollment_total_ky_college_count", 5),
        ("postsecondary_enrollment_public_ky_college_rate", 38.5),
        ("postsecondary_enrollment_private_ky_college_rate", 0.0),
        ("postsecondary_enrollment_total_ky_college_rate", 38.5),
    ):
        row = _metric_value(output, "2021", "165", metric, "Migrant")
        assert float(row["value"]) == value
        assert row["suppressed"] == "N"
    _assert_suppressed(
        output, "2021", "165", ("postsecondary_enrollment_private_ky_college_count",), "Migrant"
    )

    for year, cohort in (("2022", 15), ("2023", 10)):
        group = "Students with Disabilities (IEP)"
        row = _metric_value(output, year, "001", "postsecondary_enrollment_total_in_cohort", group)
        assert float(row["value"]) == cohort
        assert row["suppressed"] == "N"
        assert row["district"] == "Adair County"
        _assert_suppressed(output, year, "001", COUNT_METRICS, group)
        assert _rows(output, year, "001", group)["metric"].isin(RATE_METRICS).sum() == 0


def test_flagged_key_list_records_starred_cohorts_and_kyrc26_leaks():
    """The committed list is the kde-recheck record for unclassified rows.

    Collins Lane 181025 All Students is a KYRC25 row whose cohort cell is *.
    The seven KYRC26 private-count leaks are the rows issue #44 names.
    """
    import csv

    path = Path(__file__).resolve().parents[1] / "notes" / "flags" / "A-flagged-keys.csv"
    with path.open(newline="", encoding="utf-8") as handle:
        flagged = list(csv.DictReader(handle))
    assert flagged
    assert list(flagged[0]) == [
        "module",
        "year",
        "district",
        "school_id",
        "student_group",
        "metric",
        "value",
        "reason",
        "label",
    ]
    reasons = {}
    for row in flagged:
        assert row["module"] == "postsecondary_enrollment"
        assert row["label"] == "kde-recheck"
        assert row["value"] == "*"
        reasons.setdefault((row["year"], row["reason"]), 0)
        reasons[(row["year"], row["reason"])] += 1
    assert reasons == {
        ("2021", "starred cohort count"): 1260,
        ("2022", "starred cohort count"): 3125,
        ("2023", "starred cohort count"): 3064,
        ("2024", "starred cohort count"): 3240,
        ("2025", "starred cohort count"): 6375,
        ("2026", "starred cohort count"): 2827,
        ("2026", "private = total - public leaks suppressed value"): 7,
    }
    collins = [
        row
        for row in flagged
        if row["year"] == "2025"
        and row["school_id"] == "181025"
        and row["student_group"] == "All Students"
    ]
    assert len(collins) == 1
    assert collins[0]["district"] == "Franklin County"
    assert collins[0]["reason"] == "starred cohort count"
    assert collins[0]["metric"] == "postsecondary_enrollment_total_in_cohort"
    leaks = [
        (row["school_id"], row["student_group"])
        for row in flagged
        if row["reason"] == "private = total - public leaks suppressed value"
    ]
    assert sorted(leaks) == [
        ("042070", "All Students"),
        ("121030", "Male"),
        ("133040", "Male"),
        ("145010", "Female"),
        ("581200", "Economically Disadvantaged"),
        ("601084", "Gifted and Talented"),
        ("601084", "Male"),
    ]

