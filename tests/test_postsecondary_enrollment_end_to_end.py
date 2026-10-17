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
    # Known deviation from notes/8, tracked in issue #44. The current parser
    # drops every starred cell, so emitted rows are suppressed N with a value.
    # Published values below stay required. Update this when #44 emits Y.
    assert set(combined["suppressed"]) == {"N"}
    assert combined["value"].notna().all()

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

    # Known deviation from notes/8, tracked in issue #44. These groups are
    # fully starred in the source, and the current parser emits no row.
    # Published values above stay required. Update these absences when #44
    # starts emitting suppressed=Y.
    for year in ("2024", "2025", "2026"):
        assert _rows(combined, year, "165170", "Asian").empty
    for year in ("2025", "2026"):
        assert _rows(combined, year, "165000", "American Indian or Alaska Native").empty
    for year in ("2024", "2025"):
        assert _rows(combined, year, "001010", "All Students").empty

    # Adair County High School publishes the cohort. The six starred enrollment
    # cells are absent under the current parser (issue #44). The published
    # cohort value stays required.
    adair_school = _rows(combined, "2026", "001010", "All Students")
    assert set(adair_school["metric"]) == {"postsecondary_enrollment_total_in_cohort"}
    assert float(adair_school.iloc[0]["value"]) == 201
    assert adair_school.iloc[0]["suppressed"] == "N"
    assert adair_school.iloc[0]["school_name"] == "Adair County High School"
    assert adair_school.iloc[0]["nces_id"] == "210003000001"

    # Private enrollment is starred and the other five cells are published.
    # The absent private metrics are the issue #44 deviation. The five
    # published values stay required.
    bowling = _rows(combined, "2026", "042070", "All Students")
    assert set(bowling["metric"]) == set(METRICS) - set(PRIVATE_METRICS)
    assert (bowling["suppressed"] == "N").all()
    bowling_values = {
        "postsecondary_enrollment_total_in_cohort": 301,
        "postsecondary_enrollment_public_ky_college_count": 151,
        "postsecondary_enrollment_total_ky_college_count": 167,
        "postsecondary_enrollment_public_ky_college_rate": 50.2,
        "postsecondary_enrollment_total_ky_college_rate": 55.5,
    }
    for metric, value in bowling_values.items():
        assert float(_metric_value(combined, "2026", "042070", metric, "All Students")["value"]) == value
    assert bowling.iloc[0]["school_name"] == "Bowling Green High School"
    assert bowling.iloc[0]["district"] == "Bowling Green Independent"

    # District 602 is new versus KYRC25. Both All Students rows are fully
    # starred, so the current parser emits nothing (issue #44).
    assert _rows(combined, "2026", "602000", "All Students").empty
    assert _rows(combined, "2026", "602176", "All Students").empty
    assert "602000" not in set(combined["school_id"])
    assert "602176" not in set(combined["school_id"])

