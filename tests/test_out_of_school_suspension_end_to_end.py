"""End-to-end tests for the OutOfSchoolSuspensionETL."""
import pandas as pd
import tempfile
import yaml
from pathlib import Path
from etl.out_of_school_suspension import transform


class TestOutOfSchoolSuspensionE2E:
    def test_kyrc24_and_safe_schools(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            raw_dir = Path(tmpdir) / 'raw'
            proc_dir = Path(tmpdir) / 'proc'
            ky_dir = raw_dir / 'out_of_school_suspension'
            ky_dir.mkdir(parents=True)
            proc_dir.mkdir(parents=True)

            kyrc24 = pd.DataFrame({
                'School Year': ['20232024'],
                'District Name': ['Test'],
                'School Code': ['100'],
                'School Name': ['Test School'],
                'Demographic': ['All Students'],
                'Single Out-of-School With Disabilities': [1],
                'Single Out-of-School Without Disabilities': [2],
                'Multiple Out-of-School With Disabilities': [0],
                'Multiple Out-of-School Without Disabilities': [1],
                'In-School With Disabilities': [3],
                'In-School Without Disabilities': [4],
                'Expelled Receiving Services SSP1': [1],
                'Total Discipline Resolutions': [10]
            })
            kyrc24.to_csv(ky_dir / 'kyrc24.csv', index=False)

            safe = pd.DataFrame({
                'SCHOOL YEAR': ['20222023'],
                'DISTRICT NAME': ['Test'],
                'SCHOOL CODE': ['200'],
                'SCHOOL NAME': ['Test 2'],
                'DEMOGRAPHIC': ['All Students'],
                'OUT OF SCHOOL SUSPENSION SSP3': [3],
                'EXPELLED RECEIVING SERVICES SSP1': [1],
                'IN-SCHOOL REMOVAL INSR': [2]
            })
            safe.to_csv(ky_dir / 'safe.csv', index=False)

            transform(raw_dir, proc_dir, {'derive': {}})

            out_file = proc_dir / 'out_of_school_suspension.csv'
            df = pd.read_csv(out_file)
            assert len(df) == 13
            assert set(df['metric'].unique()) >= {
                'out_of_school_suspension_count',
                'out_of_school_suspension_single_with_disabilities_count'
            }


FIXTURE_DIR = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "out_of_school_suspension"


def _run_fixtures(tmp_path: Path, filenames: list) -> pd.DataFrame:
    """Copy committed real-row excerpts into a raw directory and transform them."""
    raw_dir = tmp_path / "raw"
    source_dir = raw_dir / "out_of_school_suspension"
    source_dir.mkdir(parents=True)
    proc_dir = tmp_path / "processed"
    proc_dir.mkdir()
    for name in filenames:
        (source_dir / name).write_bytes((FIXTURE_DIR / name).read_bytes())
    config = yaml.safe_load(
        (Path(__file__).resolve().parents[1] / "config" / "mappings.yaml").read_text()
    )
    transform(raw_dir, proc_dir, config["sources"]["out_of_school_suspension"])
    output = proc_dir / "out_of_school_suspension.csv"
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


def _metric_value(df: pd.DataFrame, year: str, school_id: str, metric: str, student_group: str = "All Students"):
    matches = _rows(df, year, school_id, student_group)
    matches = matches[matches["metric"] == metric]
    assert len(matches) == 1, (year, school_id, metric, student_group, len(matches))
    return matches.iloc[0]


def _is_empty(value) -> bool:
    return value is None or (isinstance(value, float) and pd.isna(value)) or pd.isna(value) or str(value) == ""


def test_kyrc26_real_rows_and_prior_year_unchanged(tmp_path):
    """2025-26 CRDC suspension KPIs from real rows, with earlier-year anchors.

    There is no school code 999000 in KYRC24, KYRC25, or KYRC26. The KPI
    year comes from School Year. Collected School Year on the 2026 file is
    20212022 on every row and is not the KPI year.
    """
    prior_names = [
        "safe_schools_discipline_2023.csv",
        "KYRC24_OVW_Student_Suspensions.csv",
        "KYRC25_OVW_Student_Suspensions.csv",
    ]
    current_names = prior_names + ["KYRC26_OVW_Student_Suspensions.csv"]
    prior_only = _run_fixtures(tmp_path / "prior", prior_names)
    combined = _run_fixtures(tmp_path / "combined", current_names)

    # Same-branch check only: adding the 2026 excerpt does not rewrite earlier rows.
    # The fixed anchors below are the prior-year regression.
    prior_slice = combined[combined["year"].isin(["2023", "2024", "2025"])]
    assert _comparable(prior_slice).equals(_comparable(prior_only))

    assert set(combined["source_file"]) == set(current_names)
    assert "20212022" not in set(combined["year"])
    assert "20202021" not in set(combined["year"])
    assert set(combined.loc[combined["source_file"].str.startswith("KYRC26_"), "year"]) == {"2026"}
    assert set(combined.loc[combined["source_file"].str.startswith("KYRC25_"), "year"]) == {"2025"}

    # Bryan Station High School, the school row. No state or district total
    # exists in the CRDC file.
    state_like = combined[
        (combined["year"] == "2026")
        & (combined["school_id"].isin(["999", "999000"]))
    ]
    assert state_like.empty
    school = _metric_value(
        combined, "2026", "165170", "out_of_school_suspension_single_with_disabilities_count"
    )
    assert float(school["value"]) == 7
    assert school["suppressed"] == "N"
    assert school["district"] == "Fayette County"
    assert school["school_name"] == "Bryan Station High School"
    assert school["year"] == "2026"
    assert school["school_id"] == "165170"
    assert school["source_file"] == "KYRC26_OVW_Student_Suspensions.csv"
    assert school["co_op"] == "CKEC"
    assert school["co_op_code"] == "901"
    assert school["nces_id"] == "210186000388"
    assert school["state_school_id"] == "034165170"
    assert school["school_type"] == "A1"
    assert school["county_name"] == "FAYETTE"
    assert float(_metric_value(combined, "2026", "165170", "out_of_school_suspension_single_without_disabilities_count")["value"]) == 28
    assert float(_metric_value(combined, "2026", "165170", "out_of_school_suspension_multiple_with_disabilities_count")["value"]) == 35
    assert float(_metric_value(combined, "2026", "165170", "out_of_school_suspension_multiple_without_disabilities_count")["value"]) == 146
    # 7 + 28 = 35 single students. 35 + 146 = 181 multiple students.
    # 35 + 181 = 216 students. In-school is 38 + 198 = 236.
    assert float(_metric_value(combined, "2026", "165170", "out_of_school_suspension_single_total_count")["value"]) == 35
    assert float(_metric_value(combined, "2026", "165170", "out_of_school_suspension_multiple_total_count")["value"]) == 181
    assert float(_metric_value(combined, "2026", "165170", "out_of_school_suspension_total_count")["value"]) == 216
    assert float(_metric_value(combined, "2026", "165170", "in_school_suspension_with_disabilities_count")["value"]) == 38
    assert float(_metric_value(combined, "2026", "165170", "in_school_suspension_without_disabilities_count")["value"]) == 198
    assert float(_metric_value(combined, "2026", "165170", "in_school_suspension_total_count")["value"]) == 236

    # Adair County High School. Multiple with disabilities for Male is a
    # published 1. KYRC26 has no star cell.
    adair = _metric_value(
        combined, "2026", "001010", "out_of_school_suspension_total_count"
    )
    assert float(adair["value"]) == 87
    assert adair["district"] == "Adair County"
    assert adair["school_name"] == "Adair County High School"
    assert adair["co_op"] == "GRREC"
    assert adair["suppressed"] == "N"
    assert float(_metric_value(combined, "2026", "001010", "out_of_school_suspension_single_total_count")["value"]) == 69
    assert float(_metric_value(combined, "2026", "001010", "out_of_school_suspension_multiple_total_count")["value"]) == 18
    assert float(_metric_value(combined, "2026", "001010", "in_school_suspension_total_count")["value"]) == 164
    male_one = _metric_value(
        combined,
        "2026",
        "001010",
        "out_of_school_suspension_multiple_with_disabilities_count",
        "Male",
    )
    assert float(male_one["value"]) == 1
    assert male_one["suppressed"] == "N"

    # American Indian at Bryan Station is a published 0 in every count.
    # KYRC25 stored small groups as ``*``. KYRC26 publishes the zero.
    indigenous = _rows(combined, "2026", "165170", "American Indian or Alaska Native")
    assert set(indigenous["metric"]) == {
        "out_of_school_suspension_single_with_disabilities_count",
        "out_of_school_suspension_single_without_disabilities_count",
        "out_of_school_suspension_multiple_with_disabilities_count",
        "out_of_school_suspension_multiple_without_disabilities_count",
        "out_of_school_suspension_single_total_count",
        "out_of_school_suspension_multiple_total_count",
        "out_of_school_suspension_total_count",
        "in_school_suspension_with_disabilities_count",
        "in_school_suspension_without_disabilities_count",
        "in_school_suspension_total_count",
    }
    assert (indigenous["suppressed"] == "N").all()
    assert (indigenous["value"].astype(float) == 0).all()

    # KYRC24 and KYRC25 Bryan Station All Students are the same CRDC
    # 2020-21 counts. The KPI year follows School Year.
    for year, source in (("2024", "KYRC24_OVW_Student_Suspensions.csv"), ("2025", "KYRC25_OVW_Student_Suspensions.csv")):
        row = _metric_value(combined, year, "165170", "out_of_school_suspension_total_count")
        assert float(row["value"]) == 30
        assert row["suppressed"] == "N"
        assert row["year"] == year
        assert row["source_file"] == source
        assert row["district"] == "Fayette County"
        assert float(_metric_value(combined, year, "165170", "out_of_school_suspension_single_with_disabilities_count")["value"]) == 5
        assert float(_metric_value(combined, year, "165170", "out_of_school_suspension_single_without_disabilities_count")["value"]) == 2
        assert float(_metric_value(combined, year, "165170", "out_of_school_suspension_multiple_without_disabilities_count")["value"]) == 20
        assert float(_metric_value(combined, year, "165170", "in_school_suspension_without_disabilities_count")["value"]) == 36

    # Camp Taylor All Students is starred in every count. The component
    # cells stay suppressed. KYRC24 leaves CO-OP blank. KYRC25 fills GREC.
    # The count cells are otherwise the same file.
    for year, co_op in (("2024", None), ("2025", "GREC")):
        row = _metric_value(
            combined, year, "275004", "out_of_school_suspension_single_with_disabilities_count"
        )
        assert row["suppressed"] == "Y"
        assert _is_empty(row["value"])
        assert row["school_name"] == "Camp Taylor Elementary"
        assert row["district"] == "Jefferson County"
        assert row["co_op_code"] == "909"
        if co_op is None:
            assert _is_empty(row["co_op"])
        else:
            assert row["co_op"] == co_op
        for metric in (
            "out_of_school_suspension_single_without_disabilities_count",
            "out_of_school_suspension_multiple_with_disabilities_count",
            "out_of_school_suspension_multiple_without_disabilities_count",
            "in_school_suspension_with_disabilities_count",
            "in_school_suspension_without_disabilities_count",
        ):
            starred = _metric_value(combined, year, "275004", metric)
            assert starred["suppressed"] == "Y"
            assert _is_empty(starred["value"])

    # South Hopkins Female is the one partial star: in-school without
    # disabilities is ``*`` and the out-of-school cells are published.
    # 0 + 1 = 1 single, 0 + 1 = 1 multiple, total 2.
    partial = _metric_value(
        combined, "2025", "265185", "in_school_suspension_without_disabilities_count", "Female"
    )
    assert partial["suppressed"] == "Y"
    assert _is_empty(partial["value"])
    assert partial["school_name"] == "South Hopkins Middle School"
    assert float(_metric_value(combined, "2025", "265185", "in_school_suspension_with_disabilities_count", "Female")["value"]) == 0
    assert float(_metric_value(combined, "2025", "265185", "out_of_school_suspension_single_without_disabilities_count", "Female")["value"]) == 1
    assert float(_metric_value(combined, "2025", "265185", "out_of_school_suspension_multiple_without_disabilities_count", "Female")["value"]) == 1
    assert float(_metric_value(combined, "2025", "265185", "out_of_school_suspension_total_count", "Female")["value"]) == 2

    # Annual Safe Schools SSP3 anchors. School code 999 is the state row.
    # There is no 999000. All Students is 40,768. Total Events is 75,620
    # and is not a demographic, so it emits no row.
    state = _metric_value(combined, "2023", "999", "out_of_school_suspension_count")
    assert float(state["value"]) == 40768
    assert state["suppressed"] == "N"
    assert state["district"] == "State"
    assert state["school_name"] == "---State Total---"
    assert state["year"] == "2023"
    assert state["source_file"] == "safe_schools_discipline_2023.csv"
    events = combined[
        (combined["year"] == "2023")
        & (combined["student_group"] == "Total Events")
    ]
    assert events.empty
    assert "75620" not in set(combined.loc[combined["year"] == "2023", "value"].dropna())
    assert "75620.0" not in set(combined.loc[combined["year"] == "2023", "value"].dropna())

    district = _metric_value(combined, "2023", "165", "out_of_school_suspension_count")
    assert float(district["value"]) == 2667
    assert district["district"] == "Fayette County"
    assert district["school_name"] == "---District Total---"
    assert district["school_id"] == "165"
    assert district["co_op"] == "CKEC"
    assert district["suppressed"] == "N"

    assert float(_metric_value(combined, "2023", "165170", "out_of_school_suspension_count")["value"]) == 265
    indigenous_2023 = _metric_value(
        combined, "2023", "165170", "out_of_school_suspension_count", "American Indian or Alaska Native"
    )
    assert indigenous_2023["suppressed"] == "Y"
    assert _is_empty(indigenous_2023["value"])

    # The 2023 file dates Breathitt County as 20022023. extract_year keeps 2023.
    breathitt = _metric_value(combined, "2023", "061", "out_of_school_suspension_count")
    assert float(breathitt["value"]) == 30
    assert breathitt["district"] == "Breathitt County"
    assert breathitt["school_name"] == "---District Total---"
    assert breathitt["year"] == "2023"
    assert "2002" not in set(combined["year"])
