"""End-to-end tests for the OutOfSchoolSuspensionETL."""
import pandas as pd
import tempfile
import yaml
from pathlib import Path
from etl.out_of_school_suspension import transform


class TestOutOfSchoolSuspensionE2E:
    def test_kyrc24_and_safe_schools(self):
        """KYRC24 emits CRDC counts. The annual file emits nothing.

        safe_schools_discipline owns ``OUT OF SCHOOL SUSPENSION SSP3``
        since e7d815a (4 Aug 2025). This module does not read
        ``safe_schools_discipline_YYYY.csv``.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            raw_dir = Path(tmpdir) / 'raw'
            proc_dir = Path(tmpdir) / 'proc'
            ky_dir = raw_dir / 'out_of_school_suspension'
            ky_dir.mkdir(parents=True)
            proc_dir.mkdir(parents=True)

            kyrc24 = pd.DataFrame({
                'School Year': ['20232024'],
                'Collected School Year': ['20202021'],
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
            safe.to_csv(ky_dir / 'safe_schools_discipline_2023.csv', index=False)

            transform(raw_dir, proc_dir, {'derive': {}})

            out_file = proc_dir / 'out_of_school_suspension.csv'
            df = pd.read_csv(out_file)
            assert len(df) == 10
            assert set(df['source_file']) == {'kyrc24.csv'}
            assert set(df['year'].astype(str)) == {'2024'}
            assert 'collected_school_year' not in df.columns
            assert 'out_of_school_suspension_count' not in set(df['metric'])
            assert 'expelled_receiving_services_count' not in set(df['metric'])
            assert 'discipline_resolution_total_count' not in set(df['metric'])
            assert 'out_of_school_suspension_single_with_disabilities_count' in set(df['metric'])
            assert '2021' not in set(df['year'].astype(str))


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
    """2025-26 CRDC suspension KPIs from real rows, with the 2024 anchor.

    The KPI year is the report-card year. 2026 is the 2021-22 CRDC
    collection and 2024 is the 2020-21 collection. Collected School Year
    is not a KPI column. There is no school code 999000. KYRC25
    republishes the 2020-21 collection and is not an input. The annual
    Safe Schools file emits nothing from this module.
    """
    prior_names = ["KYRC24_OVW_Student_Suspensions.csv"]
    current_names = prior_names + [
        "KYRC26_OVW_Student_Suspensions.csv",
        "safe_schools_discipline_2023.csv",
    ]
    prior_only = _run_fixtures(tmp_path / "prior", prior_names)
    combined = _run_fixtures(tmp_path / "combined", current_names)

    # Adding the 2026 excerpt does not rewrite the 2024 rows. The annual
    # file contributes no rows. safe_schools_discipline owns SSP3.
    prior_slice = combined[combined["year"] == "2024"]
    assert _comparable(prior_slice).equals(_comparable(prior_only))
    assert set(combined["source_file"]) == {
        "KYRC24_OVW_Student_Suspensions.csv",
        "KYRC26_OVW_Student_Suspensions.csv",
    }
    assert "collected_school_year" not in combined.columns
    assert set(combined["year"]) == {"2024", "2026"}
    assert "2022" not in set(combined["year"])
    assert "2021" not in set(combined["year"])
    assert "2025" not in set(combined["year"])
    assert "2023" not in set(combined["year"])

    # Bryan Station High School, the school row. No state or district total
    # exists in the CRDC file. Year 2026 is report-card year, CRDC 2021-22.
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

    # American Indian at Bryan Station is a published 0 in every count,
    # including the totals. KYRC26 has no star.
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

    # KYRC24 Bryan Station All Students is the 2020-21 collection filed
    # under report-card year 2024.
    row = _metric_value(combined, "2024", "165170", "out_of_school_suspension_total_count")
    assert float(row["value"]) == 30
    assert row["suppressed"] == "N"
    assert row["year"] == "2024"
    assert row["source_file"] == "KYRC24_OVW_Student_Suspensions.csv"
    assert row["district"] == "Fayette County"
    assert float(_metric_value(combined, "2024", "165170", "out_of_school_suspension_single_with_disabilities_count")["value"]) == 5
    assert float(_metric_value(combined, "2024", "165170", "out_of_school_suspension_single_without_disabilities_count")["value"]) == 2
    assert float(_metric_value(combined, "2024", "165170", "out_of_school_suspension_multiple_without_disabilities_count")["value"]) == 20
    assert float(_metric_value(combined, "2024", "165170", "in_school_suspension_without_disabilities_count")["value"]) == 36

    # Camp Taylor All Students is starred in every count. Each component
    # and each total is suppressed. CO-OP is blank. co_op_code is 909.
    for metric in (
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
    ):
        starred = _metric_value(combined, "2024", "275004", metric)
        assert starred["suppressed"] == "Y"
        assert _is_empty(starred["value"])
        assert starred["school_name"] == "Camp Taylor Elementary"
        assert starred["district"] == "Jefferson County"
        assert starred["co_op_code"] == "909"
        assert _is_empty(starred["co_op"])

    # South Hopkins Female is the one partial star. In-school without
    # disabilities is ``*`` and the other in-school cell is 0, so the
    # in-school total is suppressed. The out-of-school cells are published:
    # 0 + 1 = 1 single, 0 + 1 = 1 multiple, total 2.
    partial = _metric_value(
        combined, "2024", "265185", "in_school_suspension_without_disabilities_count", "Female"
    )
    assert partial["suppressed"] == "Y"
    assert _is_empty(partial["value"])
    assert partial["school_name"] == "South Hopkins Middle School"
    in_total = _metric_value(
        combined, "2024", "265185", "in_school_suspension_total_count", "Female"
    )
    assert in_total["suppressed"] == "Y"
    assert _is_empty(in_total["value"])
    assert float(_metric_value(combined, "2024", "265185", "in_school_suspension_with_disabilities_count", "Female")["value"]) == 0
    assert float(_metric_value(combined, "2024", "265185", "out_of_school_suspension_single_without_disabilities_count", "Female")["value"]) == 1
    assert float(_metric_value(combined, "2024", "265185", "out_of_school_suspension_multiple_without_disabilities_count", "Female")["value"]) == 1
    assert float(_metric_value(combined, "2024", "265185", "out_of_school_suspension_single_total_count", "Female")["value"]) == 1
    assert float(_metric_value(combined, "2024", "265185", "out_of_school_suspension_multiple_total_count", "Female")["value"]) == 1
    total = _metric_value(
        combined, "2024", "265185", "out_of_school_suspension_total_count", "Female"
    )
    assert float(total["value"]) == 2
    assert total["suppressed"] == "N"
