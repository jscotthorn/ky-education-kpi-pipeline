"""Tests for Postsecondary Enrollment ETL module"""
import pandas as pd
from pathlib import Path
import tempfile
import shutil
import yaml
from etl.postsecondary_enrollment import (
    PostsecondaryEnrollmentETL,
    transform,
)

ROOT = Path(__file__).resolve().parents[1]


class TestPostsecondaryEnrollmentETL:
    def setup_method(self):
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        self.sample_dir = self.raw_dir / "postsecondary_enrollment"
        self.sample_dir.mkdir(parents=True)
        self.etl = PostsecondaryEnrollmentETL("postsecondary_enrollment")

    def teardown_method(self):
        shutil.rmtree(self.test_dir)

    def create_sample_data(self):
        return pd.DataFrame({
            "School Year": ["20232024"],
            "District Name": ["Test District"],
            "School Code": ["1001"],
            "School Name": ["Test School"],
            "Demographic": ["All Students"],
            "Total In Group": [100],
            "Public College Enrolled In State": [40],
            "Private College Enrolled In State": [20],
            "College Enrolled In State": [60],
            "Percentage Public College Enrolled In State": ["40.0%"],
            "Percentage Private College Enrolled In State": ["20.0%"],
            "Percentage College Enrolled In State": ["60.0%"],
        })

    def test_normalize_column_names(self):
        df = self.create_sample_data()
        normalized = self.etl.normalize_column_names(df)
        assert "school_year" in normalized.columns
        assert "public_college_enrolled" in normalized.columns

    def test_standardize_missing_values(self):
        df = pd.DataFrame({
            "public_college_rate": ["40%", ""],
            "total_in_group": ["1,000", ""],
        })
        cleaned = self.etl.standardize_missing_values(df)
        assert cleaned.loc[0, "public_college_rate"] == 40.0
        assert pd.isna(cleaned.loc[1, "public_college_rate"])
        assert cleaned.loc[0, "total_in_group"] == 1000
        assert pd.isna(cleaned.loc[1, "total_in_group"])

    def test_convert_to_kpi_format(self):
        df = self.create_sample_data()
        df = self.etl.normalize_column_names(df)
        df = self.etl.standardize_missing_values(df)
        df["source_file"] = "test.csv"
        kpi = self.etl.convert_to_kpi_format(df, "test.csv")
        metrics = set(kpi["metric"].unique())
        assert "postsecondary_enrollment_total_ky_college_rate" in metrics
        assert len(kpi) == 7

    def test_transform_integration(self):
        df = self.create_sample_data()
        df.to_csv(self.sample_dir / "sample.csv", index=False)
        transform(self.raw_dir, self.proc_dir, {"derive": {"processing_date": "2024-07-19"}})
        output_file = self.proc_dir / "postsecondary_enrollment.csv"
        assert output_file.exists()
        out_df = pd.read_csv(output_file)
        assert len(out_df) == 7


class TestKyrc26PostsecondaryEnrollment:
    """KYRC26 opt-in. There is no temporary extract to skip."""

    def setup_method(self):
        self.test_dir = Path(tempfile.mkdtemp())
        self.sample_dir = self.test_dir / "postsecondary_enrollment"
        self.sample_dir.mkdir(parents=True)
        self.etl = PostsecondaryEnrollmentETL("postsecondary_enrollment")

    def teardown_method(self):
        shutil.rmtree(self.test_dir)

    def test_kyrc26_file_is_accepted(self):
        """The permanent KYRC files and the 2021-2023 historical names stay in scope."""
        assert self.etl.include_kyrc26 is True
        assert self.etl.module_column_mappings["Total In Group"] == "total_in_group"
        assert (
            self.etl.module_column_mappings["PERCENTAGE COLLEGE ENROLLED IN STATE"]
            == "college_enrollment_rate"
        )
        assert (
            self.etl.module_column_mappings["Percentage College Enrolled In State Table"]
            == "college_enrollment_rate"
        )

        accepted = [
            "KYRC26_ADLF_Transition_to_In_State_Postsecondary_Education.csv",
            "KYRC25_ADLF_Transition_to_In_State_Postsecondary_Education.csv",
            "KYRC24_ADLF_Transition_to_In_State_Postsecondary_Education.csv",
            "transition_in_state_postsecondary_education_2023.csv",
            "transition_in_state_postsecondary_education_2022.csv",
            "transition_in_state_postsecondary_education_2021.csv",
        ]
        for name in accepted:
            (self.sample_dir / name).write_text("School Year\n20252026\n")

        found = sorted(path.name for path, _sheet in self.etl._find_data_files(self.sample_dir))
        assert found == sorted(accepted)

    def test_blank_row_and_unmapped_sibling_emit_nothing(self):
        """An empty non-Y row is not offered. Sibling ADLF files map no measure.

        notes/8: a row that is not suppressed and has every value empty is
        the absence of a row. This file has no Suppressed column. The base
        converter already skips an empty metric, so the blank row emits
        nothing. Current Year Graduates and Graduate Outcomes do not use
        these measure headers, so they also emit nothing and need no guard.
        """
        blank = pd.DataFrame({
            "School Year": ["20252026"],
            "District Name": ["All Districts"],
            "School Name": ["All Schools"],
            "School Code": ["999000"],
            "Demographic": ["All Students"],
            "Total In Group": [""],
            "Public College Enrolled In State": [""],
            "Private College Enrolled In State": [""],
            "College Enrolled In State": [""],
            "Percentage Public College Enrolled In State": [""],
            "Percentage Private College Enrolled In State": [""],
            "Percentage College Enrolled In State Table": [""],
        })
        graduates = pd.DataFrame({
            "School Year": ["20252026"],
            "District Name": ["All Districts"],
            "School Name": ["All Schools"],
            "School Code": ["999000"],
            "Demographic": ["All Students"],
            "Graduate Count": ["100"],
        })
        outcomes = pd.DataFrame({
            "School Year": ["20252026"],
            "District Name": ["All Districts"],
            "School Name": ["All Schools"],
            "School Code": ["999000"],
            "College": ["10"],
            "Military": ["1"],
            "Working": ["5"],
            "College Percent": ["50"],
        })
        for frame, name in (
            (blank, "blank.csv"),
            (graduates, "KYRC26_ADLF_Current_Year_Graduates.csv"),
            (outcomes, "KYRC26_ADLF_Graduate_Outcomes.csv"),
        ):
            normalized = self.etl.standardize_missing_values(self.etl.normalize_column_names(frame))
            kpi = self.etl.convert_to_kpi_format(normalized, name)
            assert kpi.empty, name

        # The siblings are accepted. They do not need a filename guard.
        for name in (
            "KYRC26_ADLF_Current_Year_Graduates.csv",
            "KYRC26_ADLF_Graduate_Outcomes.csv",
        ):
            (self.sample_dir / name).write_text("School Year\n20252026\n")
            assert self.etl.accepts_data_file(self.sample_dir / name)


def test_registered_sources_are_the_permanent_files():
    """2024-25 is already the permanent KYRC25 file. No temporary name is registered."""
    config = yaml.safe_load((ROOT / "config" / "kde_sources.yaml").read_text())
    names = []
    for entry in config["raw_directories"]["postsecondary_enrollment"]:
        names.append(entry["file"] if isinstance(entry, dict) else entry)

    assert names == [
        "KYRC24_ADLF_Transition_to_In_State_Postsecondary_Education.csv",
        "KYRC25_ADLF_Transition_to_In_State_Postsecondary_Education.csv",
        "KYRC26_ADLF_Transition_to_In_State_Postsecondary_Education.csv",
        "transition_in_state_postsecondary_education_2023.csv",
        "transition_in_state_postsecondary_education_2022.csv",
        "transition_in_state_postsecondary_education_2021.csv",
    ]

