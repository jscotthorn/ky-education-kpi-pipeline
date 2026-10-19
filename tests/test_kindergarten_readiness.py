"""Unit tests for Kindergarten Readiness ETL module"""
import shutil
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from etl.kindergarten_readiness import (
    transform,
    clean_readiness_data,
    KindergartenReadinessETL,
)


class TestKindergartenReadinessETL:
    def setup_method(self):
        self.test_dir = Path(tempfile.mkdtemp())
        self.raw_dir = self.test_dir / "raw"
        self.proc_dir = self.test_dir / "processed"
        self.proc_dir.mkdir(parents=True)
        self.sample_dir = self.raw_dir / "kindergarten_readiness"
        self.sample_dir.mkdir(parents=True)
        self.etl = KindergartenReadinessETL("kindergarten_readiness")

    def teardown_method(self):
        shutil.rmtree(self.test_dir)

    def create_sample_counts_data(self):
        return pd.DataFrame({
            "School Year": ["20232024", "20232024", "20232024"],
            "District Name": ["Fayette County", "Fayette County", "Fayette County"],
            "School Code": ["1001", "1002", "1001"],
            "School Name": ["Test School A", "Test School B", "Test School A"],
            "Demographic": ["All Students", "Female", "All Students"],
            "Ready With Interventions": [10, 5, 8],
            "Ready": [20, 10, 15],
            "Ready With Enrichments": [5, 3, 4],
            "Total Ready": [35, 18, 27],
            "Suppressed": ["N", "Y", "N"],
            "Prior Setting": ["All Students", "All Students", "Child Care"],
        })

    def create_sample_percent_data(self):
        return pd.DataFrame({
            "SCHOOL YEAR": ["20212022"],
            "DISTRICT NAME": ["Fayette County"],
            "SCHOOL CODE": ["1001"],
            "SCHOOL NAME": ["Test School A"],
            "DEMOGRAPHIC": ["All Students"],
            "TOTAL PERCENT READY": [55.0],
            "NUMBER TESTED": [100],
            "SUPPRESSED": ["N"],
            "Prior Setting": ["All Students"],
        })

    def test_normalize_column_names(self):
        df = self.create_sample_counts_data()
        result = self.etl.normalize_column_names(df)
        assert "total_ready_count" in result.columns
        assert "ready_with_interventions_count" in result.columns
        assert "school_code" in result.columns

    def test_standardize_missing_values(self):
        df = pd.DataFrame({
            "total_ready_count": ["", "*", "35"],
            "ready_with_interventions_count": [10, "", "*"],
            "suppressed": ["Yes", "No", None],
        })
        cleaned = self.etl.standardize_missing_values(df)
        assert pd.isna(cleaned.loc[0, "total_ready_count"])
        assert pd.isna(cleaned.loc[1, "total_ready_count"])
        assert cleaned.loc[2, "total_ready_count"] == 35
        assert cleaned.loc[0, "suppressed"] == "Y"
        assert cleaned.loc[1, "suppressed"] == "N"
        assert cleaned.loc[2, "suppressed"] == "N"

    def test_extract_metrics_counts(self):
        df = self.create_sample_counts_data()
        df = self.etl.normalize_column_names(df)
        df = self.etl.standardize_missing_values(df)
        row = df.iloc[0]
        metrics = self.etl.extract_metrics(row)
        expected = {
            "kindergarten_readiness_rate",
            "kindergarten_ready_with_interventions_rate",
            "kindergarten_ready_rate",
            "kindergarten_ready_with_enrichments_rate",
        }
        assert set(metrics.keys()) == expected
        # The cells are published percents. The rate is that percent.
        assert metrics["kindergarten_ready_with_interventions_rate"] == 10
        assert metrics["kindergarten_ready_rate"] == 20
        assert metrics["kindergarten_ready_with_enrichments_rate"] == 5
        assert metrics["kindergarten_readiness_rate"] == 35

    def test_extract_metrics_percentage(self):
        df = self.create_sample_percent_data()
        df = self.etl.normalize_column_names(df)
        df = self.etl.standardize_missing_values(df)
        row = df.iloc[0]
        metrics = self.etl.extract_metrics(row)
        assert metrics["kindergarten_readiness_rate"] == 55.0
        assert metrics["kindergarten_readiness_total"] == 100
        assert metrics["kindergarten_readiness_count"] == 55

    def test_full_transform_pipeline(self):
        counts = self.create_sample_counts_data()
        perc = self.create_sample_percent_data()
        counts.to_csv(self.sample_dir / "counts.csv", index=False)
        perc.to_csv(self.sample_dir / "percent.csv", index=False)
        config = {"derive": {"processing_date": "2025-07-20"}}
        transform(self.raw_dir, self.proc_dir, config)
        output_file = self.proc_dir / "kindergarten_readiness.csv"
        assert output_file.exists()
        df = pd.read_csv(output_file)
        assert len(df.columns) == 19
        assert len(df) == 12
        metrics = set(df["metric"].unique())
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

    def test_kyrc26_file_is_accepted_and_temporary_extracts_are_not(self):
        """KYRC composites are in scope. A leftover temporary extract is not."""
        assert self.etl.include_kyrc26 is True
        accepted = [
            "KYRC26_ASMT_Kindergarten_Screen_Composite.csv",
            "KYRC25_ASMT_Kindergarten_Screen_Composite.csv",
            "KYRC24_ASMT_Kindergarten_Screen_Composite.csv",
            "kindergarten_screen_2023.csv",
            "kindergarten_screen_2020.csv",
        ]
        rejected = [
            "Kindergarten_Screen_2024.CSV",
            "Kindergarten_Screen_2025.CSV",
            "Kindergarten_Screen_2026.CSV",
            "KYRC23_ASMT_Kindergarten_Screen_Composite.csv",
        ]
        for name in accepted + rejected:
            (self.sample_dir / name).write_text("School Year\n20252026\n")

        found = sorted(path.name for path, _sheet in self.etl._find_data_files(self.sample_dir))
        assert found == sorted(accepted)

    def test_comma_tested_count_survives_string_dtype(self):
        """``6,104`` stays numeric on StringDtype and on object."""
        for dtype in ("string", "object"):
            frame = pd.DataFrame({
                "number_tested": pd.Series(["6,104", "20"], dtype=dtype),
                "total_percent_ready": pd.Series(["34.2", "25.0"], dtype=dtype),
                "suppressed": pd.Series(["N", "N"], dtype=dtype),
            })
            cleaned = self.etl.standardize_missing_values(frame)
            assert cleaned.loc[0, "number_tested"] == 6104
            assert cleaned.loc[1, "number_tested"] == 20
            assert cleaned.loc[0, "total_percent_ready"] == 34.2

    def test_coop_totals_use_the_published_coop_code(self):
        """Nine 2018-19 co-op totals share school code 000. The key uses 901-909.

        The workbook publishes those rows as ``---COOP Total---``. A school
        row keeps its own school code.
        """
        coop = pd.Series({
            "school_code": "000",
            "school_name": "---COOP Total---",
            "district_number": "901",
            "co_op_code": "901",
        })
        assert self.etl.extract_school_id(coop) == "901"
        other = coop.copy()
        other["co_op_code"] = "902"
        other["district_number"] = "902"
        assert self.etl.extract_school_id(other) == "902"
        school = pd.Series({
            "school_code": "001020",
            "school_name": "Adair County Primary Center",
        })
        assert self.etl.extract_school_id(school) == "001020"

    def test_suppressed_blank_percent_keeps_rate_and_count(self):
        """A Y row with NUMBER TESTED and a blank percent still names the rate.

        The tested count makes extract_metrics non-empty, so the defaults
        path does not run. The rate and the count have to be in the dict
        or the row keeps only kindergarten_readiness_total.
        """
        row = pd.Series({
            "prior_setting": "All Students",
            "demographic": "African American",
            "suppressed": "Y",
            "number_tested": 3,
            "total_percent_ready": pd.NA,
        })
        metrics = self.etl.extract_metrics(row)
        assert pd.isna(metrics["kindergarten_readiness_rate"])
        assert pd.isna(metrics["kindergarten_readiness_count"])
        assert metrics["kindergarten_readiness_total"] == 3

        published = row.copy()
        published["suppressed"] = "N"
        published["total_percent_ready"] = 51.0
        published["number_tested"] = 100
        kept = self.etl.extract_metrics(published)
        assert kept["kindergarten_readiness_rate"] == 51.0
        assert kept["kindergarten_readiness_count"] == 51
        assert kept["kindergarten_readiness_total"] == 100

    def test_zero_tested_without_a_percent_keeps_the_total(self):
        """NUMBER TESTED 0 with a blank percent is a published total, not a rate."""
        row = pd.Series({
            "prior_setting": "All Students",
            "demographic": "American Indian or Alaska Native",
            "suppressed": "N",
            "number_tested": 0,
            "total_percent_ready": pd.NA,
        })
        metrics = self.etl.extract_metrics(row)
        assert metrics == {"kindergarten_readiness_total": 0}

    def test_suppressed_prior_setting_does_not_emit_the_overall_rate(self):
        """A starred prior setting does not write kindergarten_readiness_rate.

        The overall rate belongs to prior setting All Students. Restoring
        the old defaults, which emitted that rate for every prior setting,
        fails this check.
        """
        row = pd.Series({
            "prior_setting": "Unknown",
            "demographic": "All Students",
            "suppressed": "Y",
            "number_tested": pd.NA,
            "total_percent_ready": pd.NA,
        })
        metrics = self.etl.get_suppressed_metric_defaults(row)
        assert "kindergarten_readiness_rate" not in metrics
        assert "kindergarten_readiness_count" not in metrics
        assert "kindergarten_readiness_total" not in metrics
        assert pd.isna(metrics["kindergarten_unknown_rate"])
        assert pd.isna(metrics["kindergarten_unknown_count"])

    def test_2017_blank_district_is_a_coop_total(self):
        """Districts 901-909 with a blank name are co-op totals, as in 2019.

        A missing county is blank. pandas 2 would otherwise store the text
        ``nan``. A 2019 row that already has the long name is left as published.
        """
        row = pd.Series({
            "school_year": "20172018",
            "district_number": "901",
            "district_name": pd.NA,
            "school_code": "901",
            "school_name": "---District Total---",
            "demographic": "TST",
            "prior_setting": "All Students",
            "suppressed": "N",
            "county_number": "nan",
            "total_percent_ready": 51.3,
            "number_tested": 6685,
        })
        template = self.etl.create_kpi_template(row, "KSCREEN_17-18.xlsx")
        assert template is not None
        assert template["district"] == "Central Kentucky Educational Cooperative"
        assert template["school_name"] == "---COOP Total---"
        assert template["school_id"] == "901"
        assert template["co_op"] == "CKEC"
        assert template["co_op_code"] == "901"
        assert pd.isna(template["county_number"])

        labelled = row.copy()
        labelled["district_name"] = "Central Kentucky Educational Cooperative"
        labelled["school_name"] = "---COOP Total---"
        labelled["co_op"] = "CKEC"
        labelled["co_op_code"] = "901"
        labelled["school_code"] = "000"
        kept = self.etl.create_kpi_template(labelled, "KSCREEN_18-19.xlsx")
        assert kept is not None
        assert kept["district"] == "Central Kentucky Educational Cooperative"
        assert kept["school_name"] == "---COOP Total---"
        assert kept["school_id"] == "901"

        school = row.copy()
        school["district_number"] = "001"
        school["district_name"] = "Adair County"
        school["school_name"] = "Adair County Primary Center"
        school["school_code"] = "001020"
        school["county_number"] = "001"
        adair = self.etl.create_kpi_template(school, "KSCREEN_17-18.xlsx")
        assert adair is not None
        assert adair["district"] == "Adair County"
        assert adair["county_number"] == "001"
        assert adair["school_id"] == "001020"

    def test_workbook_coversheet_labels_are_not_the_shared_map(self):
        """ACD and CSG follow the kindergarten coversheet. The shared yaml is unchanged.

        The 2018-19 data file has ACD and no CSG row. A file that is not
        one of those workbooks still uses the shared map: ACD is dropped
        and CSG becomes Students with Disabilities. Issue #76.
        """
        base = {
            "school_year": "20182019",
            "school_code": "999",
            "district_name": "State",
            "school_name": "---State Total---",
            "prior_setting": "All Students",
            "suppressed": "N",
            "total_percent_ready": 34.2,
            "number_tested": 6104,
        }
        acd = self.etl.create_kpi_template(
            pd.Series({**base, "demographic": "ACD"}),
            "KSCREEN_18-19.xlsx",
        )
        assert acd is not None
        assert acd["student_group"] == "Students with Disabilities (IEP)"
        assert acd["year"] == "2019"

        csg = self.etl.create_kpi_template(
            pd.Series({**base, "demographic": "CSG"}),
            "KSCREEN_17-18.xlsx",
        )
        assert csg is not None
        assert csg["student_group"] == "Consolidated Student Group"

        shared_acd = self.etl.create_kpi_template(
            pd.Series({**base, "demographic": "ACD"}),
            "kindergarten_screen_2020.csv",
        )
        assert shared_acd is None
        shared_csg = self.etl.create_kpi_template(
            pd.Series({**base, "demographic": "CSG"}),
            "kindergarten_screen_2020.csv",
        )
        assert shared_csg is not None
        assert shared_csg["student_group"] == "Students with Disabilities (IEP)"

