"""Catalog covariate keys must name a metric an ETL module emits.

Emitted names come from ``etl/*.py``: string literals assigned as KPI
metrics, plus names built from constants in those modules (Title I
suffixes, school-type codes, climate levels, course-type prefixes,
impact-measure labels). The test does not read ``data/raw`` and does
not download KDE files.

Issue #64 item 4: a catalog entry that matches no emitted metric fails
this test, except the names in ``ORPHAN_ALLOWLIST``. Those other orphans
are reported and not repointed.
"""
from __future__ import annotations

import ast
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
ETL_DIR = ROOT / "etl"
CATALOG_PATH = ROOT / "analysis" / "config" / "covariate_catalog.yaml"

# Catalog keys that name no metric an ETL module emits.
# Issue #64 item 4: report these orphans and do not repoint them.
# Census and demographic fields are analysis inputs, not KPI metric names.
# A few teacher and spending keys are the analysis column, while the module
# emits a different metric name. The twc_* keys are the catalog names; the
# module emits teacher_working_conditions_*.
ORPHAN_ALLOWLIST = frozenset(
    {
        "county_child_poverty_rate",
        "county_median_income",
        "county_poverty_rate",
        "emergency_provisional_rate",
        "novice_teacher_rate",
        "pct_african_american",
        "pct_economically_disadvantaged",
        "pct_english_learners",
        "pct_hispanic",
        "pct_minority",
        "pct_students_with_disabilities",
        "per_pupil_spending",
        "teacher_avg_experience",
        "tract_median_household_income",
        "tract_pct_bachelors_plus",
        "tract_pct_broadband",
        "tract_pct_housing_cost_burden_30_plus",
        "tract_pct_owner_occupied",
        "tract_pct_single_parent",
        "tract_poverty_rate",
        "tract_unemployment_rate",
        "twc_managing_student_behavior",
        "twc_school_climate",
        "twc_school_leadership",
    }
)

_SKIP_ETL = {"__init__.py", "template.py"}
_COURSE_PREFIXES = ("ap", "ib", "cambridge", "advanced")


def _const_str(node: ast.AST | None) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _metricish(text: str) -> bool:
    if not text or text.startswith("_") or not text.islower() or "_" not in text:
        return False
    if any(char in text for char in " /."):
        return False
    body = text.replace("_", "")
    return body.isalnum()


def _kpi_name(text: str) -> str:
    """The metric column drops a ``__demographic`` suffix."""
    if "__" in text:
        return text.split("__", 1)[0]
    return text


def _literal_metric_names() -> set[str]:
    """String literals ETL modules assign as KPI metric names."""
    found: set[str] = set()
    for path in sorted(ETL_DIR.glob("*.py")):
        if path.name in _SKIP_ETL:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Dict):
                for key, value in zip(node.keys, node.values):
                    for item in (_const_str(key), _const_str(value)):
                        if item and _metricish(item):
                            found.add(_kpi_name(item))
            if not isinstance(node, ast.Assign):
                continue
            for target in node.targets:
                if isinstance(target, ast.Subscript):
                    key = _const_str(target.slice)
                    if key and _metricish(key):
                        found.add(_kpi_name(key))
                    if key == "metric":
                        literal = _const_str(node.value)
                        if literal and _metricish(literal):
                            found.add(_kpi_name(literal))
    return found


def _expanded_metric_names() -> set[str]:
    """Metric names built from constants, not from a downloaded file."""
    names: set[str] = set()

    from etl.district_school_list import DistrictSchoolListETL

    for code in DistrictSchoolListETL.SCHOOL_TYPE_DUMMIES:
        names.add(f"school_type_{code.lower()}")
    names.update(
        {
            "title_i_schoolwide",
            "title_i_targeted",
            "title_i_eligible_no_program",
        }
    )

    from etl.students_taught_by_ineffective_teachers import _TITLE_I_SUFFIX as ineffective

    for suffix in set(ineffective.values()):
        names.add(f"students_taught_by_ineffective_teachers_rate_{suffix}")

    from etl.students_taught_by_out_of_field_teachers import _TITLE_I_SUFFIX as out_of_field

    for suffix in set(out_of_field.values()):
        names.add(f"students_taught_by_out_of_field_teachers_rate_{suffix}")

    from etl.novice_teachers import _TITLE_STATUSES

    for status in _TITLE_STATUSES:
        suffix = status.lower().replace(" ", "_")
        names.add(f"students_taught_by_inexperienced_teachers_rate_{suffix}")
    names.update(
        {
            "novice_teacher_rate_less_than_1_year",
            "novice_teacher_rate_1_to_3_years",
        }
    )

    from etl.safe_schools_climate import LEVEL_SUFFIXES, OUTSIDE_DOORS_METRIC

    for suffix in set(LEVEL_SUFFIXES.values()):
        names.add(f"climate_index_score_{suffix}")
        names.add(f"safety_index_score_{suffix}")
        names.add(f"climate_safety_indicator_rate_{suffix}")
        names.add(f"climate_safety_combined_indicator_rate_{suffix}")
    names.add(OUTSIDE_DOORS_METRIC)
    names.add("safety_policy_compliance_rate")

    from etl.teacher_working_conditions import TeacherWorkingConditionsETL

    for suffix in set(TeacherWorkingConditionsETL.IMPACT_MEASURE_MAP.values()):
        names.add(f"teacher_working_conditions_{suffix}")

    from etl.advanced_coursework import _OVERVIEW_METRICS, _PARTICIPATION_METRICS

    for prefix in _COURSE_PREFIXES:
        for template in list(_OVERVIEW_METRICS.values()) + list(_PARTICIPATION_METRICS.values()):
            names.add(template.format(prefix=prefix))
        names.add(f"num_{prefix}_courses_offered")
        names.add(f"{prefix}_qualifying_score_rate")
    return names


def emitted_metric_names() -> set[str]:
    """KPI metric names the ETL modules can write."""
    return _literal_metric_names() | _expanded_metric_names()


def catalog_covariate_names() -> set[str]:
    catalog = yaml.safe_load(CATALOG_PATH.read_text(encoding="utf-8"))
    names: set[str] = set()
    for covariates in catalog.get("covariates", {}).values():
        names.update(covariates.keys())
    return names


def catalog_orphans() -> set[str]:
    return catalog_covariate_names() - emitted_metric_names()


def test_catalog_orphans_match_the_allowlist():
    """Every catalog key matches an emitted metric, or is on the allowlist.

    The allowlist is the orphans left after repointing the ineffective
    and out-of-field entries and removing ``has_algebra_1_access_grade_8``.
    Issue #64 item 4.
    """
    orphans = catalog_orphans()
    assert orphans == ORPHAN_ALLOWLIST


def test_repointed_equity_entries_name_emitted_metrics():
    """The Title I metric is the name each equity module writes."""
    emitted = emitted_metric_names()
    catalog = catalog_covariate_names()
    for name in (
        "students_taught_by_ineffective_teachers_rate_title_1",
        "students_taught_by_out_of_field_teachers_rate_title_1",
    ):
        assert name in catalog
        assert name in emitted
    for old in (
        "students_taught_by_ineffective_teachers_rate",
        "students_taught_by_out_of_field_teachers_rate",
        "has_algebra_1_access_grade_8",
    ):
        assert old not in catalog


def test_emergency_provisional_source_is_teacher_certification():
    """Issue #70: the catalog names the KYRC teacher certification files."""
    catalog = yaml.safe_load(CATALOG_PATH.read_text(encoding="utf-8"))
    info = catalog["covariates"]["teacher_quality"]["emergency_provisional_rate"]
    assert info["source"] == "kde_teacher_certification"


def test_indicator_covariates_resolve_in_the_catalog():
    """Analysis loaders can look up every covariate an indicator lists."""
    from analysis.config import get_covariate_info, load_covariate_catalog

    catalog = load_covariate_catalog()
    listed = []
    for config in catalog.get("indicators", {}).values():
        for key in (
            "required_covariates",
            "recommended_covariates",
            "optional_covariates",
            "excluded_covariates",
        ):
            listed.extend(config.get(key) or [])
    assert listed
    for name in listed:
        info = get_covariate_info(name)
        assert info["category"]
