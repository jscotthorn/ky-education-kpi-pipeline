"""Regression tests for etl_runner's dynamic module loader.

``run_etl_module`` loads each pipeline with ``spec_from_file_location``.
That helper does not register the module in ``sys.modules``. A module that
uses ``from __future__ import annotations`` then leaves pydantic unable to
resolve ``Dict`` on ``Config``, and the first ``Config(**cfg)`` raises
``PydanticUserError``. These tests go through that loader.
"""
import inspect
import sys
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal
from pydantic import BaseModel, ValidationError

from etl.kentucky_summative_assessment import transform
from etl_runner import _load_etl_module, run_etl_module

ROOT = Path(__file__).resolve().parents[1]
ETL_DIR = ROOT / "etl"

# Same postponed-annotation model the Kentucky Summative Assessment module
# defines. The completion flag is checked while the module is still
# executing. Calling Config() in that frame would rebuild from the module
# locals and hide a loader that never registered the module. Registration
# after exec_module is also too late: the class body has already run.
_PROBE_SOURCE = '''
from __future__ import annotations

from typing import Any, Dict

from pydantic import BaseModel


class Config(BaseModel):
    rename: Dict[str, str] = {}
    dtype: Dict[str, str] = {}
    derive: Dict[str, Any] = {}


if not Config.__pydantic_complete__:
    raise RuntimeError("Config was not fully defined while the module executed")


def transform(raw_dir, proc_dir, cfg):
    from pathlib import Path

    conf = Config(**cfg)
    Path(proc_dir, "loaded.txt").write_text(
        f"{conf.rename['school']}|{conf.derive['flag']}"
    )
'''

_PROBE_NAME = "etl_runner_postponed_annotation_probe"


def test_run_etl_module_resolves_postponed_annotations(tmp_path, capsys):
    """A pydantic model built under the runner must be usable immediately."""
    module_path = tmp_path / "postponed_annotation_etl.py"
    module_path.write_text(_PROBE_SOURCE)
    raw_dir = tmp_path / "raw"
    proc_dir = tmp_path / "processed"
    raw_dir.mkdir()
    proc_dir.mkdir()
    sys.modules.pop(_PROBE_NAME, None)
    try:
        run_etl_module(
            _PROBE_NAME,
            raw_dir,
            proc_dir,
            {"rename": {"school": "school_name"}, "derive": {"flag": 1}},
            module_path=module_path,
        )
        captured = capsys.readouterr()
        assert "not fully defined" not in captured.out
        assert "not fully defined" not in captured.err
        assert f"✅ Completed {_PROBE_NAME} pipeline" in captured.out
        assert (proc_dir / "loaded.txt").read_text() == "school_name|1"
    finally:
        sys.modules.pop(_PROBE_NAME, None)


def test_loader_builds_pydantic_models_for_every_etl_module():
    """Every ETL file, loaded the way the runner loads it, can build its models.

    Kentucky Summative Assessment is the module that failed. ``kyrc`` and
    ``safe_schools_discipline`` also postpone annotations, but they do not
    define a pydantic model. This walks the directory so a later module
    with the same pattern fails here.
    """
    for path in sorted(ETL_DIR.glob("*.py")):
        if path.name == "__init__.py":
            continue
        module_name = f"etl_loader_probe_{path.stem}"
        sys.modules.pop(module_name, None)
        try:
            module = _load_etl_module(module_name, path)
            for _name, cls in inspect.getmembers(module, inspect.isclass):
                if cls.__module__ != module_name or not issubclass(cls, BaseModel):
                    continue
                assert cls.__pydantic_complete__, path.name
                try:
                    cls()
                except ValidationError:
                    # The model built. Required fields are a separate concern.
                    pass
        finally:
            sys.modules.pop(module_name, None)


def _sample_ksa_csv(path: Path) -> None:
    pd.DataFrame(
        {
            "School Year": ["20232024"],
            "County Name": ["ADAIR"],
            "District Name": ["Adair County"],
            "School Name": ["All Schools"],
            "School Code": ["001000"],
            "Level": ["Elementary School"],
            "Subject": ["Mathematics"],
            "Demographic": ["All Students"],
            "Suppressed": ["N"],
            "Novice": [28],
            "Apprentice": [32],
            "Proficient": [31],
            "Distinguished": [10],
            "Proficient / Distinguished": [41],
            "Content Index": [58.7],
        }
    ).to_csv(path, index=False)


def _kpi_without_timestamp(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    return frame.drop(columns=["last_updated"]).reset_index(drop=True)


def test_runner_ksa_output_matches_direct_import(tmp_path, capsys):
    """Runner-loaded KSA writes the same KPI rows as a normal import.

    ``last_updated`` is the clock at conversion time, so it is not compared.
    """
    direct_raw = tmp_path / "direct_raw"
    direct_proc = tmp_path / "direct_processed"
    runner_raw = tmp_path / "runner_raw"
    runner_proc = tmp_path / "runner_processed"
    for raw, proc in ((direct_raw, direct_proc), (runner_raw, runner_proc)):
        (raw / "kentucky_summative_assessment").mkdir(parents=True)
        proc.mkdir()
    _sample_ksa_csv(direct_raw / "kentucky_summative_assessment" / "sample.csv")
    _sample_ksa_csv(runner_raw / "kentucky_summative_assessment" / "sample.csv")

    transform(direct_raw, direct_proc, {})

    previous = sys.modules.get("kentucky_summative_assessment")
    try:
        run_etl_module("kentucky_summative_assessment", runner_raw, runner_proc, {})
        captured = capsys.readouterr()
        assert "not fully defined" not in captured.out
        assert "not fully defined" not in captured.err
        assert "✅ Completed kentucky_summative_assessment pipeline" in captured.out
    finally:
        if previous is None:
            sys.modules.pop("kentucky_summative_assessment", None)
        else:
            sys.modules["kentucky_summative_assessment"] = previous

    direct_csv = direct_proc / "kentucky_summative_assessment.csv"
    runner_csv = runner_proc / "kentucky_summative_assessment.csv"
    assert direct_csv.exists()
    assert runner_csv.exists()
    direct = _kpi_without_timestamp(direct_csv)
    runner = _kpi_without_timestamp(runner_csv)
    assert not direct.empty
    assert_frame_equal(runner, direct)
