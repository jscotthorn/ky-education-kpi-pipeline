#!/usr/bin/env python3
"""Write ``notes/flags/B-flagged-keys.csv`` from the teacher-equity sources.

Every row is labeled ``kde-recheck``. The reasons are:

* ``absolute-value gap year``: an equity-gap cell in a year KDE publishes
  as the absolute value. Those years are KYRC24 and KYRC25 for novice
  equity and for out-of-field. An ineffective year is added only when its
  published gap equals ``|Title I - Not Title I|`` on every row where the
  two sides differ.
* ``negative complement cell``: a Title I group cell below 0. KDE computed
  the complement by subtraction. Issue #64 counts 30 in 2022 and 10 in 2023.
* ``paired complement cell``: the positive partner of that cell
  (Non-White/White, economically disadvantaged, disability, English learner).
* ``UAT-negated gap``: a gap cell in a file this config loads from
  ``kyrc_uat`` whose value is the negation of the datasets cell. Pass
  ``--datasets-root`` so the comparison can run. A file loaded from
  datasets is not listed, even when UAT negates it.

The gap is not recomputed. The list is the published cells.

Reads the three registered directories under ``data/raw`` (override with
``--raw-root``).

Example
-------
python3 scripts/build_b_flagged_keys.py --raw-root /path/to/raw
python3 scripts/build_b_flagged_keys.py --raw-root /path/to/raw --datasets-root /path/to/datasets
"""

from __future__ import annotations

import argparse
import csv
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from etl.base_etl import Config  # noqa: E402
from etl.novice_teachers import transform as transform_novice  # noqa: E402
from etl.students_taught_by_ineffective_teachers import (  # noqa: E402
    transform as transform_ineffective,
)
from etl.students_taught_by_out_of_field_teachers import (  # noqa: E402
    transform as transform_out_of_field,
)

OUTPUT = ROOT / "notes" / "flags" / "B-flagged-keys.csv"
COLUMNS = (
    "module",
    "year",
    "district",
    "school_id",
    "student_group",
    "metric",
    "value",
    "reason",
    "label",
)
LABEL = "kde-recheck"
ABSOLUTE = "absolute-value gap year"
NEGATIVE = "negative complement cell"
PAIRED = "paired complement cell"
UAT_NEGATED = "UAT-negated gap"
# Named by the 27 Nov decision. The data check below is printed, not used
# to drop these years. A few cells in each year are 0.1 off the absolute
# difference. They stay in the year.
NAMED_ABSOLUTE_YEARS = {
    "novice_teachers": frozenset({"2024", "2025"}),
    "students_taught_by_out_of_field_teachers": frozenset({"2024", "2025"}),
}
TRANSFORMS = {
    "novice_teachers": transform_novice,
    "students_taught_by_ineffective_teachers": transform_ineffective,
    "students_taught_by_out_of_field_teachers": transform_out_of_field,
}
PAIRS = {
    "Non-White": "White (non-Hispanic)",
    "White (non-Hispanic)": "Non-White",
    "Economically Disadvantaged": "Non-Economically Disadvantaged",
    "Non-Economically Disadvantaged": "Economically Disadvantaged",
    "Students with Disabilities (IEP)": "Student without Disabilities (IEP)",
    "Student without Disabilities (IEP)": "Students with Disabilities (IEP)",
    "English Learner": "Non-English Learner",
    "Non-English Learner": "English Learner",
}
KEY = ("year", "school_id", "student_group")
Row = Tuple[str, str, str, str, str, str, str, str, str]


def _config() -> dict:
    return Config(
        derive={"processing_date": "2026-11-27", "data_quality_flag": "reviewed"}
    ).model_dump()


def _read_kpi(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, dtype=str)
    frame["value_num"] = pd.to_numeric(frame["value"], errors="coerce")
    return frame


def _run(raw_root: Path, proc_dir: Path) -> Dict[str, pd.DataFrame]:
    proc_dir.mkdir(parents=True, exist_ok=True)
    cfg = _config()
    frames: Dict[str, pd.DataFrame] = {}
    for name, transform in TRANSFORMS.items():
        transform(raw_root, proc_dir, cfg)
        frames[name] = _read_kpi(proc_dir / f"{name}.csv")
    return frames


def _kind(metric: str) -> str:
    if metric.endswith("_equity_gap"):
        return "gap"
    if metric.endswith("_not_title_1"):
        return "not"
    if metric.endswith("_title_1"):
        return "title"
    return "other"


def _side(frame: pd.DataFrame, kind: str, value_name: str) -> pd.DataFrame:
    part = frame[frame["metric"].map(_kind) == kind][
        ["year", "school_id", "student_group", "value_num"]
    ].rename(columns={"value_num": value_name})
    return part.drop_duplicates(list(KEY))


def absolute_years_from_data(frame: pd.DataFrame) -> Tuple[Set[str], List[str]]:
    """Years whose gap equals ``|Title I - Not Title I|`` on every differing row.

    A row where Title I equals Not Title I is not a test. Equality allows
    0.001 for binary floating point. A residual of 0.1 fails the year.
    """
    joined = (
        _side(frame, "gap", "gap")
        .merge(_side(frame, "title", "title"), on=list(KEY))
        .merge(_side(frame, "not", "nott"), on=list(KEY))
    )
    notes: List[str] = []
    years: Set[str] = set()
    if joined.empty:
        return years, ["no paired Title I / Not Title I / gap rows"]
    differ = (joined["title"] - joined["nott"]).abs() > 1e-9
    for year, group in joined.groupby("year"):
        differing = group[differ.loc[group.index]]
        if differing.empty:
            notes.append(f"{year}: no row where Title I and Not Title I differ")
            continue
        residual = (differing["gap"] - (differing["title"] - differing["nott"]).abs()).abs()
        matched = int((residual <= 0.001).sum())
        off = int((residual > 0.001).sum())
        all_match = bool((residual <= 0.001).all())
        notes.append(
            f"{year}: {len(differing)} differing rows, {matched} equal |Title I - Not Title I|, "
            f"{off} do not, max residual {float(residual.max()):.4f}"
        )
        if all_match:
            years.add(str(year))
    return years, notes


def _cell(frame_row: pd.Series, module: str, reason: str) -> Row:
    return (
        module,
        str(frame_row["year"]),
        str(frame_row["district"]),
        str(frame_row["school_id"]),
        str(frame_row["student_group"]),
        str(frame_row["metric"]),
        str(frame_row["value"]),
        reason,
        LABEL,
    )


def absolute_rows(frames: Dict[str, pd.DataFrame]) -> Tuple[List[Row], List[str]]:
    rows: List[Row] = []
    notes: List[str] = []
    for module, named in NAMED_ABSOLUTE_YEARS.items():
        frame = frames[module]
        found, detail = absolute_years_from_data(frame)
        notes.append(f"{module} paired-gap check:")
        notes.extend(f"  {line}" for line in detail)
        extra = found - named
        if extra:
            notes.append(
                f"  {module} also matches the absolute-value test in {sorted(extra)}"
            )
        years = set(named) | found
        gap = frame[
            frame["metric"].map(_kind).eq("gap") & frame["year"].isin(years)
        ]
        notes.append(f"  flagged {len(gap)} {module} gap cells in {sorted(years)}")
        for record in gap.itertuples(index=False):
            rows.append(_cell(pd.Series(record._asdict()), module, ABSOLUTE))
    ineffective = frames["students_taught_by_ineffective_teachers"]
    found, detail = absolute_years_from_data(ineffective)
    notes.append("students_taught_by_ineffective_teachers paired-gap check:")
    notes.extend(f"  {line}" for line in detail)
    if found:
        gap = ineffective[
            ineffective["metric"].map(_kind).eq("gap") & ineffective["year"].isin(found)
        ]
        notes.append(f"  flagged {len(gap)} ineffective gap cells in {sorted(found)}")
        for record in gap.itertuples(index=False):
            rows.append(
                _cell(
                    pd.Series(record._asdict()),
                    "students_taught_by_ineffective_teachers",
                    ABSOLUTE,
                )
            )
    else:
        notes.append("  no ineffective year is an absolute-value gap year")
    return rows, notes


def complement_rows(frame: pd.DataFrame) -> Tuple[List[Row], List[str]]:
    """Title I cells below 0, and the positive cell in the same complement pair."""
    title = frame[frame["metric"].map(_kind).eq("title")].copy()
    negatives = title[title["value_num"] < 0]
    index = title.set_index(list(KEY))
    rows: List[Row] = []
    missing = 0
    module = "students_taught_by_ineffective_teachers"
    for record in negatives.itertuples(index=False):
        rows.append(_cell(pd.Series(record._asdict()), module, NEGATIVE))
        partner = PAIRS.get(str(record.student_group))
        if partner is None:
            missing += 1
            continue
        key = (str(record.year), str(record.school_id), partner)
        if key not in index.index:
            missing += 1
            continue
        partner_row = index.loc[key]
        if isinstance(partner_row, pd.DataFrame):
            partner_row = partner_row.iloc[0]
        if float(partner_row["value_num"]) <= 0:
            missing += 1
            continue
        partner_full = partner_row.copy()
        partner_full["year"] = key[0]
        partner_full["school_id"] = key[1]
        partner_full["student_group"] = partner
        rows.append(_cell(partner_full, module, PAIRED))
    by_year = negatives.groupby("year").size().to_dict()
    note = (
        f"negative Title I complement cells {len(negatives)} by year {by_year}; "
        f"paired rows {sum(1 for row in rows if row[7] == PAIRED)}; "
        f"unpaired {missing}"
    )
    return rows, [note]


def _uat_files() -> List[Tuple[str, str]]:
    """``(module, filename)`` registered with ``url: kyrc_uat``."""
    config = yaml.safe_load((ROOT / "config" / "kde_sources.yaml").read_text())
    found: List[Tuple[str, str]] = []
    for module, entries in config["raw_directories"].items():
        for entry in entries:
            if isinstance(entry, dict) and entry.get("url") == "kyrc_uat":
                found.append((module, str(entry["file"])))
    return found


def _one_file_kpi(module: str, source: Path, work: Path) -> pd.DataFrame:
    raw = work / "raw"
    target = raw / module
    target.mkdir(parents=True)
    shutil.copy(source, target / source.name)
    proc = work / "proc"
    proc.mkdir()
    TRANSFORMS[module](raw, proc, _config())
    return _read_kpi(proc / f"{module}.csv")


def uat_negated_rows(
    frames: Dict[str, pd.DataFrame],
    raw_root: Path,
    datasets_root: Path,
    work: Path,
) -> Tuple[List[Row], List[str]]:
    """Gap cells loaded from UAT that negate the datasets cell."""
    rows: List[Row] = []
    notes: List[str] = []
    for module, filename in _uat_files():
        loaded = raw_root / module / filename
        other = datasets_root / filename
        if not loaded.is_file() or not other.is_file():
            notes.append(f"{filename}: skipped, loaded={loaded.is_file()} datasets={other.is_file()}")
            continue
        other_kpi = _one_file_kpi(module, other, work / filename)
        loaded_gap = frames[module]
        loaded_gap = loaded_gap[
            (loaded_gap["source_file"] == filename)
            & loaded_gap["metric"].map(_kind).eq("gap")
        ]
        other_gap = other_kpi[other_kpi["metric"].map(_kind).eq("gap")]
        if loaded_gap.empty or other_gap.empty:
            notes.append(
                f"{filename}: loaded gap cells {len(loaded_gap)}, datasets gap cells {len(other_gap)}"
            )
            continue
        other_side = other_gap.set_index(
            ["year", "school_id", "student_group", "metric"]
        )["value_num"]
        negated = 0
        for record in loaded_gap.itertuples(index=False):
            key = (str(record.year), str(record.school_id), str(record.student_group), str(record.metric))
            if key not in other_side.index:
                continue
            other_value = float(other_side.loc[key])
            if other_value == 0 or pd.isna(other_value):
                continue
            if abs(float(record.value_num) + other_value) <= 0.001:
                negated += 1
                rows.append(_cell(pd.Series(record._asdict()), module, UAT_NEGATED))
        notes.append(f"{filename}: {negated} loaded gap cells negate the datasets cell")
    if not notes:
        notes.append("no kyrc_uat file was compared")
    return rows, notes


def restored_notes(frames: Dict[str, pd.DataFrame]) -> List[str]:
    """Counts of published negatives. Novice and ineffective used to drop them."""
    notes: List[str] = []
    for module, frame in frames.items():
        neg = frame[frame["value_num"] < 0]
        by_year = neg.groupby("year").size().to_dict()
        gap = neg[neg["metric"].map(_kind).eq("gap")].groupby("year").size().to_dict()
        title = neg[neg["metric"].map(_kind).eq("title")].groupby("year").size().to_dict()
        notes.append(
            f"{module}: {len(neg)} negative cells, gap {gap}, title {title}, by year {by_year}"
        )
    return notes


def write_csv(path: Path, rows: Sequence[Row]) -> None:
    ordered = sorted(set(rows), key=lambda row: row)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(COLUMNS)
        writer.writerows(ordered)


def build(raw_root: Path, datasets_root: Optional[Path], output: Path) -> List[str]:
    notes: List[str] = []
    with tempfile.TemporaryDirectory(prefix="b-flags-") as tmp:
        work = Path(tmp)
        frames = _run(raw_root, work / "main")
        notes.extend(restored_notes(frames))
        rows, abs_notes = absolute_rows(frames)
        notes.extend(abs_notes)
        comp, comp_notes = complement_rows(frames["students_taught_by_ineffective_teachers"])
        rows.extend(comp)
        notes.extend(comp_notes)
        if datasets_root is not None:
            uat_rows, uat_notes = uat_negated_rows(frames, raw_root, datasets_root, work / "uat")
            rows.extend(uat_rows)
            notes.extend(uat_notes)
        else:
            notes.append("datasets root not passed; UAT-negated comparison was not run")
        write_csv(output, rows)
        notes.append(f"wrote {len(set(rows))} rows to {output}")
    return notes


def main(argv: Optional[Sequence[str]] = None) -> int:
    import logging
    logging.basicConfig(level=logging.WARNING)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-root", type=Path, default=ROOT / "data" / "raw")
    parser.add_argument("--datasets-root", type=Path, default=None)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args(argv)
    for line in build(args.raw_root, args.datasets_root, args.output):
        print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
