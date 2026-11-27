#!/usr/bin/env python3
"""Write ``notes/flags/A-flagged-keys.csv`` from the postsecondary source files.

The list has two reasons, both labeled ``kde-recheck``:

* ``starred cohort count``: the cohort cell is ``*``. The metric is
  ``postsecondary_enrollment_total_in_cohort``. That row stays unclassified.
* ``private = total - public leaks suppressed value``: a published count
  recovers a starred count. This is the 7 KYRC26 rows where the private
  count and the private rate are ``*`` and the public and total counts are
  numbers, plus the 2024 Marion County High School ``375515`` White
  (non-Hispanic) row. That cohort is 151, both totals are ``*``, and the
  published public and private counts are 71 and 21.

2021 has 789 rows whose private count is ``*`` while the public and total
counts are numbers. Those rows publish a private rate, so they are not
this shape and are not listed. Hancock County High School ``225005`` All
Students in 2024 hides both totals, but its cohort is ``*``, so it stays
a starred-cohort row only.

Reads the six files registered for ``postsecondary_enrollment`` from
``data/raw/postsecondary_enrollment``.

Example
-------
python3 scripts/build_a_flagged_keys.py
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from etl.postsecondary_enrollment import (  # noqa: E402
    PostsecondaryEnrollmentETL,
    _classify_cell,
)

OUTPUT = ROOT / "notes" / "flags" / "A-flagged-keys.csv"
RAW_DIR = ROOT / "data" / "raw" / "postsecondary_enrollment"
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
MODULE = "postsecondary_enrollment"
LABEL = "kde-recheck"
STARRED_COHORT = "starred cohort count"
LEAK = "private = total - public leaks suppressed value"
COHORT_METRIC = "postsecondary_enrollment_total_in_cohort"
PRIVATE_METRIC = "postsecondary_enrollment_private_ky_college_count"
TOTAL_METRIC = "postsecondary_enrollment_total_ky_college_count"


def registered_names() -> list[str]:
    """File names registered for this source, in config order."""
    config = yaml.safe_load(
        (ROOT / "config" / "kde_sources.yaml").read_text(encoding="utf-8")
    )
    names: list[str] = []
    for entry in config["raw_directories"]["postsecondary_enrollment"]:
        names.append(entry["file"] if isinstance(entry, dict) else entry)
    return names


def _is_number(kind: str, number: float | None) -> bool:
    return kind == "number" and number is not None


def _cell(row: Any, column: str) -> tuple[str, float | None]:
    if column not in row.index:
        return "missing", None
    return _classify_cell(row[column])


def flagged_rows() -> list[dict[str, str]]:
    """Return one flag record per starred cohort and per arithmetic leak."""
    etl = PostsecondaryEnrollmentETL(MODULE)
    records: list[dict[str, str]] = []
    for name in registered_names():
        path = RAW_DIR / name
        if not path.is_file():
            raise SystemExit(f"Missing source file: {path}")
        frame = etl.normalize_column_names(etl._read_data_file(path))
        for _, row in frame.iterrows():
            if etl.should_skip_row(row):
                continue
            year = etl.extract_year(row, name)
            student_group = etl.demographic_mapper.map_demographic(
                row.get("demographic"), year, name
            )
            if student_group is None:
                continue
            district = row.get("district_name", "Unknown District")
            if district is None or str(district).strip() == "":
                district = "Unknown District"
            school_id = etl.extract_school_id(row)
            cohort_kind, cohort_number = _cell(row, "total_in_group")
            public_kind, public_number = _cell(row, "public_college_enrolled")
            private_kind, _private_number = _cell(row, "private_college_enrolled")
            total_kind, total_number = _cell(row, "college_enrolled_total")
            private_rate_kind, _private_rate = _cell(row, "private_college_rate")
            total_rate_kind, _total_rate = _cell(row, "college_enrollment_rate")

            def add(metric: str, reason: str) -> None:
                records.append(
                    {
                        "module": MODULE,
                        "year": year,
                        "district": str(district),
                        "school_id": school_id,
                        "student_group": student_group,
                        "metric": metric,
                        "value": "*",
                        "reason": reason,
                        "label": LABEL,
                    }
                )

            if cohort_kind == "star":
                add(COHORT_METRIC, STARRED_COHORT)
            # Private count and private rate are hidden. Public and total
            # counts are published, so total - public is the private count.
            # A published private rate is the 2021 shape and is not listed.
            if (
                private_kind == "star"
                and private_rate_kind == "star"
                and _is_number(public_kind, public_number)
                and _is_number(total_kind, total_number)
            ):
                add(PRIVATE_METRIC, LEAK)
            # Both totals are hidden and the cohort is a published number
            # greater than 0. Public + private is the hidden total. A starred
            # cohort stays on the starred-cohort reason only.
            if (
                _is_number(cohort_kind, cohort_number)
                and cohort_number is not None
                and cohort_number > 0
                and total_kind == "star"
                and total_rate_kind == "star"
                and _is_number(public_kind, public_number)
                and _is_number(private_kind, _private_number)
            ):
                add(TOTAL_METRIC, LEAK)

    records.sort(
        key=lambda record: (
            record["year"],
            record["reason"],
            record["district"],
            record["school_id"],
            record["student_group"],
            record["metric"],
        )
    )
    return records


def write_flags(path: Path = OUTPUT) -> int:
    """Write the flag list and return the number of data rows."""
    records = flagged_rows()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(records)
    return len(records)


def main() -> None:
    count = write_flags()
    print(f"Wrote {OUTPUT} ({count} rows)")


if __name__ == "__main__":
    main()
