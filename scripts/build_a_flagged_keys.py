#!/usr/bin/env python3
"""Write ``notes/flags/A-flagged-keys.csv`` from the postsecondary source files.

Every row is labeled ``kde-recheck``. The reasons are:

* ``starred cohort count``: the cohort cell is ``*``. The metric is
  ``postsecondary_enrollment_total_in_cohort``. That row stays unclassified.
* ``private = total - public leaks suppressed value``: the 7 KYRC26 rows.
  The private count and the private rate are ``*``, and the public and
  total counts are numbers.
* ``total = public + private leaks suppressed value``: the total count is
  ``*`` and the public and private counts are numbers. Marion County High
  School ``375515`` White (non-Hispanic) in 2024 is 71 + 21 = 92. Hancock
  County High School ``225005`` All Students in 2024 is 43 + 21 = 64.
  Hancock's cohort is also ``*``, so that row is listed under both reasons.
* ``public = total - private leaks suppressed value``: South Laurel High
  School ``311210`` Economically Disadvantaged in 2024. The public count
  is ``*``, and 68 - 20 = 48. The cohort is also ``*``, so that row is
  listed under both reasons.

2021 has 789 rows whose private count is ``*`` while the public and total
counts are numbers. Those rows publish a private rate, so they are not
listed. Five 2021 rows hide the public count and publish a public rate.
Those are not listed either.

Reads the six files registered for ``postsecondary_enrollment`` from
``data/raw/postsecondary_enrollment``.

Example
-------
python3 scripts/build_a_flagged_keys.py
python3 scripts/build_a_flagged_keys.py --output /tmp/A-flagged-keys.csv
"""

from __future__ import annotations

import argparse
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
PRIVATE_LEAK = "private = total - public leaks suppressed value"
TOTAL_LEAK = "total = public + private leaks suppressed value"
PUBLIC_LEAK = "public = total - private leaks suppressed value"
COHORT_METRIC = "postsecondary_enrollment_total_in_cohort"
PRIVATE_METRIC = "postsecondary_enrollment_private_ky_college_count"
PUBLIC_METRIC = "postsecondary_enrollment_public_ky_college_count"
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
            cohort_kind, _cohort_number = _cell(row, "total_in_group")
            public_kind, public_number = _cell(row, "public_college_enrolled")
            private_kind, private_number = _cell(row, "private_college_enrolled")
            total_kind, total_number = _cell(row, "college_enrolled_total")
            public_rate_kind, _public_rate = _cell(row, "public_college_rate")
            private_rate_kind, _private_rate = _cell(row, "private_college_rate")

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
                add(PRIVATE_METRIC, PRIVATE_LEAK)
            # The total count is hidden. Public + private is that total.
            # Hancock's cohort is also *, so it keeps the starred-cohort row.
            if (
                total_kind == "star"
                and _is_number(public_kind, public_number)
                and _is_number(private_kind, private_number)
            ):
                add(TOTAL_METRIC, TOTAL_LEAK)
            # The public count and public rate are hidden. Total - private
            # is that public count. A published public rate is the 2021
            # shape and is not listed.
            if (
                public_kind == "star"
                and public_rate_kind == "star"
                and _is_number(total_kind, total_number)
                and _is_number(private_kind, private_number)
            ):
                add(PUBLIC_METRIC, PUBLIC_LEAK)

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
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT,
        help="Where to write the flag list (default: notes/flags/A-flagged-keys.csv)",
    )
    args = parser.parse_args()
    count = write_flags(args.output)
    print(f"Wrote {args.output} ({count} rows)")


if __name__ == "__main__":
    main()
