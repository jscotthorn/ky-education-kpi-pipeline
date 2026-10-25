#!/usr/bin/env python3
"""Diff three KYRC25 files: Open House 10 Feb 2026 vs later datasets copies.

The files are the accountability Kentucky Summative Assessment file, the
postsecondary readiness file, and the 4-year graduation file. For each file
the script reads three public copies:

- Open House HistoricalDatasets (the February copy found for this note)
- the live datasets container
- the datasetsuat6051276 container

It prints Last-Modified, size, MD5, and row count, then diffs the datasets
copy against the historical copy on the natural key. UAT is compared by
hash. The script writes nothing in the repository. Downloaded bytes go to
the cache directory (default /tmp/kyrc25-republish-diffs).

Stdlib only. Run from anywhere:

    python3 docs/decisions/republish-diffs/scripts/diff_kyrc25_2026_rewrites.py

The numbers in docs/decisions/republish-diffs/kyrc25-2026-rewrites.md are
from the 25 October 2026 run of this script.
"""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import statistics
import urllib.request
from collections import Counter
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Sequence, Tuple

HEADERS = {
    "Referer": "https://www.education.ky.gov/Open-House/data/Pages/Historical-SRC-Datasets.aspx",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
    ),
}

HOSTS = {
    "hist": "https://www.education.ky.gov/Open-House/data/HistoricalDatasets/",
    "ds": "https://kdeschoolreportcard.blob.core.windows.net/datasets/",
    "uat": "https://kdeschoolreportcard.blob.core.windows.net/datasetsuat6051276/",
}

FILES = {
    "ksa": "KYRC25_ACCT_Kentucky_Summative_Assessment.csv",
    "psr": "KYRC25_ACCT_Postsecondary_Readiness.csv",
    "grad": "KYRC25_ACCT_4_Year_High_School_Graduation.csv",
}

KEYS = {
    "ksa": ["School Year", "School Code", "Level", "Subject", "Demographic"],
    "psr": ["School Year", "School Code", "Demographic"],
    "grad": ["School Year", "School Code", "Demographic"],
}

MEASURES = {
    "ksa": [
        "Novice",
        "Apprentice",
        "Proficient",
        "Distinguished",
        "Proficient / Distinguished",
        "Content Index",
    ],
    "psr": ["Postsecondary Rate", "Postsecondary Rate With Bonus"],
    "grad": ["4 Year Cohort Graduation Rate"],
}

LEVELS = ["Novice", "Apprentice", "Proficient", "Distinguished"]
Row = Dict[str, str]
Key = Tuple[str, ...]


def probe(url: str) -> Dict[str, str]:
    """HEAD the object. Open House answers HEAD, and so does the blob host."""
    request = urllib.request.Request(url, method="HEAD", headers=HEADERS)
    with urllib.request.urlopen(request, timeout=60) as response:
        headers = {k.lower(): v for k, v in response.headers.items()}
        return {
            "status": str(response.status),
            "url": response.geturl(),
            "last_modified": headers.get("last-modified", ""),
            "content_length": headers.get("content-length", ""),
            "content_md5": headers.get("content-md5", ""),
            "etag": headers.get("etag", ""),
        }


def download(url: str, dest: Path) -> None:
    request = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(request, timeout=180) as response, dest.open("wb") as handle:
        while True:
            chunk = response.read(1024 * 256)
            if not chunk:
                break
            handle.write(chunk)


def load_csv(path: Path) -> Tuple[List[str], List[Row]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise SystemExit(f"{path} has no header")
        return list(reader.fieldnames), list(reader)


def index_rows(rows: Sequence[Row], keys: Sequence[str]) -> Dict[Key, Row]:
    indexed: Dict[Key, Row] = {}
    duplicates = 0
    for row in rows:
        key = tuple(row[column] for column in keys)
        if key in indexed:
            duplicates += 1
        indexed[key] = row
    if duplicates:
        raise SystemExit(f"{duplicates} duplicate natural keys")
    return indexed


def is_number(value: str) -> bool:
    text = value.strip().replace(",", "")
    if text == "":
        return False
    try:
        float(text)
    except ValueError:
        return False
    return True


def as_number(value: str) -> float:
    return float(value.strip().replace(",", ""))


def is_fayette(row: Row) -> bool:
    """District number 165, or a 165xxx school code (including 165000)."""
    district = row.get("District Number", "").strip()
    school = row.get("School Code", "").strip()
    return district == "165" or school.startswith("165")


def is_state(row: Row) -> bool:
    """District number 999, or school code 999000."""
    district = row.get("District Number", "").strip()
    school = row.get("School Code", "").strip()
    return district == "999" or school == "999000"


def cell_kind(column: str, old: str, new: str) -> str:
    """Classify one changed cell. Whitespace is stripped for the kind only."""
    old_text, new_text = old.strip(), new.strip()
    if old_text == new_text:
        return "whitespace_only"
    old_num, new_num = is_number(old_text), is_number(new_text)
    if old_num and new_num:
        if as_number(old_text) == as_number(new_text):
            return "numeric_format"
        return "numeric"
    old_star = set(old_text) <= {"*"} and "*" in old_text
    new_star = set(new_text) <= {"*"} and "*" in new_text
    if old_star or new_star:
        if old_star and new_star:
            return "star_token"
        if old_num or new_num:
            return "star_number"
        if old_text == "" or new_text == "":
            return "star_blank"
        return "star_other"
    if column == "Suppressed":
        return "suppression_flag"
    return "other_text"


def bucket_abs(delta: float) -> str:
    rounded = round(abs(delta), 4)
    if abs(rounded - 0.1) < 1e-9:
        return "0.1"
    if rounded < 1:
        return "(0.1, 1)"
    if abs(rounded - 1) < 1e-9:
        return "1"
    if rounded <= 5:
        return "(1, 5]"
    return ">5"


def content_index_stats(rows: Iterable[Row]) -> Tuple[int, int, float]:
    values: List[float] = []
    for row in rows:
        text = row["Content Index"].strip()
        if not is_number(text):
            continue
        values.append(as_number(text))
    over = sum(1 for value in values if value > 100)
    return len(values), over, max(values)


def ensure_copy(cache: Path, host: str, filename: str, refresh: bool) -> Tuple[Path, Dict[str, str]]:
    url = HOSTS[host] + filename
    dest = cache / f"{host}__{filename}"
    meta = probe(url)
    if refresh or not dest.exists():
        download(url, dest)
    data = dest.read_bytes()
    meta["bytes"] = str(len(data))
    meta["md5"] = hashlib.md5(data).hexdigest()
    meta["header_md5_matches"] = ""
    if meta["content_md5"]:
        meta["header_md5_matches"] = str(
            base64.b64decode(meta["content_md5"]) == hashlib.md5(data).digest()
        )
    return dest, meta


def print_copies(name: str, metas: Mapping[str, Mapping[str, str]], columns: Sequence[str], row_count: int) -> None:
    print(f"\n## {name}")
    print(f"columns={len(columns)} data_rows={row_count}")
    print("columns:", ", ".join(columns))
    for host in ("hist", "ds", "uat"):
        meta = metas[host]
        print(
            f"{host}\turl={meta['url']}\tLast-Modified={meta['last_modified']}\t"
            f"bytes={meta['bytes']}\tContent-Length={meta['content_length']}\t"
            f"md5={meta['md5']}\tContent-MD5={meta['content_md5'] or '(none)'}\t"
            f"header_md5_matches={meta['header_md5_matches'] or '(no header)'}\t"
            f"etag={meta['etag']}"
        )


def diff_file(name: str, old_rows: Sequence[Row], new_rows: Sequence[Row], columns: Sequence[str]) -> None:
    keys = KEYS[name]
    old_index = index_rows(old_rows, keys)
    new_index = index_rows(new_rows, keys)
    added = sorted(set(new_index) - set(old_index))
    removed = sorted(set(old_index) - set(new_index))
    print(f"natural_key={keys}")
    print(f"added_rows={len(added)} removed_rows={len(removed)} shared_keys={len(set(old_index) & set(new_index))}")
    if added or removed:
        print("added sample", added[:5])
        print("removed sample", removed[:5])

    years_old = Counter(row["School Year"] for row in old_rows)
    years_new = Counter(row["School Year"] for row in new_rows)
    print(f"school_year_hist={dict(years_old)} school_year_datasets={dict(years_new)}")

    column_counts: Counter[str] = Counter()
    kind_counts: Counter[str] = Counter()
    kind_by_column: Dict[str, Counter[str]] = {column: Counter() for column in columns}
    flag_pairs: Counter[Tuple[str, str]] = Counter()
    changed_rows = 0
    fayette_rows = sum(1 for row in old_rows if is_fayette(row))
    state_rows = sum(1 for row in old_rows if is_state(row))
    fayette_names = sorted({row["District Name"] for row in old_rows if is_fayette(row)})
    state_names = sorted({row["District Name"] for row in old_rows if is_state(row)})
    fayette_cells: List[Tuple[str, ...]] = []
    state_cells: List[Tuple[str, ...]] = []
    numeric_deltas: List[float] = []
    deltas_by_column: Dict[str, List[float]] = {column: [] for column in columns}
    cell_buckets: Counter[str] = Counter()
    raw_pairs: Counter[Tuple[str, str]] = Counter()

    print(f"fayette_rows={fayette_rows} district_names={fayette_names}")
    print(f"state_rows={state_rows} district_names={state_names}")

    for key in sorted(set(old_index) & set(new_index)):
        old, new = old_index[key], new_index[key]
        diffs = [(column, old[column], new[column]) for column in columns if old[column] != new[column]]
        if not diffs:
            continue
        changed_rows += 1
        flag_pairs[(old.get("Suppressed", ""), new.get("Suppressed", ""))] += 1
        for column, old_value, new_value in diffs:
            kind = cell_kind(column, old_value, new_value)
            column_counts[column] += 1
            kind_counts[kind] += 1
            kind_by_column[column][kind] += 1
            raw_pairs[(repr(old_value), repr(new_value))] += 1
            if kind == "numeric":
                delta = as_number(new_value) - as_number(old_value)
                numeric_deltas.append(delta)
                deltas_by_column[column].append(delta)
                cell_buckets[bucket_abs(delta)] += 1
            record = (
                old.get("District Name", ""),
                old.get("School Name", ""),
                old.get("School Code", ""),
                old.get("Level", ""),
                old.get("Subject", ""),
                old.get("Demographic", ""),
                column,
                repr(old_value),
                repr(new_value),
                kind,
            )
            if is_fayette(old) or is_fayette(new):
                fayette_cells.append(record)
            if is_state(old) or is_state(new):
                state_cells.append(record)

    print(f"changed_rows={changed_rows} changed_cells={sum(column_counts.values())}")
    print("suppressed_flag_on_changed_rows", dict(flag_pairs))
    print("cells_by_column", dict(column_counts))
    print("cells_by_kind", dict(kind_counts))
    for column, counts in kind_by_column.items():
        if counts:
            print(f"  kind[{column}]={dict(counts)}")
    if numeric_deltas:
        absolute = [abs(delta) for delta in numeric_deltas]
        print(
            f"numeric_n={len(numeric_deltas)} mean_signed={statistics.mean(numeric_deltas):.6f} "
            f"median_abs={statistics.median(absolute):.6f} min_signed={min(numeric_deltas):.6f} "
            f"max_signed={max(numeric_deltas):.6f} max_abs={max(absolute):.6f}"
        )
        print("abs_buckets", dict(cell_buckets))
        print("abs_gt_0.15", sum(1 for delta in absolute if delta > 0.15))
        for column, deltas in deltas_by_column.items():
            if not deltas:
                continue
            print(
                f"  signed[{column}] n={len(deltas)} mean={statistics.mean(deltas):.6f} "
                f"median={statistics.median(deltas):.6f} "
                f"negative={sum(delta < 0 for delta in deltas)} "
                f"positive={sum(delta > 0 for delta in deltas)}"
            )
    print(f"distinct_raw_pairs={len(raw_pairs)}")
    for pair, count in raw_pairs.most_common(12):
        print(f"  pair {count} {pair[0]} -> {pair[1]}")
    print(f"fayette_changed_cells={len(fayette_cells)}")
    for record in fayette_cells:
        print("  FAYETTE\t" + "\t".join(record))
    print(f"state_changed_cells={len(state_cells)}")
    for record in state_cells:
        print("  STATE\t" + "\t".join(record))

    if name == "ksa":
        report_ksa(old_index, new_index, columns)
    if name == "grad":
        report_spot_checks(old_index, new_index, "4 Year Cohort Graduation Rate")
    if name == "psr":
        report_spot_checks(old_index, new_index, "Postsecondary Rate", "Postsecondary Rate With Bonus")


def report_spot_checks(old_index: Mapping[Key, Row], new_index: Mapping[Key, Row], *measure_columns: str) -> None:
    """Print the state and Fayette All Students rates from both copies."""
    print("spot_checks")
    for school_code in ("999000", "165000", "165039", "165060", "165105", "165125", "165170", "165200"):
        matches = [key for key in old_index if key[1] == school_code and key[-1] == "All Students"]
        for key in matches:
            old, new = old_index[key], new_index[key]
            values = []
            for column in measure_columns:
                values.append(f"{column}: feb={old[column]!r} may_or_later={new[column]!r} same={old[column] == new[column]}")
            print(
                f"  {old['School Code']} {old['District Name']} {old['School Name']} "
                f"Suppressed feb={old['Suppressed']!r} later={new['Suppressed']!r} " + " | ".join(values)
            )


def report_ksa(old_index: Mapping[Key, Row], new_index: Mapping[Key, Row], columns: Sequence[str]) -> None:
    district_rows: Counter[str] = Counter()
    school_rows: Counter[Tuple[str, str, str]] = Counter()
    level_rows: Counter[str] = Counter()
    subject_rows: Counter[str] = Counter()
    demographic_rows: Counter[str] = Counter()
    subjects_present: Counter[str] = Counter()
    row_buckets: Counter[str] = Counter()
    content_index_only = 0
    content_index_only_tenth = 0
    big_cells = 0
    big_cells_johnson = 0
    big_rows_johnson = 0
    sum_transitions: Counter[Tuple[float, float]] = Counter()
    gap_old: List[float] = []
    gap_new: List[float] = []
    incomplete: List[str] = []
    ones_old = ones_new = 0

    for row in old_index.values():
        subjects_present[row["Subject"]] += 1
        for column in LEVELS:
            if row[column].strip() == "1":
                ones_old += 1
    for row in new_index.values():
        for column in LEVELS:
            if row[column].strip() == "1":
                ones_new += 1

    old_ci_n, old_ci_over, old_ci_max = content_index_stats(old_index.values())
    new_ci_n, new_ci_over, new_ci_max = content_index_stats(new_index.values())
    print(
        f"content_index feb n={old_ci_n} over_100={old_ci_over} max={old_ci_max} "
        f"later n={new_ci_n} over_100={new_ci_over} max={new_ci_max}"
    )
    print(f"performance_cells_equal_to_1 feb={ones_old} later={ones_new}")
    print("subjects_present", dict(subjects_present))

    for key, old in old_index.items():
        new = new_index[key]
        diffs = [column for column in columns if old[column] != new[column]]
        if not diffs:
            continue
        district_rows[old["District Name"]] += 1
        school_rows[(old["District Name"], old["School Name"], old["School Code"])] += 1
        level_rows[old["Level"]] += 1
        subject_rows[old["Subject"]] += 1
        demographic_rows[old["Demographic"]] += 1
        max_abs = 0.0
        numeric = False
        for column in diffs:
            if is_number(old[column]) and is_number(new[column]):
                delta = abs(as_number(new[column]) - as_number(old[column]))
                numeric = True
                max_abs = max(max_abs, delta)
                if delta > 5:
                    big_cells += 1
                    if old["District Name"] == "Johnson County":
                        big_cells_johnson += 1
        if not numeric:
            row_buckets["non-numeric only"] += 1
        elif max_abs <= 0.1000001:
            row_buckets["max<=0.1"] += 1
        elif max_abs <= 1.0000001:
            row_buckets["max in (0.1, 1]"] += 1
        elif max_abs <= 5.0000001:
            row_buckets["max in (1, 5]"] += 1
        else:
            row_buckets["max>5"] += 1
            if old["District Name"] == "Johnson County":
                big_rows_johnson += 1
        if diffs == ["Content Index"]:
            content_index_only += 1
            if max_abs <= 0.1000001:
                content_index_only_tenth += 1
        if all(is_number(old[column]) and is_number(new[column]) for column in LEVELS):
            old_sum = round(sum(as_number(old[column]) for column in LEVELS), 4)
            new_sum = round(sum(as_number(new[column]) for column in LEVELS), 4)
            sum_transitions[(old_sum, new_sum)] += 1
            if is_number(old["Proficient / Distinguished"]) and is_number(new["Proficient / Distinguished"]):
                gap_old.append(
                    abs(as_number(old["Proficient"]) + as_number(old["Distinguished"]) - as_number(old["Proficient / Distinguished"]))
                )
                gap_new.append(
                    abs(as_number(new["Proficient"]) + as_number(new["Distinguished"]) - as_number(new["Proficient / Distinguished"]))
                )
        else:
            shown = {column: (old[column], new[column]) for column in LEVELS + ["Proficient / Distinguished", "Content Index", "Suppressed"]}
            incomplete.append(
                f"{old['District Name']} | {old['School Name']} | {old['School Code']} | "
                f"{old['Level']} | {old['Subject']} | {old['Demographic']} | {shown}"
            )

    print("changed_rows_by_district", district_rows.most_common())
    print(f"districts={len(district_rows)} schools_including_totals={len(school_rows)}")
    print("changed_rows_by_school")
    for (district, school, code), count in school_rows.most_common():
        print(f"  {count}\t{district}\t{school}\t{code}")
    print("changed_rows_by_level", dict(level_rows))
    print("changed_rows_by_subject", dict(subject_rows))
    print("changed_rows_by_demographic", demographic_rows.most_common())
    print("row_max_buckets", dict(row_buckets))
    print(f"content_index_only_rows={content_index_only} of_which_max_abs_le_0.1={content_index_only_tenth}")
    print(f"cells_abs_gt_5={big_cells} of_which_johnson={big_cells_johnson}")
    print(f"rows_max_gt_5_johnson={big_rows_johnson}")
    print("four_level_sum_transitions", sum_transitions.most_common())
    if gap_old:
        print(
            f"proficient_plus_distinguished_gap feb_max={max(gap_old):.4f} "
            f"later_max={max(gap_new):.4f} feb_gt_1.5={sum(gap > 1.5 for gap in gap_old)} "
            f"later_gt_1.5={sum(gap > 1.5 for gap in gap_new)} n={len(gap_old)}"
        )
    out_of_range = 0
    percent_columns = LEVELS + ["Proficient / Distinguished"]
    for key, old in old_index.items():
        new = new_index[key]
        if all(old[column] == new[column] for column in columns):
            continue
        for source in (old, new):
            for column in percent_columns:
                if is_number(source[column]):
                    value = as_number(source[column])
                    if value < 0 or value > 100:
                        out_of_range += 1
    print(f"changed_row_percent_cells_outside_0_100={out_of_range}")
    print(f"changed_rows_without_four_numeric_levels={len(incomplete)}")
    for line in incomplete:
        print("  INCOMPLETE", line)

    print("example_rows")
    for school_code, level, subject, demographic in (
        ("285115", "High School", "Mathematics", "All Students"),
        ("285115", "High School", "Reading", "All Students"),
        ("999000", "High School", "Mathematics", "All Students"),
        ("251000", "Middle School", "Mathematics", "Gifted and Talented"),
        ("285115", "High School", "Reading", "Non-Gifted and Talented"),
    ):
        key = ("20242025", school_code, level, subject, demographic)
        old, new = old_index[key], new_index[key]
        print(f"  {old['School Name']} {school_code} {level} {subject} {demographic}")
        for column in LEVELS + ["Proficient / Distinguished", "Content Index", "Suppressed"]:
            mark = "" if old[column] == new[column] else " CHANGED"
            print(f"    {column}: {old[column]!r} -> {new[column]!r}{mark}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cache",
        type=Path,
        default=Path("/tmp/kyrc25-republish-diffs"),
        help="Directory for downloaded copies. Not in the repository.",
    )
    parser.add_argument("--refresh", action="store_true", help="Download again even if the cache file exists.")
    args = parser.parse_args()
    args.cache.mkdir(parents=True, exist_ok=True)

    print("KYRC25 2026 rewrite diff")
    print(f"cache={args.cache}")
    for name, filename in FILES.items():
        metas = {}
        paths = {}
        for host in ("hist", "ds", "uat"):
            path, meta = ensure_copy(args.cache, host, filename, args.refresh)
            paths[host] = path
            metas[host] = meta
        old_columns, old_rows = load_csv(paths["hist"])
        new_columns, new_rows = load_csv(paths["ds"])
        uat_columns, uat_rows = load_csv(paths["uat"])
        if old_columns != new_columns or new_columns != uat_columns:
            raise SystemExit(f"{filename} headers differ across copies")
        print_copies(filename, metas, old_columns, len(old_rows))
        print(
            f"uat_md5_equals_datasets={metas['uat']['md5'] == metas['ds']['md5']} "
            f"hist_md5_equals_datasets={metas['hist']['md5'] == metas['ds']['md5']} "
            f"uat_rows={len(uat_rows)} datasets_rows={len(new_rows)} hist_rows={len(old_rows)}"
        )
        if len({len(old_rows), len(new_rows), len(uat_rows)}) != 1:
            print("ROW COUNTS DIFFER")
        diff_file(name, old_rows, new_rows, old_columns)
        if metas["uat"]["md5"] == metas["ds"]["md5"]:
            print("uat_vs_datasets=byte_identical")
        else:
            print("uat_vs_datasets=bytes_differ")


if __name__ == "__main__":
    main()
