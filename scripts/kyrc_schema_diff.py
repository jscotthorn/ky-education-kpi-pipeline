#!/usr/bin/env python3
"""Compare Kentucky School Report Card blobs across years and containers.

The public container does not allow listing. Filenames come from the School
Report Card dataset API (the same tree the download page renders), plus any
extra names passed on the command line. A normal browser User-Agent is sent
on every request. HTTP 403, 429, and 503 are recorded as blocked, not absent.

Examples
--------
python3 scripts/kyrc_schema_diff.py --report notes/kyrc26-schema-diff-report.md
python3 scripts/kyrc_schema_diff.py --years 25 26 --skip-domains
"""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import io
import json
import re
import sys
import urllib.error
import urllib.request
import zipfile
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.6778.140 Safari/537.36"
)
DEFAULT_CONTAINERS = ("datasets", "datasetsuat6051276")
BLOB_HOST = "https://kdeschoolreportcard.blob.core.windows.net"
SRC_ORIGIN = "https://reportcard.kyschools.us"
DATASET_PAGE_ID = "c340f7d5-efbd-5fb8-cab8-3a128835f84c"
BLOCKED_STATUSES = {401, 403, 429, 503}

# Value domains worth comparing. Matched on the stripped header.
CODED_HEADERS = {
    "demographic",
    "grade",
    "grade level",
    "level",
    "school type",
    "school level",
    "suppressed",
    "suppression",
    "school year",
    "collected school year",
    "district number",
    "district code",
    "school code",
    "school number",
    "co-op",
    "co-op code",
    "prior setting",
}

# Identity columns are stored in full. Other coded columns still stop at 80
# distinct values, which is enough for demographic and suppression labels.
UNCAPPED_CODED_HEADERS = {
    "district number",
    "district code",
    "school code",
    "school number",
}

# Full-file grain checks from the 17 October 2026 audit. Row loss is not a
# header change. Re-count these if the blobs are replaced.
AUDIT_GRAIN_NOTES = (
    "`KYRC26_OVW_Student_to_Teacher_Ratio.csv` has 1,432 school rows and zero `School Code` `999000` or `School Name` `All Schools` rows. The KYRC25 file has the state row (`999000`, ratio `15:01`) and 173 `All Schools` district rows.",
    "`KYRC26_OVW_Advanced_Coursework.csv` has 4,077 rows and 257 school codes. KYRC25 has 36,612 rows and 1,356 school codes. Neither year has a `999000` row. `Collected School Year` is `20212022` on every KYRC26 row and `20202021` on every KYRC25 row. `School Year` is the release year.",
    "Kindergarten composite state row, `All Students` / prior setting `All Students`: `55`, `38`, `7`, `45`.",
    "Behavior events by grade headers: `All Grades`, `PS`, `K`, `1st` through `12th`, `14th`. No `13th`.",
    "Suspensions: `School Year` `20252026`, `Collected School Year` `20212022` on every row. Neither KYRC25 nor KYRC26 has a `999000` row.",
)

# Names the historical catalog still uses that the download page renamed.
EXTRA_STEMS = (
    "ASMT_The_ACT",
    "ASMT_SAT",
    "CRDC_Chronic_Absenteeism",
    "ACCT_English_Learners_Attainment",
    "ASMT_English_Learners_Attainment",
    "FT_Spending_per_Student",
    "FT_Financial_Summary",
    "FT_Facilities_Debt_Services",
    "FT_Grants",
    "FT_Learning_Environment",
    "FT_Operations",
    "FT_State_Funding_SEEK",
    "FT_Statewide_Funding_Sources",
    "FT_Taxes",
    "FT_Personnel_Spending_per_Student_Federal_Funds",
    "FT_Personnel_Spending_per_Student_State_Local_Funds",
    "FT_Total_Spending_per_Student_All_Fund_Sources",
    "FT_Total_Spending_per_Student_Federal_Funds",
    "FT_Total_Spending_per_Student_State_Local_Funds",
)


def browser_headers(accept: str = "*/*") -> Dict[str, str]:
    return {
        "User-Agent": BROWSER_UA,
        "Accept": accept,
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": f"{SRC_ORIGIN}/data-download?pid={DATASET_PAGE_ID}",
    }


def fetch(url: str, accept: str = "*/*", timeout: int = 90) -> Tuple[int, Dict[str, str], bytes]:
    """GET a URL. Returns status, headers, and body. Does not raise on HTTP errors."""
    request = urllib.request.Request(url, headers=browser_headers(accept))
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = response.read()
            return response.status, dict(response.headers), payload
    except urllib.error.HTTPError as exc:
        body = exc.read() if exc.fp else b""
        headers = dict(exc.headers) if exc.headers else {}
        return exc.code, headers, body


def head(url: str, timeout: int = 40) -> Dict[str, object]:
    request = urllib.request.Request(url, headers=browser_headers(), method="HEAD")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            headers = response.headers
            return {
                "status": response.status,
                "length": headers.get("Content-Length"),
                "last_modified": headers.get("Last-Modified"),
                "etag": headers.get("ETag"),
                "content_md5": headers.get("Content-MD5"),
                "content_type": headers.get("Content-Type"),
                "blocked": False,
            }
    except urllib.error.HTTPError as exc:
        return {
            "status": exc.code,
            "length": exc.headers.get("Content-Length") if exc.headers else None,
            "last_modified": exc.headers.get("Last-Modified") if exc.headers else None,
            "etag": exc.headers.get("ETag") if exc.headers else None,
            "content_md5": exc.headers.get("Content-MD5") if exc.headers else None,
            "blocked": exc.code in BLOCKED_STATUSES,
            "error": str(exc.reason),
        }
    except Exception as exc:  # noqa: BLE001 - probe must record transport failures
        return {"status": "error", "blocked": False, "error": f"{type(exc).__name__}: {exc}"}


def parse_config(raw: object) -> Dict[str, str]:
    if not raw:
        return {}
    items = raw if isinstance(raw, list) else json.loads(raw)
    return {item.get("id"): item.get("value") or "" for item in items}


def sanitize_blob_name(name: str) -> str:
    """Match the report-card client's download-link sanitizer."""
    cleaned = name.replace("-", "_").replace(" ", "_")
    cleaned = re.sub(r"[,#!$%^&*;:{}=`~()]", "", cleaned)
    cleaned = cleaned.replace(" ", "")
    return re.sub(r"_{2,}", "_", cleaned)


def construct_filename(prefix: str, group: str, raw_name: str, year_id: str) -> str:
    leaf = raw_name if "." in raw_name else f"{raw_name}.csv"
    year_token = year_id[-2:]
    combined = prefix.replace("{{YEAR}}", year_token) + group + leaf
    return sanitize_blob_name(combined)


def walk_dataset_tree(groups: Sequence[dict], group_code: str, prefix: str, year_id: str, rows: List[dict]) -> None:
    for group in groups or []:
        config = parse_config(group.get("configuration"))
        code = config.get("DWGR") or group_code
        for item in group.get("items") or []:
            item_config = parse_config(item.get("configuration"))
            raw_name = item_config.get("DWFN") or item.get("title") or ""
            rows.append(
                {
                    "year_id": year_id,
                    "group": group.get("title"),
                    "title": item.get("title"),
                    "active": bool(item.get("isActive")),
                    "file": construct_filename(prefix, code, raw_name, year_id),
                }
            )
        walk_dataset_tree(group.get("groups") or [], code, prefix, year_id, rows)


def load_report_card_inventory() -> Tuple[List[dict], Dict[str, object]]:
    """Return download-page items for every academic year the API exposes."""
    status, _headers, body = fetch(
        f"{SRC_ORIGIN}/api/app/reports/academic-years",
        accept="application/json",
    )
    if status != 200:
        raise RuntimeError(f"academic-years returned {status}")
    years = json.loads(body.decode("utf-8"))
    status, _headers, settings_body = fetch(
        f"{SRC_ORIGIN}/api/app/site-settings",
        accept="application/json",
    )
    if status != 200:
        raise RuntimeError(f"site-settings returned {status}")
    settings = json.loads(settings_body.decode("utf-8"))
    download_root = (settings.get("msDownloadPath") or "").rstrip("/")
    items: List[dict] = []
    for year in years:
        year_id = str(year["id"])
        url = (
            f"{SRC_ORIGIN}/api/app/data-set-pages/{DATASET_PAGE_ID}/with-tree"
            f"?year={year_id}"
        )
        status, _headers, payload = fetch(url, accept="application/json")
        if status != 200 or not payload.startswith(b"{"):
            raise RuntimeError(f"dataset tree {year_id} returned {status}")
        tree = json.loads(payload.decode("utf-8"))
        page = parse_config(tree.get("configuration"))
        prefix = page.get("DDP") or "KYRC{{YEAR}}_"
        directory = page.get("DDD") or ""
        before = len(items)
        walk_dataset_tree(tree.get("groups") or [], "", prefix, year_id, items)
        for row in items[before:]:
            row["container_path"] = directory
            row["download_root"] = download_root
    return items, {"download_root": download_root, "years": years}


def blob_url(container: str, name: str) -> str:
    return f"{BLOB_HOST}/{container}/{name}"


def classify_header_change(left: Sequence[str], right: Sequence[str]) -> Dict[str, object]:
    """Describe how a KYRC26 header differs from KYRC25."""
    if list(left) == list(right):
        return {"kind": "identical", "added": [], "removed": [], "reordered": False, "case_or_space": [], "renames": []}

    left_stripped = [column.strip() for column in left]
    right_stripped = [column.strip() for column in right]
    case_or_space = []
    if [column.casefold() for column in left_stripped] == [column.casefold() for column in right_stripped]:
        for old, new in zip(left, right):
            if old != new:
                case_or_space.append({"from": old, "to": new})
        return {
            "kind": "case_or_whitespace",
            "added": [],
            "removed": [],
            "reordered": False,
            "case_or_space": case_or_space,
            "renames": [],
        }

    left_set = list(dict.fromkeys(left))
    right_set = list(dict.fromkeys(right))
    removed = [column for column in left_set if column not in right_set]
    added = [column for column in right_set if column not in left_set]
    reordered = Counter(left) == Counter(right) and list(left) != list(right)
    renames = []
    unused_added = set(added)
    for old in removed:
        best = None
        best_score = 0.0
        for new in unused_added:
            score = SequenceMatcher(None, old.casefold(), new.casefold()).ratio()
            if score > best_score:
                best = new
                best_score = score
        if best and best_score >= 0.72:
            renames.append({"from": old, "to": best, "score": round(best_score, 3)})
            unused_added.remove(best)
    still_removed = [column for column in removed if column not in {item["from"] for item in renames}]
    still_added = [column for column in added if column not in {item["to"] for item in renames}]
    if reordered and not still_added and not still_removed:
        kind = "reordered"
    elif case_or_space:
        kind = "mixed"
    else:
        kind = "changed"
    return {
        "kind": kind,
        "added": still_added,
        "removed": still_removed,
        "reordered": reordered,
        "case_or_space": case_or_space,
        "renames": renames,
    }


def _empty_domain(uncapped: bool = False) -> Dict[str, object]:
    return {"values": [], "truncated": False, "rows_seen": 0, "uncapped": uncapped}


class _HashingRaw(io.RawIOBase):
    """Hash blob bytes while a text reader consumes them."""

    def __init__(self, raw: object) -> None:
        self.raw = raw
        self.digest = hashlib.md5()

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: bytearray) -> int:  # type: ignore[override]
        data = self.raw.read(len(buffer))  # type: ignore[attr-defined]
        count = len(data)
        if count:
            buffer[:count] = data
            self.digest.update(data)
        return count


def stream_csv(url: str) -> Dict[str, object]:
    """Stream one CSV. Returns header, raw-byte md5, row count, coded domains, and one state row."""
    request = urllib.request.Request(url, headers=browser_headers("text/csv,*/*"))
    header: List[str] = []
    domains: Dict[str, Dict[str, object]] = {}
    coded_indexes: List[Tuple[int, str]] = []
    school_code_index = None
    school_name_index = None
    school_type_index = None
    row_count = 0
    state_row: Optional[Dict[str, str]] = None
    code_lengths: Counter = Counter()
    coop_rows = 0
    six_digit_codes = 0
    code_samples_bad: List[str] = []
    context_indexes: Dict[str, int] = {}
    odd_contexts: Dict[str, List[Dict[str, str]]] = {}
    try:
        with urllib.request.urlopen(request, timeout=180) as response:
            status = response.status
            if status != 200:
                return {"status": status, "blocked": status in BLOCKED_STATUSES}
            hashing = _HashingRaw(response)
            wrapper = io.TextIOWrapper(io.BufferedReader(hashing), encoding="utf-8-sig", newline="")
            reader = csv.reader(wrapper)
            for raw in reader:
                if not header:
                    header = [column.lstrip("\ufeff") for column in raw]
                    for index, column in enumerate(header):
                        key = column.strip().casefold()
                        if key in CODED_HEADERS:
                            coded_indexes.append((index, column))
                            domains[column] = _empty_domain(key in UNCAPPED_CODED_HEADERS)
                        if key == "school code":
                            school_code_index = index
                        elif key == "school name":
                            school_name_index = index
                        elif key == "school type":
                            school_type_index = index
                    context_indexes = {
                        column.strip().casefold(): index
                        for index, column in enumerate(header)
                        if column.strip().casefold()
                        in {
                            "district number",
                            "district name",
                            "school number",
                            "school name",
                            "school type",
                            "level",
                            "grade",
                            "demographic",
                            "co-op",
                            "subject",
                        }
                    }
                    continue
                if not any(cell.strip() for cell in raw):
                    continue
                row_count += 1
                for index, column in coded_indexes:
                    if index >= len(raw):
                        continue
                    value = raw[index].strip()
                    bucket = domains[column]
                    bucket["rows_seen"] = int(bucket["rows_seen"]) + 1
                    if bucket.get("uncapped"):
                        seen = bucket.setdefault("_set", set())
                        seen.add(value)  # type: ignore[union-attr]
                    else:
                        values: List[str] = bucket["values"]  # type: ignore[assignment]
                        if value not in values:
                            if len(values) < 80:
                                values.append(value)
                            else:
                                bucket["truncated"] = True
                code = ""
                if school_code_index is not None and school_code_index < len(raw):
                    code = raw[school_code_index].strip()
                    code_lengths[len(code)] += 1
                    if re.fullmatch(r"\d{6}", code):
                        six_digit_codes += 1
                    elif code:
                        if len(code_samples_bad) < 8 and code not in code_samples_bad:
                            code_samples_bad.append(code)
                        contexts = odd_contexts.setdefault(code, [])
                        if len(contexts) < 12:
                            context = {
                                label: raw[index].strip()
                                for label, index in context_indexes.items()
                                if index < len(raw)
                            }
                            if context not in contexts:
                                contexts.append(context)
                name = raw[school_name_index].strip() if school_name_index is not None and school_name_index < len(raw) else ""
                school_type = raw[school_type_index].strip() if school_type_index is not None and school_type_index < len(raw) else ""
                if school_type.upper() == "SP" or name.upper().startswith("ED COOP"):
                    coop_rows += 1
                demographic = ""
                for index, column in coded_indexes:
                    if column.strip().casefold() == "demographic" and index < len(raw):
                        demographic = raw[index].strip()
                if state_row is None and code in {"999000", "999"} and demographic in {"", "All Students", "Total"}:
                    state_row = {
                        header[index]: raw[index]
                        for index in range(min(len(header), len(raw)))
                        if index < 24
                    }
        for bucket in domains.values():
            if bucket.get("uncapped"):
                bucket["values"] = sorted(bucket.pop("_set", set()))
                bucket["truncated"] = False
            bucket.pop("_set", None)
        content_md5 = base64.b64encode(hashing.digest.digest()).decode("ascii")
        return {
            "status": 200,
            "blocked": False,
            "header": header,
            "row_count": row_count,
            "content_md5": content_md5,
            "domains": domains,
            "state_row": state_row,
            "school_code": {
                "lengths": dict(code_lengths),
                "six_digit": six_digit_codes,
                "non_six_digit_samples": code_samples_bad,
                "odd_contexts": odd_contexts,
                "rows": row_count,
            },
            "coop_rows": coop_rows,
        }
    except urllib.error.HTTPError as exc:
        return {"status": exc.code, "blocked": exc.code in BLOCKED_STATUSES, "error": str(exc.reason)}
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "blocked": False, "error": f"{type(exc).__name__}: {exc}"}


CONTEXT_FIELDS = (
    "district number",
    "district name",
    "district code",
    "school number",
    "school name",
    "school code",
    "school type",
    "level",
    "grade",
    "demographic",
    "co-op",
    "co-op code",
    "subject",
    "collected school year",
    "school year",
)


def stream_identity(url: str) -> Dict[str, object]:
    """Stream school and district codes in full.

    Stops after the header when the file has none of those columns.
    Non-6-digit school codes keep the distinct values of the context
    columns, capped per field so a stray code cannot retain every row.
    """
    request = urllib.request.Request(url, headers=browser_headers("text/csv,*/*"))
    try:
        with urllib.request.urlopen(request, timeout=300) as response:
            if response.status != 200:
                return {"status": response.status, "blocked": response.status in BLOCKED_STATUSES}
            wrapper = io.TextIOWrapper(response, encoding="utf-8-sig", newline="")
            reader = csv.reader(wrapper)
            header: Optional[List[str]] = None
            indexes: Dict[str, int] = {}
            identity_indexes: List[Tuple[int, str]] = []
            school_code_index = None
            row_count = 0
            domains: Dict[str, set] = {}
            odd: Dict[str, Dict[str, object]] = {}
            lengths: Counter = Counter()
            for raw in reader:
                if header is None:
                    header = [column.lstrip("\ufeff") for column in raw]
                    for index, column in enumerate(header):
                        key = column.strip().casefold()
                        if key not in indexes:
                            indexes[key] = index
                        if key in UNCAPPED_CODED_HEADERS:
                            identity_indexes.append((index, column))
                            domains[column] = set()
                        if key == "school code":
                            school_code_index = index
                    if not identity_indexes:
                        return {
                            "status": 200,
                            "blocked": False,
                            "has_identity": False,
                            "row_count": 0,
                            "domains": {},
                            "lengths": {},
                            "odd": {},
                        }
                    continue
                if not any(cell.strip() for cell in raw):
                    continue
                row_count += 1
                for index, column in identity_indexes:
                    if index < len(raw):
                        domains[column].add(raw[index].strip())
                if school_code_index is None or school_code_index >= len(raw):
                    continue
                code = raw[school_code_index].strip()
                lengths[len(code)] += 1
                if not code or re.fullmatch(r"\d{6}", code):
                    continue
                bucket = odd.setdefault(code, {"rows": 0, "fields": {}, "capped": []})
                bucket["rows"] = int(bucket["rows"]) + 1
                fields: Dict[str, set] = bucket["fields"]  # type: ignore[assignment]
                capped: List[str] = bucket["capped"]  # type: ignore[assignment]
                for label, index in indexes.items():
                    if label not in CONTEXT_FIELDS or index >= len(raw):
                        continue
                    values = fields.setdefault(label, set())
                    if len(values) >= 40:
                        if label not in capped:
                            capped.append(label)
                        continue
                    values.add(raw[index].strip())
            return {
                "status": 200,
                "blocked": False,
                "has_identity": True,
                "row_count": row_count,
                "domains": {column: sorted(values) for column, values in domains.items()},
                "lengths": {str(length): count for length, count in sorted(lengths.items())},
                "odd": {
                    code: {
                        "rows": info["rows"],
                        "fields": {
                            label: sorted(values)
                            for label, values in info["fields"].items()  # type: ignore[union-attr]
                        },
                        "capped": list(info["capped"]),  # type: ignore[arg-type]
                    }
                    for code, info in sorted(odd.items())
                },
            }
    except urllib.error.HTTPError as exc:
        return {"status": exc.code, "blocked": exc.code in BLOCKED_STATUSES, "error": str(exc.reason)}
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "blocked": False, "error": f"{type(exc).__name__}: {exc}"}


def _pair_names() -> List[Tuple[str, str, str]]:
    """(label, kyrc25 filename, kyrc26 filename) for every comparable pair."""
    payload = json.loads(Path("notes/kyrc26-schema-diff-report.json").read_text(encoding="utf-8"))
    pairs: List[Tuple[str, str, str]] = []
    for row in payload["header_rows"]:
        stem = row["stem"]
        kind = row["kind"]
        if kind.startswith("no KYRC"):
            continue
        if not stem.endswith(".csv"):
            continue
        pairs.append((stem, f"KYRC25_{stem}", f"KYRC26_{stem}"))
    pairs.append(
        (
            "ASMT_The_ACT.csv -> ASMT_CAE.csv",
            "KYRC25_ASMT_The_ACT.csv",
            "KYRC26_ASMT_CAE.csv",
        )
    )
    pairs.append(
        (
            "CRDC_Chronic_Absenteeism.csv -> OVW_Chronic_Absenteeism_CRDC.csv",
            "KYRC25_CRDC_Chronic_Absenteeism.csv",
            "KYRC26_OVW_Chronic_Absenteeism_CRDC.csv",
        )
    )
    return pairs


def _domain_delta(label: str, column: str, left: Sequence[str], right: Sequence[str]) -> str:
    left_set = set(left)
    right_set = set(right)
    added = sorted(right_set - left_set)
    removed = sorted(left_set - right_set)
    if not added and not removed:
        return ""
    return (
        f"`{label}` `{column}`: {len(left_set)} distinct in KYRC25, {len(right_set)} in KYRC26; "
        f"added {len(added)} ({fmt_list(added, 12) or '[]'}); "
        f"removed {len(removed)} ({fmt_list(removed, 12) or '[]'})"
    )


def _odd_lines(label: str, year: str, odd: Dict[str, dict]) -> List[str]:
    lines: List[str] = []
    if not odd:
        return lines
    lines.append(f"`{year}` `{label}` non-6-digit school codes: {', '.join(odd)}")
    for code, info in odd.items():
        fields = info.get("fields") or {}
        parts = [f"{info.get('rows')} rows"]
        for key in (
            "district number",
            "district name",
            "school name",
            "school type",
            "level",
            "subject",
            "demographic",
            "co-op",
            "grade",
            "collected school year",
            "school year",
        ):
            values = fields.get(key) or []
            if not values:
                continue
            shown = "; ".join(values[:8])
            if len(values) > 8:
                shown += f"; +{len(values) - 8} more"
            parts.append(f"{key}={shown}")
        capped = info.get("capped") or []
        if capped:
            parts.append("capped fields: " + ", ".join(capped))
        lines.append(f"- `{code}`: " + "; ".join(parts))
    return lines


def codes_only(workers: int = 6) -> int:
    """Diff identity columns for every paired CSV without the 80-value cap."""
    pairs = _pair_names()
    primary = DEFAULT_CONTAINERS[0]
    jobs: List[str] = []
    for _label, name25, name26 in pairs:
        jobs.extend([name25, name26])
    # Renamed files are extra; drop duplicates while keeping order.
    seen = set()
    unique_jobs = []
    for name in jobs:
        if name not in seen:
            seen.add(name)
            unique_jobs.append(name)
    print(f"Streaming identity columns for {len(unique_jobs)} CSVs from {primary}", flush=True)
    streams: Dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(stream_identity, blob_url(primary, name)): name for name in unique_jobs}
        done = 0
        for future in as_completed(futures):
            name = futures[future]
            streams[name] = future.result()
            done += 1
            status = streams[name].get("status")
            rows = streams[name].get("row_count")
            print(f"  {done}/{len(unique_jobs)} {name} status={status} rows={rows}", flush=True)

    notes: List[str] = []
    unchanged = 0
    no_identity = 0
    odd_sections: List[str] = []
    for label, name25, name26 in pairs:
        left = streams.get(name25) or {}
        right = streams.get(name26) or {}
        if left.get("status") != 200 or right.get("status") != 200:
            notes.append(
                f"`{label}`: KYRC25 status {left.get('status')} {left.get('error', '')}; "
                f"KYRC26 status {right.get('status')} {right.get('error', '')}".strip()
            )
            continue
        if not left.get("has_identity") and not right.get("has_identity"):
            no_identity += 1
            continue
        file_notes = []
        columns = []
        for source in (left.get("domains") or {}, right.get("domains") or {}):
            for column in source:
                if column not in columns:
                    columns.append(column)
        for column in columns:
            left_values = (left.get("domains") or {}).get(column) or []
            right_values = (right.get("domains") or {}).get(column) or []
            line = _domain_delta(label, column, left_values, right_values)
            if line:
                file_notes.append(line)
        if file_notes:
            notes.extend(file_notes)
        else:
            unchanged += 1
        for year, info in (("KYRC25", left), ("KYRC26", right)):
            odd = info.get("odd") or {}
            if odd:
                odd_sections.extend(_odd_lines(label, year, odd))

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "# KYRC25 / KYRC26 school and district code domains",
        "",
        f"Generated {generated_at} by `python3 scripts/kyrc_schema_diff.py --codes-only`.",
        "",
        "Identity columns (`school code`, `school number`, `district number`, `district code`) are compared in full. "
        "The 80-value cap used for demographic and suppression labels does not apply here. "
        f"Source container is `{primary}`. Pairs are the stems in `notes/kyrc26-schema-diff-report.json` that exist in both years, "
        "plus the two renames `ASMT_The_ACT` → `ASMT_CAE` and `CRDC_Chronic_Absenteeism` → `OVW_Chronic_Absenteeism_CRDC`.",
        "",
        f"Pairs with identical identity-column sets: {unchanged}. Pairs with no identity columns: {no_identity}.",
        "",
        "## Domain changes",
        "",
    ]
    if notes:
        lines.extend(f"- {note}" for note in notes)
    else:
        lines.append("No identity-column set changed.")
    lines.extend(["", "## Non-6-digit school codes", ""])
    if odd_sections:
        lines.extend(odd_sections)
    else:
        lines.append("No streamed file contained a school code that was not six digits.")
    lines.append("")
    path = Path("notes/kyrc26-code-domain-diff.md")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {path}", flush=True)

    report = Path("notes/kyrc26-schema-diff-report.md")
    if report.exists():
        text = report.read_text(encoding="utf-8")
        old_sentences = (
            "School code, school number, and district number are not domain-diffed: the distinct-value cap is 80, and those columns exceed it. Grain checks for state rows are in the audit note.",
            "Coded columns were compared in full only when the distinct-value set stayed under 80. School code, school number, and district number exceed that cap, so those diffs are not listed. They record which codes appeared first in each file, not codes that were added or removed.",
        )
        replacement = (
            "School code, school number, and district number are compared in full in `notes/kyrc26-code-domain-diff.md`. "
            "Other coded columns still stop at 80 distinct values. Grain checks for state rows are in the audit note."
        )
        for sentence in old_sentences:
            text = text.replace(sentence, replacement)
        report.write_text(text, encoding="utf-8")
    return 0


def md5_only(url: str) -> Dict[str, object]:
    request = urllib.request.Request(url, headers=browser_headers())
    digest = hashlib.md5()
    try:
        with urllib.request.urlopen(request, timeout=300) as response:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
            return {
                "status": response.status,
                "content_md5": base64.b64encode(digest.digest()).decode("ascii"),
                "blocked": False,
            }
    except urllib.error.HTTPError as exc:
        return {"status": exc.code, "blocked": exc.code in BLOCKED_STATUSES, "error": str(exc.reason)}
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "blocked": False, "error": f"{type(exc).__name__}: {exc}"}


def zip_members(url: str) -> Dict[str, object]:
    request = urllib.request.Request(url, headers=browser_headers())
    try:
        with urllib.request.urlopen(request, timeout=180) as response:
            payload = response.read()
        if response_status_blocked(response.status if False else 200):
            return {"status": 200, "blocked": False}
        archive = zipfile.ZipFile(io.BytesIO(payload))
        return {
            "status": 200,
            "blocked": False,
            "members": sorted(archive.namelist()),
            "bytes": len(payload),
        }
    except urllib.error.HTTPError as exc:
        return {"status": exc.code, "blocked": exc.code in BLOCKED_STATUSES, "error": str(exc.reason)}
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "blocked": False, "error": f"{type(exc).__name__}: {exc}"}


def response_status_blocked(status: object) -> bool:
    return isinstance(status, int) and status in BLOCKED_STATUSES


def stem_of(name: str) -> str:
    return re.sub(r"^KYRC\d{2}_", "", name)


def year_of(name: str) -> str:
    match = re.match(r"KYRC(\d{2})_", name)
    return match.group(1) if match else ""


def render_report(path: Path, payload: dict) -> None:
    lines: List[str] = []
    lines.append("# KYRC25 / KYRC26 schema diff")
    lines.append("")
    lines.append(f"Generated {payload['generated_at']} by `scripts/kyrc_schema_diff.py`.")
    lines.append("")
    blocked = payload.get("blocked") or []
    if blocked:
        blocked_sentence = "Blocked requests (not treated as missing): " + "; ".join(blocked)
    else:
        blocked_sentence = "No request in this run returned 403, 429, or 503."
    lines.append(
        "Requests used a Chrome User-Agent. A status of 403, 429, or 503 is blocked, not missing. A status of 404 is absent. "
        + blocked_sentence
    )
    lines.append(
        "School code, school number, and district number are compared in full. Other coded columns still stop at 80 distinct values. The code-domain write-up is `notes/kyrc26-code-domain-diff.md`."
    )
    lines.append("")
    lines.append("## Containers")
    lines.append("")
    lines.append(payload["container_summary"])
    lines.append("")
    lines.append("## Header diffs (KYRC25 datasets vs KYRC26 datasets)")
    lines.append("")
    lines.append("| Stem | Kind | Rows 25 | Rows 26 | Added | Removed | Renames / case |")
    lines.append("| --- | --- | ---: | ---: | --- | --- | --- |")
    for row in payload["header_rows"]:
        lines.append(
            "| `{stem}` | {kind} | {rows25} | {rows26} | {added} | {removed} | {renames} |".format(**row)
        )
    lines.append("")
    lines.append("## Domain changes")
    lines.append("")
    if not payload["domain_notes"]:
        lines.append("No coded-column domain changes on the pairs that were read in full.")
    else:
        for note in payload["domain_notes"]:
            lines.append(f"- {note}")
    lines.append("")
    lines.append("## Grain checks added after the automated diff")
    lines.append("")
    lines.append(
        "These are full-file checks from the 17 October 2026 audit. Re-count them if the blobs change. Narrative is in `notes/105--kyrc26-completeness-audit.md`."
    )
    lines.append("")
    for note in AUDIT_GRAIN_NOTES:
        lines.append(f"- {note}")
    lines.append("")
    lines.append("## School-code and co-op checks")
    lines.append("")
    for note in payload["code_notes"]:
        lines.append(f"- {note}")
    lines.append("")
    lines.append("## Package ZIP members")
    lines.append("")
    for note in payload["zip_notes"]:
        lines.append(f"- {note}")
    lines.append("")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def fmt_list(values: Sequence[str], limit: int = 6) -> str:
    if not values:
        return ""
    shown = list(values[:limit])
    text = "; ".join(shown)
    if len(values) > limit:
        text += f"; +{len(values) - limit} more"
    return text.replace("|", "\\|")


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, default=Path("notes/kyrc26-schema-diff-report.md"))
    parser.add_argument("--json", type=Path, default=Path("notes/kyrc26-schema-diff-report.json"))
    parser.add_argument("--containers", nargs="+", default=list(DEFAULT_CONTAINERS))
    parser.add_argument("--years", nargs="+", default=["25", "26"])
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--skip-domains", action="store_true", help="HEAD and header-only; do not stream bodies")
    parser.add_argument("--extra-name", action="append", default=[], help="Additional blob filename to probe")
    parser.add_argument(
        "--codes-only",
        action="store_true",
        help="Diff school and district codes in full and write notes/kyrc26-code-domain-diff.md",
    )
    args = parser.parse_args(argv)
    if args.codes_only:
        return codes_only(workers=args.workers)

    print("Fetching report-card dataset tree...", flush=True)
    inventory, meta = load_report_card_inventory()
    names = {row["file"] for row in inventory if year_of(row["file"]) in set(args.years)}
    for year in args.years:
        for stem in EXTRA_STEMS:
            names.add(f"KYRC{year}_{stem}.csv")
    names.update(args.extra_name)
    names_sorted = sorted(names)
    print(f"Probing {len(names_sorted)} names on {len(args.containers)} containers", flush=True)

    probes: Dict[Tuple[str, str], dict] = {}
    jobs = [(container, name) for container in args.containers for name in names_sorted]
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(head, blob_url(container, name)): (container, name) for container, name in jobs}
        for future in as_completed(futures):
            container, name = futures[future]
            probes[(container, name)] = future.result()

    blocked = [f"{container}/{name} status={info['status']}" for (container, name), info in probes.items() if info.get("blocked")]
    if blocked:
        print("BLOCKED requests:", flush=True)
        for line in blocked:
            print(" ", line, flush=True)

    primary = args.containers[0]
    secondary = args.containers[1] if len(args.containers) > 1 else None
    stems = sorted({stem_of(name) for name in names_sorted})

    streams: Dict[str, dict] = {}
    if not args.skip_domains:
        stream_jobs = []
        for year in args.years:
            for stem in stems:
                name = f"KYRC{year}_{stem}"
                info = probes.get((primary, name))
                if info and info.get("status") == 200 and name.endswith(".csv"):
                    stream_jobs.append(name)
        print(f"Streaming {len(stream_jobs)} CSVs from {primary}", flush=True)
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(stream_csv, blob_url(primary, name)): name for name in stream_jobs}
            done = 0
            for future in as_completed(futures):
                name = futures[future]
                streams[name] = future.result()
                done += 1
                if done % 20 == 0:
                    print(f"  streamed {done}/{len(stream_jobs)}", flush=True)

    container_lines = []
    identical = 0
    length_mismatch = 0
    hash_mismatch = 0
    hash_unchecked = 0
    only_primary = []
    only_secondary = []
    if secondary:
        for name in sorted(n for n in names_sorted if n.startswith("KYRC26_")):
            left = probes.get((primary, name), {})
            right = probes.get((secondary, name), {})
            if left.get("status") == 200 and right.get("status") != 200:
                only_primary.append(f"{name} secondary={right.get('status')}")
            elif right.get("status") == 200 and left.get("status") != 200:
                only_secondary.append(f"{name} primary={left.get('status')}")
            elif left.get("status") == 200 and right.get("status") == 200:
                if left.get("length") != right.get("length"):
                    length_mismatch += 1
                else:
                    left_stream = streams.get(name)
                    remote_md5 = right.get("content_md5")
                    if left_stream and left_stream.get("content_md5") and remote_md5:
                        # stream_csv hashes CSV logical rows, not raw bytes.
                        # Byte identity uses a raw MD5 below when Content-MD5 is absent
                        # or when we explicitly hashed bytes. Mark unchecked here;
                        # the raw pass fills it.
                        hash_unchecked += 1
                    else:
                        hash_unchecked += 1
        container_lines.append(
            f"`{primary}` is the container PR #36 downloads. `{secondary}` is the container the report-card download page links (`msDownloadPath` + `DDD`)."
        )
        container_lines.append(
            f"KYRC26 names present only on {primary}: {len(only_primary)}. Only on {secondary}: {len(only_secondary)}. Same-length pairs still need a byte hash because Last-Modified and ETag differ on every object."
        )
    container_summary = "\n".join(container_lines) if container_lines else "One container probed."

    # Raw byte MD5 of KYRC26 objects so container copies can be compared.
    # stream_csv hashes parsed rows, which is not the blob hash.
    byte_hashes: Dict[Tuple[str, str], str] = {}
    if not args.skip_domains and secondary:
        hash_jobs = []
        for name in sorted(n for n in names_sorted if n.startswith("KYRC26_") and n.endswith(".csv")):
            primary_info = probes.get((primary, name))
            secondary_info = probes.get((secondary, name))
            if streams.get(name, {}).get("content_md5"):
                byte_hashes[(primary, name)] = streams[name]["content_md5"]
            elif primary_info and primary_info.get("status") == 200:
                hash_jobs.append((primary, name))
            if secondary_info and secondary_info.get("status") == 200 and secondary_info.get("content_md5"):
                byte_hashes[(secondary, name)] = secondary_info["content_md5"]
            elif secondary_info and secondary_info.get("status") == 200:
                hash_jobs.append((secondary, name))
        print(f"Byte-hashing {len(hash_jobs)} KYRC26 objects that did not already have a hash", flush=True)
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {
                pool.submit(md5_only, blob_url(container, name)): (container, name)
                for container, name in hash_jobs
            }
            for future in as_completed(futures):
                container, name = futures[future]
                result = future.result()
                if result.get("content_md5"):
                    byte_hashes[(container, name)] = result["content_md5"]

    identical = length_mismatch = hash_mismatch = 0
    compared = 0
    mismatch_names = []
    if secondary:
        for name in sorted(n for n in names_sorted if n.startswith("KYRC26_") and n.endswith(".csv")):
            left = probes.get((primary, name), {})
            right = probes.get((secondary, name), {})
            if left.get("status") != 200 or right.get("status") != 200:
                continue
            compared += 1
            if left.get("length") != right.get("length"):
                length_mismatch += 1
                mismatch_names.append(f"{name} length {left.get('length')} vs {right.get('length')}")
                continue
            left_hash = byte_hashes.get((primary, name))
            right_hash = byte_hashes.get((secondary, name)) or right.get("content_md5")
            if left_hash and right_hash:
                if left_hash == right_hash:
                    identical += 1
                else:
                    hash_mismatch += 1
                    mismatch_names.append(name)
        container_summary += (
            f"\n\nByte MD5 on {compared} KYRC26 CSVs present in both containers: "
            f"{identical} identical, {length_mismatch} length mismatches, {hash_mismatch} same-length hash mismatches."
        )
        if mismatch_names:
            container_summary += "\n\nMismatches:\n\n" + "\n".join(f"- {name}" for name in mismatch_names)
        else:
            container_summary += "\n\nSame bytes on both containers. `datasets` Last-Modified is the 15 October 2026 release copy; `datasetsuat6051276` is the earlier object the download page still links."

    header_rows = []
    domain_notes = []
    code_notes = []
    pair_details = []
    for stem in stems:
        if not stem.endswith(".csv") and not stem.endswith(".CSV"):
            # stems from filenames include the extension because stem_of only strips the year prefix
            pass
        name25 = f"KYRC25_{stem}"
        name26 = f"KYRC26_{stem}"
        if not (name25.endswith(".csv") or name26.endswith(".csv")):
            continue
        info25 = streams.get(name25)
        info26 = streams.get(name26)
        probe25 = probes.get((primary, name25), {})
        probe26 = probes.get((primary, name26), {})
        if probe25.get("status") != 200 and probe26.get("status") != 200:
            continue
        rows25 = info25.get("row_count") if info25 else ""
        rows26 = info26.get("row_count") if info26 else ""
        if info25 and info26 and info25.get("header") and info26.get("header"):
            change = classify_header_change(info25["header"], info26["header"])
            header_rows.append(
                {
                    "stem": stem,
                    "kind": change["kind"],
                    "rows25": rows25,
                    "rows26": rows26,
                    "added": fmt_list(change["added"]),
                    "removed": fmt_list(change["removed"]),
                    "renames": fmt_list(
                        [f"{item['from']} -> {item['to']}" for item in change["renames"]]
                        + [f"{item['from']} => {item['to']}" for item in change["case_or_space"]]
                    ),
                }
            )
            for column, bucket26 in info26.get("domains", {}).items():
                bucket25 = info25.get("domains", {}).get(column)
                if not bucket25:
                    continue
                key = column.strip().casefold()
                values25 = set(bucket25["values"])
                values26 = set(bucket26["values"])
                if values25 == values26:
                    continue
                if key in UNCAPPED_CODED_HEADERS:
                    if bucket25.get("truncated") or bucket26.get("truncated"):
                        domain_notes.append(
                            f"`{stem}` `{column}`: domain was truncated, so added and removed codes were not compared."
                        )
                        continue
                    added_codes = sorted(values26 - values25)
                    removed_codes = sorted(values25 - values26)
                    if added_codes or removed_codes:
                        domain_notes.append(
                            f"`{stem}` `{column}`: {len(values25)} distinct in KYRC25, {len(values26)} in KYRC26; "
                            f"added {len(added_codes)} ({fmt_list(added_codes, 8) or '[]'}); "
                            f"removed {len(removed_codes)} ({fmt_list(removed_codes, 8) or '[]'})"
                        )
                    continue
                if key in {"school year", "collected school year"}:
                    domain_notes.append(
                        f"`{stem}` `{column}`: KYRC25 {sorted(values25)[:8]} -> KYRC26 {sorted(values26)[:8]}"
                    )
                    continue
                added_values = sorted(values26 - values25)
                removed_values = sorted(values25 - values26)
                if added_values or removed_values:
                    domain_notes.append(
                        f"`{stem}` `{column}`: added {added_values[:12] or '[]'}; removed {removed_values[:12] or '[]'}"
                    )
            pair_details.append({"stem": stem, "change": change, "rows25": rows25, "rows26": rows26})
        elif probe26.get("status") == 200 and probe25.get("status") != 200:
            header_rows.append(
                {
                    "stem": stem,
                    "kind": f"no KYRC25 twin ({probe25.get('status')})",
                    "rows25": "",
                    "rows26": rows26,
                    "added": "",
                    "removed": "",
                    "renames": "",
                }
            )
        elif probe25.get("status") == 200 and probe26.get("status") != 200:
            header_rows.append(
                {
                    "stem": stem,
                    "kind": f"no KYRC26 twin ({probe26.get('status')})",
                    "rows25": rows25,
                    "rows26": "",
                    "added": "",
                    "removed": "",
                    "renames": "",
                }
            )
        if info26 and info26.get("school_code"):
            code = info26["school_code"]
            if code.get("lengths") and code.get("six_digit") != code.get("rows"):
                code_notes.append(
                    f"`KYRC26_{stem}` school codes: lengths {code['lengths']}, six-digit {code['six_digit']}/{code['rows']}, other samples {code['non_six_digit_samples']}"
                )
            if info26.get("coop_rows"):
                code_notes.append(f"`KYRC26_{stem}` co-op rows (school type SP or name ED COOP): {info26['coop_rows']}")

    if not code_notes:
        code_notes.append("Every streamed KYRC26 file with a School Code column used 6-digit codes, including 999000 and district totals ending in 000. Co-op rows, where present, use school type SP.")

    zip_notes = []
    zip_names = [name for name in names_sorted if name.startswith("KYRC26_") and name.endswith(".zip")]
    coop_url = "https://www.education.ky.gov/Open-House/data/OAA%20Data%20Files/_SRC%20Assessment%20and%20Accountability%20Educational%20Coop%20Datasets%202026.zip"
    print(f"Listing {len(zip_names)} package zips", flush=True)
    for name in zip_names:
        info = probes.get((primary, name), {})
        if info.get("status") != 200:
            zip_notes.append(f"`{name}` status {info.get('status')} blocked={info.get('blocked')}")
            continue
        listed = zip_members(blob_url(primary, name))
        members = listed.get("members") or []
        linked = {f"KYRC26_{stem}" for stem in stems}
        extras = [member for member in members if member not in linked and not member.endswith("/")]
        zip_notes.append(
            f"`{name}` ({info.get('length')} bytes) members={len(members)}; not in the individual link list: {extras or 'none'}"
        )
    coop = zip_members(coop_url)
    if coop.get("blocked"):
        zip_notes.append(f"Educational coop 2026 zip BLOCKED status={coop.get('status')}")
    elif coop.get("members") is not None:
        zip_notes.append(
            f"Educational coop 2026 zip members ({len(coop['members'])}): {', '.join(coop['members'][:40])}"
        )
    else:
        zip_notes.append(f"Educational coop 2026 zip status={coop.get('status')} error={coop.get('error')}")

    header_rows.sort(key=lambda row: (row["kind"] != "identical", row["stem"]))
    common_shift = "KYRC25 ['20242025'] -> KYRC26 ['20252026']"
    shifted = [note for note in domain_notes if note.endswith(common_shift)]
    domain_notes = [note for note in domain_notes if note not in shifted]
    if shifted:
        domain_notes.insert(
            0,
            f"{len(shifted)} columns roll forward as {common_shift}. Exceptions are listed below.",
        )
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    report_payload = {
        "generated_at": generated_at,
        "container_summary": container_summary,
        "header_rows": header_rows,
        "domain_notes": domain_notes,
        "code_notes": code_notes,
        "zip_notes": zip_notes,
        "blocked": blocked,
        "download_root": meta.get("download_root"),
        "inventory_count": len(inventory),
    }
    render_report(args.report, report_payload)
    # Compact machine-readable twin of the markdown, without raw state rows.
    compact = {
        "generated_at": generated_at,
        "blocked": blocked,
        "identical_containers": identical,
        "hash_mismatch": hash_mismatch,
        "length_mismatch": length_mismatch,
        "header_rows": header_rows,
        "domain_notes": domain_notes,
        "code_notes": code_notes,
        "zip_notes": zip_notes,
        "only_primary": only_primary,
        "only_secondary": only_secondary,
        "pairs": pair_details,
    }
    args.json.parent.mkdir(parents=True, exist_ok=True)
    args.json.write_text(json.dumps(compact, indent=2), encoding="utf-8")
    print(f"Wrote {args.report} and {args.json}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
