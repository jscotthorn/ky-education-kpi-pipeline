#!/usr/bin/env python3
"""Diff pytest failures against the journal 105 develop baseline.

The develop baseline is the bullet list under ``## Develop test baseline`` in
``notes/105--kyrc26-completeness-audit.md`` (commit b14ec3c: 42 failed).
Paste this script's report into the pull request body. Do not commit a
per-script copy of the output.

The process exits nonzero when a failure node id is not on that list, when
pytest reports an error, or when an ERROR line is present. ERROR lines count
as failures. A baseline failure that now passes is reported as removed and
does not by itself fail the process.

Examples
--------
python3 scripts/compare_pytest_to_journal105.py
python3 scripts/compare_pytest_to_journal105.py --pytest-output /tmp/pytest.txt
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
JOURNAL = ROOT / "notes" / "105--kyrc26-completeness-audit.md"
COUNT_PATTERNS = {
    "failed": re.compile(r"(\d+) failed\b"),
    "passed": re.compile(r"(\d+) passed\b"),
    "skipped": re.compile(r"(\d+) skipped\b"),
    "error": re.compile(r"(\d+) errors?\b"),
}


def baseline_node_ids(journal: Path) -> list[str]:
    """Return the develop failure node ids from journal 105."""
    text = journal.read_text(encoding="utf-8")
    start = text.find("## Develop test baseline")
    if start < 0:
        raise SystemExit(f"{journal} has no '## Develop test baseline' section")
    rest = text[start:]
    next_heading = rest.find("\n## ", 1)
    section = rest if next_heading < 0 else rest[:next_heading]
    return re.findall(r"- `(tests/[^`]+)`", section)


def _node_id(rest: str) -> str:
    """Drop a trailing pytest reason (`` - AssertionError: ...``)."""
    if " - " in rest:
        return rest.split(" - ", 1)[0].strip()
    return rest.strip()


def node_ids_with_prefix(pytest_output: str, prefix: str) -> list[str]:
    """Return node ids from pytest summary lines that start with ``prefix``."""
    found = []
    for line in pytest_output.splitlines():
        stripped = line.strip()
        if stripped.startswith(prefix):
            found.append(_node_id(stripped.split(prefix, 1)[1]))
    return found


def summary_and_counts(pytest_output: str) -> tuple[str, dict[str, int], bool]:
    """Parse failed, passed, skipped, and error counts from the totals line."""
    summary = ""
    for line in pytest_output.splitlines():
        stripped = line.strip().strip("=").strip()
        if COUNT_PATTERNS["failed"].search(stripped) or COUNT_PATTERNS["passed"].search(stripped):
            summary = stripped
    counts = {name: 0 for name in COUNT_PATTERNS}
    if not summary:
        return "(no pytest summary line found)", counts, False
    for name, pattern in COUNT_PATTERNS.items():
        match = pattern.search(summary)
        if match:
            counts[name] = int(match.group(1))
    return summary, counts, True


def head_sha() -> str:
    """Return the current commit, or ``unknown`` when git cannot answer."""
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        return "unknown"
    return completed.stdout.strip()


def pandas_version() -> str:
    """Return the installed pandas version."""
    try:
        import pandas
    except ImportError:
        return "unavailable"
    return str(pandas.__version__)


def build_report(
    failed_ids: list[str],
    error_ids: list[str],
    pytest_output: str,
    baseline: list[str],
) -> tuple[str, bool]:
    """Format the report and whether the run has a new failure or error.

    ERROR node ids are treated as failures. They are new unless the baseline
    list already names them.
    """
    summary, counts, found_summary = summary_and_counts(pytest_output)
    # ERROR lines count as failures even when the totals line omitted them.
    counts["error"] = max(counts["error"], len(set(error_ids)))
    baseline_set = set(baseline)
    observed = set(failed_ids) | set(error_ids)
    added = sorted(observed - baseline_set)
    removed = sorted(baseline_set - observed)
    # A summary that claims more failures than the baseline, without a parsed
    # node id, is still a new failure.
    summary_has_new_failure = found_summary and counts["failed"] > len(baseline_set)
    has_new_problem = bool(added) or counts["error"] > 0 or bool(error_ids) or summary_has_new_failure
    if not found_summary:
        has_new_problem = True

    lines = [
        "Pytest failures compared with notes/105--kyrc26-completeness-audit.md",
        f"Head: {head_sha()}",
        f"Python: {sys.version.split()[0]}",
        f"pandas: {pandas_version()}",
        "Develop baseline: origin/develop b14ec3c, 42 failed / 375 passed / 104 skipped.",
        summary,
        (
            f"Counts: failed={counts['failed']} passed={counts['passed']} "
            f"skipped={counts['skipped']} errors={counts['error']}"
        ),
        f"Baseline node ids: {len(baseline_set)}",
        f"Observed FAILED node ids: {len(set(failed_ids))}",
        f"Observed ERROR node ids: {len(set(error_ids))} (counted as failures)",
        f"Added node ids ({len(added)}):",
    ]
    lines.extend(f"  {node_id}" for node_id in added)
    if not added:
        lines.append("  (none)")
    lines.append(f"Removed node ids ({len(removed)}):")
    lines.extend(f"  {node_id}" for node_id in removed)
    if not removed:
        lines.append("  (none)")
    if not has_new_problem and not removed:
        lines.append("No new failures or errors. Failure node ids match the journal 105 baseline.")
    elif not has_new_problem:
        lines.append("No new failures or errors. Removed baseline node ids are listed above.")
    else:
        lines.append("New failure or error. This does not match the journal 105 baseline.")
    return "\n".join(lines) + "\n", has_new_problem


def run_pytest() -> str:
    """Run the full suite and return combined stdout and stderr."""
    completed = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/", "--tb=no", "-q"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    return completed.stdout + completed.stderr


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--pytest-output",
        type=Path,
        help="Read a saved pytest log instead of running the suite",
    )
    args = parser.parse_args()

    baseline = baseline_node_ids(JOURNAL)
    if args.pytest_output:
        pytest_output = args.pytest_output.read_text(encoding="utf-8")
    else:
        pytest_output = run_pytest()
    failed_ids = node_ids_with_prefix(pytest_output, "FAILED ")
    error_ids = node_ids_with_prefix(pytest_output, "ERROR ")
    report, has_new_problem = build_report(failed_ids, error_ids, pytest_output, baseline)
    sys.stdout.write(report)
    return 1 if has_new_problem else 0


if __name__ == "__main__":
    raise SystemExit(main())
