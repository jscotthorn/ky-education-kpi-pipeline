#!/usr/bin/env python3
"""Diff pytest failures against the journal 105 develop baseline.

The develop baseline is the bullet list under ``## Develop test baseline`` in
``notes/105--kyrc26-completeness-audit.md`` (commit b14ec3c: 42 failed).
There is no CI on this repository, so a script pull request should commit
the report this script writes.

Examples
--------
python3 scripts/compare_pytest_to_journal105.py
python3 scripts/compare_pytest_to_journal105.py --pytest-output /tmp/pytest.txt \\
    --write notes/kyrc26-chronic-absenteeism-pytest-diff.txt
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
JOURNAL = ROOT / "notes" / "105--kyrc26-completeness-audit.md"


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


def failure_node_ids(pytest_output: str) -> list[str]:
    """Return node ids from pytest's FAILED lines."""
    found = []
    for line in pytest_output.splitlines():
        stripped = line.strip()
        if stripped.startswith("FAILED "):
            found.append(stripped.split("FAILED ", 1)[1].strip())
    return found


def summary_line(pytest_output: str) -> str:
    """Return the pytest totals line, or a note when it is missing."""
    found = "(no pytest summary line found)"
    for line in pytest_output.splitlines():
        stripped = line.strip().strip("=").strip()
        if re.search(r"\d+ failed, \d+ passed", stripped):
            found = stripped
    return found


def build_report(observed: list[str], pytest_output: str, baseline: list[str]) -> str:
    """Format the node-id diff."""
    observed_set = set(observed)
    baseline_set = set(baseline)
    only_run = sorted(observed_set - baseline_set)
    only_journal = sorted(baseline_set - observed_set)
    lines = [
        "Pytest failures compared with notes/105--kyrc26-completeness-audit.md",
        "Develop baseline: origin/develop b14ec3c, 42 failed / 375 passed / 104 skipped.",
        summary_line(pytest_output),
        f"Baseline node ids: {len(baseline_set)}",
        f"Observed failure node ids: {len(observed_set)}",
        f"Only in this run ({len(only_run)}):",
    ]
    lines.extend(f"  {node_id}" for node_id in only_run)
    if not only_run:
        lines.append("  (none)")
    lines.append(f"Only in journal 105 ({len(only_journal)}):")
    lines.extend(f"  {node_id}" for node_id in only_journal)
    if not only_journal:
        lines.append("  (none)")
    if not only_run and not only_journal:
        lines.append("Failure node ids match the journal 105 baseline.")
    else:
        lines.append("Failure node ids differ from the journal 105 baseline.")
    return "\n".join(lines) + "\n"


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
    parser.add_argument(
        "--write",
        type=Path,
        help="Also write the report to this path",
    )
    args = parser.parse_args()

    baseline = baseline_node_ids(JOURNAL)
    if args.pytest_output:
        pytest_output = args.pytest_output.read_text(encoding="utf-8")
    else:
        pytest_output = run_pytest()
    observed = failure_node_ids(pytest_output)
    report = build_report(observed, pytest_output, baseline)
    sys.stdout.write(report)
    if args.write:
        destination = args.write if args.write.is_absolute() else ROOT / args.write
        destination.write_text(report, encoding="utf-8")
    only_run = set(observed) - set(baseline)
    only_journal = set(baseline) - set(observed)
    return 0 if not only_run and not only_journal else 1


if __name__ == "__main__":
    raise SystemExit(main())
