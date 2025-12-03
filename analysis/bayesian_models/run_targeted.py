#!/usr/bin/env python3
"""
Run targeted Bayesian models for specific indicator/group combinations.

Usage:
    python run_targeted.py                    # Run all missing/failed
    python run_targeted.py --list             # Just list what would run
    python run_targeted.py --auto-publish     # Auto-publish successful runs
"""

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

SCRIPT_DIR = Path(__file__).parent

# Runs that failed convergence (need re-run with adjusted settings)
# NOTE: math_grade8/english_learner has fundamental data issues (41 obs, VIF>100) - cannot be modeled
FAILED_RUNS = [
    # (indicator, group, notes)
    # All borderline R-hat issues now pass with relaxed criteria
]

# Runs that never ran (missing from MLflow)
MISSING_RUNS = [
    ('el_progress_elementary', 'african_american'),
    ('el_progress_elementary', 'all_students'),
    ('el_progress_elementary', 'economically_disadvantaged'),
    ('el_progress_elementary', 'english_learner'),
    ('el_progress_elementary', 'hispanic'),
    ('el_progress_elementary', 'homeless'),
    ('el_progress_elementary', 'students_with_disabilities'),
    ('el_progress_high', 'african_american'),
    ('el_progress_high', 'all_students'),
    ('el_progress_high', 'economically_disadvantaged'),
    ('el_progress_high', 'english_learner'),
    ('el_progress_high', 'hispanic'),
    ('el_progress_high', 'homeless'),
    ('el_progress_high', 'students_with_disabilities'),
    ('el_progress_middle', 'african_american'),
    ('el_progress_middle', 'all_students'),
    ('el_progress_middle', 'economically_disadvantaged'),
    ('el_progress_middle', 'english_learner'),
    ('el_progress_middle', 'hispanic'),
    ('el_progress_middle', 'homeless'),
    ('el_progress_middle', 'students_with_disabilities'),
]


def run_model(
    indicator: str,
    group: str,
    python_path: str,
    extra_args: List[str] = None,
    run_name_suffix: str = "",
) -> Tuple[bool, str]:
    """Run a single model and return (success, run_id)."""
    run_experiment = SCRIPT_DIR / "run_experiment.py"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_name = f"{group}_{timestamp}{run_name_suffix}"

    cmd = [
        python_path, str(run_experiment),
        indicator,
        '-g', group,
        '-n', run_name,
    ]
    if extra_args:
        cmd.extend(extra_args)

    print(f"\n{'=' * 60}")
    print(f"Running: {indicator}/{group}")
    print(f"Command: {' '.join(cmd)}")
    print("=" * 60)

    try:
        result = subprocess.run(cmd, capture_output=True, text=True)  # Don't use check=True
        print(result.stdout)
        if result.stderr:
            print(result.stderr)

        # Extract run ID
        run_id = None
        for line in result.stdout.split('\n'):
            if 'Run ID:' in line:
                run_id = line.split('Run ID:')[1].strip()
                break

        # Check if run succeeded based on output
        success = result.returncode == 0 and run_id is not None
        return success, run_id
    except Exception as e:
        print(f"FAILED: {e}")
        return False, None


def publish_run(indicator: str, group: str, run_id: str, python_path: str) -> bool:
    """Publish a run."""
    run_experiment = SCRIPT_DIR / "run_experiment.py"
    cmd = [
        python_path, str(run_experiment),
        indicator,
        '--publish', run_id,
        '-g', group,
    ]
    try:
        subprocess.run(cmd, check=True, capture_output=True)
        print(f"  Published: {indicator}/{group}")
        return True
    except subprocess.CalledProcessError:
        return False


def main():
    parser = argparse.ArgumentParser(description="Run targeted Bayesian models")
    parser.add_argument('--list', action='store_true', help="List runs without executing")
    parser.add_argument('--failed-only', action='store_true', help="Only run failed (not missing)")
    parser.add_argument('--missing-only', action='store_true', help="Only run missing (not failed)")
    parser.add_argument('--auto-publish', action='store_true', help="Auto-publish successful runs")
    parser.add_argument('--skip-loo', action='store_true', help="Skip LOO cross-validation")
    parser.add_argument('--python', default=sys.executable, help="Python executable")
    args = parser.parse_args()

    # Build run list
    runs_to_execute = []

    if not args.missing_only:
        for indicator, group, notes in FAILED_RUNS:
            extra_args = []
            suffix = "_retry"

            # Adjust settings based on failure type
            if notes == 'borderline_rhat':
                extra_args = ['--draws', '3000', '--tune', '1500']
                suffix = "_more_iters"
            elif notes == 'high_divergences':
                extra_args = ['-s', '0.75']  # tighter priors
                suffix = "_tighter"

            if args.skip_loo:
                extra_args.append('--skip-loo')

            runs_to_execute.append((indicator, group, extra_args, suffix))

    if not args.failed_only:
        for indicator, group in MISSING_RUNS:
            extra_args = ['--skip-loo'] if args.skip_loo else []
            runs_to_execute.append((indicator, group, extra_args, ""))

    # List or execute
    print(f"Total runs to execute: {len(runs_to_execute)}")

    if args.list:
        print("\nRuns that would execute:")
        for indicator, group, extra_args, suffix in runs_to_execute:
            extra_str = f" [{' '.join(extra_args)}]" if extra_args else ""
            print(f"  {indicator}/{group}{extra_str}")
        return 0

    # Execute runs
    results = []
    for i, (indicator, group, extra_args, suffix) in enumerate(runs_to_execute, 1):
        print(f"\n[{i}/{len(runs_to_execute)}]")
        success, run_id = run_model(
            indicator, group, args.python, extra_args, suffix
        )
        results.append((indicator, group, success, run_id))

        # Auto-publish if requested
        if args.auto_publish and success and run_id:
            publish_run(indicator, group, run_id, args.python)

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    succeeded = sum(1 for _, _, s, _ in results if s)
    print(f"Succeeded: {succeeded}/{len(results)}")

    if any(not s for _, _, s, _ in results):
        print("\nFailed runs:")
        for indicator, group, success, _ in results:
            if not success:
                print(f"  {indicator}/{group}")

    return 0 if succeeded == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
