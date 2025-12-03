#!/usr/bin/env python3
"""
Run All Bayesian Analysis Pipelines for All Student Groups

This script runs the complete Bayesian bright spots analysis pipeline for all
target student groups (All Students + 6 target demographics following QuantCrit methodology).

Pipeline steps:
1. Generate analysis datasets for each indicator
2. Run Bayesian hierarchical models for each indicator (with MLflow tracking)
3. Optionally publish successful runs
4. Combine all results into bayesian_results.json

Usage:
    python run_all_groups.py                    # Run everything with MLflow tracking
    python run_all_groups.py --datasets-only    # Only generate datasets
    python run_all_groups.py --skip-datasets    # Skip dataset generation, use existing
    python run_all_groups.py --skip-loo         # Skip LOO cross-validation (faster)
    python run_all_groups.py --auto-publish     # Automatically publish successful runs
    python run_all_groups.py --indicators graduation chronic_absenteeism  # Specific indicators

Student Groups Processed:
    - all_students
    - economically_disadvantaged
    - african_american
    - students_with_disabilities
    - hispanic
    - homeless
    - english_learner
"""

import argparse
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, Tuple

# Script directories
SCRIPT_DIR = Path(__file__).parent
ANALYSIS_SCRIPTS_DIR = SCRIPT_DIR / "scripts"
BAYESIAN_MODELS_DIR = SCRIPT_DIR / "bayesian_models"

# Student groups to process
STUDENT_GROUPS = [
    'all_students',
    'economically_disadvantaged',
    'african_american',
    'students_with_disabilities',
    'hispanic',
    'homeless',
    'english_learner',
]

# All available indicators and their corresponding scripts
INDICATORS = {
    'graduation': {
        'analysis_script': 'graduation_analysis.py',
        'model_name': 'graduation',
    },
    'chronic_absenteeism': {
        'analysis_script': 'chronic_absenteeism_analysis.py',
        'model_name': 'chronic_absenteeism',
    },
    'reading_grade3': {
        'analysis_script': 'reading_grade3_analysis.py',
        'model_name': 'reading_grade3',
    },
    'math_grade8': {
        'analysis_script': 'math_grade8_analysis.py',
        'model_name': 'math_grade8',
    },
    'kindergarten_readiness': {
        'analysis_script': 'kindergarten_readiness_analysis.py',
        'model_name': 'kindergarten_readiness',
    },
    'postsecondary_enrollment': {
        'analysis_script': 'postsecondary_enrollment_analysis.py',
        'model_name': 'postsecondary_enrollment',
    },
    'postsecondary_readiness': {
        'analysis_script': 'postsecondary_readiness_analysis.py',
        'model_name': 'postsecondary_readiness',
    },
    'school_climate': {
        'analysis_script': 'school_climate_analysis.py',
        'model_name': 'school_climate',
    },
    'el_progress_elementary': {
        'analysis_script': 'el_progress_elementary_analysis.py',
        'model_name': 'el_progress_elementary',
    },
    'el_progress_middle': {
        'analysis_script': 'el_progress_middle_analysis.py',
        'model_name': 'el_progress_middle',
    },
    'el_progress_high': {
        'analysis_script': 'el_progress_high_analysis.py',
        'model_name': 'el_progress_high',
    },
}


def run_command(cmd: list, description: str, capture_output: bool = False) -> Tuple[bool, Optional[str]]:
    """Run a command and return success status and optional output."""
    print(f"\n{'=' * 60}")
    print(f"RUNNING: {description}")
    print(f"Command: {' '.join(cmd)}")
    print("=" * 60)

    try:
        if capture_output:
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            print(result.stdout)
            if result.stderr:
                print(result.stderr)
            return True, result.stdout
        else:
            result = subprocess.run(cmd, check=True)
            print(f"SUCCESS: {description}")
            return True, None
    except subprocess.CalledProcessError as e:
        print(f"FAILED: {description}")
        print(f"Error: {e}")
        if capture_output and e.stdout:
            print(e.stdout)
        if capture_output and e.stderr:
            print(e.stderr)
        return False, None


def run_single_dataset(indicator: str, python_path: str) -> Tuple[str, bool]:
    """Run a single dataset generation script. Returns (indicator, success)."""
    if indicator not in INDICATORS:
        print(f"Warning: Unknown indicator '{indicator}', skipping")
        return indicator, False

    script = ANALYSIS_SCRIPTS_DIR / INDICATORS[indicator]['analysis_script']
    if not script.exists():
        print(f"Warning: Script not found: {script}")
        return indicator, False

    cmd = [python_path, str(script), '--all-groups']
    success, _ = run_command(cmd, f"Dataset: {indicator}")
    return indicator, success


def run_datasets(indicators: list, python_path: str, max_workers: int = 1) -> dict:
    """Run analysis dataset scripts for specified indicators."""
    results = {}

    if max_workers == 1:
        # Sequential execution
        for indicator in indicators:
            ind, success = run_single_dataset(indicator, python_path)
            results[ind] = success
    else:
        # Parallel execution
        print(f"\nRunning {len(indicators)} dataset scripts with {max_workers} workers...")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(run_single_dataset, ind, python_path): ind
                for ind in indicators
            }
            for future in as_completed(futures):
                indicator, success = future.result()
                results[indicator] = success
                status = "SUCCESS" if success else "FAILED"
                print(f"  [{status}] Dataset: {indicator}")

    return results


def run_single_model(
    indicator: str,
    student_group: str,
    python_path: str,
    run_experiment_script: Path,
    skip_loo: bool,
    run_name_prefix: str,
    prior_scale: float,
    timestamp: str,
) -> Tuple[str, str, dict]:
    """Run a single model. Returns (indicator, student_group, result_dict)."""
    model_name = INDICATORS[indicator]['model_name']

    # Build run name
    if run_name_prefix:
        run_name = f"{run_name_prefix}_{student_group}_{timestamp}"
    else:
        run_name = f"{student_group}_{timestamp}"

    # Build command
    cmd = [
        python_path, str(run_experiment_script),
        model_name,
        '-g', student_group,
        '-n', run_name,
    ]

    if skip_loo:
        cmd.append('--skip-loo')

    if prior_scale != 1.0:
        cmd.extend(['-s', str(prior_scale)])

    description = f"Model: {indicator}/{student_group}"
    success, output = run_command(cmd, description, capture_output=True)

    # Extract run ID from output
    run_id = None
    convergence_passed = False
    if success and output:
        for line in output.split('\n'):
            if 'Run ID:' in line:
                run_id = line.split('Run ID:')[1].strip()
            if 'Convergence: PASSED' in line:
                convergence_passed = True

    return indicator, student_group, {
        'success': success,
        'run_id': run_id,
        'convergence_passed': convergence_passed,
    }


def run_models_with_mlflow(
    indicators: list,
    python_path: str,
    skip_loo: bool = False,
    run_name_prefix: str = "",
    prior_scale: float = 1.0,
    max_workers: int = 1,
) -> Dict[str, Dict[str, dict]]:
    """
    Run Bayesian models using run_experiment.py with MLflow tracking.

    Returns:
        Dict mapping indicator -> student_group -> {success, run_id, convergence_passed}
    """
    results = {ind: {} for ind in indicators if ind in INDICATORS}
    run_experiment_script = BAYESIAN_MODELS_DIR / "run_experiment.py"

    if not run_experiment_script.exists():
        print(f"Error: run_experiment.py not found at {run_experiment_script}")
        return results

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Build list of all jobs
    jobs = []
    for indicator in indicators:
        if indicator not in INDICATORS:
            print(f"Warning: Unknown indicator '{indicator}', skipping")
            continue
        for student_group in STUDENT_GROUPS:
            jobs.append((indicator, student_group))

    total_jobs = len(jobs)

    if max_workers == 1:
        # Sequential execution
        for i, (indicator, student_group) in enumerate(jobs, 1):
            print(f"\n[{i}/{total_jobs}] Running {indicator}/{student_group}...")
            ind, grp, result = run_single_model(
                indicator, student_group, python_path, run_experiment_script,
                skip_loo, run_name_prefix, prior_scale, timestamp
            )
            results[ind][grp] = result
    else:
        # Parallel execution
        print(f"\nRunning {total_jobs} model jobs with {max_workers} workers...")
        completed = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    run_single_model,
                    ind, grp, python_path, run_experiment_script,
                    skip_loo, run_name_prefix, prior_scale, timestamp
                ): (ind, grp)
                for ind, grp in jobs
            }
            for future in as_completed(futures):
                indicator, student_group, result = future.result()
                results[indicator][student_group] = result
                completed += 1
                status = "PASS" if result['success'] else "FAIL"
                conv = "conv:OK" if result.get('convergence_passed') else "conv:ISSUES"
                print(f"  [{completed}/{total_jobs}] [{status}] [{conv}] {indicator}/{student_group}")

    return results


def publish_single_run(
    indicator: str,
    student_group: str,
    result: dict,
    python_path: str,
    run_experiment_script: Path,
    require_convergence: bool,
) -> Tuple[str, str, bool]:
    """Publish a single run. Returns (indicator, student_group, success)."""
    if not result['success'] or not result['run_id']:
        return indicator, student_group, False

    if require_convergence and not result['convergence_passed']:
        print(f"Skipping publish for {indicator}/{student_group}: convergence issues")
        return indicator, student_group, False

    model_name = INDICATORS[indicator]['model_name']

    # Publish the run
    cmd = [
        python_path, str(run_experiment_script),
        model_name,
        '--publish', result['run_id'],
        '-g', student_group,
    ]

    # For convergence-failed runs that we're publishing anyway, auto-confirm
    if not result['convergence_passed']:
        full_cmd = f"echo 'y' | {' '.join(cmd)}"
        try:
            subprocess.run(full_cmd, shell=True, check=True, capture_output=True)
            return indicator, student_group, True
        except subprocess.CalledProcessError:
            return indicator, student_group, False
    else:
        success, _ = run_command(cmd, f"Publish: {indicator}/{student_group}", capture_output=True)
        return indicator, student_group, success


def publish_runs(
    model_results: Dict[str, Dict[str, dict]],
    python_path: str,
    require_convergence: bool = True,
    max_workers: int = 1,
) -> Dict[str, Dict[str, bool]]:
    """
    Publish successful runs.

    Args:
        model_results: Results from run_models_with_mlflow
        python_path: Python executable path
        require_convergence: Only publish runs that passed convergence checks
        max_workers: Number of parallel workers

    Returns:
        Dict mapping indicator -> student_group -> published (bool)
    """
    publish_results = {ind: {} for ind in model_results}
    run_experiment_script = BAYESIAN_MODELS_DIR / "run_experiment.py"

    # Build list of jobs
    jobs = []
    for indicator, groups in model_results.items():
        for student_group, result in groups.items():
            jobs.append((indicator, student_group, result))

    total_jobs = len(jobs)

    if max_workers == 1:
        # Sequential execution
        for indicator, student_group, result in jobs:
            ind, grp, success = publish_single_run(
                indicator, student_group, result, python_path,
                run_experiment_script, require_convergence
            )
            publish_results[ind][grp] = success
    else:
        # Parallel execution
        print(f"\nPublishing {total_jobs} runs with {max_workers} workers...")
        completed = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    publish_single_run,
                    ind, grp, result, python_path,
                    run_experiment_script, require_convergence
                ): (ind, grp)
                for ind, grp, result in jobs
            }
            for future in as_completed(futures):
                indicator, student_group, success = future.result()
                publish_results[indicator][student_group] = success
                completed += 1
                status = "PUBLISHED" if success else "SKIPPED"
                print(f"  [{completed}/{total_jobs}] [{status}] {indicator}/{student_group}")

    return publish_results


def run_combine_results(python_path: str, from_mlflow: bool = True) -> bool:
    """Run combine_results.py to merge all model outputs."""
    script = BAYESIAN_MODELS_DIR / "combine_results.py"
    if not script.exists():
        print(f"Warning: combine_results.py not found")
        return False

    cmd = [python_path, str(script)]
    if from_mlflow:
        cmd.append('--from-mlflow')

    success, _ = run_command(cmd, "Combining all results")
    return success


def main():
    parser = argparse.ArgumentParser(
        description="Run Bayesian analysis pipeline for all student groups with MLflow tracking"
    )
    parser.add_argument(
        '--indicators',
        nargs='+',
        choices=list(INDICATORS.keys()),
        default=list(INDICATORS.keys()),
        help=f"Indicators to process. Default: all"
    )
    parser.add_argument(
        '--datasets-only',
        action='store_true',
        help="Only generate analysis datasets, skip models"
    )
    parser.add_argument(
        '--models-only',
        action='store_true',
        help="Only run models (datasets must already exist)"
    )
    parser.add_argument(
        '--skip-datasets',
        action='store_true',
        help="Skip dataset generation, use existing datasets"
    )
    parser.add_argument(
        '--skip-loo',
        action='store_true',
        help="Skip LOO cross-validation (much faster)"
    )
    parser.add_argument(
        '--skip-combine',
        action='store_true',
        help="Skip the final combine_results step"
    )
    parser.add_argument(
        '--auto-publish',
        action='store_true',
        help="Automatically publish runs that pass convergence checks"
    )
    parser.add_argument(
        '--publish-all',
        action='store_true',
        help="Publish all successful runs, even with convergence issues"
    )
    parser.add_argument(
        '--run-name-prefix',
        type=str,
        default="",
        help="Prefix for run names (e.g., 'baseline', 'sensitivity')"
    )
    parser.add_argument(
        '--prior-scale',
        type=float,
        default=1.0,
        help="Prior scale factor for sensitivity analysis (default: 1.0)"
    )
    parser.add_argument(
        '--no-mlflow-combine',
        action='store_true',
        help="Use filesystem instead of MLflow for combine step"
    )
    parser.add_argument(
        '--workers', '-w',
        type=int,
        default=1,
        help="Number of parallel workers (default: 1 = sequential)"
    )
    parser.add_argument(
        '--python',
        type=str,
        default=sys.executable,
        help="Python executable to use"
    )
    args = parser.parse_args()

    start_time = datetime.now()

    print("=" * 60)
    print("BAYESIAN BRIGHT SPOTS ANALYSIS - ALL STUDENT GROUPS")
    print("=" * 60)
    print(f"Started: {start_time}")
    print(f"Indicators: {args.indicators}")
    print(f"Student Groups: {STUDENT_GROUPS}")
    print(f"Python: {args.python}")
    print(f"Skip LOO: {args.skip_loo}")
    print(f"Auto-publish: {args.auto_publish}")
    print(f"Prior scale: {args.prior_scale}")
    print(f"Workers: {args.workers}")
    if args.run_name_prefix:
        print(f"Run name prefix: {args.run_name_prefix}")

    all_results = {
        'datasets': {},
        'models': {},
        'publish': {},
        'combine': None
    }

    # Step 1: Generate datasets
    if not args.models_only and not args.skip_datasets:
        print("\n" + "#" * 60)
        print("# PHASE 1: GENERATING ANALYSIS DATASETS")
        print("#" * 60)
        all_results['datasets'] = run_datasets(args.indicators, args.python, max_workers=args.workers)
    elif args.skip_datasets:
        print("\n" + "#" * 60)
        print("# PHASE 1: SKIPPING DATASETS (using existing)")
        print("#" * 60)

    # Step 2: Run Bayesian models with MLflow tracking
    if not args.datasets_only:
        print("\n" + "#" * 60)
        print("# PHASE 2: RUNNING BAYESIAN MODELS (MLflow Tracked)")
        print("#" * 60)
        all_results['models'] = run_models_with_mlflow(
            args.indicators,
            args.python,
            skip_loo=args.skip_loo,
            run_name_prefix=args.run_name_prefix,
            prior_scale=args.prior_scale,
            max_workers=args.workers,
        )

    # Step 3: Auto-publish if requested
    if (args.auto_publish or args.publish_all) and all_results['models']:
        print("\n" + "#" * 60)
        print("# PHASE 3: PUBLISHING SUCCESSFUL RUNS")
        print("#" * 60)
        all_results['publish'] = publish_runs(
            all_results['models'],
            args.python,
            require_convergence=not args.publish_all,
            max_workers=args.workers,
        )

    # Step 4: Combine results
    if not args.datasets_only and not args.skip_combine:
        print("\n" + "#" * 60)
        print("# PHASE 4: COMBINING RESULTS")
        print("#" * 60)
        all_results['combine'] = run_combine_results(
            args.python,
            from_mlflow=not args.no_mlflow_combine
        )

    # Summary
    end_time = datetime.now()
    duration = end_time - start_time

    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE")
    print("=" * 60)
    print(f"Duration: {duration}")

    if all_results['datasets']:
        dataset_success = sum(all_results['datasets'].values())
        dataset_total = len(all_results['datasets'])
        print(f"Datasets: {dataset_success}/{dataset_total} succeeded")

    if all_results['models']:
        model_success = 0
        model_total = 0
        convergence_passed = 0
        for indicator, groups in all_results['models'].items():
            for student_group, result in groups.items():
                model_total += 1
                if result['success']:
                    model_success += 1
                if result.get('convergence_passed'):
                    convergence_passed += 1
        print(f"Models: {model_success}/{model_total} succeeded, {convergence_passed} passed convergence")

    if all_results['publish']:
        publish_success = 0
        publish_total = 0
        for indicator, groups in all_results['publish'].items():
            for student_group, success in groups.items():
                publish_total += 1
                if success:
                    publish_success += 1
        print(f"Published: {publish_success}/{publish_total}")

    if all_results['combine'] is not None:
        print(f"Combine: {'succeeded' if all_results['combine'] else 'failed'}")

    # List any failures
    failures = []

    for indicator, success in all_results.get('datasets', {}).items():
        if not success:
            failures.append(f"dataset/{indicator}")

    for indicator, groups in all_results.get('models', {}).items():
        for student_group, result in groups.items():
            if not result['success']:
                failures.append(f"model/{indicator}/{student_group}")

    if all_results['combine'] is False:
        failures.append("combine")

    if failures:
        print(f"\nFailures ({len(failures)}):")
        for f in failures[:10]:  # Show first 10
            print(f"  - {f}")
        if len(failures) > 10:
            print(f"  ... and {len(failures) - 10} more")
        return 1
    else:
        print("\nAll tasks completed successfully!")
        return 0


if __name__ == "__main__":
    sys.exit(main())
