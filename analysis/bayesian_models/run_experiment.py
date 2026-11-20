#!/usr/bin/env python3
"""
MLflow Experiment Runner for Bayesian Hierarchical Models

This script runs model experiments with MLflow tracking, enabling:
- Systematic comparison of different prior specifications
- Sensitivity analysis across configurations
- Reproducible experiment tracking

Usage:
    # Run a single experiment with default settings
    python run_experiment.py graduation --student-group all_students

    # Run with a named configuration
    python run_experiment.py graduation --student-group all_students --run-name baseline

    # Run sensitivity analysis with tighter priors
    python run_experiment.py graduation --student-group all_students \\
        --run-name tighter_priors --prior-scale 0.75

    # Run sensitivity analysis with looser priors
    python run_experiment.py graduation --student-group all_students \\
        --run-name looser_priors --prior-scale 1.5

    # Compare runs
    python run_experiment.py graduation --compare

    # Start MLflow UI
    python run_experiment.py --ui

Available Models:
    graduation, reading_grade3, math_grade8, chronic_absenteeism,
    kindergarten_readiness, postsecondary_readiness, postsecondary_enrollment,
    school_climate

Author: FCPS Equity Council Analysis Team
Date: November 2025
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple
import subprocess

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from mlflow_tracking import (
    MLflowTracker,
    ModelRunConfig,
    ModelMetrics,
    create_run_config_from_model,
    create_metrics_from_model,
    get_all_published_across_experiments,
)

# Model imports - lazy loaded to avoid import overhead
MODEL_REGISTRY = {
    'graduation': 'graduation_rate_model.GraduationRateModel',
    'reading_grade3': 'reading_grade3_model.ReadingGrade3Model',
    'math_grade8': 'math_grade8_model.MathGrade8Model',
    'chronic_absenteeism': 'chronic_absenteeism_model.ChronicAbsenteeismModel',
    'kindergarten_readiness': 'kindergarten_readiness_model.KindergartenReadinessModel',
    'postsecondary_readiness': 'postsecondary_readiness_model.PostsecondaryReadinessModel',
    'postsecondary_enrollment': 'postsecondary_enrollment_model.PostsecondaryEnrollmentModel',
    'school_climate': 'school_climate_model.SchoolClimateModel',
    'el_progress_elementary': 'el_progress_elementary_model.ELProgressElementaryModel',
    'el_progress_middle': 'el_progress_middle_model.ELProgressMiddleModel',
    'el_progress_high': 'el_progress_high_model.ELProgressHighModel',
}


def get_model_class(model_name: str):
    """Dynamically import and return the model class."""
    if model_name not in MODEL_REGISTRY:
        raise ValueError(f"Unknown model: {model_name}. Available: {list(MODEL_REGISTRY.keys())}")

    module_name, class_name = MODEL_REGISTRY[model_name].rsplit('.', 1)
    module = __import__(module_name, fromlist=[class_name])
    return getattr(module, class_name)


def run_experiment(
    model_name: str,
    student_group: str = "all_students",
    run_name: Optional[str] = None,
    prior_type: str = "finnish",
    prior_scale: float = 1.0,
    non_centered: bool = True,
    county_varying_slopes: bool = True,
    run_prior_check: bool = True,
    run_loo: bool = True,
    description: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None,
    mcmc_draws: int = 4000,
    mcmc_tune: int = 2000,
) -> Tuple[str, ModelMetrics]:
    """
    Run a model experiment with MLflow tracking.

    Args:
        model_name: Name of the model to run (e.g., 'graduation')
        student_group: Student group slug
        run_name: Name for this run (auto-generated if not provided)
        prior_type: Prior type ("normal", "horseshoe", "finnish")
        prior_scale: Multiplier for prior scales (1.0 = baseline, 0.75 = tighter, 1.5 = looser)
        non_centered: Use non-centered parameterization
        county_varying_slopes: Enable county-varying slopes
        run_prior_check: Run prior predictive check
        run_loo: Run LOO cross-validation
        description: Run description
        tags: Additional tags
        mcmc_draws: Number of MCMC draws
        mcmc_tune: Number of tuning steps

    Returns:
        Tuple of (run_id, metrics)
    """
    import numpy as np

    # Generate run name if not provided
    if run_name is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        scale_suffix = ""
        if prior_scale != 1.0:
            scale_suffix = f"_scale{prior_scale:.2f}"
        run_name = f"{student_group}_{prior_type}{scale_suffix}_{timestamp}"

    # Set up MLflow tracker
    experiment_name = f"bayesian_{model_name}"
    tracker = MLflowTracker(experiment_name=experiment_name)

    # Prepare tags
    run_tags = tags or {}
    run_tags.update({
        "model": model_name,
        "student_group": student_group,
        "prior_type": prior_type,
        "prior_scale": str(prior_scale),
    })

    # Load and configure model
    ModelClass = get_model_class(model_name)
    model = ModelClass(verbose=True, student_group=student_group)

    # Scale priors if requested
    if prior_scale != 1.0:
        original_get_variance_priors = model.get_variance_priors

        def scaled_variance_priors():
            priors = original_get_variance_priors()
            return {k: v * prior_scale for k, v in priors.items()}

        model.get_variance_priors = scaled_variance_priors

        original_get_state_mean_prior = model.get_state_mean_prior

        def scaled_state_mean_prior():
            mu, sigma = original_get_state_mean_prior()
            return (mu, sigma * prior_scale)

        model.get_state_mean_prior = scaled_state_mean_prior

        print(f"\nApplied prior scale factor: {prior_scale}")

    # Start MLflow run
    with tracker.start_run(run_name=run_name, tags=run_tags, description=description):
        try:
            # Load and prepare data
            model.load_and_prepare_data()

            # Create and log config
            config = create_run_config_from_model(model, prior_type=prior_type)
            config.mcmc_draws = mcmc_draws
            config.mcmc_tune = mcmc_tune
            config.non_centered = non_centered
            tracker.log_config(config)

            # Log prior scale as explicit parameter
            tracker.log_param("prior_scale_factor", prior_scale)

            # Build model
            model.build_model(
                prior_type=prior_type,
                non_centered=non_centered,
                county_varying_slopes=county_varying_slopes
            )

            # Prior predictive check
            if run_prior_check:
                prior_passed = model.prior_predictive_check()
                tracker.log_metric("prior_check_passed", int(prior_passed))

            # Sample posterior
            model.sample_posterior(
                draws=mcmc_draws,
                tune=mcmc_tune,
                chains=4,
                target_accept=0.95
            )

            # Check convergence
            converged = model.check_convergence()

            # Posterior predictive checks
            ppc_passed = model.posterior_predictive_checks()

            # Collinearity diagnostics
            model.compute_collinearity_diagnostics()

            # LOO-CV
            loo_result = None
            if run_loo:
                loo_result, loo_reliable = model.loo_cross_validation()

            # Save results
            school_effects_df = model.save_results()

            # Create and log metrics
            metrics = create_metrics_from_model(model, loo_result)
            metrics.ppc_passed = ppc_passed
            metrics.prior_check_passed = prior_passed if run_prior_check else None
            tracker.log_metrics(metrics)

            # Log artifacts
            tracker.log_artifacts(model.MODEL_DIR, artifact_path="model_outputs")
            tracker.log_artifacts(model.DIAG_DIR, artifact_path="diagnostics")

            # Set final status tag - based on MCMC convergence only
            # LOO reliability is informational (common to have some high-k observations)
            if metrics.convergence_passed:
                tracker.set_tag("status", "success")
            else:
                tracker.set_tag("status", "needs_review")

            # Separate tag for LOO status
            if metrics.loo_reliable is not None:
                tracker.set_tag("loo_status", "reliable" if metrics.loo_reliable else "some_high_k")

            print(f"\n{'=' * 60}")
            print(f"EXPERIMENT COMPLETE")
            print(f"{'=' * 60}")
            print(f"Run ID: {tracker.run_id}")
            print(f"Run Name: {run_name}")
            print(f"Experiment: {experiment_name}")
            print(f"\nKey Metrics:")
            print(f"  Convergence: {'PASSED' if metrics.convergence_passed else 'ISSUES'}")
            print(f"  Divergences: {metrics.n_divergences} ({metrics.divergence_pct:.2f}%)")
            print(f"  Max R-hat: {metrics.max_rhat:.4f}")
            print(f"  Min ESS (bulk): {metrics.min_ess_bulk:.0f}")
            if metrics.loo_elpd is not None:
                print(f"  LOO-ELPD: {metrics.loo_elpd:.2f} (SE: {metrics.loo_se:.2f})")
                loo_status = "YES" if metrics.loo_reliable else f"NO ({metrics.loo_pct_bad_k:.1f}% high-k obs)"
                print(f"  LOO Reliable: {loo_status}")
            print(f"\nTo view results:")
            print(f"  python run_experiment.py --ui")
            print(f"  # Or: mlflow ui --backend-store-uri sqlite:///{Path(__file__).parent.parent / 'mlruns.db'}")

            return tracker.run_id, metrics

        except Exception as e:
            tracker.set_tag("status", "failed")
            tracker.set_tag("error", str(e)[:250])
            raise


def compare_runs(model_name: str, run_ids: Optional[list] = None, n_runs: int = 5):
    """
    Compare recent runs for a model.

    Args:
        model_name: Name of the model
        run_ids: Specific run IDs to compare (optional)
        n_runs: Number of recent runs to compare if run_ids not provided
    """
    import pandas as pd

    experiment_name = f"bayesian_{model_name}"
    tracker = MLflowTracker(experiment_name=experiment_name)

    if run_ids:
        runs = [tracker.client.get_run(rid) for rid in run_ids]
    else:
        runs = tracker.get_all_runs()[:n_runs]

    if not runs:
        print(f"No runs found for experiment: {experiment_name}")
        return

    # Build comparison table
    comparison_data = []
    for run in runs:
        row = {
            'run_name': run.info.run_name,
            'run_id': run.info.run_id[:8],
            'student_group': run.data.params.get('student_group', 'N/A'),
            'prior_type': run.data.params.get('prior_type', 'N/A'),
            'prior_scale': run.data.params.get('prior_scale_factor', '1.0'),
            'loo_elpd': run.data.metrics.get('loo_elpd', None),
            'loo_se': run.data.metrics.get('loo_se', None),
            'n_divergences': run.data.metrics.get('n_divergences', None),
            'max_rhat': run.data.metrics.get('max_rhat', None),
            'convergence': 'PASS' if run.data.metrics.get('convergence_passed', 0) else 'FAIL',
            'status': run.data.tags.get('status', 'unknown'),
        }
        comparison_data.append(row)

    df = pd.DataFrame(comparison_data)

    print(f"\n{'=' * 80}")
    print(f"RUN COMPARISON: {experiment_name}")
    print(f"{'=' * 80}")
    print(df.to_string(index=False))

    # Highlight best run by LOO-ELPD
    if 'loo_elpd' in df.columns and df['loo_elpd'].notna().any():
        best_idx = df['loo_elpd'].idxmax()
        best_run = df.loc[best_idx]
        print(f"\nBest run by LOO-ELPD: {best_run['run_name']} (ELPD: {best_run['loo_elpd']:.2f})")


def publish_run(model_name: str, run_id: str, student_group: str):
    """
    Publish a run for use in production.

    Args:
        model_name: Model/indicator name
        run_id: Run ID to publish
        student_group: Student group for this run
    """
    experiment_name = f"bayesian_{model_name}"
    tracker = MLflowTracker(experiment_name=experiment_name)

    # Verify run exists
    try:
        run = tracker.client.get_run(run_id)
    except Exception as e:
        print(f"Error: Run {run_id} not found in experiment {experiment_name}")
        return False

    # Get run details
    run_name = run.info.run_name
    convergence = run.data.metrics.get("convergence_passed", 0)
    loo_elpd = run.data.metrics.get("loo_elpd", "N/A")

    print(f"\nPublishing run:")
    print(f"  Experiment: {experiment_name}")
    print(f"  Run: {run_name} ({run_id[:8]})")
    print(f"  Student Group: {student_group}")
    print(f"  Convergence: {'PASSED' if convergence else 'ISSUES'}")
    print(f"  LOO-ELPD: {loo_elpd}")

    if not convergence:
        print("\nWARNING: This run has convergence issues. Publish anyway? (y/n)")
        response = input().strip().lower()
        if response != 'y':
            print("Cancelled.")
            return False

    tracker.publish_run(run_id, student_group)
    print(f"\nRun published successfully!")
    print(f"This run will be used when combine_results.py runs with --from-mlflow")
    return True


def unpublish_run(model_name: str, student_group: str):
    """
    Unpublish the current published run for a model/student_group.

    Args:
        model_name: Model/indicator name
        student_group: Student group to unpublish
    """
    experiment_name = f"bayesian_{model_name}"
    tracker = MLflowTracker(experiment_name=experiment_name)

    published = tracker.get_published_run(student_group)
    if not published:
        print(f"No published run found for {model_name}/{student_group}")
        return False

    run_name = published.info.run_name
    run_id = published.info.run_id

    print(f"\nUnpublishing:")
    print(f"  Experiment: {experiment_name}")
    print(f"  Run: {run_name} ({run_id[:8]})")
    print(f"  Student Group: {student_group}")

    tracker.unpublish_run(run_id)
    print(f"\nRun unpublished successfully!")
    return True


def list_published(model_name: Optional[str] = None):
    """
    List all published runs.

    Args:
        model_name: Optional model name to filter by
    """
    if model_name:
        # List published runs for a specific model
        experiment_name = f"bayesian_{model_name}"
        tracker = MLflowTracker(experiment_name=experiment_name)
        published = tracker.list_published()

        if not published:
            print(f"No published runs for {model_name}")
            return

        print(f"\nPublished runs for {model_name}:")
        print("-" * 80)
        for student_group, info in sorted(published.items()):
            conv = "PASS" if info.get("convergence_passed") else "ISSUES"
            loo = info.get("loo_elpd", "N/A")
            loo_str = f"{loo:.2f}" if isinstance(loo, float) else str(loo)
            print(f"  {student_group:30s} {info['run_name']:25s} "
                  f"Conv:{conv:6s} LOO:{loo_str:>8s}")
    else:
        # List all published runs across all experiments
        all_published = get_all_published_across_experiments()

        if not all_published:
            print("No published runs found")
            return

        print(f"\nAll published runs:")
        print("=" * 90)

        # Group by indicator
        by_indicator = {}
        for key, info in all_published.items():
            indicator = info['indicator']
            if indicator not in by_indicator:
                by_indicator[indicator] = []
            by_indicator[indicator].append(info)

        for indicator in sorted(by_indicator.keys()):
            print(f"\n{indicator}:")
            print("-" * 80)
            for info in sorted(by_indicator[indicator], key=lambda x: x['student_group']):
                conv = "PASS" if info.get("convergence_passed") else "ISSUES"
                loo = info.get("loo_elpd", "N/A")
                loo_str = f"{loo:.2f}" if isinstance(loo, float) else str(loo)
                print(f"  {info['student_group']:30s} {info['run_name']:25s} "
                      f"Conv:{conv:6s} LOO:{loo_str:>8s}")

        print(f"\nTotal: {len(all_published)} published runs")


def start_mlflow_ui():
    """Start the MLflow UI."""
    db_path = Path(__file__).parent.parent / "mlruns.db"
    tracking_uri = f"sqlite:///{db_path}"

    # Artifacts are stored in mlartifacts directory
    artifacts_dir = Path(__file__).parent.parent / "mlartifacts"
    artifacts_dir.mkdir(exist_ok=True)

    print(f"Starting MLflow UI...")
    print(f"Tracking URI: {tracking_uri}")
    print(f"Artifacts: {artifacts_dir}")
    print(f"Open http://localhost:5000 in your browser")
    print(f"Press Ctrl+C to stop\n")

    try:
        subprocess.run(
            ["mlflow", "ui",
             "--backend-store-uri", tracking_uri,
             "--default-artifact-root", str(artifacts_dir)],
            check=True
        )
    except KeyboardInterrupt:
        print("\nMLflow UI stopped")
    except FileNotFoundError:
        print("Error: mlflow command not found. Make sure mlflow is installed:")
        print("  pip install mlflow")


def main():
    parser = argparse.ArgumentParser(
        description="Run Bayesian model experiments with MLflow tracking",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument(
        'model',
        nargs='?',
        choices=list(MODEL_REGISTRY.keys()),
        help="Model to run"
    )
    parser.add_argument(
        '--student-group', '-g',
        default='all_students',
        help="Student group slug (default: all_students)"
    )
    parser.add_argument(
        '--run-name', '-n',
        help="Name for this run (auto-generated if not provided)"
    )
    parser.add_argument(
        '--prior-type', '-p',
        choices=['normal', 'horseshoe', 'finnish'],
        default='finnish',
        help="Prior type (default: finnish)"
    )
    parser.add_argument(
        '--prior-scale', '-s',
        type=float,
        default=1.0,
        help="Prior scale factor: 1.0=baseline, 0.75=tighter, 1.5=looser (default: 1.0)"
    )
    parser.add_argument(
        '--draws',
        type=int,
        default=4000,
        help="Number of MCMC draws (default: 4000)"
    )
    parser.add_argument(
        '--tune',
        type=int,
        default=2000,
        help="Number of tuning steps (default: 2000)"
    )
    parser.add_argument(
        '--skip-loo',
        action='store_true',
        help="Skip LOO cross-validation"
    )
    parser.add_argument(
        '--skip-prior-check',
        action='store_true',
        help="Skip prior predictive check"
    )
    parser.add_argument(
        '--no-county-slopes',
        action='store_true',
        help="Disable county-varying slopes"
    )
    parser.add_argument(
        '--description', '-d',
        help="Run description"
    )
    parser.add_argument(
        '--compare', '-c',
        action='store_true',
        help="Compare recent runs instead of running a new experiment"
    )
    parser.add_argument(
        '--ui',
        action='store_true',
        help="Start MLflow UI"
    )

    # Publishing commands
    parser.add_argument(
        '--publish',
        metavar='RUN_ID',
        help="Publish a run for production use. Requires --student-group."
    )
    parser.add_argument(
        '--unpublish',
        action='store_true',
        help="Unpublish the current published run. Requires model and --student-group."
    )
    parser.add_argument(
        '--list-published',
        action='store_true',
        help="List all published runs. Optionally filter by model."
    )

    args = parser.parse_args()

    if args.ui:
        start_mlflow_ui()
        return

    if args.list_published:
        list_published(args.model)
        return

    if args.publish:
        if args.model is None:
            print("Error: --publish requires a model name")
            return
        publish_run(args.model, args.publish, args.student_group)
        return

    if args.unpublish:
        if args.model is None:
            print("Error: --unpublish requires a model name")
            return
        unpublish_run(args.model, args.student_group)
        return

    if args.model is None:
        parser.print_help()
        return

    if args.compare:
        compare_runs(args.model)
        return

    # Run experiment
    run_id, metrics = run_experiment(
        model_name=args.model,
        student_group=args.student_group,
        run_name=args.run_name,
        prior_type=args.prior_type,
        prior_scale=args.prior_scale,
        non_centered=True,
        county_varying_slopes=not args.no_county_slopes,
        run_prior_check=not args.skip_prior_check,
        run_loo=not args.skip_loo,
        description=args.description,
        mcmc_draws=args.draws,
        mcmc_tune=args.tune,
    )


if __name__ == "__main__":
    main()
