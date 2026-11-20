#!/usr/bin/env python3
"""
Diagnose MLflow Runs - Analyze failure patterns across experiments.

Usage:
    python diagnose_runs.py                    # Summary of all runs
    python diagnose_runs.py --failures         # Focus on failed runs
    python diagnose_runs.py --indicator graduation  # Specific indicator
    python diagnose_runs.py --group african_american  # Specific student group
    python diagnose_runs.py --details          # Show detailed failure info
"""

import argparse
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional

import mlflow
from mlflow.tracking import MlflowClient

# Set up MLflow
SCRIPT_DIR = Path(__file__).parent
ANALYSIS_DIR = SCRIPT_DIR.parent
DB_PATH = ANALYSIS_DIR / "mlruns.db"


def setup_mlflow():
    """Initialize MLflow connection."""
    mlflow.set_tracking_uri(f"sqlite:///{DB_PATH}")
    return MlflowClient()


def get_all_runs(client: MlflowClient, indicator: Optional[str] = None) -> List[dict]:
    """Get all runs, optionally filtered by indicator."""
    runs = []

    # Get all experiments
    experiments = client.search_experiments()

    for exp in experiments:
        if not exp.name.startswith("bayesian_"):
            continue

        exp_indicator = exp.name.replace("bayesian_", "")
        if indicator and exp_indicator != indicator:
            continue

        # Get runs for this experiment
        exp_runs = client.search_runs(
            experiment_ids=[exp.experiment_id],
            order_by=["start_time DESC"],
        )

        for run in exp_runs:
            runs.append({
                'indicator': exp_indicator,
                'run_id': run.info.run_id,
                'run_name': run.info.run_name or run.info.run_id[:8],
                'status': run.info.status,
                'student_group': run.data.params.get('student_group', 'unknown'),
                'prior_type': run.data.params.get('prior_type', 'unknown'),
                'prior_scale': float(run.data.params.get('prior_scale', 1.0)),
                # Metrics
                'convergence_passed': run.data.metrics.get('convergence_passed', 0) == 1,
                'n_divergences': int(run.data.metrics.get('n_divergences', -1)),
                'divergence_pct': run.data.metrics.get('divergence_pct', -1),
                'max_rhat': run.data.metrics.get('max_rhat', -1),
                'min_ess_bulk': run.data.metrics.get('min_ess_bulk', -1),
                'min_ess_tail': run.data.metrics.get('min_ess_tail', -1),
                'loo_reliable': run.data.metrics.get('loo_reliable'),  # None if not computed
                'loo_elpd': run.data.metrics.get('loo_elpd', None),
                # Tags
                'is_published': run.data.tags.get('published', 'false') == 'true',
            })

    return runs


def categorize_failure(run: dict) -> List[str]:
    """Categorize the type of failure for a run."""
    issues = []

    if run['status'] != 'FINISHED':
        issues.append(f"run_status:{run['status']}")
        return issues  # Can't diagnose further if run didn't finish

    # Use divergence percentage if available, else fall back to count
    div_pct = run.get('divergence_pct', -1)
    if div_pct > 0:
        if div_pct >= 1.0:
            issues.append("high_divergences")  # >=1% is concerning
        elif div_pct > 0:
            issues.append("some_divergences")  # <1% is usually acceptable
    elif run['n_divergences'] > 0:
        # Fallback for old runs without divergence_pct
        if run['n_divergences'] > 100:
            issues.append("high_divergences")
        else:
            issues.append("some_divergences")

    if run['max_rhat'] > 1.1:
        issues.append("high_rhat")
    elif run['max_rhat'] > 1.01:
        issues.append("borderline_rhat")

    if run['min_ess_bulk'] != -1 and run['min_ess_bulk'] < 100:
        issues.append("very_low_ess")
    elif run['min_ess_bulk'] != -1 and run['min_ess_bulk'] < 400:
        issues.append("low_ess")

    # LOO reliability (only flag if LOO was run and is unreliable)
    if run['loo_reliable'] is not None and run['loo_reliable'] == 0:
        issues.append("loo_unreliable")

    return issues if issues else ["unknown"]


def print_summary(runs: List[dict], show_details: bool = False):
    """Print summary statistics."""
    if not runs:
        print("No runs found.")
        return

    total = len(runs)
    finished = [r for r in runs if r['status'] == 'FINISHED']
    converged = [r for r in finished if r['convergence_passed']]
    published = [r for r in runs if r['is_published']]

    print("=" * 70)
    print("MLFLOW RUNS SUMMARY")
    print("=" * 70)
    print(f"Total runs:        {total}")
    print(f"Finished:          {len(finished)} ({100*len(finished)/total:.0f}%)")
    print(f"Converged:         {len(converged)} ({100*len(converged)/total:.0f}%)")
    print(f"Published:         {len(published)} ({100*len(published)/total:.0f}%)")

    # Failure categorization
    failed_runs = [r for r in runs if not r['convergence_passed'] or r['status'] != 'FINISHED']

    if failed_runs:
        print("\n" + "-" * 70)
        print("FAILURE ANALYSIS")
        print("-" * 70)

        issue_counts: Dict[str, int] = defaultdict(int)
        issue_examples: Dict[str, List[dict]] = defaultdict(list)

        for run in failed_runs:
            issues = categorize_failure(run)
            for issue in issues:
                issue_counts[issue] += 1
                if len(issue_examples[issue]) < 3:
                    issue_examples[issue].append(run)

        print("\nIssue breakdown (runs may have multiple issues):")
        for issue, count in sorted(issue_counts.items(), key=lambda x: -x[1]):
            pct = 100 * count / len(failed_runs)
            print(f"  {issue:25s} {count:3d} ({pct:4.0f}%)")

        if show_details:
            print("\n" + "-" * 70)
            print("ISSUE DETAILS WITH EXAMPLES")
            print("-" * 70)

            for issue in sorted(issue_counts.keys(), key=lambda x: -issue_counts[x]):
                print(f"\n### {issue} ({issue_counts[issue]} runs)")

                if issue == "high_divergences":
                    print("  Cause: Prior-likelihood conflict or difficult posterior geometry")
                    print("  Fix: Adjust priors, reparameterize model, or increase adapt_delta")
                elif issue == "some_divergences":
                    print("  Cause: Minor sampling issues, often acceptable if <1% of samples")
                    print("  Fix: Often OK, but monitor. Try increasing adapt_delta if persistent")
                elif issue == "high_rhat":
                    print("  Cause: Chains haven't converged to same distribution")
                    print("  Fix: Increase tuning iterations, check for multimodality")
                elif issue == "borderline_rhat":
                    print("  Cause: Chains may need more iterations to fully converge")
                    print("  Fix: Increase draws/tune, or accept as borderline OK")
                elif issue == "very_low_ess":
                    print("  Cause: High autocorrelation in chains")
                    print("  Fix: Reparameterize, increase iterations, or thin samples")
                elif issue == "low_ess":
                    print("  Cause: Moderate autocorrelation")
                    print("  Fix: Increase iterations or accept with caution")
                elif issue == "loo_unreliable":
                    print("  Cause: Some observations have high Pareto k values")
                    print("  Fix: Check for outliers, consider robust likelihood")

                print("\n  Examples:")
                for ex in issue_examples[issue][:2]:
                    print(f"    - {ex['indicator']}/{ex['student_group']}")
                    print(f"      divergences={ex['n_divergences']}, rhat={ex['max_rhat']:.3f}, "
                          f"ess={ex['min_ess_bulk']:.0f}")

    # By indicator
    print("\n" + "-" * 70)
    print("BY INDICATOR")
    print("-" * 70)

    by_indicator: Dict[str, List[dict]] = defaultdict(list)
    for run in runs:
        by_indicator[run['indicator']].append(run)

    print(f"\n{'Indicator':<25} {'Total':>6} {'Finish':>6} {'Conv':>6} {'Pub':>6}")
    print("-" * 55)
    for indicator in sorted(by_indicator.keys()):
        ind_runs = by_indicator[indicator]
        ind_finished = len([r for r in ind_runs if r['status'] == 'FINISHED'])
        ind_converged = len([r for r in ind_runs if r['convergence_passed']])
        ind_published = len([r for r in ind_runs if r['is_published']])
        print(f"{indicator:<25} {len(ind_runs):>6} {ind_finished:>6} {ind_converged:>6} {ind_published:>6}")

    # By student group
    print("\n" + "-" * 70)
    print("BY STUDENT GROUP")
    print("-" * 70)

    by_group: Dict[str, List[dict]] = defaultdict(list)
    for run in runs:
        by_group[run['student_group']].append(run)

    print(f"\n{'Student Group':<30} {'Total':>6} {'Finish':>6} {'Conv':>6}")
    print("-" * 55)
    for group in sorted(by_group.keys()):
        grp_runs = by_group[group]
        grp_finished = len([r for r in grp_runs if r['status'] == 'FINISHED'])
        grp_converged = len([r for r in grp_runs if r['convergence_passed']])
        print(f"{group:<30} {len(grp_runs):>6} {grp_finished:>6} {grp_converged:>6}")


def print_failures_list(runs: List[dict], student_group: Optional[str] = None):
    """Print list of failed runs with diagnostics."""
    failed = [r for r in runs if not r['convergence_passed'] or r['status'] != 'FINISHED']

    if student_group:
        failed = [r for r in failed if r['student_group'] == student_group]

    if not failed:
        print("No failed runs found.")
        return

    print("=" * 90)
    print(f"FAILED RUNS ({len(failed)} total)")
    print("=" * 90)

    # Sort by indicator then student group
    failed.sort(key=lambda x: (x['indicator'], x['student_group']))

    print(f"\n{'Indicator':<22} {'Group':<25} {'Div':>5} {'Rhat':>6} {'ESS':>6} {'Issues'}")
    print("-" * 90)

    for run in failed:
        issues = categorize_failure(run)
        issue_str = ", ".join(issues)[:25]

        div_str = str(run['n_divergences']) if run['n_divergences'] >= 0 else "N/A"
        rhat_str = f"{run['max_rhat']:.3f}" if run['max_rhat'] > 0 else "N/A"
        ess_str = f"{run['min_ess_bulk']:.0f}" if run['min_ess_bulk'] > 0 else "N/A"

        print(f"{run['indicator']:<22} {run['student_group']:<25} {div_str:>5} {rhat_str:>6} {ess_str:>6} {issue_str}")


def suggest_remediation(runs: List[dict]):
    """Suggest remediation strategies based on failure patterns."""
    failed = [r for r in runs if not r['convergence_passed'] or r['status'] != 'FINISHED']

    if not failed:
        print("All runs converged successfully!")
        return

    # Analyze patterns
    all_issues: Dict[str, int] = defaultdict(int)
    by_indicator: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    by_group: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for run in failed:
        issues = categorize_failure(run)
        for issue in issues:
            all_issues[issue] += 1
            by_indicator[run['indicator']][issue] += 1
            by_group[run['student_group']][issue] += 1

    print("\n" + "=" * 70)
    print("REMEDIATION RECOMMENDATIONS")
    print("=" * 70)

    # Check for indicator-specific issues
    print("\n### Indicator-Specific Issues")
    for indicator, issues in by_indicator.items():
        total_failures = sum(1 for r in failed if r['indicator'] == indicator)
        if total_failures >= 3:
            top_issue = max(issues.items(), key=lambda x: x[1])
            print(f"\n{indicator} ({total_failures} failures):")
            print(f"  Primary issue: {top_issue[0]} ({top_issue[1]} runs)")

            if top_issue[0] in ["high_divergences", "some_divergences"]:
                print("  → Check prior specification in prior_analysis/indicators/{indicator}/")
                print("  → Consider running with --prior-scale 0.75 for tighter priors")
            elif top_issue[0] in ["high_rhat", "borderline_rhat"]:
                print("  → Try increasing iterations: --draws 4000 --tune 2000")
            elif top_issue[0] in ["very_low_ess", "low_ess"]:
                print("  → Model may need reparameterization")
                print("  → Check for near-collinearity in covariates")

    # Check for group-specific issues
    print("\n### Student Group Issues")
    for group, issues in sorted(by_group.items(), key=lambda x: -sum(x[1].values())):
        total_failures = sum(1 for r in failed if r['student_group'] == group)
        total_runs = sum(1 for r in runs if r['student_group'] == group)
        if total_failures > total_runs * 0.5:  # More than half failed
            print(f"\n{group} ({total_failures}/{total_runs} failed):")
            print("  → May have insufficient data for reliable estimation")
            print("  → Check analysis/datasets/*_{group}.csv for sample sizes")
            print("  → Consider stronger priors or pooling with similar groups")

    # Overall recommendations
    print("\n### Quick Fixes to Try")

    if all_issues.get("high_divergences", 0) + all_issues.get("some_divergences", 0) > len(failed) * 0.3:
        print("\n1. DIVERGENCE ISSUES (most common)")
        print("   Run sensitivity analysis with tighter priors:")
        print("   python run_all_groups.py --skip-datasets -s 0.75 --auto-publish")

    if all_issues.get("high_rhat", 0) + all_issues.get("borderline_rhat", 0) > len(failed) * 0.2:
        print("\n2. CONVERGENCE ISSUES")
        print("   Increase MCMC iterations in run_experiment.py or model scripts")
        print("   Default is 2000 draws, 1000 tune - try 4000/2000")

    if all_issues.get("very_low_ess", 0) + all_issues.get("low_ess", 0) > len(failed) * 0.2:
        print("\n3. MIXING ISSUES (low ESS)")
        print("   Check for multicollinearity in covariates")
        print("   Review model parameterization in bayesian_models/*_model.py")


def main():
    parser = argparse.ArgumentParser(description="Diagnose MLflow experiment runs")
    parser.add_argument('--indicator', '-i', help="Filter by indicator")
    parser.add_argument('--group', '-g', help="Filter by student group")
    parser.add_argument('--failures', '-f', action='store_true', help="Show failure list")
    parser.add_argument('--details', '-d', action='store_true', help="Show detailed failure analysis")
    parser.add_argument('--remediation', '-r', action='store_true', help="Show remediation suggestions")
    args = parser.parse_args()

    client = setup_mlflow()
    runs = get_all_runs(client, indicator=args.indicator)

    if args.group:
        runs = [r for r in runs if r['student_group'] == args.group]

    if args.failures:
        print_failures_list(runs, student_group=args.group)
    else:
        print_summary(runs, show_details=args.details)

    if args.remediation:
        suggest_remediation(runs)


if __name__ == "__main__":
    main()
