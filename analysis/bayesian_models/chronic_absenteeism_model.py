#!/usr/bin/env python3
"""
Bayesian Hierarchical Model for Chronic Absenteeism

Subclass of BaseHierarchicalModel for chronic absenteeism analysis.
Chronic absenteeism measures the percentage of students who miss
10% or more of school days. Lower rates are better.

Kentucky's average chronic absenteeism rate is ~29%.

Usage:
    python chronic_absenteeism_model.py                               # Default (all_students)
    python chronic_absenteeism_model.py --student-group african_american
    python chronic_absenteeism_model.py --all-groups                  # Run for all target groups
    python chronic_absenteeism_model.py --horseshoe                   # Classic horseshoe
    python chronic_absenteeism_model.py --normal                      # Normal priors (no shrinkage)
"""

import sys
from pathlib import Path
from typing import Dict, List, Tuple

# Add config directory to path for imports
CONFIG_DIR = Path(__file__).parent.parent / "config"
sys.path.insert(0, str(CONFIG_DIR))
from student_groups import ALL_GROUP_SLUGS, TARGET_GROUP_SLUGS

from base_hierarchical_model import BaseHierarchicalModel


class ChronicAbsenteeismModel(BaseHierarchicalModel):
    """
    Bayesian hierarchical model for chronic absenteeism rates.

    Chronic absenteeism in Kentucky averages ~29% with high variance.
    Note: Unlike other outcomes where higher is better, lower chronic
    absenteeism is desirable. Interpretation of effects is reversed.
    """

    @property
    def OUTCOME_NAME(self) -> str:
        return 'chronic_absenteeism_rate'

    @property
    def MODEL_NAME(self) -> str:
        return 'chronic_absenteeism'

    def get_state_mean_prior(self) -> Tuple[float, float]:
        """KY chronic absenteeism ~24.4% (from empirical prior analysis)."""
        return (24.4, 5.0)

    def get_variance_priors(self) -> Dict[str, float]:
        """
        Variance priors from empirical analysis (1.5x observed SD).
        Chronic absenteeism shows high district variation with tighter observation noise.
        """
        return {
            'sigma_district': 15.1,
            'sigma_school': 9.9,
            'sigma_y': 4.5
        }

    def get_slab_parameters(self) -> Tuple[float, float]:
        """Slab parameters for Finnish horseshoe: c2 ~ InverseGamma(2, 8)."""
        return (2.0, 4.0)

    def get_tract_columns(self) -> List[str]:
        """
        Tract columns for chronic absenteeism model.
        Includes all tract variables - socioeconomic factors strongly
        influence attendance patterns.
        """
        return [
            'tract_median_household_income',
            'tract_poverty_rate',
            'tract_pct_bachelors_plus',
            'tract_unemployment_rate',
            'tract_pct_single_parent',
            'tract_pct_owner_occupied',
            'tract_pct_broadband',
            'tract_pct_housing_cost_burden_30_plus',
        ]

    def get_model_description(self) -> str:
        return "BAYESIAN HIERARCHICAL MODEL - CHRONIC ABSENTEEISM"


def run_for_group(student_group: str, prior_type: str, non_centered: bool,
                  run_prior_check: bool, run_loo: bool):
    """Run model for a single student group."""
    model = ChronicAbsenteeismModel(verbose=True, student_group=student_group)
    return model.run(
        prior_type=prior_type,
        non_centered=non_centered,
        run_prior_check=run_prior_check,
        run_loo=run_loo
    )


def main():
    """Main execution."""
    import argparse

    parser = argparse.ArgumentParser(description="Run Bayesian hierarchical model for chronic absenteeism")
    parser.add_argument('--student-group', type=str, default='all_students',
                       choices=ALL_GROUP_SLUGS,
                       help=f"Student group to analyze. Choices: {ALL_GROUP_SLUGS}")
    parser.add_argument('--all-groups', action='store_true',
                       help="Run for all student groups (all_students + target demographics)")
    parser.add_argument('--prior', type=str, choices=['normal', 'horseshoe', 'finnish'],
                       default='finnish', help="Prior type (default: finnish)")
    parser.add_argument('--horseshoe', action='store_true', help="Use classic horseshoe prior")
    parser.add_argument('--finnish', action='store_true', help="Use Finnish horseshoe (default)")
    parser.add_argument('--normal', action='store_true', help="Use normal priors")
    parser.add_argument('--non-centered', action='store_true', default=True,
                       help="Use non-centered parameterization (default: True)")
    parser.add_argument('--centered', action='store_true', help="Use centered parameterization")
    parser.add_argument('--skip-loo', action='store_true', help="Skip LOO cross-validation")
    parser.add_argument('--skip-prior-check', action='store_true', help="Skip prior predictive check")
    args = parser.parse_args()

    # Determine prior type
    prior_type = args.prior
    if args.normal:
        prior_type = "normal"
    elif args.horseshoe:
        prior_type = "horseshoe"

    # Non-centered default unless --centered specified
    non_centered = not args.centered

    # Determine which groups to run
    if args.all_groups:
        groups_to_run = ['all_students'] + TARGET_GROUP_SLUGS
        print(f"\nRunning for {len(groups_to_run)} student groups: {groups_to_run}\n")
    else:
        groups_to_run = [args.student_group]

    # Run for each group
    results = {}
    for i, group in enumerate(groups_to_run, 1):
        if len(groups_to_run) > 1:
            print(f"\n{'#' * 60}")
            print(f"# GROUP {i}/{len(groups_to_run)}: {group}")
            print(f"{'#' * 60}\n")
        results[group] = run_for_group(
            group, prior_type, non_centered,
            not args.skip_prior_check, not args.skip_loo
        )

    if len(groups_to_run) > 1:
        print(f"\n{'=' * 60}")
        print(f"ALL GROUPS COMPLETE: {len(results)} models run")
        print("=" * 60)

    return results if len(results) > 1 else list(results.values())[0]


if __name__ == "__main__":
    main()
