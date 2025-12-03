#!/usr/bin/env python3
"""
Bayesian Hierarchical Model for Third Grade Reading Proficiency

Subclass of BaseHierarchicalModel for 3rd grade reading analysis.
Third grade reading is a critical indicator - Kentucky's Read to Achieve
initiative focuses on ensuring all students read proficiently by grade 3.

Kentucky's average 3rd grade reading proficiency is ~43%.

Usage:
    python reading_grade3_model.py                    # Default (Finnish horseshoe)
    python reading_grade3_model.py --horseshoe       # Classic horseshoe
    python reading_grade3_model.py --normal          # Normal priors (no shrinkage)
    python reading_grade3_model.py --non-centered    # Non-centered parameterization
    python reading_grade3_model.py --student-group african_american
    python reading_grade3_model.py --all-groups
"""

from typing import Dict, List, Optional, Tuple
import sys
from pathlib import Path
import pandas as pd

from base_hierarchical_model import BaseHierarchicalModel

# Add config directory to path for imports
CONFIG_DIR = Path(__file__).parent.parent / "config"
sys.path.insert(0, str(CONFIG_DIR))
from student_groups import ALL_GROUP_SLUGS, TARGET_GROUP_SLUGS


# Default priors for all_students (fallback if prior analysis not available)
DEFAULT_STATE_MEAN_PRIOR = (46.4, 5.0)
DEFAULT_VARIANCE_PRIORS = {
    'sigma_district': 18.4,
    'sigma_school': 16.8,
    'sigma_y': 12.6
}


class ReadingGrade3Model(BaseHierarchicalModel):
    """
    Bayesian hierarchical model for 3rd grade reading proficiency.

    Reading proficiency in Kentucky averages ~43% with high variance across schools.
    This model uses wider variance priors than graduation to accommodate the
    greater variation in reading outcomes.

    Priors are loaded from the prior analysis outputs when available, allowing
    group-specific priors for each demographic group (QuantCrit methodology).
    """

    def __init__(self, verbose: bool = True, student_group: str = "all_students"):
        """
        Initialize the model and load group-specific priors.

        Args:
            verbose: If True, print progress messages
            student_group: Student group slug (e.g., 'all_students', 'african_american')
        """
        # Call parent init first to set up student_group_slug
        super().__init__(verbose=verbose, student_group=student_group)

        # Load group-specific priors from prior analysis
        self._load_group_priors()

    def _load_group_priors(self) -> None:
        """
        Load group-specific priors from prior analysis output files.

        Priors are loaded from:
        analysis/outputs/prior_analysis/third_grade_reading/{student_group}/recommended_priors.csv
        """
        prior_dir = self.BASE_DIR / "analysis" / "outputs" / "prior_analysis" / "third_grade_reading" / self.student_group_slug
        priors_file = prior_dir / "recommended_priors.csv"

        if priors_file.exists():
            try:
                priors_df = pd.read_csv(priors_file)

                # Extract mu_state prior
                mu_state_row = priors_df[priors_df['parameter'] == 'mu_state']
                if len(mu_state_row) > 0:
                    self._state_mean_location = float(mu_state_row['location'].iloc[0])
                    self._state_mean_scale = float(mu_state_row['scale'].iloc[0])
                else:
                    self._state_mean_location, self._state_mean_scale = DEFAULT_STATE_MEAN_PRIOR

                # Extract variance priors
                sigma_district_row = priors_df[priors_df['parameter'] == 'sigma_district']
                sigma_school_row = priors_df[priors_df['parameter'] == 'sigma_school']
                sigma_y_row = priors_df[priors_df['parameter'] == 'sigma_y']

                self._variance_priors = {
                    'sigma_district': float(sigma_district_row['scale'].iloc[0]) if len(sigma_district_row) > 0 else DEFAULT_VARIANCE_PRIORS['sigma_district'],
                    'sigma_school': float(sigma_school_row['scale'].iloc[0]) if len(sigma_school_row) > 0 else DEFAULT_VARIANCE_PRIORS['sigma_school'],
                    'sigma_y': float(sigma_y_row['scale'].iloc[0]) if len(sigma_y_row) > 0 else DEFAULT_VARIANCE_PRIORS['sigma_y'],
                }

                self.log(f"\nLoaded group-specific priors for {self.student_group_name}:")
                self.log(f"  mu_state ~ Normal({self._state_mean_location:.1f}, {self._state_mean_scale:.1f})")
                self.log(f"  sigma_district ~ HalfCauchy({self._variance_priors['sigma_district']:.1f})")
                self.log(f"  sigma_school ~ HalfCauchy({self._variance_priors['sigma_school']:.1f})")
                self.log(f"  sigma_y ~ HalfCauchy({self._variance_priors['sigma_y']:.1f})")

            except Exception as e:
                self.log(f"\nWarning: Could not load priors from {priors_file}: {e}")
                self.log("Using default priors (all_students)")
                self._state_mean_location, self._state_mean_scale = DEFAULT_STATE_MEAN_PRIOR
                self._variance_priors = DEFAULT_VARIANCE_PRIORS.copy()
        else:
            self.log(f"\nPrior analysis not found for {self.student_group_name}: {priors_file}")
            self.log("Using default priors (all_students)")
            self._state_mean_location, self._state_mean_scale = DEFAULT_STATE_MEAN_PRIOR
            self._variance_priors = DEFAULT_VARIANCE_PRIORS.copy()

    @property
    def OUTCOME_NAME(self) -> str:
        return 'reading_proficiency_rate'

    @property
    def MODEL_NAME(self) -> str:
        return 'reading_grade3'

    def get_state_mean_prior(self) -> Tuple[float, float]:
        """
        Return group-specific state mean prior.

        Priors are loaded from prior analysis outputs, with defaults for all_students:
        - All Students: ~46.4%
        - Economically Disadvantaged: ~40.1%
        - African American: ~32.4%
        - Students with Disabilities: ~37.4%
        - Hispanic: ~36.4%
        - Homeless: ~38.1%
        - English Learner: ~33.8%
        """
        return (self._state_mean_location, self._state_mean_scale)

    def get_variance_priors(self) -> Dict[str, float]:
        """
        Return group-specific variance priors.

        Priors are loaded from prior analysis outputs (1.5x observed SD).
        Different student groups may have different levels of variation.
        """
        return self._variance_priors

    def get_slab_parameters(self) -> Tuple[float, float]:
        """Slab parameters for Finnish horseshoe: c2 ~ InverseGamma(2, 8)."""
        return (2.0, 4.0)

    def get_tract_columns(self) -> List[str]:
        """
        Tract columns for reading model.
        Includes all tract variables (reading outcomes may have different drivers).
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
        return "BAYESIAN HIERARCHICAL MODEL - GRADE 3 READING PROFICIENCY"


def run_for_group(student_group: str, prior_type: str, non_centered: bool,
                  run_prior_check: bool, run_loo: bool):
    """Run model for a single student group."""
    model = ReadingGrade3Model(verbose=True, student_group=student_group)
    return model.run(
        prior_type=prior_type,
        non_centered=non_centered,
        run_prior_check=run_prior_check,
        run_loo=run_loo
    )


def main():
    """Main execution."""
    import argparse

    parser = argparse.ArgumentParser(description="Run Bayesian hierarchical model for 3rd grade reading")
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
