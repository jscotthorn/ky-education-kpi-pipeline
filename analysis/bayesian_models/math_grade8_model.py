#!/usr/bin/env python3
"""
Bayesian Hierarchical Model for Eighth Grade Math Proficiency

Subclass of BaseHierarchicalModel for 8th grade math analysis.
Eighth grade math is a critical transition point - algebra readiness
is a key predictor of high school math success and post-secondary outcomes.

Kentucky's average 8th grade math proficiency is typically ~35-40%.

Usage:
    python math_grade8_model.py                    # Default (Finnish horseshoe)
    python math_grade8_model.py --horseshoe       # Classic horseshoe
    python math_grade8_model.py --normal          # Normal priors (no shrinkage)
    python math_grade8_model.py --non-centered    # Non-centered parameterization
"""

from typing import Dict, List, Tuple

from base_hierarchical_model import BaseHierarchicalModel


class MathGrade8Model(BaseHierarchicalModel):
    """
    Bayesian hierarchical model for 8th grade math proficiency.

    Math proficiency in Kentucky averages ~35-40% with high variance across schools.
    This model uses similar priors to reading grade 3 given the comparable
    proficiency rates and variation patterns.
    """

    @property
    def OUTCOME_NAME(self) -> str:
        return 'math_proficiency_rate'

    @property
    def MODEL_NAME(self) -> str:
        return 'math_grade8'

    def get_state_mean_prior(self) -> Tuple[float, float]:
        """KY 8th grade math proficiency ~38%."""
        return (38.0, 15.0)

    def get_variance_priors(self) -> Dict[str, float]:
        """Wider variance for math (more variation across schools)."""
        return {
            'sigma_district': 10.0,
            'sigma_school': 5.0,
            'sigma_y': 5.0
        }

    def get_tau_scale(self) -> float:
        """
        Tau scale for horseshoe.
        With ~22 predictors and ~5 expected effective: 0.5
        """
        return 0.5

    def get_slab_parameters(self) -> Tuple[float, float]:
        """Slab parameters for Finnish horseshoe."""
        return (3.0, 4.0)

    def get_tract_columns(self) -> List[str]:
        """
        Tract columns for math model.
        Includes all tract variables (math outcomes may have different drivers than reading).
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
        return "BAYESIAN HIERARCHICAL MODEL - GRADE 8 MATH PROFICIENCY"


def main():
    """Main execution."""
    import argparse

    parser = argparse.ArgumentParser(description="Run Bayesian hierarchical model for 8th grade math")
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

    # Run model
    model = MathGrade8Model(verbose=True)
    school_effects = model.run(
        prior_type=prior_type,
        non_centered=non_centered,
        run_prior_check=not args.skip_prior_check,
        run_loo=not args.skip_loo
    )

    return school_effects


if __name__ == "__main__":
    main()
