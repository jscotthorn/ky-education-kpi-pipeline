#!/usr/bin/env python3
"""
Bayesian Hierarchical Model for Postsecondary Readiness

Subclass of BaseHierarchicalModel for postsecondary readiness analysis.
Postsecondary readiness measures whether students are academically
prepared for college or career pathways upon graduation.

Kentucky's average postsecondary readiness rate is ~83%.

Usage:
    python postsecondary_readiness_model.py                    # Default (Finnish horseshoe)
    python postsecondary_readiness_model.py --horseshoe       # Classic horseshoe
    python postsecondary_readiness_model.py --normal          # Normal priors (no shrinkage)
    python postsecondary_readiness_model.py --non-centered    # Non-centered parameterization
"""

from typing import Dict, List, Tuple

from base_hierarchical_model import BaseHierarchicalModel


class PostsecondaryReadinessModel(BaseHierarchicalModel):
    """
    Bayesian hierarchical model for postsecondary readiness rates.

    Postsecondary readiness in Kentucky averages ~83% with moderate variance.
    This is a high school outcome measuring college/career preparation.
    """

    @property
    def OUTCOME_NAME(self) -> str:
        return 'postsecondary_readiness_rate'

    @property
    def MODEL_NAME(self) -> str:
        return 'postsecondary_readiness'

    def get_state_mean_prior(self) -> Tuple[float, float]:
        """KY postsecondary readiness ~83%."""
        return (83.0, 10.0)

    def get_variance_priors(self) -> Dict[str, float]:
        """Narrower variance for postsecondary readiness (high mean, less spread)."""
        return {
            'sigma_district': 6.0,
            'sigma_school': 4.0,
            'sigma_y': 4.0
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
        Tract columns for postsecondary readiness model.
        Includes all tract variables - socioeconomic factors strongly
        influence college/career readiness.
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
        return "BAYESIAN HIERARCHICAL MODEL - POSTSECONDARY READINESS"


def main():
    """Main execution."""
    import argparse

    parser = argparse.ArgumentParser(description="Run Bayesian hierarchical model for postsecondary readiness")
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
    model = PostsecondaryReadinessModel(verbose=True)
    school_effects = model.run(
        prior_type=prior_type,
        non_centered=non_centered,
        run_prior_check=not args.skip_prior_check,
        run_loo=not args.skip_loo
    )

    return school_effects


if __name__ == "__main__":
    main()
