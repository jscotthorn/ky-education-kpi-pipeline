#!/usr/bin/env python3
"""
Bayesian Hierarchical Model for School Climate Index

Subclass of BaseHierarchicalModel for school climate analysis.
School climate index measures student perceptions of safety,
engagement, and environment. Scores typically range 0-100.

Usage:
    python school_climate_model.py                    # Default (Finnish horseshoe)
    python school_climate_model.py --horseshoe       # Classic horseshoe
    python school_climate_model.py --normal          # Normal priors (no shrinkage)
"""

from typing import Dict, List, Tuple

from base_hierarchical_model import BaseHierarchicalModel


class SchoolClimateModel(BaseHierarchicalModel):
    """
    Bayesian hierarchical model for school climate index scores.

    School climate scores in Kentucky average ~85 with moderate variance.
    Higher scores indicate better perceived climate.
    """

    @property
    def OUTCOME_NAME(self) -> str:
        return 'school_climate_index'

    @property
    def MODEL_NAME(self) -> str:
        return 'school_climate'

    def get_state_mean_prior(self) -> Tuple[float, float]:
        """School climate ~85 (0-100 scale)."""
        return (85.0, 10.0)

    def get_variance_priors(self) -> Dict[str, float]:
        """Moderate variance for climate scores."""
        return {
            'sigma_district': 5.0,
            'sigma_school': 4.0,
            'sigma_y': 4.0
        }

    def get_tau_scale(self) -> float:
        """Tau scale for horseshoe."""
        return 0.5

    def get_slab_parameters(self) -> Tuple[float, float]:
        """Slab parameters for Finnish horseshoe."""
        return (3.0, 4.0)

    def get_tract_columns(self) -> List[str]:
        """Tract columns for school climate model."""
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
        return "BAYESIAN HIERARCHICAL MODEL - SCHOOL CLIMATE INDEX"


def main():
    """Main execution."""
    import argparse

    parser = argparse.ArgumentParser(description="Run Bayesian hierarchical model for school climate")
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

    prior_type = args.prior
    if args.normal:
        prior_type = "normal"
    elif args.horseshoe:
        prior_type = "horseshoe"

    non_centered = not args.centered

    model = SchoolClimateModel(verbose=True)
    school_effects = model.run(
        prior_type=prior_type,
        non_centered=non_centered,
        run_prior_check=not args.skip_prior_check,
        run_loo=not args.skip_loo
    )

    return school_effects


if __name__ == "__main__":
    main()
