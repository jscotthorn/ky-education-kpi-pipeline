#!/usr/bin/env python3
"""
Base Hierarchical Bayesian Model with Horseshoe Prior Support

This module provides a base class for building hierarchical Bayesian models
with automatic covariate shrinkage using horseshoe priors.

References:
- Carvalho et al. (2010). The horseshoe estimator for sparse signals.
- Harra & Kaplan (2023). On the Performance of Horseshoe Priors for Inducing
  Sparsity in Structural Equation Models. Structural Equation Modeling.
- Piironen & Vehtari (2017). Sparsity information and regularization in the
  horseshoe and other shrinkage priors.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import numpy as np
import pandas as pd
import pymc as pm
import arviz as az


class PriorType(Enum):
    """Types of priors for regression coefficients."""
    NORMAL = "normal"           # Standard weakly informative Normal(0, sigma)
    HORSESHOE = "horseshoe"     # Horseshoe prior for automatic shrinkage
    REGULARIZED_HORSESHOE = "regularized_horseshoe"  # Finnish horseshoe with slab


@dataclass
class ModelConfig:
    """Configuration for hierarchical Bayesian model.

    Attributes:
        prior_type: Type of prior for regression coefficients
        mu_mean: Prior mean for state-level intercept
        mu_sigma: Prior standard deviation for state-level intercept
        sigma_district_beta: Scale parameter for district random effects
        sigma_school_beta: Scale parameter for school random effects
        beta_sigma: Prior sigma for Normal priors on coefficients (if prior_type=NORMAL)
        sigma_y_beta: Scale parameter for observation noise
        horseshoe_tau_scale: Global shrinkage scale for horseshoe (if prior_type=HORSESHOE)
        horseshoe_slab_scale: Slab scale for regularized horseshoe
        horseshoe_slab_df: Slab degrees of freedom for regularized horseshoe
        mcmc_draws: Number of posterior samples per chain
        mcmc_tune: Number of tuning samples
        mcmc_chains: Number of MCMC chains
        mcmc_target_accept: Target acceptance rate for NUTS
        random_seed: Random seed for reproducibility
    """
    prior_type: PriorType = PriorType.NORMAL
    mu_mean: float = 93.0
    mu_sigma: float = 10.0
    sigma_district_beta: float = 5.0
    sigma_school_beta: float = 3.0
    beta_sigma: float = 5.0
    sigma_y_beta: float = 2.0
    # Horseshoe parameters
    horseshoe_tau_scale: float = 1.0
    horseshoe_slab_scale: float = 2.0
    horseshoe_slab_df: float = 4.0
    # MCMC parameters
    mcmc_draws: int = 2000
    mcmc_tune: int = 1000
    mcmc_chains: int = 4
    mcmc_target_accept: float = 0.95
    random_seed: int = 42


@dataclass
class ModelData:
    """Prepared data for hierarchical model.

    Attributes:
        df: Original dataframe
        y: Outcome variable array
        X_scaled: Standardized predictor matrix
        X_mean: Means used for standardization
        X_std: Standard deviations used for standardization
        predictor_names: List of predictor column names
        district_idx: District index for each observation
        school_idx: School index for each observation
        school_to_district: Mapping from school to district
        n_districts: Number of districts
        n_schools: Number of schools
        n_predictors: Number of predictors
        predictor_categories: Dict mapping category names to predictor lists
    """
    df: pd.DataFrame
    y: np.ndarray
    X_scaled: np.ndarray
    X_mean: np.ndarray
    X_std: np.ndarray
    predictor_names: List[str]
    district_idx: np.ndarray
    school_idx: np.ndarray
    school_to_district: np.ndarray
    n_districts: int
    n_schools: int
    n_predictors: int
    predictor_categories: Dict[str, List[str]] = field(default_factory=dict)


class BaseHierarchicalModel(ABC):
    """Abstract base class for hierarchical Bayesian models.

    Provides common functionality for building and fitting hierarchical models
    with optional horseshoe priors for automatic covariate selection.
    """

    def __init__(self, config: Optional[ModelConfig] = None):
        """Initialize with model configuration.

        Args:
            config: Model configuration. Uses defaults if not provided.
        """
        self.config = config or ModelConfig()
        self.model: Optional[pm.Model] = None
        self.trace: Optional[az.InferenceData] = None
        self.data: Optional[ModelData] = None

    @abstractmethod
    def load_data(self, data_path: Path) -> ModelData:
        """Load and prepare data for modeling.

        Subclasses must implement to define outcome and predictors.

        Args:
            data_path: Path to data file

        Returns:
            ModelData object with prepared data
        """
        pass

    def build_model(self, data: ModelData) -> pm.Model:
        """Build hierarchical Bayesian model.

        Constructs a three-level hierarchical model:
        - Level 3: State-level mean
        - Level 2: District random effects
        - Level 1: School random effects (nested in districts)

        Args:
            data: Prepared model data

        Returns:
            PyMC model object
        """
        self.data = data

        with pm.Model() as model:
            # Data containers
            y_obs = pm.Data('y_obs', data.y)
            X = pm.Data('X', data.X_scaled)
            district_idx = pm.Data('district_idx', data.district_idx)
            school_idx = pm.Data('school_idx', data.school_idx)
            school_to_district = pm.Data('school_to_district', data.school_to_district)

            # State-level intercept
            mu_state = pm.Normal('mu_state',
                                mu=self.config.mu_mean,
                                sigma=self.config.mu_sigma)

            # District random effects (Level 2)
            sigma_district = pm.HalfCauchy('sigma_district',
                                          beta=self.config.sigma_district_beta)
            district_effect = pm.Normal('district_effect',
                                       mu=0,
                                       sigma=sigma_district,
                                       shape=data.n_districts)

            # School random effects (Level 1, nested in districts)
            sigma_school = pm.HalfCauchy('sigma_school',
                                        beta=self.config.sigma_school_beta)
            school_effect = pm.Normal('school_effect',
                                     mu=district_effect[school_to_district],
                                     sigma=sigma_school,
                                     shape=data.n_schools)

            # Regression coefficients with chosen prior
            beta = self._build_coefficient_prior(data.n_predictors)

            # Expected outcome
            mu = (mu_state +
                  district_effect[district_idx] +
                  school_effect[school_idx] +
                  pm.math.dot(X, beta))

            # Observation noise
            sigma_y = pm.HalfCauchy('sigma_y', beta=self.config.sigma_y_beta)

            # Likelihood
            likelihood = pm.Normal('y', mu=mu, sigma=sigma_y, observed=y_obs)

        self.model = model
        return model

    def _build_coefficient_prior(self, n_predictors: int) -> pm.Distribution:
        """Build prior distribution for regression coefficients.

        Supports three prior types:
        - NORMAL: Standard weakly informative Normal(0, sigma)
        - HORSESHOE: Classic horseshoe prior (Carvalho et al., 2010)
        - REGULARIZED_HORSESHOE: Finnish horseshoe with slab (Piironen & Vehtari, 2017)

        Args:
            n_predictors: Number of predictor variables

        Returns:
            PyMC distribution for beta coefficients
        """
        if self.config.prior_type == PriorType.NORMAL:
            # Standard weakly informative prior
            return pm.Normal('beta',
                            mu=0,
                            sigma=self.config.beta_sigma,
                            shape=n_predictors)

        elif self.config.prior_type == PriorType.HORSESHOE:
            # Classic horseshoe prior
            # tau controls global shrinkage (toward zero)
            # lambdas control local shrinkage (per coefficient)
            tau = pm.HalfCauchy('tau', beta=self.config.horseshoe_tau_scale)
            lambdas = pm.HalfCauchy('lambdas', beta=1, shape=n_predictors)

            return pm.Normal('beta',
                            mu=0,
                            sigma=tau * lambdas,
                            shape=n_predictors)

        elif self.config.prior_type == PriorType.REGULARIZED_HORSESHOE:
            # Regularized horseshoe (Finnish horseshoe) - prevents infinite variance
            # Better behaved in practice, recommended by Piironen & Vehtari (2017)
            tau = pm.HalfCauchy('tau', beta=self.config.horseshoe_tau_scale)
            lambdas = pm.HalfCauchy('lambdas', beta=1, shape=n_predictors)

            # Slab component prevents overshrinkage of large effects
            c2 = pm.InverseGamma('c2',
                                alpha=self.config.horseshoe_slab_df / 2,
                                beta=self.config.horseshoe_slab_df * self.config.horseshoe_slab_scale**2 / 2)

            # Regularized local shrinkage
            lambdas_tilde = lambdas * pm.math.sqrt(c2 / (c2 + tau**2 * lambdas**2))

            return pm.Normal('beta',
                            mu=0,
                            sigma=tau * lambdas_tilde,
                            shape=n_predictors)

        else:
            raise ValueError(f"Unknown prior type: {self.config.prior_type}")

    def fit(self, data: Optional[ModelData] = None) -> az.InferenceData:
        """Fit model using MCMC sampling.

        Args:
            data: Model data. Uses previously loaded data if not provided.

        Returns:
            ArviZ InferenceData with posterior samples
        """
        if data is not None:
            self.build_model(data)
        elif self.model is None:
            raise ValueError("No model built. Call build_model() first or provide data.")

        with self.model:
            self.trace = pm.sample(
                draws=self.config.mcmc_draws,
                tune=self.config.mcmc_tune,
                chains=self.config.mcmc_chains,
                target_accept=self.config.mcmc_target_accept,
                return_inferencedata=True,
                random_seed=self.config.random_seed
            )

            # Add posterior predictive samples
            pm.sample_posterior_predictive(self.trace, extend_inferencedata=True)

        return self.trace

    def get_coefficient_summary(self) -> pd.DataFrame:
        """Get summary of regression coefficients.

        Returns DataFrame with posterior mean, std, 95% HDI, and shrinkage info
        for each predictor.

        Returns:
            DataFrame with coefficient summaries
        """
        if self.trace is None or self.data is None:
            raise ValueError("Model not fitted. Call fit() first.")

        beta_samples = self.trace.posterior['beta'].values
        beta_means = beta_samples.mean(axis=(0, 1))
        beta_stds = beta_samples.std(axis=(0, 1))
        beta_lower = np.percentile(beta_samples, 2.5, axis=(0, 1))
        beta_upper = np.percentile(beta_samples, 97.5, axis=(0, 1))

        df = pd.DataFrame({
            'predictor': self.data.predictor_names,
            'mean': beta_means,
            'std': beta_stds,
            'hdi_2.5%': beta_lower,
            'hdi_97.5%': beta_upper,
            'zero_in_hdi': (beta_lower <= 0) & (beta_upper >= 0)
        })

        # Add shrinkage information for horseshoe priors
        if self.config.prior_type in [PriorType.HORSESHOE, PriorType.REGULARIZED_HORSESHOE]:
            if 'lambdas' in self.trace.posterior:
                lambdas = self.trace.posterior['lambdas'].values.mean(axis=(0, 1))
                df['local_shrinkage'] = 1 / (1 + lambdas**2)  # Shrinkage factor
            if 'tau' in self.trace.posterior:
                tau = float(self.trace.posterior['tau'].values.mean())
                df['global_tau'] = tau

        # Add category
        def get_category(predictor):
            for cat, preds in self.data.predictor_categories.items():
                if predictor in preds:
                    return cat
            return 'other'

        df['category'] = df['predictor'].apply(get_category)

        # Sort by absolute effect
        df['abs_mean'] = df['mean'].abs()
        df = df.sort_values('abs_mean', ascending=False)

        return df

    def get_effective_covariates(self, threshold: float = 0.1) -> List[str]:
        """Get list of covariates that survived shrinkage.

        For horseshoe priors, returns covariates with shrinkage factor below threshold.
        For normal priors, returns covariates where 95% HDI excludes zero.

        Args:
            threshold: Shrinkage threshold (for horseshoe) or p-value threshold (for normal)

        Returns:
            List of predictor names that are effectively non-zero
        """
        summary = self.get_coefficient_summary()

        if self.config.prior_type in [PriorType.HORSESHOE, PriorType.REGULARIZED_HORSESHOE]:
            # Use shrinkage factor
            if 'local_shrinkage' in summary.columns:
                effective = summary[summary['local_shrinkage'] < threshold]['predictor'].tolist()
            else:
                # Fall back to HDI
                effective = summary[~summary['zero_in_hdi']]['predictor'].tolist()
        else:
            # Use HDI for normal priors
            effective = summary[~summary['zero_in_hdi']]['predictor'].tolist()

        return effective

    def compare_priors(self, data: ModelData,
                       prior_types: Optional[List[PriorType]] = None) -> pd.DataFrame:
        """Compare model fit across different prior specifications.

        Fits model with each prior type and compares using PSIS-LOO.

        Args:
            data: Model data
            prior_types: List of prior types to compare. Defaults to all types.

        Returns:
            DataFrame with comparison results
        """
        if prior_types is None:
            prior_types = list(PriorType)

        traces = {}

        for prior_type in prior_types:
            print(f"\nFitting with {prior_type.value} prior...")

            # Create config with this prior type
            config = ModelConfig(
                prior_type=prior_type,
                # Use fewer samples for comparison
                mcmc_draws=1000,
                mcmc_tune=500,
                mcmc_chains=2
            )

            # Temporarily swap config
            old_config = self.config
            self.config = config

            try:
                self.build_model(data)
                trace = self.fit()
                traces[prior_type.value] = trace
            except Exception as e:
                print(f"  Failed: {e}")
            finally:
                self.config = old_config

        # Compare using PSIS-LOO
        if len(traces) >= 2:
            comparison = az.compare(traces)
            return comparison

        return pd.DataFrame()


def recommended_tau_scale(n_predictors: int, n_effective: int = 5) -> float:
    """Calculate recommended global shrinkage scale.

    Following Piironen & Vehtari (2017), the recommended scale is:
    tau_0 = (p_0 / (p - p_0)) * (sigma / sqrt(n))

    where p_0 is expected number of effective predictors.

    For simplicity, we use a heuristic: tau = p_0 / p

    Args:
        n_predictors: Total number of predictors
        n_effective: Expected number of effective predictors

    Returns:
        Recommended tau scale
    """
    return n_effective / n_predictors
