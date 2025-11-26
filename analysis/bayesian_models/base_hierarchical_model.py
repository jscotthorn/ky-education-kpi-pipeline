#!/usr/bin/env python3
"""
Base Class for Bayesian Hierarchical Models

Provides common infrastructure for three-level hierarchical models:
- Level 1: School-year observations
- Level 2: Schools within districts
- Level 3: Districts within state

Subclasses define outcome-specific parameters (priors, file paths, etc.)
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
import pymc as pm
import arviz as az
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')


class BaseHierarchicalModel(ABC):
    """
    Abstract base class for Bayesian hierarchical models.

    Subclasses must define:
    - OUTCOME_NAME: Name of outcome variable in dataset
    - MODEL_NAME: Short name for output directories
    - get_state_mean_prior(): Returns (mu, sigma) for state-level prior
    - get_variance_priors(): Returns dict of beta values for HalfCauchy priors
    """

    # =========================================================================
    # ABSTRACT PROPERTIES - Subclasses MUST define these
    # =========================================================================

    @property
    @abstractmethod
    def OUTCOME_NAME(self) -> str:
        """Name of outcome variable in the dataset (e.g., 'graduation_rate')."""
        pass

    @property
    @abstractmethod
    def MODEL_NAME(self) -> str:
        """Short name for output directories (e.g., 'graduation', 'reading_grade3')."""
        pass

    @abstractmethod
    def get_state_mean_prior(self) -> Tuple[float, float]:
        """
        Return (mu, sigma) for state-level mean prior.

        Examples:
            Graduation: (93, 10)  # KY avg ~93%
            Reading:    (43, 15)  # KY avg ~43%
        """
        pass

    @abstractmethod
    def get_variance_priors(self) -> Dict[str, float]:
        """
        Return beta values for HalfCauchy variance priors.

        Must include keys: 'sigma_district', 'sigma_school', 'sigma_y'

        Examples:
            Graduation: {'sigma_district': 5, 'sigma_school': 3, 'sigma_y': 2}
            Reading:    {'sigma_district': 10, 'sigma_school': 5, 'sigma_y': 5}
        """
        pass

    # =========================================================================
    # OPTIONAL OVERRIDES
    # =========================================================================

    def get_tau_scale(self) -> float:
        """
        Return tau scale for horseshoe priors.
        Default: p0/p where p0 ~ 5 expected effective predictors.
        """
        return 0.3  # Default for ~20 predictors, ~5 effective

    def get_slab_parameters(self) -> Tuple[float, float]:
        """Return (slab_scale, slab_df) for Finnish horseshoe."""
        return (2.5, 4.0)

    def get_tract_columns(self) -> List[str]:
        """
        Return list of tract-level census columns to use.
        Can be overridden to exclude problematic columns.
        """
        return [
            'tract_median_household_income',
            'tract_poverty_rate',
            'tract_pct_bachelors_plus',
            'tract_unemployment_rate',
            'tract_pct_single_parent',
            'tract_pct_owner_occupied',
        ]

    def get_model_description(self) -> str:
        """Return description for print output."""
        return f"BAYESIAN HIERARCHICAL MODEL - {self.MODEL_NAME.upper()}"

    # =========================================================================
    # INITIALIZATION
    # =========================================================================

    def __init__(self, verbose: bool = True):
        """
        Initialize the model.

        Args:
            verbose: If True, print progress messages
        """
        self.verbose = verbose

        # Set up paths
        self.BASE_DIR = Path(__file__).parent.parent.parent
        self.DATA_FILE = self.BASE_DIR / "analysis" / "datasets" / f"{self.MODEL_NAME}_analysis.csv"
        self.OUTPUT_DIR = self.BASE_DIR / "analysis" / "outputs"
        self.MODEL_DIR = self.OUTPUT_DIR / "models" / self.MODEL_NAME
        self.DIAG_DIR = self.OUTPUT_DIR / "diagnostics" / self.MODEL_NAME

        # Create output directories
        self.MODEL_DIR.mkdir(exist_ok=True, parents=True)
        self.DIAG_DIR.mkdir(exist_ok=True, parents=True)

        # Model state
        self.data = None
        self.model = None
        self.trace = None

    def log(self, message: str, header: bool = False):
        """Print message if verbose mode is on."""
        if self.verbose:
            if header:
                print("\n" + "=" * 80)
                print(message)
                print("=" * 80)
            else:
                print(message)

    # =========================================================================
    # DATA LOADING
    # =========================================================================

    def load_and_prepare_data(self) -> Dict:
        """Load and prepare data for modeling."""
        self.log("LOADING AND PREPARING DATA", header=True)

        if not self.DATA_FILE.exists():
            raise FileNotFoundError(f"Data file not found: {self.DATA_FILE}")

        df = pd.read_csv(self.DATA_FILE)
        self.log(f"Loaded {len(df):,} observations from {self.DATA_FILE.name}")

        # Create categorical indices for hierarchical structure
        df['district_cat'] = df['district'].astype('category')
        df['school_cat'] = df['school_id'].astype('category')

        district_idx = df['district_cat'].cat.codes.values
        school_idx = df['school_cat'].cat.codes.values

        n_districts = len(df['district_cat'].cat.categories)
        n_schools = len(df['school_cat'].cat.categories)

        self.log(f"Districts: {n_districts}")
        self.log(f"Schools: {n_schools}")
        self.log(f"Observations per school: {len(df) / n_schools:.1f}")

        # Define predictor categories
        demographic_cols = [
            'pct_economically_disadvantaged',
            'pct_english_learners',
            'pct_students_with_disabilities',
            'pct_african_american',
            'pct_hispanic',
        ]

        teacher_quality_cols = [
            'novice_teacher_rate',
            'teacher_avg_experience',
            'student_teacher_ratio',
            'teacher_turnover_rate',
            'emergency_provisional_rate',
        ]

        financial_cols = [
            'per_pupil_spending',
            'students_per_certified_staff',
        ]

        county_census_cols = [
            'county_median_income',
            'county_poverty_rate',
            'county_child_poverty_rate',
        ]

        tract_census_cols = self.get_tract_columns()

        # Combine all predictor categories
        all_predictor_cols = (demographic_cols + teacher_quality_cols + financial_cols +
                              county_census_cols + tract_census_cols)

        # Check which predictors are available and have data
        # Exclude columns that are entirely NaN (e.g., tract data for high schools without geocoding)
        available_predictors = [
            col for col in all_predictor_cols
            if col in df.columns and df[col].notna().any()
        ]

        # Categorize for reporting
        avail_demo = [c for c in demographic_cols if c in available_predictors]
        avail_teacher = [c for c in teacher_quality_cols if c in available_predictors]
        avail_financial = [c for c in financial_cols if c in available_predictors]
        avail_county = [c for c in county_census_cols if c in available_predictors]
        avail_tract = [c for c in tract_census_cols if c in available_predictors]

        self.log("\n" + "-" * 60)
        self.log("PREDICTORS BY CATEGORY")
        self.log("-" * 60)

        self._print_predictor_category("1. Demographics", avail_demo, demographic_cols, df)
        self._print_predictor_category("2. Teacher Quality", avail_teacher, teacher_quality_cols, df)
        self._print_predictor_category("3. Financial Resources", avail_financial, financial_cols, df)
        self._print_predictor_category("4. Census - County Level", avail_county, county_census_cols, df)
        self._print_predictor_category("5. Census - Tract Level", avail_tract, tract_census_cols, df)

        self.log(f"\nTotal predictors: {len(available_predictors)}")

        # Get predictor matrix and handle missing values
        X = df[available_predictors].copy()

        missing_counts = X.isna().sum()
        if missing_counts.sum() > 0:
            self.log(f"\nHandling missing predictor values:")
            for col in X.columns:
                n_missing = X[col].isna().sum()
                if n_missing > 0:
                    col_mean = X[col].mean()
                    X[col] = X[col].fillna(col_mean)
                    self.log(f"  {col}: Imputed {n_missing} missing with mean {col_mean:.2f}")

        X = X.values

        # Standardize predictors
        X_mean = X.mean(axis=0)
        X_std = X.std(axis=0)
        X_std = np.where(X_std == 0, 1, X_std)
        X_scaled = (X - X_mean) / X_std

        # Outcome
        y = df[self.OUTCOME_NAME].values

        self.log(f"\nOutcome ({self.OUTCOME_NAME}):")
        self.log(f"  Mean: {y.mean():.2f}")
        self.log(f"  Std: {y.std():.2f}")
        self.log(f"  Range: [{y.min():.2f}, {y.max():.2f}]")

        # Create mapping from school to district
        school_to_district = df.groupby('school_cat')['district_cat'].first().cat.codes.values

        self.data = {
            'df': df,
            'y': y,
            'X_scaled': X_scaled,
            'X_mean': X_mean,
            'X_std': X_std,
            'predictor_names': available_predictors,
            'district_idx': district_idx,
            'school_idx': school_idx,
            'school_to_district': school_to_district,
            'n_districts': n_districts,
            'n_schools': n_schools,
            'n_predictors': len(available_predictors),
            'predictor_categories': {
                'demographics': avail_demo,
                'teacher_quality': avail_teacher,
                'financial': avail_financial,
                'county_census': avail_county,
                'tract_census': avail_tract
            }
        }

        return self.data

    def _print_predictor_category(self, name: str, available: List[str],
                                   total: List[str], df: pd.DataFrame):
        """Print predictor category summary."""
        self.log(f"\n{name} ({len(available)}/{len(total)}):")
        for col in available:
            vals = df[col].dropna()
            if len(vals) > 0:
                self.log(f"   {col}: mean={vals.mean():.2f}, std={vals.std():.2f}")

    # =========================================================================
    # MODEL BUILDING
    # =========================================================================

    def build_model(self, prior_type: str = "finnish", non_centered: bool = True) -> pm.Model:
        """
        Build the hierarchical Bayesian model.

        Args:
            prior_type: "normal", "horseshoe", or "finnish"
            non_centered: If True, use non-centered parameterization

        Returns:
            PyMC model object
        """
        if self.data is None:
            raise ValueError("Must call load_and_prepare_data() first")

        self.log("BUILDING HIERARCHICAL MODEL", header=True)
        self.log(f"\nPrior type: {prior_type.upper()}")
        if non_centered:
            self.log("  Using NON-CENTERED parameterization")

        mu_state_mu, mu_state_sigma = self.get_state_mean_prior()
        var_priors = self.get_variance_priors()

        with pm.Model() as model:
            # Data
            y_obs = pm.Data('y_obs', self.data['y'])
            X = pm.Data('X', self.data['X_scaled'])
            district_idx = pm.Data('district_idx', self.data['district_idx'])
            school_idx = pm.Data('school_idx', self.data['school_idx'])
            school_to_district = pm.Data('school_to_district', self.data['school_to_district'])

            # State-level hyperprior
            mu_state = pm.Normal('mu_state', mu=mu_state_mu, sigma=mu_state_sigma)

            # Variance parameters
            sigma_district = pm.HalfCauchy('sigma_district', beta=var_priors['sigma_district'])
            sigma_school = pm.HalfCauchy('sigma_school', beta=var_priors['sigma_school'])

            if non_centered:
                # Non-centered parameterization
                district_effect_raw = pm.Normal('district_effect_raw', mu=0, sigma=1,
                                               shape=self.data['n_districts'])
                district_effect = pm.Deterministic('district_effect',
                                                   sigma_district * district_effect_raw)

                school_effect_raw = pm.Normal('school_effect_raw', mu=0, sigma=1,
                                             shape=self.data['n_schools'])
                school_effect = pm.Deterministic('school_effect',
                                                 district_effect[school_to_district] +
                                                 sigma_school * school_effect_raw)
            else:
                # Centered parameterization
                district_effect = pm.Normal('district_effect',
                                            mu=0,
                                            sigma=sigma_district,
                                            shape=self.data['n_districts'])

                school_effect = pm.Normal('school_effect',
                                          mu=district_effect[school_to_district],
                                          sigma=sigma_school,
                                          shape=self.data['n_schools'])

            # Regression coefficients
            beta = self._build_coefficient_priors(prior_type)

            # Expected outcome
            mu = mu_state + district_effect[district_idx] + school_effect[school_idx] + pm.math.dot(X, beta)

            # Likelihood
            sigma_y = pm.HalfCauchy('sigma_y', beta=var_priors['sigma_y'])
            likelihood = pm.Normal('y', mu=mu, sigma=sigma_y, observed=y_obs)

        self.log("\nModel structure:")
        self.log(f"  State mean: mu_state ~ Normal({mu_state_mu}, {mu_state_sigma})")
        self.log(f"  District effects: {self.data['n_districts']} (σ ~ HalfCauchy({var_priors['sigma_district']}))")
        self.log(f"  School effects: {self.data['n_schools']} (σ ~ HalfCauchy({var_priors['sigma_school']}))")
        self.log(f"  Predictors: {self.data['n_predictors']} with {prior_type.upper()} prior")
        self.log(f"  Observation noise: σ_y ~ HalfCauchy({var_priors['sigma_y']})")

        self.model = model
        return model

    def _build_coefficient_priors(self, prior_type: str):
        """Build coefficient priors based on prior type.

        Uses non-centered parameterization for horseshoe variants to avoid
        funnel geometry that causes divergences.
        """
        tau_scale = self.get_tau_scale()
        n_predictors = self.data['n_predictors']

        if prior_type == "horseshoe":
            # Non-centered parameterization to avoid funnel geometry
            tau = pm.HalfCauchy('tau', beta=tau_scale)
            lambdas = pm.HalfCauchy('lambdas', beta=1, shape=n_predictors)
            z = pm.Normal('z', mu=0, sigma=1, shape=n_predictors)
            beta = pm.Deterministic('beta', tau * lambdas * z)
            self.log(f"  Horseshoe (non-centered): tau ~ HalfCauchy({tau_scale})")

        elif prior_type == "finnish":
            # Non-centered Finnish horseshoe
            slab_scale, slab_df = self.get_slab_parameters()
            tau = pm.HalfCauchy('tau', beta=tau_scale)
            lambdas = pm.HalfCauchy('lambdas', beta=1, shape=n_predictors)
            c2 = pm.InverseGamma('c2', alpha=slab_df / 2, beta=slab_df * slab_scale**2 / 2)
            lambdas_tilde = lambdas * pm.math.sqrt(c2 / (c2 + tau**2 * lambdas**2))
            z = pm.Normal('z', mu=0, sigma=1, shape=n_predictors)
            beta = pm.Deterministic('beta', tau * lambdas_tilde * z)
            self.log(f"  Finnish horseshoe (non-centered): tau ~ HalfCauchy({tau_scale})")
            self.log(f"  Slab: c2 ~ InverseGamma({slab_df/2}, {slab_df * slab_scale**2 / 2})")

        else:
            beta = pm.Normal('beta', mu=0, sigma=5, shape=n_predictors)
            self.log(f"  Normal prior: beta ~ Normal(0, 5)")

        return beta

    # =========================================================================
    # SAMPLING
    # =========================================================================

    def sample_posterior(self, draws: int = 4000, tune: int = 2000,
                        chains: int = 4, target_accept: float = 0.95) -> az.InferenceData:
        """Run MCMC sampling."""
        if self.model is None:
            raise ValueError("Must call build_model() first")

        self.log("SAMPLING FROM POSTERIOR", header=True)
        self.log(f"\nMCMC configuration:")
        self.log(f"  Chains: {chains}")
        self.log(f"  Draws per chain: {draws}")
        self.log(f"  Tuning steps: {tune}")
        self.log(f"  Target accept: {target_accept}")
        self.log("\nStarting MCMC sampling...")
        self.log("(This may take 15-30 minutes)")

        with self.model:
            trace = pm.sample(
                draws=draws,
                tune=tune,
                chains=chains,
                target_accept=target_accept,
                return_inferencedata=True,
                random_seed=42
            )

            self.log("\nGenerating posterior predictive samples...")
            pm.sample_posterior_predictive(trace, extend_inferencedata=True)

        self.log("\nSampling complete!")
        self.trace = trace
        return trace

    # =========================================================================
    # DIAGNOSTICS
    # =========================================================================

    def check_convergence(self) -> bool:
        """Check MCMC convergence diagnostics."""
        if self.trace is None:
            raise ValueError("Must call sample_posterior() first")

        self.log("CONVERGENCE DIAGNOSTICS", header=True)

        key_vars = ['mu_state', 'sigma_district', 'sigma_school', 'sigma_y']
        issues = []

        # 1. Divergence check
        self.log("\n" + "-" * 60)
        self.log("1. DIVERGENCE CHECK")
        self.log("-" * 60)

        if 'sample_stats' in self.trace and 'diverging' in self.trace.sample_stats:
            divergences = int(self.trace.sample_stats['diverging'].sum().values)
            total_samples = int(self.trace.sample_stats['diverging'].size)
            div_pct = (divergences / total_samples) * 100

            if divergences == 0:
                self.log(f"  No divergences detected (0/{total_samples})")
            else:
                self.log(f"  {divergences} divergent transitions ({div_pct:.2f}%)")
                issues.append(f"divergences: {divergences}")

        # 2. R-hat check
        self.log("\n" + "-" * 60)
        self.log("2. R-HAT VALUES (should be < 1.01)")
        self.log("-" * 60)

        rhat = az.rhat(self.trace)
        for var in key_vars:
            if var in rhat:
                val = float(rhat[var].values)
                status = "PASS" if val < 1.01 else "FAIL"
                self.log(f"  {status} {var}: {val:.4f}")
                if val >= 1.01:
                    issues.append(f"R-hat({var}): {val:.4f}")

        if 'beta' in rhat:
            beta_rhat = rhat['beta'].values
            max_beta_rhat = beta_rhat.max()
            status = "PASS" if max_beta_rhat < 1.01 else "FAIL"
            self.log(f"  {status} beta (max): {max_beta_rhat:.4f}")

        # 3. ESS check
        self.log("\n" + "-" * 60)
        self.log("3. EFFECTIVE SAMPLE SIZE (should be > 400)")
        self.log("-" * 60)

        ess_bulk = az.ess(self.trace, method='bulk')
        for var in key_vars:
            if var in ess_bulk:
                val = float(ess_bulk[var].values)
                status = "PASS" if val > 400 else "FAIL"
                self.log(f"  {status} {var}: {val:.0f}")
                if val <= 400:
                    issues.append(f"ESS({var}): {val:.0f}")

        # 4. Tail ESS check
        self.log("\n" + "-" * 60)
        self.log("4. TAIL EFFECTIVE SAMPLE SIZE (should be > 400)")
        self.log("-" * 60)

        ess_tail = az.ess(self.trace, method='tail')
        for var in key_vars:
            if var in ess_tail:
                val = float(ess_tail[var].values)
                status = "PASS" if val > 400 else "FAIL"
                self.log(f"  {status} {var}: {val:.0f}")
                if val <= 400:
                    issues.append(f"ESS_tail({var}): {val:.0f}")

        # Overall assessment
        self.log("\n" + "=" * 80)
        if len(issues) == 0:
            self.log("CONVERGENCE GOOD - All diagnostics passed")
            return True
        else:
            self.log("CONVERGENCE WARNING - Issues detected:")
            for issue in issues:
                self.log(f"    - {issue}")
            return False

    def prior_predictive_check(self, n_samples: int = 500) -> bool:
        """Perform prior predictive check."""
        if self.model is None:
            raise ValueError("Must call build_model() first")

        self.log("PRIOR PREDICTIVE CHECK", header=True)
        self.log(f"\nDrawing {n_samples} samples from prior...")

        with self.model:
            prior = pm.sample_prior_predictive(samples=n_samples, random_seed=42)

        prior_y = prior.prior_predictive['y'].values.flatten()

        self.log(f"\nPrior Predictive Distribution:")
        self.log(f"  Mean: {prior_y.mean():.1f}")
        self.log(f"  SD: {prior_y.std():.1f}")
        self.log(f"  95% Interval: [{np.percentile(prior_y, 2.5):.1f}, {np.percentile(prior_y, 97.5):.1f}]")

        pct_in_range = ((prior_y >= 0) & (prior_y <= 100)).mean() * 100
        self.log(f"  % in valid range [0, 100]: {pct_in_range:.1f}%")

        # Generate visualization
        fig, axes = plt.subplots(1, 2, figsize=(12, 5))

        ax1 = axes[0]
        ax1.hist(prior_y, bins=50, alpha=0.7, color='blue', density=True, edgecolor='black')
        ax1.axvline(0, color='red', linestyle='--', alpha=0.7)
        ax1.axvline(100, color='red', linestyle='--', alpha=0.7)
        mu_state, _ = self.get_state_mean_prior()
        ax1.axvline(mu_state, color='green', linestyle='-', linewidth=2, label=f'Prior mean ({mu_state}%)')
        ax1.set_xlabel(f'{self.OUTCOME_NAME} (%)')
        ax1.set_ylabel('Density')
        ax1.set_title('Prior Predictive Distribution')
        ax1.legend()

        ax2 = axes[1]
        ax2.hist(prior_y, bins=50, alpha=0.5, color='blue', density=True, label='Prior Predictive')
        ax2.hist(self.data['y'], bins=30, alpha=0.7, color='red', density=True, label='Observed')
        ax2.set_xlabel(f'{self.OUTCOME_NAME} (%)')
        ax2.set_ylabel('Density')
        ax2.set_title('Prior Predictive vs Observed')
        ax2.legend()

        plt.tight_layout()
        ppc_file = self.DIAG_DIR / 'prior_predictive_check.png'
        plt.savefig(ppc_file, dpi=300)
        plt.close()

        self.log(f"\n  Saved: {ppc_file}")

        is_reasonable = pct_in_range >= 50
        if is_reasonable:
            self.log("\nPRIOR PREDICTIVE CHECK PASSED")
        else:
            self.log("\nPRIOR PREDICTIVE CHECK WARNING - priors may be too diffuse")

        return is_reasonable

    def posterior_predictive_checks(self) -> bool:
        """Perform posterior predictive checks."""
        if self.trace is None:
            raise ValueError("Must call sample_posterior() first")

        self.log("POSTERIOR PREDICTIVE CHECKS", header=True)

        y_obs = self.data['y']

        if 'posterior_predictive' not in self.trace or 'y' not in self.trace.posterior_predictive:
            self.log("  No posterior predictive samples found")
            return False

        y_rep = self.trace.posterior_predictive['y'].values
        n_samples = y_rep.shape[0] * y_rep.shape[1]
        y_rep_flat = y_rep.reshape(n_samples, -1)

        # Test statistics
        rep_means = y_rep_flat.mean(axis=1)
        obs_mean = y_obs.mean()
        pval_mean = (rep_means >= obs_mean).mean()

        rep_sds = y_rep_flat.std(axis=1)
        obs_sd = y_obs.std()
        pval_sd = (rep_sds >= obs_sd).mean()

        self.log(f"\nMean: observed={obs_mean:.2f}, p-value={pval_mean:.3f}")
        self.log(f"SD: observed={obs_sd:.2f}, p-value={pval_sd:.3f}")

        # Generate visualization
        fig, axes = plt.subplots(1, 3, figsize=(15, 5))

        ax1 = axes[0]
        for i in range(min(100, n_samples)):
            ax1.hist(y_rep_flat[i], bins=30, alpha=0.02, color='blue', density=True)
        ax1.hist(y_obs, bins=30, alpha=0.8, color='red', density=True, label='Observed')
        ax1.set_xlabel(f'{self.OUTCOME_NAME} (%)')
        ax1.set_ylabel('Density')
        ax1.set_title('Posterior Predictive Check: Distribution')
        ax1.legend()

        ax2 = axes[1]
        ax2.hist(rep_means, bins=50, alpha=0.7, color='blue', density=True)
        ax2.axvline(obs_mean, color='red', linewidth=2, label=f'Observed: {obs_mean:.2f}')
        ax2.set_xlabel(f'Mean {self.OUTCOME_NAME}')
        ax2.set_title(f'PPC: Mean (p={pval_mean:.3f})')
        ax2.legend()

        ax3 = axes[2]
        ax3.hist(rep_sds, bins=50, alpha=0.7, color='blue', density=True)
        ax3.axvline(obs_sd, color='red', linewidth=2, label=f'Observed: {obs_sd:.2f}')
        ax3.set_xlabel(f'SD of {self.OUTCOME_NAME}')
        ax3.set_title(f'PPC: Standard Deviation (p={pval_sd:.3f})')
        ax3.legend()

        plt.tight_layout()
        ppc_file = self.DIAG_DIR / 'posterior_predictive_check.png'
        plt.savefig(ppc_file, dpi=300)
        plt.close()

        self.log(f"\n  Saved: {ppc_file}")

        all_good = (0.05 < pval_mean < 0.95) and (0.05 < pval_sd < 0.95)
        if all_good:
            self.log("\nPOSTERIOR PREDICTIVE CHECKS PASSED")
        else:
            self.log("\nPOSTERIOR PREDICTIVE CHECKS - potential misfit")

        return all_good

    def loo_cross_validation(self) -> Tuple[Optional[az.ELPDData], bool]:
        """Perform LOO cross-validation."""
        if self.trace is None:
            raise ValueError("Must call sample_posterior() first")

        self.log("LEAVE-ONE-OUT CROSS-VALIDATION", header=True)
        self.log("\nComputing PSIS-LOO...")

        try:
            loo = az.loo(self.trace, pointwise=True)

            self.log(f"\nLOO-CV Results:")
            self.log(f"  elpd_loo: {loo.elpd_loo:.2f} (SE: {loo.se:.2f})")
            self.log(f"  p_loo: {loo.p_loo:.2f}")

            pareto_k = loo.pareto_k.values
            n_bad = (pareto_k > 0.7).sum()
            pct_bad = 100 * n_bad / len(pareto_k)

            self.log(f"\nPareto k > 0.7: {n_bad} ({pct_bad:.1f}%)")

            # Generate plot
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.scatter(range(len(pareto_k)), pareto_k, alpha=0.5, s=10)
            ax.axhline(0.5, color='green', linestyle='--', label='k=0.5')
            ax.axhline(0.7, color='orange', linestyle='--', label='k=0.7')
            ax.axhline(1.0, color='red', linestyle='--', label='k=1.0')
            ax.set_xlabel('Observation Index')
            ax.set_ylabel('Pareto k')
            ax.set_title('LOO-CV Pareto k Diagnostics')
            ax.legend()

            plt.tight_layout()
            loo_file = self.DIAG_DIR / 'loo_cv_diagnostics.png'
            plt.savefig(loo_file, dpi=300)
            plt.close()

            self.log(f"\n  Saved: {loo_file}")

            is_reliable = pct_bad < 5
            if is_reliable:
                self.log("\nLOO-CV ESTIMATES ARE RELIABLE")
            else:
                self.log("\nLOO-CV ESTIMATES MAY BE UNRELIABLE")

            return loo, is_reliable

        except Exception as e:
            self.log(f"\nLOO-CV failed: {e}")
            return None, False

    # =========================================================================
    # RESULTS SAVING
    # =========================================================================

    def save_results(self) -> pd.DataFrame:
        """Save model results."""
        if self.trace is None:
            raise ValueError("Must call sample_posterior() first")

        self.log("SAVING RESULTS", header=True)

        # Save trace
        trace_file = self.MODEL_DIR / f"{self.MODEL_NAME}_trace.nc"
        self.log(f"\nSaving trace to: {trace_file}")
        self.trace.to_netcdf(str(trace_file))

        # Save model summary
        summary = az.summary(self.trace, var_names=['mu_state', 'sigma_district', 'sigma_school',
                                                     'sigma_y', 'beta'])
        summary_file = self.MODEL_DIR / "model_summary.csv"
        self.log(f"Saving summary to: {summary_file}")
        summary.to_csv(summary_file)

        # Save covariate effects
        beta_samples = self.trace.posterior['beta'].values
        beta_means = beta_samples.mean(axis=(0, 1))
        beta_stds = beta_samples.std(axis=(0, 1))
        beta_lower_2_5 = np.percentile(beta_samples, 2.5, axis=(0, 1))
        beta_upper_97_5 = np.percentile(beta_samples, 97.5, axis=(0, 1))
        beta_lower_10 = np.percentile(beta_samples, 10, axis=(0, 1))
        beta_upper_90 = np.percentile(beta_samples, 90, axis=(0, 1))

        covariate_df = pd.DataFrame({
            'predictor': self.data['predictor_names'],
            'effect_mean': beta_means,
            'effect_std': beta_stds,
            'ci_lower_2.5': beta_lower_2_5,
            'ci_upper_97.5': beta_upper_97_5,
            'ci_lower_10': beta_lower_10,
            'ci_upper_90': beta_upper_90
        })

        # Add category labels
        categories = self.data.get('predictor_categories', {})
        def get_category(predictor):
            for cat_name, predictors in categories.items():
                if predictor in predictors:
                    return cat_name
            return 'other'
        covariate_df['category'] = covariate_df['predictor'].apply(get_category)

        # Check for horseshoe priors
        has_horseshoe = 'tau' in self.trace.posterior and 'lambdas' in self.trace.posterior
        if has_horseshoe:
            tau_samples = self.trace.posterior['tau'].values
            lambdas_samples = self.trace.posterior['lambdas'].values
            tau_mean = tau_samples.mean()
            lambdas_mean = lambdas_samples.mean(axis=(0, 1))
            shrinkage_factor = 1 / (1 + lambdas_mean**2 * tau_mean**2)
            covariate_df['shrinkage_factor'] = shrinkage_factor
            covariate_df['effective'] = shrinkage_factor < 0.5
            self.log(f"\n  Global shrinkage (tau): {tau_mean:.4f}")
            self.log(f"  Effective covariates: {covariate_df['effective'].sum()} / {len(covariate_df)}")

        covariate_df['abs_effect'] = covariate_df['effect_mean'].abs()
        covariate_df = covariate_df.sort_values('abs_effect', ascending=False)

        covariate_file = self.MODEL_DIR / "covariate_effects.csv"
        self.log(f"Saving covariate effects to: {covariate_file}")
        covariate_df.to_csv(covariate_file, index=False)

        # Print covariate summary
        self.log("\n" + "-" * 60)
        self.log("COVARIATE EFFECTS (sorted by magnitude)")
        self.log("-" * 60)

        for _, row in covariate_df.iterrows():
            sig = "***" if row['ci_lower_2.5'] * row['ci_upper_97.5'] > 0 else ""
            if has_horseshoe:
                shrink = row['shrinkage_factor']
                eff = "+" if row['effective'] else " "
                self.log(f"  {eff} {row['predictor']:35s}: {row['effect_mean']:+.3f} "
                        f"({row['ci_lower_2.5']:+.3f}, {row['ci_upper_97.5']:+.3f}) shrink={shrink:.2f} {sig}")
            else:
                self.log(f"  {row['predictor']:37s}: {row['effect_mean']:+.3f} "
                        f"({row['ci_lower_2.5']:+.3f}, {row['ci_upper_97.5']:+.3f}) {sig}")

        # Save school effects
        school_effects = self.trace.posterior['school_effect'].values
        school_effects_mean = school_effects.mean(axis=(0, 1))
        school_effects_std = school_effects.std(axis=(0, 1))
        school_effects_lower_2_5 = np.percentile(school_effects, 2.5, axis=(0, 1))
        school_effects_upper_97_5 = np.percentile(school_effects, 97.5, axis=(0, 1))
        school_effects_lower_10 = np.percentile(school_effects, 10, axis=(0, 1))
        school_effects_upper_90 = np.percentile(school_effects, 90, axis=(0, 1))

        school_ids = self.data['df'].groupby('school_cat')['school_id'].first().values
        school_names = self.data['df'].groupby('school_cat')['school_name'].first().values
        district_names = self.data['df'].groupby('school_cat')['district'].first().values
        is_fayette = self.data['df'].groupby('school_cat')['is_fayette'].first().values

        school_effects_df = pd.DataFrame({
            'school_id': school_ids,
            'school_name': school_names,
            'district': district_names,
            'is_fayette': is_fayette,
            'effect_mean': school_effects_mean,
            'effect_std': school_effects_std,
            'ci_lower_2.5': school_effects_lower_2_5,
            'ci_upper_97.5': school_effects_upper_97_5,
            'ci_lower_10': school_effects_lower_10,
            'ci_upper_90': school_effects_upper_90
        })

        effects_file = self.MODEL_DIR / "school_effects.csv"
        self.log(f"\nSaving school effects to: {effects_file}")
        school_effects_df.to_csv(effects_file, index=False)

        self.log("\nAll results saved")
        return school_effects_df

    # =========================================================================
    # MAIN EXECUTION
    # =========================================================================

    def run(self, prior_type: str = "finnish", non_centered: bool = True,
            run_prior_check: bool = True, run_loo: bool = True) -> pd.DataFrame:
        """
        Run the complete modeling pipeline.

        Args:
            prior_type: "normal", "horseshoe", or "finnish"
            non_centered: If True, use non-centered parameterization
            run_prior_check: If True, run prior predictive check
            run_loo: If True, run LOO cross-validation

        Returns:
            DataFrame with school effects
        """
        self.log(self.get_model_description(), header=True)
        if prior_type != "normal":
            self.log(f"WITH {prior_type.upper()} PRIORS")
        if non_centered:
            self.log("WITH NON-CENTERED PARAMETERIZATION")

        # Load data
        self.load_and_prepare_data()

        # Build model
        self.build_model(prior_type=prior_type, non_centered=non_centered)

        # Prior predictive check
        if run_prior_check:
            self.prior_predictive_check()

        # Sample posterior
        self.sample_posterior()

        # Check convergence
        converged = self.check_convergence()

        # Posterior predictive checks
        ppc_passed = self.posterior_predictive_checks()

        # LOO-CV
        if run_loo:
            loo_result, loo_reliable = self.loo_cross_validation()
        else:
            loo_reliable = None

        # Save results
        school_effects_df = self.save_results()

        # Show Fayette County results
        self.log("FAYETTE COUNTY SCHOOLS", header=True)
        fayette_df = school_effects_df[school_effects_df['is_fayette'] == 1].copy()
        fayette_df = fayette_df.sort_values('effect_mean', ascending=False)

        self.log(f"\nTop 10 Fayette schools (positive = above expected):")
        self.log(fayette_df.head(10).to_string(index=False))

        # Final summary
        self.log("MODEL FITTING COMPLETE", header=True)
        self.log(f"\nDiagnostics Summary:")
        self.log(f"  Convergence: {'PASSED' if converged else 'ISSUES'}")
        self.log(f"  Posterior Predictive: {'PASSED' if ppc_passed else 'REVIEW'}")
        if run_loo and loo_reliable is not None:
            self.log(f"  LOO-CV: {'RELIABLE' if loo_reliable else 'CHECK PARETO K'}")

        self.log(f"\nOutput files: {self.MODEL_DIR}")

        return school_effects_df
