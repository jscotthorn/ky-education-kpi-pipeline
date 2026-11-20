#!/usr/bin/env python3
"""
MLflow Experiment Tracking for Bayesian Hierarchical Models

This module provides MLflow integration for tracking model runs, comparing
different prior specifications, and enabling sensitivity analysis.

Usage:
    from mlflow_tracking import MLflowTracker

    tracker = MLflowTracker(experiment_name="graduation_rate")
    with tracker.start_run(run_name="baseline_priors"):
        # ... run model ...
        tracker.log_model_config(config)
        tracker.log_metrics(metrics)
        tracker.log_artifacts(output_dir)

Features:
    - Automatic experiment organization by indicator
    - Configuration versioning and comparison
    - Metric tracking (LOO-CV, convergence diagnostics)
    - Artifact storage (traces, plots, summaries)
    - Run comparison utilities

Author: FCPS Equity Council Analysis Team
Date: November 2025
"""

import json
import os
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import logging

import mlflow
from mlflow.tracking import MlflowClient

# Configure logging
logger = logging.getLogger(__name__)


@dataclass
class ModelRunConfig:
    """Configuration for a model run, logged to MLflow."""
    # Indicator identification
    indicator_id: str
    indicator_name: str
    student_group: str
    student_group_name: str

    # Prior configuration
    prior_type: str  # "normal", "horseshoe", "finnish"
    mu_state_location: float
    mu_state_scale: float
    sigma_district_scale: float
    sigma_school_scale: float
    sigma_y_scale: float

    # Horseshoe-specific (optional)
    tau_scale: Optional[float] = None
    slab_scale: Optional[float] = None
    slab_df: Optional[float] = None

    # Model structure
    likelihood_type: str = "normal"
    non_centered: bool = True
    county_varying_slopes: bool = True

    # MCMC settings
    mcmc_draws: int = 4000
    mcmc_tune: int = 2000
    mcmc_chains: int = 4
    mcmc_target_accept: float = 0.95
    random_seed: int = 42

    # Data info
    n_observations: int = 0
    n_schools: int = 0
    n_districts: int = 0
    n_counties: int = 0
    n_predictors: int = 0

    # Metadata
    run_timestamp: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for MLflow logging."""
        return asdict(self)

    def to_flat_params(self) -> Dict[str, Any]:
        """Flatten to params dict for MLflow (no nested dicts)."""
        d = self.to_dict()
        # MLflow params must be strings, numbers, or bools
        return {k: v for k, v in d.items() if v is not None}


@dataclass
class ModelMetrics:
    """Metrics from a model run, logged to MLflow."""
    # Convergence diagnostics
    n_divergences: int = 0
    divergence_pct: float = 0.0
    max_rhat: float = 0.0
    min_ess_bulk: float = 0.0
    min_ess_tail: float = 0.0
    convergence_passed: bool = False

    # LOO-CV metrics
    loo_elpd: Optional[float] = None
    loo_se: Optional[float] = None
    loo_p_loo: Optional[float] = None
    loo_n_bad_k: Optional[int] = None
    loo_pct_bad_k: Optional[float] = None
    loo_reliable: Optional[bool] = None

    # Posterior predictive checks
    ppc_mean_pval: Optional[float] = None
    ppc_sd_pval: Optional[float] = None
    ppc_passed: Optional[bool] = None

    # Prior predictive check
    prior_pct_in_range: Optional[float] = None
    prior_check_passed: Optional[bool] = None

    # Model fit summary
    state_mean_posterior: Optional[float] = None
    state_mean_ci_lower: Optional[float] = None
    state_mean_ci_upper: Optional[float] = None
    sigma_district_posterior: Optional[float] = None
    sigma_school_posterior: Optional[float] = None
    sigma_y_posterior: Optional[float] = None

    # Covariate summary
    n_significant_covariates: int = 0
    n_effective_covariates: int = 0  # For horseshoe

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for MLflow logging."""
        d = asdict(self)
        # Filter out None values for cleaner logging
        return {k: v for k, v in d.items() if v is not None}


class MLflowTracker:
    """
    MLflow experiment tracker for Bayesian hierarchical models.

    Organizes experiments by indicator (e.g., "graduation_rate", "chronic_absenteeism")
    and tracks runs with different configurations for comparison.
    """

    def __init__(
        self,
        experiment_name: str,
        tracking_uri: Optional[str] = None,
        artifact_location: Optional[str] = None
    ):
        """
        Initialize the tracker.

        Args:
            experiment_name: Name of the MLflow experiment (typically indicator name)
            tracking_uri: MLflow tracking URI (default: local ./mlruns)
            artifact_location: Where to store artifacts (default: ./mlruns artifacts)
        """
        self.experiment_name = experiment_name

        # Set up tracking URI
        if tracking_uri:
            mlflow.set_tracking_uri(tracking_uri)
        else:
            # Default to SQLite database in the analysis directory
            # SQLite is recommended over filesystem for better querying
            base_dir = Path(__file__).parent.parent
            db_path = base_dir / "mlruns.db"
            mlflow.set_tracking_uri(f"sqlite:///{db_path}")

        # Set or create experiment
        experiment = mlflow.get_experiment_by_name(experiment_name)
        if experiment is None:
            self.experiment_id = mlflow.create_experiment(
                experiment_name,
                artifact_location=artifact_location
            )
            logger.info(f"Created new experiment: {experiment_name}")
        else:
            self.experiment_id = experiment.experiment_id
            logger.info(f"Using existing experiment: {experiment_name}")

        mlflow.set_experiment(experiment_name)

        self.client = MlflowClient()
        self.active_run = None
        self._config: Optional[ModelRunConfig] = None
        self._metrics: Optional[ModelMetrics] = None

    def start_run(
        self,
        run_name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        description: Optional[str] = None
    ):
        """
        Start a new MLflow run.

        Args:
            run_name: Name for this run (e.g., "baseline_all_students", "tighter_priors")
            tags: Additional tags to add to the run
            description: Run description

        Returns:
            Self for use as context manager
        """
        # Generate default run name if not provided
        if run_name is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            run_name = f"run_{timestamp}"

        # Prepare tags
        run_tags = tags or {}
        if description:
            run_tags["mlflow.note.content"] = description

        self.active_run = mlflow.start_run(
            run_name=run_name,
            tags=run_tags
        )

        logger.info(f"Started MLflow run: {run_name} (ID: {self.active_run.info.run_id})")
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_run()
        return False

    def end_run(self, status: str = "FINISHED"):
        """End the current run."""
        if self.active_run:
            mlflow.end_run(status=status)
            logger.info(f"Ended MLflow run with status: {status}")
            self.active_run = None

    def log_config(self, config: ModelRunConfig):
        """Log model configuration as parameters."""
        self._config = config
        params = config.to_flat_params()

        # MLflow has a limit on param value length, truncate if needed
        for key, value in params.items():
            if isinstance(value, str) and len(value) > 250:
                params[key] = value[:247] + "..."

        mlflow.log_params(params)
        logger.info(f"Logged {len(params)} configuration parameters")

    def log_metrics(self, metrics: ModelMetrics, step: Optional[int] = None):
        """Log model metrics."""
        self._metrics = metrics
        metrics_dict = metrics.to_dict()

        # Log each metric
        for key, value in metrics_dict.items():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                mlflow.log_metric(key, value, step=step)
            elif isinstance(value, bool):
                mlflow.log_metric(key, int(value), step=step)

        logger.info(f"Logged {len(metrics_dict)} metrics")

    def log_metric(self, key: str, value: float, step: Optional[int] = None):
        """Log a single metric."""
        mlflow.log_metric(key, value, step=step)

    def log_param(self, key: str, value: Any):
        """Log a single parameter."""
        mlflow.log_param(key, value)

    def log_artifacts(self, local_dir: Union[str, Path], artifact_path: Optional[str] = None):
        """
        Log all files in a directory as artifacts.

        Args:
            local_dir: Local directory containing artifacts
            artifact_path: Subdirectory in artifact store (optional)
        """
        local_dir = Path(local_dir)
        if local_dir.exists():
            mlflow.log_artifacts(str(local_dir), artifact_path)
            n_files = len(list(local_dir.glob("*")))
            logger.info(f"Logged {n_files} artifacts from {local_dir}")
        else:
            logger.warning(f"Artifact directory not found: {local_dir}")

    def log_artifact(self, local_path: Union[str, Path], artifact_path: Optional[str] = None):
        """Log a single file as an artifact."""
        local_path = Path(local_path)
        if local_path.exists():
            mlflow.log_artifact(str(local_path), artifact_path)
            logger.info(f"Logged artifact: {local_path.name}")
        else:
            logger.warning(f"Artifact not found: {local_path}")

    def log_dict(self, dictionary: Dict[str, Any], artifact_file: str):
        """Log a dictionary as a JSON artifact."""
        mlflow.log_dict(dictionary, artifact_file)

    def set_tag(self, key: str, value: str):
        """Set a tag on the current run."""
        mlflow.set_tag(key, value)

    def set_tags(self, tags: Dict[str, str]):
        """Set multiple tags on the current run."""
        mlflow.set_tags(tags)

    @property
    def run_id(self) -> Optional[str]:
        """Get the current run ID."""
        return self.active_run.info.run_id if self.active_run else None

    @property
    def artifact_uri(self) -> Optional[str]:
        """Get the artifact URI for the current run."""
        return self.active_run.info.artifact_uri if self.active_run else None

    # =========================================================================
    # PUBLISHING UTILITIES
    # =========================================================================

    def publish_run(self, run_id: str, student_group: str) -> bool:
        """
        Mark a run as published for its indicator+student_group combination.

        Only one run can be published per indicator+student_group at a time.
        Publishing a new run automatically unpublishes the previous one.

        Args:
            run_id: The run ID to publish
            student_group: The student group (used to identify the combination)

        Returns:
            True if successful
        """
        # First, unpublish any existing published run for this student_group
        existing = self.get_published_run(student_group)
        if existing:
            self.unpublish_run(existing.info.run_id)
            logger.info(f"Unpublished previous run: {existing.info.run_id}")

        # Tag the new run as published
        self.client.set_tag(run_id, "published", "true")
        self.client.set_tag(run_id, "published_at", datetime.now().isoformat())
        self.client.set_tag(run_id, "published_student_group", student_group)

        logger.info(f"Published run {run_id} for {self.experiment_name}/{student_group}")
        return True

    def unpublish_run(self, run_id: str) -> bool:
        """
        Remove the published tag from a run.

        Args:
            run_id: The run ID to unpublish

        Returns:
            True if successful
        """
        self.client.set_tag(run_id, "published", "false")
        self.client.delete_tag(run_id, "published_at")
        logger.info(f"Unpublished run {run_id}")
        return True

    def get_published_run(self, student_group: str) -> Optional[mlflow.entities.Run]:
        """
        Get the currently published run for a student_group.

        Args:
            student_group: The student group slug

        Returns:
            The published Run object, or None if no run is published
        """
        filter_str = (
            f"tags.published = 'true' AND "
            f"tags.published_student_group = '{student_group}'"
        )
        runs = self.get_all_runs(filter_string=filter_str)
        return runs[0] if runs else None

    def get_all_published_runs(self) -> List[mlflow.entities.Run]:
        """
        Get all published runs in this experiment.

        Returns:
            List of published Run objects
        """
        return self.get_all_runs(filter_string="tags.published = 'true'")

    def list_published(self) -> Dict[str, Dict[str, Any]]:
        """
        List all published runs with their metadata.

        Returns:
            Dict mapping student_group -> run info
        """
        published = {}
        for run in self.get_all_published_runs():
            student_group = run.data.tags.get("published_student_group", "unknown")
            published[student_group] = {
                "run_id": run.info.run_id,
                "run_name": run.info.run_name,
                "published_at": run.data.tags.get("published_at"),
                "prior_type": run.data.params.get("prior_type"),
                "loo_elpd": run.data.metrics.get("loo_elpd"),
                "convergence_passed": run.data.metrics.get("convergence_passed"),
            }
        return published

    # =========================================================================
    # COMPARISON UTILITIES
    # =========================================================================

    def get_all_runs(
        self,
        filter_string: Optional[str] = None,
        order_by: Optional[List[str]] = None
    ) -> List[mlflow.entities.Run]:
        """
        Get all runs in this experiment.

        Args:
            filter_string: MLflow filter string (e.g., "params.prior_type = 'finnish'")
            order_by: List of columns to order by (e.g., ["metrics.loo_elpd DESC"])

        Returns:
            List of Run objects
        """
        runs = self.client.search_runs(
            experiment_ids=[self.experiment_id],
            filter_string=filter_string,
            order_by=order_by or ["start_time DESC"]
        )
        return runs

    def compare_runs(
        self,
        run_ids: List[str],
        metrics: Optional[List[str]] = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        Compare metrics across multiple runs.

        Args:
            run_ids: List of run IDs to compare
            metrics: List of metric names to compare (default: all)

        Returns:
            Dict mapping run_id -> {metric_name: value}
        """
        comparison = {}

        for run_id in run_ids:
            run = self.client.get_run(run_id)
            run_metrics = run.data.metrics
            run_params = run.data.params

            if metrics:
                run_metrics = {k: v for k, v in run_metrics.items() if k in metrics}

            comparison[run_id] = {
                "run_name": run.info.run_name,
                "start_time": run.info.start_time,
                "params": run_params,
                "metrics": run_metrics
            }

        return comparison

    def get_best_run(
        self,
        metric: str = "loo_elpd",
        ascending: bool = False,
        filter_string: Optional[str] = None
    ) -> Optional[mlflow.entities.Run]:
        """
        Get the best run by a specific metric.

        Args:
            metric: Metric to optimize
            ascending: If True, lower is better
            filter_string: Optional filter

        Returns:
            Best Run object or None
        """
        order = "ASC" if ascending else "DESC"
        runs = self.get_all_runs(
            filter_string=filter_string,
            order_by=[f"metrics.{metric} {order}"]
        )
        return runs[0] if runs else None


def create_run_config_from_model(model, prior_type: str = "finnish") -> ModelRunConfig:
    """
    Create a ModelRunConfig from a BaseHierarchicalModel instance.

    Args:
        model: BaseHierarchicalModel instance (after load_and_prepare_data())
        prior_type: The prior type being used

    Returns:
        ModelRunConfig with all settings
    """
    mu_location, mu_scale = model.get_state_mean_prior()
    var_priors = model.get_variance_priors()

    config = ModelRunConfig(
        indicator_id=model.MODEL_NAME,
        indicator_name=model.get_model_description(),
        student_group=model.student_group_slug,
        student_group_name=model.student_group_name,
        prior_type=prior_type,
        mu_state_location=mu_location,
        mu_state_scale=mu_scale,
        sigma_district_scale=var_priors.get('sigma_district', 5.0),
        sigma_school_scale=var_priors.get('sigma_school', 3.0),
        sigma_y_scale=var_priors.get('sigma_y', 2.0),
        likelihood_type=model.get_likelihood_type(),
        run_timestamp=datetime.now().isoformat()
    )

    # Add horseshoe-specific params if applicable
    if prior_type in ("horseshoe", "finnish"):
        config.tau_scale = model.get_tau_scale()
        if prior_type == "finnish":
            slab_scale, slab_df = model.get_slab_parameters()
            config.slab_scale = slab_scale
            config.slab_df = slab_df

    # Add data info if available
    if model.data:
        config.n_observations = len(model.data.get('y', []))
        config.n_schools = model.data.get('n_schools', 0)
        config.n_districts = model.data.get('n_districts', 0)
        config.n_counties = model.data.get('n_counties', 0)
        config.n_predictors = model.data.get('n_predictors', 0)

    return config


def create_metrics_from_model(model, loo_result=None) -> ModelMetrics:
    """
    Create ModelMetrics from a fitted BaseHierarchicalModel.

    Args:
        model: BaseHierarchicalModel instance (after sample_posterior())
        loo_result: Optional LOO-CV result from arviz

    Returns:
        ModelMetrics with all diagnostic values
    """
    import arviz as az
    import numpy as np

    metrics = ModelMetrics()
    trace = model.trace

    if trace is None:
        return metrics

    # Convergence diagnostics
    if 'sample_stats' in trace and 'diverging' in trace.sample_stats:
        divergences = int(trace.sample_stats['diverging'].sum().values)
        total_samples = int(trace.sample_stats['diverging'].size)
        metrics.n_divergences = divergences
        metrics.divergence_pct = (divergences / total_samples) * 100

    # R-hat
    rhat = az.rhat(trace)
    rhat_values = []
    for var in ['mu_state', 'sigma_district', 'sigma_school', 'sigma_y', 'beta']:
        if var in rhat:
            vals = rhat[var].values
            if hasattr(vals, '__iter__'):
                rhat_values.extend(vals.flatten())
            else:
                rhat_values.append(float(vals))
    if rhat_values:
        metrics.max_rhat = float(max(rhat_values))

    # ESS
    ess_bulk = az.ess(trace, method='bulk')
    ess_tail = az.ess(trace, method='tail')

    ess_bulk_values = []
    ess_tail_values = []
    for var in ['mu_state', 'sigma_district', 'sigma_school', 'sigma_y']:
        if var in ess_bulk:
            ess_bulk_values.append(float(ess_bulk[var].values))
        if var in ess_tail:
            ess_tail_values.append(float(ess_tail[var].values))

    if ess_bulk_values:
        metrics.min_ess_bulk = float(min(ess_bulk_values))
    if ess_tail_values:
        metrics.min_ess_tail = float(min(ess_tail_values))

    # Convergence summary
    # Standard practice: accept <1% divergences (strict would be 0)
    # R-hat < 1.01 is strict; 1.02 is acceptable for complex hierarchical models
    # ESS > 400 is recommended minimum for reliable inference
    metrics.convergence_passed = (
        metrics.divergence_pct < 1.0 and  # <1% divergence rate
        metrics.max_rhat < 1.02 and       # slightly relaxed from 1.01
        metrics.min_ess_bulk > 400 and
        metrics.min_ess_tail > 400
    )

    # LOO-CV metrics
    if loo_result is not None:
        metrics.loo_elpd = float(loo_result.elpd_loo)
        metrics.loo_se = float(loo_result.se)
        metrics.loo_p_loo = float(loo_result.p_loo)
        pareto_k = loo_result.pareto_k.values
        metrics.loo_n_bad_k = int((pareto_k > 0.7).sum())
        metrics.loo_pct_bad_k = float(100 * metrics.loo_n_bad_k / len(pareto_k))
        metrics.loo_reliable = metrics.loo_pct_bad_k < 5

    # Posterior summaries
    if 'mu_state' in trace.posterior:
        mu_state = trace.posterior['mu_state'].values.flatten()
        metrics.state_mean_posterior = float(mu_state.mean())
        metrics.state_mean_ci_lower = float(np.percentile(mu_state, 2.5))
        metrics.state_mean_ci_upper = float(np.percentile(mu_state, 97.5))

    if 'sigma_district' in trace.posterior:
        metrics.sigma_district_posterior = float(
            trace.posterior['sigma_district'].values.mean()
        )

    if 'sigma_school' in trace.posterior:
        metrics.sigma_school_posterior = float(
            trace.posterior['sigma_school'].values.mean()
        )

    if 'sigma_y' in trace.posterior:
        metrics.sigma_y_posterior = float(
            trace.posterior['sigma_y'].values.mean()
        )

    # Covariate summary
    if 'beta' in trace.posterior:
        beta = trace.posterior['beta'].values
        beta_lower = np.percentile(beta, 2.5, axis=(0, 1))
        beta_upper = np.percentile(beta, 97.5, axis=(0, 1))
        significant = (beta_lower > 0) | (beta_upper < 0)
        metrics.n_significant_covariates = int(significant.sum())

        # For horseshoe, count effective covariates
        if 'tau' in trace.posterior and 'lambdas' in trace.posterior:
            tau = trace.posterior['tau'].values.mean()
            lambdas = trace.posterior['lambdas'].values.mean(axis=(0, 1))
            shrinkage = 1 / (1 + lambdas**2 * tau**2)
            metrics.n_effective_covariates = int((shrinkage < 0.5).sum())

    return metrics


# Convenience function for quick experiment setup
def setup_experiment(
    indicator: str,
    tracking_uri: Optional[str] = None
) -> MLflowTracker:
    """
    Quick setup for an MLflow experiment.

    Args:
        indicator: Indicator name (used as experiment name)
        tracking_uri: Optional tracking URI

    Returns:
        Configured MLflowTracker
    """
    return MLflowTracker(
        experiment_name=indicator,
        tracking_uri=tracking_uri
    )


def get_all_published_across_experiments(
    experiment_prefix: str = "bayesian_"
) -> Dict[str, Dict[str, Any]]:
    """
    Get all published runs across all Bayesian experiments.

    This is used by combine_results.py to find which run to use
    for each indicator+student_group combination.

    Args:
        experiment_prefix: Prefix for experiment names to search

    Returns:
        Dict mapping "indicator/student_group" -> run info including artifact paths
    """
    from pathlib import Path

    # Set up tracking URI
    base_dir = Path(__file__).parent.parent
    db_path = base_dir / "mlruns.db"
    mlflow.set_tracking_uri(f"sqlite:///{db_path}")

    client = MlflowClient()

    # Find all bayesian experiments
    all_experiments = client.search_experiments()
    bayesian_experiments = [
        exp for exp in all_experiments
        if exp.name.startswith(experiment_prefix)
    ]

    published_runs = {}

    for experiment in bayesian_experiments:
        # Extract indicator name from experiment name
        indicator = experiment.name.replace(experiment_prefix, "")

        # Find published runs in this experiment
        runs = client.search_runs(
            experiment_ids=[experiment.experiment_id],
            filter_string="tags.published = 'true'"
        )

        for run in runs:
            student_group = run.data.tags.get("published_student_group", "unknown")
            key = f"{indicator}/{student_group}"

            published_runs[key] = {
                "indicator": indicator,
                "student_group": student_group,
                "run_id": run.info.run_id,
                "run_name": run.info.run_name,
                "experiment_name": experiment.name,
                "artifact_uri": run.info.artifact_uri,
                "published_at": run.data.tags.get("published_at"),
                "prior_type": run.data.params.get("prior_type"),
                "prior_scale": run.data.params.get("prior_scale_factor", "1.0"),
                "loo_elpd": run.data.metrics.get("loo_elpd"),
                "convergence_passed": bool(run.data.metrics.get("convergence_passed", 0)),
            }

    return published_runs


def download_published_artifacts(
    indicator: str,
    student_group: str,
    dest_dir: Optional[Path] = None
) -> Optional[Path]:
    """
    Download artifacts from a published run.

    Args:
        indicator: Indicator name (e.g., "graduation")
        student_group: Student group slug
        dest_dir: Destination directory (default: temp directory)

    Returns:
        Path to downloaded artifacts directory, or None if no published run
    """
    import tempfile

    tracker = MLflowTracker(experiment_name=f"bayesian_{indicator}")
    published = tracker.get_published_run(student_group)

    if not published:
        logger.warning(f"No published run for {indicator}/{student_group}")
        return None

    if dest_dir is None:
        dest_dir = Path(tempfile.mkdtemp(prefix=f"mlflow_{indicator}_{student_group}_"))

    # Download artifacts
    artifact_uri = published.info.artifact_uri
    client = MlflowClient()
    client.download_artifacts(published.info.run_id, "", str(dest_dir))

    logger.info(f"Downloaded artifacts to {dest_dir}")
    return dest_dir
