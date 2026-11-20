"""
Configuration utilities for Bayesian analysis.
"""

from pathlib import Path
from typing import Dict, List, Optional, Any
import yaml


CONFIG_DIR = Path(__file__).parent


def load_covariate_catalog() -> Dict[str, Any]:
    """Load the covariate catalog from YAML.

    Returns:
        Dictionary containing covariate definitions and indicator configurations
    """
    catalog_path = CONFIG_DIR / "covariate_catalog.yaml"
    with open(catalog_path, 'r') as f:
        return yaml.safe_load(f)


def get_indicator_covariates(indicator: str,
                             include_optional: bool = False) -> List[str]:
    """Get list of covariates for a specific indicator.

    Args:
        indicator: Name of the indicator (e.g., 'graduation_rate')
        include_optional: Whether to include optional covariates

    Returns:
        List of covariate column names
    """
    catalog = load_covariate_catalog()

    if indicator not in catalog.get('indicators', {}):
        raise ValueError(f"Unknown indicator: {indicator}. "
                        f"Available: {list(catalog.get('indicators', {}).keys())}")

    config = catalog['indicators'][indicator]

    covariates = []
    covariates.extend(config.get('required_covariates', []))
    covariates.extend(config.get('recommended_covariates', []))

    if include_optional:
        covariates.extend(config.get('optional_covariates', []))

    return covariates


def get_covariate_info(covariate: str) -> Dict[str, Any]:
    """Get metadata for a specific covariate.

    Args:
        covariate: Column name of the covariate

    Returns:
        Dictionary with covariate metadata (source, description, etc.)
    """
    catalog = load_covariate_catalog()

    for category, covariates in catalog.get('covariates', {}).items():
        if covariate in covariates:
            info = covariates[covariate].copy()
            info['category'] = category
            return info

    raise ValueError(f"Unknown covariate: {covariate}")


def get_excluded_covariates(indicator: Optional[str] = None) -> Dict[str, str]:
    """Get covariates that are excluded with reasons.

    Args:
        indicator: Optional indicator name to filter by

    Returns:
        Dictionary mapping covariate names to exclusion reasons
    """
    catalog = load_covariate_catalog()

    excluded = {}

    # Get globally excluded covariates
    for category, covariates in catalog.get('covariates', {}).items():
        for name, info in covariates.items():
            if info.get('status') == 'excluded':
                excluded[name] = info.get('exclusion_reason', 'No reason provided')

    # Get indicator-specific exclusions
    if indicator and indicator in catalog.get('indicators', {}):
        for cov in catalog['indicators'][indicator].get('excluded_covariates', []):
            if cov not in excluded:
                excluded[cov] = f"Excluded for {indicator}"

    return excluded


def get_model_selection_config() -> Dict[str, Any]:
    """Get model selection configuration.

    Returns:
        Dictionary with horseshoe settings, MCMC params, and thresholds
    """
    catalog = load_covariate_catalog()
    return catalog.get('model_selection', {})


def validate_covariates(covariates: List[str]) -> Dict[str, List[str]]:
    """Validate a list of covariates against the catalog.

    Args:
        covariates: List of covariate column names

    Returns:
        Dictionary with 'valid', 'excluded', and 'unknown' lists
    """
    catalog = load_covariate_catalog()

    all_covariates = {}
    excluded = set()

    for category, cov_dict in catalog.get('covariates', {}).items():
        for name, info in cov_dict.items():
            all_covariates[name] = info
            if info.get('status') == 'excluded':
                excluded.add(name)

    result = {
        'valid': [],
        'excluded': [],
        'unknown': []
    }

    for cov in covariates:
        if cov in excluded:
            result['excluded'].append(cov)
        elif cov in all_covariates:
            result['valid'].append(cov)
        else:
            result['unknown'].append(cov)

    return result
