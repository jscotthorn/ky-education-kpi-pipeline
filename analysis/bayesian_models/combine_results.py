#!/usr/bin/env python3
"""
Combine Bayesian Model Results

Reads model outputs and combines them into a single JSON file at data/bayesian/bayesian_results.json.

Two modes of operation:
    1. Filesystem mode (default): Reads from analysis/outputs/models/{model_name}_{student_group}/
    2. MLflow mode (--from-mlflow): Reads from published MLflow runs

Directory naming convention (filesystem mode):
    - Old format: graduation/ (interpreted as graduation_all_students)
    - New format: graduation_african_american/, graduation_economically_disadvantaged/

Usage:
    # Filesystem mode (legacy)
    python combine_results.py                 # Combine all from filesystem
    python combine_results.py --models graduation_all_students graduation_african_american

    # MLflow mode (recommended)
    python combine_results.py --from-mlflow   # Combine from published MLflow runs
    python combine_results.py --from-mlflow --list  # List what would be combined
"""

import argparse
import json
import math
import re
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# Add config directory to path for student_groups import
CONFIG_DIR = Path(__file__).parent.parent / "config"
sys.path.insert(0, str(CONFIG_DIR))
from student_groups import ALL_GROUP_SLUGS, STUDENT_GROUP_BY_SLUG

# Add bayesian_models to path for mlflow_tracking import
BAYESIAN_DIR = Path(__file__).parent
sys.path.insert(0, str(BAYESIAN_DIR))

# Conditional import for MLflow (only needed in --from-mlflow mode)
try:
    from mlflow_tracking import get_all_published_across_experiments
    from mlflow.tracking import MlflowClient
    import mlflow
    MLFLOW_AVAILABLE = True
except ImportError:
    MLFLOW_AVAILABLE = False


def sanitize_for_json(obj: Any) -> Any:
    """Recursively sanitize object, converting NaN/Inf to None for valid JSON."""
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    elif isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_json(item) for item in obj]
    return obj


# Path setup
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
MODELS_DIR = PROJECT_ROOT / "analysis" / "outputs" / "models"
OUTPUT_DIR = PROJECT_ROOT / "data" / "bayesian"

# Known model base names (without student_group suffix)
KNOWN_MODEL_NAMES = [
    'graduation', 'chronic_absenteeism', 'reading_grade3', 'math_grade8',
    'kindergarten_readiness', 'postsecondary_enrollment', 'postsecondary_readiness',
    'el_progress_elementary', 'el_progress_middle', 'el_progress_high', 'school_climate'
]


def parse_model_dir_name(dir_name: str) -> Tuple[str, str, str]:
    """
    Parse a model directory name to extract model name and student_group.

    Examples:
        "graduation" -> ("graduation", "All Students", "all_students")
        "graduation_all_students" -> ("graduation", "All Students", "all_students")
        "graduation_african_american" -> ("graduation", "African American", "african_american")
        "el_progress_elementary_economically_disadvantaged" ->
            ("el_progress_elementary", "Economically Disadvantaged", "economically_disadvantaged")

    Returns:
        Tuple of (model_name, student_group_name, student_group_slug)
    """
    # Try each known model name, starting with longest to avoid partial matches
    for model_name in sorted(KNOWN_MODEL_NAMES, key=len, reverse=True):
        if dir_name == model_name:
            # Old format without student_group suffix -> assume all_students
            return (model_name, "All Students", "all_students")

        if dir_name.startswith(model_name + "_"):
            # New format with student_group suffix
            student_group_slug = dir_name[len(model_name) + 1:]
            if student_group_slug in STUDENT_GROUP_BY_SLUG:
                student_group_name = STUDENT_GROUP_BY_SLUG[student_group_slug].name
                return (model_name, student_group_name, student_group_slug)

    # Fallback: assume entire dir_name is model_name with all_students
    return (dir_name, "All Students", "all_students")


def discover_models() -> List[str]:
    """Discover available model directories."""
    models = []
    if MODELS_DIR.exists():
        for path in MODELS_DIR.iterdir():
            if path.is_dir() and (path / "school_effects.csv").exists():
                models.append(path.name)
    return sorted(models)


def load_school_effects(dir_name: str) -> List[Dict[str, Any]]:
    """Load school effects for a model.

    Args:
        dir_name: Directory name (e.g., 'graduation_african_american')

    Returns:
        List of school effect records with student_group fields.
    """
    effects_file = MODELS_DIR / dir_name / "school_effects.csv"
    if not effects_file.exists():
        print(f"  Warning: {effects_file} not found")
        return []

    df = pd.read_csv(effects_file)

    # Parse directory name to get model and student_group info
    model_name, student_group_name, student_group_slug = parse_model_dir_name(dir_name)

    # Convert to list of dicts with proper types
    records = []
    for _, row in df.iterrows():
        record = {
            "model": model_name,  # Use parsed model name (without student_group suffix)
            "dir_name": dir_name,  # Full directory name for reference
            "school_id": str(row.get("school_id", "")),
            "school_name": str(row["school_name"]),
            "district": str(row["district"]),
            "is_fayette": bool(row["is_fayette"]),
            "effect_mean": float(row["effect_mean"]),
            "effect_std": float(row["effect_std"]),
        }
        # Add student_group fields - from CSV if present, otherwise from directory parsing
        if "student_group" in df.columns:
            record["student_group"] = str(row["student_group"])
        else:
            record["student_group"] = student_group_name
        if "student_group_slug" in df.columns:
            record["student_group_slug"] = str(row["student_group_slug"])
        else:
            record["student_group_slug"] = student_group_slug

        # Add CI columns if present
        for col in ["ci_lower_2.5", "ci_upper_97.5", "ci_lower_10", "ci_upper_90"]:
            if col in df.columns:
                record[col] = float(row[col])
        # Add expected_pct columns if present (interpretable scale)
        for col in ["expected_pct", "expected_pct_lower", "expected_pct_upper"]:
            if col in df.columns:
                record[col] = float(row[col])
        # Add pooling diagnostics if present
        if "pooling_factor" in df.columns:
            record["pooling_factor"] = float(row["pooling_factor"])
        if "reliability" in df.columns:
            record["reliability"] = float(row["reliability"])
        records.append(record)

    return records


def load_covariate_effects(dir_name: str) -> List[Dict[str, Any]]:
    """Load covariate effects for a model.

    Args:
        dir_name: Directory name (e.g., 'graduation_african_american')

    Returns:
        List of covariate effect records with student_group fields.
    """
    effects_file = MODELS_DIR / dir_name / "covariate_effects.csv"
    if not effects_file.exists():
        print(f"  Warning: {effects_file} not found")
        return []

    df = pd.read_csv(effects_file)

    # Parse directory name to get model and student_group info
    model_name, student_group_name, student_group_slug = parse_model_dir_name(dir_name)

    records = []
    for _, row in df.iterrows():
        record = {
            "model": model_name,  # Use parsed model name (without student_group suffix)
            "dir_name": dir_name,  # Full directory name for reference
            "student_group": student_group_name,
            "student_group_slug": student_group_slug,
            "predictor": str(row["predictor"]),
            "category": str(row.get("category", "other")),
            "effect_mean": float(row["effect_mean"]),
            "effect_std": float(row["effect_std"]),
            "ci_lower_2.5": float(row["ci_lower_2.5"]),
            "ci_upper_97.5": float(row["ci_upper_97.5"]),
        }
        # Add 80% CI if present
        for col in ["ci_lower_10", "ci_upper_90"]:
            if col in df.columns:
                record[col] = float(row[col])
        # Add horseshoe-specific fields if present
        if "shrinkage_factor" in df.columns:
            record["shrinkage_factor"] = float(row["shrinkage_factor"])
            record["effective"] = bool(row["effective"])
        # Add collinearity diagnostic fields if present
        if "bivariate_corr" in df.columns:
            record["bivariate_corr"] = float(row["bivariate_corr"])
        if "statewide_interpretation" in df.columns:
            record["statewide_interpretation"] = str(row["statewide_interpretation"])
        if "interpretation" in df.columns:
            # Keep for backward compatibility
            record["interpretation"] = str(row["interpretation"])
        if "report_safe" in df.columns:
            record["report_safe"] = bool(row["report_safe"])
        records.append(record)

    return records


def load_county_covariate_effects(dir_name: str) -> List[Dict[str, Any]]:
    """Load county-specific covariate effects for a model.

    Args:
        dir_name: Directory name (e.g., 'graduation_african_american')

    Returns:
        List of county covariate effect records with student_group fields.
    """
    effects_file = MODELS_DIR / dir_name / "county_covariate_effects.csv"
    if not effects_file.exists():
        # Not all models may have county-varying slopes
        return []

    df = pd.read_csv(effects_file)

    # Parse directory name to get model and student_group info
    model_name, student_group_name, student_group_slug = parse_model_dir_name(dir_name)

    records = []
    for _, row in df.iterrows():
        record = {
            "model": model_name,  # Use parsed model name (without student_group suffix)
            "dir_name": dir_name,  # Full directory name for reference
            "student_group": student_group_name,
            "student_group_slug": student_group_slug,
            "county": str(row["county"]),
            "predictor": str(row["predictor"]),
            "global_effect": float(row["global_effect"]),
            "county_effect": float(row["county_effect"]),
            "county_deviation": float(row["county_deviation"]),
            "effect_std": float(row["effect_std"]),
            "ci_lower_2.5": float(row["ci_lower_2.5"]),
            "ci_upper_97.5": float(row["ci_upper_97.5"]),
            "differs_from_global": bool(row["differs_from_global"]),
        }
        # Add county reliability metrics if present
        if "n_schools" in df.columns:
            record["n_schools"] = int(row["n_schools"])
        if "reliability" in df.columns:
            record["reliability"] = float(row["reliability"])
        if "shrinkage" in df.columns:
            record["shrinkage"] = float(row["shrinkage"])
        # Add interpretation fields if present
        if "statewide_interpretation" in df.columns:
            record["statewide_interpretation"] = str(row["statewide_interpretation"])
        if "county_interpretation" in df.columns:
            record["county_interpretation"] = str(row["county_interpretation"])
        records.append(record)

    return records


def load_model_diagnostics(model_name: str) -> Optional[Dict[str, Any]]:
    """Load model diagnostics from summary file."""
    summary_file = MODELS_DIR / model_name / "model_summary.csv"
    if not summary_file.exists():
        return None

    df = pd.read_csv(summary_file, index_col=0)

    # Extract key diagnostics
    diagnostics: Dict[str, Any] = {}

    # R-hat values
    if "r_hat" in df.columns:
        diagnostics["max_rhat"] = float(df["r_hat"].max())
        diagnostics["rhat_passing"] = bool(df["r_hat"].max() < 1.01)

    # ESS values
    for ess_col in ["ess_bulk", "ess_tail"]:
        if ess_col in df.columns:
            diagnostics[f"min_{ess_col}"] = float(df[ess_col].min())

    if "ess_bulk" in df.columns:
        diagnostics["ess_passing"] = bool(df["ess_bulk"].min() > 400)

    return diagnostics if diagnostics else None


def combine_results(dir_names: Optional[List[str]] = None) -> Dict[str, Any]:
    """Combine results from multiple model directories into a single structure.

    Args:
        dir_names: List of directory names (e.g., ['graduation_all_students', 'graduation_african_american'])
                   If None, discovers all available models.

    Returns:
        Combined results dictionary with metadata, school_effects, covariate_effects, etc.
    """
    if dir_names is None:
        dir_names = discover_models()

    if not dir_names:
        print("No models found to combine")
        return {}

    print(f"Combining results from {len(dir_names)} directories: {', '.join(dir_names)}")

    all_school_effects: List[Dict[str, Any]] = []
    all_covariate_effects: List[Dict[str, Any]] = []
    all_county_covariate_effects: List[Dict[str, Any]] = []
    model_diagnostics: Dict[str, Any] = {}

    # Track unique models and student_groups
    unique_models: set = set()
    unique_student_groups: set = set()

    for dir_name in dir_names:
        print(f"\n  Loading {dir_name}...")

        # Parse to get model and student_group for tracking
        model_name, student_group_name, student_group_slug = parse_model_dir_name(dir_name)
        unique_models.add(model_name)
        unique_student_groups.add(student_group_slug)

        school_effects = load_school_effects(dir_name)
        print(f"    School effects: {len(school_effects)} records")
        all_school_effects.extend(school_effects)

        covariate_effects = load_covariate_effects(dir_name)
        print(f"    Covariate effects: {len(covariate_effects)} records")
        all_covariate_effects.extend(covariate_effects)

        county_covariate_effects = load_county_covariate_effects(dir_name)
        if county_covariate_effects:
            print(f"    County covariate effects: {len(county_covariate_effects)} records")
            all_county_covariate_effects.extend(county_covariate_effects)

        diagnostics = load_model_diagnostics(dir_name)
        if diagnostics:
            model_diagnostics[dir_name] = diagnostics
            print(f"    Diagnostics loaded")

    result = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "directories": dir_names,  # Full directory names
            "models": sorted(unique_models),  # Unique model base names
            "student_groups": sorted(unique_student_groups),  # Unique student_group slugs
            "total_schools": len(all_school_effects),
            "total_covariates": len(all_covariate_effects),
            "total_county_covariates": len(all_county_covariate_effects),
        },
        "school_effects": all_school_effects,
        "covariate_effects": all_covariate_effects,
        "county_covariate_effects": all_county_covariate_effects,
        "model_diagnostics": model_diagnostics,
    }

    return result


# ============================================================================
# MLflow Integration
# ============================================================================

def combine_results_from_mlflow() -> Dict[str, Any]:
    """
    Combine results from published MLflow runs.

    This reads artifacts from all published runs across all Bayesian experiments
    and combines them into a single structure for the portal.

    Returns:
        Combined results dictionary with metadata, school_effects, covariate_effects, etc.
    """
    if not MLFLOW_AVAILABLE:
        raise ImportError("MLflow is not available. Install with: pip install mlflow")

    # Set up MLflow tracking
    base_dir = Path(__file__).parent.parent
    db_path = base_dir / "mlruns.db"
    mlflow.set_tracking_uri(f"sqlite:///{db_path}")

    client = MlflowClient()

    # Get all published runs
    published = get_all_published_across_experiments()

    if not published:
        print("No published runs found")
        return {}

    print(f"Combining results from {len(published)} published MLflow runs")

    all_school_effects: List[Dict[str, Any]] = []
    all_covariate_effects: List[Dict[str, Any]] = []
    all_county_covariate_effects: List[Dict[str, Any]] = []
    model_diagnostics: Dict[str, Any] = {}
    run_metadata: Dict[str, Any] = {}

    # Track unique models and student_groups
    unique_models: set = set()
    unique_student_groups: set = set()

    for key, info in sorted(published.items()):
        indicator = info['indicator']
        student_group = info['student_group']
        run_id = info['run_id']
        run_name = info['run_name']

        print(f"\n  Loading {indicator}/{student_group} from run {run_name}...")

        unique_models.add(indicator)
        unique_student_groups.add(student_group)

        # Download artifacts to temp directory
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            try:
                # Download model_outputs artifacts
                client.download_artifacts(run_id, "model_outputs", str(temp_path))
                artifact_dir = temp_path / "model_outputs"
            except Exception as e:
                print(f"    Warning: Could not download artifacts: {e}")
                continue

            # Construct dir_name for parsing
            dir_name = f"{indicator}_{student_group}"

            # Load school effects
            school_effects = load_school_effects_from_path(
                artifact_dir, dir_name, indicator, student_group
            )
            print(f"    School effects: {len(school_effects)} records")
            all_school_effects.extend(school_effects)

            # Load covariate effects
            covariate_effects = load_covariate_effects_from_path(
                artifact_dir, dir_name, indicator, student_group
            )
            print(f"    Covariate effects: {len(covariate_effects)} records")
            all_covariate_effects.extend(covariate_effects)

            # Load county covariate effects
            county_effects = load_county_covariate_effects_from_path(
                artifact_dir, dir_name, indicator, student_group
            )
            if county_effects:
                print(f"    County covariate effects: {len(county_effects)} records")
                all_county_covariate_effects.extend(county_effects)

            # Load diagnostics
            diagnostics = load_model_diagnostics_from_path(artifact_dir)
            if diagnostics:
                model_diagnostics[dir_name] = diagnostics

        # Track run metadata
        run_metadata[key] = {
            "run_id": run_id,
            "run_name": run_name,
            "published_at": info.get("published_at"),
            "prior_type": info.get("prior_type"),
            "prior_scale": info.get("prior_scale"),
            "loo_elpd": info.get("loo_elpd"),
            "convergence_passed": info.get("convergence_passed"),
        }

    result = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "source": "mlflow",
            "models": sorted(unique_models),
            "student_groups": sorted(unique_student_groups),
            "total_schools": len(all_school_effects),
            "total_covariates": len(all_covariate_effects),
            "total_county_covariates": len(all_county_covariate_effects),
            "published_runs": run_metadata,
        },
        "school_effects": all_school_effects,
        "covariate_effects": all_covariate_effects,
        "county_covariate_effects": all_county_covariate_effects,
        "model_diagnostics": model_diagnostics,
    }

    return result


def load_school_effects_from_path(
    artifact_dir: Path,
    dir_name: str,
    model_name: str,
    student_group_slug: str
) -> List[Dict[str, Any]]:
    """Load school effects from a downloaded artifact directory."""
    effects_file = artifact_dir / "school_effects.csv"
    if not effects_file.exists():
        return []

    df = pd.read_csv(effects_file)
    student_group_name = STUDENT_GROUP_BY_SLUG.get(student_group_slug)
    if student_group_name:
        student_group_name = student_group_name.name
    else:
        student_group_name = student_group_slug.replace("_", " ").title()

    records = []
    for _, row in df.iterrows():
        record = {
            "model": model_name,
            "dir_name": dir_name,
            "school_id": str(row.get("school_id", "")),
            "school_name": str(row["school_name"]),
            "district": str(row["district"]),
            "is_fayette": bool(row["is_fayette"]),
            "effect_mean": float(row["effect_mean"]),
            "effect_std": float(row["effect_std"]),
            "student_group": str(row.get("student_group", student_group_name)),
            "student_group_slug": str(row.get("student_group_slug", student_group_slug)),
        }
        # Add optional columns
        for col in ["ci_lower_2.5", "ci_upper_97.5", "ci_lower_10", "ci_upper_90",
                    "expected_pct", "expected_pct_lower", "expected_pct_upper",
                    "pooling_factor", "reliability"]:
            if col in df.columns:
                record[col] = float(row[col])
        records.append(record)

    return records


def load_covariate_effects_from_path(
    artifact_dir: Path,
    dir_name: str,
    model_name: str,
    student_group_slug: str
) -> List[Dict[str, Any]]:
    """Load covariate effects from a downloaded artifact directory."""
    effects_file = artifact_dir / "covariate_effects.csv"
    if not effects_file.exists():
        return []

    df = pd.read_csv(effects_file)
    student_group_name = STUDENT_GROUP_BY_SLUG.get(student_group_slug)
    if student_group_name:
        student_group_name = student_group_name.name
    else:
        student_group_name = student_group_slug.replace("_", " ").title()

    records = []
    for _, row in df.iterrows():
        record = {
            "model": model_name,
            "dir_name": dir_name,
            "student_group": student_group_name,
            "student_group_slug": student_group_slug,
            "predictor": str(row["predictor"]),
            "category": str(row.get("category", "other")),
            "effect_mean": float(row["effect_mean"]),
            "effect_std": float(row["effect_std"]),
            "ci_lower_2.5": float(row["ci_lower_2.5"]),
            "ci_upper_97.5": float(row["ci_upper_97.5"]),
        }
        # Add optional columns
        for col in ["ci_lower_10", "ci_upper_90", "shrinkage_factor", "effective",
                    "bivariate_corr", "statewide_interpretation", "interpretation", "report_safe"]:
            if col in df.columns:
                val = row[col]
                if col in ["effective", "report_safe"]:
                    record[col] = bool(val)
                elif col in ["statewide_interpretation", "interpretation"]:
                    record[col] = str(val)
                else:
                    record[col] = float(val)
        records.append(record)

    return records


def load_county_covariate_effects_from_path(
    artifact_dir: Path,
    dir_name: str,
    model_name: str,
    student_group_slug: str
) -> List[Dict[str, Any]]:
    """Load county covariate effects from a downloaded artifact directory."""
    effects_file = artifact_dir / "county_covariate_effects.csv"
    if not effects_file.exists():
        return []

    df = pd.read_csv(effects_file)
    student_group_name = STUDENT_GROUP_BY_SLUG.get(student_group_slug)
    if student_group_name:
        student_group_name = student_group_name.name
    else:
        student_group_name = student_group_slug.replace("_", " ").title()

    records = []
    for _, row in df.iterrows():
        record = {
            "model": model_name,
            "dir_name": dir_name,
            "student_group": student_group_name,
            "student_group_slug": student_group_slug,
            "county": str(row["county"]),
            "predictor": str(row["predictor"]),
            "global_effect": float(row["global_effect"]),
            "county_effect": float(row["county_effect"]),
            "county_deviation": float(row["county_deviation"]),
            "effect_std": float(row["effect_std"]),
            "ci_lower_2.5": float(row["ci_lower_2.5"]),
            "ci_upper_97.5": float(row["ci_upper_97.5"]),
            "differs_from_global": bool(row["differs_from_global"]),
        }
        # Add optional columns
        for col in ["n_schools", "reliability", "shrinkage",
                    "statewide_interpretation", "county_interpretation"]:
            if col in df.columns:
                val = row[col]
                if col == "n_schools":
                    record[col] = int(val)
                elif col in ["statewide_interpretation", "county_interpretation"]:
                    record[col] = str(val)
                else:
                    record[col] = float(val)
        records.append(record)

    return records


def load_model_diagnostics_from_path(artifact_dir: Path) -> Optional[Dict[str, Any]]:
    """Load model diagnostics from a downloaded artifact directory."""
    summary_file = artifact_dir / "model_summary.csv"
    if not summary_file.exists():
        return None

    df = pd.read_csv(summary_file, index_col=0)
    diagnostics: Dict[str, Any] = {}

    if "r_hat" in df.columns:
        diagnostics["max_rhat"] = float(df["r_hat"].max())
        diagnostics["rhat_passing"] = bool(df["r_hat"].max() < 1.01)

    for ess_col in ["ess_bulk", "ess_tail"]:
        if ess_col in df.columns:
            diagnostics[f"min_{ess_col}"] = float(df[ess_col].min())

    if "ess_bulk" in df.columns:
        diagnostics["ess_passing"] = bool(df["ess_bulk"].min() > 400)

    return diagnostics if diagnostics else None


def list_published_runs():
    """List all published runs that would be combined."""
    if not MLFLOW_AVAILABLE:
        print("MLflow is not available. Install with: pip install mlflow")
        return

    published = get_all_published_across_experiments()

    if not published:
        print("No published runs found")
        return

    print(f"\nPublished runs that will be combined ({len(published)} total):")
    print("=" * 90)

    # Group by indicator
    by_indicator = {}
    for key, info in published.items():
        indicator = info['indicator']
        if indicator not in by_indicator:
            by_indicator[indicator] = []
        by_indicator[indicator].append(info)

    for indicator in sorted(by_indicator.keys()):
        print(f"\n{indicator}:")
        print("-" * 80)
        for info in sorted(by_indicator[indicator], key=lambda x: x['student_group']):
            conv = "PASS" if info.get("convergence_passed") else "ISSUES"
            loo = info.get("loo_elpd")
            loo_str = f"{loo:.2f}" if isinstance(loo, float) else "N/A"
            print(f"  {info['student_group']:30s} {info['run_name']:25s} "
                  f"Conv:{conv:6s} LOO:{loo_str:>8s}")


def save_results(results: Dict[str, Any], output_path: Optional[Path] = None) -> Path:
    """Save combined results to JSON file."""
    if output_path is None:
        output_path = OUTPUT_DIR / "bayesian_results.json"

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Sanitize NaN/Inf values before serialization
    sanitized_results = sanitize_for_json(results)
    with open(output_path, "w") as f:
        json.dump(sanitized_results, f, indent=2)

    print(f"\nResults saved to: {output_path}")
    return output_path


def main():
    parser = argparse.ArgumentParser(description="Combine Bayesian model results")
    parser.add_argument(
        "--models",
        nargs="+",
        help="Specific models to combine (default: all available)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Output file path (default: data/bayesian/bayesian_results.json)",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available models and exit",
    )
    parser.add_argument(
        "--from-mlflow",
        action="store_true",
        help="Read from published MLflow runs instead of filesystem",
    )
    args = parser.parse_args()

    if args.list:
        if args.from_mlflow:
            list_published_runs()
        else:
            dirs = discover_models()
            if dirs:
                print("Available model directories:")
                for d in dirs:
                    model_name, student_group_name, student_group_slug = parse_model_dir_name(d)
                    print(f"  - {d}  (model: {model_name}, student_group: {student_group_name})")
            else:
                print("No models found")
        return

    # Combine results from appropriate source
    if args.from_mlflow:
        results = combine_results_from_mlflow()
    else:
        results = combine_results(args.models)

    if results:
        save_results(results, args.output)

        # Print summary
        print(f"\nSummary:")
        if args.from_mlflow:
            print(f"  Source: MLflow published runs")
            print(f"  Published runs: {len(results['metadata'].get('published_runs', {}))}")
        else:
            print(f"  Source: Filesystem")
            print(f"  Directories: {len(results['metadata'].get('directories', []))}")
        print(f"  Unique models: {len(results['metadata']['models'])}")
        print(f"  Student groups: {len(results['metadata']['student_groups'])} ({', '.join(results['metadata']['student_groups'])})")
        print(f"  School effects: {results['metadata']['total_schools']}")
        print(f"  Covariate effects: {results['metadata']['total_covariates']}")
        if results['metadata'].get('total_county_covariates', 0) > 0:
            print(f"  County covariate effects: {results['metadata']['total_county_covariates']}")


if __name__ == "__main__":
    main()
