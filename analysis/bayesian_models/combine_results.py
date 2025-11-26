#!/usr/bin/env python3
"""
Combine Bayesian Model Results

Reads model outputs from analysis/outputs/models/{model_name}/ and combines
them into a single JSON file at data/bayesian/bayesian_results.json.

Usage:
    python combine_results.py                 # Combine all available models
    python combine_results.py --models graduation reading_grade3
"""

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


# Path setup
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
MODELS_DIR = PROJECT_ROOT / "analysis" / "outputs" / "models"
OUTPUT_DIR = PROJECT_ROOT / "data" / "bayesian"


def discover_models() -> List[str]:
    """Discover available model directories."""
    models = []
    if MODELS_DIR.exists():
        for path in MODELS_DIR.iterdir():
            if path.is_dir() and (path / "school_effects.csv").exists():
                models.append(path.name)
    return sorted(models)


def load_school_effects(model_name: str) -> List[Dict[str, Any]]:
    """Load school effects for a model."""
    effects_file = MODELS_DIR / model_name / "school_effects.csv"
    if not effects_file.exists():
        print(f"  Warning: {effects_file} not found")
        return []

    df = pd.read_csv(effects_file)

    # Convert to list of dicts with proper types
    records = []
    for _, row in df.iterrows():
        record = {
            "model": model_name,
            "school_id": str(row.get("school_id", "")),
            "school_name": str(row["school_name"]),
            "district": str(row["district"]),
            "is_fayette": bool(row["is_fayette"]),
            "effect_mean": float(row["effect_mean"]),
            "effect_std": float(row["effect_std"]),
        }
        # Add CI columns if present
        for col in ["ci_lower_2.5", "ci_upper_97.5", "ci_lower_10", "ci_upper_90"]:
            if col in df.columns:
                record[col] = float(row[col])
        records.append(record)

    return records


def load_covariate_effects(model_name: str) -> List[Dict[str, Any]]:
    """Load covariate effects for a model."""
    effects_file = MODELS_DIR / model_name / "covariate_effects.csv"
    if not effects_file.exists():
        print(f"  Warning: {effects_file} not found")
        return []

    df = pd.read_csv(effects_file)

    records = []
    for _, row in df.iterrows():
        record = {
            "model": model_name,
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


def combine_results(model_names: Optional[List[str]] = None) -> Dict[str, Any]:
    """Combine results from multiple models into a single structure."""

    if model_names is None:
        model_names = discover_models()

    if not model_names:
        print("No models found to combine")
        return {}

    print(f"Combining results from {len(model_names)} models: {', '.join(model_names)}")

    all_school_effects: List[Dict[str, Any]] = []
    all_covariate_effects: List[Dict[str, Any]] = []
    model_diagnostics: Dict[str, Any] = {}

    for model_name in model_names:
        print(f"\n  Loading {model_name}...")

        school_effects = load_school_effects(model_name)
        print(f"    School effects: {len(school_effects)} records")
        all_school_effects.extend(school_effects)

        covariate_effects = load_covariate_effects(model_name)
        print(f"    Covariate effects: {len(covariate_effects)} records")
        all_covariate_effects.extend(covariate_effects)

        diagnostics = load_model_diagnostics(model_name)
        if diagnostics:
            model_diagnostics[model_name] = diagnostics
            print(f"    Diagnostics loaded")

    result = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "models": model_names,
            "total_schools": len(all_school_effects),
            "total_covariates": len(all_covariate_effects),
        },
        "school_effects": all_school_effects,
        "covariate_effects": all_covariate_effects,
        "model_diagnostics": model_diagnostics,
    }

    return result


def save_results(results: Dict[str, Any], output_path: Optional[Path] = None) -> Path:
    """Save combined results to JSON file."""
    if output_path is None:
        output_path = OUTPUT_DIR / "bayesian_results.json"

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)

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
    args = parser.parse_args()

    if args.list:
        models = discover_models()
        if models:
            print("Available models:")
            for model in models:
                print(f"  - {model}")
        else:
            print("No models found")
        return

    results = combine_results(args.models)

    if results:
        save_results(results, args.output)

        # Print summary
        print(f"\nSummary:")
        print(f"  Models: {len(results['metadata']['models'])}")
        print(f"  School effects: {results['metadata']['total_schools']}")
        print(f"  Covariate effects: {results['metadata']['total_covariates']}")


if __name__ == "__main__":
    main()
