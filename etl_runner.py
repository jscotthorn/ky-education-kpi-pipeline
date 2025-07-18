#!/usr/bin/env python3
"""
ETL Runner - Orchestrator CLI for the Equity Scorecard ETL Pipeline
"""
import sys
import argparse
from pathlib import Path
from ruamel.yaml import YAML
import importlib.util


def load_config(config_path: Path) -> dict:
    """Load YAML configuration file."""
    yaml = YAML(typ="safe")
    with open(config_path) as f:
        return yaml.load(f)


def run_etl_module(module_name: str, raw_dir: Path, proc_dir: Path, cfg: dict) -> None:
    """Dynamically import and run an ETL module's transform function."""
    module_path = Path(__file__).parent / "etl" / f"{module_name}.py"
    
    if not module_path.exists():
        print(f"Warning: ETL module {module_name} not found at {module_path}")
        return
    
    # Dynamic import
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    
    # Run transform function
    print(f"Running {module_name} transform...")
    module.transform(raw_dir, proc_dir, cfg)


def main():
    parser = argparse.ArgumentParser(description="Run Equity Scorecard ETL Pipeline")
    parser.add_argument(
        "--config", 
        type=Path, 
        default="config/mappings.yaml",
        help="Path to configuration file"
    )
    parser.add_argument(
        "--draft", 
        action="store_true",
        help="Draft mode - generate initial ETL logic from raw data"
    )
    
    args = parser.parse_args()
    
    # Setup paths
    base_dir = Path(__file__).parent
    raw_dir = base_dir / "data" / "raw"
    proc_dir = base_dir / "data" / "processed"
    kpi_dir = base_dir / "kpi"
    
    # Ensure directories exist
    proc_dir.mkdir(parents=True, exist_ok=True)
    
    # Load configuration
    config_path = base_dir / args.config
    if not config_path.exists():
        print(f"Config file not found: {config_path}")
        sys.exit(1)
    
    config = load_config(config_path)
    
    if args.draft:
        print("Draft mode: Analyzing raw data and generating ETL logic...")
        # TODO: Implement draft mode logic
        print("Draft mode not yet implemented")
        return
    
    # Run ETL modules
    for source_name, source_config in config.get("sources", {}).items():
        run_etl_module(source_name, raw_dir, proc_dir, source_config)
    
    # Combine all processed files
    print("Combining processed files into master KPI file...")
    from kpi.combine import combine_all
    combine_all(proc_dir, kpi_dir / "kpi_master.csv")
    
    print("ETL pipeline completed successfully!")


if __name__ == "__main__":
    main()