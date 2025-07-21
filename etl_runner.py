#!/usr/bin/env python3
# Copyright 2025 Kentucky Open Government Coalition
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
ETL Runner - Kentucky Education Data Pipeline Orchestrator

Processes Kentucky Department of Education CSV exports into standardized
KPI format for multi-year educational performance reporting.
"""
import sys
import argparse
from pathlib import Path
from typing import Optional
from ruamel.yaml import YAML
import importlib.util
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def load_config(config_path: Path) -> dict:
    """Load YAML configuration file."""
    yaml = YAML(typ="safe")
    with open(config_path) as f:
        return yaml.load(f)


def validate_kpi_format(df: pd.DataFrame, source_name: str) -> bool:
    """Validate that dataframe is in correct KPI format."""
    required_columns = [
        "district",
        "school_id",
        "school_name",
        "year",
        "student_group",
        "metric",
        "value",
        "suppressed",
        "source_file",
        "last_updated",
    ]

    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        logger.warning(
            f"File {source_name} missing required KPI columns: {missing_columns}"
        )
        return False

    # Check if 'value' column is numeric
    if not pd.api.types.is_numeric_dtype(df["value"]):
        logger.warning(f"File {source_name} has non-numeric 'value' column")
        return False

    return True


def combine_kpi_files(
    proc_dir: Path,
    output_csv_path: Path,
    output_parquet_path: Optional[Path] = None,
) -> None:
    """Combine all processed KPI CSV files into a master dataset.

    Parameters
    ----------
    proc_dir : Path
        Directory containing per-source processed KPI CSV files.
    output_csv_path : Path
        Path for the combined CSV output.
    output_parquet_path : Optional[Path], optional
        If provided, the combined dataframe is also written to this path as a
        parquet file. When omitted, a `.parquet` file will be created next to
        ``output_csv_path``.
    """
    kpi_dfs = []

    # Only process main KPI files, skip audit files
    for csv_file in proc_dir.glob("*.csv"):
        if "audit" in csv_file.name:
            continue

        print(f"  Processing {csv_file.name}")

        try:
            df = pd.read_csv(csv_file)

            if df.empty:
                print(f"  Warning: Empty file: {csv_file.name}")
                continue

            # Validate KPI format
            if not validate_kpi_format(df, csv_file.name):
                print(f"  Skipping {csv_file.name} - invalid KPI format")
                continue

            # Ensure source_file is populated
            if "source_file" not in df.columns or df["source_file"].isna().all():
                df["source_file"] = csv_file.name

            kpi_dfs.append(df)
            print(f"  Added {len(df):,} KPI rows from {csv_file.name}")

        except Exception as e:
            print(f"  Error processing {csv_file.name}: {e}")
            continue

    if not kpi_dfs:
        print("  Warning: No valid KPI files found; creating empty master file.")
        # Create empty file with correct KPI schema
        empty_df = pd.DataFrame(
            columns=[
                "district",
                "school_id",
                "school_name",
                "year",
                "student_group",
                "metric",
                "value",
                "suppressed",
                "source_file",
                "last_updated",
            ]
        )
        empty_df.to_csv(output_csv_path, index=False)
        parquet_path = (
            output_parquet_path
            if output_parquet_path
            else output_csv_path.with_suffix(".parquet")
        )
        empty_df.to_parquet(parquet_path, index=False)
        return

    # Combine all KPI dataframes
    master_df = pd.concat(kpi_dfs, ignore_index=True, sort=False)

    # Ensure consistent column order
    kpi_columns = [
        "district",
        "school_id",
        "school_name",
        "year",
        "student_group",
        "metric",
        "value",
        "suppressed",
        "source_file",
        "last_updated",
    ]

    # Only include columns that exist
    available_columns = [col for col in kpi_columns if col in master_df.columns]
    master_df = master_df[available_columns]

    # Sort by key fields for consistent output
    sort_columns = ["district", "school_name", "year", "student_group", "metric"]
    existing_sort_columns = [col for col in sort_columns if col in master_df.columns]
    if existing_sort_columns:
        master_df = master_df.sort_values(existing_sort_columns).reset_index(drop=True)

    # Write master KPI file
    master_df.to_csv(output_csv_path, index=False)
    parquet_path = (
        output_parquet_path
        if output_parquet_path
        else output_csv_path.with_suffix(".parquet")
    )
    master_df.to_parquet(parquet_path, index=False)

    print(f"  Combined {len(kpi_dfs)} KPI sources into {output_csv_path}")
    print(
        f"  Master KPI file: {len(master_df):,} rows, {len(master_df.columns)} columns"
    )


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
    parser = argparse.ArgumentParser(
        description="Run Kentucky Education Data ETL Pipeline"
    )
    parser.add_argument(
        "--config",
        type=Path,
        default="config/mappings.yaml",
        help="Path to configuration file",
    )
    parser.add_argument(
        "--draft",
        action="store_true",
        help="Draft mode - generate initial ETL logic from raw data",
    )

    args = parser.parse_args()

    # Setup paths
    base_dir = Path(__file__).parent
    raw_dir = base_dir / "data" / "raw"
    proc_dir = base_dir / "data" / "processed"
    kpi_dir = base_dir / "data" / "kpi"

    # Ensure directories exist
    proc_dir.mkdir(parents=True, exist_ok=True)
    kpi_dir.mkdir(parents=True, exist_ok=True)

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
    combine_kpi_files(
        proc_dir,
        kpi_dir / "kpi_master.csv",
        kpi_dir / "kpi_master.parquet",
    )

    print("ETL pipeline completed successfully!")


if __name__ == "__main__":
    main()
