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
from etl.constants import KPI_COLUMNS
from pathlib import Path
from types import ModuleType
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
    required_columns = KPI_COLUMNS

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
    chunk_size: int = 10000,
) -> None:
    """Combine all processed KPI CSV files into a master dataset using chunked processing.

    Parameters
    ----------
    proc_dir : Path
        Directory containing per-source processed KPI CSV files.
    output_csv_path : Path
        Path for the combined CSV output.
    chunk_size : int, optional
        Number of rows to process in each chunk (default: 10000).
    """
    import csv
    
    # Get list of CSV files to process (skip audit files)
    csv_files = [f for f in proc_dir.glob("*.csv") if "audit" not in f.name]
    
    if not csv_files:
        print("  Warning: No valid KPI files found; creating empty master file.")
        # Create empty file with correct KPI schema
        empty_df = pd.DataFrame(columns=KPI_COLUMNS)
        empty_df.to_csv(output_csv_path, index=False)
        return

    total_rows = 0
    files_processed = 0

    # Write header to output file first
    with open(output_csv_path, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
        writer.writerow(KPI_COLUMNS)
        
        # Process each CSV file
        for csv_file in csv_files:
            print(f"  Processing {csv_file.name}")
            
            try:
                with open(csv_file, 'r', newline='', encoding='utf-8') as infile:
                    reader = csv.reader(infile)
                    
                    # Read and validate header
                    try:
                        header = next(reader)
                    except StopIteration:
                        print(f"  Warning: Empty file: {csv_file.name}")
                        continue
                    
                    # Basic header validation
                    if not header or len(header) < len(KPI_COLUMNS):
                        print(f"  Skipping {csv_file.name} - invalid header")
                        continue
                    
                    # Process data rows in chunks
                    file_rows = 0
                    chunk = []
                    chunks_written = 0
                    
                    for row in reader:
                        if row:  # Skip empty rows
                            chunk.append(row)
                            
                            # Write chunk when it reaches chunk_size
                            if len(chunk) >= chunk_size:
                                writer.writerows(chunk)
                                file_rows += len(chunk)
                                chunks_written += 1
                                chunk = []
                                
                                # Log progress every 10 chunks (100k rows)
                                if chunks_written % 10 == 0:
                                    print(f"    Processed {file_rows:,} rows ({chunks_written} chunks)")
                    
                    # Write remaining rows in final chunk
                    if chunk:
                        writer.writerows(chunk)
                        file_rows += len(chunk)
                        chunks_written += 1
                    
                    total_rows += file_rows
                    files_processed += 1
                    print(f"  Added {file_rows:,} KPI rows from {csv_file.name}")
                    
            except Exception as e:
                print(f"  Error processing {csv_file.name}: {e}")
                continue

    print(f"  Combined {files_processed} KPI sources into {output_csv_path}")
    print(f"  Master KPI file: {total_rows:,} rows")


def _load_etl_module(module_name: str, module_path: Path) -> ModuleType:
    """Load ``module_path`` as ``module_name`` and register it first.

    ``spec_from_file_location`` does not add the module to ``sys.modules``.
    Pydantic resolves postponed annotations (``from __future__ import
    annotations``) by looking up the class's module there. Without that
    entry, a model such as ``Config`` stays unfinished and the first
    ``Config(**cfg)`` raises ``PydanticUserError`` because ``Dict`` is
    undefined. Inserting the module before execution matches the import
    system, so the class body can resolve names from this module's globals.
    """
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load ETL module {module_name} from {module_path}")

    module = importlib.util.module_from_spec(spec)
    previous = sys.modules.get(module_name)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        if previous is None:
            sys.modules.pop(module_name, None)
        else:
            sys.modules[module_name] = previous
        raise
    return module


def run_etl_module(
    module_name: str,
    raw_dir: Path,
    proc_dir: Path,
    cfg: dict,
    *,
    module_path: Optional[Path] = None,
) -> None:
    """Dynamically import and run an ETL module's transform function."""
    if module_path is None:
        module_path = Path(__file__).parent / "etl" / f"{module_name}.py"

    if not module_path.exists():
        print(f"Warning: ETL module {module_name} not found at {module_path}")
        return

    print(f"\n🔄 Running {module_name} ETL pipeline...")
    print(f"   Source: {raw_dir / module_name}")
    print(f"   Output: {proc_dir}")
    
    try:
        module = _load_etl_module(module_name, module_path)
        module.transform(raw_dir, proc_dir, cfg)
        print(f"✅ Completed {module_name} pipeline")
        
    except Exception as e:
        print(f"❌ Error in {module_name} pipeline: {e}")
        logger.error(f"Pipeline {module_name} failed", exc_info=True)
        # Continue with other pipelines rather than failing completely


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
    parser.add_argument(
        "--skip-etl",
        action="store_true",
        help="Skip all ETL pipelines and only run the combine function",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging from ETL pipelines",
    )

    args = parser.parse_args()

    # Configure logging to show ETL pipeline output
    log_level = logging.INFO if args.verbose else logging.WARNING
    logging.basicConfig(
        level=log_level,
        format='%(levelname)s: %(message)s',
        handlers=[logging.StreamHandler()]
    )

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

    # Run ETL modules unless skip-etl flag is set
    if not args.skip_etl:
        sources = config.get("sources", {})
        total_sources = len(sources)
        print(f"\n🚀 Starting ETL pipeline processing ({total_sources} sources)")
        
        for i, (source_name, source_config) in enumerate(sources.items(), 1):
            print(f"\n📊 Pipeline {i}/{total_sources}: {source_name}")
            run_etl_module(source_name, raw_dir, proc_dir, source_config)
            
        print(f"\n🎉 Completed all {total_sources} ETL pipelines")
    else:
        print("Skipping ETL pipelines due to --skip-etl flag")

    # Combine all processed files
    print("\n📁 Combining processed files into master KPI file...")
    combine_kpi_files(
        proc_dir,
        kpi_dir / "kpi_master.csv",
    )

    print("\n🎯 ETL pipeline completed successfully!")


if __name__ == "__main__":
    main()
