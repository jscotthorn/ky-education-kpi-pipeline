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
import os
import sys
import argparse
import tempfile
from etl.constants import KPI_COLUMNS
from pathlib import Path
from types import ModuleType
from typing import Optional
from ruamel.yaml import YAML
import importlib.util
import pandas as pd
import logging

logger = logging.getLogger(__name__)

# Issue #128. At 1aef27b the combined file had 1,813 duplicate keys on
# (district, school_id, school_name, year, student_group, metric): 1,810 in
# 2019, one in 2022, and two in 2023. Each group was a duplicate source row
# or a coversheet label the shared map had collapsed, and the source modules
# now keep one row under the decision-log rules. No group was left undecided,
# so this allowlist is empty. A later group that none of those rules covers
# is listed here, with a comment citing #128, and is not decided by this check.
DUPLICATE_KEY_COLUMNS = (
    "district",
    "school_id",
    "school_name",
    "year",
    "student_group",
    "metric",
)
DUPLICATE_KEY_ALLOWLIST: frozenset[tuple[str, ...]] = frozenset()
# 16-byte key for pandas.util.hash_pandas_object. Both passes of one run
# must use the same key. It is not a secret.
_DUPLICATE_KEY_HASH_KEY = "ky-kpi-key-hash!"


class DuplicateKpiKeyError(RuntimeError):
    """Raised when a combined KPI file repeats the pipeline key."""


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

    The master is written to a temporary file in the same directory, checked,
    and renamed into place. A failed check removes the temporary file and
    leaves any previous master untouched.

    Parameters
    ----------
    proc_dir : Path
        Directory containing per-source processed KPI CSV files.
    output_csv_path : Path
        Path for the combined CSV output.
    chunk_size : int, optional
        Number of rows to process in each chunk (default: 10000).
    """
    output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, tmp_name = tempfile.mkstemp(
        prefix=f".{output_csv_path.name}.",
        suffix=".partial",
        dir=output_csv_path.parent,
    )
    os.close(descriptor)
    tmp_path = Path(tmp_name)
    try:
        _write_combined_kpi_csv(proc_dir, tmp_path, chunk_size)
        _assert_no_duplicate_kpi_keys(tmp_path)
        os.replace(tmp_path, output_csv_path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


def _kpi_column_positions(header: list[str], source_name: str) -> list[int]:
    """Return source indexes for ``KPI_COLUMNS`` order.

    A processed file may list the same columns in another order. Rows are
    rewritten in ``KPI_COLUMNS`` order. A missing column is an error, not a
    skipped file: writing the row under the fixed header would put cells in
    the wrong fields.
    """
    index = {name: position for position, name in enumerate(header)}
    missing = [column for column in KPI_COLUMNS if column not in index]
    if missing:
        raise ValueError(
            f"{source_name} is missing KPI columns {missing}"
        )
    return [index[column] for column in KPI_COLUMNS]


def _align_kpi_row(row: list[str], positions: list[int], source_name: str) -> list[str]:
    """Pick cells in ``KPI_COLUMNS`` order. A short row is an error."""
    aligned: list[str] = []
    width = len(row)
    for position in positions:
        if position >= width:
            raise ValueError(
                f"{source_name} has a row shorter than its header"
            )
        aligned.append(row[position])
    return aligned


def _write_combined_kpi_csv(
    proc_dir: Path,
    output_csv_path: Path,
    chunk_size: int,
) -> None:
    """Write the combined CSV. The caller renames it after the key check."""
    import csv

    csv_files = [f for f in proc_dir.glob("*.csv") if "audit" not in f.name]

    if not csv_files:
        print("  Warning: No valid KPI files found; creating empty master file.")
        empty_df = pd.DataFrame(columns=KPI_COLUMNS)
        empty_df.to_csv(output_csv_path, index=False)
        return

    total_rows = 0
    files_processed = 0

    with open(output_csv_path, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC)
        writer.writerow(KPI_COLUMNS)

        for csv_file in csv_files:
            print(f"  Processing {csv_file.name}")

            try:
                with open(csv_file, "r", newline="", encoding="utf-8") as infile:
                    reader = csv.reader(infile)

                    try:
                        header = next(reader)
                    except StopIteration:
                        print(f"  Warning: Empty file: {csv_file.name}")
                        continue

                    positions = _kpi_column_positions(header, csv_file.name)

                    file_rows = 0
                    chunk: list[list[str]] = []
                    chunks_written = 0

                    for row in reader:
                        if not row:
                            continue
                        chunk.append(_align_kpi_row(row, positions, csv_file.name))

                        if len(chunk) >= chunk_size:
                            writer.writerows(chunk)
                            file_rows += len(chunk)
                            chunks_written += 1
                            chunk = []

                            if chunks_written % 10 == 0:
                                print(
                                    f"    Processed {file_rows:,} rows "
                                    f"({chunks_written} chunks)"
                                )

                    if chunk:
                        writer.writerows(chunk)
                        file_rows += len(chunk)
                        chunks_written += 1

                    total_rows += file_rows
                    files_processed += 1
                    print(f"  Added {file_rows:,} KPI rows from {csv_file.name}")

            except ValueError:
                raise
            except Exception as e:
                print(f"  Error processing {csv_file.name}: {e}")
                continue

    print(f"  Combined {files_processed} KPI sources into {output_csv_path}")
    print(f"  Master KPI file: {total_rows:,} rows")


def _hash_key_frame(frame: pd.DataFrame):
    """8-byte hash of each row's pipeline key.

    ``pandas.util.hash_pandas_object`` is the vectorized hash. The same
    ``hash_key`` is used when those hashes are confirmed against the text
    of the key. ``categorize=False`` avoids building a categorical of every
    distinct school name in the chunk.
    """
    hashed = pd.util.hash_pandas_object(
        frame.loc[:, list(DUPLICATE_KEY_COLUMNS)],
        index=False,
        categorize=False,
        hash_key=_DUPLICATE_KEY_HASH_KEY,
    )
    return hashed.to_numpy()


def _assert_no_duplicate_kpi_keys(output_csv_path: Path) -> None:
    """Fail the combine when a key is repeated and is not allowlisted.

    The key is ``DUPLICATE_KEY_COLUMNS``. Counts are stored as 8-byte
    hashes, one per row, then sorted. Only a hash that occurs more than
    once is read back as the real key, so a hash collision between two
    different keys is not reported as a duplicate. ``DUPLICATE_KEY_ALLOWLIST``
    is empty: the #128 groups are resolved in the source modules.
    """
    import numpy as np

    if not output_csv_path.is_file():
        raise DuplicateKpiKeyError(
            f"KPI master file is missing: {output_csv_path}"
        )
    if output_csv_path.stat().st_size == 0:
        raise DuplicateKpiKeyError(
            f"KPI master file is empty: {output_csv_path}"
        )

    key_columns = list(DUPLICATE_KEY_COLUMNS)
    try:
        reader = pd.read_csv(
            output_csv_path,
            dtype=str,
            keep_default_na=False,
            usecols=key_columns,
            chunksize=200_000,
        )
    except ValueError as exc:
        raise DuplicateKpiKeyError(
            f"{output_csv_path} is missing key columns ({exc})"
        ) from exc

    capacity = 1 << 20
    hashes = np.empty(capacity, dtype=np.uint64)
    offset = 0
    try:
        for chunk in reader:
            if chunk.empty:
                continue
            hashed = np.asarray(_hash_key_frame(chunk), dtype=np.uint64)
            count = int(hashed.size)
            if offset + count > hashes.size:
                grown = np.empty(
                    max(hashes.size * 2, offset + count), dtype=np.uint64
                )
                grown[:offset] = hashes[:offset]
                del hashes
                hashes = grown
            hashes[offset:offset + count] = hashed
            offset += count
            del hashed
    except ValueError as exc:
        raise DuplicateKpiKeyError(
            f"{output_csv_path} is missing key columns ({exc})"
        ) from exc

    if offset == 0:
        return

    exact = np.empty(offset, dtype=np.uint64)
    exact[:] = hashes[:offset]
    del hashes
    exact.sort()

    repeated = np.flatnonzero(exact[1:] == exact[:-1])
    if repeated.size == 0:
        return
    colliding = {int(value) for value in np.unique(exact[1:][repeated])}
    del exact

    counts: dict[tuple[str, ...], int] = {}
    confirm = pd.read_csv(
        output_csv_path,
        dtype=str,
        keep_default_na=False,
        usecols=key_columns,
        chunksize=200_000,
    )
    colliding_array = np.fromiter(colliding, dtype=np.uint64)
    for chunk in confirm:
        if chunk.empty:
            continue
        hashed = np.asarray(_hash_key_frame(chunk), dtype=np.uint64)
        mask = np.isin(hashed, colliding_array)
        if not bool(mask.any()):
            continue
        matched = chunk.iloc[np.flatnonzero(mask)].loc[:, key_columns]
        for row in matched.itertuples(index=False, name=None):
            key = tuple("" if part is None else str(part) for part in row)
            counts[key] = counts.get(key, 0) + 1

    duplicates = {
        key: count
        for key, count in counts.items()
        if count > 1 and key not in DUPLICATE_KEY_ALLOWLIST
    }
    if not duplicates:
        return

    lines = [
        f"Duplicate KPI keys in {output_csv_path}: {len(duplicates)} keys.",
        "Key is (district, school_id, school_name, year, student_group, metric).",
    ]
    for index, (key, count) in enumerate(duplicates.items()):
        if index >= 50:
            lines.append(f"  ... {len(duplicates) - 50} more keys")
            break
        district, school_id, school_name, year, student_group, metric = key
        lines.append(
            "  "
            f"{count} rows: district={district!r} school_id={school_id!r} "
            f"school_name={school_name!r} year={year!r} "
            f"student_group={student_group!r} metric={metric!r}"
        )
    raise DuplicateKpiKeyError("\n".join(lines))


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
