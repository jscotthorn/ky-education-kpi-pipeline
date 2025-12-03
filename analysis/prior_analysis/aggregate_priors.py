#!/usr/bin/env python3
"""
Aggregate Prior Recommendations Across All Indicators and Student Groups

This script combines all individual indicator prior recommendations into
a single summary file for cross-indicator comparison and documentation.

Directory Structure (Updated for Student Groups):
    analysis/outputs/prior_analysis/
    ├── graduation_rate/
    │   ├── all_students/
    │   │   ├── variance_components.csv
    │   │   └── recommended_priors.csv
    │   ├── african_american/
    │   │   └── ...
    │   └── ... (other student groups)
    ├── chronic_absenteeism/
    │   └── ... (same structure)
    └── summary/
        ├── all_indicators_priors_summary.csv
        └── all_indicators_variance_components.csv

Usage:
    python aggregate_priors.py

Outputs:
    - summary/all_indicators_priors_summary.csv
    - summary/all_indicators_variance_components.csv
"""

from pathlib import Path
from datetime import date

import pandas as pd


def find_indicator_files(indicators_dir: Path) -> list:
    """
    Find all indicator/student_group directories and their output files.

    New structure: {indicator_id}/{student_group}/variance_components.csv

    Returns list of dicts with paths and metadata.
    """
    results = []

    if not indicators_dir.exists():
        return results

    # Skip 'summary' directory - that's for aggregated outputs
    skip_dirs = {'summary'}

    for indicator_dir in indicators_dir.iterdir():
        if not indicator_dir.is_dir():
            continue
        if indicator_dir.name in skip_dirs:
            continue

        indicator_id = indicator_dir.name

        # Check for student group subdirectories
        for subdir in indicator_dir.iterdir():
            if not subdir.is_dir():
                continue

            student_group = subdir.name
            priors_file = subdir / 'recommended_priors.csv'
            variance_file = subdir / 'variance_components.csv'

            if priors_file.exists() or variance_file.exists():
                results.append({
                    'indicator_id': indicator_id,
                    'student_group': student_group,
                    'dir': subdir,
                    'priors': priors_file if priors_file.exists() else None,
                    'variance': variance_file if variance_file.exists() else None
                })

    return results


def aggregate_files(file_entries: list, file_key: str) -> pd.DataFrame:
    """Concatenate all CSV files into a single DataFrame."""
    file_paths = [entry[file_key] for entry in file_entries if entry[file_key] is not None]

    if not file_paths:
        return pd.DataFrame()

    dfs = []
    for entry in file_entries:
        f = entry[file_key]
        if f is None:
            continue
        try:
            df = pd.read_csv(f)
            dfs.append(df)
            print(f"  Loaded: {entry['indicator_id']}/{entry['student_group']}/{f.name} ({len(df)} rows)")
        except Exception as e:
            print(f"  Warning: Could not load {f}: {e}")

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


def main():
    """Main execution function."""
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.parent  # ky-education-kpi-pipeline/

    # Outputs are in analysis/outputs/prior_analysis/
    outputs_base = project_root / 'analysis' / 'outputs' / 'prior_analysis'
    summary_dir = outputs_base / 'summary'

    # Ensure summary directory exists
    summary_dir.mkdir(parents=True, exist_ok=True)

    # Look for indicator outputs (not scripts)
    indicators_dir = outputs_base

    print("=" * 60)
    print("AGGREGATING PRIOR RECOMMENDATIONS ACROSS INDICATORS")
    print("=" * 60)

    # Find all indicator/student_group combinations
    file_entries = find_indicator_files(indicators_dir)

    if not file_entries:
        print("\nNo indicator directories found in:", indicators_dir)
        return

    # Group by indicator for display
    indicators = {}
    for entry in file_entries:
        ind = entry['indicator_id']
        if ind not in indicators:
            indicators[ind] = []
        indicators[ind].append(entry['student_group'])

    print(f"\nFound {len(indicators)} indicator(s) with {len(file_entries)} total combinations:")
    for indicator_id, groups in sorted(indicators.items()):
        print(f"  - {indicator_id}/ ({len(groups)} student groups)")
        for group in sorted(groups):
            print(f"      - {group}/")

    # Aggregate priors
    entries_with_priors = [e for e in file_entries if e['priors'] is not None]
    if entries_with_priors:
        print("\n" + "-" * 60)
        print("Aggregating prior recommendations...")
        priors_df = aggregate_files(entries_with_priors, 'priors')

        if len(priors_df) > 0:
            priors_df['aggregation_date'] = date.today().isoformat()

            # Sort by indicator, student_group, parameter
            sort_cols = ['indicator_id', 'student_group', 'parameter'] if 'student_group' in priors_df.columns else ['indicator_id', 'parameter']
            priors_df = priors_df.sort_values(sort_cols)

            output_path = summary_dir / 'all_indicators_priors_summary.csv'
            priors_df.to_csv(output_path, index=False)
            print(f"\nSaved: {output_path}")
            print(f"Total rows: {len(priors_df)}")

            print("\nSummary by indicator/student_group:")
            if 'student_group' in priors_df.columns:
                summary = priors_df.groupby(['indicator_id', 'student_group']).agg({
                    'parameter': 'count',
                    'analysis_date': 'first'
                }).rename(columns={'parameter': 'n_parameters'})
            else:
                summary = priors_df.groupby('indicator_id').agg({
                    'parameter': 'count',
                    'analysis_date': 'first'
                }).rename(columns={'parameter': 'n_parameters'})
            print(summary.to_string())

    # Aggregate variance components
    entries_with_variance = [e for e in file_entries if e['variance'] is not None]
    if entries_with_variance:
        print("\n" + "-" * 60)
        print("Aggregating variance components...")
        variance_df = aggregate_files(entries_with_variance, 'variance')

        if len(variance_df) > 0:
            variance_df['aggregation_date'] = date.today().isoformat()

            # Sort by indicator, student_group, parameter
            sort_cols = ['indicator_id', 'student_group', 'parameter'] if 'student_group' in variance_df.columns else ['indicator_id', 'parameter']
            variance_df = variance_df.sort_values(sort_cols)

            output_path = summary_dir / 'all_indicators_variance_components.csv'
            variance_df.to_csv(output_path, index=False)
            print(f"\nSaved: {output_path}")
            print(f"Total rows: {len(variance_df)}")

    print("\n" + "=" * 60)
    print("AGGREGATION COMPLETE")
    print("=" * 60)


if __name__ == '__main__':
    main()
