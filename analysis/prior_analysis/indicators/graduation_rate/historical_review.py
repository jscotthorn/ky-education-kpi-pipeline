#!/usr/bin/env python3
"""
Historical Data Review for Graduation Rate Prior Specification

This script implements Stage 1 (Empirical Foundation) of the hybrid approach
outlined in PRIOR_SPECIFICATION_GUIDE.md. It analyzes historical graduation
rate data from Kentucky schools to establish baseline variance estimates for
Bayesian hierarchical model priors.

Data Sources:
    - graduation_analysis_{student_group}.csv: Merged dataset with school-level graduation rates
    - Years analyzed: 2022-23 (year=2023) and 2023-24 (year=2024)

Outputs:
    1. Console report of empirical statistics
    2. CSV file with detailed estimates
    3. Recommended prior parameters for expert review

Usage:
    python historical_review.py                               # Default (all_students)
    python historical_review.py --student-group african_american
    python historical_review.py --all-groups                  # Run for all target groups

Author: FCPS Equity Council Analysis Team
Date: November 2025
"""

import argparse
import sys
from pathlib import Path
from typing import Dict, Any
from dataclasses import dataclass
from datetime import date

import pandas as pd
import numpy as np

# Add config directory to path for imports
CONFIG_DIR = Path(__file__).parent.parent.parent.parent / "config"
sys.path.insert(0, str(CONFIG_DIR))
from student_groups import ALL_GROUP_SLUGS, TARGET_GROUP_SLUGS, get_student_group

# Indicator metadata for aggregation across models
INDICATOR_ID = 'graduation_rate'
INDICATOR_NAME = 'Graduation Rate (4-Year)'
ANALYSIS_YEARS = [2023, 2024]  # year=2023 is 2022-23, year=2024 is 2023-24
ANALYSIS_FILE_BASE = 'graduation_analysis'  # graduation_analysis_{student_group}.csv


@dataclass
class VarianceComponents:
    """Stores estimated variance components from historical data."""
    state_mean: float
    state_sd: float
    between_district_sd: float
    within_district_sd: float
    year_to_year_sd: float
    n_schools: int
    n_districts: int
    n_observations: int


def load_historical_data(data_path: Path, years: list[int]) -> pd.DataFrame:
    """
    Load and filter graduation analysis data for specified years.

    Args:
        data_path: Path to graduation_analysis.csv
        years: List of years to include (e.g., [2023, 2024])

    Returns:
        Filtered DataFrame (deduplicated by school_id and year)
    """
    df = pd.read_csv(data_path)
    df_filtered = df[df['year'].isin(years)].copy()

    # Basic validation
    assert len(df_filtered) > 0, f"No data found for years {years}"
    assert 'graduation_rate' in df_filtered.columns, "Missing graduation_rate column"
    assert 'district' in df_filtered.columns, "Missing district column"
    assert 'school_id' in df_filtered.columns, "Missing school_id column"

    # Deduplicate by school_id and year (keep first row, values should be identical)
    n_before = len(df_filtered)
    df_filtered = df_filtered.drop_duplicates(subset=['school_id', 'year'], keep='first')
    n_after = len(df_filtered)
    if n_before != n_after:
        print(f"  Deduplicated: {n_before} → {n_after} rows (removed {n_before - n_after} duplicates)")

    return df_filtered


def compute_state_level_statistics(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Compute state-level graduation rate statistics.

    Returns dict with:
        - mean, median, sd
        - percentiles (2.5, 5, 10, 25, 50, 75, 90, 95, 97.5)
        - min, max
    """
    rates = df['graduation_rate']

    percentiles = rates.quantile([0.025, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.975])

    return {
        'mean': rates.mean(),
        'median': rates.median(),
        'sd': rates.std(),
        'min': rates.min(),
        'max': rates.max(),
        'percentiles': percentiles.to_dict(),
        'n': len(rates)
    }


def compute_between_district_variation(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Compute variation between district average graduation rates.

    This measures how much districts differ from each other in their
    typical graduation rates.
    """
    district_means = df.groupby('district')['graduation_rate'].mean()

    return {
        'mean_of_district_means': district_means.mean(),
        'sd_of_district_means': district_means.std(),
        'min_district_mean': district_means.min(),
        'max_district_mean': district_means.max(),
        'n_districts': len(district_means),
        'district_percentiles': district_means.quantile([0.025, 0.10, 0.25, 0.50, 0.75, 0.90, 0.975]).to_dict()
    }


def compute_within_district_variation(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Compute within-district (between-school) variation.

    This measures how much schools within the same district differ
    from their district average.
    """
    # Get district means
    district_means = df.groupby('district')['graduation_rate'].mean()
    district_means_df = district_means.reset_index()
    district_means_df.columns = ['district', 'district_mean']

    # Get school means (across years if multiple)
    school_means = df.groupby(['district', 'school_id'])['graduation_rate'].mean().reset_index()
    school_means.columns = ['district', 'school_id', 'school_mean']

    # Merge and compute deviations
    merged = school_means.merge(district_means_df, on='district')
    merged['school_deviation'] = merged['school_mean'] - merged['district_mean']

    # For multi-school districts, compute within-district SD
    multi_school_districts = df.groupby(['district', 'year']).filter(lambda x: len(x) > 1)
    if len(multi_school_districts) > 0:
        within_sd_by_district = multi_school_districts.groupby('district')['graduation_rate'].std()
        mean_within_sd = within_sd_by_district.mean()
        median_within_sd = within_sd_by_district.median()
    else:
        mean_within_sd = np.nan
        median_within_sd = np.nan

    return {
        'school_deviation_sd': merged['school_deviation'].std(),
        'mean_within_district_sd': mean_within_sd,
        'median_within_district_sd': median_within_sd,
        'n_schools': len(merged),
        'n_multi_school_districts': within_sd_by_district.count() if len(multi_school_districts) > 0 else 0
    }


def compute_year_to_year_variation(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Compute year-to-year variation within schools.

    This estimates the observation-level noise in graduation rates,
    accounting for true measurement variation and random fluctuation.

    Note: SD of changes = sqrt(2) * observation SD, so we divide by sqrt(2)
    to get the implied observation SD.
    """
    # Schools with data in both years
    schools_both_years = df.groupby('school_id').filter(lambda x: len(x) == 2)

    if len(schools_both_years) == 0:
        return {
            'n_schools_both_years': 0,
            'mean_change': np.nan,
            'sd_of_changes': np.nan,
            'implied_observation_sd': np.nan,
            'change_percentiles': {},
            'pct_large_changes': np.nan
        }

    # Pivot to wide format
    pivot = schools_both_years.pivot(index='school_id', columns='year', values='graduation_rate')
    years = sorted(pivot.columns)
    pivot['change'] = pivot[years[1]] - pivot[years[0]]

    # Implied observation SD: if change ~ N(0, 2*sigma^2), then sd(change) = sqrt(2) * sigma
    sd_changes = pivot['change'].std()
    implied_obs_sd = sd_changes / np.sqrt(2)

    return {
        'n_schools_both_years': len(pivot),
        'mean_change': pivot['change'].mean(),
        'sd_of_changes': sd_changes,
        'implied_observation_sd': implied_obs_sd,
        'change_percentiles': pivot['change'].quantile([0.025, 0.10, 0.25, 0.50, 0.75, 0.90, 0.975]).to_dict(),
        'pct_large_changes': ((pivot['change'].abs() > 5).sum() / len(pivot)) * 100
    }


def compute_all_variance_components(df: pd.DataFrame) -> VarianceComponents:
    """
    Compute all variance components for prior specification.
    """
    state_stats = compute_state_level_statistics(df)
    district_stats = compute_between_district_variation(df)
    within_stats = compute_within_district_variation(df)
    temporal_stats = compute_year_to_year_variation(df)

    return VarianceComponents(
        state_mean=state_stats['mean'],
        state_sd=state_stats['sd'],
        between_district_sd=district_stats['sd_of_district_means'],
        within_district_sd=within_stats['school_deviation_sd'],
        year_to_year_sd=temporal_stats['implied_observation_sd'],
        n_schools=df['school_id'].nunique(),
        n_districts=df['district'].nunique(),
        n_observations=len(df)
    )


def recommend_priors(components: VarianceComponents, multiplier: float = 1.5) -> Dict[str, Dict[str, Any]]:
    """
    Translate variance components into recommended prior parameters.

    Following guidance from PRIOR_SPECIFICATION_GUIDE.md:
    - Use HalfNormal with scale = multiplier × empirical SD for variance parameters
    - Use Normal for state mean with reasonable uncertainty

    Args:
        components: Estimated variance components from historical data
        multiplier: Scale factor for variance priors (default 1.5, use 1.0 for tighter priors)
    """
    # Adjust state mean scale based on multiplier (roughly proportional)
    state_mean_scale = max(1.5, 2.5 * (multiplier / 1.5))

    return {
        'mu_state': {
            'distribution': 'Normal',
            'location': round(components.state_mean, 1),
            'scale': round(state_mean_scale, 1),
            'rationale': f'Empirical mean {components.state_mean:.1f}%, scale gives 95% CI of ~{2*state_mean_scale:.0f} points',
            '95_interval': [float(round(components.state_mean - 2*state_mean_scale, 1)), float(round(min(100, components.state_mean + 2*state_mean_scale), 1))]
        },
        'sigma_district': {
            'distribution': 'HalfNormal',
            'scale': round(components.between_district_sd * multiplier, 1),
            'empirical_sd': round(components.between_district_sd, 2),
            'rationale': f'Scale = {multiplier}x empirical between-district SD'
        },
        'sigma_school': {
            'distribution': 'HalfNormal',
            'scale': round(components.within_district_sd * multiplier, 1),
            'empirical_sd': round(components.within_district_sd, 2),
            'rationale': f'Scale = {multiplier}x empirical within-district school deviation SD'
        },
        'sigma_y': {
            'distribution': 'HalfNormal',
            'scale': round(components.year_to_year_sd * multiplier, 1),
            'empirical_sd': round(components.year_to_year_sd, 2),
            'rationale': f'Scale = {multiplier}x implied observation SD from year-to-year changes'
        }
    }


def print_report(df: pd.DataFrame, components: VarianceComponents, priors: Dict) -> None:
    """Print formatted report to console."""

    years = sorted(df['year'].unique())
    year_labels = [f"{y-1}-{str(y)[-2:]}" for y in years]

    print("=" * 80)
    print("HISTORICAL DATA REVIEW FOR GRADUATION RATE PRIOR SPECIFICATION")
    print(f"School Years: {', '.join(year_labels)}")
    print("=" * 80)

    # Data overview
    print(f"\n📊 DATA OVERVIEW")
    print(f"Total observations: {components.n_observations}")
    print(f"Unique schools: {components.n_schools}")
    print(f"Unique districts: {components.n_districts}")
    for year in years:
        n = len(df[df['year'] == year])
        print(f"  - {year-1}-{str(year)[-2:]}: {n} schools")

    # Section 1: State-level statistics
    print("\n" + "=" * 80)
    print("SECTION 1: STATE-LEVEL STATISTICS")
    print("=" * 80)

    state_stats = compute_state_level_statistics(df)
    print(f"\nState graduation rate:")
    print(f"  Mean:   {state_stats['mean']:.2f}%")
    print(f"  Median: {state_stats['median']:.2f}%")
    print(f"  SD:     {state_stats['sd']:.2f}%")
    print(f"  Range:  {state_stats['min']:.1f}% - {state_stats['max']:.1f}%")

    print(f"\nPercentile distribution:")
    for pct, val in state_stats['percentiles'].items():
        print(f"  {int(pct*100):3d}th percentile: {val:.1f}%")

    # Section 2: Between-district variation
    print("\n" + "=" * 80)
    print("SECTION 2: BETWEEN-DISTRICT VARIATION")
    print("=" * 80)

    district_stats = compute_between_district_variation(df)
    print(f"\nDistrict-level graduation rates (n={district_stats['n_districts']} districts):")
    print(f"  Mean of district means: {district_stats['mean_of_district_means']:.2f}%")
    print(f"  SD of district means:   {district_stats['sd_of_district_means']:.2f}%")
    print(f"  Range: {district_stats['min_district_mean']:.1f}% - {district_stats['max_district_mean']:.1f}%")

    sd = district_stats['sd_of_district_means']
    mean = district_stats['mean_of_district_means']
    print(f"\n95% of districts expected within: {mean:.0f}% ± {1.96*sd:.1f}%")

    # Section 3: Within-district variation
    print("\n" + "=" * 80)
    print("SECTION 3: WITHIN-DISTRICT (BETWEEN-SCHOOL) VARIATION")
    print("=" * 80)

    within_stats = compute_within_district_variation(df)
    print(f"\nWithin-district school variation:")
    print(f"  School deviation from district mean SD: {within_stats['school_deviation_sd']:.2f}%")
    if not np.isnan(within_stats['mean_within_district_sd']):
        print(f"  Mean within-district SD: {within_stats['mean_within_district_sd']:.2f}%")
        print(f"  (for {within_stats['n_multi_school_districts']} multi-school districts)")

    # Section 4: Year-to-year variation
    print("\n" + "=" * 80)
    print("SECTION 4: YEAR-TO-YEAR VARIATION (OBSERVATION NOISE)")
    print("=" * 80)

    temporal_stats = compute_year_to_year_variation(df)
    print(f"\nSchools with data in both years: {temporal_stats['n_schools_both_years']}")
    print(f"\nYear-to-year changes:")
    print(f"  Mean change:           {temporal_stats['mean_change']:+.2f}%")
    print(f"  SD of changes:         {temporal_stats['sd_of_changes']:.2f}%")
    print(f"  Implied observation SD: {temporal_stats['implied_observation_sd']:.2f}%")
    print(f"  (computed as SD(changes) / √2)")

    if temporal_stats['change_percentiles']:
        print(f"\nChange distribution:")
        for pct, val in temporal_stats['change_percentiles'].items():
            print(f"  {int(pct*100):3d}th percentile: {val:+.1f}%")
        pct_large = temporal_stats['pct_large_changes']
        if not np.isnan(pct_large):
            print(f"\nSchools with |change| > 5pp: {pct_large:.1f}%")

    # Section 5: Summary and recommendations
    print("\n" + "=" * 80)
    print("SECTION 5: RECOMMENDED PRIORS FOR MODEL SPECIFICATION")
    print("=" * 80)

    print(f"""
EMPIRICAL ESTIMATES:
--------------------
1. State Mean (μ_state):
   - Empirical mean: {components.state_mean:.1f}%

2. Between-District SD (σ_district):
   - Empirical SD: {components.between_district_sd:.2f}%

3. Within-District/School SD (σ_school):
   - Empirical SD: {components.within_district_sd:.2f}%

4. Observation Noise SD (σ_y):
   - Implied SD: {components.year_to_year_sd:.2f}%
""")

    print("RECOMMENDED PRIOR PARAMETERS:")
    print("-" * 50)
    for param, spec in priors.items():
        print(f"\n{param}:")
        print(f"  Distribution: {spec['distribution']}")
        if 'location' in spec:
            print(f"  Location: {spec['location']}")
        print(f"  Scale: {spec['scale']}")
        print(f"  Rationale: {spec['rationale']}")
        if '95_interval' in spec:
            print(f"  95% prior interval: {spec['95_interval']}")


def save_results(components: VarianceComponents, priors: Dict, output_dir: Path,
                 student_group: str) -> None:
    """Save results to CSV for documentation and aggregation across indicators."""

    analysis_date = date.today().isoformat()
    years_str = ', '.join([f"{y-1}-{str(y)[-2:]}" for y in ANALYSIS_YEARS])
    group_info = get_student_group(student_group)
    student_group_name = group_info.name

    # Variance components with indicator metadata
    components_df = pd.DataFrame([{
        'indicator_id': INDICATOR_ID,
        'indicator_name': INDICATOR_NAME,
        'student_group': student_group,
        'student_group_name': student_group_name,
        'analysis_date': analysis_date,
        'school_years': years_str,
        'parameter': 'state_mean',
        'empirical_value': components.state_mean,
        'unit': 'percent',
        'n_schools': components.n_schools,
        'n_districts': components.n_districts,
        'n_observations': components.n_observations
    }, {
        'indicator_id': INDICATOR_ID,
        'indicator_name': INDICATOR_NAME,
        'student_group': student_group,
        'student_group_name': student_group_name,
        'analysis_date': analysis_date,
        'school_years': years_str,
        'parameter': 'state_sd',
        'empirical_value': components.state_sd,
        'unit': 'percent',
        'n_schools': components.n_schools,
        'n_districts': components.n_districts,
        'n_observations': components.n_observations
    }, {
        'indicator_id': INDICATOR_ID,
        'indicator_name': INDICATOR_NAME,
        'student_group': student_group,
        'student_group_name': student_group_name,
        'analysis_date': analysis_date,
        'school_years': years_str,
        'parameter': 'between_district_sd',
        'empirical_value': components.between_district_sd,
        'unit': 'percent',
        'n_schools': components.n_schools,
        'n_districts': components.n_districts,
        'n_observations': components.n_observations
    }, {
        'indicator_id': INDICATOR_ID,
        'indicator_name': INDICATOR_NAME,
        'student_group': student_group,
        'student_group_name': student_group_name,
        'analysis_date': analysis_date,
        'school_years': years_str,
        'parameter': 'within_district_sd',
        'empirical_value': components.within_district_sd,
        'unit': 'percent',
        'n_schools': components.n_schools,
        'n_districts': components.n_districts,
        'n_observations': components.n_observations
    }, {
        'indicator_id': INDICATOR_ID,
        'indicator_name': INDICATOR_NAME,
        'student_group': student_group,
        'student_group_name': student_group_name,
        'analysis_date': analysis_date,
        'school_years': years_str,
        'parameter': 'year_to_year_sd',
        'empirical_value': components.year_to_year_sd,
        'unit': 'percent',
        'n_schools': components.n_schools,
        'n_districts': components.n_districts,
        'n_observations': components.n_observations
    }])

    components_path = output_dir / 'variance_components.csv'
    components_df.to_csv(components_path, index=False)
    print(f"\nSaved variance components to: {components_path}")

    # Prior recommendations with indicator metadata
    priors_data = []
    for param, spec in priors.items():
        row = {
            'indicator_id': INDICATOR_ID,
            'indicator_name': INDICATOR_NAME,
            'student_group': student_group,
            'student_group_name': student_group_name,
            'analysis_date': analysis_date,
            'school_years': years_str,
            'parameter': param,
            'distribution': spec['distribution'],
            'scale': spec['scale'],
            'rationale': spec['rationale']
        }
        if 'location' in spec:
            row['location'] = spec['location']
        if 'empirical_sd' in spec:
            row['empirical_sd'] = spec['empirical_sd']
        if '95_interval' in spec:
            row['prior_95_lower'] = spec['95_interval'][0]
            row['prior_95_upper'] = spec['95_interval'][1]
        priors_data.append(row)

    priors_df = pd.DataFrame(priors_data)
    priors_path = output_dir / 'recommended_priors.csv'
    priors_df.to_csv(priors_path, index=False)
    print(f"Saved prior recommendations to: {priors_path}")


def run_for_group(student_group: str, multiplier: float = 1.5) -> dict:
    """Run prior analysis for a single student group.

    Args:
        student_group: Student group slug (e.g., 'all_students', 'african_american')
        multiplier: Scale factor for variance priors (default 1.5, use 1.0 for tighter priors)
    """

    # Paths - script is in prior_analysis/indicators/graduation_rate/
    script_dir = Path(__file__).parent
    prior_analysis_dir = script_dir.parent.parent  # prior_analysis/
    project_root = prior_analysis_dir.parent.parent  # ky-education-kpi-pipeline/

    # Data path: graduation_analysis_{student_group}.csv
    data_filename = f"{ANALYSIS_FILE_BASE}_{student_group}.csv"
    data_path = project_root / 'analysis' / 'datasets' / data_filename

    # Output to analysis/outputs/prior_analysis/{indicator_id}/{student_group}/
    output_dir = project_root / 'analysis' / 'outputs' / 'prior_analysis' / INDICATOR_ID / student_group
    output_dir.mkdir(parents=True, exist_ok=True)

    group_info = get_student_group(student_group)

    print("=" * 60)
    print(f"PRIOR ANALYSIS: {INDICATOR_NAME}")
    print(f"Student Group: {group_info.name}")
    print(f"Variance Multiplier: {multiplier}x")
    print("=" * 60)

    print(f"\nLoading data from: {data_path}")
    df = load_historical_data(data_path, ANALYSIS_YEARS)

    # Compute variance components
    components = compute_all_variance_components(df)

    # Get prior recommendations
    priors = recommend_priors(components, multiplier=multiplier)

    # Print report
    print_report(df, components, priors)

    # Save results
    save_results(components, priors, output_dir, student_group)

    return {
        'components': components,
        'priors': priors
    }


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description=f"Run prior analysis for {INDICATOR_NAME}"
    )
    parser.add_argument(
        '--student-group',
        type=str,
        default='all_students',
        choices=ALL_GROUP_SLUGS,
        help=f"Student group to analyze. Choices: {ALL_GROUP_SLUGS}"
    )
    parser.add_argument(
        '--all-groups',
        action='store_true',
        help="Run for all student groups (all_students + target demographics)"
    )
    parser.add_argument(
        '--multiplier',
        type=float,
        default=1.0,
        help="Scale factor for variance priors (default: 1.0, was 1.5)"
    )
    args = parser.parse_args()

    # Determine which groups to run
    if args.all_groups:
        groups_to_run = ['all_students'] + TARGET_GROUP_SLUGS
        print(f"\nRunning for {len(groups_to_run)} student groups: {groups_to_run}\n")
    else:
        groups_to_run = [args.student_group]

    print(f"Prior variance multiplier: {args.multiplier}x empirical SD")

    # Run for each group
    results = {}
    for i, group in enumerate(groups_to_run, 1):
        if len(groups_to_run) > 1:
            print(f"\n{'#' * 60}")
            print(f"# GROUP {i}/{len(groups_to_run)}: {group}")
            print(f"{'#' * 60}\n")
        results[group] = run_for_group(group, multiplier=args.multiplier)

    if len(groups_to_run) > 1:
        print(f"\n{'=' * 60}")
        print(f"ALL GROUPS COMPLETE: {len(results)} prior analyses run")
        print("=" * 60)

    print("\n" + "=" * 80)
    print("NEXT STEPS (per PRIOR_SPECIFICATION_GUIDE.md):")
    print("=" * 80)
    print("""
1. EXPERT REVIEW: Present these findings to domain experts for validation
   - Do the district/school variation estimates match their experience?
   - Are there post-COVID effects that should adjust these estimates?

2. PRIOR PREDICTIVE CHECK: Simulate from proposed priors to verify
   - 95% of simulated rates should fall in [~75%, 100%]
   - No predictions exceeding 100%

3. SENSITIVITY ANALYSIS: Test model with 3 prior specifications
   - Baseline (these recommendations)
   - Tighter (0.75x variance scales)
   - Looser (1.5x variance scales)
""")

    return results if len(results) > 1 else list(results.values())[0]


if __name__ == '__main__':
    main()
