#!/usr/bin/env python3
"""
Generate data-driven report drafts for the Equity Scorecard Report.

This script reads KPI data and generates Markdown draft documents for each
indicator, providing a skeleton with all statistics pre-filled for committee
members to add narrative analysis.

Usage:
    python scripts/generate_report_drafts.py --county "Fayette County" --output reports/drafts
    python scripts/generate_report_drafts.py --county "Jefferson County" --output reports/drafts
    python scripts/generate_report_drafts.py --county "Fayette County" --indicator reading_3rd_grade
"""

import argparse
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# Indicator configuration: metric_name -> (display_name, is_reverse, school_level)
INDICATORS = {
    'reading_3rd_grade': {
        'metric': 'kentucky_summative_assessment_reading_proficient_distinguished_rate_grade_3',
        'display_name': '3rd Grade Reading Proficiency',
        'is_reverse': False,
        'school_level': 'Elementary',
        'unit': '%',
        'description': '''3rd Grade Reading Proficiency is a critical milestone. It marks the transition from learning to read to reading to learn, which is fundamental for success in all other subjects.

Students are assessed on reading comprehension and literacy skills as a component of the Kentucky Summative Assessment (KSA), aligned to Kentucky Academic Standards for Reading and Writing.'''
    },
    'math_8th_grade': {
        'metric': 'kentucky_summative_assessment_math_proficient_distinguished_rate_grade_8',
        'display_name': '8th Grade Math Proficiency',
        'is_reverse': False,
        'school_level': 'Middle',
        'unit': '%',
        'description': '''8th Grade Math Proficiency is a key indicator of future academic success.

Students are assessed on mathematics content including linear equations, functions, geometry, and statistics as a component of the Kentucky Summative Assessment (KSA), aligned to Kentucky Academic Standards for Mathematics. Performance contributes to school accountability ratings.'''
    },
    'kindergarten_readiness': {
        'metric': 'kindergarten_readiness_rate',
        'display_name': 'Kindergarten Readiness',
        'is_reverse': False,
        'school_level': 'Elementary',
        'unit': '%',
        'school_variation_terms': ('Highest readiness rate', 'Lowest readiness rate'),
        'description': '''Kindergarten Readiness is a tool that educators utilize to assess a child's development prior to school. This helps teachers tailor their learning plan for the year to help best support their students.

Students are assessed using the BRIGANCE screening tool, which measures five areas: academic skills, language, physical development, self-help, and social-emotional development. All students entering kindergarten are screened within the first 30 days of school.

**Note:** Kindergarten readiness is typically screened before a child ever attends the school where they enroll, and is not a measure of school quality. It reflects preparation students arrive with, not something the receiving school produced.'''
    },
    'graduation_rate': {
        'metric': 'graduation_rate_4_year',
        'display_name': '4-Year Graduation Rate',
        'is_reverse': False,
        'school_level': 'High',
        'unit': '%',
        'description': '''The 4-Year Graduation Rate is a critical measure of student success. It shows the percentage of students who complete high school in four years, which is essential for college and career readiness.

This metric tracks students from when they enter high school through graduation, adjusted for students who transfer in or out.''',
        # KDE reports 0.0% for FCPS alternative/support programs that don't graduate students
        # directly (School Classification A5/A7 rather than A1) — their enrolled students are
        # counted toward a home high school's rate instead. Included in school-level averages
        # they would misrepresent a data-reporting artifact as the district's lowest-performing
        # school, so School Variation should only compare the 6 accountability (A1) high schools.
        'school_variation_exclude': [
            'Martin Luther King Academy',
            'The Stables',
            'Family Care Center',
            'Steam Academy',
            'Success Academy',
            'Ridge Hospital Alt. High School',
            'Fayette County Learning Center',
            'Opportunity Middle College',
        ],
    },
    'postsecondary_readiness': {
        'metric': 'postsecondary_readiness_rate',
        'display_name': 'Postsecondary Readiness',
        'is_reverse': False,
        'school_level': 'High',
        'unit': '%',
        'description': '''Postsecondary Readiness measures whether students are prepared for college or career after high school. This includes meeting benchmarks in academics, career exploration, and college preparation.

Students must earn a diploma and meet at least one readiness measure, such as college entrance exam scores, dual credit courses, AP/IB exams, industry certifications, or approved work experiences.''',
        'extra_metrics': [
            {
                'metric': 'postsecondary_readiness_rate_with_bonus',
                'label': 'With High-Demand Bonus',
            },
        ],
        'data_note': '''**Data note:** KDE publishes two Postsecondary Readiness rates — a base rate (above) and a rate that adds a bonus for readiness measures in high-demand career sectors (shown above as "With High-Demand Bonus"). KDE revises this dataset after the accountability appeals window closes, so small movements between snapshots of the same school year are expected. See `notes/102--postsecondary-readiness-figure-reconciliation.md` in the KPI pipeline repo for the full reconciliation against FCPS-reported figures.''',
    },
    'postsecondary_enrollment': {
        'metric': 'postsecondary_enrollment_total_ky_college_rate',
        'display_name': 'Postsecondary Enrollment',
        'is_reverse': False,
        'school_level': 'High',
        'unit': '%',
        'description': '''Postsecondary Enrollment measures the percentage of students who enroll in Kentucky-based public or private colleges or other postsecondary programs after high school graduation.

**Note: This indicator only tracks enrollment in Kentucky institutions. Students who enroll in out-of-state or international institutions are not counted.**'''
    },
    'chronic_absenteeism': {
        'metric': 'chronic_absenteeism_rate_all_grades',
        'display_name': 'Chronic Absenteeism',
        'is_reverse': True,  # Lower is better
        'school_level': 'All',
        'unit': '%',
        'description': '''Chronic Absenteeism measures the percentage of students who miss 10% or more of school days. Regular attendance is critical for academic success and student engagement.

Students who miss 10% of yearly school time (about two days a month) for any reason, excused or unexcused, are considered chronically absent. This measure helps identify students who may need extra support to stay engaged in school.'''
    },
    'el_progress_elementary': {
        'metric': 'english_learner_score_60_80_elementary',
        'display_name': 'English Learner Progress (Elementary)',
        'is_reverse': False,
        'school_level': 'Elementary',
        'unit': '%',
        'description': 'Measures how English learners are improving their English language skills using the WIDA ACCESS assessment. This indicator tracks student growth toward becoming proficient in English.'
    },
    'el_progress_middle': {
        'metric': 'english_learner_score_60_80_middle',
        'display_name': 'English Learner Progress (Middle)',
        'is_reverse': False,
        'school_level': 'Middle',
        'unit': '%',
        'description': 'Measures how English learners are improving their English language skills using the WIDA ACCESS assessment. This indicator tracks student growth toward becoming proficient in English.'
    },
    'el_progress_high': {
        'metric': 'english_learner_score_60_80_high',
        'display_name': 'English Learner Progress (High)',
        'is_reverse': False,
        'school_level': 'High',
        'unit': '%',
        'description': 'Measures how English learners are improving their English language skills using the WIDA ACCESS assessment. This indicator tracks student growth toward becoming proficient in English.'
    },
    'el_proficiency': {
        'display_name': 'English Learner Proficiency',
        'is_reverse': False,
        'school_level': 'All',
        'unit': '%',
        'multi_level': True,
        'description': '''English Learner Proficiency measures the percentage of English learners making meaningful progress in English language acquisition, as assessed by the WIDA ACCESS for ELLs assessment.

Students are counted as making progress if they received a nonzero score on the ACCESS value table — specifically those scoring at the 60-80, 100, or 140 point levels. A score of 0 indicates no measurable progress. This combined rate captures all students demonstrating any level of English language growth, reported separately for elementary, middle, and high school.'''
    },
    'school_climate': {
        'metric': 'climate_index_score_calculated',
        'display_name': 'School Climate Index',
        'is_reverse': False,
        'school_level': 'All',
        'unit': ' points',
        'school_level_breakdown': True,
        'description': '''School Climate Index measures students' sense of belonging, safety, and engagement at school. A positive school climate is essential for student well-being and academic success.

This indicator captures student perceptions of whether their school is caring and welcoming, whether rules are fair, whether adults care about and support students, and whether students feel encouraged and part of their school. It is measured through 14 survey questions on a scale from Strongly Disagree to Strongly Agree.'''
    },
    '9th_grade_on_track': {
        'metric': '9th_grade_on_track',
        'display_name': '9th Grade On-Track',
        'is_reverse': False,
        'school_level': 'High',
        'unit': '%',
        'fayette_only': True,  # No state-level comparison available
        'description': '''9th Grade On-Track measures the percentage of 9th graders who meet promotion criteria by the end of the school year, meaning they have earned enough credits to be on track toward graduation.

Students who fall off-track in 9th grade are significantly less likely to graduate on time. This indicator is tracked for Fayette County high schools only.

**Note: State-level comparison data is not available for this indicator.**'''
    }
}

# Student groups to include in demographic breakdowns
STUDENT_GROUPS = [
    'All Students',
    'Economically Disadvantaged',
    'African American',
    'Hispanic or Latino',
    'White (non-Hispanic)',
    'Students with Disabilities (IEP)',
    'English Learner',
]

# Per-level config for the combined EL proficiency report
EL_PROFICIENCY_LEVELS = [
    {
        'name': 'Elementary',
        'metric': 'english_learner_proficiency_rate_elementary',
        'combined_metrics': [
            'english_learner_score_60_80_elementary',
            'english_learner_score_100_elementary',
            'english_learner_score_140_elementary',
        ],
    },
    {
        'name': 'Middle',
        'metric': 'english_learner_proficiency_rate_middle',
        'combined_metrics': [
            'english_learner_score_60_80_middle',
            'english_learner_score_100_middle',
            'english_learner_score_140_middle',
        ],
    },
    {
        'name': 'High',
        'metric': 'english_learner_proficiency_rate_high',
        'combined_metrics': [
            'english_learner_score_60_80_high',
            'english_learner_score_100_high',
            'english_learner_score_140_high',
        ],
    },
]


# Demographic mapping from 9th-grade CSV format to KPI student_group names
NINTH_GRADE_DEMOGRAPHIC_MAPPING = {
    'All Students': 'All Students',
    'Asian': 'Asian',
    'Black': 'African American',
    'Hispanic': 'Hispanic or Latino',
    'Two or More': 'Two or More Races',
    'White': 'White (non-Hispanic)',
    'EL': 'English Learner',
    'F/R Lunch': 'Economically Disadvantaged',
    'SpEd': 'Students with Disabilities (IEP)',
}


def load_ninth_grade_data(csv_path: Path, csv_24_25_path: Optional[Path], county: str) -> pd.DataFrame:
    """Load 9th grade on-track CSV files and normalize to KPI DataFrame format.

    Handles two file formats:
    - Standard file: School Year, School, Demographics, Percent of students that met criteria by group
    - 24-25 file: adds a 'Suppressed qualified percentage' column

    Returns a DataFrame with columns matching the KPI format:
        year, metric, district, school_name, student_group, value, suppressed
    """
    rows = []

    def parse_file(path: Path, has_suppressed_col: bool):
        df = pd.read_csv(path, dtype=str)
        for _, row in df.iterrows():
            # Handle suppression
            if has_suppressed_col and row.get('Suppressed qualified percentage', 'No') == 'Yes':
                continue

            school_year = str(row.get('School Year', '')).strip()
            if not school_year:
                continue
            parts = school_year.split('-')
            if len(parts) != 2:
                continue
            year = int('20' + parts[1])

            demographic = str(row.get('Demographics', '')).strip()
            mapped = NINTH_GRADE_DEMOGRAPHIC_MAPPING.get(demographic)
            if mapped is None:
                continue  # unmapped/irrelevant group

            pct_str = str(row.get('Percent of students that met criteria by group', '')).strip()
            if not pct_str:
                continue
            try:
                value = float(pct_str.replace('%', ''))
            except ValueError:
                continue

            school = str(row.get('School', '')).strip()
            # 'All Schools' is the district total
            school_name = '---District Total---' if school == 'All Schools' else school

            rows.append({
                'year': year,
                'metric': '9th_grade_on_track',
                'district': county,
                'school_name': school_name,
                'student_group': mapped,
                'value': value,
                'suppressed': 'N',
            })

    if csv_path and csv_path.exists():
        parse_file(csv_path, has_suppressed_col=False)
    if csv_24_25_path and csv_24_25_path.exists():
        parse_file(csv_24_25_path, has_suppressed_col=True)

    if not rows:
        return pd.DataFrame(columns=['year', 'metric', 'district', 'school_name',
                                     'student_group', 'value', 'suppressed'])

    return pd.DataFrame(rows)


def grade_to_num(grade: str) -> Optional[float]:
    """Convert a grade label to a numeric value for midpoint calculations.

    Handles formats: 'Preschool', 'K', '1st'–'12th', plain integers.
    """
    import re
    g = str(grade).strip().upper()
    if g in ('PK', 'P', 'PRE-K', 'PREK', 'PRESCHOOL'): return -1.0
    if g in ('K', 'KG', 'KN'): return 0.0
    # Strip ordinal suffixes: 1st, 2nd, 3rd, 4th … 12th
    m = re.match(r'^(\d+)(?:ST|ND|RD|TH)?$', g)
    if m:
        return float(m.group(1))
    return None


def classify_school_level(low_grade: str, high_grade: str) -> List[str]:
    """Classify a school into one or more levels based on grade-range midpoint.

    Boundaries: Elementary/Middle at 5.5, Middle/High at 8.5.
    Schools whose midpoint falls exactly on a boundary are included in both
    adjacent levels.
    """
    low = grade_to_num(low_grade)
    high = grade_to_num(high_grade)
    if low is None or high is None:
        return ['Other']

    midpoint = (low + high) / 2

    ELEM_MID = 5.5
    MID_HIGH = 8.5

    levels = []
    if midpoint <= ELEM_MID:
        levels.append('Elementary')
    if ELEM_MID <= midpoint <= MID_HIGH:
        levels.append('Middle')
    if midpoint >= MID_HIGH:
        levels.append('High')

    return levels if levels else ['Other']


def load_school_grade_ranges(school_list_path: Path, county: str) -> Dict[str, List[str]]:
    """Load the KYRC district school list CSV and return a school-name → [levels] lookup.

    Args:
        school_list_path: Path to KYRC district school list CSV
        county: District name to filter by (e.g. 'Fayette County')

    Returns:
        Dict mapping school_name to list of level strings, e.g. {'Tates Creek High School': ['High']}
    """
    lookup: Dict[str, List[str]] = {}
    try:
        df = pd.read_csv(school_list_path, dtype=str, low_memory=False)
        mask = (
            (df['District Name'] == county) &
            (df['School Name'].notna()) &
            (~df['School Name'].isin(['---District Total---', '---District---']))
        )
        for _, row in df[mask].iterrows():
            name = str(row['School Name']).strip()
            levels = classify_school_level(
                row.get('Low Grade', ''),
                row.get('High Grade', '')
            )
            lookup[name] = levels
    except Exception as e:
        print(f"  Warning: Could not load school list for level classification: {e}")
    return lookup


def load_data(data_path: Path, county: Optional[str] = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load KPI data from CSV.

    For large files (like kpi_master.csv with 57M+ rows), this function:
    - Only loads columns needed for report generation
    - Filters by county during load using chunked reading for memory efficiency
    - Also loads state-level data for comparison

    Args:
        data_path: Path to the CSV file
        county: If specified, filter to only this county during load

    Returns:
        Tuple of (county DataFrame, state DataFrame)
    """
    # Columns we need for report generation
    use_columns = ['year', 'metric', 'district', 'school_name', 'student_group',
                   'value', 'suppressed']

    # Check file size to determine loading strategy
    file_size = data_path.stat().st_size
    is_large_file = file_size > 100_000_000  # > 100MB

    if is_large_file and county:
        # For large files, use chunked reading with filtering
        print(f"  Large file detected ({file_size / 1_000_000:.0f}MB), loading in chunks...")
        county_chunks = []
        state_chunks = []
        chunk_size = 500_000

        for chunk in pd.read_csv(data_path, usecols=use_columns,
                                  low_memory=False, chunksize=chunk_size):
            # Filter to the target county
            county_filtered = chunk[chunk['district'] == county]
            if len(county_filtered) > 0:
                county_chunks.append(county_filtered)
            # Also get state data
            state_filtered = chunk[chunk['district'] == 'State']
            if len(state_filtered) > 0:
                state_chunks.append(state_filtered)

        if county_chunks:
            df = pd.concat(county_chunks, ignore_index=True)
            print(f"  Loaded {len(df):,} rows for {county}")
        else:
            df = pd.DataFrame(columns=use_columns)
            print(f"  Warning: No data found for {county}")

        if state_chunks:
            state_df = pd.concat(state_chunks, ignore_index=True)
            print(f"  Loaded {len(state_df):,} rows for State averages")
        else:
            state_df = pd.DataFrame(columns=use_columns)
    elif is_large_file:
        # Large file but no county filter - just load with appropriate settings
        print(f"  Large file detected ({file_size / 1_000_000:.0f}MB), loading...")
        df = pd.read_csv(data_path, usecols=use_columns, low_memory=False)
        state_df = df[df['district'] == 'State'].copy()
        print(f"  Loaded {len(df):,} rows")
    else:
        # Small file, load normally
        df = pd.read_csv(data_path, low_memory=False)
        # Keep only needed columns if they exist
        available_cols = [c for c in use_columns if c in df.columns]
        if available_cols:
            df = df[available_cols]
        state_df = df[df['district'] == 'State'].copy()
        if county:
            df = df[df['district'] == county]

    return df, state_df


def get_year_label(year: int) -> str:
    """Convert year to school year label (e.g., 2025 -> '2024-25')."""
    return f"{year - 1}-{str(year)[2:]}"


def get_trend_arrow(change: float, is_reverse: bool = False) -> str:
    """Get trend arrow based on change direction."""
    if pd.isna(change) or abs(change) < 0.5:
        return "→"  # Stable
    if is_reverse:
        return "↓" if change < 0 else "↑"  # For reverse indicators, down is good
    return "↑" if change > 0 else "↓"


def calculate_gap(all_students_value: float, group_value: float, is_reverse: bool = False) -> Optional[float]:
    """Calculate gap from all students average."""
    if pd.isna(all_students_value) or pd.isna(group_value):
        return None
    gap = group_value - all_students_value
    return gap


def inject_combined_metric(
    df: pd.DataFrame,
    combined_metrics: List[str],
    metric_name: str
) -> pd.DataFrame:
    """Sum multiple source metrics into a single combined metric row.

    Only non-suppressed component rows are included. If no components have data
    for a given group/year/school combination, no combined row is created.
    """
    component_rows = df[
        df['metric'].isin(combined_metrics) &
        (df['suppressed'] != 'Y')
    ].copy()
    if len(component_rows) == 0:
        return df

    id_cols = [c for c in ['year', 'district', 'school_name', 'student_group']
               if c in component_rows.columns]

    summed = (
        component_rows
        .groupby(id_cols)['value']
        .sum(min_count=1)
        .reset_index()
    )
    summed['metric'] = metric_name
    summed['suppressed'] = 'N'

    return pd.concat([df, summed], ignore_index=True)


def _build_level_section(
    df: pd.DataFrame,
    state_df: pd.DataFrame,
    county: str,
    level_name: str,
    metric: str,
    is_reverse: bool,
    unit: str,
    current_year: int,
) -> str:
    """Return a markdown section covering one school level for the EL proficiency report."""
    year_label = get_year_label(current_year)
    county_short = county.replace(' County', '')

    district_data = get_district_data(df, county, metric, current_year)
    state_average = get_state_average(state_df, metric, current_year)
    school_data = get_school_data(df, county, metric, current_year)
    trends = calculate_trend(df, county, metric, current_year)

    # Build demographic table
    demo_rows = []
    all_students_value = None

    for group in STUDENT_GROUPS:
        group_data = district_data[district_data['student_group'] == group]
        if len(group_data) > 0:
            value = float(group_data['value'].iloc[0])
            if group == 'All Students':
                all_students_value = value

            gap = calculate_gap(all_students_value, value) if all_students_value and group != 'All Students' else None
            trend = trends.get(group)
            trend_arrow = get_trend_arrow(trend, is_reverse) if trend else "N/A"
            trend_str = f"{trend_arrow} {'+' if trend and trend > 0 else ''}{trend:.1f}" if trend else "N/A"
            gap_str = f"{'+' if gap and gap > 0 else ''}{gap:.1f} pp" if gap else "--"

            demo_rows.append({'group': group, 'value': value, 'gap': gap_str, 'trend': trend_str})
        else:
            demo_rows.append({'group': group, 'value': "N/A", 'gap': "N/A", 'trend': "N/A"})

    # Find largest gap
    largest_gap_group = None
    largest_gap_value = 0.0
    for row in demo_rows:
        if row['gap'] not in ['--', 'N/A'] and row['group'] != 'All Students':
            gap_val = float(row['gap'].replace(' pp', '').replace('+', ''))
            if gap_val < largest_gap_value:
                largest_gap_value = gap_val
                largest_gap_group = row['group']

    # School variation
    if len(school_data) > 0:
        school_values = school_data['value'].astype(float)
        max_school = school_data.loc[school_values.idxmax(), 'school_name']
        min_school = school_data.loc[school_values.idxmin(), 'school_name']
        max_value = float(school_values.max())
        min_value = float(school_values.min())
        school_range = max_value - min_value
        highest_school, lowest_school = max_school, min_school
        highest_value, lowest_value = max_value, min_value
    else:
        highest_school = lowest_school = "N/A"
        highest_value = lowest_value = school_range = None

    # Trend categorization
    improving: List[Tuple[str, float]] = []
    declining: List[Tuple[str, float]] = []
    stable: List[Tuple[str, float]] = []

    for group in STUDENT_GROUPS:
        if group == 'All Students':
            continue
        trend = trends.get(group)
        if trend is None:
            continue
        if trend > 1:
            improving.append((group, trend))
        elif trend < -1:
            declining.append((group, trend))
        else:
            stable.append((group, trend))

    # Assemble section
    md = f"## {level_name} School\n\n"
    md += f"### Current Status ({year_label})\n\n"

    if all_students_value is not None:
        md += f"- **{county_short} District Average**: {all_students_value:.1f}{unit}\n"
    else:
        md += f"- **{county_short} District Average**: Data not available\n"

    if state_average is not None:
        md += f"- **State Average**: {state_average:.1f}{unit}\n"
        if all_students_value is not None:
            diff = all_students_value - state_average
            comparison = "above" if diff > 0 else "below" if diff < 0 else "equal to"
            md += f"- **Comparison**: {county_short} is {abs(diff):.1f} pp {comparison} state average\n"
    else:
        md += "- **State Average**: Not available\n"

    if largest_gap_group:
        md += f"- **Largest Gap**: {largest_gap_group} ({abs(largest_gap_value):.1f} pp below district average)\n"

    md += "\n### Demographic Breakdown\n\n"
    md += f"| Student Group | Rate ({unit}) | Gap from All Students | 3-Year Trend |\n"
    md += f"|---------------|{'-' * 15}|----------------------|--------------|\n"
    for row in demo_rows:
        value_str = f"{row['value']:.1f}" if isinstance(row['value'], (int, float)) else row['value']
        md += f"| {row['group']} | {value_str} | {row['gap']} | {row['trend']} |\n"

    md += "\n### Key Findings\n\n"
    md += "#### Largest Gaps\n\n"
    if largest_gap_group:
        md += f"The largest gap exists for **{largest_gap_group}** ({abs(largest_gap_value):.1f} pp below district average).\n\n"
    else:
        md += "[No significant gaps identified or data unavailable]\n\n"

    md += "#### Trend Analysis\n\n"
    if improving:
        md += "**Improving:**\n"
        for group, trend in improving:
            md += f"- {group} ({'+' if trend > 0 else ''}{trend:.1f} pp over 3 years)\n"
        md += "\n"
    if stable:
        md += "**Stable (change < 1 pp):**\n"
        for group, trend in stable:
            md += f"- {group} ({'+' if trend > 0 else ''}{trend:.1f} pp)\n"
        md += "\n"
    if declining:
        md += "**Declining:**\n"
        for group, trend in declining:
            md += f"- {group} ({'+' if trend > 0 else ''}{trend:.1f} pp over 3 years)\n"
        md += "\n"
    if not improving and not stable and not declining:
        md += "[Insufficient trend data available]\n\n"

    md += "#### School Variation\n\n"
    if school_range is not None:
        md += f"- **Highest performing school**: {highest_school} ({highest_value:.1f}{unit})\n"
        md += f"- **Lowest performing school**: {lowest_school} ({lowest_value:.1f}{unit})\n"
        md += f"- **Range**: {school_range:.1f} percentage points\n\n"
    else:
        md += "[School-level data not available]\n\n"

    return md


def generate_el_proficiency_draft(
    df: pd.DataFrame,
    state_df: pd.DataFrame,
    county: str,
    output_dir: Path,
    current_year: int,
) -> Path:
    """Generate a single combined EL proficiency draft covering Elementary, Middle, and High."""
    config = INDICATORS['el_proficiency']
    year_label = get_year_label(current_year)

    # Inject all three combined metrics into both DataFrames up front
    for level_cfg in EL_PROFICIENCY_LEVELS:
        df = inject_combined_metric(df, level_cfg['combined_metrics'], level_cfg['metric'])
        state_df = inject_combined_metric(state_df, level_cfg['combined_metrics'], level_cfg['metric'])

    md = f"""# English Learner Proficiency

*Draft generated on {datetime.now().strftime('%B %d, %Y')}*
*Data source: Kentucky Department of Education, {year_label} school year*

---

## Indicator Overview

{config['description']}

---

## Key Observations and Recommendations

*[Committee members: Add your analysis, interpretation, and recommendations here. Consider the data across all three school levels when forming your narrative.]*

---

"""

    for level_cfg in EL_PROFICIENCY_LEVELS:
        md += _build_level_section(
            df, state_df, county,
            level_name=level_cfg['name'],
            metric=level_cfg['metric'],
            is_reverse=config['is_reverse'],
            unit=config['unit'],
            current_year=current_year,
        )
        md += "\n---\n\n"

    md += "*This draft was auto-generated from KDE data. All statistics should be verified before publication.*\n"

    output_path = output_dir / "el_proficiency_draft.md"
    with open(output_path, 'w') as f:
        f.write(md)

    return output_path


def get_district_data(
    df: pd.DataFrame,
    county: str,
    metric: str,
    year: int
) -> pd.DataFrame:
    """Get district total data for a metric."""
    mask = (
        (df['district'] == county) &
        (df['metric'] == metric) &
        (df['year'] == year) &
        (df['school_name'] == '---District Total---') &
        (df['suppressed'] != 'Y')
    )
    return df[mask]


def get_school_data(
    df: pd.DataFrame,
    county: str,
    metric: str,
    year: int
) -> pd.DataFrame:
    """Get individual school data for a metric."""
    mask = (
        (df['district'] == county) &
        (df['metric'] == metric) &
        (df['year'] == year) &
        (df['school_name'] != '---District Total---') &
        (df['suppressed'] != 'Y') &
        (df['student_group'] == 'All Students')
    )
    return df[mask]


def get_state_average(
    state_df: pd.DataFrame,
    metric: str,
    year: int,
    student_group: str = 'All Students'
) -> Optional[float]:
    """Get the state average for a metric.

    State data has district='State' and school_name='---District Total---'.

    Args:
        state_df: DataFrame containing state-level data
        metric: The metric name to look up
        year: The year to look up
        student_group: The student group (default 'All Students')

    Returns:
        The state average value, or None if not found
    """
    mask = (
        (state_df['metric'] == metric) &
        (state_df['year'] == year) &
        (state_df['school_name'] == '---District Total---') &
        (state_df['student_group'] == student_group) &
        (state_df['suppressed'] != 'Y')
    )
    result = state_df[mask]
    if len(result) > 0:
        return float(result['value'].iloc[0])
    return None


def calculate_trend(
    df: pd.DataFrame,
    county: str,
    metric: str,
    current_year: int,
    years_back: int = 3
) -> Dict[str, Optional[float]]:
    """Calculate trend over the past N years for each student group."""
    trends = {}

    for group in STUDENT_GROUPS:
        # Get current year value
        current = df[
            (df['district'] == county) &
            (df['metric'] == metric) &
            (df['year'] == current_year) &
            (df['school_name'] == '---District Total---') &
            (df['student_group'] == group) &
            (df['suppressed'] != 'Y')
        ]['value']

        # Get baseline year value
        baseline_year = current_year - years_back
        baseline = df[
            (df['district'] == county) &
            (df['metric'] == metric) &
            (df['year'] == baseline_year) &
            (df['school_name'] == '---District Total---') &
            (df['student_group'] == group) &
            (df['suppressed'] != 'Y')
        ]['value']

        if len(current) > 0 and len(baseline) > 0:
            trends[group] = float(current.iloc[0]) - float(baseline.iloc[0])
        else:
            trends[group] = None

    return trends


def generate_indicator_draft(
    df: pd.DataFrame,
    state_df: pd.DataFrame,
    county: str,
    indicator_key: str,
    output_dir: Path,
    current_year: int,
    school_level_lookup: Optional[Dict[str, List[str]]] = None,
) -> Path:
    """Generate a markdown draft for a single indicator."""

    config = INDICATORS[indicator_key]
    metric = config['metric']
    display_name = config['display_name']
    is_reverse = config['is_reverse']
    unit = config['unit']

    # If this indicator combines multiple metrics, inject the summed metric into
    # both DataFrames so the rest of the function works without changes.
    if 'combined_metrics' in config:
        df = inject_combined_metric(df, config['combined_metrics'], metric)
        state_df = inject_combined_metric(state_df, config['combined_metrics'], metric)

    # Get district data for current year
    district_data = get_district_data(df, county, metric, current_year)

    # Get state average for comparison
    state_average = get_state_average(state_df, metric, current_year)

    # Get school-level data
    school_data = get_school_data(df, county, metric, current_year)

    # Exclude schools that don't meaningfully report this metric (e.g. alternative
    # programs with no real graduation rate) from school-level comparisons.
    exclude_schools = config.get('school_variation_exclude')
    if exclude_schools:
        school_data = school_data[~school_data['school_name'].isin(exclude_schools)]

    # Calculate trends
    trends = calculate_trend(df, county, metric, current_year)

    # Build demographic table data
    demo_rows = []
    all_students_value = None

    for group in STUDENT_GROUPS:
        group_data = district_data[district_data['student_group'] == group]
        if len(group_data) > 0:
            value = float(group_data['value'].iloc[0])
            if group == 'All Students':
                all_students_value = value

            gap = calculate_gap(all_students_value, value, is_reverse) if all_students_value and group != 'All Students' else None
            trend = trends.get(group)
            trend_arrow = get_trend_arrow(trend, is_reverse) if trend else "N/A"
            trend_str = f"{trend_arrow} {'+' if trend and trend > 0 else ''}{trend:.1f}" if trend else "N/A"
            gap_str = f"{'+' if gap and gap > 0 else ''}{gap:.1f} pp" if gap else "--"

            demo_rows.append({
                'group': group,
                'value': value,
                'gap': gap_str,
                'trend': trend_str
            })
        else:
            demo_rows.append({
                'group': group,
                'value': "N/A",
                'gap': "N/A",
                'trend': "N/A"
            })

    # Find largest gap
    largest_gap_group = None
    largest_gap_value = 0
    for row in demo_rows:
        if row['gap'] not in ['--', 'N/A'] and row['group'] != 'All Students':
            gap_val = float(row['gap'].replace(' pp', '').replace('+', ''))
            if is_reverse:
                if gap_val > largest_gap_value:
                    largest_gap_value = gap_val
                    largest_gap_group = row['group']
            else:
                if gap_val < largest_gap_value:
                    largest_gap_value = gap_val
                    largest_gap_group = row['group']

    # School variation
    if len(school_data) > 0:
        school_values = school_data['value'].astype(float)
        max_school = school_data.loc[school_values.idxmax(), 'school_name']
        min_school = school_data.loc[school_values.idxmin(), 'school_name']
        max_value = school_values.max()
        min_value = school_values.min()
        school_range = max_value - min_value
        # For reverse indicators, lowest value = best performance
        if is_reverse:
            highest_school, lowest_school = min_school, max_school
            highest_value, lowest_value = min_value, max_value
        else:
            highest_school, lowest_school = max_school, min_school
            highest_value, lowest_value = max_value, min_value
    else:
        highest_school = lowest_school = "N/A"
        highest_value = lowest_value = school_range = None

    # Categorize trends
    improving = []
    declining = []
    stable = []

    for group in STUDENT_GROUPS:
        if group == 'All Students':
            continue
        trend = trends.get(group)
        if trend is None:
            continue
        if is_reverse:
            if trend < -1:
                improving.append((group, trend))
            elif trend > 1:
                declining.append((group, trend))
            else:
                stable.append((group, trend))
        else:
            if trend > 1:
                improving.append((group, trend))
            elif trend < -1:
                declining.append((group, trend))
            else:
                stable.append((group, trend))

    # Generate markdown
    year_label = get_year_label(current_year)
    county_short = county.replace(' County', '')

    md = f"""# {display_name}

*Draft generated on {datetime.now().strftime('%B %d, %Y')}*
*Data source: Kentucky Department of Education, {year_label} school year*

---

## Indicator Overview

{config['description']}

{"**Note:** For this indicator, **lower values indicate better performance.**" if is_reverse else ""}

---

## Current Status ({year_label})

"""

    # Add current status section
    if all_students_value:
        md += f"- **{county_short} District Average**: {all_students_value:.1f}{unit}\n"
    else:
        md += f"- **{county_short} District Average**: Data not available\n"

    # Add state average comparison
    if state_average is not None:
        md += f"- **State Average**: {state_average:.1f}{unit}\n"
        if all_students_value is not None:
            diff = all_students_value - state_average
            if is_reverse:
                # For reverse indicators (like chronic absenteeism), lower is better
                comparison = "better than" if diff < 0 else "worse than" if diff > 0 else "equal to"
            else:
                comparison = "above" if diff > 0 else "below" if diff < 0 else "equal to"
            md += f"- **Comparison**: {county_short} is {abs(diff):.1f} pp {comparison} state average\n"
    else:
        md += "- **State Average**: Not available\n"

    # Add largest gap to current status
    if largest_gap_group:
        md += f"- **Largest Gap**: {largest_gap_group} ({abs(largest_gap_value):.1f} pp {'above' if is_reverse else 'below'} district average)\n"

    # Add any configured extra metrics (e.g. an alternate calculation of the same indicator)
    for extra in config.get('extra_metrics', []):
        extra_district = get_district_data(df, county, extra['metric'], current_year)
        extra_all_students = extra_district[extra_district['student_group'] == 'All Students']
        extra_value = float(extra_all_students['value'].iloc[0]) if len(extra_all_students) > 0 else None
        extra_state = get_state_average(state_df, extra['metric'], current_year)

        if extra_value is not None:
            md += f"- **{county_short} District Average ({extra['label']})**: {extra_value:.1f}{unit}\n"
        if extra_state is not None:
            md += f"- **State Average ({extra['label']})**: {extra_state:.1f}{unit}\n"

    md += "\n"

    if config.get('data_note'):
        md += f"{config['data_note']}\n\n"

    md += """---

## Key Observations and Recommendations

*[Committee members: Add your analysis, interpretation, and recommendations here. Consider the data presented below when forming your narrative.]*

---

## Demographic Breakdown

| Student Group | {metric_label} | Gap from All Students | 3-Year Trend |
|---------------|{header_sep}|----------------------|--------------|
""".format(
        metric_label=f"Rate ({unit})" if unit == '%' else f"Score ({unit.strip()})",
        header_sep="-" * 15
    )

    for row in demo_rows:
        value_str = f"{row['value']:.1f}" if isinstance(row['value'], (int, float)) else row['value']
        md += f"| {row['group']} | {value_str} | {row['gap']} | {row['trend']} |\n"

    md += """
---

## Key Findings

### Largest Gaps

"""
    if largest_gap_group:
        md += f"The largest achievement gap exists for **{largest_gap_group}** ({abs(largest_gap_value):.1f} percentage points {'above' if is_reverse else 'below'} district average).\n\n"
    else:
        md += "[No significant gaps identified or data unavailable]\n\n"

    md += """### Trend Analysis

"""

    if improving:
        md += "**Improving:**\n"
        for group, trend in improving:
            md += f"- {group} ({'+' if trend > 0 else ''}{trend:.1f} pp over 3 years)\n"
        md += "\n"

    if stable:
        md += "**Stable (change < 1 pp):**\n"
        for group, trend in stable:
            md += f"- {group} ({'+' if trend > 0 else ''}{trend:.1f} pp)\n"
        md += "\n"

    if declining:
        md += "**Declining:**\n"
        for group, trend in declining:
            md += f"- {group} ({'+' if trend > 0 else ''}{trend:.1f} pp over 3 years)\n"
        md += "\n"

    if not improving and not stable and not declining:
        md += "[Insufficient trend data available]\n\n"

    md += """### School Variation

"""

    highest_label, lowest_label = config.get(
        'school_variation_terms', ('Highest performing school', 'Lowest performing school')
    )

    use_level_breakdown = (
        config.get('school_level_breakdown')
        and school_level_lookup
        and len(school_data) > 0
    )

    if use_level_breakdown:
        # Build per-level sub-sections
        level_order = ['Elementary', 'Middle', 'High', 'Other']
        # Map each school in school_data to its levels
        school_data = school_data.copy()
        school_data['_levels'] = school_data['school_name'].map(
            lambda n: school_level_lookup.get(n, ['Other'])
        )

        unmatched = school_data[school_data['_levels'].apply(lambda ls: ls == ['Other'])]
        if len(unmatched) > 0:
            print(f"  Warning: {len(unmatched)} schools could not be classified by level "
                  f"(name mismatch with school list): "
                  f"{', '.join(unmatched['school_name'].tolist())}")

        any_level_data = False
        for level in level_order:
            if level == 'Other':
                continue  # Suppress — artifact of name mismatches
            level_rows = school_data[school_data['_levels'].apply(lambda ls: level in ls)]
            if len(level_rows) == 0:
                continue
            any_level_data = True
            vals = level_rows['value'].astype(float)
            max_school = level_rows.loc[vals.idxmax(), 'school_name']
            min_school = level_rows.loc[vals.idxmin(), 'school_name']
            max_val = float(vals.max())
            min_val = float(vals.min())
            rng = max_val - min_val
            if is_reverse:
                best_school, worst_school = min_school, max_school
                best_val, worst_val = min_val, max_val
            else:
                best_school, worst_school = max_school, min_school
                best_val, worst_val = max_val, min_val
            md += f"**{level}** ({len(level_rows)} schools)\n"
            md += f"- **{highest_label}**: {best_school} ({best_val:.1f}{unit})\n"
            md += f"- **{lowest_label}**: {worst_school} ({worst_val:.1f}{unit})\n"
            md += f"- **Range**: {rng:.1f} percentage points\n\n"

        if not any_level_data:
            md += "[School-level data not available]\n\n"

    elif school_range is not None:
        md += f"""- **{highest_label}**: {highest_school} ({highest_value:.1f}{unit})
- **{lowest_label}**: {lowest_school} ({lowest_value:.1f}{unit})
- **Range**: {school_range:.1f} percentage points

"""
    else:
        md += "[School-level data not available]\n\n"

    md += """
---

*This draft was auto-generated from KDE data. All statistics should be verified before publication.*
"""

    # Write to file
    output_path = output_dir / f"{indicator_key}_draft.md"
    with open(output_path, 'w') as f:
        f.write(md)

    return output_path


def main():
    parser = argparse.ArgumentParser(
        description='Generate data-driven report drafts for the Equity Scorecard Report'
    )
    parser.add_argument(
        '--county',
        type=str,
        default='Fayette County',
        help='County/district name (e.g., "Fayette County", "Jefferson County")'
    )
    parser.add_argument(
        '--data',
        type=str,
        default='data/kpi/kpi_master.csv',
        help='Path to KPI data CSV'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='reports/drafts',
        help='Output directory for draft files'
    )
    parser.add_argument(
        '--indicator',
        type=str,
        default=None,
        help='Generate draft for a single indicator (e.g., "reading_3rd_grade")'
    )
    parser.add_argument(
        '--year',
        type=int,
        default=None,
        help='School year to use (e.g., 2025 for 2024-25). Defaults to most recent.'
    )
    parser.add_argument(
        '--school-list',
        type=str,
        default=None,
        help='Path to KYRC district school list CSV (for school-level breakdown by grade range)'
    )
    parser.add_argument(
        '--ninth-grade-csv',
        type=str,
        default=None,
        help='Path to 9th grade on-track CSV (pre-2024-25 years)'
    )
    parser.add_argument(
        '--ninth-grade-24-25-csv',
        type=str,
        default=None,
        help='Path to 9th grade on-track 24-25 CSV'
    )
    parser.add_argument(
        '--list-indicators',
        action='store_true',
        help='List available indicators and exit'
    )

    args = parser.parse_args()

    if args.list_indicators:
        print("\nAvailable indicators:")
        print("-" * 60)
        for key, config in INDICATORS.items():
            print(f"  {key}")
            print(f"    Display name: {config['display_name']}")
            print(f"    School level: {config['school_level']}")
            print()
        return

    # Load data
    data_path = Path(args.data)
    if not data_path.exists():
        # Try relative to script location
        script_dir = Path(__file__).parent.parent
        data_path = script_dir / args.data

    if not data_path.exists():
        print(f"Error: Data file not found at {data_path}")
        print("Expected location: data/kpi/kpi_master.csv")
        print("Please run the ETL pipeline first or specify --data path.")
        return

    print(f"Loading data from {data_path}...")
    df, state_df = load_data(data_path, county=args.county)

    # Load 9th grade on-track data and merge into df if paths provided
    ninth_csv = Path(args.ninth_grade_csv) if args.ninth_grade_csv else None
    ninth_csv_2425 = Path(args.ninth_grade_24_25_csv) if args.ninth_grade_24_25_csv else None

    if ninth_csv or ninth_csv_2425:
        ninth_df = load_ninth_grade_data(ninth_csv, ninth_csv_2425, args.county)
        if len(ninth_df) > 0:
            df = pd.concat([df, ninth_df], ignore_index=True)
            print(f"  Loaded {len(ninth_df):,} 9th grade on-track rows")
        else:
            print("  Warning: No 9th grade on-track data loaded (check file paths)")

    # Load school grade-range lookup for level-breakdown indicators
    school_level_lookup: Optional[Dict[str, List[str]]] = None
    if args.school_list:
        school_list_path = Path(args.school_list)
        if school_list_path.exists():
            school_level_lookup = load_school_grade_ranges(school_list_path, args.county)
            print(f"  Loaded grade ranges for {len(school_level_lookup)} schools")
        else:
            print(f"  Warning: School list not found at {args.school_list}")

    # Determine year
    if args.year:
        current_year = args.year
    else:
        current_year = int(df['year'].max())

    print(f"Using school year: {get_year_label(current_year)}")
    print(f"County: {args.county}")

    # Create output directory
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate drafts
    if args.indicator:
        if args.indicator not in INDICATORS:
            print(f"Error: Unknown indicator '{args.indicator}'")
            print("Use --list-indicators to see available options.")
            return
        indicators = [args.indicator]
    else:
        indicators = list(INDICATORS.keys())

    generated = []
    skipped = []

    for indicator_key in indicators:
        config = INDICATORS[indicator_key]

        # Check if data exists for this indicator
        if config.get('multi_level'):
            # Multi-level indicator: check across all component metrics for any level
            all_component_metrics = [
                m for level_cfg in EL_PROFICIENCY_LEVELS
                for m in level_cfg['combined_metrics']
            ]
            has_data = len(df[
                (df['district'] == args.county) &
                (df['metric'].isin(all_component_metrics)) &
                (df['year'] == current_year)
            ]) > 0
        elif 'combined_metrics' in config:
            has_data = len(df[
                (df['district'] == args.county) &
                (df['metric'].isin(config['combined_metrics'])) &
                (df['year'] == current_year)
            ]) > 0
        else:
            has_data = len(df[
                (df['district'] == args.county) &
                (df['metric'] == config['metric']) &
                (df['year'] == current_year)
            ]) > 0

        if has_data:
            if config.get('multi_level'):
                output_path = generate_el_proficiency_draft(
                    df, state_df, args.county, output_dir, current_year
                )
            else:
                output_path = generate_indicator_draft(
                    df, state_df, args.county, indicator_key, output_dir, current_year,
                    school_level_lookup=school_level_lookup,
                )
            generated.append((indicator_key, output_path))
            print(f"  ✓ Generated: {indicator_key}")
        else:
            skipped.append(indicator_key)
            print(f"  ✗ Skipped (no data): {indicator_key}")

    print()
    print(f"Generated {len(generated)} drafts in {output_dir}/")
    if skipped:
        print(f"Skipped {len(skipped)} indicators due to missing data: {', '.join(skipped)}")

    # Generate index file
    if generated:
        index_path = output_dir / "README.md"
        with open(index_path, 'w') as f:
            f.write(f"# Equity Scorecard Report Drafts\n\n")
            f.write(f"Generated: {datetime.now().strftime('%B %d, %Y')}\n")
            f.write(f"County: {args.county}\n")
            f.write(f"School Year: {get_year_label(current_year)}\n\n")
            f.write("## Available Drafts\n\n")
            for key, path in generated:
                config = INDICATORS[key]
                f.write(f"- [{config['display_name']}]({path.name})\n")
        print(f"\nIndex file: {index_path}")


if __name__ == '__main__':
    main()
