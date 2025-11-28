#!/usr/bin/env python3
"""
Base Class for Creating Analysis Datasets

This module provides a base class that standardizes the process of creating
analysis datasets by combining:
- Outcome data (graduation rates, reading proficiency, etc.)
- Student demographics
- Teacher quality metrics
- Financial/resource metrics
- Census economic data (county-level and tract-level)

Subclasses implement the specific outcome loading logic while inheriting
all the shared covariate loading and merging functionality.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional
import pandas as pd
import numpy as np


class BaseAnalysisDataset(ABC):
    """
    Base class for creating analysis datasets.

    Subclasses must implement:
    - OUTCOME_NAME: Name of the outcome column in the output
    - OUTPUT_FILENAME: Name of the output CSV file
    - load_outcome_data(): Load and return the primary outcome variable

    Optional overrides:
    - get_merge_strategy(): Return 'school_id' or 'name_based' for merging
    - get_year_range(): Return tuple of (min_year, max_year) to filter
    - get_school_type_filter(): Return school_type value to filter (e.g., 'A1')
    """

    # Subclasses must define these
    OUTCOME_NAME: str = None  # e.g., 'graduation_rate', 'reading_proficiency_rate'
    OUTPUT_FILENAME: str = None  # e.g., 'graduation_analysis.csv'

    # Paths - relative to the analysis/scripts directory
    BASE_DIR = Path(__file__).parent.parent.parent  # ky-education-kpi-pipeline
    DATA_DIR = BASE_DIR / "data"
    KPI_FILE = DATA_DIR / "kpi" / "kpi_master.csv"
    PROCESSED_DIR = BASE_DIR.parent / "ky-education-portal" / "src" / "data" / "processed"
    EXTERNAL_DIR = DATA_DIR / "external"
    OUTPUT_DIR = BASE_DIR / "analysis" / "datasets"

    # Region mappings for Kentucky
    BLUEGRASS_COUNTIES = ['FAYETTE', 'BOURBON', 'CLARK', 'JESSAMINE', 'MADISON',
                          'SCOTT', 'WOODFORD', 'FRANKLIN', 'ANDERSON']
    NORTHERN_COUNTIES = ['BOONE', 'CAMPBELL', 'KENTON', 'GALLATIN', 'GRANT', 'PENDLETON']
    LOUISVILLE_COUNTIES = ['JEFFERSON', 'BULLITT', 'OLDHAM', 'SHELBY']
    APPALACHIAN_COUNTIES = ['BELL', 'FLOYD', 'HARLAN', 'JOHNSON', 'KNOTT', 'LESLIE',
                            'LETCHER', 'MAGOFFIN', 'MARTIN', 'PERRY', 'PIKE', 'WHITLEY']

    def __init__(self, verbose: bool = True):
        """Initialize the dataset builder."""
        self.verbose = verbose
        self.OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

        # Validate subclass configuration
        if not self.OUTCOME_NAME:
            raise ValueError("Subclass must define OUTCOME_NAME")
        if not self.OUTPUT_FILENAME:
            raise ValueError("Subclass must define OUTPUT_FILENAME")

    def log(self, message: str, header: bool = False) -> None:
        """Print a log message if verbose mode is enabled."""
        if self.verbose:
            if header:
                print("\n" + "=" * 60)
                print(message)
                print("=" * 60)
            else:
                print(message)

    @staticmethod
    def normalize_name(name) -> str:
        """Normalize a school/district name for matching."""
        if pd.isna(name):
            return ''
        return str(name).lower().strip()

    # =========================================================================
    # CHUNKED KPI READING - For handling large KPI master file efficiently
    # =========================================================================

    def read_kpi_chunked(
        self,
        metrics: list[str],
        chunksize: int = 500_000,
        additional_filters: Optional[dict] = None
    ) -> pd.DataFrame:
        """
        Read KPI master file in chunks, filtering to specific metrics.

        This dramatically reduces memory usage and load time for the 11GB+ KPI file
        by only keeping rows that match the specified metrics.

        Args:
            metrics: List of metric names to filter for (e.g., ['graduation_rate_4_year'])
            chunksize: Number of rows to read per chunk (default 500k)
            additional_filters: Optional dict of {column: value} for additional filtering
                               e.g., {'student_group': 'All Students', 'suppressed': lambda x: x != 'Y'}

        Returns:
            DataFrame containing only rows matching the specified metrics
        """
        self.log(f"Reading KPI master file (chunked): {self.KPI_FILE}")
        self.log(f"  Filtering for metrics: {metrics}")

        chunks = []
        total_rows_read = 0
        total_rows_kept = 0

        for chunk in pd.read_csv(self.KPI_FILE, chunksize=chunksize, low_memory=False):
            total_rows_read += len(chunk)

            # Filter to specified metrics
            filtered = chunk[chunk['metric'].isin(metrics)].copy()

            # Apply additional filters if provided
            if additional_filters:
                for col, filter_val in additional_filters.items():
                    if col in filtered.columns:
                        if callable(filter_val):
                            filtered = filtered[filter_val(filtered[col])]
                        else:
                            filtered = filtered[filtered[col] == filter_val]

            if len(filtered) > 0:
                chunks.append(filtered)
                total_rows_kept += len(filtered)

            # Progress logging every 5M rows
            if total_rows_read % 5_000_000 == 0:
                self.log(f"  Processed {total_rows_read:,} rows, kept {total_rows_kept:,}")

        self.log(f"  Total: processed {total_rows_read:,} rows, kept {total_rows_kept:,}")

        if chunks:
            result = pd.concat(chunks, ignore_index=True)
            self.log(f"  Result: {len(result):,} records for {len(metrics)} metric(s)")
            return result
        else:
            self.log("  Warning: No matching records found!")
            return pd.DataFrame()

    # =========================================================================
    # ABSTRACT METHODS - Subclasses must implement
    # =========================================================================

    @abstractmethod
    def load_outcome_data(self) -> pd.DataFrame:
        """
        Load the primary outcome variable.

        Returns:
            DataFrame with columns:
            - year
            - school_id
            - school_name
            - district
            - district_number
            - county_number
            - county_name
            - {OUTCOME_NAME}: the outcome variable

            Additional columns (school_code, school_type, etc.) are optional.
        """
        pass

    # =========================================================================
    # CONFIGURATION METHODS - Subclasses can override
    # =========================================================================

    def get_merge_strategy(self) -> str:
        """
        Return the merge strategy for joining covariates.

        Options:
        - 'school_id': Merge on year + school_id (requires consistent IDs)
        - 'name_based': Merge on year + school_name_norm + district_norm

        Default is 'school_id'. Override for datasets with inconsistent IDs.
        """
        return 'school_id'

    def get_year_range(self) -> Optional[tuple]:
        """
        Return (min_year, max_year) tuple to filter data.
        Return None to include all years.
        """
        return None

    def get_school_type_filter(self) -> Optional[str]:
        """
        Return school_type value to filter (e.g., 'A1' for high schools).
        Return None to include all school types.
        """
        return None

    def use_precomputed_demographics(self) -> bool:
        """
        Whether to use precomputed demographics file vs computing from enrollment.
        Default is False (compute from enrollment).
        """
        return False

    # =========================================================================
    # SHARED LOADING METHODS
    # =========================================================================

    def load_demographics(self) -> pd.DataFrame:
        """Load student demographics from enrollment data."""
        self.log("LOADING STUDENT DEMOGRAPHICS", header=True)

        # Check for precomputed demographics file first
        precomputed_file = self.BASE_DIR / "analysis" / "datasets" / "demographic_predictors.csv"
        if self.use_precomputed_demographics() and precomputed_file.exists():
            self.log(f"  Loading precomputed demographics from: {precomputed_file}")
            demo_df = pd.read_csv(precomputed_file)
            self.log(f"  Loaded {len(demo_df):,} demographic records")
            return demo_df

        # Otherwise compute from enrollment
        enrollment_file = self.PROCESSED_DIR / "student_enrollment.csv"
        if not enrollment_file.exists():
            self.log(f"  Warning: {enrollment_file} not found")
            return pd.DataFrame()

        df = pd.read_csv(enrollment_file, low_memory=False)
        self.log(f"  Loaded {len(df):,} enrollment records")

        result = self._compute_demographics(df)
        return result

    def _compute_demographics(self, enrollment_df: pd.DataFrame) -> pd.DataFrame:
        """Compute demographic percentages from enrollment by student group."""
        self.log("  Computing demographic percentages...")

        # Get total enrollment
        totals = enrollment_df[
            (enrollment_df['metric'] == 'student_enrollment_total') &
            (enrollment_df['student_group'] == 'All Students') &
            (enrollment_df['school_name'] != '---District Total---')
        ][['year', 'school_id', 'school_name', 'district', 'district_number', 'value']].copy()
        totals['value'] = pd.to_numeric(totals['value'], errors='coerce')

        # Handle duplicates by taking max
        totals = totals.groupby(
            ['year', 'school_id', 'school_name', 'district', 'district_number'],
            as_index=False
        ).agg({'value': 'max'})
        totals = totals.rename(columns={'value': 'total_enrollment'})

        # Define demographic groups
        demo_groups = {
            'African American': 'pct_african_american',
            'Hispanic or Latino': 'pct_hispanic',
            'White (non-Hispanic)': 'pct_white',
            'Asian': 'pct_asian',
            'Two or More Races': 'pct_two_or_more_races',
            'American Indian or Alaska Native': 'pct_american_indian',
            'Economically Disadvantaged': 'pct_economically_disadvantaged',
            'Students with Disabilities (IEP)': 'pct_students_with_disabilities',
            'English Learner': 'pct_english_learners'
        }

        # Get counts for each demographic
        demo_counts = enrollment_df[
            (enrollment_df['metric'] == 'student_enrollment_total') &
            (enrollment_df['student_group'].isin(demo_groups.keys())) &
            (enrollment_df['school_name'] != '---District Total---')
        ][['year', 'school_id', 'student_group', 'value']].copy()
        demo_counts['value'] = pd.to_numeric(demo_counts['value'], errors='coerce')

        # Pivot to wide format
        demo_wide = demo_counts.pivot_table(
            index=['year', 'school_id'],
            columns='student_group',
            values='value',
            aggfunc='max'
        ).reset_index()

        # Rename columns
        demo_wide = demo_wide.rename(columns=demo_groups)

        # Merge with totals
        result = totals.merge(demo_wide, on=['year', 'school_id'], how='left')

        # Compute percentages
        for col in demo_groups.values():
            if col in result.columns:
                result[col] = (result[col] / result['total_enrollment'] * 100).fillna(0)

        self.log(f"    Computed demographics for {len(result):,} school-years")

        return result

    def load_teacher_quality_metrics(self) -> pd.DataFrame:
        """
        Load teacher quality metrics from processed KPI files.

        Metrics loaded:
        - novice_teacher_rate (combined <1yr + 1-3yr)
        - teacher_average_years_experience
        - student_teacher_ratio
        - teacher_turnover_rate
        - emergency_provisional_teacher_rate
        """
        self.log("LOADING TEACHER QUALITY METRICS", header=True)

        use_name_based = self.get_merge_strategy() == 'name_based'
        teacher_metrics = {}

        # Define key columns based on merge strategy
        if use_name_based:
            key_cols = ['year', 'school_name_norm', 'district_norm']
        else:
            key_cols = ['year', 'school_id', 'district']

        # 1. Novice Teachers (<1yr + 1-3yr combined)
        novice_file = self.PROCESSED_DIR / "novice_teachers.csv"
        if novice_file.exists():
            df = pd.read_csv(novice_file, low_memory=False)
            df = df[(df['student_group'] == 'All Students') &
                    (df['suppressed'] != 'Y') &
                    (df['school_name'] != '---District Total---')].copy()
            df['value'] = pd.to_numeric(df['value'], errors='coerce')

            if use_name_based:
                df['school_name_norm'] = df['school_name'].apply(self.normalize_name)
                df['district_norm'] = df['district'].apply(self.normalize_name)
                pivot_index = ['year', 'school_name_norm', 'district_norm']
            else:
                pivot_index = ['year', 'school_id', 'district']

            novice_pivot = df.pivot_table(
                index=pivot_index,
                columns='metric',
                values='value',
                aggfunc='first'
            ).reset_index()

            rate_cols = ['novice_teacher_rate_less_than_1_year', 'novice_teacher_rate_1_to_3_years']
            existing_cols = [c for c in rate_cols if c in novice_pivot.columns]
            if existing_cols:
                novice_pivot['novice_teacher_rate'] = novice_pivot[existing_cols].sum(axis=1, skipna=True)
                teacher_metrics['novice_teachers'] = novice_pivot[key_cols + ['novice_teacher_rate']]
                self.log(f"  Novice teachers: {len(novice_pivot):,} records")

        # 2. Teacher Experience
        exp_file = self.PROCESSED_DIR / "teacher_experience.csv"
        if exp_file.exists():
            df = pd.read_csv(exp_file, low_memory=False)
            df = df[(df['student_group'] == 'All Students') &
                    (df['suppressed'] != 'Y') &
                    (df['school_name'] != '---District Total---')].copy()
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            df = df[df['metric'] == 'teacher_average_years_experience']
            df = df.rename(columns={'value': 'teacher_avg_experience'})

            if use_name_based:
                df['school_name_norm'] = df['school_name'].apply(self.normalize_name)
                df['district_norm'] = df['district'].apply(self.normalize_name)

            teacher_metrics['experience'] = df[key_cols + ['teacher_avg_experience']]
            self.log(f"  Teacher experience: {len(df):,} records")

        # 3. Student-Teacher Ratio
        str_file = self.PROCESSED_DIR / "student_teacher_ratio.csv"
        if str_file.exists():
            df = pd.read_csv(str_file, low_memory=False)
            df = df[(df['student_group'] == 'All Students') &
                    (df['suppressed'] != 'Y') &
                    (df['school_name'] != '---District Total---')].copy()
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            df = df[df['metric'] == 'student_teacher_ratio']
            df = df.rename(columns={'value': 'student_teacher_ratio'})

            if use_name_based:
                df['school_name_norm'] = df['school_name'].apply(self.normalize_name)
                df['district_norm'] = df['district'].apply(self.normalize_name)

            teacher_metrics['str'] = df[key_cols + ['student_teacher_ratio']]
            self.log(f"  Student-teacher ratio: {len(df):,} records")

        # 4. Teacher Turnover
        turnover_file = self.PROCESSED_DIR / "teacher_turnover.csv"
        if turnover_file.exists():
            df = pd.read_csv(turnover_file, low_memory=False)
            df = df[(df['student_group'] == 'All Students') &
                    (df['suppressed'] != 'Y') &
                    (df['school_name'] != '---District Total---')].copy()
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            df = df[df['metric'] == 'teacher_turnover_rate']
            df = df.rename(columns={'value': 'teacher_turnover_rate'})

            if use_name_based:
                df['school_name_norm'] = df['school_name'].apply(self.normalize_name)
                df['district_norm'] = df['district'].apply(self.normalize_name)

            teacher_metrics['turnover'] = df[key_cols + ['teacher_turnover_rate']]
            self.log(f"  Teacher turnover: {len(df):,} records")

        # 5. Emergency/Provisional Certification
        cert_file = self.PROCESSED_DIR / "teacher_certification.csv"
        if cert_file.exists():
            df = pd.read_csv(cert_file, low_memory=False)
            df = df[(df['student_group'] == 'All Students') &
                    (df['suppressed'] != 'Y') &
                    (df['school_name'] != '---District Total---')].copy()
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            df = df[df['metric'] == 'emergency_provisional_teacher_rate']
            df = df.rename(columns={'value': 'emergency_provisional_rate'})

            if use_name_based:
                df['school_name_norm'] = df['school_name'].apply(self.normalize_name)
                df['district_norm'] = df['district'].apply(self.normalize_name)

            teacher_metrics['certification'] = df[key_cols + ['emergency_provisional_rate']]
            self.log(f"  Emergency/provisional certification: {len(df):,} records")

        # Merge all teacher metrics
        if not teacher_metrics:
            self.log("  Warning: No teacher quality metrics found!")
            return pd.DataFrame()

        result = None
        for name, df in teacher_metrics.items():
            if result is None:
                result = df
            else:
                result = result.merge(df, on=key_cols, how='outer')

        self.log(f"\n  Combined teacher metrics: {len(result):,} records")
        return result

    def load_financial_metrics(self) -> pd.DataFrame:
        """
        Load financial/resource metrics from processed KPI files.

        Metrics loaded (district-level):
        - total_spending_per_student_all_funds
        - certified_staff_fte
        """
        self.log("LOADING FINANCIAL METRICS", header=True)

        financial_metrics = {}

        # 1. Per-pupil spending (district-level)
        spending_file = self.PROCESSED_DIR / "spending_per_student.csv"
        if spending_file.exists():
            df = pd.read_csv(spending_file, low_memory=False)
            df = df[(df['student_group'] == 'All Students') & (df['suppressed'] != 'Y')].copy()
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            df = df[df['metric'] == 'total_spending_per_student_all_funds']
            df = df[df['school_name'].str.contains('District Total', na=False)]
            df = df.rename(columns={'value': 'per_pupil_spending'})
            financial_metrics['spending'] = df[['year', 'district', 'per_pupil_spending']]
            self.log(f"  Per-pupil spending: {len(df):,} district records")

        # 2. Financial summary (district-level staffing)
        fin_file = self.PROCESSED_DIR / "financial_summary.csv"
        if fin_file.exists():
            df = pd.read_csv(fin_file, low_memory=False)
            df = df[(df['student_group'] == 'All Students') & (df['suppressed'] != 'Y')].copy()
            df['value'] = pd.to_numeric(df['value'], errors='coerce')

            pivot_df = df.pivot_table(
                index=['year', 'district'],
                columns='metric',
                values='value',
                aggfunc='first'
            ).reset_index()

            if 'eoy_student_membership' in pivot_df.columns and 'certified_staff_fte' in pivot_df.columns:
                pivot_df['students_per_certified_staff'] = (
                    pivot_df['eoy_student_membership'] / pivot_df['certified_staff_fte']
                )

            financial_metrics['summary'] = pivot_df
            self.log(f"  Financial summary: {len(pivot_df):,} district records")

        # Merge financial metrics
        if not financial_metrics:
            self.log("  Warning: No financial metrics found!")
            return pd.DataFrame()

        result = None
        for name, df in financial_metrics.items():
            if result is None:
                result = df
            else:
                result = result.merge(df, on=['year', 'district'], how='outer')

        self.log(f"\n  Combined financial metrics: {len(result):,} records")
        return result

    def load_census_county_data(self) -> pd.DataFrame:
        """
        Load Census SAIPE economic indicators (county-level).

        Metrics loaded:
        - median_household_income
        - poverty_rate_all_ages
        - poverty_rate_5_17
        """
        self.log("LOADING CENSUS ECONOMIC DATA (County-Level)", header=True)

        saipe_file = self.EXTERNAL_DIR / "census_saipe" / "census_saipe_combined.csv"
        if not saipe_file.exists():
            saipe_file = self.EXTERNAL_DIR / "census_saipe" / "census_saipe_2023.csv"

        if not saipe_file.exists():
            self.log("  Warning: Census SAIPE data not found!")
            return pd.DataFrame()

        df = pd.read_csv(saipe_file)
        self.log(f"  Raw records: {len(df):,}")

        # Pivot to wide format
        census_wide = df.pivot_table(
            index=['county_name', 'data_vintage'],
            columns='metric',
            values='value',
            aggfunc='first'
        ).reset_index()

        # Rename columns for clarity
        col_renames = {
            'median_household_income': 'county_median_income',
            'poverty_rate_all_ages': 'county_poverty_rate',
            'poverty_rate_5_17': 'county_child_poverty_rate'
        }
        census_wide = census_wide.rename(columns=col_renames)

        # Standardize county name for matching
        census_wide['county_name_upper'] = census_wide['county_name'].str.upper()

        self.log(f"  Census data: {len(census_wide):,} county records")
        self.log(f"  Counties: {census_wide['county_name'].nunique()}")

        return census_wide

    def load_institutional_characteristics(self) -> pd.DataFrame:
        """
        Load institutional characteristics from KPI master file.

        These are dummy-encoded categorical variables:
        - Title I Status (schoolwide, targeted, eligible_no_program)
        - School Type dummies (a5=alternative, a6=state agency children)

        Note: School Type A1 is the reference category (standard public schools).
        Note: "Not a Title 1 School" is the reference category for Title I.
        """
        self.log("LOADING INSTITUTIONAL CHARACTERISTICS", header=True)

        # Define the institutional metrics to load
        institutional_metrics = [
            'title_i_schoolwide',
            'title_i_targeted',
            'title_i_eligible_no_program',
            'school_type_a5',
            'school_type_a6',
        ]

        # Use chunked reading to efficiently load from large KPI file
        inst_df = self.read_kpi_chunked(institutional_metrics)
        self.log(f"  Found {len(inst_df):,} institutional characteristic records")

        if len(inst_df) == 0:
            self.log("  Warning: No institutional characteristics found in KPI master!")
            return pd.DataFrame()

        # Convert value to numeric
        inst_df['value'] = pd.to_numeric(inst_df['value'], errors='coerce')

        # Pivot to wide format
        pivot_df = inst_df.pivot_table(
            index=['year', 'school_id'],
            columns='metric',
            values='value',
            aggfunc='first'
        ).reset_index()

        # Fill missing with 0 (reference category)
        for col in institutional_metrics:
            if col in pivot_df.columns:
                pivot_df[col] = pivot_df[col].fillna(0).astype(int)
            else:
                pivot_df[col] = 0

        self.log(f"  Institutional characteristics: {len(pivot_df):,} school-year records")

        # Report distribution
        for col in institutional_metrics:
            if col in pivot_df.columns:
                n_ones = pivot_df[col].sum()
                self.log(f"    {col}: {n_ones} schools ({100*n_ones/len(pivot_df):.1f}%)")

        return pivot_df

    def load_tract_level_data(self) -> pd.DataFrame:
        """
        Load tract-level Census ACS data for within-county variation.

        Metrics loaded (where available):
        - tract_median_household_income
        - tract_poverty_rate
        - tract_unemployment_rate
        - tract_pct_bachelors_plus
        - tract_pct_single_parent
        - tract_pct_owner_occupied
        - tract_pct_broadband
        - tract_pct_housing_cost_burden_30_plus
        """
        self.log("LOADING TRACT-LEVEL CENSUS DATA", header=True)

        use_name_based = self.get_merge_strategy() == 'name_based'

        # Check for geocoded school-to-tract mapping
        tract_file = self.EXTERNAL_DIR / "school_tracts" / "school_tracts_statewide_2021.csv"
        if not tract_file.exists():
            tract_file = self.EXTERNAL_DIR / "school_tracts" / "school_tracts_fayette_2021.csv"

        if not tract_file.exists():
            self.log("  Warning: School-tract geocoding not found!")
            self.log("    Run: python etl/school_tract_geocoding.py")
            return pd.DataFrame()

        # Preserve school_id as string to maintain leading zeros
        geocoded_df = pd.read_csv(tract_file, dtype={'school_id': str}, low_memory=False)
        self.log(f"  Loaded {len(geocoded_df):,} school-tract mappings")

        # Check if ACS data is already joined
        acs_cols_present = any(col.startswith('tract_') and col not in ['tract_fips', 'tract_code']
                              for col in geocoded_df.columns)

        if acs_cols_present:
            self.log("  ACS data already joined in geocoding file")
            df = geocoded_df
        else:
            # Need to join with statewide ACS tract data
            acs_file = self.EXTERNAL_DIR / "census_acs" / "census_acs_tracts_statewide_2022.csv"
            if not acs_file.exists():
                acs_file = self.EXTERNAL_DIR / "census_acs" / "census_acs_tracts_fayette_2022.csv"

            if not acs_file.exists():
                self.log("  Warning: ACS tract data not found!")
                self.log("    Run: python etl/census_acs.py --statewide")
                # Return with key columns for matching
                if use_name_based and 'school_name' in geocoded_df.columns:
                    geocoded_df['school_name_norm'] = geocoded_df['school_name'].apply(self.normalize_name)
                    geocoded_df['district_norm'] = geocoded_df['district'].apply(self.normalize_name)
                    return geocoded_df[['school_name_norm', 'district_norm', 'tract_fips']].copy()
                return pd.DataFrame()

            acs_df = pd.read_csv(acs_file, low_memory=False)
            self.log(f"  Loaded {len(acs_df):,} ACS tract records")

            # Select relevant ACS columns
            acs_cols = ['tract_fips']
            potential_acs = [
                'median_household_income', 'poverty_rate', 'unemployment_rate',
                'pct_bachelors_plus', 'pct_single_parent', 'pct_owner_occupied',
                'pct_broadband', 'pct_housing_cost_burden_30_plus'
            ]
            for col in potential_acs:
                if col in acs_df.columns:
                    acs_cols.append(col)

            acs_subset = acs_df[acs_cols].copy()

            # Rename to tract_ prefix
            rename_map = {col: f'tract_{col}' for col in potential_acs if col in acs_subset.columns}
            acs_subset = acs_subset.rename(columns=rename_map)

            # Ensure tract_fips is string for matching
            geocoded_df['tract_fips'] = geocoded_df['tract_fips'].astype(str)
            acs_subset['tract_fips'] = acs_subset['tract_fips'].astype(str)

            # Join
            df = geocoded_df.merge(acs_subset, on='tract_fips', how='left')

            matched = df['tract_median_household_income'].notna().sum() if 'tract_median_household_income' in df.columns else 0
            self.log(f"  Matched {matched}/{len(df)} schools to ACS tract data")

        # Determine key columns based on merge strategy
        if use_name_based and 'school_name' in df.columns and 'district' in df.columns:
            df['school_name_norm'] = df['school_name'].apply(self.normalize_name)
            df['district_norm'] = df['district'].apply(self.normalize_name)
            key_cols = ['school_name_norm', 'district_norm']
            self.log("  Using name-based matching (school_name + district)")
        else:
            key_cols = ['school_id'] if 'school_id' in df.columns else []

        # Select tract-level variables for output
        tract_cols = key_cols.copy()
        potential_cols = [
            'tract_fips',
            'tract_median_household_income',
            'tract_poverty_rate',
            'tract_unemployment_rate',
            'tract_pct_bachelors_plus',
            'tract_pct_single_parent',
            'tract_pct_owner_occupied',
            'tract_pct_broadband',
            'tract_pct_housing_cost_burden_30_plus'
        ]

        for col in potential_cols:
            if col in df.columns:
                tract_cols.append(col)

        tract_df = df[tract_cols].copy()

        available_metrics = [c for c in tract_cols if c.startswith('tract_') and c != 'tract_fips']
        self.log(f"  Available tract metrics: {len(available_metrics)}")
        for col in available_metrics:
            non_null = tract_df[col].notna().sum()
            self.log(f"    - {col}: {non_null} values")

        return tract_df

    # =========================================================================
    # DATA TRANSFORMATION METHODS
    # =========================================================================

    def add_region_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add Kentucky region classification based on county."""

        def classify_region(county):
            if pd.isna(county):
                return 'Other'
            county_upper = str(county).upper().strip()

            if county_upper in self.BLUEGRASS_COUNTIES:
                return 'Bluegrass'
            elif county_upper in self.NORTHERN_COUNTIES:
                return 'Northern'
            elif county_upper in self.LOUISVILLE_COUNTIES:
                return 'Louisville'
            elif county_upper in self.APPALACHIAN_COUNTIES:
                return 'Appalachian'
            else:
                return 'Other'

        df['region'] = df['county_name'].apply(classify_region)

        self.log(f"\nRegion distribution:")
        self.log(str(df['region'].value_counts()))

        return df

    def impute_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values with mean imputation."""
        self.log("\n" + "-" * 40)
        self.log("HANDLING MISSING VALUES")
        self.log("-" * 40)

        # Demographic percentages
        pct_cols = [col for col in df.columns if col.startswith('pct_')]
        for col in pct_cols:
            missing_count = df[col].isna().sum()
            if missing_count > 0:
                mean_val = df[col].mean()
                df[col] = df[col].fillna(mean_val)
                self.log(f"  {col}: Filled {missing_count} missing with mean {mean_val:.2f}")

        # Teacher quality metrics
        teacher_cols = ['novice_teacher_rate', 'teacher_avg_experience', 'student_teacher_ratio',
                        'teacher_turnover_rate', 'emergency_provisional_rate']
        for col in teacher_cols:
            if col in df.columns:
                missing_count = df[col].isna().sum()
                if missing_count > 0:
                    mean_val = df[col].mean()
                    df[col] = df[col].fillna(mean_val)
                    self.log(f"  {col}: Filled {missing_count} missing with mean {mean_val:.2f}")

        # Financial metrics
        financial_cols = ['per_pupil_spending', 'students_per_certified_staff']
        for col in financial_cols:
            if col in df.columns:
                missing_count = df[col].isna().sum()
                if missing_count > 0:
                    mean_val = df[col].mean()
                    df[col] = df[col].fillna(mean_val)
                    self.log(f"  {col}: Filled {missing_count} missing with mean {mean_val:.2f}")

        # Census metrics (county-level)
        census_cols = ['county_median_income', 'county_poverty_rate', 'county_child_poverty_rate']
        for col in census_cols:
            if col in df.columns:
                missing_count = df[col].isna().sum()
                if missing_count > 0:
                    mean_val = df[col].mean()
                    df[col] = df[col].fillna(mean_val)
                    self.log(f"  {col}: Filled {missing_count} missing with mean {mean_val:.2f}")

        # Tract-level metrics
        tract_cols = [
            'tract_median_household_income', 'tract_poverty_rate', 'tract_unemployment_rate',
            'tract_pct_bachelors_plus', 'tract_pct_single_parent', 'tract_pct_owner_occupied',
            'tract_pct_broadband', 'tract_pct_housing_cost_burden_30_plus'
        ]
        for col in tract_cols:
            if col in df.columns:
                missing_count = df[col].isna().sum()
                if missing_count > 0:
                    mean_val = df[col].mean()
                    if pd.notna(mean_val):
                        df[col] = df[col].fillna(mean_val)
                        self.log(f"  {col}: Filled {missing_count} missing with statewide mean {mean_val:.2f}")

        return df

    # =========================================================================
    # MERGING LOGIC
    # =========================================================================

    def merge_covariates(self, outcome_df: pd.DataFrame, demo_df: pd.DataFrame,
                         teacher_df: pd.DataFrame, financial_df: pd.DataFrame,
                         census_df: pd.DataFrame, tract_df: pd.DataFrame,
                         institutional_df: pd.DataFrame = None) -> pd.DataFrame:
        """Merge all covariate DataFrames with the outcome data."""
        self.log("MERGING DATASETS", header=True)

        use_name_based = self.get_merge_strategy() == 'name_based'
        merged = outcome_df.copy()

        # Add normalized names if using name-based matching
        if use_name_based:
            merged['school_name_norm'] = merged['school_name'].apply(self.normalize_name)
            merged['district_norm'] = merged['district'].apply(self.normalize_name)

        # 1. Merge demographics
        if demo_df is not None and len(demo_df) > 0:
            self.log("\n1. Merging demographics...")

            if use_name_based:
                demo_df = demo_df.copy()
                demo_df['school_name_norm'] = demo_df['school_name'].apply(self.normalize_name)
                demo_df['district_norm'] = demo_df['district'].apply(self.normalize_name)

                demo_cols = [c for c in demo_df.columns
                            if c.startswith('pct_') or c == 'total_enrollment'
                            or c in ['year', 'school_name_norm', 'district_norm']]

                merged = merged.merge(
                    demo_df[demo_cols],
                    on=['year', 'school_name_norm', 'district_norm'],
                    how='left'
                )
            else:
                demo_cols = [c for c in demo_df.columns
                            if c.startswith('pct_') or c == 'total_enrollment'
                            or c in ['year', 'school_id']]

                merged = merged.merge(
                    demo_df[demo_cols],
                    on=['year', 'school_id'],
                    how='left'
                )

            if 'pct_economically_disadvantaged' in merged.columns:
                n_matched = merged['pct_economically_disadvantaged'].notna().sum()
                self.log(f"   Demographic matches: {n_matched} / {len(merged)} ({100*n_matched/len(merged):.1f}%)")
        else:
            self.log("\n1. Skipping demographics (not available)")

        # 2. Merge teacher quality metrics
        if teacher_df is not None and len(teacher_df) > 0:
            self.log("\n2. Merging teacher quality metrics...")

            if use_name_based:
                merge_keys = ['year', 'school_name_norm', 'district_norm']
            else:
                merge_keys = ['year', 'school_id', 'district']

            # Select only valid columns
            teacher_cols = [c for c in teacher_df.columns if c not in merge_keys or c in merge_keys]

            merged = merged.merge(
                teacher_df,
                on=merge_keys,
                how='left'
            )

            for col in ['novice_teacher_rate', 'teacher_avg_experience', 'student_teacher_ratio',
                        'teacher_turnover_rate', 'emergency_provisional_rate']:
                if col in merged.columns:
                    n_matched = merged[col].notna().sum()
                    self.log(f"   {col}: {n_matched} / {len(merged)} ({100*n_matched/len(merged):.1f}%)")
        else:
            self.log("\n2. Skipping teacher metrics (not available)")

        # 3. Merge financial metrics (district-level)
        if financial_df is not None and len(financial_df) > 0:
            self.log("\n3. Merging financial metrics (district-level)...")

            fin_cols = ['year', 'district']
            if 'per_pupil_spending' in financial_df.columns:
                fin_cols.append('per_pupil_spending')
            if 'students_per_certified_staff' in financial_df.columns:
                fin_cols.append('students_per_certified_staff')

            fin_subset = financial_df[fin_cols].drop_duplicates()
            merged = merged.merge(
                fin_subset,
                on=['year', 'district'],
                how='left'
            )

            for col in ['per_pupil_spending', 'students_per_certified_staff']:
                if col in merged.columns:
                    n_matched = merged[col].notna().sum()
                    self.log(f"   {col}: {n_matched} / {len(merged)} ({100*n_matched/len(merged):.1f}%)")
        else:
            self.log("\n3. Skipping financial metrics (not available)")

        # 4. Merge Census county data
        if census_df is not None and len(census_df) > 0:
            self.log("\n4. Merging Census economic data (county-level)...")

            merged['county_name_upper'] = merged['county_name'].str.upper().str.strip()

            # Use most recent Census vintage available
            census_recent = census_df.sort_values('data_vintage', ascending=False).drop_duplicates('county_name_upper')

            census_cols = ['county_name_upper']
            for col in ['county_median_income', 'county_poverty_rate', 'county_child_poverty_rate']:
                if col in census_recent.columns:
                    census_cols.append(col)

            merged = merged.merge(
                census_recent[census_cols],
                on='county_name_upper',
                how='left'
            )
            merged = merged.drop(columns=['county_name_upper'])

            for col in ['county_median_income', 'county_poverty_rate', 'county_child_poverty_rate']:
                if col in merged.columns:
                    n_matched = merged[col].notna().sum()
                    self.log(f"   {col}: {n_matched} / {len(merged)} ({100*n_matched/len(merged):.1f}%)")
        else:
            self.log("\n4. Skipping Census county data (not available)")

        # 5. Merge tract-level data
        if tract_df is not None and len(tract_df) > 0:
            self.log("\n5. Merging tract-level neighborhood data...")

            if use_name_based and 'school_name_norm' in tract_df.columns:
                merged = merged.merge(
                    tract_df,
                    on=['school_name_norm', 'district_norm'],
                    how='left'
                )
            elif 'school_id' in tract_df.columns:
                # Normalize school_id to integer string (remove float decimal, e.g., "1010.0" -> "1010")
                # This handles cases where school_id is float64 due to NaN values in other merges
                def normalize_school_id(x):
                    if pd.isna(x):
                        return None
                    return str(int(float(x)))

                merged['school_id'] = merged['school_id'].apply(normalize_school_id)
                tract_df['school_id'] = tract_df['school_id'].apply(normalize_school_id)
                merged = merged.merge(
                    tract_df,
                    on='school_id',
                    how='left'
                )

            tract_cols_available = [c for c in tract_df.columns if c.startswith('tract_') and c != 'tract_fips']
            if tract_cols_available:
                matched = merged[tract_cols_available[0]].notna().sum()
                self.log(f"   Matched {matched}/{len(merged)} schools to tract data ({100*matched/len(merged):.1f}%)")
        else:
            self.log("\n5. Skipping tract-level data (not available)")

        # 6. Merge institutional characteristics (Title I, School Type dummies)
        if institutional_df is not None and len(institutional_df) > 0:
            self.log("\n6. Merging institutional characteristics...")

            # Normalize school_id for matching
            def normalize_school_id(x):
                if pd.isna(x):
                    return None
                return str(int(float(x)))

            merged['school_id'] = merged['school_id'].apply(normalize_school_id)
            institutional_df = institutional_df.copy()
            institutional_df['school_id'] = institutional_df['school_id'].apply(normalize_school_id)

            merged = merged.merge(
                institutional_df,
                on=['year', 'school_id'],
                how='left'
            )

            inst_cols = [c for c in institutional_df.columns if c.startswith('title_i_') or c.startswith('school_type_')]
            for col in inst_cols:
                if col in merged.columns:
                    # Fill NaN with 0 (reference category)
                    merged[col] = merged[col].fillna(0).astype(int)
                    n_ones = merged[col].sum()
                    self.log(f"   {col}: {n_ones} / {len(merged)} ({100*n_ones/len(merged):.1f}%)")
        else:
            self.log("\n6. Skipping institutional characteristics (not available)")

        return merged

    # =========================================================================
    # VALIDATION AND OUTPUT
    # =========================================================================

    def validate_dataset(self, df: pd.DataFrame) -> None:
        """Validate the analysis dataset and print summary statistics."""
        self.log("DATASET SUMMARY", header=True)

        self.log(f"  Years: {sorted(df['year'].unique())}")
        self.log(f"  Schools: {df['school_id'].nunique()}")
        self.log(f"  Observations: {len(df)}")

        if 'is_fayette' in df.columns:
            self.log(f"  Fayette County schools: {df['is_fayette'].sum()}")

        self.log(f"\n  Outcome ({self.OUTCOME_NAME}):")
        self.log(f"    Mean: {df[self.OUTCOME_NAME].mean():.1f}")
        self.log(f"    Std: {df[self.OUTCOME_NAME].std():.1f}")
        self.log(f"    Range: [{df[self.OUTCOME_NAME].min():.1f}, {df[self.OUTCOME_NAME].max():.1f}]")

        # Report on covariates
        self.log("\n" + "-" * 40)
        self.log("COVARIATE SUMMARY")
        self.log("-" * 40)

        demo_cols = [c for c in df.columns if c.startswith('pct_')]
        self.log(f"\nDemographic Covariates ({len(demo_cols)}):")
        for col in demo_cols[:5]:
            self.log(f"  {col}: mean={df[col].mean():.2f}, std={df[col].std():.2f}")
        if len(demo_cols) > 5:
            self.log(f"  ... and {len(demo_cols) - 5} more")

        teacher_cols = [c for c in ['novice_teacher_rate', 'teacher_avg_experience', 'student_teacher_ratio',
                                     'teacher_turnover_rate', 'emergency_provisional_rate'] if c in df.columns]
        self.log(f"\nTeacher Quality Covariates ({len(teacher_cols)}):")
        for col in teacher_cols:
            self.log(f"  {col}: mean={df[col].mean():.2f}, std={df[col].std():.2f}")

        financial_cols = [c for c in ['per_pupil_spending', 'students_per_certified_staff'] if c in df.columns]
        self.log(f"\nFinancial Covariates ({len(financial_cols)}):")
        for col in financial_cols:
            self.log(f"  {col}: mean={df[col].mean():.2f}, std={df[col].std():.2f}")

        census_cols = [c for c in ['county_median_income', 'county_poverty_rate', 'county_child_poverty_rate'] if c in df.columns]
        self.log(f"\nCensus Economic - County Level ({len(census_cols)}):")
        for col in census_cols:
            self.log(f"  {col}: mean={df[col].mean():.2f}, std={df[col].std():.2f}")

        tract_cols = [c for c in df.columns if c.startswith('tract_') and c != 'tract_fips']
        self.log(f"\nCensus Economic - Tract Level ({len(tract_cols)}):")
        for col in tract_cols[:5]:
            vals = df[col].dropna()
            if len(vals) > 0:
                self.log(f"  {col}: mean={vals.mean():.2f}, std={vals.std():.2f}")
        if len(tract_cols) > 5:
            self.log(f"  ... and {len(tract_cols) - 5} more")

        inst_cols = [c for c in df.columns if c.startswith('title_i_') or c.startswith('school_type_')]
        if inst_cols:
            self.log(f"\nInstitutional Characteristics ({len(inst_cols)}):")
            for col in inst_cols:
                n_ones = df[col].sum()
                self.log(f"  {col}: {n_ones} schools ({100*n_ones/len(df):.1f}%)")

    # =========================================================================
    # MAIN WORKFLOW
    # =========================================================================

    def create_dataset(self) -> pd.DataFrame:
        """
        Main workflow to create the analysis dataset.

        Steps:
        1. Load outcome data
        2. Load all covariates
        3. Merge datasets
        4. Add derived features (region, is_fayette)
        5. Impute missing values
        6. Validate and save
        """
        self.log(f"CREATING {self.OUTCOME_NAME.upper().replace('_', ' ')} ANALYSIS DATASET", header=True)

        # Load all data sources
        outcome_df = self.load_outcome_data()
        demo_df = self.load_demographics()
        teacher_df = self.load_teacher_quality_metrics()
        financial_df = self.load_financial_metrics()
        census_df = self.load_census_county_data()
        tract_df = self.load_tract_level_data()
        institutional_df = self.load_institutional_characteristics()

        # Merge all datasets
        merged = self.merge_covariates(
            outcome_df, demo_df, teacher_df, financial_df, census_df, tract_df,
            institutional_df
        )

        # Add region classification
        merged = self.add_region_mapping(merged)

        # Add Fayette County indicator (check both district name and county_name)
        is_fayette_district = merged['district'] == 'Fayette County'
        is_fayette_county = merged['county_name'].str.upper() == 'FAYETTE' if 'county_name' in merged.columns else False
        merged['is_fayette'] = (is_fayette_district | is_fayette_county).astype(int)
        self.log(f"\nFayette County schools: {merged['is_fayette'].sum()}")

        # Handle missing values
        merged = self.impute_missing_values(merged)

        # Clean up temporary columns
        for col in ['school_name_norm', 'district_norm']:
            if col in merged.columns:
                merged = merged.drop(columns=[col])

        # Validate
        self.validate_dataset(merged)

        # Save
        output_file = self.OUTPUT_DIR / self.OUTPUT_FILENAME
        merged.to_csv(output_file, index=False)
        self.log(f"\nSaved: {output_file}")

        return merged
