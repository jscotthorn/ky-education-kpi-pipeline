# 40 - Graduation Rates KPI Expansion Analysis

**Date**: 2025-07-23

## Overview
Brief review of `etl/graduation_rates.py` focusing on how KPI values are produced from the raw KDE files and what additional data could be surfaced.

## Current KPI Mapping
- The module extends `BaseETL` and defines column mappings for suppression flags, rate values, and cohort counts.
- Metrics extracted from each row are:
  - `graduation_rate_4_year`
  - `graduation_count_4_year`
  - `graduation_total_4_year`
  - `graduation_rate_5_year`
  - `graduation_count_5_year`
  - `graduation_total_5_year`
- Suppressed records receive NA values for all KPIs.
- The ETL assigns a simple `data_source` label based on which columns are present (2021_detailed, 2022_2023_standard, or 2024_simplified).

## Source File Summary
- Historical analysis shows schema changes across years:
  - **2021** files contain both 4- and 5-year rates plus cohort counts and separate suppression flags.
  - **2022-2023** files drop the cohort counts but still provide both rates and suppression indicators.
  - **2024** files simplify further with only the 4-year rate.
- The config lists six raw files for the module including the new `KYRC24_ACCT_5_Year_High_School_Graduation.csv`.

## Potential Additional KPIs
- The raw files include columns like `co_op`, `co_op_code`, and `school_type` that are currently ignored—these are categorical attributes rather than numeric metrics.
- Some KDE documentation references “Crosstab Cohort” files that offer intersectional 4- and 5-year graduation rates. Integrating those would produce additional metrics by gender×race×economic status×disability groups.
- No other numeric fields appear in the sampled files that could directly translate to new KPIs beyond the existing rate, count, and total metrics. Calculated metrics (e.g., non-graduates) could be derived from the counts but may overlap with dropout datasets.

**Conclusion**: The pipeline currently exposes all numeric graduation metrics available in the base files. Future KPI expansion could come from the cross‑tabulated graduation datasets rather than the existing files themselves.
