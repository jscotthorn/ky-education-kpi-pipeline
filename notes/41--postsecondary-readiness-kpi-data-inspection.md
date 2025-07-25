# Postsecondary Readiness KPI Data Inspection

**Date:** 2025-07-24

## Goal
Review the raw postsecondary readiness files to confirm if additional metrics beyond the two readiness rates exist.

## Process
1. Installed dependencies with `pip install -e .` and `pip install requests pyyaml`.
2. Downloaded raw files using `python3 data/prepare_kde_data.py postsecondary_readiness`.
3. Examined the headers of `KYRC24_ACCT_Postsecondary_Readiness.csv`, `postsecondary_readiness_2023.csv`, and `postsecondary_readiness_2022.csv`.

## Findings
- Each file contains the same 17 columns.
- Only metric fields are `Postsecondary Rate` and `Postsecondary Rate With Bonus`.
- No counts, totals, or component-level indicators are present in these datasets.

## Conclusion
The current pipeline correctly exposes all available readiness metrics from the source files. Additional KPIs such as academic/career counts are not derivable from these CSVs without supplemental data.
