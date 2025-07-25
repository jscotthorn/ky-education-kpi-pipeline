# Safe Schools Climate KPI Mapping Review

## Date: 2025-07-22

## Overview
Review of how the `safe_schools_climate` pipeline generates KPIs from source files and identification of additional data that could be surfaced as metrics.

## Current KPI Mappings
1. **climate_index_score**
   - Sourced from `CLIMATE INDEX` column in `KYRC24_ACCT_Index_Scores.csv` and similar files.
   - For survey result files (`KYRC24_ACCT_Survey_Results.csv`), mapped from `Question Index` when `Question Type` equals `climate`.
   - For accountability profile files, derived from `QUALITY OF SCHOOL CLIMATE AND SAFETY COMBINED INDICATOR RATE`.
2. **safety_index_score**
   - Sourced from `SAFETY INDEX` column in index score files.
   - For survey results, mapped from `Question Index` when `Question Type` equals `safety`.
   - Also derived from the combined indicator rate in accountability profiles.
3. **safety_policy_compliance_rate**
   - Calculated across eight yes/no policy questions in precautionary measures files (e.g., `KYRC24_SAFE_Precautionary_Measures.csv`).
   - Uses percent of "Yes" responses to produce overall policy compliance.

## Potential Additional KPIs
- Individual policy compliance metrics for each yes/no question (visitors sign-in, presence of a School Resource Officer, mental health referral process, etc.).
- `Agree / Strongly Agree` percentages from survey results, either per question or aggregated by `Question Type`.
- `QUALITY OF SCHOOL CLIMATE AND SAFETY STATUS` and `STATUS RATING` fields from accountability profile data.
- Survey response rate metrics if participation counts are available.

These fields already exist in the raw files and could provide deeper insight if exposed through new KPIs.
