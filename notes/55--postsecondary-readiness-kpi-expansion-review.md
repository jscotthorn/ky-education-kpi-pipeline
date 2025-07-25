# Postsecondary Readiness KPI Expansion Review

**Date:** 2025-07-23

## Overview
Review of `etl/postsecondary_readiness.py` to understand current KPI mapping and explore potential additional metrics from the raw files.

## Current KPI Mapping
- Source columns `Postsecondary Rate` or `POSTSECONDARY RATE` → metric `postsecondary_readiness_rate`.
- Source columns `Postsecondary Rate With Bonus` or `POSTSECONDARY RATE WITH BONUS` → metric `postsecondary_readiness_rate_with_bonus`.
- The pipeline always emits two KPI rows per source record: one for the base rate and one for the bonus rate. Suppressed rows are retained with `value=NaN`.
- No counts or totals are currently produced—only rate metrics are extracted.

## Potential Additional KPIs
While the open datasets referenced in `data/raw/postsecondary_readiness/index.html` mention assessment scores and technical indicators, the current ETL only exposes the two readiness rates. The CSV files may include fields such as:
- Numbers of students meeting the academic indicator
- Numbers of students meeting the career/technical indicator
- Total graduates or participants used as denominators
- Specific assessment metrics (e.g., ACT benchmark met, industry certifications earned)

Exposing these counts would enable calculation of readiness counts and rates by component. If available, potential KPIs might include:
1. `postsecondary_readiness_academic_count` and `_total`
2. `postsecondary_readiness_career_count` and `_total`
3. Counts for ACT benchmark, dual credit, or industry certification attainment

These additional metrics would complement the existing rates and provide more granular insight into how students achieve readiness.

## Next Steps
1. Inspect the raw CSV headers to confirm whether counts or component indicators are present.
2. If so, extend `PostsecondaryReadinessETL.extract_metrics` to return these additional fields using the same KPI naming convention.
3. Update tests and documentation to cover new metrics.
