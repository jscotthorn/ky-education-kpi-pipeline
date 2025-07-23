# Kindergarten Readiness KPI Review

## Date
2025-07-21

## Objective
Examine how `etl/kindergarten_readiness.py` maps source fields to KPIs and identify potential additional KPIs that could be exposed from the raw data.

## Current KPI Generation
- The ETL normalizes various column names such as `Total Percent Ready`, `Ready With Interventions`, `Ready`, `Ready With Enrichments`, `Total Ready`, and `Number Tested`.
- `clean_readiness_data()` coerces these columns to numeric, ensuring rates remain between 0 and 100 and counts are non‑negative.
- `extract_metrics()` handles two formats:
  - **Count format (2024)** – when all three readiness component columns are present. It sums the components to derive `kindergarten_readiness_total`, uses `Total Ready` as `kindergarten_readiness_count`, and calculates `kindergarten_readiness_rate` as `count / total * 100`.
  - **Percentage format (2021‑2023)** – uses `Total Percent Ready` (or `Total Ready` when labeled as a rate) as `kindergarten_readiness_rate`. When `Number Tested` exists, it becomes `kindergarten_readiness_total` and the count is derived as `rate/100 * total`.
- Suppressed rows emit NA for the three metrics.

## Potential Additional KPIs
Based on the columns handled in the pipeline and notes from the raw data page, several other metrics could be created:
1. **Readiness Component Breakdowns**
   - Counts and rates for `Ready With Interventions`, `Ready`, and `Ready With Enrichments` would provide insight into the type of readiness support students required.
   - A complementary "Not Ready" metric (total tested minus total ready) could also be derived when counts are available.
2. **Prior Setting Analysis**
   - The raw files include a `Prior Setting` column (e.g., child care, Head Start). Currently the pipeline filters to `All Students`; exposing KPIs by prior setting could highlight the impact of early childhood experiences on readiness.
3. **Developmental Domain Scores**
   - The dataset list shows a separate file `KYRC24_ASMT_Kindergarten_Screen_Developmental_Domains.csv` that is not yet processed. Domain‑level metrics (language, literacy, etc.) would expand the view beyond the composite score.

These additions would require updating the configuration to include the domain file and extending `extract_metrics()` to map the extra fields.
