# Safe Schools Climate Raw Metrics Exposed

## Date: 2025-07-24

## Overview
Extended the `safe_schools_climate` pipeline so that every column from the raw files is preserved as a KPI metric. Derivative calculations such as policy compliance rates are no longer produced.

## Key Changes
- `extract_metrics` now returns each normalized column value directly.
- `get_suppressed_metric_defaults` supplies `pd.NA` for every available raw column.
- Custom `convert_to_kpi_format` keeps string and numeric values without conversion.
- Updated unit and end-to-end tests to expect raw metrics instead of derived scores.

This approach surfaces all source data for analysis while keeping the pipeline logic simple.
