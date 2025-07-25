# Journal Entry 41: Safe Schools Discipline Counts

## Objective
Add raw count and total metrics to the safe schools discipline pipeline.

## Changes
- Modified `extract_metrics` to output count metrics and total enrollment values for both discipline resolutions and legal sanctions.
- Updated suppressed metric defaults to include new metrics.
- Adjusted integration tests to validate rates separately from counts.
- Added unit tests for count extraction and zero-total handling.

Counts now accompany rate metrics, providing absolute volume alongside percentages.
