# Out-of-School Suspension Additional Metrics Implementation

## Date: 2025-07-23

## Overview
Implemented new KPI outputs in the `out_of_school_suspension` pipeline to capture
additional counts present in the raw Safe Schools discipline data. Added unit and
end-to-end tests verifying the new metrics.

## Key Changes
- Numeric cleaning now handles in-school suspension, expulsion, and discipline
  resolution fields.
- `extract_metrics` emits counts for in-school suspensions, expulsions, removals,
  and total discipline resolutions when columns are present.
- Suppression defaults include these new metrics.
- Tests updated to cover additional metric extraction and end-to-end generation.
