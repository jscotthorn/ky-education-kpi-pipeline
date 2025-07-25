# Safe Schools Climate Precautionary Measures Historical Data

## Date: 2025-07-22

## Overview
Extended the safe_schools_climate pipeline to support precautionary measures files prior to 2024. Older files use different column labels, so additional mappings and tests were required.

## Key Changes
- Added synonym column mappings in `safe_schools_climate.py` for older precautionary files.
- Updated unit tests to verify synonym handling and added an end-to-end sample with a 2022 file.
- Inserted `safe_schools_climate` in `config/mappings.yaml` so etl_runner processes the module.

## Testing
- `pytest tests/test_safe_schools_climate.py -v`
- `pytest tests/test_safe_schools_climate_end_to_end.py -v`
