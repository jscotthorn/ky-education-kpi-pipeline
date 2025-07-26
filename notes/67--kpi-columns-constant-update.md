# KPI Columns Constant Update

## Overview
Added a shared `KPI_COLUMNS` constant under `etl/constants.py` and refactored ETL modules, runner utilities, and tests to reference this list. This ensures consistent column ordering across the project and simplifies future schema changes.

## Key Changes
- New `etl/constants.py` defining the 19 standard KPI columns
- Updated ETL modules and `etl_runner` to import the constant
- Adjusted Safe Schools Events derived rate logic to retain location fields
- Revised tests to use `KPI_COLUMNS` and updated sample data accordingly
