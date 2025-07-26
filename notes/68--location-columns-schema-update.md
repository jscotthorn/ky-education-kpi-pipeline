# Location Columns Schema Update

The KPI schema was updated to place the new location fields in a standardized order and all modules now populate these values when available.

## Changes
- Defined `KPI_COLUMNS` constant with final order starting with `year` and `metric`.
- Updated ETL runner to cast ID fields to numeric for consistent CSV/Parquet output.
- Documented column descriptions in `KPIS.md`.
