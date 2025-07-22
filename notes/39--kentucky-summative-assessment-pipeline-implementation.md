# 39 - Kentucky Summative Assessment Pipeline Implementation

**Date**: 2025-07-22

## Overview
Implemented a new ETL pipeline to process Kentucky Summative Assessment (KSA) files. Added raw data preparation via the existing downloader and created unit and integration tests.

## Key Points
- Mapped multiple KSA file formats (2023-2024) to common columns.
- Normalized subject and grade/level values for metric naming.
- Created metrics for each performance level and content index.
- Output metrics by both grade and school level for cross-year consistency.
- Added unit tests and end-to-end test verifying KPI output.
- Installed missing dependencies (`ruamel.yaml`, `tabulate`).
