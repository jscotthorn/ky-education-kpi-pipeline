# Out-of-School Suspension KPI Mapping Review

## Date: 2025-07-23

## Overview
Summarized how `etl/out_of_school_suspension.py` derives KPIs from source columns and identified unused data that could support additional metrics.

## Current KPI Generation
- **KYRC24 Format** (`KYRC24_OVW_Student_Suspensions.csv`)
  - `Single Out-of-School With Disabilities` → `out_of_school_suspension_single_with_disabilities_count`
  - `Single Out-of-School Without Disabilities` → `out_of_school_suspension_single_without_disabilities_count`
  - `Multiple Out-of-School With Disabilities` → `out_of_school_suspension_multiple_with_disabilities_count`
  - `Multiple Out-of-School Without Disabilities` → `out_of_school_suspension_multiple_without_disabilities_count`
  - Single + Multiple totals are summed to create `*_single_total_count`, `*_multiple_total_count`, and `out_of_school_suspension_total_count`.
- **Historical Safe Schools Format** (`safe_schools_discipline_[year].csv`)
  - `OUT OF SCHOOL SUSPENSION SSP3` → `out_of_school_suspension_count`
- Suppression logic marks rows as suppressed when all out-of-school columns are missing.

## Unused Source Columns
The column mapping table exposes several fields not currently transformed into KPIs:
- In-school suspension counts (`In-School With Disabilities`, `In-School Without Disabilities`)
- Discipline resolution counts (`Expelled Receiving Services SSP1`, `Expelled Not Receiving Services SSP2`, `Corporal Punishment SSP5`, `In-School Removal INSR`, `Restraint SSP7`, `Seclusion SSP8`, `Unilateral Removal by School Personnel IAES1`, `Removal by Hearing Officer IAES2`)
- `Total Discipline Resolutions` (could be used for rate calculations)

These are leveraged by the separate `safe_schools_discipline` pipeline to produce rates, but raw counts are not output anywhere.

## Potential Additional KPIs
1. **In-School Suspension Counts** – capturing totals by disability status could provide complementary context to out-of-school counts.
2. **Expulsion and Removal Counts** – direct counts for expulsions or removals might be valuable alongside the rate metrics currently produced in the discipline pipeline.
3. **Discipline Resolution Totals** – publishing total resolution counts would enable independent rate calculations or trend analysis outside the ETL.

Adding these metrics would require extending either this pipeline or the safe schools discipline pipeline, ensuring adherence to the existing naming convention (`{indicator}_count`).
