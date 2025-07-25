# Journal Entry 40: Safe Schools Discipline KPI Mapping Review

## Objective
Summarize how the `safe_schools_discipline` pipeline converts source columns into KPI metrics and identify potential additional metrics that could be surfaced.

## Current KPI Mapping
- Input files include:
  - `KYRC24_SAFE_Discipline_Resolutions.csv`
  - `KYRC24_SAFE_Legal_Sanctions.csv`
  - Historical `safe_schools_discipline_<year>.csv`
- `module_column_mappings` normalizes discipline resolution and legal sanction columns. Key mappings include:
  - `Corporal Punishment (SSP5)` → `corporal_punishment_count`
  - `Restraint (SSP7)` → `restraint_count`
  - `Seclusion (SSP8)` → `seclusion_count`
  - `Expelled, Not Receiving Services (SSP2)` → `expelled_not_receiving_services_count`
  - `Expelled, Receiving Services (SSP1)` → `expelled_receiving_services_count`
  - `In-School Removal (INSR) Or In-District Removal (INDR) >=.5` → `in_school_removal_count`
  - `Out-Of-School Suspensions (SSP3)` → `out_of_school_suspension_count`
  - `Removal By Hearing Officer (IAES2)` → `removal_by_hearing_officer_count`
  - `Unilateral Removal By School Personnel (IAES1)` → `unilateral_removal_count`
  - Legal sanction columns such as `Arrests`, `Charges`, `Civil Proceedings`, `Court Designated Worker Involvement`, `School Resource Officer Involvement` map to corresponding `_count` fields.
- In `extract_metrics`, the pipeline calculates rate metrics for each count using the row total (`Total` or `Total Discipline Resolutions`) as the denominator.
- Only non-zero rates are emitted, following the naming convention `{indicator}_rate`.
- Suppressed rows output all rate metrics with `NA` values via `get_suppressed_metric_defaults`.

## Potential Additional KPIs
The source files also contain raw counts and totals that are currently only used to compute rates. Exposing these values would allow absolute comparisons alongside percentage rates. Possible new metrics include:
- Counts for each discipline resolution and legal sanction (e.g., `restraint_count`, `arrest_count`).
- Total discipline resolutions per demographic group (`discipline_resolutions_total_count`).
- Totals for legal sanctions if present (`legal_sanctions_total_count`).

Providing both rates and counts could help stakeholders gauge overall volume of disciplinary actions in addition to proportional impact.

