# Journal Entry 34: Safe Schools Discipline Pipeline Development

## Objective
Develop a new ETL pipeline for safe schools discipline data that combines multiple files:
- KYRC24_SAFE_Discipline_Resolutions.csv 
- KYRC24_SAFE_Legal_Sanctions.csv
- Historical safe_schools_discipline_[year].csv files

## Pipeline Requirements
- Generate ~6 KPI metrics: suspension rates, expulsion rates, arrest rates, SRO involvement
- Follow standard KPI output format with 10 columns
- Use metric naming convention: {indicator}_rate_{period}
- Determine if BaseETL class can be used or if custom structure needed (like first safe schools pipeline)

## Development Plan
1. Create data/raw/safe_schools_discipline folder
2. Copy source files from downloads directory
3. Analyze file structures to determine ETL approach
4. Implement pipeline with comprehensive error handling
5. Create unit and e2e tests following existing patterns
6. Test and validate output

## Implementation Notes

### Pipeline Architecture
- Successfully extended BaseETL class for consistent processing
- Combined multiple file types: KYRC24 discipline resolutions, KYRC24 legal sanctions, and historical files
- Generated 95,935 KPI records from 6 source files covering years 2020-2024

### Key Technical Decisions
1. **BaseETL Integration**: Used standard BaseETL transform patterns instead of custom pipeline logic
2. **Metric Focus**: Generated rate-based metrics only (excluded zero-value rates to reduce data size)
3. **Column Mapping**: Comprehensive mapping for both current KYRC24 and historical column formats
4. **Year Extraction**: BaseETL automatically extracts correct year from school_year field

### Generated Metrics
- **Discipline Rates**: corporal_punishment_rate, restraint_rate, seclusion_rate, expelled_not_receiving_services_rate, expelled_receiving_services_rate, in_school_removal_rate, out_of_school_suspension_rate, removal_by_hearing_officer_rate, unilateral_removal_rate
- **Legal Sanction Rates**: arrest_rate, charges_rate, civil_proceedings_rate, court_designated_worker_rate, school_resource_officer_rate

### Data Quality Results
- **Top Metrics by Volume**: out_of_school_suspension_rate (40,302 records), in_school_removal_rate (38,707 records), restraint_rate (10,114 records)
- **Year Coverage**: 2020-2024 with proper longitudinal consistency
- **Demographic Coverage**: 95,935 total records across all student groups
- **Data Validation**: All rates between 0-100%, proper school ID extraction

### Files Processed
- `KYRC24_SAFE_Discipline_Resolutions.csv` → 21,339 KPI rows
- `KYRC24_SAFE_Legal_Sanctions.csv` → 3,775 KPI rows  
- `safe_schools_discipline_2020.csv` → 20,467 KPI rows
- `safe_schools_discipline_2021.csv` → 10,024 KPI rows
- `safe_schools_discipline_2022.csv` → 19,365 KPI rows
- `safe_schools_discipline_2023.csv` → 20,965 KPI rows

### Testing Results
- **Unit Tests**: 10/10 passing - comprehensive coverage of ETL class methods
- **E2E Tests**: 7/7 passing - validated end-to-end pipeline functionality
- **Test Coverage**: Individual file processing, combined file processing, edge cases

### Output Files
- Main KPI data: `data/processed/safe_schools_discipline.csv`
- Demographic audit: `data/processed/safe_schools_discipline_demographic_audit.csv`

## Completion Status
✅ Pipeline successfully developed, tested, and validated
✅ All tests passing
✅ Comprehensive documentation completed