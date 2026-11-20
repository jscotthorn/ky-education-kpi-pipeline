# Financial Summary Pipeline Implementation

**Date**: 2025-11-24

## Summary

Implemented a new ETL pipeline for the KDE Financial Summary dataset, which provides district-level financial and staffing metrics. This addresses the "Student Support Ratios" priority item from the Bright Spots Data Pipeline Priorities document, though the KDE data does not contain specific support staff roles (counselors, librarians, nurses) - only aggregate certified/classified staff counts.

## Data Source Analysis

### Available KDE Data
The financial_summary files from KDE contain:
- **End-of-Year Student Membership**: District enrollment counts
- **Fund Balance**: District fund balance in dollars and as percentage
- **Certified Staff FTE**: Total certified staff and teachers separately
- **Classified Staff FTE**: Non-certified staff

### Data NOT Available
The KDE historical datasets do NOT include specific support staff breakdowns:
- Counselor FTE
- Librarian FTE
- Nurse FTE
- Social Worker FTE

These would require a different data source (possibly direct district reporting or NCES data).

## Files Created/Modified

### New Files
- `etl/financial_summary.py` - Main ETL pipeline
- `tests/test_financial_summary.py` - Unit tests (12 tests)
- `tests/test_financial_summary_end_to_end.py` - End-to-end tests (8 tests)

### Modified Files
- `config/kde_sources.yaml` - Added financial_summary data source configuration
- `config/mappings.yaml` - Added financial_summary to pipeline list
- `KPIS.md` - Documented new KPIs

## KPIs Generated

### Primary Metrics (from source data)
1. `eoy_student_membership` - End-of-year student count
2. `fund_balance` - Fund balance in dollars
3. `fund_balance_pct` - Fund balance percentage
4. `certified_staff_fte` - Total certified staff FTE
5. `certified_staff_teachers_fte` - Teacher FTE only
6. `classified_staff_fte` - Classified staff FTE

### Derived Metrics (calculated)
7. `certified_staff_non_teachers_fte` - Non-teacher certified staff (certified_total - teachers)
8. `total_staff_fte` - All staff (certified + classified)

## Data Coverage

| Year | Districts | Records |
|------|-----------|---------|
| 2020 | 171 | 1,368 |
| 2021 | 171 | 1,368 |
| 2022 | 171 | 1,367 |
| 2023 | 171 | 1,368 |
| 2024 | 171 | 1,366 |
| **Total** | - | **6,837** |

## Schema Variations Handled

### KYRC24 Format (2024)
- Title case column names: "End-of-Year Student Membership", "Certified Staff"
- Values with commas: "636,427", "52,601"

### Historical Format (2020-2023)
- Uppercase column names: "MEMBERSHIP", "FTE CERTIFIED STAFF"
- Some values with commas, some without
- 2020 file missing SCHOOL TYPE column

## Test Results

```
tests/test_financial_summary.py - 12 passed
tests/test_financial_summary_end_to_end.py - 8 passed
Total: 20 passed
```

## Usage

```bash
# Download raw data
python3 data/prepare_kde_data.py financial_summary

# Run pipeline
python3 etl/financial_summary.py

# Or run as part of full ETL
python3 etl_runner.py
```

## Future Considerations

1. **Support Staff Data**: If specific support staff ratios are needed, investigate:
   - NCES School/District staffing reports
   - Direct KDE requests for staff detail data
   - District-level MUNIS/financial system exports

2. **Student-to-Staff Ratios**: Could calculate ratios by combining with student enrollment data:
   - `certified_staff_per_100_students = (certified_staff_fte / eoy_student_membership) * 100`

3. **Fund Balance Analysis**: The fund balance data could be useful for:
   - Identifying financially stressed districts
   - Analyzing relationship between reserves and outcomes
