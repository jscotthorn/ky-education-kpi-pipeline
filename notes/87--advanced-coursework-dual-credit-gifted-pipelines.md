# Advanced Coursework, Dual Credit, and Gifted/Talented Pipeline Implementation

**Date**: 2025-11-24

## Overview

Implemented three ETL pipelines to extract educational opportunity indicators from KDE datasets:

1. **Advanced Coursework** - AP, IB, Cambridge, and dual credit course participation/performance
2. **Dual Credit** - Dedicated pipeline for dual credit course data
3. **Gifted and Talented** - Gifted program participation by grade level

## Data Sources

### Advanced Coursework
- `KYRC25_EDOP_Advanced_Courses_Participation_and_Performance.csv`
- `KYRC24_EDOP_Advanced_Courses_Participation_and_Performance.csv`
- `advanced_courses_participation_and_performance_2023.csv`
- `advanced_courses_participation_and_performance_2022.csv`
- `advanced_courses_participation_and_performance_2021.csv`
- `KYRC25_OVW_Advanced_Coursework.csv`
- `KYRC24_OVW_Advanced_Coursework.csv`

### Dual Credit
- `KYRC25_EDOP_Dual_Credit_Participation_and_Performance.csv`
- `KYRC24_EDOP_Dual_Credit_Participation_and_Performance.csv`
- `KYRC25_EDOP_Dual_Credit_Courses_Offered.csv`
- `KYRC24_EDOP_Dual_Credit_Courses_Offered.csv`

### Gifted and Talented
- `KYRC25_EDOP_Gifted_Participation_by_Grade_Level.csv`
- `KYRC24_EDOP_Gifted_Participation_by_Grade_Level.csv`
- `gifted_and_talented_2023.csv`
- `gifted_and_talented_2022.csv`
- `gifted_and_talented_2021.csv`

## KPIs Generated

### Advanced Coursework
| Metric | Description |
|--------|-------------|
| `{course_type}_enrollment_count` | Students enrolled in courses |
| `{course_type}_completer_count` | Students completing courses |
| `{course_type}_tested_count` | Students tested |
| `{course_type}_qualifying_score_count` | Students with qualifying scores |
| `{course_type}_qualifying_score_rate` | Percentage with qualifying scores |

Course types: `ap`, `ib`, `cambridge`, `dual_credit`, `advanced_coursework_total`

### Dual Credit
| Metric | Description |
|--------|-------------|
| `dual_credit_enrollment_count` | Students enrolled |
| `dual_credit_completer_count` | Students completing |
| `dual_credit_qualifying_score_count` | Students with qualifying scores |
| `dual_credit_completion_rate` | Completion percentage |

### Gifted and Talented
| Metric | Description |
|--------|-------------|
| `gifted_participation_count_all_grades` | Total gifted students |
| `gifted_participation_count_kindergarten` | Kindergarten gifted students |
| `gifted_participation_count_grade_{1-12}` | By grade level |

## Pipeline Output Summary

| Pipeline | Total KPI Rows | Years | Demographics |
|----------|----------------|-------|--------------|
| Advanced Coursework | 157,945 | 2021-2025 | 18 groups |
| Dual Credit | 34,753 | 2024-2025 | 18 groups |
| Gifted and Talented | 187,216 | 2021-2025 | 17 groups |

## Implementation Notes

### Data Format Variations
- KYRC24/25 files use mixed case column names (e.g., "School Year")
- Historical files use uppercase column names (e.g., "SCHOOL YEAR")
- Numbers in newer files may contain commas (e.g., "35,687")
- Suppression markers: `*`, `**`, empty strings

### Course Type Normalization
```
AP / Advanced Placement → ap
IB / International Baccalaureate → ib
CAI / Cambridge Advanced International → cambridge
DC / Dual Credit → dual_credit
ALL → advanced_coursework_total
```

### Pipeline Architecture
All three pipelines extend `BaseETL` and implement:
- `module_column_mappings` - Source to normalized column mapping
- `extract_metrics()` - Metric extraction logic
- `get_suppressed_metric_defaults()` - Default values for suppressed records

### File Type Detection
The advanced coursework pipeline detects file type from filename patterns:
- `participation` or `performance` → participation file
- `offered` → offered courses file
- `ovw_advanced_coursework` → overview file

## Test Coverage

### Unit Tests (37 tests)
- `tests/test_advanced_coursework.py` - 12 tests
- `tests/test_dual_credit.py` - 14 tests
- `tests/test_gifted_talented.py` - 12 tests

### End-to-End Tests (15 tests)
- `tests/test_advanced_coursework_end_to_end.py` - 4 tests
- `tests/test_dual_credit_end_to_end.py` - 5 tests
- `tests/test_gifted_talented_end_to_end.py` - 6 tests

## Configuration Updates

Updated `config/kde_sources.yaml` to include:
- 2022 historical data files for advanced coursework
- 2022 historical data file for gifted and talented
- KYRC25 category file for gifted participation

## Known Issues

1. **Category files not processed**: The `KYRC24_EDOP_Gifted_Participation_by_Category.csv` and `KYRC25_EDOP_Gifted_Participation_by_Category.csv` files are skipped because they lack demographic column. The current ETL focuses on grade-level participation data.

2. **Overview file demographic handling**: KYRC25_OVW_Advanced_Coursework has a different demographic format that requires special handling.

## Files Modified/Created

### ETL Modules (existing, verified working)
- `etl/advanced_coursework.py`
- `etl/dual_credit.py`
- `etl/gifted_talented.py`

### Test Files
- `tests/test_advanced_coursework.py` (existing)
- `tests/test_dual_credit.py` (existing)
- `tests/test_gifted_talented.py` (created)
- `tests/test_advanced_coursework_end_to_end.py` (existing)
- `tests/test_dual_credit_end_to_end.py` (existing)
- `tests/test_gifted_talented_end_to_end.py` (created)

### Configuration
- `config/kde_sources.yaml` - Updated with 2022 files

### Documentation
- `KPIS.md` - Added sections for all three pipelines
